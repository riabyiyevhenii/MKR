#include <chrono>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <librdkafka/rdkafkacpp.h>

const std::string BOOTSTRAP_SERVERS = "kafka:29092";
const std::string REQUEST_TOPIC     = "demo-requests";
const std::string RESPONSE_TOPIC    = "demo-responses";
const int         START             = 10;
const int         FINISH            = 100;

static std::string generateUUID() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> dis;
    char buf[37];
    std::snprintf(buf, sizeof(buf),
        "%08x-%04x-4%03x-%04x-%08x%04x",
        dis(gen),
        dis(gen) & 0xFFFFu,
        dis(gen) & 0x0FFFu,
        (dis(gen) & 0x3FFFu) | 0x8000u,
        dis(gen),
        dis(gen) & 0xFFFFu);
    return buf;
}

int main() {
    std::string errstr;
    std::string correlationId = generateUUID();

    // --- 1. Підписуємось на топік відповідей ПЕРЕД відправкою запиту ---
    RdKafka::Conf *cConf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    cConf->set("bootstrap.servers", BOOTSTRAP_SERVERS, errstr);
    cConf->set("group.id",          "producer-" + generateUUID(), errstr);
    cConf->set("auto.offset.reset", "latest", errstr);

    RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(cConf, errstr);
    delete cConf;

    consumer->subscribe({RESPONSE_TOPIC});

    // "Розігрів" — чекаємо присвоєння партиції до відправки запиту.
    RdKafka::Message *warmMsg = consumer->consume(2000);
    delete warmMsg;

    // --- 2. Шлемо ОДИН запит ---
    RdKafka::Conf *pConf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    pConf->set("bootstrap.servers", BOOTSTRAP_SERVERS, errstr);

    RdKafka::Producer *producer = RdKafka::Producer::create(pConf, errstr);
    delete pConf;

    std::string requestValue = std::to_string(START) + "," + std::to_string(FINISH);

    // Headers — власність передається бібліотеці після produce().
    RdKafka::Headers *hdrs = RdKafka::Headers::create();
    hdrs->add("correlation-id", correlationId.c_str(), (int64_t)correlationId.size());

    producer->produce(
        REQUEST_TOPIC,
        RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY,
        (void *)requestValue.c_str(), requestValue.size(),
        nullptr, 0,
        0, hdrs, nullptr);
    producer->flush(5000);
    delete producer;

    std::cout << "-> Запит надіслано: start=" << START
              << " finish=" << FINISH
              << " (id=" << correlationId << ")" << std::endl;

    // --- 3. Чекаємо відповідь зі своїм correlation-id ---
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    bool gotReply = false;

    while (std::chrono::steady_clock::now() < deadline) {
        RdKafka::Message *msg = consumer->consume(1000);

        if (msg->err() == RdKafka::ERR_NO_ERROR) {
            std::string replyId;
            RdKafka::Headers *replyHdrs = msg->headers();
            if (replyHdrs) {
                auto list = replyHdrs->get("correlation-id");
                if (!list.empty())
                    replyId = std::string((const char *)list.back().value(),
                                         list.back().value_size());
            }

            if (replyId == correlationId) {
                std::string val((const char *)msg->payload(), msg->len());
                std::cout << "<- Отримано відповідь: avgSteps=" << val << std::endl;
                gotReply = true;
                delete msg;
                break;
            }
        }
        delete msg;
    }

    if (!gotReply)
        std::cout << "!! Відповідь не прийшла за 30 сек." << std::endl;

    // --- 4. Контейнер живе вічно (поки docker stop / Ctrl+C) ---
    std::cout << "Готово. Контейнер живе. Ctrl+C / docker stop — вихід." << std::endl;
    consumer->close();
    delete consumer;

    while (true)
        std::this_thread::sleep_for(std::chrono::hours(1));
}
