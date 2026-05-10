#include <iostream>
#include <string>
#include <librdkafka/rdkafkacpp.h>

const std::string BOOTSTRAP_SERVERS = "kafka:29092";
const std::string REQUEST_TOPIC     = "demo-requests";
const std::string RESPONSE_TOPIC    = "demo-responses";

int main() {
    std::string errstr;

    // --- 1. Створюємо клієнти ---
    RdKafka::Conf *cConf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    cConf->set("bootstrap.servers", BOOTSTRAP_SERVERS, errstr);
    cConf->set("group.id",          "demo-responder-group", errstr);
    cConf->set("auto.offset.reset", "earliest", errstr);

    RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(cConf, errstr);
    delete cConf;

    RdKafka::Conf *pConf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    pConf->set("bootstrap.servers", BOOTSTRAP_SERVERS, errstr);

    RdKafka::Producer *producer = RdKafka::Producer::create(pConf, errstr);
    delete pConf;

    // --- 2. Підписуємось і обробляємо запити в нескінченному циклі ---
    consumer->subscribe({REQUEST_TOPIC});
    std::cout << "Чекаю запитів у '" << REQUEST_TOPIC << "'. Ctrl+C — вихід." << std::endl;

    while (true) {
        RdKafka::Message *msg = consumer->consume(1000);

        if (msg->err() != RdKafka::ERR_NO_ERROR) {
            delete msg;
            continue;
        }

        // Парсимо формат "start,finish"
        std::string value((const char *)msg->payload(), msg->len());
        size_t comma  = value.find(',');
        int    start  = std::stoi(value.substr(0, comma));
        int    finish = std::stoi(value.substr(comma + 1));
        std::cout << "<- Отримано запит: start=" << start
                  << " finish=" << finish << std::endl;

        // Бізнес-логіка: рахуємо avgSteps.
        int         avgSteps = (start + finish) / 2;
        std::string response = std::to_string(avgSteps);

        // Кладемо у відповідь той самий correlation-id, що прийшов у запиті.
        RdKafka::Headers *replyHdrs = RdKafka::Headers::create();
        RdKafka::Headers *reqHdrs   = msg->headers();
        if (reqHdrs) {
            auto list = reqHdrs->get("correlation-id");
            if (!list.empty())
                replyHdrs->add("correlation-id",
                               list.back().value(),
                               (int64_t)list.back().value_size());
        }

        producer->produce(
            RESPONSE_TOPIC,
            RdKafka::Topic::PARTITION_UA,
            RdKafka::Producer::RK_MSG_COPY,
            (void *)response.c_str(), response.size(),
            nullptr, 0,
            0, replyHdrs, nullptr);
        producer->flush(5000);

        std::cout << "-> Надіслано відповідь: avgSteps=" << avgSteps << std::endl;
        delete msg;
    }

    consumer->close();
    delete consumer;
    delete producer;
}
