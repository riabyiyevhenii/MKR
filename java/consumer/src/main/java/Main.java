import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class Main {

    static final String BOOTSTRAP_SERVERS = "kafka:29092";
    static final String REQUEST_TOPIC     = "demo-requests";
    static final String RESPONSE_TOPIC    = "demo-responses";

    public static void main(String[] args) throws Exception {

        // --- 1. Створюємо топіки (якщо їх ще немає) ---
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        AdminClient admin = AdminClient.create(adminProps);
        try {
            admin.createTopics(Arrays.asList(
                new NewTopic(REQUEST_TOPIC,  1, (short) 1),
                new NewTopic(RESPONSE_TOPIC, 1, (short) 1)
            )).all().get();
        } catch (ExecutionException e) {
            // топіки вже існують — все ок
        }
        admin.close();

        // --- 2. Створюємо клієнти ---
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,                 "demo-responder-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,        BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // --- 3. Підписуємось і обробляємо запити в нескінченному циклі ---
        consumer.subscribe(Collections.singletonList(REQUEST_TOPIC));
        System.out.println("Чекаю запитів у '" + REQUEST_TOPIC + "'. Ctrl+C — вихід.");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> request : records) {
                // Парсимо формат "start,finish"
                String[] parts = request.value().split(",");
                int start  = Integer.parseInt(parts[0]);
                int finish = Integer.parseInt(parts[1]);
                System.out.println("<- Отримано запит: start=" + start + " finish=" + finish);

                // Бізнес-логіка: рахуємо avgSteps.
                int avgSteps = (start + finish) / 2;

                // Кладемо у відповідь той самий correlation-id, що прийшов у запиті.
                ProducerRecord<String, String> reply =
                    new ProducerRecord<>(RESPONSE_TOPIC, null, String.valueOf(avgSteps));
                Header h = request.headers().lastHeader("correlation-id");
                if (h != null) reply.headers().add(new RecordHeader("correlation-id", h.value()));

                producer.send(reply).get();
                System.out.println("-> Надіслано відповідь: avgSteps=" + avgSteps);
            }
        }
    }
}
