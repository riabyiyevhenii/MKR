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
    static final int    START             = 10;
    static final int    FINISH            = 100;

    public static void main(String[] args) throws Exception {
        String correlationId = UUID.randomUUID().toString();

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

        // --- 2. Підписуємось на топік відповідей ПЕРЕД відправкою запиту ---
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,           "producer-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "latest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(RESPONSE_TOPIC));

        // "Розігрів" — чекаємо присвоєння партиції до відправки запиту.
        consumer.poll(Duration.ofSeconds(2));

        // --- 3. Шлемо ОДИН запит ---
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,        BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        ProducerRecord<String, String> record = new ProducerRecord<>(REQUEST_TOPIC, null, START + "," + FINISH);
        record.headers().add(new RecordHeader("correlation-id", correlationId.getBytes(StandardCharsets.UTF_8)));
        producer.send(record).get();
        producer.close();

        System.out.println("-> Запит надіслано: start=" + START + " finish=" + FINISH + " (id=" + correlationId + ")");

        // --- 4. Чекаємо відповідь зі своїм correlation-id ---
        long deadline  = System.currentTimeMillis() + 30_000;
        boolean gotReply = false;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> reply : records) {
                Header h = reply.headers().lastHeader("correlation-id");
                String replyId = h != null ? new String(h.value(), StandardCharsets.UTF_8) : "";
                if (replyId.equals(correlationId)) {
                    int avgSteps = Integer.parseInt(reply.value());
                    System.out.println("<- Отримано відповідь: avgSteps=" + avgSteps);
                    gotReply = true;
                    break;
                }
            }
            if (gotReply) break;
        }

        if (!gotReply) System.out.println("!! Відповідь не прийшла за 30 сек.");

        // --- 5. Контейнер живе вічно (поки docker stop / Ctrl+C) ---
        System.out.println("Готово. Контейнер живе. Ctrl+C / docker stop — вихід.");
        Thread.sleep(Long.MAX_VALUE);
    }
}
