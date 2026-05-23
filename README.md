Цей проект реалізує шаблон "request-reply" за допомогою Apache Kafka
Проект складається з двох окремих сервісів

Producer -надсилає запит з діапазоном чисел "start, finish"
Consumer - отримує запит, обчислює середню кількість кроків для послідовності Колатца

Сервіси спілкуються через 2 Кафка Топіки:

demo-requests -для запитів
demo-responses - для відповідей

Команди для запуску
1) Створити докер мережу

docker network create kafka-net

2) запустити кафка

docker run -d --name kafka --network kafka-net -p 9092:9092 \
-e KAFKA_NODE_ID=1 \
-e KAFKA_PROCESS_ROLES=broker,controller \
-e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 \
-e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
-e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093,PLAINTEXT_HOST://0.0.0.0:9092 \
-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092 \
-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
-e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
-e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
-e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
-e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
-e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
-e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
-v kafka-data:/var/lib/kafka/data \
confluentinc/cp-kafka:7.7.1

3) Зібрати Consumer
   docker build -t kafka-demo-consumer -f java/consumer/Dockerfile .

4) Зібрати Producer
   docker build -t kafka-demo-producer -f java/producer/Dockerfile .

5) Запустити Consumer
   docker run -d --name kafka-consumer --network kafka-net kafka-demo-consumer

6) Запустити Producer
   docker run -d --name kafka-producer --network kafka-net kafka-demo-producer

7) Перевірити контейнери та логи
   docker ps
   docker logs kafka-consumer
   docker logs kafka-producer

Очікуваний результат :
Consumer:
Чекаю запитів у 'demo-requests'. Ctrl+C — вихід.
<- Отримано запит: start=1 finish=10000000
-> Надіслано відповідь: avgSteps=155

Producer:
-> Запит надіслано: start=1 finish=10000000 (id=83956b15-e950-4448-bbef-7be9c244153a)
<- Отримано відповідь: avgSteps=155
Готово. Контейнер живе. Ctrl+C / docker stop — вихід.
