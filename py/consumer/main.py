from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


BOOTSTRAP_SERVERS = "kafka:29092"
REQUEST_TOPIC = "demo-requests"
RESPONSE_TOPIC = "demo-responses"


def main():
    # --- 1. Створюємо топіки (якщо їх ще немає) ---
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    futures = admin.create_topics([
        NewTopic(REQUEST_TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(RESPONSE_TOPIC, num_partitions=1, replication_factor=1),
    ])
    for _topic, fut in futures.items():
        try:
            fut.result()
        except Exception:
            pass

    # --- 2. Створюємо клієнти ---
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "demo-responder-group",
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    # --- 3. Підписуємось і обробляємо запити в нескінченному циклі ---
    consumer.subscribe([REQUEST_TOPIC])
    print(f"Чекаю запитів у '{REQUEST_TOPIC}'. Ctrl+C — вихід.", flush=True)

    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue

        # Парсимо формат "start,finish"
        value = msg.value().decode("utf-8")
        parts = value.split(",")
        start = int(parts[0])
        finish = int(parts[1])
        print(f"<- Отримано запит: start={start} finish={finish}", flush=True)

        # Бізнес-логіка: рахуємо avgSteps.
        avg_steps = (start + finish) // 2

        # Кладемо у відповідь той самий correlation-id, що прийшов у запиті.
        headers = dict(msg.headers() or [])
        reply_headers = []
        if "correlation-id" in headers:
            reply_headers.append(("correlation-id", headers["correlation-id"]))

        producer.produce(
            RESPONSE_TOPIC,
            value=str(avg_steps).encode("utf-8"),
            headers=reply_headers,
        )
        producer.flush()

        print(f"-> Надіслано відповідь: avgSteps={avg_steps}", flush=True)


if __name__ == "__main__":
    main()
