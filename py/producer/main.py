import time
import uuid

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


BOOTSTRAP_SERVERS = "kafka:29092"
REQUEST_TOPIC = "demo-requests"
RESPONSE_TOPIC = "demo-responses"

START = 10
FINISH = 100


def main():
    correlation_id = str(uuid.uuid4())

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

    # --- 2. Підписуємось на топік відповідей ПЕРЕД відправкою запиту ---
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "producer-" + str(uuid.uuid4()),
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([RESPONSE_TOPIC])

    # "Розігрів" — щоб брокер встиг присвоїти партицію до відправки запиту.
    consumer.poll(2.0)

    # --- 3. Шлемо ОДИН запит ---
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    request_value = f"{START},{FINISH}"

    producer.produce(
        REQUEST_TOPIC,
        value=request_value.encode("utf-8"),
        headers=[("correlation-id", correlation_id.encode("utf-8"))],
    )
    producer.flush()

    print(
        f"-> Запит надіслано: start={START} finish={FINISH} (id={correlation_id})",
        flush=True,
    )

    # --- 4. Чекаємо відповідь зі своїм correlation-id ---
    deadline = time.time() + 30
    got_reply = False
    while time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue

        headers = dict(msg.headers() or [])
        reply_id_bytes = headers.get("correlation-id", b"")
        reply_id = reply_id_bytes.decode("utf-8")

        if reply_id == correlation_id:
            avg_steps = int(msg.value().decode("utf-8"))
            print(f"<- Отримано відповідь: avgSteps={avg_steps}", flush=True)
            got_reply = True
            break
        # інакше — це чужий reply, ігноруємо

    if not got_reply:
        print("!! Відповідь не прийшла за 30 сек.", flush=True)

    # --- 5. Контейнер живе вічно (поки docker stop / Ctrl+C) ---
    print("Готово. Контейнер живе. Ctrl+C / docker stop — вихід.", flush=True)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
