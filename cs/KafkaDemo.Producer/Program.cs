using System;
using System.Text;
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

class Program
{
    const string BootstrapServers = "kafka:29092";
    const string RequestTopic = "demo-requests";
    const string ResponseTopic = "demo-responses";

    // Параметри запиту.
    const int Start = 10;
    const int Finish = 100;

    static void Main()
    {
        // correlation-id — унікальний ID запиту. По ньому знайдемо "свою" відповідь.
        string correlationId = Guid.NewGuid().ToString();

        // --- 1. Створюємо топіки (якщо їх ще немає) ---
        IAdminClient admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();
        try
        {
            admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification { Name = RequestTopic,  NumPartitions = 1, ReplicationFactor = 1 },
                new TopicSpecification { Name = ResponseTopic, NumPartitions = 1, ReplicationFactor = 1 },
            }).GetAwaiter().GetResult();
        }
        catch (CreateTopicsException) { /* топіки вже існують — все ок */ }

        // --- 2. Підписуємось на топік відповідей ПЕРЕД відправкою запиту ---
        IConsumer<string, string> consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = "producer-" + Guid.NewGuid(),  // унікальна група на кожен запуск
            AutoOffsetReset = AutoOffsetReset.Latest, // нас цікавлять тільки нові відповіді
        }).Build();

        consumer.Subscribe(ResponseTopic);

        // "Розігрів" — щоб брокер встиг присвоїти партицію до того, як ми надішлемо запит.
        consumer.Consume(TimeSpan.FromSeconds(2));

        // --- 3. Шлемо ОДИН запит ---
        IProducer<string, string> producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
        }).Build();

        // Формат повідомлення: "start,finish"
        string requestValue = Start + "," + Finish;

        producer.ProduceAsync(RequestTopic, new Message<string, string>
        {
            Value = requestValue,
            Headers = new Headers { { "correlation-id", Encoding.UTF8.GetBytes(correlationId) } },
        }).GetAwaiter().GetResult();

        Console.WriteLine("-> Запит надіслано: start=" + Start + " finish=" + Finish + " (id=" + correlationId + ")");

        // --- 4. Чекаємо відповідь зі своїм correlation-id ---
        while (true)
        {
            ConsumeResult<string, string> reply = consumer.Consume(TimeSpan.FromSeconds(30));
            if (reply == null)
            {
                Console.WriteLine("!! Відповідь не прийшла за 30 сек.");
                break;
            }

            string replyId = Encoding.UTF8.GetString(reply.Message.Headers.GetLastBytes("correlation-id"));
            if (replyId == correlationId)
            {
                int avgSteps = int.Parse(reply.Message.Value);
                Console.WriteLine("<- Отримано відповідь: avgSteps=" + avgSteps);
                break;
            }
            // інакше — це чужий reply, ігноруємо
        }

        // --- 5. Контейнер живе вічно (поки docker stop / Ctrl+C) ---
        Console.WriteLine("Готово. Контейнер живе. Ctrl+C / docker stop — вихід.");
        Thread.Sleep(Timeout.Infinite);
    }
}
