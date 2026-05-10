using System;
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

class Program
{
    const string BootstrapServers = "kafka:29092";
    const string RequestTopic = "demo-requests";
    const string ResponseTopic = "demo-responses";

    static void Main()
    {
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

        // --- 2. Створюємо клієнти ---
        IConsumer<string, string> consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = "demo-responder-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        }).Build();

        IProducer<string, string> producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
        }).Build();

        // --- 3. Підписуємось і обробляємо запити в нескінченному циклі ---
        consumer.Subscribe(RequestTopic);
        Console.WriteLine("Чекаю запитів у '" + RequestTopic + "'. Ctrl+C — вихід.");

        while (true)
        {
            ConsumeResult<string, string> request = consumer.Consume();

            // Парсимо формат "start,finish"
            string[] parts = request.Message.Value.Split(',');
            int start  = int.Parse(parts[0]);
            int finish = int.Parse(parts[1]);
            Console.WriteLine("<- Отримано запит: start=" + start + " finish=" + finish);

            // Бізнес-логіка: рахуємо avgSteps.
            int avgSteps = (start + finish) / 2;
            string responseText = avgSteps.ToString();

            // Кладемо у відповідь той самий correlation-id, що прийшов у запиті.
            Headers headers = new Headers();
            byte[] corrId;
            if (request.Message.Headers.TryGetLastBytes("correlation-id", out corrId))
                headers.Add("correlation-id", corrId);

            producer.ProduceAsync(ResponseTopic, new Message<string, string>
            {
                Value = responseText,
                Headers = headers,
            }).GetAwaiter().GetResult();

            Console.WriteLine("-> Надіслано відповідь: avgSteps=" + avgSteps);
        }
    }
}
