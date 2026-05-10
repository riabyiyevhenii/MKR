package main

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	kafka "github.com/segmentio/kafka-go"
)

const (
	bootstrapServers = "kafka:29092"
	requestTopic     = "demo-requests"
	responseTopic    = "demo-responses"
)

func createTopics() {
	conn, err := kafka.Dial("tcp", bootstrapServers)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err)
	}

	ctrlConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err)
	}
	defer ctrlConn.Close()

	// Ігноруємо помилку "topic already exists"
	ctrlConn.CreateTopics(
		kafka.TopicConfig{Topic: requestTopic,  NumPartitions: 1, ReplicationFactor: 1},
		kafka.TopicConfig{Topic: responseTopic, NumPartitions: 1, ReplicationFactor: 1},
	)
}

func main() {
	// --- 1. Створюємо топіки (якщо їх ще немає) ---
	createTopics()

	// --- 2. Створюємо клієнти ---
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{bootstrapServers},
		Topic:       requestTopic,
		GroupID:     "demo-responder-group",
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{bootstrapServers},
		Topic:   responseTopic,
	})
	defer writer.Close()

	// --- 3. Підписуємось і обробляємо запити в нескінченному циклі ---
	fmt.Printf("Чекаю запитів у '%s'. Ctrl+C — вихід.\n", requestTopic)

	for {
		msg, err := reader.FetchMessage(context.Background())
		if err != nil {
			continue
		}

		// Парсимо формат "start,finish"
		parts := strings.Split(string(msg.Value), ",")
		start, _ := strconv.Atoi(parts[0])
		finish, _ := strconv.Atoi(parts[1])
		fmt.Printf("<- Отримано запит: start=%d finish=%d\n", start, finish)

		// Бізнес-логіка: рахуємо avgSteps.
		avgSteps := (start + finish) / 2

		// Кладемо у відповідь той самий correlation-id, що прийшов у запиті.
		var replyHeaders []kafka.Header
		for _, h := range msg.Headers {
			if h.Key == "correlation-id" {
				replyHeaders = []kafka.Header{{Key: "correlation-id", Value: h.Value}}
				break
			}
		}

		writer.WriteMessages(context.Background(), kafka.Message{ //nolint:errcheck
			Value:   []byte(strconv.Itoa(avgSteps)),
			Headers: replyHeaders,
		})

		reader.CommitMessages(context.Background(), msg) //nolint:errcheck
		fmt.Printf("-> Надіслано відповідь: avgSteps=%d\n", avgSteps)
	}
}
