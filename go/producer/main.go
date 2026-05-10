package main

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

const (
	bootstrapServers = "kafka:29092"
	requestTopic     = "demo-requests"
	responseTopic    = "demo-responses"
	start            = 10
	finish           = 100
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
	correlationID := uuid.New().String()

	// --- 1. Створюємо топіки (якщо їх ще немає) ---
	createTopics()

	// --- 2. Підписуємось на топік відповідей ПЕРЕД відправкою запиту ---
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{bootstrapServers},
		Topic:       responseTopic,
		GroupID:     "producer-" + uuid.New().String(),
		StartOffset: kafka.LastOffset,
	})
	defer reader.Close()

	// "Розігрів" — чекаємо присвоєння партиції до відправки запиту.
	warmCtx, warmCancel := context.WithTimeout(context.Background(), 2*time.Second)
	reader.FetchMessage(warmCtx) //nolint:errcheck
	warmCancel()

	// --- 3. Шлемо ОДИН запит ---
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{bootstrapServers},
		Topic:   requestTopic,
	})

	err := writer.WriteMessages(context.Background(), kafka.Message{
		Value: []byte(fmt.Sprintf("%d,%d", start, finish)),
		Headers: []kafka.Header{
			{Key: "correlation-id", Value: []byte(correlationID)},
		},
	})
	writer.Close()
	if err != nil {
		panic(err)
	}

	fmt.Printf("-> Запит надіслано: start=%d finish=%d (id=%s)\n", start, finish, correlationID)

	// --- 4. Чекаємо відповідь зі своїм correlation-id ---
	deadline := time.Now().Add(30 * time.Second)
	gotReply := false
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		msg, err := reader.FetchMessage(ctx)
		cancel()
		if err != nil {
			continue
		}

		replyID := ""
		for _, h := range msg.Headers {
			if h.Key == "correlation-id" {
				replyID = string(h.Value)
			}
		}
		if replyID == correlationID {
			avgSteps, _ := strconv.Atoi(string(msg.Value))
			fmt.Printf("<- Отримано відповідь: avgSteps=%d\n", avgSteps)
			gotReply = true
			break
		}
	}

	if !gotReply {
		fmt.Println("!! Відповідь не прийшла за 30 сек.")
	}

	// --- 5. Контейнер живе вічно (поки docker stop / Ctrl+C) ---
	fmt.Println("Готово. Контейнер живе. Ctrl+C / docker stop — вихід.")
	select {}
}
