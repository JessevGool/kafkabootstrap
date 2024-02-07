package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type OrderPlacer struct {
	producer      *kafka.Producer
	topic         string
	delivery_chan chan kafka.Event
}

func NewOrderPlacer(p *kafka.Producer, topic string) *OrderPlacer {
	return &OrderPlacer{
		producer:      p,
		topic:         topic,
		delivery_chan: make(chan kafka.Event, 10000),
	}
}

func (op *OrderPlacer) placeOrder(orderType string, size int) error {
	var (
		format  = fmt.Sprintf(" %s - %d", orderType, size)
		payload = []byte(format)
	)
	err := op.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &op.topic, Partition: kafka.PartitionAny},
		Value:          payload,
	},
		op.delivery_chan,
	)
	if err != nil {
		return err
	}

	<-op.delivery_chan
	fmt.Printf("Order placed successfully: %s\n", format)
	return nil
}

func main() {
	topic := "testTopic"

	//Producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"acks":              "all",
		"client.id":         "producerTest",
	})
	if err != nil {
		log.Fatal(err)
	}
	op := NewOrderPlacer(p, topic)

	for i := 0; i < 1000; i++ {
		if err := op.placeOrder("MarketOrder", i+1); err != nil {
			log.Fatal(err)
		}

		time.Sleep(3 * time.Second)
	}

}
