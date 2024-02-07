package main

import (
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	topic := "testTopic"

	//Consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "testConsumer",
		"auto.offset.reset": "smallest",
	})
	if err != nil {
		log.Fatal(err)
	}
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatal(err)
	}
	for {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("Message on %s:\n%s\n",
				e.TopicPartition, string(e.Value))
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
			if e.Code() == kafka.ErrAllBrokersDown {
				break
			}
		}
	}

}
