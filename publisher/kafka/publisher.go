package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

type Publisher struct {
	publisher *kafka.Producer
}

func (p *Publisher) Connect(address string) error {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": address})
	if err != nil {
		return ConnectionError{err.Error()}
	}
	p.publisher = producer

	return nil
}

func (p *Publisher) Publish(destination string, data []byte) {

	//// Delivery report handler for produced messages
	//go func() {
	//	for e := range p.publisher.Events() {
	//		switch ev := e.(type) {
	//		case *kafka.Message:
	//			if ev.TopicPartition.Error != nil {
	//				fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
	//			} else {
	//				log.Info("Delivered message to %v\n", ev.TopicPartition)
	//			}
	//		}
	//	}
	//}()

	// Produce messages to topic (asynchronously)
	err := p.publisher.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &destination, Partition: kafka.PartitionAny},
		Value:          data,
	}, nil,
	)
	if err != nil {
		log.Warn(PublishingFailed{err.Error()})
	}
	// Wait for message deliveries
	p.publisher.Flush(1 * 1000)
	}
