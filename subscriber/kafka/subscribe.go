package kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"

)

type Subscriber struct {
	consumer *kafka.Consumer
}

func (s *Subscriber) Connect(address string, groupId string, offset string) error {
	c, err := kafka.NewConsumer(
		&kafka.ConfigMap{
			"bootstrap.servers": address,
			"group.id":          groupId,
			"auto.offset.reset": offset,
		})
	if err != nil {
		return ConnectionError{err.Error()}
	}
	s.consumer = c
	return nil
}

func (s *Subscriber) Close() {
	s.consumer.Close()
}

func (s *Subscriber) Unsubscribe(channel string) error {
		err := s.consumer.Unsubscribe()
		if err != nil {
			return UnsubscribeFailure{err.Error()}
		}
		return nil
}

func (s *Subscriber) Subscribe(channel string, response chan<- string) {

	err := s.consumer.SubscribeTopics([]string{channel}, nil)
	if err != nil {
		log.Warn( SubscribeFailure{err.Error()})
	}

	for {
		msg, err := s.consumer.ReadMessage(-1)
		if err == nil {
			response <- string(msg.Value)
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}

	s.Close()
}
