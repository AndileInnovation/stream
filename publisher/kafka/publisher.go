package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/andile-innovation/popcorn/log"
)

type Publisher struct {
	producer sarama.SyncProducer
}

func (p *Publisher) Connect(brokerList []string) error {
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true

	// On the broker side, you may want to change the following settings to get
	// stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Error("Failed to start Sarama producer:", err)
		return ConnectionError{err.Error()}
	}

	p.producer = producer
	return nil
}

func (p *Publisher) Publish(destination string, data []byte) error {
	// We are not setting a message key, which means that all messages will
	// be distributed randomly over the different partitions.
	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		//partition, offset, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: destination,
		Value: sarama.ByteEncoder(data),
	})
	if err != nil {
		return PublishingFailed{Reason: err.Error()}
	} else {
		// The tuple (topic, partition, offset) can be used as a unique identifier
		// for a message in a Kafka cluster.
		//log.Debug("Published kafka message", "`"+string(data)[:30]+"...`", "to", "`"+destination+"`", "on partition", partition, "with offset", offset)
	}
	return nil
}
