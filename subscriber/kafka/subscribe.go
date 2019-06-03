package kafka

import (
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"time"
)

type Offset int64

const (
	// OffsetNewest stands for the log head offset, i.e. the offset that will be
	// assigned to the next message that will be produced to the partition. You
	// can send this to a client's GetOffset method to get this offset, or when
	// calling ConsumePartition to start consuming new messages.
	OffsetNewest Offset = -1
	// OffsetOldest stands for the oldest offset available on the broker for a
	// partition. You can send this to a client's GetOffset method to get this
	// offset, or when calling ConsumePartition to start consuming from the
	// oldest offset that is still available on the broker.
	OffsetOldest Offset = -2
)

type Subscriber struct {
	consumer        sarama.Consumer
	subscribers     map[string]*subscriber
	retentionPeriod time.Duration
	offset          Offset
	EnableLogging   bool
}

type subscriber struct {
	channel      string
	response     chan<- string
	unsubscribed chan struct{}
	done         chan struct{}
}

func (p *Subscriber) Connect(brokerList []string, offset Offset) error {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Retention = p.retentionPeriod

	consumer, err := sarama.NewConsumer(brokerList, config)

	if err != nil {
		return ConnectionError{Reason: err.Error()}
	}

	p.consumer = consumer
	p.offset = offset
	p.subscribers = make(map[string]*subscriber)

	return nil
}

func (p *Subscriber) Close() {
	for _, x := range p.subscribers {
		p.Unsubscribe(x.channel)
	}
	if err := p.consumer.Close(); err != nil {
		log.Error("Could not close consumer: ", err)
	}
}

func (p *Subscriber) Unsubscribe(channel string) {
	if p.EnableLogging {
		log.Debug("Unsubscribing from " + channel)
	}
	//Wait for un-subscribed
	close(p.subscribers[channel].done)
	for {
		select {
		case <-time.After(time.Second * 2):
			if p.EnableLogging {
				log.Debug("waiting on " + channel + " to unsubscribe..")
			}
		case _, _ = <-p.subscribers[channel].unsubscribed:
			return
		}
	}
}

func (p *Subscriber) Subscribe(channel string, response chan<- string) {
	t, err := p.consumer.Topics()
	if err != nil {
		log.Fatal(err)
	}
	if p.EnableLogging {
		log.Debug("Topics ", t)
		log.Debug("Subscribing to " + channel)
	}

	sub := subscriber{
		channel:      channel,
		response:     response,
		done:         make(chan struct{}),
		unsubscribed: make(chan struct{}),
	}
	p.subscribers[channel] = &sub

	partitionConsumer, err := p.consumer.ConsumePartition(channel, 0, int64(p.offset))
	if err != nil {
		log.Error(err)
		return
	}

	go func() {
		consumed := 0
	ConsumerLoop:
		for {
			select {
			case msg := <-partitionConsumer.Messages():
				if p.EnableLogging {
					log.Debugf("Consumed message from %s/%d/%d", msg.Topic, msg.Partition, msg.Offset)
				}
				sub.response <- string(msg.Value)
				consumed++
			case <-sub.done:
				if p.EnableLogging {
					log.Debug("done on channel " + sub.channel)
				}
				close(sub.unsubscribed)
				break ConsumerLoop
			}
		}
		log.Info("subscriber read ", consumed, " messages from ", sub.channel, " and is done now")
	}()
}
