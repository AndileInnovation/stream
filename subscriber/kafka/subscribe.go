package kafka

import (
	log "github.com/sirupsen/logrus"
	"gopkg.in/Shopify/sarama.v1"
	"time"
)

type Subscriber struct {
	consumer    sarama.Consumer
	subscribers map[string]*subscriber
}

type subscriber struct {
	channel      string
	response     chan<- string
	unsubscribed chan struct{}
	done         chan struct{}
}

func (p *Subscriber) Connect(brokerList []string) error {
	consumer, err := sarama.NewConsumer(brokerList, nil)
	if err != nil {
		return ConnectionError{Reason: err.Error()}
	}

	p.consumer = consumer
	p.subscribers = make(map[string]*subscriber)

	return nil
}

func (p *Subscriber) Close() {
	for _, x := range p.subscribers {
		p.Unsubscribe(x.channel)
	}
	if err := p.consumer.Close(); err != nil {
		log.Fatalln(err)
	}
}

func (p *Subscriber) Unsubscribe(channel string) {
	log.Debug("Unsubscribing from " + channel)
	//Wait for un-subscribed
	close(p.subscribers[channel].done)
	for {
		select {
		case <-time.After(time.Second * 2):
			log.Debug("waiting on " + channel + " to unsubscribe..")
		case _, _ = <-p.subscribers[channel].unsubscribed:
			return
		}
	}
}

func (p *Subscriber) Subscribe(channel string, response chan<- string) {
	log.Debug("Subscribing to " + channel)

	sub := subscriber{
		channel:      channel,
		response:     response,
		done:         make(chan struct{}),
		unsubscribed: make(chan struct{}),
	}
	p.subscribers[channel] = &sub

	partitionConsumer, err := p.consumer.ConsumePartition(channel, 0, sarama.OffsetNewest)
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
				log.Debugf("Consumed message from %s/%d/%d",msg.Topic, msg.Partition, msg.Offset)
				sub.response<-string(msg.Value)
				consumed++
			case <-sub.done:
				log.Debug("done on channel " + sub.channel)
				close(sub.unsubscribed)
				break ConsumerLoop
			}
		}
		log.Info("subscriber read ", consumed, " messages from ", sub.channel, " and is done now")
	}()
}