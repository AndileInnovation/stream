package redis

import (
	"github.com/mediocregopher/radix.v2/pubsub"
	"github.com/mediocregopher/radix.v2/sentinel"
	log "github.com/sirupsen/logrus"
	"time"
)

type Subscriber struct {
	sentinel *sentinel.Client
	master   string
	subscribers map[string]*subscriber
}

type subscriber struct {
	channel      string
	response     chan<- string
	unsubscribed chan struct{}
	done         chan struct{}
}

// Connect creates a sentinel client. Connects to the given sentinel instance,
// pulls the information for the master, and creates an initial pool of connections
// for the master. The client will automatically replace the pool for any master
// should sentinel decide to fail the master over
func (p *Subscriber) Connect(address string, master string, poolSize int) error {
	sentinelClient, err := sentinel.NewClient("tcp", address, poolSize, master)
	if err != nil {
		return ConnectionError{err.Error()}
	}
	p.sentinel = sentinelClient
	p.master = master
	p.subscribers = make(map[string]*subscriber)
	return nil
}

func (p *Subscriber) Close() {
	for _, x := range p.subscribers {
		p.Unsubscribe(x.channel)
	}
}

func (p *Subscriber) Unsubscribe(channel string) {
	log.Debug("Unsubscribing from " + channel)
	//Wait for un-subscribed
	close(p.subscribers[channel].done)
	for {
		select {
		case <-time.After(time.Second * 3):
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

	//Get a connection from sentinel master
	master, err := p.sentinel.GetMaster(p.master)
	if err != nil {
		log.Warn(CouldNotGetMaster{err.Error()})
		return
	}

	//Convert client to PubSub client
	subscriberClient := pubsub.NewSubClient(master)
	if err := subscriberClient.Subscribe(channel); err.Err != nil {
		log.Warn(SubscribeFailure{err.Err.Error()})
		return
	}

	go func() {
		defer p.sentinel.PutMaster(p.master, master)

		if err := sub.startRead(subscriberClient); err != nil {
			p.sentinel.PutMaster(p.master, master)
			log.Fatal(err)
		}
		log.Debug("startRead finished for " + channel)
	}()
}

func (p *subscriber) startRead(subscriberClient *pubsub.SubClient) error {
	//Set a read timeout, if we don't receive any messages Receive() will block forever without a timeout
	subscriberClient.Client.ReadTimeout = 10 * time.Second
	for {
		select {
		case <-p.done:
			log.Debug("done on channel " + p.channel)
			close(p.unsubscribed)
			return nil
		default:
			//Wait for the next message
			subResp := subscriberClient.Receive()
			if subResp.Err != nil {
				if subResp.Timeout() {
					log.Debug("receive timeout reached on channel " + p.channel)
				} else {
					return ReceiveFailure{subResp.Err.Error()}
				}
			} else {
				log.Debug("new message on channel " + p.channel + " -- " + subResp.Message)
				p.response <- subResp.Message
			}
		}
	}
	return nil
}