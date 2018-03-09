package redis

import (
	"github.com/mediocregopher/radix.v2/pubsub"
	"github.com/mediocregopher/radix.v2/sentinel"
	log "github.com/sirupsen/logrus"
	"time"
)

type Subscriber struct {
	sentinel     *sentinel.Client
	master       string
	done         chan struct{}
	unsubscribed chan struct{}
}

// Connect creates a sentinel client. Connects to the given sentinel instance,
// pulls the information for the master, and creates an initial pool of connections
// for the master. The client will automatically replace the pool for any master
// should sentinel decide to fail the master over
func (p *Subscriber) Connect(address string, master string) error {
	sentinelClient, err := sentinel.NewClient("tcp", address, 3, master)
	if err != nil {
		return ConnectionError{err.Error()}
	}
	p.sentinel = sentinelClient
	p.master = master
	p.done = make(chan struct{})
	p.unsubscribed = make(chan struct{})
	return nil
}

func (p *Subscriber) Unsubscribe() {
	close(p.done)
	log.Debug("waiting for un-subscribe signal")
	//Wait for un-subscribed
	for {
		select {
		case <-time.After(time.Second * 3):
			log.Debug("waiting..")
		case _, _ = <-p.unsubscribed:
			return
		}
	}
}

func (p *Subscriber) Subscribe(channel string, response chan<- string) {
	go func() {
		if err := p.startRead(channel, response); err != nil {
			log.Warn("subscribe error: ", err)
		}
	}()
}

func (p *Subscriber) startRead(channel string, response chan<- string) error {
	//Get a connection from sentinel master
	master, err := p.sentinel.GetMaster(p.master)
	if err != nil {
		return CouldNotGetMaster{err.Error()}
	}
	defer p.sentinel.PutMaster(p.master, master)

	//Convert client to PubSub client
	subscriberClient := pubsub.NewSubClient(master)
	if err := subscriberClient.Subscribe(channel); err.Err != nil {
		return SubscribeFailure{err.Err.Error()}
	}

	//Set a read timeout, if we don't receive any messages it will block forever
	subscriberClient.Client.ReadTimeout = 15 * time.Second
	for {
		select {
		case <-p.done:
			log.Debug("done..")
			close(p.unsubscribed)
			return nil
		default:
			//Wait for the next message
			log.Debug("waiting for next message..")
			subResp := subscriberClient.Receive()
			if subResp.Err != nil {
				if subResp.Timeout() {
					log.Debug("receive timeout reached, reconnecting")
					p.sentinel.PutMaster(p.master, master)
					return p.startRead(channel, response)
				} else {
					return ReceiveFailure{err.Error()}
				}
			}
			response <- subResp.Message
		}
	}
	return nil
}
