package redis

import (
	"github.com/mediocregopher/radix.v2/pubsub"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/mediocregopher/radix.v2/sentinel"
	"gitlab.com/andile/go/popcorn/log"
	"time"
)

type Subscriber struct {
	sentinel    *sentinel.Client
	subClient   *pubsub.SubClient
	master      string
	subscribers map[string]*subscriber
	useSentinel bool

	done         chan struct{}
	unsubscribed chan struct{}
}

type subscriber struct {
	response chan<- string
}

// Connect creates a sentinel client. Connects to the given sentinel instance,
// pulls the information for the master, and creates an initial pool of connections
// for the master. The client will automatically replace the pool for any master
// should sentinel decide to fail the master over
func (p *Subscriber) Connect(address string, master string, poolSize int, useSentinel bool) error {
	p.useSentinel = useSentinel

	if useSentinel {
		sentinelClient, err := sentinel.NewClient("tcp", address, poolSize, master)
		if err != nil {
			return ConnectionError{err.Error()}
		}
		p.sentinel = sentinelClient
		p.master = master

		p.subscribers = make(map[string]*subscriber)
		return nil
	} else {
		client, err := redis.Dial("tcp", address)
		if err != nil {
			return ConnectionError{err.Error()}
		}
		p.subClient = pubsub.NewSubClient(client)

		p.subscribers = make(map[string]*subscriber)
		return nil
	}
}

func (p *Subscriber) Close() {
	p.Unsubscribe("")

	if !p.useSentinel {
		p.subClient.Client.Close()
	}
}

func (p *Subscriber) Unsubscribe(channel string) {
	log.Debug("Unsubscribing from all channels")
	//Wait for un-subscribed
	close(p.done)
	for {
		select {
		case <-time.After(time.Second * 5):
			close(p.unsubscribed)//TODO: This should not happen
			log.Debug("forcefully closing")
		case <-p.unsubscribed:
			return
		}
	}
}

func (p *Subscriber) Subscribe(channel string, response chan<- string) {
	sub := subscriber{
		response: response,
	}
	p.subscribers[channel] = &sub
}

func (p *Subscriber) StartSubscribers() {
	var channels []string
	for channel := range p.subscribers {
		channels = append(channels, channel)
	}
	p.unsubscribed = make(chan struct{})
	d := make(chan struct{})
	var subClient *pubsub.SubClient
	var master *redis.Client

	if p.useSentinel {
		//Get a connection from sentinel master
		master, err := p.sentinel.GetMaster(p.master)
		if err != nil {
			log.Warn(CouldNotGetMaster{err.Error()})
			return
		}

		//Convert client to PubSub client
		subClient = pubsub.NewSubClient(master)
	}

	log.Debug("Subscribing to ", channels)
	subClient = p.subClient
	if err := subClient.Subscribe(channels); err.Err != nil {
		log.Warn(SubscribeFailure{err.Err.Error()})
		return
	}
	go func() {
		if p.useSentinel {
			defer p.sentinel.PutMaster(p.master, master)
		}

		if err := p.startRead(subClient, d); err != nil {
			p.sentinel.PutMaster(p.master, master)
			log.Fatal(err)
		}
		log.Debug("StartSubscribers finished")
	}()
	p.done =d
}

func (p *Subscriber) startRead(subscriberClient *pubsub.SubClient, done chan struct{}) error {
	//Set a read timeout, if we don't receive any messages Receive() will block forever without a timeout
	subscriberClient.Client.ReadTimeout = 3 * time.Second
	for {
		select {
		case <-done:
			close(p.unsubscribed)
			return nil
		default:
			//Wait for the next message
			subResp := subscriberClient.Receive()
			if subResp.Err != nil {
				if subResp.Timeout() {
					//log.Debug("receive timeout reached on channel " + p.channel)
				} else {
					return ReceiveFailure{subResp.Err.Error()}
				}
			} else {
				for c, sub := range p.subscribers {
					if c == subResp.Channel {
						sub.response <- subResp.Message
					}
				}
			}
		}
	}
	return nil
}
