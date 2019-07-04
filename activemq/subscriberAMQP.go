package activemq

import (
	"context"
	"github.com/andile-innovation/popcorn/log"
	"pack.ag/amqp"
	"strconv"
	"time"
)

type NewAMQPSubscriberRequest struct {
	Host     string
	Port     int
	Username string
	Password string
}

type subscriber struct {
	channel      string
	response     chan<- string
	unsubscribed chan struct{}
	done         chan struct{}
}

func NewAMQPSubscriber(request NewAMQPSubscriberRequest) AMQPSubscriber {
	return AMQPSubscriber{
		host:     request.Host,
		port:     request.Port,
		username: request.Username,
		password: request.Password,
	}
}

type AMQPSubscriber struct {
	receiver      *amqp.Receiver
	client        *amqp.Client
	session       *amqp.Session
	host          string
	port          int
	username      string
	password      string
	subscribers   map[string]*subscriber
	ctx           context.Context
	EnableLogging bool
}

func (p *AMQPSubscriber) Connect() error {
	client, err := amqp.Dial(p.host+strconv.Itoa(p.port),
		amqp.ConnSASLPlain(p.username, p.password),
	)
	if err != nil {
		log.Error(err)
		return err
	}
	session, err := client.NewSession()
	if err != nil {
		log.Error(err)
		return err
	}

	p.client = client
	p.session = session
	return nil
}

func (p *AMQPSubscriber) Close() {
	for _, x := range p.subscribers {
		p.Unsubscribe(x.channel)
	}
	//if err := p.receiver.Close(p.ctx); err != nil {
	//	log.Error("Could not close consumer: ", err)
	//}
}
func (p *AMQPSubscriber) Unsubscribe(channel string) {
	log.Debug("unsubscribing from " + channel)
	//Wait for un-subscribed
	close(p.subscribers[channel].done)
	for {
		select {
		case <-time.After(time.Second * 2):
			log.Debug("waiting on " + channel + " to unsubscribe..")
		case _, _ = <-p.subscribers[channel].unsubscribed:
			log.Debug("unsubscribed from: " + channel)
			return
		}
	}
}

func (p *AMQPSubscriber) Subscribe(channel string, response chan<- string) {

	sub := subscriber{
		channel:      channel,
		response:     response,
		done:         make(chan struct{}),
		unsubscribed: make(chan struct{}),
	}
	if p.subscribers == nil {
		p.subscribers = make(map[string]*subscriber)
	}
	p.subscribers[channel] = &sub

	if sub, ok := p.subscribers[channel]; ok {
		close(sub.unsubscribed)
	}
	receiver, err := p.session.NewReceiver(
		amqp.LinkSourceAddress(channel),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Error(err)
	}
	p.receiver = receiver

	// todo to take parent context from argument, not Background context
	ctx, cancel := context.WithCancel(context.Background())
	p.ctx = ctx
	go func() {
		for {
			select {
			case <-p.subscribers[channel].done:
				return
			default:
				msg, err := receiver.Receive(ctx)
				if err != nil {
					err = receiver.Close(ctx)
					if err != nil {
						log.Error(err)
					}
					cancel()
					return
				}
				sub.response <- string(msg.Data[0])
				// Accept message
				err = msg.Accept()
				if err != nil {
					log.Error(err)
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-sub.done:
				log.Debug(sub.channel, "done")
				cancel()
				return
			case <-ctx.Done():
				log.Debug("context done")
				return
			}
		}
	}()

}
