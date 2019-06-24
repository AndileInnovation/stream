package activemq

import (
	"context"
	"github.com/andile-innovation/popcorn/log"
	"pack.ag/amqp"
	"strconv"
	"time"
)
type NewAMQPSubscriberRequest struct {
	Host string
	Port int
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
		host:		request.Host,
		port: 		request.Port,
		username: 	request.Username,
		password: 	request.Password,
	}
}

type AMQPSubscriber struct {
	receiver *amqp.Receiver
	client *amqp.Client
	session *amqp.Session
	host string
	port int
	username string
	password string
	subscribers     map[string]*subscriber
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
	}

	p.client = client
	p.session = session
	return nil
}

//func (p *AMQPSubscriber) Close() {
//	for _, x := range p.subscribers {
//		p.Unsubscribe(x.channel)
//	}
//	if err := p.receiver.Close(); err != nil {
//		log.Error("Could not close consumer: ", err)
//	}
//}
//func (p *AMQPSubscriber) Unsubscribe(channel string) {
//	if p.EnableLogging {
//		log.Debug("Unsubscribing from " + channel)
//	}
//	//Wait for un-subscribed
//	close(p.subscribers[channel].done)
//	for {
//		select {
//		case <-time.After(time.Second * 2):
//			if p.EnableLogging {
//				log.Debug("waiting on " + channel + " to unsubscribe..")
//			}
//		case _, _ = <-p.subscribers[channel].unsubscribed:
//			return
//		}
//	}
//}

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

	// todo to pass context as argument
	ctx := context.TODO()

	receiver, err := p.session.NewReceiver(
		amqp.LinkSourceAddress(channel),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Error(err)
	}
	p.receiver = receiver
	defer func() {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		err = receiver.Close(ctx)
		if err != nil {
			log.Error(err)
		}
		cancel()
	}()

	subCtx, cancel := context.WithCancel(ctx)
	go func() {
		for {
			msg, err := receiver.Receive(subCtx)
			if err != nil {
				cancel()
				return
			}

			log.Info("ch1 -- ", string(msg.GetData()))
			sub.response <- string(msg.GetData())
			if err != nil {
				log.Error(err)
			}

			// Accept message
			err = msg.Accept()
			if err != nil {
				log.Error(err)
			}
		}
	}()

	go func() {
		for {
			select {
				case <-sub.done:
					cancel()
					return
				case <-subCtx.Done():
					return
			}
		}
	}()




}

