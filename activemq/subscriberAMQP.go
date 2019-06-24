package activemq

import (
	"context"
	"github.com/andile-innovation/popcorn/log"
	"pack.ag/amqp"
	"strconv"
	"time"
)
type NewAMQPSubsciberRequest struct {
	Host string
	Port int
	Username string
	Password string
}

func NewAMQPSubsciber(request NewAMQPSubsciberRequest) AMQPSubsciber {
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
}

func (p *AMQPSubscriber) Connect() error {
	client, err := amqp.Dial(p.host+strconv.Itoa(p.port),
		amqp.ConnSASLPlain(p.username, p.password),
	)
	if err != nil {
		log.Error(err)
		return err
	}
	defer func() {
		err := client.Close()
		if err != nil {
			log.Error(err)
		}
	}()
	session, err := client.NewSession()
	if err != nil {
		log.Error(err)
	}

	ctx := context.Background()

	receiver, err := session.NewReceiver(
		amqp.LinkSourceAddress("/"),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Error(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		err := receiver.Close(ctx)
		if err != nil {
			log.Error(err)
		}
		cancel()
	}()

	p.Client = client
	p.Session = session
	return nil
}

func (p *AMQPSubscriber) Subscribe(channel string, response chan<- string) {
	ctx := context.Background()

	receiver, err := p.Session.NewReceiver(
		amqp.LinkSourceAddress(channel),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Error(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		err := receiver.Close(ctx)
		if err != nil {
			log.Error(err)
		}
		cancel()
	}()
}

