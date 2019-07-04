package activemq

import (
	"context"
	"github.com/andile-innovation/popcorn/log"
	"net"
	"pack.ag/amqp"
	"strconv"
	"time"
)

type NewAMQPPublisherRequest struct {
	Host     string
	Port     int
	Username string
	Password string
}

func NewAMQPPublisher(request NewAMQPPublisherRequest) AMQPPublisher {
	return AMQPPublisher{
		host:     request.Host,
		port:     request.Port,
		username: request.Username,
		password: request.Password,
	}
}

type AMQPPublisher struct {
	client   *amqp.Client
	conn     net.Conn
	host     string
	port     int
	username string
	password string
}

func (p *AMQPPublisher) Connect() error {
	client, err := amqp.Dial(p.host+strconv.Itoa(p.port),
		amqp.ConnSASLPlain(p.username, p.password),
	)
	if err != nil {
		return err
	}

	p.client = client
	return nil
}

func (p *AMQPPublisher) Publish(channel string, data []byte) error {
	session, err := p.client.NewSession()
	if err != nil {
		log.Error("Creating AMQP session:", err)
		return err
	}

	// todo pass context as argument
	ctx := context.Background()
	{
		sender, err := session.NewSender(amqp.LinkTargetAddress(channel))
		if err != nil {
			return err
		}
		defer func() {
			err := sender.Close(ctx)
			if err != nil {
				log.Error(err)
			}
		}()

		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		// Send message
		err = sender.Send(ctx, amqp.NewMessage(data))
		if err != nil {
			log.Error(err)
			return err
		}

	}
	return nil

}
