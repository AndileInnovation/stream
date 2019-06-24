package activemq

import (
	"context"
	"github.com/andile-innovation/popcorn/log"
	"net"
	"pack.ag/amqp"
	"time"
)

type NewAMQPPublisherRequest struct {
	Host string
	Port int
}

func NewAMQPPublisher(request NewAMQPPublisherRequest) AMQPPublisher {
	return AMQPPublisher{

	}
}

type AMQPPublisher struct {
	Client *amqp.Client
	Conn net.Conn
}

func (p *AMQPPublisher) Connect(address string) error {
	client, err := amqp.Dial(address,
		amqp.ConnSASLPlain("admin", "admin"),
	)
	if err != nil {
		return err
	}

	p.Client = client
	return nil
}

func (p *AMQPPublisher) Publish(destination string, data []byte) error {
	session, err := p.Client.NewSession()
	if err != nil {
		log.Error("Creating AMQP session:", err)
		return err
	}

	// todo pass context as argument
	ctx := context.TODO()
	{
		sender, err := session.NewSender(amqp.LinkTargetAddress(destination),
		)
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

		// Send message
		err = sender.Send(ctx, amqp.NewMessage(data))
		if err != nil {
			log.Error(err)
			return err
		}

		cancel()
	}
	return nil

}
