package activemq

import (
	"context"
	"github.com/andile-innovation/popcorn/log"
	"pack.ag/amqp"
	"time"
)

type AMQPSubscriber struct {
	Receiver *amqp.Receiver
	Client *amqp.Client
	Session *amqp.Session
}

func (p *AMQPSubscriber) Connect(address string) error {
	client, err := amqp.Dial(address,
		amqp.ConnSASLPlain("admin", "admin"),
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

	receiver, err := amqp.Session.NewReceiver(
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

