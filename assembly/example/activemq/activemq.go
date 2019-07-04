package main

import "github.com/andile-innovation/stream/activemq"

func main() {
	amqPub := activemq.NewAMQPPublisher(activemq.NewAMQPPublisherRequest{
		Host:     "localhost",
		Port:     5672,
		Username: "admin",
		Password: "admin",
	})

	if err := amqPub.Connect(); err != nil {
		panic(err)
	}

	if err := amqPub.Publish("MYQ1", []byte("123")); err != nil {
		panic(err)
	}

}
