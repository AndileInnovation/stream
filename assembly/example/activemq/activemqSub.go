package main

import (
	"context"
	"github.com/andile-innovation/popcorn/log"
	"github.com/andile-innovation/stream/activemq"
	"strconv"
	"time"
)

func main() {

	amqSub := activemq.NewAMQPSubscriber(activemq.NewAMQPSubscriberRequest{
		Host: "localhost",
		Port: 5672,
		Username: "admin",
		Password: "admin",
	})

	if err := amqSub.Connect(); err != nil {
		panic(err)
	}
	ch1 := make(chan string)
	amqSub.Subscribe("MYQ1", ch1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func(ctx context.Context) {
		log.Info("Starting to listen for new messages")
		for {
			select {
			case <-ctx.Done():
				log.Info("Stop listening for messages")
				return
			case c1 := <-ch1:
				//sp := strings.LastIndex(c1, " ")
				//t1 := c1[sp+1:]
				t2 := strconv.Itoa(time.Now().Minute()) + "." + strconv.Itoa(time.Now().Second())
				log.Info("ch1 -- ", c1, " vs ", t2)
			}
		}
	}(ctx)



}