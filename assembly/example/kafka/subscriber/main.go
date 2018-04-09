package main

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/andile/go/stream/subscriber/kafka"
	"context"
	"time"
)

func main() {
	log.Info("Starting example application (kafka subscriber)")
	ks := kafka.Subscriber{}

	ks.Connect("10.3.0.94:9092", "myGroup", "beginning")

	ch1 := make(chan string)
	ks.Subscribe("golandTopic", ch1)
	//defer ks.Unsubscribe("golandTopic")

	ctx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context) {
		log.Info("Starting to listen for new messages")
		for {
			select {
			case <-ctx.Done():
				log.Info("Stop listening for messages")
				return
			case c1 := <-ch1:
				log.Info("ch1 -- ", c1)
			}
		}
	}(ctx)

	time.Sleep(time.Second * 30)
	cancel()
	time.Sleep(time.Second * 1)
	log.Info("Application shutting down..")
}
