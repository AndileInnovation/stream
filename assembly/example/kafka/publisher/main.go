package main

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/andile/go/stream/publisher/kafka"
	"context"
	"time"
)

func main() {
	log.Info("Starting example application")

	kp := kafka.Publisher{}

	log.Info("Connecting publisher..")
	err := kp.Connect("10.3.0.94:9092")
	if err != nil {
		panic(err)
	}
	log.Info("Publisher connected")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func(ctx context.Context) {
		count := 0
		for {
			count = count + 1
			select {
			case <-ctx.Done():
				log.Info("Go routine finished..")
				return
			case <-time.After(time.Second * 3):
				log.Info(count)
				kp.Publish("golandTopic", []byte("3 second interval @ "+time.Now().String()))
			}
		}
	}(ctx)


	time.Sleep(time.Minute * 3)
	log.Info("Application shutting down")
}
