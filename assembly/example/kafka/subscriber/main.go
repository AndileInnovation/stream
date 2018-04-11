package main

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/andile/go/stream/subscriber/kafka"
	"context"
	"time"
	"os"
	"os/signal"
)

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
}

func main() {
	log.Info("Starting example application (kafka subscriber)")
	ks := kafka.Subscriber{}

	ks.Connect([]string{"localhost:9092", "localhost:9093", "localhost:9094"})
	defer ks.Close()

	ch1 := make(chan string)
	ch2 := make(chan string)
	ks.Subscribe("golandTopic", ch1)
	ks.Subscribe("golandTopic2", ch2)

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
				log.Info("ch1 -- ", c1)
			case c2 := <-ch2:
				log.Info("ch2 -- ", c2)
			}
		}
	}(ctx)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

MainLoop:
	for {
		select {
		case <-c:
			log.Info("Application interrupted")
			break MainLoop
		case <-time.After(time.Minute * 2):
			log.Info("Application timeout")
			break MainLoop
		}
	}

	time.Sleep(time.Second * 4)
	log.Info("Application shutting down..")
}
