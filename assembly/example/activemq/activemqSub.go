package main

import (
	"context"
	"github.com/andile-innovation/popcorn/log"
	"github.com/andile-innovation/stream/activemq"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
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
	messageReceivedC := make(chan string)
	amqSub.Subscribe("MYQ1", messageReceivedC)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gracefulStop := make(chan os.Signal)
	signal.Notify(gracefulStop, os.Interrupt)
	signal.Notify(gracefulStop, syscall.SIGINT)

	go func() {
		<-gracefulStop
		log.Info("Exit Ctr+C")
		os.Exit(0)
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(ctx context.Context) {

		defer wg.Done()
		log.Info("Starting to listen for new messages")
		for {
			select {
			case <-gracefulStop:
				log.Info("Exit Ctr+C")
				os.Exit(0)
			case <-ctx.Done():
				log.Info("Stop listening for messages")
				return
			case msg := <-messageReceivedC:
				t2 := strconv.Itoa(time.Now().Minute()) + "." + strconv.Itoa(time.Now().Second())
				log.Info("messageReceived -- ", msg, " vs ", t2)

			}
		}
	}(ctx)


	wg.Wait()
}