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
	gracefulStop := make(chan os.Signal, 2)
	signal.Notify(gracefulStop, os.Interrupt)
	signal.Notify(gracefulStop, os.Interrupt, syscall.SIGINT)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func(ctx context.Context) {

		defer wg.Done()
		log.Info("Starting to listen for new messages")
		for {
			select {
			case <-ctx.Done():
				log.Info("Stop listening for messages")
				return
			case msg := <-messageReceivedC:
				t2 := strconv.Itoa(time.Now().Minute()) + "." + strconv.Itoa(time.Now().Second())
				log.Info("messageReceived -- ", msg, " vs ", t2)
			case sig :=  <-gracefulStop:
				log.Info("caught sig: ", sig)
				log.Info("Wait for 5 second to finish processing")
				time.Sleep(5*time.Second)
				amqSub.Close()
				wg.Done()
				os.Exit(1)

			}
		}
	}(ctx)


	wg.Wait()
}