package main

import (
	"context"
	"github.com/andile-innovation/stream/subscriber/redis"
	log "github.com/sirupsen/logrus"
	"time"
)

func main() {
	log.Info("Starting example application")

	rs := redis.Subscriber{}
	defer rs.Close()
	log.Info("Connecting subscriber..")
	if err := rs.Connect("localhost:16380", "redis-cluster", 10); err != nil {
		panic(err)
	}

	ch1 := make(chan string)
	ch2 := make(chan string)
	rs.Subscribe("ch1", ch1)
	rs.Subscribe("ch2", ch2)
	//defer rs.Unsubscribe("ch1")
	//defer rs.Unsubscribe("ch2")

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
			case c2 := <-ch2:
				log.Info("ch2 -- ", c2)
			}
		}
	}(ctx)

	time.Sleep(time.Second * 30)
	cancel()
	time.Sleep(time.Second * 1)
	log.Info("Application shutting down..")
}
