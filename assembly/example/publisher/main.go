package main

import (
	"context"
	log "github.com/sirupsen/logrus"
	"gitlab.com/andile/go/stream/publisher/redis"
	"os"
	"time"
)

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
}

func main() {
	log.Info("Starting example application")

	rs := redis.Publisher{}

	log.Info("Connecting publisher..")
	if err := rs.Connect([]string{"localhost:16380","localhost:16381","localhost:16382"}, "redis-cluster"); err != nil {
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
			case <-time.After(time.Second * 7):
				log.Info(count)
				rs.Publish("ch1", []byte("7 second interval @ "+time.Now().String()))
			}
		}
	}(ctx)

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
				rs.Publish("ch2", []byte("3 second interval @ "+time.Now().String()))
			}
		}
	}(ctx)

	time.Sleep(time.Minute * 3)
	log.Info("Application shutting down")
}
