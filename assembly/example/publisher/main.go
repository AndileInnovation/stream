package main

import (
	"time"
	log "github.com/sirupsen/logrus"
	"gitlab.com/andile/go/streamer/publisher/redis"
	"context"
	"os"
	"strconv"
)

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
}

func main() {
	log.Info("Starting example application")

	rs := redis.Publisher{}

	log.Info("Connecting publisher..")
	if err := rs.Connect("10.3.0.139:16382", "redis-cluster"); err != nil {
		panic(err)
	}
	log.Info("Publisher connected")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func(ctx context.Context) {
		count := 0
		for {
			count = count +1
			select {
			case <- ctx.Done():
				log.Info("Go routine finished..")
				return
			case <-time.After(time.Millisecond * 800):
				log.Info(count)
				rs.Publish("ch1", []byte(strconv.FormatInt(time.Now().UnixNano(), 10)))
			}
		}
	}(ctx)

	time.Sleep(time.Minute * 10)
	log.Info("Application shutting down")
}
