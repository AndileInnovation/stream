package main

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/andile/go/stream/publisher/kafka"
	"context"
	"time"
	"strconv"
	"os"
)

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
}

func main() {
	log.Info("Starting example application")

	kp := kafka.Publisher{}
	log.Info("Connecting publisher..")
	err := kp.Connect([]string{"localhost:9092", "localhost:9093", "localhost:9094"})
	if err != nil {
		panic(err)
	}
	log.Info("Publisher connected")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 1
	count2 := 1
	t1 := time.NewTicker(time.Second * 1)
	t2 := time.NewTicker(time.Second * 3)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				log.Info("Go routine finished..")
				return
			case <-t1.C:
				msg := "1 - Message number " + strconv.Itoa(count)
				count++
				log.Debug("Sending: ", msg)
				kp.Publish("golandTopic", []byte(msg))
			case <-t2.C:
				msg := "2 - Message number " + strconv.Itoa(count2)
				count2++
				log.Debug("Sending: ", msg)
				kp.Publish("golandTopic2", []byte(msg))

			}

		}
	}(ctx)

	time.Sleep(time.Minute * 1)
	log.Info("Application shutting down")
}
