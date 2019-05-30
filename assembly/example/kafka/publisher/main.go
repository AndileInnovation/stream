package main

import (
	"context"
	"github.com/andile-innovation/stream/publisher/kafka"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"time"
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
	//count2 := 1
	//count3 := 1
	t1 := time.NewTicker(time.Second * 1)
	//t2 := time.NewTicker(time.Second * 3)
	//t3 := time.NewTicker(time.Millisecond * 500)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				log.Info("Go routine finished..")
				return
			case <-t1.C:
				msg := "1 - Message number " + strconv.Itoa(count) + " " + strconv.Itoa(time.Now().Minute()) + "." + strconv.Itoa(time.Now().Second())
				count++
				log.Debug("Sending: ", msg)
				kp.Publish("low.retention", []byte(msg))
				//case <-t2.C:
				//	msg := "2 - Message number " + strconv.Itoa(count2) + " " + strconv.Itoa(time.Now().Minute()) + "." + strconv.Itoa(time.Now().Second())
				//	count2++
				//	log.Debug("Sending: ", msg)
				//	kp.Publish("golandTopic2", []byte(msg))
				//case <-t3.C:
				//	msg := "3 - Message number " + strconv.Itoa(count3) + " " + strconv.Itoa(time.Now().Minute()) + "." + strconv.Itoa(time.Now().Second())
				//	count3++
				//	log.Debug("Sending: ", msg)
				//	kp.Publish("golandTopic3", []byte(msg))
			}

		}
	}(ctx)

	time.Sleep(time.Minute * 10)
	log.Info("Application shutting down")
}
