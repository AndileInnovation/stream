package main

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/andile/go/streamer/subscriber/redis"
	"context"
	"time"
	"os"
	"strconv"
)

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {
	log.Info("Starting example application")

	rs := redis.Subscriber{}

	log.Info("Connecting subscriber..")
	if err := rs.Connect("10.3.0.139:16382", "redis-cluster"); err != nil {
		panic(err)
	}

	messages := make(chan string)
	rs.Subscribe("ch1", messages)
	defer rs.Unsubscribe()

	ctx, cancel := context.WithCancel(context.Background())
	tot := float64(0)
	count := 0
	go func(ctx context.Context) {
		log.Info("Starting to listen for new messages")
		for {
			select {
			case <- ctx.Done():
				log.Info("Stop listening for messages")
				return
			case m := <-messages:
				o, err := strconv.ParseInt(m, 10, 64)
				if err != nil {
					log.Warn("Cant parse ", m)
				}
				lat := float64(time.Now().UnixNano() - o)/1000000.0
				if lat >= 4 {
					log.Warning(lat)
				}
				tot = tot + lat
				count = count +1
				avg := tot/float64(count)
				log.Info(m, " Latency = ", lat, " ms", " avg: ", avg, " count: ", count)
			}
		}
	}(ctx)

	time.Sleep(time.Minute)
	cancel()
	time.Sleep(time.Second*1)
	log.Info("Application shutting down..")
}
