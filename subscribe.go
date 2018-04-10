package stream

type Subscriber interface {
	Subscribe(channel string, response chan<- string)
	Unsubscribe(channel string)
	Close()
}
