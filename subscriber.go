package stream

type Subscriber interface {
	Subscribe(channel string, response chan<- string)
}
