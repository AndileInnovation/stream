package stream

type Publisher interface {
	Publish(destination string, data []byte) error
}