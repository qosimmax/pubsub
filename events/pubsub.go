package events

import "context"

type Subscriber interface {
	AddSubscribe(topic string) error
	Subscribe(c context.Context, topic string) ([]byte, error)
}

type Publisher interface {
	AddPublisher(topic string) error
	Publish(c context.Context, topic string, payload []byte) error
}
