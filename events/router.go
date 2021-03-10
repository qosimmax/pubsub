package events

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// HandlerFunc defines the handler used by gin middleware as return value.
type HandlerFunc func(ctx context.Context, payload []byte) ([]byte, error)

type Router struct {
	subscriber      Subscriber
	publisher       Publisher
	handlers        map[string]HandlerFunc
	publisherTopics map[string]string
	wg              sync.WaitGroup
}

func NewRouter(subscriber Subscriber, publisher Publisher) *Router {
	return &Router{
		subscriber:      subscriber,
		publisher:       publisher,
		handlers:        make(map[string]HandlerFunc),
		publisherTopics: make(map[string]string),
	}

}

func (r *Router) AddHandler(topic string, publisherTopic string, handler HandlerFunc) {
	//add subscriber
	err := r.subscriber.AddSubscribe(topic)
	if err != nil {
		panic(err)
	}
	r.handlers[topic] = handler

	// add publisher
	err = r.publisher.AddPublisher(publisherTopic)
	if err != nil {
		panic(err)
	}
	r.publisherTopics[topic] = publisherTopic

}

func (r *Router) AddNoPublisherHandler(topic string, handler HandlerFunc) {
	err := r.subscriber.AddSubscribe(topic)
	if err != nil {
		panic(err)
	}
	r.handlers[topic] = handler
}

func (r *Router) Run() error {
	for topic, _ := range r.handlers {
		r.wg.Add(1)
		go r.subscribe(topic)
	}
	r.wg.Wait()
	return nil
}

func (r *Router) subscribe(topic string) {
	for {
		data, err := r.subscriber.Subscribe(context.Background(), topic)
		if err != nil {
			fmt.Println(err)
			time.Sleep(50 * time.Millisecond)
		}

		resp, err := r.handlers[topic](context.Background(), data)
		if err != nil {
			fmt.Println(err)
		}

		if publisherTopic, ok := r.publisherTopics[topic]; ok {
			err = r.publisher.Publish(context.Background(), publisherTopic, resp)
			if err != nil {
				fmt.Println(err)
			}
		}

	}

}
