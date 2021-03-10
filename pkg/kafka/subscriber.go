package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/qosimmax/pubsub/events"
	"github.com/segmentio/kafka-go"
)

var (
	TopicAlreadyExists = errors.New("topic already exists")
	TopicNotFound      = errors.New("topic not found")
)

type kafkaSub struct {
	consumers map[string]*kafka.Reader
	config    *Config
}

func NewKafkaSubscribe(config *Config) events.Subscriber {
	return &kafkaSub{
		consumers: make(map[string]*kafka.Reader),
		config:    config,
	}
}

func (k *kafkaSub) AddSubscribe(topic string) error {
	if _, ok := k.consumers[topic]; ok {
		return TopicAlreadyExists
	}

	k.consumers[topic] = kafka.NewReader(kafka.ReaderConfig{
		//@TODO get from conf
		Brokers:  []string{k.config.Address},
		Topic:    topic,
		GroupID:  k.config.GroupId,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  5 * time.Millisecond,
	})
	return nil
}

func (k *kafkaSub) Subscribe(c context.Context, topic string) ([]byte, error) {
	if _, ok := k.consumers[topic]; !ok {
		return nil, TopicNotFound
	}

	m, err := k.consumers[topic].ReadMessage(c)
	if err != nil {
		return nil, err
	}

	return m.Value, nil

}
