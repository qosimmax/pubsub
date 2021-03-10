package kafka

import (
	"context"

	"github.com/qosimmax/pubsub/events"
	"github.com/segmentio/kafka-go"
)

type kafkaPub struct {
	publishers map[string]*kafka.Writer
	config     *Config
}

func NewKafkaPublisher(config *Config) events.Publisher {
	return &kafkaPub{
		publishers: make(map[string]*kafka.Writer),
		config:     config,
	}
}

func (k kafkaPub) Publish(c context.Context, topic string, payload []byte) error {
	return k.publishers[topic].WriteMessages(c, kafka.Message{
		Key:   nil,
		Value: payload,
	})
}

func (k kafkaPub) AddPublisher(topic string) error {
	//@TODO get from config
	k.publishers[topic] = &kafka.Writer{
		Addr:     kafka.TCP(k.config.Address),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	return nil
}
