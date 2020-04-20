package sutinah

import (
	"context"

	"github.com/linkedin/goavro"
	"github.com/segmentio/kafka-go"
)

// ConsumerChannel - Self defined
type ConsumerChannel struct {
	Message interface{}
	Err     error
}

// Consumer - Self defined
type Consumer struct {
	r     *kafka.Reader
	codec *goavro.Codec
}

// NewConsumer - Create new Consumer
func NewConsumer(address string, topic string, codec *goavro.Codec) *Consumer {
	return &Consumer{
		r: kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{address},
			Topic:     topic,
			Partition: 0,
			MinBytes:  10e3, // 10KB
			MaxBytes:  10e6, // 10MB
		}),
		codec: codec,
	}
}

// Consume - listen message from broker
func (c *Consumer) Consume(
	ctx context.Context,
	m chan *ConsumerChannel,
) {
	for {
		message, err := c.r.ReadMessage(context.Background())
		if err != nil {
			m <- errorChannel(err)
			close(m)
			break
		}

		result, _, err := c.codec.NativeFromBinary(message.Value)
		if err != nil {
			m <- errorChannel(err)
			close(m)
			break
		}
		m <- &ConsumerChannel{
			Message: result,
			Err:     nil,
		}
	}
	c.r.Close()
}

func errorChannel(err error) *ConsumerChannel {
	return &ConsumerChannel{
		Message: nil,
		Err:     err,
	}
}
