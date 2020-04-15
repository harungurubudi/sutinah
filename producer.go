package sutinah

import (
	"context"
	"errors"

	"github.com/linkedin/goavro"
	"github.com/segmentio/kafka-go"
)

// Producer - Self defined
type Producer struct {
	w     *kafka.Writer
	codec *goavro.Codec
}

// NewProducer - Create new producer
func NewProducer(address string, topic string, codec *goavro.Codec) *Producer {
	return &Producer{
		w: kafka.NewWriter(kafka.WriterConfig{
			Brokers:  []string{address},
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		}),
		codec: codec,
	}
}

// Produce - produce a new message to
func (p *Producer) Produce(
	ctx context.Context,
	key string,
	val interface{},
) error {
	mapVal, ok := val.(map[string]interface{})
	if !ok {
		return errors.New("Val does not implement map[string]interface{}")
	}

	messageVal, err := p.codec.BinaryFromNative(nil, mapVal)
	if err != nil {
		return err
	}

	return p.w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: messageVal,
	})
}

// Close - Close writer connection
func (p *Producer) Close() {
	p.w.Close()
}
