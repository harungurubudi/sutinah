package sutinah

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/linkedin/goavro"
)

func TestProduceConsume(t *testing.T) {
	expected := map[string]interface{}{
		"id":       "1234578",
		"username": "harun",
		"fullname": "Harun Al Rasyid",
		"password": "encrypted password",
		"email":    "my@email.mail",
		"phone":    "+6285298765432",
	}

	ctx := context.Background()

	consume(ctx, expected, t)
	err := produce(ctx, expected, t)
	if err != nil {
		t.Error(err)
	}
}

func produce(ctx context.Context, data map[string]interface{}, t *testing.T) error {
	config := getConfig()
	codec, err := goavro.NewCodec(getRecordCodecSchema())
	if err != nil {
		return err
	}

	producer := NewProducer(
		config["kafka_address"].(string),
		"user",
		codec,
	)

	time.Sleep(5 * time.Second)
	err = producer.Produce(ctx, data["id"].(string), data)
	if err != nil {
		return err
	}
	producer.Close()
	return nil
}

func consume(ctx context.Context, expected map[string]interface{}, t *testing.T) {
	m := make(chan *ConsumerChannel)
	config := getConfig()
	codec, err := goavro.NewCodec(getRecordCodecSchema())
	if err != nil {
		t.Error(err)
	}

	consumer := NewConsumer(
		config["kafka_address"].(string),
		"user",
		codec,
	)

	go consumer.Consume(ctx, m)
	result := <-m
	if result.Err != nil {
		t.Error(result.Err)
	}

	var passed bool = true
	for k, v := range result.Message.(map[string]interface{}) {
		if v != expected[k] {
			passed = false
		}
	}
	if !passed {
		t.Error("Expected consumed object is invalid")
	}

}

func getRecordCodecSchema() string {
	return `
	{
		"name": "User",
		"type": "record",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "username", "type": "string"},
			{"name": "fullname", "type": "string"},
			{"name": "password", "type": "string"},
			{"name": "email", "type": "string"},
			{"name": "phone", "type": "string"}
		]
	}`
}

func getArrayCodecSchema() string {
	return fmt.Sprintf(`
	{
		"name": "Users",
		"type": {
			"type": "array",
			"items": %s
		}
	}`, getRecordCodecSchema())
}

func getConfig() map[string]interface{} {
	return map[string]interface{}{
		"kafka_address": "localhost:29092",
	}
}
