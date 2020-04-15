package sutinah

import (
	"context"
	"fmt"
	"testing"

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
	err := make(chan error)

	go produce(ctx, expected, err)
	if <-err != nil {
		t.Error(err)
	}
}

func produce(ctx context.Context, data map[string]interface{}, e chan error) {
	config := getConfig()
	codec, err := goavro.NewCodec(getRecordCodecSchema())
	if err != nil {
		e <- err
		return
	}

	producer := NewProducer(
		config["kafka_address"].(string),
		"user",
		codec,
	)

	err = producer.Produce(ctx, data["id"].(string), data)
	if err != nil {
		e <- err
		return
	}
	producer.Close()
	close(e)
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
