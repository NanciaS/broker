package event

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Emitter struct {
	connection *amqp.Connection
}

func (e *Emitter) setup() error {
	ch, err := e.connection.Channel()
	if err != nil {
		return err
	}

	defer ch.Close()
	return declareExchange(ch)
}

// Push event into the queue
func (e *Emitter) Push(event string, severity string) error {
	ch, err := e.connection.Channel()
	if err != nil {
		return err
	}

	defer ch.Close()

	log.Println("Pushing to channel")

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = ch.PublishWithContext(ctx, "logs_topic", severity, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(event),
	})
	if err != nil {
		return err
	}
	return nil
}

func NewEventEmitter(conn *amqp.Connection) (Emitter, error) {
	emitter := Emitter{
		connection: conn,
	}
	err := emitter.setup()
	if err != nil {
		return Emitter{}, err
	}
	return emitter, nil
}
