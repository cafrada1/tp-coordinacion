package middleware

import (
	"fmt"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

const consumerTag = "tp-mom"

type queueMiddleware struct {
	conn      *Connection
	queueName string

	consuming atomic.Bool

	stopped  atomic.Bool
	isClosed atomic.Bool
}

func newQueueMiddleware(queueName string, settings ConnSettings) (*queueMiddleware, error) {
	conn, err := dial(settings)
	if err != nil {
		return nil, err
	}

	_, err = conn.consumerChannel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &queueMiddleware{conn: conn, queueName: queueName}, nil
}

func (q *queueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if q.isClosed.Load() {
		return ErrMessageMiddlewareDisconnected
	}

	if q.consuming.Load() {
		return nil
	}

	q.stopped.Store(false)
	q.consuming.Store(true)
	defer q.consuming.Store(false)

	messages, err := q.conn.consumerChannel.Consume(
		q.queueName,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	closeNotify := q.conn.consumerChannel.NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case message, ok := <-messages:
			if !ok {
				if q.stopped.Load() {
					return nil
				}
				return ErrMessageMiddlewareDisconnected
			}

			callbackFunc(
				Message{Body: string(message.Body)},
				func() { message.Ack(false) },
				func() { message.Nack(false, true) },
			)

		case amqpErr := <-closeNotify:
			if amqpErr != nil {
				return fmt.Errorf("%w: %v", ErrMessageMiddlewareDisconnected, amqpErr)
			}
			return nil
		}
	}
}

func (q *queueMiddleware) StopConsuming() error {
	if !q.consuming.Load() {
		return nil
	}

	q.stopped.Store(true)
	if err := q.conn.consumerChannel.Cancel(consumerTag, false); err != nil {
		return fmt.Errorf("%w: %v", ErrMessageMiddlewareDisconnected, err)
	}
	return nil
}

func (q *queueMiddleware) Send(msg Message) error {
	err := q.conn.consumerChannel.Publish(
		"",
		q.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Body:         []byte(msg.Body),
		},
	)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrMessageMiddlewareMessage, err)
	}
	return nil
}

func (q *queueMiddleware) Close() error {
	if q.isClosed.Swap(true) {
		return nil
	}
	return q.conn.Close()
}
