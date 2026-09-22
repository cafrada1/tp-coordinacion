package middleware

import (
	"fmt"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

type exchangeMiddleware struct {
	conn     *Connection
	exchange string
	keys     []string

	consuming atomic.Bool
	stopped   atomic.Bool
	isClosed  atomic.Bool
}

func newExchangeMiddleware(exchange string, keys []string, settings ConnSettings) (*exchangeMiddleware, error) {
	conn, err := dial(settings)
	if err != nil {
		return nil, err
	}

	kind := "direct"
	if len(keys) == 0 {
		kind = "fanout"
		keys = []string{""}
	}

	err = conn.consumerChannel.ExchangeDeclare(
		exchange,
		kind,
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

	return &exchangeMiddleware{conn: conn, exchange: exchange, keys: keys}, nil
}

func (e *exchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if e.isClosed.Load() {
		return ErrMessageMiddlewareDisconnected
	}

	if e.consuming.Load() {
		return nil
	}

	e.stopped.Store(false)
	e.consuming.Store(true)
	defer e.consuming.Store(false)

	queue, err := e.conn.consumerChannel.QueueDeclare(
		"",
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for _, key := range e.keys {
		err := e.conn.consumerChannel.QueueBind(queue.Name, key, e.exchange, false, nil)
		if err != nil {
			return err
		}
	}

	messages, err := e.conn.consumerChannel.Consume(
		queue.Name,
		consumerTag,
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	closeNotify := e.conn.consumerChannel.NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case message, ok := <-messages:
			if !ok {
				if e.stopped.Load() {
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

func (e *exchangeMiddleware) StopConsuming() error {
	if !e.consuming.Load() {
		return nil
	}

	e.stopped.Store(true)
	if err := e.conn.consumerChannel.Cancel(consumerTag, false); err != nil {
		return fmt.Errorf("%w: %v", ErrMessageMiddlewareDisconnected, err)
	}
	return nil
}

func (e *exchangeMiddleware) Send(msg Message) error {
	for _, key := range e.keys {
		err := e.conn.publisherChannel.Publish(e.exchange, key, false, false, amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
		if err != nil {
			return fmt.Errorf("%w: %v", ErrMessageMiddlewareMessage, err)
		}
	}
	return nil
}

func (e *exchangeMiddleware) Close() error {
	if e.isClosed.Swap(true) {
		return nil
	}
	return e.conn.Close()
}
