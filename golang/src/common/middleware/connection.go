package middleware

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Connection struct {
	conn             *amqp.Connection
	consumerChannel  *amqp.Channel
	publisherChannel *amqp.Channel
}

func (c *Connection) Close() error {
	channelPublisherErr := c.publisherChannel.Close()
	channelConsumerErr := c.consumerChannel.Close()
	connErr := c.conn.Close()
	if channelPublisherErr != nil {
		return fmt.Errorf("%w: %v", ErrMessageMiddlewareClose, channelPublisherErr)
	}
	if channelConsumerErr != nil {
		return fmt.Errorf("%w: %v", ErrMessageMiddlewareClose, channelConsumerErr)
	}
	if connErr != nil {
		return fmt.Errorf("%w: %v", ErrMessageMiddlewareClose, connErr)
	}
	return nil
}

func dial(settings ConnSettings) (*Connection, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", settings.Hostname, settings.Port)

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	consumerChannel, err := getChannel(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}

	publisherChannel, err := getChannel(conn)
	if err != nil {
		conn.Close()
		consumerChannel.Close()
		return nil, err
	}

	connection := &Connection{
		conn:             conn,
		consumerChannel:  consumerChannel,
		publisherChannel: publisherChannel,
	}

	return connection, nil
}

func getChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = channel.Qos(1, 0, false)
	if err != nil {
		channel.Close()
		return nil, err
	}
	return channel, nil
}
