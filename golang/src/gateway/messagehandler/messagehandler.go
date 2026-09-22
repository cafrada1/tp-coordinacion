package messagehandler

import (
	"fmt"
	"sync/atomic"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageHandler struct {
	clientId    string
	recordsSent int
}

var clientIdCounter atomic.Int64

func NewMessageHandler() MessageHandler {
	id := clientIdCounter.Add(1)
	return MessageHandler{clientId: fmt.Sprintf("client-%d", id)}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	messageHandler.recordsSent++
	return inner.SerializeDataMessage(messageHandler.clientId, fruitRecord)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeEOFMessage(messageHandler.clientId, messageHandler.recordsSent)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	clientId, fruitTop, isTop, err := inner.DeserializeTopMessage(message)
	if err != nil {
		return nil, err
	}
	if !isTop || clientId != messageHandler.clientId {
		return nil, nil
	}
	return fruitTop, nil
}
