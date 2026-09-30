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
	return inner.Serialize(inner.Data{ClientID: messageHandler.clientId, Record: fruitRecord})
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.Serialize(inner.EOF{ClientID: messageHandler.clientId, Total: messageHandler.recordsSent})
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	parsed, err := inner.Deserialize(message)
	if err != nil {
		return nil, err
	}
	top, ok := parsed.(inner.Top)
	if !ok || top.ClientID != messageHandler.clientId {
		return nil, nil
	}
	return top.Fruits, nil
}
