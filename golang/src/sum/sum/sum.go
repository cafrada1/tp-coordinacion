package sum

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue      middleware.Middleware
	outputExchanges []middleware.Middleware

	mu      sync.Mutex
	clients map[string]map[string]fruititem.FruitItem
	flushed map[string]bool
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}
	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:      inputQueue,
		outputExchanges: []middleware.Middleware{outputExchange},
		clients:         map[string]map[string]fruititem.FruitItem{},
		flushed:         map[string]bool{},
	}, nil
}

func (sum *Sum) Run() {
	if err := sum.inputQueue.StartConsuming(sum.handleInputMessage); err != nil {
		slog.Error("While consuming input queue", "err", err)
	}
}

func (sum *Sum) Stop() {
	sum.inputQueue.StopConsuming()
	sum.inputQueue.Close()
	for _, outputExchange := range sum.outputExchanges {
		outputExchange.Close()
	}
}

func (sum *Sum) handleInputMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	if clientId, fruitRecord, isData, err := inner.DeserializeDataMessage(&msg); err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	} else if isData {
		sum.handleDataMessage(clientId, fruitRecord)
		return
	}

	if clientId, total, isEOF, err := inner.DeserializeEOFMessage(&msg); err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	} else if isEOF {
		if err := sum.flushClient(clientId, total); err != nil {
			slog.Error("While flushing client data", "err", err)
		}
		return
	}

	slog.Error("Received message of unknown type")
}

func (sum *Sum) flushClient(clientId string, total int) error {
	sum.mu.Lock()
	if sum.flushed[clientId] {
		sum.mu.Unlock()
		return nil
	}
	sum.flushed[clientId] = true
	fruitItemMap := sum.clients[clientId]
	delete(sum.clients, clientId)
	sum.mu.Unlock()

	outputExchange := sum.outputExchanges[0]
	for _, fruitItem := range fruitItemMap {
		message, err := inner.SerializeDataMessage(clientId, fruitItem)
		if err != nil {
			return err
		}
		if err := outputExchange.Send(*message); err != nil {
			return err
		}
	}

	eofMessage, err := inner.SerializeEOFMessage(clientId, 0)
	if err != nil {
		return err
	}
	if err := outputExchange.Send(*eofMessage); err != nil {
		return err
	}

	slog.Info("Flushed client data", "client", clientId, "fruits", len(fruitItemMap))
	return nil
}

func (sum *Sum) handleDataMessage(clientId string, fruitRecord fruititem.FruitItem) {
	sum.mu.Lock()
	defer sum.mu.Unlock()

	fruitItemMap, ok := sum.clients[clientId]
	if !ok {
		fruitItemMap = map[string]fruititem.FruitItem{}
		sum.clients[clientId] = fruitItemMap
	}

	if current, ok := fruitItemMap[fruitRecord.Fruit]; ok {
		fruitItemMap[fruitRecord.Fruit] = current.Sum(fruitRecord)
	} else {
		fruitItemMap[fruitRecord.Fruit] = fruitRecord
	}
}
