package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type clientState struct {
	fruitItemMap map[string]fruititem.FruitItem
	eofCount     int
}

type Aggregation struct {
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	sumAmount     int
	topSize       int
	clients       map[string]*clientState
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		sumAmount:     config.SumAmount,
		topSize:       config.TopSize,
		clients:       map[string]*clientState{},
	}, nil
}

func (aggregation *Aggregation) Run() {
	if err := aggregation.inputExchange.StartConsuming(aggregation.handleMessage); err != nil {
		slog.Error("While consuming input exchange", "err", err)
	}
}

func (aggregation *Aggregation) Stop() {
	aggregation.inputExchange.StopConsuming()
	aggregation.inputExchange.Close()
	aggregation.outputQueue.Close()
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	if clientId, fruitRecord, isData, err := inner.DeserializeDataMessage(&msg); err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	} else if isData {
		aggregation.handleDataMessage(clientId, fruitRecord)
		return
	}

	if clientId, _, isEOF, err := inner.DeserializeEOFMessage(&msg); err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	} else if isEOF {
		if err := aggregation.handleEndOfRecordsMessage(clientId); err != nil {
			slog.Error("While handling end of records message", "err", err)
		}
		return
	}

	slog.Error("Received message of unknown type")
}

func (aggregation *Aggregation) handleDataMessage(clientId string, fruitRecord fruititem.FruitItem) {
	state, ok := aggregation.clients[clientId]
	if !ok {
		state = &clientState{fruitItemMap: map[string]fruititem.FruitItem{}}
		aggregation.clients[clientId] = state
	}

	if current, ok := state.fruitItemMap[fruitRecord.Fruit]; ok {
		state.fruitItemMap[fruitRecord.Fruit] = current.Sum(fruitRecord)
	} else {
		state.fruitItemMap[fruitRecord.Fruit] = fruitRecord
	}
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId string) error {
	state, ok := aggregation.clients[clientId]
	if !ok {
		state = &clientState{fruitItemMap: map[string]fruititem.FruitItem{}}
		aggregation.clients[clientId] = state
	}

	state.eofCount++
	if state.eofCount < aggregation.sumAmount {
		return nil
	}

	fruitTop := buildFruitTop(state.fruitItemMap, aggregation.topSize)
	message, err := inner.SerializeTopMessage(clientId, fruitTop)
	if err != nil {
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		return err
	}

	delete(aggregation.clients, clientId)
	slog.Info("Sent partial top", "client", clientId, "fruits", len(state.fruitItemMap))
	return nil
}

func buildFruitTop(fruitItemMap map[string]fruititem.FruitItem, topSize int) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(fruitItemMap))
	for _, item := range fruitItemMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
