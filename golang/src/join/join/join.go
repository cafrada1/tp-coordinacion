package join

import (
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	aggregationAmount int
	topSize           int
	partialTops       map[string][][]fruititem.FruitItem
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		aggregationAmount: config.AggregationAmount,
		topSize:           config.TopSize,
		partialTops:       map[string][][]fruititem.FruitItem{},
	}, nil
}

func (join *Join) Run() {
	if err := join.inputQueue.StartConsuming(join.handleMessage); err != nil {
		slog.Error("While consuming input queue", "err", err)
	}
}

func (join *Join) Stop() {
	join.inputQueue.StopConsuming()
	join.inputQueue.Close()
	join.outputQueue.Close()
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitTop, isTop, err := inner.DeserializeTopMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}
	if !isTop {
		slog.Error("Received message of unknown type")
		return
	}

	tops := append(join.partialTops[clientId], fruitTop)
	if len(tops) < join.aggregationAmount {
		join.partialTops[clientId] = tops
		return
	}

	delete(join.partialTops, clientId)
	finalTop := mergeFruitTops(tops, join.topSize)
	message, err := inner.SerializeTopMessage(clientId, finalTop)
	if err != nil {
		slog.Error("While serializing final top", "err", err)
		return
	}
	if err := join.outputQueue.Send(*message); err != nil {
		slog.Error("While sending final top", "err", err)
	}
}

func mergeFruitTops(partialTops [][]fruititem.FruitItem, topSize int) []fruititem.FruitItem {
	fruitItems := []fruititem.FruitItem{}
	for _, top := range partialTops {
		fruitItems = append(fruitItems, top...)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
