package sum

import (
	"fmt"
	"hash/fnv"
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
	id              int
	inputQueue      middleware.Middleware
	controlExchange middleware.Middleware
	outputExchanges []middleware.Middleware

	mu      sync.Mutex
	clients map[string]map[string]fruititem.FruitItem
	counts  map[string]int
	totals  map[string]int
	reports map[string]map[int]int
	flushed map[string]bool
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	controlExchangeName := fmt.Sprintf("%s_control", config.SumPrefix)
	controlExchange, err := middleware.CreateExchangeMiddleware(controlExchangeName, nil, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	outputExchanges := make([]middleware.Middleware, config.AggregationAmount)
	for i := range config.AggregationAmount {
		routeKey := fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
		outputExchanges[i], err = middleware.CreateExchangeMiddleware(config.AggregationPrefix, []string{routeKey}, connSettings)
		if err != nil {
			inputQueue.Close()
			controlExchange.Close()
			for _, outputExchange := range outputExchanges[:i] {
				outputExchange.Close()
			}
			return nil, err
		}
	}

	return &Sum{
		id:              config.Id,
		inputQueue:      inputQueue,
		controlExchange: controlExchange,
		outputExchanges: outputExchanges,
		clients:         map[string]map[string]fruititem.FruitItem{},
		counts:          map[string]int{},
		totals:          map[string]int{},
		reports:         map[string]map[int]int{},
		flushed:         map[string]bool{},
	}, nil
}

func (sum *Sum) Run() {
	go func() {
		if err := sum.controlExchange.StartConsuming(sum.handleControlMessage); err != nil {
			slog.Error("While consuming control exchange", "err", err)
		}
	}()

	if err := sum.inputQueue.StartConsuming(sum.handleInputMessage); err != nil {
		slog.Error("While consuming input queue", "err", err)
	}
}

func (sum *Sum) Stop() {
	sum.inputQueue.StopConsuming()
	sum.controlExchange.StopConsuming()
	sum.inputQueue.Close()
	sum.controlExchange.Close()
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
		if err := sum.forwardEOF(clientId, total); err != nil {
			slog.Error("While forwarding EOF message", "err", err)
		}
		return
	}

	slog.Error("Received message of unknown type")
}

func (sum *Sum) handleControlMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	if clientId, total, isEOF, err := inner.DeserializeEOFMessage(&msg); err != nil {
		slog.Error("While deserializing control message", "err", err)
		return
	} else if isEOF {
		sum.openRound(clientId, total)
		return
	}

	if clientId, replicaId, count, isReport, err := inner.DeserializeReportMessage(&msg); err != nil {
		slog.Error("While deserializing control message", "err", err)
		return
	} else if isReport {
		sum.applyReport(clientId, replicaId, count)
		return
	}

	slog.Error("Received control message of unknown type")
}

func (sum *Sum) openRound(clientId string, total int) {
	sum.mu.Lock()
	sum.totals[clientId] = total
	count := sum.counts[clientId]
	sum.mu.Unlock()

	sum.reportCount(clientId, count)
}

func (sum *Sum) applyReport(clientId string, replicaId int, count int) {
	sum.mu.Lock()
	if _, ok := sum.totals[clientId]; !ok {
		sum.mu.Unlock()
		return
	}
	replicaCounts, ok := sum.reports[clientId]
	if !ok {
		replicaCounts = map[int]int{}
		sum.reports[clientId] = replicaCounts
	}
	if count > replicaCounts[replicaId] {
		replicaCounts[replicaId] = count
	}
	complete := sum.roundComplete(clientId)
	sum.mu.Unlock()

	if complete {
		if err := sum.flushClient(clientId); err != nil {
			slog.Error("While flushing client data", "err", err)
		}
	}
}

func (sum *Sum) roundComplete(clientId string) bool {
	processed := 0
	for _, count := range sum.reports[clientId] {
		processed += count
	}
	return processed >= sum.totals[clientId]
}

func (sum *Sum) forwardEOF(clientId string, total int) error {
	message, err := inner.SerializeEOFMessage(clientId, total)
	if err != nil {
		return err
	}
	return sum.controlExchange.Send(*message)
}

func (sum *Sum) reportCount(clientId string, count int) {
	message, err := inner.SerializeReportMessage(clientId, sum.id, count)
	if err != nil {
		slog.Error("While serializing report message", "err", err)
		return
	}
	if err := sum.controlExchange.Send(*message); err != nil {
		slog.Error("While sending report message", "err", err)
	}
}

func (sum *Sum) flushClient(clientId string) error {
	sum.mu.Lock()
	if sum.flushed[clientId] {
		sum.mu.Unlock()
		return nil
	}
	sum.flushed[clientId] = true
	fruitItemMap := sum.clients[clientId]
	total := sum.totals[clientId]
	delete(sum.clients, clientId)
	delete(sum.counts, clientId)
	delete(sum.totals, clientId)
	delete(sum.reports, clientId)
	sum.mu.Unlock()

	for fruit, fruitItem := range fruitItemMap {
		message, err := inner.SerializeDataMessage(clientId, fruitItem)
		if err != nil {
			return err
		}
		partition := hashFruit(fruit) % uint32(len(sum.outputExchanges))
		if err := sum.outputExchanges[partition].Send(*message); err != nil {
			return err
		}
	}

	eofMessage, err := inner.SerializeEOFMessage(clientId, total)
	if err != nil {
		return err
	}
	for _, outputExchange := range sum.outputExchanges {
		if err := outputExchange.Send(*eofMessage); err != nil {
			return err
		}
	}

	slog.Info("Flushed client data", "client", clientId, "fruits", len(fruitItemMap))
	return nil
}

func (sum *Sum) handleDataMessage(clientId string, fruitRecord fruititem.FruitItem) {
	sum.mu.Lock()

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

	sum.counts[clientId]++
	_, roundOpen := sum.totals[clientId]
	count := sum.counts[clientId]
	sum.mu.Unlock()

	if roundOpen {
		sum.reportCount(clientId, count)
	}
}

func hashFruit(fruit string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(fruit))
	return h.Sum32()
}
