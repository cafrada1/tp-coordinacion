package inner

import (
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const (
	dataMessageType   = "D"
	eofMessageType    = "E"
	topMessageType    = "T"
	reportMessageType = "R"
)

func SerializeDataMessage(clientId string, fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	body, err := serializeJson([]any{dataMessageType, clientId, fruitRecord.Fruit, fruitRecord.Amount})
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}
	return &message, nil
}

func DeserializeDataMessage(message *middleware.Message) (string, fruititem.FruitItem, bool, error) {
	data, err := deserializeJson([]byte(message.Body))
	if err != nil {
		return "", fruititem.FruitItem{}, false, err
	}
	if len(data) != 4 || data[0] != dataMessageType {
		return "", fruititem.FruitItem{}, false, nil
	}

	clientId, ok := data[1].(string)
	if !ok {
		return "", fruititem.FruitItem{}, false, errors.New("Data message client id is not a string")
	}
	fruit, ok := data[2].(string)
	if !ok {
		return "", fruititem.FruitItem{}, false, errors.New("Data message fruit is not a string")
	}
	amount, ok := data[3].(float64)
	if !ok {
		return "", fruititem.FruitItem{}, false, errors.New("Data message amount is not a number")
	}

	return clientId, fruititem.FruitItem{Fruit: fruit, Amount: uint32(amount)}, true, nil
}

func SerializeEOFMessage(clientId string, total int) (*middleware.Message, error) {
	body, err := serializeJson([]any{eofMessageType, clientId, total})
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}
	return &message, nil
}

func DeserializeEOFMessage(message *middleware.Message) (string, int, bool, error) {
	data, err := deserializeJson([]byte(message.Body))
	if err != nil {
		return "", 0, false, err
	}
	if len(data) != 3 || data[0] != eofMessageType {
		return "", 0, false, nil
	}

	clientId, ok := data[1].(string)
	if !ok {
		return "", 0, false, errors.New("EOF message client id is not a string")
	}
	total, ok := data[2].(float64)
	if !ok {
		return "", 0, false, errors.New("EOF message total is not a number")
	}

	return clientId, int(total), true, nil
}

func SerializeTopMessage(clientId string, fruitTop []fruititem.FruitItem) (*middleware.Message, error) {
	var top []any
	for _, fruitRecord := range fruitTop {
		top = append(top, []any{fruitRecord.Fruit, fruitRecord.Amount})
	}
	body, err := serializeJson([]any{topMessageType, clientId, top})
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}
	return &message, nil
}

func DeserializeTopMessage(message *middleware.Message) (string, []fruititem.FruitItem, bool, error) {
	data, err := deserializeJson([]byte(message.Body))
	if err != nil {
		return "", nil, false, err
	}
	if len(data) != 3 || data[0] != topMessageType {
		return "", nil, false, nil
	}

	clientId, ok := data[1].(string)
	if !ok {
		return "", nil, false, errors.New("Top message client id is not a string")
	}
	topData, ok := data[2].([]any)
	if !ok {
		return "", nil, false, errors.New("Top message top is not an array")
	}

	var fruitTop []fruititem.FruitItem
	for _, datum := range topData {
		fruitPair, ok := datum.([]any)
		if !ok || len(fruitPair) != 2 {
			return "", nil, false, errors.New("Top message entry is not a (fruit, amount) pair")
		}
		fruit, ok := fruitPair[0].(string)
		if !ok {
			return "", nil, false, errors.New("Top message fruit is not a string")
		}
		amount, ok := fruitPair[1].(float64)
		if !ok {
			return "", nil, false, errors.New("Top message amount is not a number")
		}
		fruitTop = append(fruitTop, fruititem.FruitItem{Fruit: fruit, Amount: uint32(amount)})
	}

	return clientId, fruitTop, true, nil
}

func SerializeReportMessage(clientId string, replicaId int, count int) (*middleware.Message, error) {
	body, err := serializeJson([]any{reportMessageType, clientId, replicaId, count})
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}
	return &message, nil
}

func DeserializeReportMessage(message *middleware.Message) (string, int, int, bool, error) {
	data, err := deserializeJson([]byte(message.Body))
	if err != nil {
		return "", 0, 0, false, err
	}
	if len(data) != 4 || data[0] != reportMessageType {
		return "", 0, 0, false, nil
	}

	clientId, ok := data[1].(string)
	if !ok {
		return "", 0, 0, false, errors.New("Report message client id is not a string")
	}
	replicaId, ok := data[2].(float64)
	if !ok {
		return "", 0, 0, false, errors.New("Report message replica id is not a number")
	}
	count, ok := data[3].(float64)
	if !ok {
		return "", 0, 0, false, errors.New("Report message count is not a number")
	}

	return clientId, int(replicaId), int(count), true, nil
}
