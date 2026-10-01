package inner

import (
	"errors"
	"fmt"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

const (
	fruitPairFieldsLen = 2
	fruitNameIndex     = 0
	fruitAmountIndex   = 1
)

// top: top de frutas (parcial o final) de un cliente.
type Top struct {
	ClientID string
	Fruits   []fruititem.FruitItem
}

func (t Top) messageKind() string { return topMessageKind }

func deserializeTop(data []any) (Top, error) {
	if len(data) != topFieldsLen {
		return Top{}, fmt.Errorf("top message does not have %d fields", topFieldsLen)
	}
	clientID, ok := data[topClientIdIndex].(string)
	if !ok {
		return Top{}, errors.New("top message client id is not a string")
	}
	topData, ok := data[topFruitsIndex].([]any)
	if !ok {
		return Top{}, errors.New("top message fruits are not an array")
	}

	var fruits []fruititem.FruitItem
	for _, datum := range topData {
		fruitPair, ok := datum.([]any)
		if !ok || len(fruitPair) != fruitPairFieldsLen {
			return Top{}, errors.New("top message entry is not a (fruit, amount) pair")
		}
		fruit, ok := fruitPair[fruitNameIndex].(string)
		if !ok {
			return Top{}, errors.New("top message fruit is not a string")
		}
		amount, ok := fruitPair[fruitAmountIndex].(float64)
		if !ok {
			return Top{}, errors.New("top message amount is not a number")
		}
		fruits = append(fruits, fruititem.FruitItem{Fruit: fruit, Amount: uint32(amount)})
	}

	return Top{ClientID: clientID, Fruits: fruits}, nil
}
