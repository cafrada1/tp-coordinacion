package inner

import (
	"errors"
	"fmt"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type Data struct {
	ClientID string
	Record   fruititem.FruitItem
}

func (d Data) messageKind() string { return dataMessageKind }

func deserializeData(data []any) (Data, error) {
	if len(data) != dataFieldsLen {
		return Data{}, fmt.Errorf("data message does not have %d fields", dataFieldsLen)
	}
	clientID, ok := data[dataClientIdIndex].(string)
	if !ok {
		return Data{}, errors.New("data message client id is not a string")
	}

	fruit, ok := data[dataFruitIndex].(string)
	if !ok {
		return Data{}, errors.New("data message fruit is not a string")
	}

	amount, ok := data[dataAmountIndex].(float64)
	if !ok {
		return Data{}, errors.New("data message amount is not a number")
	}

	return Data{ClientID: clientID, Record: fruititem.FruitItem{Fruit: fruit, Amount: uint32(amount)}}, nil
}
