package inner

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

// protocolo interno: array json posicional [tipo, ...] con la
// etiqueta del mensaje primero. los indices de cada tipo estan en constantes.
const (
	dataMessageKind   = "D"
	eofMessageKind    = "E"
	topMessageKind    = "T"
	reportMessageKind = "R"

	eofFieldsLen     = 3
	eofClientIdIndex = 1
	eofTotalIndex    = 2

	dataFieldsLen     = 4
	dataClientIdIndex = 1
	dataFruitIndex    = 2
	dataAmountIndex   = 3

	topFieldsLen     = 3
	topClientIdIndex = 1
	topFruitsIndex   = 2

	reportFieldsLen      = 4
	reportClientIdIndex  = 1
	reportReplicaIdIndex = 2
	reportCountIndex     = 3
)

// message un tipo por cada mensaje del pipeline.
// el metodo no exportado sella la interfaz, solo los tipos de este
// paquete la pueden implementar.
type Message interface {
	messageKind() string
}

// serialize convierte el mensaje en el json posicional [tipo, ...].
func Serialize(message Message) (*middleware.Message, error) {
	switch m := message.(type) {
	case Data:
		body, err := serializeJson([]any{dataMessageKind, m.ClientID, m.Record.Fruit, m.Record.Amount})
		if err != nil {
			return nil, err
		}
		return &middleware.Message{Body: string(body)}, nil
	case EOF:
		body, err := serializeJson([]any{eofMessageKind, m.ClientID, m.Total})
		if err != nil {
			return nil, err
		}
		return &middleware.Message{Body: string(body)}, nil
	case Top:
		var fruits []any
		for _, fruitRecord := range m.Fruits {
			fruits = append(fruits, []any{fruitRecord.Fruit, fruitRecord.Amount})
		}
		body, err := serializeJson([]any{topMessageKind, m.ClientID, fruits})
		if err != nil {
			return nil, err
		}
		return &middleware.Message{Body: string(body)}, nil
	case Report:
		body, err := serializeJson([]any{reportMessageKind, m.ClientID, m.ReplicaID, m.Count})
		if err != nil {
			return nil, err
		}
		return &middleware.Message{Body: string(body)}, nil
	default:
		return nil, fmt.Errorf("cannot serialize message of kind %q", message.messageKind())
	}
}

// deserialize clasifica por su tipo y devuelve el struct del
// tipo concreto
func Deserialize(message *middleware.Message) (Message, error) {
	data, err := deserializeJson([]byte(message.Body))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, errors.New("message is empty")
	}

	kind, ok := data[0].(string)
	if !ok {
		return nil, errors.New("message kind is not a string")
	}

	switch kind {
	case dataMessageKind:
		return deserializeData(data)
	case eofMessageKind:
		return deserializeEOF(data)
	case topMessageKind:
		return deserializeTop(data)
	case reportMessageKind:
		return deserializeReport(data)
	default:
		return nil, fmt.Errorf("unknown message kind %q", kind)
	}
}

func serializeJson(message []any) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]any, error) {
	var data []any
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}
