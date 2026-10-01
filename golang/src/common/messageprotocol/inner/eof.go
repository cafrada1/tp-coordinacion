package inner

import (
	"errors"
	"fmt"
)

// eof: fin de la ingesta de un cliente. total es la cantidad de registros
// que el gateway envio para ese cliente.
type EOF struct {
	ClientID string
	Total    int
}

func (e EOF) messageKind() string { return eofMessageKind }

func deserializeEOF(data []any) (EOF, error) {
	if len(data) != eofFieldsLen {
		return EOF{}, fmt.Errorf("EOF message does not have %d fields", eofFieldsLen)
	}
	clientID, ok := data[eofClientIdIndex].(string)
	if !ok {
		return EOF{}, errors.New("EOF message client id is not a string")
	}
	total, ok := data[eofTotalIndex].(float64)
	if !ok {
		return EOF{}, errors.New("EOF message total is not a number")
	}
	return EOF{ClientID: clientID, Total: int(total)}, nil
}
