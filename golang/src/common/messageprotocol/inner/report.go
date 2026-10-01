package inner

import (
	"errors"
	"fmt"
)

// report: conteo absoluto de registros procesados por una replica de sum
// para un cliente. viaja solo por el exchange de control.
type Report struct {
	ClientID  string
	ReplicaID int
	Count     int
}

func (r Report) messageKind() string { return reportMessageKind }

func deserializeReport(data []any) (Report, error) {
	if len(data) != reportFieldsLen {
		return Report{}, fmt.Errorf("report message does not have %d fields", reportFieldsLen)
	}
	clientID, ok := data[reportClientIdIndex].(string)
	if !ok {
		return Report{}, errors.New("report message client id is not a string")
	}
	replicaID, ok := data[reportReplicaIdIndex].(float64)
	if !ok {
		return Report{}, errors.New("report message replica id is not a number")
	}
	count, ok := data[reportCountIndex].(float64)
	if !ok {
		return Report{}, errors.New("report message count is not a number")
	}
	return Report{ClientID: clientID, ReplicaID: int(replicaID), Count: int(count)}, nil
}
