package groupbys

import (
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// RecordGroupBy defines the interface for different types of groupby operations
type RecordGroupBy interface {
	ProcessBatch(records []protocol.Record, eof bool) ([]protocol.Record, error)
	Name() string
	NewGroupByBatch(batchIndex int, records []protocol.Record, eof bool) *protocol.BatchMessage
}
