package groupbys

import (
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// RecordGroupBy defines the interface for different types of groupby operations
type RecordGroupBy interface {
	ProcessBatch(records []protocol.Record, eof bool) ([]protocol.Record, error)
	Name() string
	// NewGroupByBatch creates a batch message with the grouped records
	// clientID is propagated from incoming messages to route responses back to the correct client
	NewGroupByBatch(batchIndex int, records []protocol.Record, eof bool, clientID string) *protocol.BatchMessage
}
