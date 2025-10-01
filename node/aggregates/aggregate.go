package aggregates

import (
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// RecordAggregate defines the interface for different types of aggregate operations
type RecordAggregate interface {
	// AccumulateBatch processes and accumulates a batch of records
	AccumulateBatch(records []protocol.Record, batchIndex int) error

	// Finalize processes all accumulated data and returns the final aggregated results
	// This is called when EOF is received and all batches have been processed
	Finalize() ([]protocol.Record, error)

	// IsComplete checks if all expected batches have been received
	IsComplete(maxBatchIndex int) bool

	// Name returns the name of the aggregate for logging
	Name() string
}
