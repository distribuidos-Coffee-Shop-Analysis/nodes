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

	// GetAccumulatedBatchCount returns the count of batches that have been accumulated so far
	GetAccumulatedBatchCount() int

	// Name returns the name of the aggregate for logging
	Name() string
}
