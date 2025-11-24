package aggregates

import (
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// BatchToPublish represents a batch message with optional custom routing key
type BatchToPublish struct {
	Batch      *protocol.BatchMessage
	RoutingKey string // If empty, use default routing from config
}

// Aggregate defines the interface for different types of aggregate operations
// Each aggregate instance is tied to a specific client and maintains isolated state
type Aggregate interface {
	// AccumulateBatch processes and accumulates a batch of records for a specific client
	AccumulateBatch(records []protocol.Record, batchIndex int) error

	// Finalize processes all accumulated data and returns the final aggregated results
	// This is called when EOF is received and all batches have been processed for this client
	Finalize(clientId string) ([]protocol.Record, error)

	// Name returns the name of the aggregate for logging
	Name() string

	// GetBatchesToPublish performs the full finalization process:
	// 1. Merges all historical incremental snapshots (append-only persistence)
	// 2. Combines with current in-memory state
	// 3. Computes final aggregated results
	// 4. Returns batches ready to publish with optional custom routing keys
	//
	// historicalIncrements: Previously persisted incremental snapshots (from StateStore)
	// batchIndex: The batch index to use for the final published batch
	// clientID: Propagated from incoming messages to route responses back to the correct client
	//
	// Returns a slice of batches to publish (can be 1 for simple aggregates, N for partitioned)
	GetBatchesToPublish(historicalIncrements [][]byte, batchIndex int, clientID string) ([]BatchToPublish, error)

	// Cleanup releases all resources held by this aggregate instance
	// This includes clearing maps, slices, and closing any open file descriptors
	// Called after finalization to prevent memory leaks
	Cleanup() error

	// PERSISTANCY

	// SerializeState exports the current aggregate buffer as a byte slice
	// This is used to persist the incremental state to disk
	// Returns ONLY the data accumulated since last persist (buffer)
	SerializeState() ([]byte, error)

	// RestoreState merges data from a byte slice into the aggregate state
	// This is used to restore incremental snapshots from disk
	// MUST use += (merge) instead of = (replace) to support append-only persistence
	RestoreState(data []byte) error

	// ClearBuffer clears the in-memory buffer after successful persistence
	// This frees memory while keeping historical data on disk
	// Called after each persist operation to reset the buffer
	ClearBuffer() error
}
