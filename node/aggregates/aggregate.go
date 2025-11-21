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

	// GetBatchesToPublish returns the batches to publish with optional custom routing keys
	// Each aggregate decides how to partition/organize its results
	// Returns a slice of batches to publish (can be 1 for simple aggregates, N for partitioned)
	// clientID is propagated from incoming messages to route responses back to the correct client
	GetBatchesToPublish(batchIndex int, clientID string) ([]BatchToPublish, error)

	// Cleanup releases all resources held by this aggregate instance
	// This includes clearing maps, slices, and closing any open file descriptors
	// Called after finalization to prevent memory leaks
	Cleanup() error

	// PERSISTANCY 
	
	// SerializeState exports the aggregate state as a byte slice
	// This is used to persist the state to disk
	SerializeState() ([]byte, error)

	// RestoreState restores the aggregate state from a byte slice
	// This is used to restore the state from disk
	RestoreState(data []byte) error
}
