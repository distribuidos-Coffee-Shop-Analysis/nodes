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
	Name() string
	Finalize(clientId string) ([]protocol.Record, error)
	GetBatchesToPublish(historicalIncrements [][]byte, batchIndex int, clientID string) ([]BatchToPublish, error)
	Cleanup() error

	// PERSISTENCY
	SerializeRecords(records []protocol.Record, batchIndex int) ([]byte, error)

	// CACHE
	CacheIncrement(batchIndex int, data []byte)
	GetCachedBatchIndices() map[int]bool
	ClearCache()
}
