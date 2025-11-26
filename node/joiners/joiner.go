package joiners

import "github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"

// Joiner defines the interface for different types of join operations
// Each joiner instance is tied to a specific client and maintains isolated reference data
type Joiner interface {
	Name() string
	PerformJoin(aggregatedRecords []protocol.Record, clientId string, historicalIncrements [][]byte) ([]protocol.Record, error)

	GetOutputDatasetType() protocol.DatasetType
	AcceptsReferenceType(datasetType protocol.DatasetType) bool
	AcceptsAggregateType(datasetType protocol.DatasetType) bool

	Cleanup() error
	ShouldCleanupAfterEOF() bool

	// PERSISTENCY
	SerializeReferenceRecords(records []protocol.Record, batchIndex int) ([]byte, error)
	SerializeBufferedBatch(batch *protocol.BatchMessage) ([]byte, error)
	RestoreBufferedBatches(data []byte) ([]protocol.BatchMessage, error)
}
