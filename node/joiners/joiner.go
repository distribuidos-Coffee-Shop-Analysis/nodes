package joiners

import "github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"

// Joiner defines the interface for different types of join operations
// Each joiner instance is tied to a specific client and maintains isolated reference data
type Joiner interface {
	// Name returns the name of this joiner
	Name() string

	// StoreReferenceDataset stores reference data (e.g., menu items, users, stores) for future joins
	// This data is kept separate per client
	StoreReferenceDataset(records []protocol.Record) error

	// PerformJoin joins aggregated data with stored reference data for a specific client
	PerformJoin(aggregatedRecords []protocol.Record, clientId string) ([]protocol.Record, error)

	// GetOutputDatasetType returns the dataset type for the joined output
	GetOutputDatasetType() protocol.DatasetType

	// AcceptsReferenceType checks if this joiner accepts the given reference data type
	AcceptsReferenceType(datasetType protocol.DatasetType) bool

	// AcceptsAggregateType checks if this joiner accepts the given aggregate data type
	AcceptsAggregateType(datasetType protocol.DatasetType) bool

	// Cleanup releases resources held by this joiner instance
	Cleanup() error

	// SerializeState exports the joiner state as a byte slice
	// This is used to persist the state to disk
	SerializeState() ([]byte, error)

	// RestoreState restores the joiner state from a byte slice
	// This is used to restore the state from disk
	RestoreState(data []byte) error
}
