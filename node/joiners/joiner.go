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
	// Called after all aggregate batches are processed (EOF threshold reached)
	// Implementation is joiner-specific:
	//   - Q4UserJoiner: Clears users map (large dataset - GBs of memory)
	//   - Q4StoreJoiner: No-op (stores dataset is tiny - ~10 rows)
	//   - Other joiners: No-op (reference data is small and kept)
	// Returns nil for no-op implementations
	Cleanup() error
}
