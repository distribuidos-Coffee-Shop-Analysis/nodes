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
	// Note: With multiple upstream nodes, EOF may arrive multiple times
	// (e.g., 5 Q4 User Joiners → 1 Q4 Store Joiner = 5 EOF batches)
	// Therefore, cleanup is typically a no-op for joiners with small reference data.
	// Implementation is joiner-specific:
	//   - Q4UserJoiner: No-op (we keep reference data to handle multiple EOF batches)
	//   - Q4StoreJoiner: No-op (stores dataset is tiny - ~10 rows, ~1KB)
	//   - Q2Joiner: No-op (menu items dataset is small - ~100 rows)
	//   - Q3Joiner: No-op (stores dataset is small - ~10 rows)
	// Returns nil for no-op implementations
	Cleanup() error
}

// PersistentJoiner marks joiners that can serialize their state to disk and restore it.
// This interface enables recovery from crashes by persisting reference data.
type PersistentJoiner interface {
	SerializeState() ([]byte, error)
	RestoreState(data []byte) error
}
