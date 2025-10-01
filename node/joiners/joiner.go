package joiners

import "github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"

// RecordJoiner defines the interface for different types of join operations
type RecordJoiner interface {
	// Name returns the name of this joiner
	Name() string

	// StoreReferenceData stores reference data (e.g., menu items, users, stores) for future joins
	StoreReferenceData(records []protocol.Record) error

	// PerformJoin joins aggregated data with stored reference data
	PerformJoin(aggregatedRecords []protocol.Record) ([]protocol.Record, error)

	// GetOutputDatasetType returns the dataset type for the joined output
	GetOutputDatasetType() protocol.DatasetType

	// AcceptsReferenceType checks if this joiner accepts the given reference data type
	AcceptsReferenceType(datasetType protocol.DatasetType) bool

	// AcceptsAggregateType checks if this joiner accepts the given aggregate data type
	AcceptsAggregateType(datasetType protocol.DatasetType) bool
}
