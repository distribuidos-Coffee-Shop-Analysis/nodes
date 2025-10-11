package filters

import (
	"fmt"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Filter defines the interface for different types of filters
type Filter interface {
	// Filter returns true if the record should be kept, false if it should be filtered out
	Filter(record protocol.Record) bool
	// Name returns the filter name for logging purposes
	Name() string
}

// extractCreatedAt extracts the created_at field from a record
func extractCreatedAt(record protocol.Record) (string, error) {
	switch r := record.(type) {
	case *protocol.TransactionRecord:
		return r.CreatedAt, nil
	case *protocol.TransactionItemRecord:
		return r.CreatedAt, nil
	default:
		return "", fmt.Errorf("unsupported record type for created_at extraction")
	}
}

// extractTransactionID extracts the transaction_id field from a record for logging
func ExtractTransactionID(record protocol.Record) string {
	switch r := record.(type) {
	case *protocol.TransactionRecord:
		return r.TransactionID
	case *protocol.TransactionItemRecord:
		return r.TransactionID
	default:
		return "unknown"
	}
}
