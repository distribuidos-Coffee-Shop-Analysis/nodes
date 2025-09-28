package protocol

// Message types (matching Go client)
const (
	MessageTypeBatch    = 1
	MessageTypeResponse = 2
)

// DatasetType represents the type of dataset
type DatasetType int

const (
	DatasetTypeMenuItems DatasetType = iota + 1
	DatasetTypeStores
	DatasetTypeTransactionItems
	DatasetTypeTransactions
	DatasetTypeUsers

	// Query types
	DatasetTypeQ1 DatasetType = iota + 5
	DatasetTypeQ2
	DatasetTypeQ3
	DatasetTypeQ4
)

// String returns string representation of DatasetType
func (dt DatasetType) String() string {
	switch dt {
	case DatasetTypeMenuItems:
		return "MENU_ITEMS"
	case DatasetTypeStores:
		return "STORES"
	case DatasetTypeTransactionItems:
		return "TRANSACTION_ITEMS"
	case DatasetTypeTransactions:
		return "TRANSACTIONS"
	case DatasetTypeUsers:
		return "USERS"
	case DatasetTypeQ1:
		return "Q1"
	case DatasetTypeQ2:
		return "Q2"
	case DatasetTypeQ3:
		return "Q3"
	case DatasetTypeQ4:
		return "Q4"
	default:
		return "UNKNOWN"
	}
}

// Record interface for all record types
type Record interface {
	Serialize() string
	GetType() DatasetType
}
