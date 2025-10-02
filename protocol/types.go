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

	DatasetTypeQ1 // creo q igual a transactions

	DatasetTypeQ2Groups // group Q2
	DatasetTypeQ2Agg // agg Q2
	DatasetTypeQ2AggWithName // join Q2

	DatasetTypeQ3Groups // group Q3
	DatasetTypeQ3Agg // agg Q3
	DatasetTypeQ3AggWithName // join Q3

	DatasetTypeQ4Groups // group Q4
	DatasetTypeQ4Agg // agg Q4
	DatasetTypeQ4AggWithUser // join Q4 user
	DatasetTypeQ4AggWithUserAndStore // join Q4 store
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
	case DatasetTypeQ2Groups:
		return "Q2Groups"
	case DatasetTypeQ2Agg:
		return "Q2Agg"
	case DatasetTypeQ2AggWithName:
		return "Q2AggWithName"
	case DatasetTypeQ3Groups:
		return "Q3Groups"
	case DatasetTypeQ3Agg:
		return "Q3Agg"
	case DatasetTypeQ3AggWithName:
		return "Q3AggWithName"
	case DatasetTypeQ4Groups:
		return "Q4Groups"
	case DatasetTypeQ4Agg:
		return "Q4Agg"
	case DatasetTypeQ4AggWithUser:
		return "Q4AggWithUser"
	case DatasetTypeQ4AggWithUserAndStore:
		return "Q4AggWithUserAndStore"
	default:
		return "UNKNOWN"
	}
}

// Record interface for all record types
type Record interface {
	Serialize() string
	GetType() DatasetType
}
