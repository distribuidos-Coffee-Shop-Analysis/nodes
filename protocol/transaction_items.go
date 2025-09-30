package protocol

import "fmt"

// TransactionItemRecord represents transaction item record: transaction_id, item_id, quantity, unit_price, subtotal, created_at
type TransactionItemRecord struct {
	TransactionID string
	ItemID        string
	Quantity      string
	UnitPrice     string
	Subtotal      string
	CreatedAt     string
}

const TransactionItemRecordParts = 6

// Serialize returns the string representation of the transaction item record
func (t *TransactionItemRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s",
		t.TransactionID, t.ItemID, t.Quantity, t.UnitPrice, t.Subtotal, t.CreatedAt)
}

// GetType returns the dataset type for transaction item records
func (t *TransactionItemRecord) GetType() DatasetType {
	return DatasetTypeTransactionItems
}

// NewTransactionItemRecordFromParts creates a TransactionItemRecord from string parts
func NewTransactionItemRecordFromParts(parts []string) (*TransactionItemRecord, error) {
	if len(parts) < TransactionItemRecordParts {
		return nil, fmt.Errorf("invalid TransactionItemRecord format: expected %d fields, got %d",
			TransactionItemRecordParts, len(parts))
	}

	return &TransactionItemRecord{
		TransactionID: parts[0],
		ItemID:        parts[1],
		Quantity:      parts[2],
		UnitPrice:     parts[3],
		Subtotal:      parts[4],
		CreatedAt:     parts[5],
	}, nil
}