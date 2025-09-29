package protocol

import (
	"fmt"
	"strings"
)

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

// NewTransactionItemRecordFromString creates a TransactionItemRecord from a string
func NewTransactionItemRecordFromString(data string) (*TransactionItemRecord, error) {
	parts := strings.Split(data, "|")
	return NewTransactionItemRecordFromParts(parts)
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

// TransactionRecord represents transaction record: transaction_id, store_id, payment_method_id, voucher_id, user_id, original_amount, discount_applied, final_amount, created_at
type TransactionRecord struct {
	TransactionID   string
	StoreID         string
	PaymentMethodID string
	VoucherID       string
	UserID          string
	OriginalAmount  string
	DiscountApplied string
	FinalAmount     string
	CreatedAt       string
}

const TransactionRecordParts = 9

// Serialize returns the string representation of the transaction record
func (t *TransactionRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s",
		t.TransactionID, t.StoreID, t.PaymentMethodID, t.VoucherID, t.UserID,
		t.OriginalAmount, t.DiscountApplied, t.FinalAmount, t.CreatedAt)
}

// GetType returns the dataset type for transaction records
func (t *TransactionRecord) GetType() DatasetType {
	return DatasetTypeTransactions
}

// NewTransactionRecordFromString creates a TransactionRecord from a string
func NewTransactionRecordFromString(data string) (*TransactionRecord, error) {
	parts := strings.Split(data, "|")
	return NewTransactionRecordFromParts(parts)
}

// NewTransactionRecordFromParts creates a TransactionRecord from string parts
func NewTransactionRecordFromParts(parts []string) (*TransactionRecord, error) {
	if len(parts) < TransactionRecordParts {
		return nil, fmt.Errorf("invalid TransactionRecord format: expected %d fields, got %d",
			TransactionRecordParts, len(parts))
	}

	return &TransactionRecord{
		TransactionID:   parts[0],
		StoreID:         parts[1],
		PaymentMethodID: parts[2],
		VoucherID:       parts[3],
		UserID:          parts[4],
		OriginalAmount:  parts[5],
		DiscountApplied: parts[6],
		FinalAmount:     parts[7],
		CreatedAt:       parts[8],
	}, nil
}
