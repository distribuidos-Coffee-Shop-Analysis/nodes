package protocol

import (
	"fmt"
)

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


