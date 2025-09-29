package protocol

import (
	"fmt"
	"strings"
)

// Q1Record represents Q1 record: transaction_id, final_amount
type Q1Record struct {
	TransactionID string
	FinalAmount   string
}

const Q1RecordParts = 2

// Serialize returns the string representation of the Q1 record
func (q *Q1Record) Serialize() string {
	return fmt.Sprintf("%s|%s", q.TransactionID, q.FinalAmount)
}

// GetType returns the dataset type for Q1 records
func (q *Q1Record) GetType() DatasetType {
	return DatasetTypeQ1
}

// NewQ1RecordFromString creates a Q1Record from a string
func NewQ1RecordFromString(data string) (*Q1Record, error) {
	parts := strings.Split(data, "|")
	return NewQ1RecordFromParts(parts)
}

// NewQ1RecordFromParts creates a Q1Record from string parts
func NewQ1RecordFromParts(parts []string) (*Q1Record, error) {
	if len(parts) < Q1RecordParts {
		return nil, fmt.Errorf("invalid Q1Record format: expected %d fields, got %d",
			Q1RecordParts, len(parts))
	}

	return &Q1Record{
		TransactionID: parts[0],
		FinalAmount:   parts[1],
	}, nil
}

// Q2Record represents Q2 record: year_month_created_at, item_name, sellings_qty
type Q2Record struct {
	YearMonthCreatedAt string
	ItemName           string
	SellingsQty        string
}

const Q2RecordParts = 3

// Serialize returns the string representation of the Q2 record
func (q *Q2Record) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.YearMonthCreatedAt, q.ItemName, q.SellingsQty)
}

// GetType returns the dataset type for Q2 records
func (q *Q2Record) GetType() DatasetType {
	return DatasetTypeQ2
}

// NewQ2RecordFromString creates a Q2Record from a string
func NewQ2RecordFromString(data string) (*Q2Record, error) {
	parts := strings.Split(data, "|")
	return NewQ2RecordFromParts(parts)
}

// NewQ2RecordFromParts creates a Q2Record from string parts
func NewQ2RecordFromParts(parts []string) (*Q2Record, error) {
	if len(parts) < Q2RecordParts {
		return nil, fmt.Errorf("invalid Q2Record format: expected %d fields, got %d",
			Q2RecordParts, len(parts))
	}

	return &Q2Record{
		YearMonthCreatedAt: parts[0],
		ItemName:           parts[1],
		SellingsQty:        parts[2],
	}, nil
}

// Q3Record represents Q3 record: year_half_created_at, store_name, tpv
type Q3Record struct {
	YearHalfCreatedAt string
	StoreName         string
	TPV               string
}

const Q3RecordParts = 3

// Serialize returns the string representation of the Q3 record
func (q *Q3Record) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.YearHalfCreatedAt, q.StoreName, q.TPV)
}

// GetType returns the dataset type for Q3 records
func (q *Q3Record) GetType() DatasetType {
	return DatasetTypeQ3
}

// NewQ3RecordFromString creates a Q3Record from a string
func NewQ3RecordFromString(data string) (*Q3Record, error) {
	parts := strings.Split(data, "|")
	return NewQ3RecordFromParts(parts)
}

// NewQ3RecordFromParts creates a Q3Record from string parts
func NewQ3RecordFromParts(parts []string) (*Q3Record, error) {
	if len(parts) < Q3RecordParts {
		return nil, fmt.Errorf("invalid Q3Record format: expected %d fields, got %d",
			Q3RecordParts, len(parts))
	}

	return &Q3Record{
		YearHalfCreatedAt: parts[0],
		StoreName:         parts[1],
		TPV:               parts[2],
	}, nil
}

// Q4Record represents Q4 record: store_name, birthdate
type Q4Record struct {
	StoreName string
	Birthdate string
}

const Q4RecordParts = 2

// Serialize returns the string representation of the Q4 record
func (q *Q4Record) Serialize() string {
	return fmt.Sprintf("%s|%s", q.StoreName, q.Birthdate)
}

// GetType returns the dataset type for Q4 records
func (q *Q4Record) GetType() DatasetType {
	return DatasetTypeQ4
}

// NewQ4RecordFromString creates a Q4Record from a string
func NewQ4RecordFromString(data string) (*Q4Record, error) {
	parts := strings.Split(data, "|")
	return NewQ4RecordFromParts(parts)
}

// NewQ4RecordFromParts creates a Q4Record from string parts
func NewQ4RecordFromParts(parts []string) (*Q4Record, error) {
	if len(parts) < Q4RecordParts {
		return nil, fmt.Errorf("invalid Q4Record format: expected %d fields, got %d",
			Q4RecordParts, len(parts))
	}

	return &Q4Record{
		StoreName: parts[0],
		Birthdate: parts[1],
	}, nil
}
