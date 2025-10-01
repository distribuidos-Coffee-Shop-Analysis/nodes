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

// ===== Q2 Groups Records (output from GroupBy node) =====

// Q2GroupWithQuantityRecord represents grouped data with quantity: year_month, item_id, sellings_qty
type Q2GroupWithQuantityRecord struct {
	YearMonth   string
	ItemID      string
	SellingsQty string
}

const Q2GroupWithQuantityRecordParts = 3

func (q *Q2GroupWithQuantityRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.YearMonth, q.ItemID, q.SellingsQty)
}

func (q *Q2GroupWithQuantityRecord) GetType() DatasetType {
	return DatasetTypeQ2Groups
}

func NewQ2GroupWithQuantityRecordFromParts(parts []string) (*Q2GroupWithQuantityRecord, error) {
	if len(parts) < Q2GroupWithQuantityRecordParts {
		return nil, fmt.Errorf("invalid Q2GroupWithQuantityRecord format: expected %d fields, got %d",
			Q2GroupWithQuantityRecordParts, len(parts))
	}
	return &Q2GroupWithQuantityRecord{
		YearMonth:   parts[0],
		ItemID:      parts[1],
		SellingsQty: parts[2],
	}, nil
}

// Q2GroupWithSubtotalRecord represents grouped data with subtotal: year_month, item_id, profit_sum
type Q2GroupWithSubtotalRecord struct {
	YearMonth string
	ItemID    string
	ProfitSum string
}

const Q2GroupWithSubtotalRecordParts = 3

func (q *Q2GroupWithSubtotalRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.YearMonth, q.ItemID, q.ProfitSum)
}

func (q *Q2GroupWithSubtotalRecord) GetType() DatasetType {
	return DatasetTypeQ2Groups
}

func NewQ2GroupWithSubtotalRecordFromParts(parts []string) (*Q2GroupWithSubtotalRecord, error) {
	if len(parts) < Q2GroupWithSubtotalRecordParts {
		return nil, fmt.Errorf("invalid Q2GroupWithSubtotalRecord format: expected %d fields, got %d",
			Q2GroupWithSubtotalRecordParts, len(parts))
	}
	return &Q2GroupWithSubtotalRecord{
		YearMonth: parts[0],
		ItemID:    parts[1],
		ProfitSum: parts[2],
	}, nil
}

// ===== Q2 Aggregated Records (output from Aggregate node) =====

// Q2BestSellingRecord represents best selling items: year_month, item_id, sellings_qty
type Q2BestSellingRecord struct {
	YearMonth   string
	ItemID      string
	SellingsQty string
}

const Q2BestSellingRecordParts = 3

func (q *Q2BestSellingRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.YearMonth, q.ItemID, q.SellingsQty)
}

func (q *Q2BestSellingRecord) GetType() DatasetType {
	return DatasetTypeQ2Agg
}

func NewQ2BestSellingRecordFromParts(parts []string) (*Q2BestSellingRecord, error) {
	if len(parts) < Q2BestSellingRecordParts {
		return nil, fmt.Errorf("invalid Q2BestSellingRecord format: expected %d fields, got %d",
			Q2BestSellingRecordParts, len(parts))
	}
	return &Q2BestSellingRecord{
		YearMonth:   parts[0],
		ItemID:      parts[1],
		SellingsQty: parts[2],
	}, nil
}

// Q2MostProfitsRecord represents most profitable items: year_month, item_id, profit_sum
type Q2MostProfitsRecord struct {
	YearMonth string
	ItemID    string
	ProfitSum string
}

const Q2MostProfitsRecordParts = 3

func (q *Q2MostProfitsRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.YearMonth, q.ItemID, q.ProfitSum)
}

func (q *Q2MostProfitsRecord) GetType() DatasetType {
	return DatasetTypeQ2Agg
}

func NewQ2MostProfitsRecordFromParts(parts []string) (*Q2MostProfitsRecord, error) {
	if len(parts) < Q2MostProfitsRecordParts {
		return nil, fmt.Errorf("invalid Q2MostProfitsRecord format: expected %d fields, got %d",
			Q2MostProfitsRecordParts, len(parts))
	}
	return &Q2MostProfitsRecord{
		YearMonth: parts[0],
		ItemID:    parts[1],
		ProfitSum: parts[2],
	}, nil
}

// ===== Q2 Joined Records (output from Join node) =====

// Q2BestSellingWithNameRecord represents best selling items with names: year_month, item_name, sellings_qty
type Q2BestSellingWithNameRecord struct {
	YearMonth   string
	ItemName    string
	SellingsQty string
}

const Q2BestSellingWithNameRecordParts = 3

func (q *Q2BestSellingWithNameRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.YearMonth, q.ItemName, q.SellingsQty)
}

func (q *Q2BestSellingWithNameRecord) GetType() DatasetType {
	return DatasetTypeQ2AggWithName
}

func NewQ2BestSellingWithNameRecordFromParts(parts []string) (*Q2BestSellingWithNameRecord, error) {
	if len(parts) < Q2BestSellingWithNameRecordParts {
		return nil, fmt.Errorf("invalid Q2BestSellingWithNameRecord format: expected %d fields, got %d",
			Q2BestSellingWithNameRecordParts, len(parts))
	}
	return &Q2BestSellingWithNameRecord{
		YearMonth:   parts[0],
		ItemName:    parts[1],
		SellingsQty: parts[2],
	}, nil
}

// Q2MostProfitsWithNameRecord represents most profitable items with names: year_month, item_name, profit_sum
type Q2MostProfitsWithNameRecord struct {
	YearMonth string
	ItemName  string
	ProfitSum string
}

const Q2MostProfitsWithNameRecordParts = 3

func (q *Q2MostProfitsWithNameRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.YearMonth, q.ItemName, q.ProfitSum)
}

func (q *Q2MostProfitsWithNameRecord) GetType() DatasetType {
	return DatasetTypeQ2AggWithName
}

func NewQ2MostProfitsWithNameRecordFromParts(parts []string) (*Q2MostProfitsWithNameRecord, error) {
	if len(parts) < Q2MostProfitsWithNameRecordParts {
		return nil, fmt.Errorf("invalid Q2MostProfitsWithNameRecord format: expected %d fields, got %d",
			Q2MostProfitsWithNameRecordParts, len(parts))
	}
	return &Q2MostProfitsWithNameRecord{
		YearMonth: parts[0],
		ItemName:  parts[1],
		ProfitSum: parts[2],
	}, nil
}

// ===== Q3 Groups Records (output from GroupBy node) =====

// Q3GroupedRecord represents grouped transaction data by year_half and store: year_half_created_at, store_id, tpv
type Q3GroupedRecord struct {
	YearHalf string
	StoreID  string
	TPV      string
}

const Q3GroupedRecordParts = 3

func (q *Q3GroupedRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.YearHalf, q.StoreID, q.TPV)
}

func (q *Q3GroupedRecord) GetType() DatasetType {
	return DatasetTypeQ3Groups
}

func NewQ3GroupedRecordFromParts(parts []string) (*Q3GroupedRecord, error) {
	if len(parts) < Q3GroupedRecordParts {
		return nil, fmt.Errorf("invalid Q3GroupedRecord format: expected %d fields, got %d",
			Q3GroupedRecordParts, len(parts))
	}
	return &Q3GroupedRecord{
		YearHalf: parts[0],
		StoreID:  parts[1],
		TPV:      parts[2],
	}, nil
}

// ===== Q3 Aggregated Records (output from Aggregate node) =====

// Q3AggregatedRecord represents aggregated Q3 data: year_half_created_at, store_id, tpv
type Q3AggregatedRecord struct {
	YearHalf string
	StoreID  string
	TPV      string
}

const Q3AggregatedRecordParts = 3

func (q *Q3AggregatedRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.YearHalf, q.StoreID, q.TPV)
}

func (q *Q3AggregatedRecord) GetType() DatasetType {
	return DatasetTypeQ3Agg
}

func NewQ3AggregatedRecordFromParts(parts []string) (*Q3AggregatedRecord, error) {
	if len(parts) < Q3AggregatedRecordParts {
		return nil, fmt.Errorf("invalid Q3AggregatedRecord format: expected %d fields, got %d",
			Q3AggregatedRecordParts, len(parts))
	}
	return &Q3AggregatedRecord{
		YearHalf: parts[0],
		StoreID:  parts[1],
		TPV:      parts[2],
	}, nil
}

// ===== Q3 Joined Records (output from Join node) =====

// Q3JoinedRecord represents Q3 joined data with store name: year_half_created_at, store_name, tpv
type Q3JoinedRecord struct {
	YearHalf  string
	StoreName string
	TPV       string
}

const Q3JoinedRecordParts = 3

func (q *Q3JoinedRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.YearHalf, q.StoreName, q.TPV)
}

func (q *Q3JoinedRecord) GetType() DatasetType {
	return DatasetTypeQ3AggWithName
}

func NewQ3JoinedRecordFromParts(parts []string) (*Q3JoinedRecord, error) {
	if len(parts) < Q3JoinedRecordParts {
		return nil, fmt.Errorf("invalid Q3JoinedRecord format: expected %d fields, got %d",
			Q3JoinedRecordParts, len(parts))
	}
	return &Q3JoinedRecord{
		YearHalf:  parts[0],
		StoreName: parts[1],
		TPV:       parts[2],
	}, nil
}

// ===== Q4 Groups Records (output from GroupBy node) =====

// Q4GroupedRecord represents grouped transaction data: store_id, user_id, transaction_count
type Q4GroupedRecord struct {
	StoreID          string
	UserID           string
	TransactionCount string
}

const Q4GroupedRecordParts = 3

func (q *Q4GroupedRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.StoreID, q.UserID, q.TransactionCount)
}

func (q *Q4GroupedRecord) GetType() DatasetType {
	return DatasetTypeQ4Groups
}

func NewQ4GroupedRecordFromParts(parts []string) (*Q4GroupedRecord, error) {
	if len(parts) < Q4GroupedRecordParts {
		return nil, fmt.Errorf("invalid Q4GroupedRecord format: expected %d fields, got %d",
			Q4GroupedRecordParts, len(parts))
	}
	return &Q4GroupedRecord{
		StoreID:          parts[0],
		UserID:           parts[1],
		TransactionCount: parts[2],
	}, nil
}

// ===== Q4 Aggregated Records (output from Aggregate node - top 3 per store) =====

// Q4AggregatedRecord represents top 3 customers per store: store_id, user_id, purchases_qty
type Q4AggregatedRecord struct {
	StoreID      string
	UserID       string
	PurchasesQty string
}

const Q4AggregatedRecordParts = 3

func (q *Q4AggregatedRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s", q.StoreID, q.UserID, q.PurchasesQty)
}

func (q *Q4AggregatedRecord) GetType() DatasetType {
	return DatasetTypeQ4Agg
}

func NewQ4AggregatedRecordFromParts(parts []string) (*Q4AggregatedRecord, error) {
	if len(parts) < Q4AggregatedRecordParts {
		return nil, fmt.Errorf("invalid Q4AggregatedRecord format: expected %d fields, got %d",
			Q4AggregatedRecordParts, len(parts))
	}
	return &Q4AggregatedRecord{
		StoreID:      parts[0],
		UserID:       parts[1],
		PurchasesQty: parts[2],
	}, nil
}

// ===== Q4 Joined Records (first join with users) =====

// Q4JoinedWithUserRecord represents data joined with user birthdate: store_id, user_id, purchases_qty, birthdate
type Q4JoinedWithUserRecord struct {
	StoreID      string
	UserID       string
	PurchasesQty string
	Birthdate    string
}

const Q4JoinedWithUserRecordParts = 4

func (q *Q4JoinedWithUserRecord) Serialize() string {
	return fmt.Sprintf("%s|%s|%s|%s", q.StoreID, q.UserID, q.PurchasesQty, q.Birthdate)
}

func (q *Q4JoinedWithUserRecord) GetType() DatasetType {
	return DatasetTypeQ4AggWithUser
}

func NewQ4JoinedWithUserRecordFromParts(parts []string) (*Q4JoinedWithUserRecord, error) {
	if len(parts) < Q4JoinedWithUserRecordParts {
		return nil, fmt.Errorf("invalid Q4JoinedWithUserRecord format: expected %d fields, got %d",
			Q4JoinedWithUserRecordParts, len(parts))
	}
	return &Q4JoinedWithUserRecord{
		StoreID:      parts[0],
		UserID:       parts[1],
		PurchasesQty: parts[2],
		Birthdate:    parts[3],
	}, nil
}

// ===== Q4 Final Joined Records (second join with stores) =====

// Q4JoinedWithStoreAndUserRecord represents final Q4 data: store_name, birthdate
type Q4JoinedWithStoreAndUserRecord struct {
	StoreName string
	Birthdate string
}

const Q4JoinedWithStoreAndUserRecordParts = 2

func (q *Q4JoinedWithStoreAndUserRecord) Serialize() string {
	return fmt.Sprintf("%s|%s", q.StoreName, q.Birthdate)
}

func (q *Q4JoinedWithStoreAndUserRecord) GetType() DatasetType {
	return DatasetTypeQ4AggWithUserAndStore
}

func NewQ4JoinedWithStoreAndUserRecordFromParts(parts []string) (*Q4JoinedWithStoreAndUserRecord, error) {
	if len(parts) < Q4JoinedWithStoreAndUserRecordParts {
		return nil, fmt.Errorf("invalid Q4JoinedWithStoreAndUserRecord format: expected %d fields, got %d",
			Q4JoinedWithStoreAndUserRecordParts, len(parts))
	}
	return &Q4JoinedWithStoreAndUserRecord{
		StoreName: parts[0],
		Birthdate: parts[1],
	}, nil
}
