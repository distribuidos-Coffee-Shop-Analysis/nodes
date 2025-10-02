package filters

import (
	"fmt"
	"log"
	"strconv"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// AmountFilter filters transactions by final amount
type AmountFilter struct {
	MinAmount int
}

func NewAmountFilter(minAmount int) *AmountFilter {
	return &AmountFilter{
		MinAmount: minAmount,
	}
}

func (af *AmountFilter) Name() string {
	return fmt.Sprintf("amount_filter_min_%d", af.MinAmount)
}

func (af *AmountFilter) Filter(record protocol.Record) bool {
	// Only apply to TransactionRecord
	transactionRecord, ok := record.(*protocol.TransactionRecord)
	if !ok {
		// For non-transaction records (like TransactionItems), we pass them through
		return true
	}

	amountFloat, err := strconv.ParseFloat(transactionRecord.FinalAmount, 64)
	if err != nil {
		log.Printf("action: AMOUNT_FILTER_PARSE_ERROR | transaction_id: %s | final_amount: %s | error: %v", transactionRecord.TransactionID, transactionRecord.FinalAmount, err)
		return false
	}

	amount := int64(amountFloat)
	result := amount > int64(af.MinAmount)

	return result
}
