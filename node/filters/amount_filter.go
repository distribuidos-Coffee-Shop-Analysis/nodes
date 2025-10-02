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

	amount, err := strconv.ParseInt(transactionRecord.FinalAmount, 10, 64)
	if err != nil {
		return false
	}

	log.Printf("action: AMOUNT FILTER | transaction_id: %s | "+
			"final amount: %s f",
			transactionRecord.TransactionID, transactionRecord.FinalAmount)

	return amount > int64(af.MinAmount)
}
