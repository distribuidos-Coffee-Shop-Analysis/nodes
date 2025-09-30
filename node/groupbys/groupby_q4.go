package groupbys

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q4GroupBy groups transaction records by StoreID and UserID within each batch
type Q4GroupBy struct {
}

// NewQ4GroupBy creates a new Q4 groupby processor
func NewQ4GroupBy() *Q4GroupBy {
	return &Q4GroupBy{}
}

func (g *Q4GroupBy) Name() string {
	return "q4_groupby_store_user"
}

// ProcessBatch groups records by StoreID and UserID 
func (g *Q4GroupBy) ProcessBatch(records []protocol.Record, eof bool) ([]protocol.Record, error) {
	groupCounts := make(map[string]int)

	for _, record := range records {
		transactionRecord, ok := record.(*protocol.TransactionRecord)
		if !ok {
			log.Printf("action: groupby_q4_process | result: warning | msg: skipping non-transaction record")
			continue
		}

		if transactionRecord.StoreID == "" || transactionRecord.UserID == "" {
			log.Printf("action: groupby_q4_filter_null | result: dropped | "+
				"transaction_id: %s | store_id: %s | user_id: %s | reason: null_groupby_key",
				transactionRecord.TransactionID, transactionRecord.StoreID, transactionRecord.UserID)
			continue
		}

		key := fmt.Sprintf("%s|%s", transactionRecord.StoreID, transactionRecord.UserID)
		
		groupCounts[key]++
	}

	var result []protocol.Record
	for key, count := range groupCounts {
	
		parts := splitGroupKey(key)
		storeID := parts[0]
		userID := parts[1]
		
		groupedRecord := &protocol.Q4GroupedRecord{
			StoreID:          storeID,
			UserID:           userID,
			TransactionCount: strconv.Itoa(count),
		}
		
		result = append(result, groupedRecord)
		
		log.Printf("action: groupby_q4_emit | store_id: %s | user_id: %s | "+
			"transaction_count: %d", storeID, userID, count)
	}

	log.Printf("action: groupby_q4_complete | total_groups: %d | eof: %t",
		len(result), eof)

	return result, nil
}

func (g *Q4GroupBy) Reset() {
	log.Printf("action: groupby_q4_reset | result: success | note: stateless, nothing to reset")
}

// splitGroupKey splits the composite key "storeID|userID" into parts
func splitGroupKey(key string) []string {
	return strings.Split(key, "|")
}