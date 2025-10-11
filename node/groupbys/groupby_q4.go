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

	}

	return result, nil
}

// NewGroupByBatch creates a new batch message for the group by processor
func (g *Q4GroupBy) NewGroupByBatch(batchIndex int, records []protocol.Record, eof bool, clientID string) *protocol.BatchMessage {
	return &protocol.BatchMessage{
		Type:        protocol.MessageTypeBatch,
		DatasetType: protocol.DatasetTypeQ4Groups,
		ClientID:    clientID,
		BatchIndex:  batchIndex,
		Records:     records,
		EOF:         eof,
	}
}

// splitGroupKey splits the composite key "storeID|userID" into parts
func splitGroupKey(key string) []string {
	return strings.Split(key, "|")
}
