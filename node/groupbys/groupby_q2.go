package groupbys

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q2GroupBy groups transaction item records by year_month and item_id, generating quantity and subtotal datasets
type Q2GroupBy struct {
}

// NewQ2GroupBy creates a new Q2 groupby processor
func NewQ2GroupBy() *Q2GroupBy {
	return &Q2GroupBy{}
}

func (g *Q2GroupBy) Name() string {
	return "q2_groupby_year_month_item"
}

// NewGroupByBatch creates a batch message specifically for Q2 grouped data with dual subdatasets
func (g *Q2GroupBy) NewGroupByBatch(batchIndex int, records []protocol.Record, eof bool, clientID string) *protocol.BatchMessage {
	return &protocol.BatchMessage{
		Type:        protocol.MessageTypeBatch,
		DatasetType: protocol.DatasetTypeQ2Groups,
		ClientID:    clientID,
		BatchIndex:  batchIndex,
		Records:     records,
		EOF:         eof,
	}
}

// ProcessBatch groups records by year_month_created_at and item_id, generating two subdatasets:
// 1. Quantity records (count of transactions)
// 2. Subtotal records (sum of subtotals)
func (g *Q2GroupBy) ProcessBatch(records []protocol.Record, eof bool) ([]protocol.Record, error) {
	// Maps to accumulate data by year_month + item_id
	groupQuantity := make(map[string]float64) // For summing quantities
	groupSubtotal := make(map[string]float64) // For summing subtotals

	for _, record := range records {
		transactionItemRecord, ok := record.(*protocol.TransactionItemRecord)
		if !ok {
			log.Printf("action: groupby_q2_process | result: warning | msg: skipping non-transaction-item record")
			continue
		}

		// Skip records with missing required fields
		if transactionItemRecord.ItemID == "" || transactionItemRecord.CreatedAt == "" || transactionItemRecord.Subtotal == "" || transactionItemRecord.Quantity == "" {
			log.Printf("action: groupby_q2_filter_null | result: dropped | "+
				"transaction_id: %s | item_id: %s | created_at: %s | subtotal: %s | quantity: %s | reason: null_groupby_key",
				transactionItemRecord.TransactionID, transactionItemRecord.ItemID,
				transactionItemRecord.CreatedAt, transactionItemRecord.Subtotal, transactionItemRecord.Quantity)
			continue
		}

		// Extract year_month from created_at
		yearMonth, err := g.extractYearMonth(transactionItemRecord.CreatedAt)
		if err != nil {
			log.Printf("action: groupby_q2_parse_date | result: error | "+
				"transaction_id: %s | created_at: %s | error: %v",
				transactionItemRecord.TransactionID, transactionItemRecord.CreatedAt, err)
			continue
		}

		// Parse quantity
		quantity, err := strconv.ParseFloat(transactionItemRecord.Quantity, 64)
		if err != nil {
			log.Printf("action: groupby_q2_parse_quantity | result: error | "+
				"transaction_id: %s | quantity: %s | error: %v",
				transactionItemRecord.TransactionID, transactionItemRecord.Quantity, err)
			continue
		}

		// Parse subtotal
		subtotal, err := strconv.ParseFloat(transactionItemRecord.Subtotal, 64)
		if err != nil {
			log.Printf("action: groupby_q2_parse_subtotal | result: error | "+
				"transaction_id: %s | subtotal: %s | error: %v",
				transactionItemRecord.TransactionID, transactionItemRecord.Subtotal, err)
			continue
		}

		// Create group key: year_month|item_id
		key := fmt.Sprintf("%s|%s", yearMonth, transactionItemRecord.ItemID)

		// Accumulate quantity (sum) and subtotal (sum)
		groupQuantity[key] += quantity
		groupSubtotal[key] += subtotal
	}

	// Convert grouped data to Q2 records
	var quantityRecords []protocol.Record
	var subtotalRecords []protocol.Record

	for key, quantity := range groupQuantity {
		parts := strings.Split(key, "|")
		yearMonth := parts[0]
		itemID := parts[1]

		// Create quantity record
		quantityRecord := &protocol.Q2GroupWithQuantityRecord{
			YearMonth:   yearMonth,
			ItemID:      itemID,
			SellingsQty: fmt.Sprintf("%.0f", quantity), // Format as integer-like (no decimals)
		}
		quantityRecords = append(quantityRecords, quantityRecord)

		// Create subtotal record (get subtotal from the other map)
		if subtotalSum, exists := groupSubtotal[key]; exists {
			subtotalRecord := &protocol.Q2GroupWithSubtotalRecord{
				YearMonth: yearMonth,
				ItemID:    itemID,
				ProfitSum: fmt.Sprintf("%.2f", subtotalSum),
			}
			subtotalRecords = append(subtotalRecords, subtotalRecord)
		}
	}

	// Combine both types of records into a single slice
	// The Q2 dual dataset format will handle the serialization
	var result []protocol.Record
	result = append(result, quantityRecords...)
	result = append(result, subtotalRecords...)

	// log.Printf("action: groupby_q2_complete | total_groups: %d | quantity_records: %d | subtotal_records: %d | eof: %t",
	// 	len(groupQuantity), len(quantityRecords), len(subtotalRecords), eof)

	return result, nil
}

// extractYearMonth extracts year and month from created_at timestamp
// Returns format like "2024-01" or "2024-12"
func (g *Q2GroupBy) extractYearMonth(createdAt string) (string, error) {
	// Handle different timestamp formats
	var datePart string
	if strings.Contains(createdAt, "T") {
		// ISO format: "2024-01-15T10:30:00"
		datePart = strings.Split(createdAt, "T")[0]
	} else if strings.Contains(createdAt, " ") {
		// Space format: "2024-01-15 10:30:00"
		datePart = strings.Split(createdAt, " ")[0]
	} else {
		// Date only: "2024-01-15"
		datePart = createdAt
	}

	// Parse the date part
	parsedTime, err := time.Parse("2006-01-02", datePart)
	if err != nil {
		return "", fmt.Errorf("failed to parse date %s: %v", datePart, err)
	}

	// Format as YYYY-MM
	return parsedTime.Format("2006-01"), nil
}
