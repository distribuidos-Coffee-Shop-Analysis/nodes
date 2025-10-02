package groupbys

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q3GroupBy groups transaction records by year_half and store_id, calculating TPV (Total Payment Volume)
type Q3GroupBy struct {
}

// NewQ3GroupBy creates a new Q3 groupby processor
func NewQ3GroupBy() *Q3GroupBy {
	return &Q3GroupBy{}
}

func (g *Q3GroupBy) Name() string {
	return "q3_groupby_year_half_store"
}

// ProcessBatch groups records by year_half_created_at and store_id, summing final_amount as TPV
func (g *Q3GroupBy) ProcessBatch(records []protocol.Record, eof bool) ([]protocol.Record, error) {
	// Map to accumulate TPV by year_half + store_id
	groupTPV := make(map[string]float64)

	for _, record := range records {
		transactionRecord, ok := record.(*protocol.TransactionRecord)
		if !ok {
			log.Printf("action: groupby_q3_process | result: warning | msg: skipping non-transaction record")
			continue
		}

		// Skip records with missing required fields
		if transactionRecord.StoreID == "" || transactionRecord.CreatedAt == "" || transactionRecord.FinalAmount == "" {
			log.Printf("action: groupby_q3_filter_null | result: dropped | "+
				"transaction_id: %s | store_id: %s | created_at: %s | final_amount: %s | reason: null_groupby_key",
				transactionRecord.TransactionID, transactionRecord.StoreID,
				transactionRecord.CreatedAt, transactionRecord.FinalAmount)
			continue
		}

		// Parse created_at to determine year and half
		yearHalf, err := g.extractYearHalf(transactionRecord.CreatedAt)
		if err != nil {
			log.Printf("action: groupby_q3_parse_date | result: error | "+
				"transaction_id: %s | created_at: %s | error: %v",
				transactionRecord.TransactionID, transactionRecord.CreatedAt, err)
			continue
		}

		// Parse final_amount
		finalAmount, err := strconv.ParseFloat(transactionRecord.FinalAmount, 64)
		if err != nil {
			log.Printf("action: groupby_q3_parse_amount | result: error | "+
				"transaction_id: %s | final_amount: %s | error: %v",
				transactionRecord.TransactionID, transactionRecord.FinalAmount, err)
			continue
		}

		// Create group key: year_half|store_id
		key := fmt.Sprintf("%s|%s", yearHalf, transactionRecord.StoreID)

		// Accumulate TPV
		groupTPV[key] += finalAmount
	}

	// Convert grouped data to Q3GroupedRecord
	var result []protocol.Record
	for key, tpv := range groupTPV {
		parts := strings.Split(key, "|")
		yearHalf := parts[0]
		storeID := parts[1]

		groupedRecord := &protocol.Q3GroupedRecord{
			YearHalf: yearHalf,
			StoreID:  storeID,
			TPV:      fmt.Sprintf("%.2f", tpv),
		}

		result = append(result, groupedRecord)
	}

	// log.Printf("action: groupby_q3_complete | total_groups: %d | eof: %t",
	// 	len(result), eof)

	return result, nil
}

// extractYearHalf extracts year and half from created_at timestamp
// Returns format like "2024-H1" or "2024-H2"
func (g *Q3GroupBy) extractYearHalf(createdAt string) (string, error) {
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

	parsedTime, err := time.Parse("2006-01-02", datePart)
	if err != nil {
		return "", fmt.Errorf("failed to parse date %s: %v", datePart, err)
	}

	year := parsedTime.Year()
	month := parsedTime.Month()

	// Determine half: H1 (Jan-Jun) or H2 (Jul-Dec)
	var half string
	if month <= 6 {
		half = "H1"
	} else {
		half = "H2"
	}

	return fmt.Sprintf("%d-%s", year, half), nil
}
