package aggregates

import (
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q3Aggregate handles aggregation of Q3 grouped data to accumulate TPV by year_half and store
type Q3Aggregate struct {
	mu sync.RWMutex

	// Map to accumulate TPV by year_half + store_id
	tpvData map[string]float64 // year_half|store_id -> accumulated tpv

	// Track received batches to handle out-of-order EOF
	receivedBatches map[int]bool
	maxBatchIndex   int
	eofReceived     bool
}

// NewQ3Aggregate creates a new Q3 aggregate processor
func NewQ3Aggregate() *Q3Aggregate {
	return &Q3Aggregate{
		tpvData:         make(map[string]float64),
		receivedBatches: make(map[int]bool),
		maxBatchIndex:   -1,
		eofReceived:     false,
	}
}

func (a *Q3Aggregate) Name() string {
	return "q3_aggregate_tpv_by_year_half_store"
}

// AccumulateBatch processes and accumulates a batch of Q3 grouped records
func (a *Q3Aggregate) AccumulateBatch(records []protocol.Record, batchIndex int) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Mark this batch as received
	a.receivedBatches[batchIndex] = true

	log.Printf("action: q3_aggregate_batch | batch_index: %d | record_count: %d",
		batchIndex, len(records))

	for _, record := range records {
		q3GroupedRecord, ok := record.(*protocol.Q3GroupedRecord)
		if !ok {
			log.Printf("action: q3_aggregate_invalid_record | result: warning | "+
				"record_type: %T | expected: Q3GroupedRecord", record)
			continue
		}

		// Skip records with missing required fields
		if q3GroupedRecord.YearHalf == "" || q3GroupedRecord.StoreID == "" || q3GroupedRecord.TPV == "" {
			log.Printf("action: q3_aggregate_filter_null | result: dropped | "+
				"year_half: %s | store_id: %s | tpv: %s | reason: null_fields",
				q3GroupedRecord.YearHalf, q3GroupedRecord.StoreID, q3GroupedRecord.TPV)
			continue
		}

		// Parse TPV
		tpv, err := strconv.ParseFloat(q3GroupedRecord.TPV, 64)
		if err != nil {
			log.Printf("action: q3_aggregate_parse_tpv | result: error | "+
				"year_half: %s | store_id: %s | tpv: %s | error: %v",
				q3GroupedRecord.YearHalf, q3GroupedRecord.StoreID, q3GroupedRecord.TPV, err)
			continue
		}

		// Create aggregate key: year_half|store_id
		key := fmt.Sprintf("%s|%s", q3GroupedRecord.YearHalf, q3GroupedRecord.StoreID)

		// Accumulate TPV
		a.tpvData[key] += tpv

		log.Printf("action: q3_aggregate_accumulate | year_half: %s | store_id: %s | "+
			"batch_tpv: %.2f | accumulated_tpv: %.2f",
			q3GroupedRecord.YearHalf, q3GroupedRecord.StoreID, tpv, a.tpvData[key])
	}

	return nil
}

// Finalize generates the final aggregated TPV records by year_half and store
func (a *Q3Aggregate) Finalize() ([]protocol.Record, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	log.Printf("action: q3_aggregate_finalize | tpv_entries: %d", len(a.tpvData))

	var result []protocol.Record

	// Convert accumulated data to Q3AggregatedRecord
	for key, tpv := range a.tpvData {
		parts := parseAggregateKey(key)
		yearHalf, storeID := parts[0], parts[1]

		record := &protocol.Q3AggregatedRecord{
			YearHalf: yearHalf,
			StoreID:  storeID,
			TPV:      fmt.Sprintf("%.2f", tpv),
		}

		result = append(result, record)

		log.Printf("action: q3_aggregate_emit | year_half: %s | store_id: %s | tpv: %.2f",
			yearHalf, storeID, tpv)
	}

	log.Printf("action: q3_aggregate_finalize_complete | total_results: %d", len(result))

	return result, nil
}

// IsComplete checks if all expected batches have been received
func (a *Q3Aggregate) IsComplete(maxBatchIndex int) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if !a.eofReceived || maxBatchIndex < 0 {
		return false
	}

	// Check if all batches from 0 to maxBatchIndex have been received
	for i := 0; i <= maxBatchIndex; i++ {
		if !a.receivedBatches[i] {
			return false
		}
	}

	return true
}

// SetEOF marks that EOF has been received with the given max batch index
func (a *Q3Aggregate) SetEOF(maxBatchIndex int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.eofReceived = true
	a.maxBatchIndex = maxBatchIndex

	log.Printf("action: q3_aggregate_eof | max_batch_index: %d | received_batches: %d",
		maxBatchIndex, len(a.receivedBatches))
}
