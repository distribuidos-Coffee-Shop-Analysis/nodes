package aggregates

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q3Aggregate handles aggregation of Q3 grouped data to accumulate TPV by year_half and store
type Q3Aggregate struct {
	mu sync.RWMutex

	// Map to accumulate TPV by year_half + store_id
	tpvData map[string]float64 // year_half|store_id -> accumulated tpv

	// Track unique batch indices
	seenBatchIndices map[int]bool // track which batch indices we've seen
	uniqueBatchCount atomic.Int32 // count of unique batch indices
}

// NewQ3Aggregate creates a new Q3 aggregate processor
func NewQ3Aggregate() *Q3Aggregate {
	return &Q3Aggregate{
		tpvData:         make(map[string]float64),
		seenBatchIndices: make(map[int]bool),
		uniqueBatchCount: atomic.Int32{},
	}
}

func (a *Q3Aggregate) Name() string {
	return "q3_aggregate_tpv_by_year_half_store"
}

// AccumulateBatch processes and accumulates a batch of Q3 grouped records
func (a *Q3Aggregate) AccumulateBatch(records []protocol.Record, batchIndex int) error {
	// Only count as one batch per batchIndex (Q2 has dual datasets but same batchIndex)
	// We'll track seen batch indices to avoid double counting
	a.trackBatchIndex(batchIndex)

	// Process records locally without lock (no shared state access)
	localTPV := make(map[string]float64)

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

		// Accumulate TPV in local map
		localTPV[key] += tpv
	}

	// Only lock for the final merge into shared map (critical section)
	a.mu.Lock()
	defer a.mu.Unlock()

	for key, tpv := range localTPV {
		a.tpvData[key] += tpv
	}

	return nil
}

// Finalize generates the final aggregated TPV records by year_half and store
func (a *Q3Aggregate) Finalize() ([]protocol.Record, error) {

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

// GetAccumulatedBatchCount returns the number of batches received so far
func (a *Q3Aggregate) GetAccumulatedBatchCount() int {
	return int(a.uniqueBatchCount.Load()) // No lock needed for atomic read
}

func (a *Q3Aggregate) trackBatchIndex(batchIndex int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.seenBatchIndices[batchIndex] {
		a.seenBatchIndices[batchIndex] = true
		a.uniqueBatchCount.Add(1)
	}
}

// GetBatchesToPublish returns a single batch with all aggregated results
// Q3 doesn't need partitioning, so returns a single batch with empty routing key (uses default from config)
func (a *Q3Aggregate) GetBatchesToPublish(batchIndex int) ([]BatchToPublish, error) {
	results, err := a.Finalize()
	if err != nil {
		return nil, err
	}

	batch := protocol.NewAggregateBatch(batchIndex, results, true)

	return []BatchToPublish{
		{
			Batch:      batch,
			RoutingKey: "",
		},
	}, nil
}
