package aggregates

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q3RawRecord stores raw data before aggregation
type Q3RawRecord struct {
	YearHalf string
	StoreID  string
	TPV      float64
}

type Q3AggregateState struct {
	RawRecords []Q3RawRecord // Store raw records, aggregate only on Finalize
}

// Q3Aggregate handles aggregation of Q3 grouped data to accumulate TPV by year_half and store
type Q3Aggregate struct {
	mu       sync.RWMutex
	state    *Q3AggregateState
	clientID string
}

// NewQ3Aggregate creates a new Q3 aggregate processor
func NewQ3Aggregate() *Q3Aggregate {
	return &Q3Aggregate{
		state: &Q3AggregateState{
			RawRecords: make([]Q3RawRecord, 0),
		},
	}
}

func (a *Q3Aggregate) Name() string {
	return "q3_aggregate_tpv_by_year_half_store"
}

// AccumulateBatch stores raw records WITHOUT aggregation
// Aggregation happens only in Finalize() to ensure correctness
func (a *Q3Aggregate) AccumulateBatch(records []protocol.Record, batchIndex int) error {

	// Process records locally without lock
	localRawRecords := make([]Q3RawRecord, 0, len(records))

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

		// STORE RAW - NO AGGREGATION YET
		localRawRecords = append(localRawRecords, Q3RawRecord{
			YearHalf: q3GroupedRecord.YearHalf,
			StoreID:  q3GroupedRecord.StoreID,
			TPV:      tpv,
		})
	}

	// Only lock for the final append into shared state
	a.mu.Lock()
	defer a.mu.Unlock()

	a.state.RawRecords = append(a.state.RawRecords, localRawRecords...)

	return nil
}

// Finalize aggregates ALL raw records and generates final TPV by year_half and store
func (a *Q3Aggregate) Finalize(clientId string) ([]protocol.Record, error) {

	if a.clientID == "" {
		a.clientID = clientId
	}

	log.Printf("action: q3_aggregate_finalize_start | client_id: %s | raw_records: %d",
		clientId, len(a.state.RawRecords))

	// NOW we aggregate: sum TPV by (year_half, store_id)
	tpvData := make(map[string]float64)

	for _, rawRecord := range a.state.RawRecords {
		key := fmt.Sprintf("%s|%s", rawRecord.YearHalf, rawRecord.StoreID)
		tpvData[key] += rawRecord.TPV
	}

	log.Printf("action: q3_aggregate_finalize_aggregated | client_id: %s | unique_keys: %d",
		clientId, len(tpvData))

	var result []protocol.Record

	// Convert aggregated data to Q3AggregatedRecord
	for key, tpv := range tpvData {
		parts := parseAggregateKey(key)
		yearHalf, storeID := parts[0], parts[1]

		record := &protocol.Q3AggregatedRecord{
			YearHalf: yearHalf,
			StoreID:  storeID,
			TPV:      fmt.Sprintf("%.2f", tpv),
		}

		result = append(result, record)

		log.Printf("action: q3_aggregate_emit | client_id: %s | year_half: %s | store_id: %s | tpv: %.2f",
			clientId, yearHalf, storeID, tpv)
	}

	log.Printf("action: q3_aggregate_finalize_complete | client_id: %s | total_results: %d", clientId, len(result))

	return result, nil
}

// GetBatchesToPublish returns a single batch with all aggregated results
// Q3 doesn't need partitioning, so returns a single batch with empty routing key (uses default from config)
func (a *Q3Aggregate) GetBatchesToPublish(historicalIncrements [][]byte, batchIndex int, clientID string) ([]BatchToPublish, error) {

	// APPEND-ONLY BUFFER STRATEGY:
	// Merge all historical increments (data that was persisted and cleared from memory)
	// Then combine with current in-memory buffer to get complete dataset
	if len(historicalIncrements) > 0 {
		log.Printf("action: q3_merge_start | client_id: %s | increments: %d",
			clientID, len(historicalIncrements))

		for i, incrementData := range historicalIncrements {
			if len(incrementData) > 0 {
				if err := a.RestoreState(incrementData); err != nil {
					log.Printf("action: q3_merge_increment | client_id: %s | increment: %d | result: fail | error: %v",
						clientID, i, err)
					// Continue with other increments
				}
			}
		}

		log.Printf("action: q3_merge_complete | client_id: %s | increments_merged: %d | raw_records: %d",
			clientID, len(historicalIncrements), len(a.state.RawRecords))
	}

	results, err := a.Finalize(clientID)
	if err != nil {
		return nil, err
	}

	batch := protocol.NewAggregateBatch(batchIndex, results, clientID, true)

	return []BatchToPublish{
		{
			Batch:      batch,
			RoutingKey: "",
		},
	}, nil
}

// ClearBuffer clears the in-memory raw records buffer after successful persistence
// This frees memory while keeping historical data on disk
// NOTE: With atomic snapshot in SerializeState, this is now a no-op (already cleared)
func (a *Q3Aggregate) ClearBuffer() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Clear already done in SerializeState atomically with snapshot
	// This is kept for interface compatibility
	a.state.RawRecords = make([]Q3RawRecord, 0)

	return nil
}

// Cleanup releases all resources held by this aggregate
func (a *Q3Aggregate) Cleanup() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.state.RawRecords = nil
	a.state = nil

	return nil
}

type q3AggregateSnapshot struct {
	TPVData map[string]float64 `json:"tpv_data"`
}

// Format: key|tpv\n
// ATOMIC SNAPSHOT: This method takes a snapshot of current data, clears the buffer, then serializes
// This prevents race condition where data arrives between serialize and clear
func (a *Q3Aggregate) SerializeState() ([]byte, error) {
	// CRITICAL: Use WRITE lock and clear buffer atomically with snapshot
	a.mu.Lock()

	recordCount := len(a.state.RawRecords)
	log.Printf("action: q3_serialize_state | raw_records_count: %d", recordCount)

	// Make a snapshot copy of current buffer
	snapshot := make([]Q3RawRecord, recordCount)
	copy(snapshot, a.state.RawRecords)

	// ATOMICALLY clear the buffer (new data can now accumulate during serialization)
	a.state.RawRecords = make([]Q3RawRecord, 0)
	log.Printf("action: q3_serialize_clear | cleared: %d | new_buffer_ready", recordCount)

	a.mu.Unlock()

	// Serialize the snapshot WITHOUT holding the lock (can take time)
	var buf bytes.Buffer
	buf.Grow(recordCount * 50)

	// Serialize raw records (no aggregation)
	// Format: year_half|store_id|tpv
	for _, rawRecord := range snapshot {
		buf.WriteString(rawRecord.YearHalf)
		buf.WriteByte('|')
		buf.WriteString(rawRecord.StoreID)
		buf.WriteByte('|')
		buf.WriteString(strconv.FormatFloat(rawRecord.TPV, 'f', 2, 64))
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

// RestoreState restores Q3 aggregate state
// Format: year_half|store_id|tpv
// Note: key is composite "year_half|store_id" so we need to reconstruct it
func (a *Q3Aggregate) RestoreState(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Initialize slice if not already created
	if a.state.RawRecords == nil {
		a.state.RawRecords = make([]Q3RawRecord, 0)
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))
	lineNum := 0
	restoredCount := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		if len(line) == 0 {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) != 3 {
			log.Printf("action: q3_restore_skip_invalid_line | line: %d | parts: %d | expected: 3", lineNum, len(parts))
			continue
		}

		yearHalf := parts[0]
		storeID := parts[1]
		tpvValue := parts[2]

		tpv, err := strconv.ParseFloat(tpvValue, 64)
		if err != nil {
			log.Printf("action: q3_restore_skip_invalid_tpv | line: %d | value: %s | error: %v",
				lineNum, tpvValue, err)
			continue
		}

		// APPEND raw record (no aggregation)
		a.state.RawRecords = append(a.state.RawRecords, Q3RawRecord{
			YearHalf: yearHalf,
			StoreID:  storeID,
			TPV:      tpv,
		})
		restoredCount++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan q3 aggregate snapshot: %w", err)
	}

	return nil
}
