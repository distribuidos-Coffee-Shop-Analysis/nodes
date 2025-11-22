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

type Q3AggregateState struct {
	TPVData map[string]float64 // year_half|store_id -> accumulated tpv
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
			TPVData: make(map[string]float64),
		},
	}
}

func (a *Q3Aggregate) Name() string {
	return "q3_aggregate_tpv_by_year_half_store"
}

// AccumulateBatch processes and accumulates a batch of Q3 grouped records
func (a *Q3Aggregate) AccumulateBatch(records []protocol.Record, batchIndex int) error {

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

	// Only lock for the final merge into shared state (critical section)
	a.mu.Lock()
	defer a.mu.Unlock()

	for key, tpv := range localTPV {
		a.state.TPVData[key] += tpv
	}

	return nil
}

// Finalize generates the final aggregated TPV records by year_half and store
func (a *Q3Aggregate) Finalize(clientId string) ([]protocol.Record, error) {

	if a.clientID == "" {
		a.clientID = clientId
	}

	log.Printf("action: q3_aggregate_finalize | client_id: %s | tpv_entries: %d", clientId, len(a.state.TPVData))

	var result []protocol.Record

	// Convert accumulated data to Q3AggregatedRecord
	for key, tpv := range a.state.TPVData {
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
func (a *Q3Aggregate) GetBatchesToPublish(batchIndex int, clientID string) ([]BatchToPublish, error) {
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

// Cleanup releases all resources held by this aggregate
func (a *Q3Aggregate) Cleanup() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.state.TPVData = nil
	a.state = nil

	return nil
}

type q3AggregateSnapshot struct {
	TPVData map[string]float64 `json:"tpv_data"`
}

// Format: key|tpv\n
func (a *Q3Aggregate) SerializeState() ([]byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var buf bytes.Buffer
	buf.Grow(len(a.state.TPVData) * 50)

	for key, tpv := range a.state.TPVData {
		buf.WriteString(key)
		buf.WriteByte('|')
		buf.WriteString(strconv.FormatFloat(tpv, 'f', 2, 64))
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

	a.state.TPVData = make(map[string]float64)

	scanner := bufio.NewScanner(bytes.NewReader(data))
	lineNum := 0

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

		// Reconstruct composite key: "year_half|store_id"
		key := yearHalf + "|" + storeID

		tpv, err := strconv.ParseFloat(tpvValue, 64)
		if err != nil {
			log.Printf("action: q3_restore_skip_invalid_tpv | line: %d | value: %s | error: %v",
				lineNum, parts[1], err)
			continue
		}

		a.state.TPVData[key] = tpv
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan q3 aggregate snapshot: %w", err)
	}

	log.Printf("action: q3_restore_complete | tpv_entries: %d", len(a.state.TPVData))

	return nil
}
