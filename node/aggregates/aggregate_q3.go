package aggregates

import (
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

type Q3AggregateState struct {
	TPVData map[string]float64 // year_half|store_id -> accumulated tpv
}

// Q3Aggregate handles aggregation of Q3 grouped data to accumulate TPV by year_half and store
type Q3Aggregate struct {
	mu          sync.RWMutex
	state       *Q3AggregateState
	persistence *common.StatePersistence
	clientID    string
}

// NewQ3Aggregate creates a new Q3 aggregate processor
func NewQ3Aggregate() *Q3Aggregate {
	return NewQ3AggregateWithPersistence("/app/state")
}

func NewQ3AggregateWithPersistence(stateDir string) *Q3Aggregate {
	persistence, err := common.NewStatePersistence(stateDir)
	if err != nil {
		log.Printf("action: q3_aggregate_init | result: fail | error: %v | fallback: memory_only", err)
		persistence = nil
	}

	return &Q3Aggregate{
		state: &Q3AggregateState{
			TPVData: make(map[string]float64),
		},
		persistence: persistence,
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

	if a.persistence != nil && a.clientID != "" {
		if err := a.persistence.SaveState(a.Name(), a.clientID, a.state); err != nil {
			log.Printf("action: q3_save_state | result: fail | client_id: %s | error: %v",
				a.clientID, err)
		}
	}

	return nil
}

// Finalize generates the final aggregated TPV records by year_half and store
func (a *Q3Aggregate) Finalize(clientId string) ([]protocol.Record, error) {

	if a.clientID == "" {
		a.clientID = clientId

		if a.persistence != nil {
			var savedState Q3AggregateState
			if err := a.persistence.LoadState(a.Name(), clientId, &savedState); err != nil {
				log.Printf("action: q3_load_state | result: fail | client_id: %s | error: %v",
					clientId, err)
			} else if savedState.TPVData != nil {
				a.mu.Lock()
				for key, tpv := range savedState.TPVData {
					a.state.TPVData[key] += tpv
				}
				a.mu.Unlock()
				log.Printf("action: q3_load_state | result: success | client_id: %s | tpv_entries: %d",
					clientId, len(savedState.TPVData))
			}
		}
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

	if a.persistence != nil && a.clientID != "" {
		if err := a.persistence.DeleteState(a.Name(), a.clientID); err != nil {
			log.Printf("action: q3_delete_state | result: fail | client_id: %s | error: %v",
				a.clientID, err)
		}
	}

	a.state.TPVData = nil
	a.state = nil

	return nil
}
