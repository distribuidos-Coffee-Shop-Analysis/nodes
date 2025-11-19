package joiners

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

type Q3JoinerState struct {
	Stores map[string]*protocol.StoreRecord // key: store_id, value: store record
}

// Q3Joiner handles joining Q3 aggregate data with store names
type Q3Joiner struct {
	state    *Q3JoinerState
	mu       sync.RWMutex
	clientID string
}

// NewQ3Joiner creates a new Q3Joiner instance
func NewQ3Joiner() *Q3Joiner {
	return &Q3Joiner{
		state: &Q3JoinerState{
			Stores: make(map[string]*protocol.StoreRecord),
		},
	}
}

// Name returns the name of this joiner
func (j *Q3Joiner) Name() string {
	return "q3_joiner_stores"
}

// StoreReferenceDataset stores store reference data for future joins
func (j *Q3Joiner) StoreReferenceDataset(records []protocol.Record) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	for _, record := range records {
		storeRecord, ok := record.(*protocol.StoreRecord)
		if !ok {
			return fmt.Errorf("expected StoreRecord, got %T", record)
		}

		j.state.Stores[storeRecord.StoreID] = storeRecord
	}

	return nil
}

// PerformJoin joins Q3 aggregated data with stored store information
func (j *Q3Joiner) PerformJoin(aggregatedRecords []protocol.Record, clientId string) ([]protocol.Record, error) {
	if j.clientID == "" {
		j.clientID = clientId
	}

	j.mu.RLock()
	defer j.mu.RUnlock()

	var joinedRecords []protocol.Record

	for _, record := range aggregatedRecords {
		aggRecord, ok := record.(*protocol.Q3AggregatedRecord)
		if !ok {
			return nil, fmt.Errorf("expected Q3AggregatedRecord, got %T", record)
		}

		// Join aggregated data with store names
		storeRecord, exists := j.state.Stores[aggRecord.StoreID]
		if !exists {
			continue // Skip records without matching stores
		}

		joinedRecord := &protocol.Q3JoinedRecord{
			YearHalf:  aggRecord.YearHalf,
			StoreName: storeRecord.StoreName,
			TPV:       aggRecord.TPV,
		}
		joinedRecords = append(joinedRecords, joinedRecord)
	}

	return joinedRecords, nil
}

// GetOutputDatasetType returns the dataset type for Q3 joined output
func (j *Q3Joiner) GetOutputDatasetType() protocol.DatasetType {
	return protocol.DatasetTypeQ3AggWithName
}

// AcceptsReferenceType checks if this joiner accepts stores as reference data
func (j *Q3Joiner) AcceptsReferenceType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeStores
}

// AcceptsAggregateType checks if this joiner accepts Q3 aggregate data
func (j *Q3Joiner) AcceptsAggregateType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeQ3Agg
}

// Cleanup releases resources
// Note: We keep reference data because multiple EOF batches may arrive from upstream
// Stores dataset is tiny (~10 rows, ~1KB) so keeping it in memory is fine
func (j *Q3Joiner) Cleanup() error {
	// No-op: we keep reference data to handle multiple EOF batches
	return nil
}

// SerializeState exports the current joiner state as compact JSON
func (j *Q3Joiner) SerializeState() ([]byte, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	return json.Marshal(j.state)
}

// RestoreState restores the joiner state from a JSON snapshot
func (j *Q3Joiner) RestoreState(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var state Q3JoinerState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("decode q3 joiner snapshot: %w", err)
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	if state.Stores != nil {
		j.state.Stores = state.Stores
	} else {
		j.state.Stores = make(map[string]*protocol.StoreRecord)
	}

	return nil
}
