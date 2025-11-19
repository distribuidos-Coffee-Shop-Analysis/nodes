package joiners

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

type Q4StoreJoinerState struct {
	Stores map[string]*protocol.StoreRecord // key: store_id, value: store record
}

// Q4StoreJoiner handles the second join in Q4: joining user-joined data with store names
type Q4StoreJoiner struct {
	state    *Q4StoreJoinerState
	mu       sync.RWMutex
	clientID string
}

// NewQ4StoreJoiner creates a new Q4StoreJoiner instance
func NewQ4StoreJoiner() *Q4StoreJoiner {
	return &Q4StoreJoiner{
		state: &Q4StoreJoinerState{
			Stores: make(map[string]*protocol.StoreRecord),
		},
	}
}

// Name returns the name of this joiner
func (j *Q4StoreJoiner) Name() string {
	return "q4_joiner_stores"
}

// StoreReferenceDataset stores store reference data for future joins
func (j *Q4StoreJoiner) StoreReferenceDataset(records []protocol.Record) error {
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

// PerformJoin joins Q4 user-joined data with stored store information to produce final output
func (j *Q4StoreJoiner) PerformJoin(userJoinedRecords []protocol.Record, clientId string) ([]protocol.Record, error) {
	if j.clientID == "" {
		j.clientID = clientId
	}

	j.mu.RLock()
	defer j.mu.RUnlock()

	var joinedRecords []protocol.Record

	for _, record := range userJoinedRecords {
		userJoinedRecord, ok := record.(*protocol.Q4JoinedWithUserRecord)
		if !ok {
			return nil, fmt.Errorf("expected Q4JoinedWithUserRecord, got %T", record)
		}

		// Join user-joined data with store names
		storeRecord, exists := j.state.Stores[userJoinedRecord.StoreID]
		if !exists {
			continue // Skip records without matching stores
		}

		joinedRecord := &protocol.Q4JoinedWithStoreAndUserRecord{
			StoreName:    storeRecord.StoreName,
			PurchasesQty: userJoinedRecord.PurchasesQty,
			Birthdate:    userJoinedRecord.Birthdate,
		}
		joinedRecords = append(joinedRecords, joinedRecord)
	}

	return joinedRecords, nil
}

// GetOutputDatasetType returns the dataset type for Q4 final joined output
func (j *Q4StoreJoiner) GetOutputDatasetType() protocol.DatasetType {
	return protocol.DatasetTypeQ4AggWithUserAndStore
}

// AcceptsReferenceType checks if this joiner accepts stores as reference data
func (j *Q4StoreJoiner) AcceptsReferenceType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeStores
}

// AcceptsAggregateType checks if this joiner accepts Q4 user-joined data
func (j *Q4StoreJoiner) AcceptsAggregateType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeQ4AggWithUser
}

// Cleanup releases resources
// Note: We keep reference data because multiple EOF batches may arrive from upstream
// (e.g., 5 Q4 User Joiners all send EOF to 1 Q4 Store Joiner)
// Stores dataset is tiny (~10 rows, ~1KB) so keeping it in memory is fine
func (j *Q4StoreJoiner) Cleanup() error {
	// No-op: we keep reference data to handle multiple EOF batches
	return nil
}

// SerializeState exports the current joiner state as compact JSON
func (j *Q4StoreJoiner) SerializeState() ([]byte, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	return json.Marshal(j.state)
}

// RestoreState restores the joiner state from a JSON snapshot
func (j *Q4StoreJoiner) RestoreState(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var state Q4StoreJoinerState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("decode q4 store joiner snapshot: %w", err)
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
