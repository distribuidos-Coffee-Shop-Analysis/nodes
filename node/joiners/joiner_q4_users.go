package joiners

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

type Q4UserJoinerState struct {
	Users map[string]string // key: user_id, value: birthdate
}

// Q4UserJoiner handles joining Q4 aggregate data with user information (birthdate)
type Q4UserJoiner struct {
	state    *Q4UserJoinerState
	mu       sync.RWMutex
	clientID string
}

// NewQ4UserJoiner creates a new Q4UserJoiner instance
func NewQ4UserJoiner() *Q4UserJoiner {
	return &Q4UserJoiner{
		state: &Q4UserJoinerState{
			Users: make(map[string]string),
		},
	}
}

// Name returns the name of this joiner
func (j *Q4UserJoiner) Name() string {
	return "q4_joiner_users"
}

// StoreReferenceDataset stores user reference data for future joins
// Only stores user_id and birthdate to optimize memory usage (users dataset is large)
func (j *Q4UserJoiner) StoreReferenceDataset(records []protocol.Record) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	for _, record := range records {
		userRecord, ok := record.(*protocol.UserRecord)
		if !ok {
			return fmt.Errorf("expected UserRecord, got %T", record)
		}

		normalizedUserID := common.NormalizeUserID(userRecord.UserID)
		j.state.Users[normalizedUserID] = userRecord.Birthdate
	}

	return nil
}

// PerformJoin joins Q4 aggregated data with stored user information
func (j *Q4UserJoiner) PerformJoin(aggregatedRecords []protocol.Record, clientId string) ([]protocol.Record, error) {
	if j.clientID == "" {
		j.clientID = clientId
	}

	j.mu.RLock()
	defer j.mu.RUnlock()

	var joinedRecords []protocol.Record

	for _, record := range aggregatedRecords {
		aggRecord, ok := record.(*protocol.Q4AggregatedRecord)
		if !ok {
			return nil, fmt.Errorf("expected Q4AggregatedRecord, got %T", record)
		}

		// Normalize UserID to handle potential ".0" suffix
		normalizedUserID := common.NormalizeUserID(aggRecord.UserID)

		// Join aggregated data with user birthdate
		birthdate, exists := j.state.Users[normalizedUserID]
		if !exists {
			continue // Skip records without matching users
		}

		joinedRecord := &protocol.Q4JoinedWithUserRecord{
			StoreID:      aggRecord.StoreID,
			UserID:       aggRecord.UserID,
			PurchasesQty: aggRecord.PurchasesQty,
			Birthdate:    birthdate,
		}
		joinedRecords = append(joinedRecords, joinedRecord)
	}

	return joinedRecords, nil
}

// GetOutputDatasetType returns the dataset type for Q4 user joined output
func (j *Q4UserJoiner) GetOutputDatasetType() protocol.DatasetType {
	return protocol.DatasetTypeQ4AggWithUser
}

// AcceptsReferenceType checks if this joiner accepts users as reference data
func (j *Q4UserJoiner) AcceptsReferenceType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeUsers
}

// AcceptsAggregateType checks if this joiner accepts Q4 aggregate data
func (j *Q4UserJoiner) AcceptsAggregateType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeQ4Agg
}

// Cleanup releases resources
// Note: We keep reference data because multiple EOF batches may arrive from upstream
// Even though users dataset is large, we need it to handle all EOF batches from multiple aggregates
func (j *Q4UserJoiner) Cleanup() error {
	// No-op: we keep reference data to handle multiple EOF batches
	return nil
}

// SerializeState exports the current joiner state as compact JSON
func (j *Q4UserJoiner) SerializeState() ([]byte, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	return json.Marshal(j.state)
}

// RestoreState restores the joiner state from a JSON snapshot
func (j *Q4UserJoiner) RestoreState(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var state Q4UserJoinerState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("decode q4 user joiner snapshot: %w", err)
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	if state.Users != nil {
		j.state.Users = state.Users
	} else {
		j.state.Users = make(map[string]string)
	}

	return nil
}
