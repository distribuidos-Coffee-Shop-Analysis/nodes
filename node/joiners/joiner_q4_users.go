package joiners

import (
	"fmt"
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	nodeCommon "github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"	
)

type Q4UserJoinerState struct {
	Users map[string]string // key: user_id, value: birthdate
}

// Q4UserJoiner handles joining Q4 aggregate data with user information (birthdate)
type Q4UserJoiner struct {
	state       *Q4UserJoinerState
	mu          sync.RWMutex
	persistence *nodeCommon.StatePersistence
	clientID    string
}

// NewQ4UserJoiner creates a new Q4UserJoiner instance
func NewQ4UserJoiner() *Q4UserJoiner {
	return NewQ4UserJoinerWithPersistence("/app/state")
}

func NewQ4UserJoinerWithPersistence(stateDir string) *Q4UserJoiner {
	persistence, err := nodeCommon.NewStatePersistence(stateDir)
	if err != nil {
		log.Printf("action: q4_user_joiner_init | result: fail | error: %v | fallback: memory_only", err)
		persistence = nil
	}

	return &Q4UserJoiner{
		state: &Q4UserJoinerState{
			Users: make(map[string]string),
		},
		persistence: persistence,
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

	if j.persistence != nil && j.clientID != "" {
		if err := j.persistence.SaveState(j.Name(), j.clientID, j.state); err != nil {
			log.Printf("action: q4_user_joiner_save_state | result: fail | client_id: %s | error: %v",
				j.clientID, err)
		}
	}

	return nil
}

// PerformJoin joins Q4 aggregated data with stored user information
func (j *Q4UserJoiner) PerformJoin(aggregatedRecords []protocol.Record, clientId string) ([]protocol.Record, error) {

	if j.clientID == "" {
		j.clientID = clientId

		if j.persistence != nil {
			var savedState Q4UserJoinerState
			if err := j.persistence.LoadState(j.Name(), clientId, &savedState); err != nil {
				log.Printf("action: q4_user_joiner_load_state | result: fail | client_id: %s | error: %v",
					clientId, err)
			} else if savedState.Users != nil {
				j.mu.Lock()
				for key, birthdate := range savedState.Users {
					j.state.Users[key] = birthdate
				}
				j.mu.Unlock()
				log.Printf("action: q4_user_joiner_load_state | result: success | client_id: %s | users: %d",
					clientId, len(savedState.Users))
			}
		}
	}

	j.mu.RLock()
	defer j.mu.RUnlock()

	var joinedRecords []protocol.Record

	for _, record := range aggregatedRecords {
		aggRecord, ok := record.(*protocol.Q4AggregatedRecord)
		if !ok {
			return nil, fmt.Errorf("expected Q4AggregatedRecord, got %T", record)
		}

		// Normalize user_id before lookup
		normalizedUserID := common.NormalizeUserID(aggRecord.UserID)

		// Join aggregated data with user information (lookup birthdate)
		birthdate, exists := j.state.Users[normalizedUserID]
		if !exists {
			log.Printf("action: q4_user_join_missing | client_id: %s | user_id: %s | store_id: %s",
				clientId, normalizedUserID, aggRecord.StoreID)
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

	log.Printf("action: q4_user_join_complete | client_id: %s | input: %d | joined: %d",
		clientId, len(aggregatedRecords), len(joinedRecords))

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

// Cleanup releases memory held by the users map and deletes state file
// Users dataset is HUGE (millions of rows, GBs of memory) so this is critical
// Called after EOF is received and all batches are processed
func (j *Q4UserJoiner) Cleanup() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	log.Printf("action: q4_user_joiner_cleanup | users_count: %d | releasing_memory", len(j.state.Users))

	if j.persistence != nil && j.clientID != "" {
		if err := j.persistence.DeleteState(j.Name(), j.clientID); err != nil {
			log.Printf("action: q4_user_joiner_delete_state | result: fail | client_id: %s | error: %v",
				j.clientID, err)
		}
	}

	j.state.Users = nil
	j.state = nil

	log.Printf("action: q4_user_joiner_cleanup | result: success")

	return nil
}
