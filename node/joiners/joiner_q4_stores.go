package joiners

import (
	"fmt"
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

type Q4StoreJoinerState struct {
	Stores map[string]*protocol.StoreRecord // key: store_id, value: store record
}

// Q4StoreJoiner handles the second join in Q4: joining user-joined data with store names
type Q4StoreJoiner struct {
	state       *Q4StoreJoinerState
	mu          sync.RWMutex
	persistence *common.StatePersistence
	clientID    string
}

// NewQ4StoreJoiner creates a new Q4StoreJoiner instance
func NewQ4StoreJoiner() *Q4StoreJoiner {
	return NewQ4StoreJoinerWithPersistence("/app/state")
}

func NewQ4StoreJoinerWithPersistence(stateDir string) *Q4StoreJoiner {
	persistence, err := common.NewStatePersistence(stateDir)
	if err != nil {
		log.Printf("action: q4_store_joiner_init | result: fail | error: %v | fallback: memory_only", err)
		persistence = nil
	}

	return &Q4StoreJoiner{
		state: &Q4StoreJoinerState{
			Stores: make(map[string]*protocol.StoreRecord),
		},
		persistence: persistence,
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

	log.Printf("action: q4_store_store_reference_data | count: %d", len(records))

	for _, record := range records {
		storeRecord, ok := record.(*protocol.StoreRecord)
		if !ok {
			return fmt.Errorf("expected StoreRecord, got %T", record)
		}

		j.state.Stores[storeRecord.StoreID] = storeRecord
		log.Printf("action: q4_store_stored | store_id: %s | store_name: %s",
			storeRecord.StoreID, storeRecord.StoreName)
	}

	log.Printf("action: q4_store_reference_data_stored | total_stores: %d", len(j.state.Stores))

	if j.persistence != nil && j.clientID != "" {
		if err := j.persistence.SaveState(j.Name(), j.clientID, j.state); err != nil {
			log.Printf("action: q4_store_joiner_save_state | result: fail | client_id: %s | error: %v",
				j.clientID, err)
		}
	}

	return nil
}

// PerformJoin joins Q4 user-joined data with stored store information to produce final output
func (j *Q4StoreJoiner) PerformJoin(userJoinedRecords []protocol.Record, clientId string) ([]protocol.Record, error) {

	if j.clientID == "" {
		j.clientID = clientId

		if j.persistence != nil {
			var savedState Q4StoreJoinerState
			if err := j.persistence.LoadState(j.Name(), clientId, &savedState); err != nil {
				log.Printf("action: q4_store_joiner_load_state | result: fail | client_id: %s | error: %v",
					clientId, err)
			} else if savedState.Stores != nil {
				j.mu.Lock()
				for key, store := range savedState.Stores {
					j.state.Stores[key] = store
				}
				j.mu.Unlock()
				log.Printf("action: q4_store_joiner_load_state | result: success | client_id: %s | stores: %d",
					clientId, len(savedState.Stores))
			}
		}
	}

	j.mu.RLock()
	defer j.mu.RUnlock()

	var joinedRecords []protocol.Record

	for _, record := range userJoinedRecords {
		userJoinedRecord, ok := record.(*protocol.Q4JoinedWithUserRecord)
		if !ok {
			return nil, fmt.Errorf("expected Q4JoinedWithUserRecord, got %T", record)
		}

		// Join with store information
		storeRecord, exists := j.state.Stores[userJoinedRecord.StoreID]
		if !exists {
			continue // Skip records without matching stores
		}

		// Create final record with store_name, purchases_qty, and birthdate
		finalRecord := &protocol.Q4JoinedWithStoreAndUserRecord{
			StoreName:    storeRecord.StoreName,
			PurchasesQty: userJoinedRecord.PurchasesQty,
			Birthdate:    userJoinedRecord.Birthdate,
		}
		joinedRecords = append(joinedRecords, finalRecord)
	}

	log.Printf("action: q4_store_join_complete | client_id: %s | input: %d | joined: %d",
		clientId, len(userJoinedRecords), len(joinedRecords))

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

// Cleanup releases resources and deletes state file
func (j *Q4StoreJoiner) Cleanup() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.persistence != nil && j.clientID != "" {
		if err := j.persistence.DeleteState(j.Name(), j.clientID); err != nil {
			log.Printf("action: q4_store_joiner_delete_state | result: fail | client_id: %s | error: %v",
				j.clientID, err)
		}
	}

	j.state.Stores = nil
	j.state = nil

	return nil
}
