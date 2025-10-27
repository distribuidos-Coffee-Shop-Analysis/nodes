package joiners

import (
	"fmt"
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

type Q3JoinerState struct {
	Stores map[string]*protocol.StoreRecord // key: store_id, value: store record
}

// Q3Joiner handles joining Q3 aggregate data with store names
type Q3Joiner struct {
	state       *Q3JoinerState
	mu          sync.RWMutex
	persistence *common.StatePersistence
	clientID    string
}

// NewQ3Joiner creates a new Q3Joiner instance
func NewQ3Joiner() *Q3Joiner {
	return NewQ3JoinerWithPersistence("/app/state")
}

func NewQ3JoinerWithPersistence(stateDir string) *Q3Joiner {
	persistence, err := common.NewStatePersistence(stateDir)
	if err != nil {
		log.Printf("action: q3_joiner_init | result: fail | error: %v | fallback: memory_only", err)
		persistence = nil
	}

	return &Q3Joiner{
		state: &Q3JoinerState{
			Stores: make(map[string]*protocol.StoreRecord),
		},
		persistence: persistence,
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

	log.Printf("action: q3_store_reference_data | count: %d", len(records))

	for _, record := range records {
		storeRecord, ok := record.(*protocol.StoreRecord)
		if !ok {
			return fmt.Errorf("expected StoreRecord, got %T", record)
		}

		j.state.Stores[storeRecord.StoreID] = storeRecord
		log.Printf("action: q3_store_stored | store_id: %s | store_name: %s",
			storeRecord.StoreID, storeRecord.StoreName)
	}

	log.Printf("action: q3_reference_data_stored | total_stores: %d", len(j.state.Stores))

	if j.persistence != nil && j.clientID != "" {
		if err := j.persistence.SaveState(j.Name(), j.clientID, j.state); err != nil {
			log.Printf("action: q3_joiner_save_state | result: fail | client_id: %s | error: %v",
				j.clientID, err)
		}
	}

	return nil
}

// PerformJoin joins Q3 aggregated data with stored store information
func (j *Q3Joiner) PerformJoin(aggregatedRecords []protocol.Record, clientId string) ([]protocol.Record, error) {

	if j.clientID == "" {
		j.clientID = clientId

		if j.persistence != nil {
			var savedState Q3JoinerState
			if err := j.persistence.LoadState(j.Name(), clientId, &savedState); err != nil {
				log.Printf("action: q3_joiner_load_state | result: fail | client_id: %s | error: %v",
					clientId, err)
			} else if savedState.Stores != nil {
				j.mu.Lock()
				for key, store := range savedState.Stores {
					j.state.Stores[key] = store
				}
				j.mu.Unlock()
				log.Printf("action: q3_joiner_load_state | result: success | client_id: %s | stores: %d",
					clientId, len(savedState.Stores))
			}
		}
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
			log.Printf("action: q3_join_warning | client_id: %s | store_id: %s | error: store_not_found", clientId, aggRecord.StoreID)
			continue // Skip records without matching stores
		}

		joinedRecord := &protocol.Q3JoinedRecord{
			YearHalf:  aggRecord.YearHalf,
			StoreName: storeRecord.StoreName,
			TPV:       aggRecord.TPV,
		}
		joinedRecords = append(joinedRecords, joinedRecord)

		log.Printf("action: q3_join_success | client_id: %s | year_half: %s | store_id: %s | store_name: %s | tpv: %s",
			clientId, aggRecord.YearHalf, aggRecord.StoreID, storeRecord.StoreName, aggRecord.TPV)
	}

	log.Printf("action: q3_join_complete | client_id: %s | total_joined: %d", clientId, len(joinedRecords))

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

// Cleanup releases resources and deletes state file
func (j *Q3Joiner) Cleanup() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.persistence != nil && j.clientID != "" {
		if err := j.persistence.DeleteState(j.Name(), j.clientID); err != nil {
			log.Printf("action: q3_joiner_delete_state | result: fail | client_id: %s | error: %v",
				j.clientID, err)
		}
	}

	j.state.Stores = nil
	j.state = nil

	return nil
}
