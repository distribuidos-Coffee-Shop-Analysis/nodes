package joiners

import (
	"fmt"
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q4StoreJoiner handles the second join in Q4: joining user-joined data with store names
type Q4StoreJoiner struct {
	// In-memory storage for stores (reference data)
	stores map[string]*protocol.StoreRecord // key: store_id, value: store record
	mu     sync.RWMutex                     // mutex for thread-safe access
}

// NewQ4StoreJoiner creates a new Q4StoreJoiner instance
func NewQ4StoreJoiner() *Q4StoreJoiner {
	return &Q4StoreJoiner{
		stores: make(map[string]*protocol.StoreRecord),
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

		j.stores[storeRecord.StoreID] = storeRecord
		log.Printf("action: q4_store_stored | store_id: %s | store_name: %s",
			storeRecord.StoreID, storeRecord.StoreName)
	}

	log.Printf("action: q4_store_reference_data_stored | total_stores: %d", len(j.stores))
	return nil
}

// PerformJoin joins Q4 user-joined data with stored store information to produce final output
func (j *Q4StoreJoiner) PerformJoin(userJoinedRecords []protocol.Record, clientId string) ([]protocol.Record, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	var joinedRecords []protocol.Record

	for _, record := range userJoinedRecords {
		userJoinedRecord, ok := record.(*protocol.Q4JoinedWithUserRecord)
		if !ok {
			return nil, fmt.Errorf("expected Q4JoinedWithUserRecord, got %T", record)
		}

		// Join with store information
		storeRecord, exists := j.stores[userJoinedRecord.StoreID]
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
