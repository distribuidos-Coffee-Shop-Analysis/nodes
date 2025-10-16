package joiners

import (
	"fmt"
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q3Joiner handles joining Q3 aggregate data with store names
type Q3Joiner struct {
	// In-memory storage for stores (reference data)
	stores map[string]*protocol.StoreRecord // key: store_id, value: store record
	mu     sync.RWMutex                     // mutex for thread-safe access
}

// NewQ3Joiner creates a new Q3Joiner instance
func NewQ3Joiner() *Q3Joiner {
	return &Q3Joiner{
		stores: make(map[string]*protocol.StoreRecord),
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

		j.stores[storeRecord.StoreID] = storeRecord
		log.Printf("action: q3_store_stored | store_id: %s | store_name: %s",
			storeRecord.StoreID, storeRecord.StoreName)
	}

	log.Printf("action: q3_reference_data_stored | total_stores: %d", len(j.stores))
	return nil
}

// PerformJoin joins Q3 aggregated data with stored store information
func (j *Q3Joiner) PerformJoin(aggregatedRecords []protocol.Record, clientId string) ([]protocol.Record, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	var joinedRecords []protocol.Record

	for _, record := range aggregatedRecords {
		aggRecord, ok := record.(*protocol.Q3AggregatedRecord)
		if !ok {
			return nil, fmt.Errorf("expected Q3AggregatedRecord, got %T", record)
		}

		// Join aggregated data with store names
		storeRecord, exists := j.stores[aggRecord.StoreID]
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

// Cleanup releases all resources held by this joiner
func (j *Q3Joiner) Cleanup() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	// Clear stores map to release memory
	j.stores = nil

	return nil
}
