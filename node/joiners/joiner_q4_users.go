package joiners

import (
	"fmt"
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q4UserJoiner handles joining Q4 aggregate data with user information (birthdate)
type Q4UserJoiner struct {
	// In-memory storage for users (reference data) - only user_id and birthdate
	// Optimized to reduce memory footprint since users dataset is very large
	users map[string]string // key: user_id, value: birthdate
	mu    sync.RWMutex      // mutex for thread-safe access
}

// NewQ4UserJoiner creates a new Q4UserJoiner instance
func NewQ4UserJoiner() *Q4UserJoiner {
	return &Q4UserJoiner{
		users: make(map[string]string),
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

	log.Printf("action: q4_user_store_reference_data | count: %d", len(records))

	for _, record := range records {
		userRecord, ok := record.(*protocol.UserRecord)
		if !ok {
			return fmt.Errorf("expected UserRecord, got %T", record)
		}

		// Store only birthdate (user_id is the key) to reduce memory footprint
		j.users[userRecord.UserID] = userRecord.Birthdate
		log.Printf("action: q4_user_stored | user_id: %s | birthdate: %s",
			userRecord.UserID, userRecord.Birthdate)
	}

	log.Printf("action: q4_user_reference_data_stored | total_users: %d", len(j.users))
	return nil
}

// PerformJoin joins Q4 aggregated data with stored user information
func (j *Q4UserJoiner) PerformJoin(aggregatedRecords []protocol.Record) ([]protocol.Record, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	var joinedRecords []protocol.Record

	for _, record := range aggregatedRecords {
		aggRecord, ok := record.(*protocol.Q4AggregatedRecord)
		if !ok {
			return nil, fmt.Errorf("expected Q4AggregatedRecord, got %T", record)
		}

		// Join aggregated data with user information (lookup birthdate)
		birthdate, exists := j.users[aggRecord.UserID]
		if !exists {
			log.Printf("action: q4_user_join_warning | user_id: %s | error: user_not_found", aggRecord.UserID)
			continue // Skip records without matching users
		}

		joinedRecord := &protocol.Q4JoinedWithUserRecord{
			StoreID:      aggRecord.StoreID,
			UserID:       aggRecord.UserID,
			PurchasesQty: aggRecord.PurchasesQty,
			Birthdate:    birthdate,
		}
		joinedRecords = append(joinedRecords, joinedRecord)

		log.Printf("action: q4_user_join_success | store_id: %s | user_id: %s | purchases_qty: %s | birthdate: %s",
			aggRecord.StoreID, aggRecord.UserID, aggRecord.PurchasesQty, birthdate)
	}

	log.Printf("action: q4_user_join_complete | total_joined: %d", len(joinedRecords))

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
