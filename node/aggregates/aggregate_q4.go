package aggregates

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q4Aggregate calculates the top 3 customers per store.
// Input: Q4GroupedRecord (store_id, user_id, transaction_count)
// Output: Q4AggregatedRecord (store_id, user_id, purchases_qty) - top 3 per store
type Q4Aggregate struct {
	mu sync.RWMutex

	// Map from store_id to map[user_id]transaction_count
	storeUserCounts map[string]map[string]int
	// Track number of batches received
	batchesReceived atomic.Int32 // Lock is not needed for atomic operations
}

// NewQ4Aggregate creates a new Q4Aggregate instance.
func NewQ4Aggregate() *Q4Aggregate {
	return &Q4Aggregate{
		storeUserCounts: make(map[string]map[string]int),
		batchesReceived: atomic.Int32{},
	}
}

// Name returns the name of this aggregate.
func (q *Q4Aggregate) Name() string {
	return "Q4Aggregate"
}

// AccumulateBatch processes a batch of Q4GroupedRecord.
func (q *Q4Aggregate) AccumulateBatch(records []protocol.Record, batchIndex int) error {
	log.Printf("action: q4_accumulate_batch | batch_index: %d | records: %d",
		batchIndex, len(records))

	// Process records locally without lock, then we merge into shared map
	// Structure: store_id -> user_id -> transaction_count
	localStoreUserCounts := make(map[string]map[string]int)

	for _, record := range records {
		q4Grouped, ok := record.(*protocol.Q4GroupedRecord)
		if !ok {
			return fmt.Errorf("expected Q4GroupedRecord, got %T", record)
		}

		// Parse transaction count from string
		transactionCount, err := strconv.Atoi(q4Grouped.TransactionCount)
		if err != nil {
			return fmt.Errorf("invalid transaction count '%s': %w", q4Grouped.TransactionCount, err)
		}

		// Initialize store map if needed (in local map)
		if localStoreUserCounts[q4Grouped.StoreID] == nil {
			localStoreUserCounts[q4Grouped.StoreID] = make(map[string]int)
		}

		// Accumulate transaction count in local map
		localStoreUserCounts[q4Grouped.StoreID][q4Grouped.UserID] += transactionCount
	}

	// Only lock for the final merge into shared map
	q.mu.Lock()
	defer q.mu.Unlock()

	for storeID, userCounts := range localStoreUserCounts {
		// Initialize store map if needed (in shared map)
		if q.storeUserCounts[storeID] == nil {
			q.storeUserCounts[storeID] = make(map[string]int)
		}

		// Merge user counts from local map to shared map
		for userID, count := range userCounts {
			q.storeUserCounts[storeID][userID] += count

			log.Printf("action: q4_accumulate_customer | store: %s | user: %s | batch_transactions: %d | total_transactions: %d",
				storeID, userID, count, q.storeUserCounts[storeID][userID])
		}
	}

	// Increment batch counter (atomic, thread-safe without lock)
	q.batchesReceived.Add(1)

	return nil
}

// GetAccumulatedBatchCount returns the number of batches received so far
func (a *Q4Aggregate) GetAccumulatedBatchCount() int {
	return int(a.batchesReceived.Load()) // No lock needed for atomic read
}

// Finalize generates the top 3 customers per store.
func (q *Q4Aggregate) Finalize() ([]protocol.Record, error) {
	log.Printf("action: q4_finalize_start | stores: %d", len(q.storeUserCounts))

	var results []protocol.Record

	// For each store, get top 3 customers
	for storeId, userCounts := range q.storeUserCounts {
		// Create slice of customers for this store
		type Customer struct {
			UserId           string
			TransactionCount int
		}

		var customers []Customer
		for userId, count := range userCounts {
			customers = append(customers, Customer{
				UserId:           userId,
				TransactionCount: count,
			})
		}

		// Sort by transaction count (descending)
		sort.Slice(customers, func(i, j int) bool {
			return customers[i].TransactionCount > customers[j].TransactionCount
		})

		// Take top 3
		top3Count := len(customers)
		if top3Count > 3 {
			top3Count = 3
		}

		for i := 0; i < top3Count; i++ {
			customer := customers[i]
			q4Agg := &protocol.Q4AggregatedRecord{
				StoreID:      storeId,
				UserID:       customer.UserId,
				PurchasesQty: strconv.Itoa(customer.TransactionCount),
			}
			results = append(results, q4Agg)

			log.Printf("action: q4_top_customer | store: %s | user: %s | transactions: %d | rank: %d",
				storeId, customer.UserId, customer.TransactionCount, i+1)
		}
	}

	log.Printf("action: q4_finalize_complete | total_results: %d", len(results))

	return results, nil
}
