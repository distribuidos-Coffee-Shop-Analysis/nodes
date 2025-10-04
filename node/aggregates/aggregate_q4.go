package aggregates

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q4Aggregate calculates the top 3 customers per store.
// Input: Q4GroupedRecord (store_id, user_id, transaction_count)
// Output: Q4AggregatedRecord (store_id, user_id, purchases_qty) - top 3 per store
type Q4Aggregate struct {
	mu sync.RWMutex

	// Map from store_id to map[user_id]transaction_count
	storeUserCounts map[string]map[string]int
	// Track unique batch indices
	seenBatchIndices map[int]bool // track which batch indices we've seen
	uniqueBatchCount atomic.Int32 // count of unique batch indices
}

// NewQ4Aggregate creates a new Q4Aggregate instance.
func NewQ4Aggregate() *Q4Aggregate {
	return &Q4Aggregate{
		storeUserCounts:  make(map[string]map[string]int),
		seenBatchIndices: make(map[int]bool),
		uniqueBatchCount: atomic.Int32{},
	}
}

// Name returns the name of this aggregate.
func (q *Q4Aggregate) Name() string {
	return "Q4Aggregate"
}

// AccumulateBatch processes a batch of Q4GroupedRecord.
func (q *Q4Aggregate) AccumulateBatch(records []protocol.Record, batchIndex int) error {
	// Only count as one batch per batchIndex
	// We'll track seen batch indices to avoid double counting
	q.trackBatchIndex(batchIndex)

	// Process records locally without lock, then we merge into shared map
	// Structure: store_id -> user_id -> transaction_count
	localStoreUserCounts := make(map[string]map[string]int)

	for _, record := range records {
		q4Grouped, ok := record.(*protocol.Q4GroupedRecord)
		if !ok {
			return fmt.Errorf("expected Q4GroupedRecord, got %T", record)
		}

		// Skip records with invalid UserID or StoreID
		if q4Grouped.UserID == "" || q4Grouped.StoreID == "" {
			log.Printf("action: q4_skip_invalid_record | user_id: '%s' | store_id: '%s'",
				q4Grouped.UserID, q4Grouped.StoreID)
			continue
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
		}
	}

	return nil
}

func (a *Q4Aggregate) trackBatchIndex(batchIndex int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.seenBatchIndices[batchIndex] {
		a.seenBatchIndices[batchIndex] = true
		a.uniqueBatchCount.Add(1)
	}
}

// GetAccumulatedBatchCount returns the number of batches received so far
func (a *Q4Aggregate) GetAccumulatedBatchCount() int {
	return int(a.uniqueBatchCount.Load())
}

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
			// Skip users with empty/invalid ID as a safety check
			if userId == "" {
				log.Printf("action: q4_skip_empty_user | store_id: %s", storeId)
				continue
			}

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
		}
	}

	log.Printf("action: q4_finalize_complete | total_results: %d", len(results))

	return results, nil
}

// GetBatchesToPublish returns batches partitioned by user_id for distributed join
// Implements the RecordAggregate interface
func (q *Q4Aggregate) GetBatchesToPublish(batchIndex int) ([]BatchToPublish, error) {

	cfg := common.GetConfig()
	joinersCount := cfg.GetQ4JoinersCount()

	results, err := q.Finalize()
	if err != nil {
		return nil, err
	}

	partitionedRecords := make(map[int][]protocol.Record)

	for _, record := range results {
		q4Record, ok := record.(*protocol.Q4AggregatedRecord)
		if !ok {
			return nil, fmt.Errorf("expected Q4AggregatedRecord, got %T", record)
		}

		// Normalize UserID to remove ".0" suffix for consistent hashing
		normalizedUserID := common.NormalizeUserID(q4Record.UserID)

		partition := common.GetJoinerPartition(normalizedUserID, joinersCount)

		if partitionedRecords[partition] == nil {
			partitionedRecords[partition] = make([]protocol.Record, 0)
		}
		partitionedRecords[partition] = append(partitionedRecords[partition], record)
	}

	log.Printf("action: q4_partitioned_results | partitions: %d | total_records: %d",
		len(partitionedRecords), len(results))

	var batchesToPublish []BatchToPublish

	for partition, records := range partitionedRecords {
		routingKey := fmt.Sprintf("joiner.%d.q4_agg", partition)

		batch := protocol.NewAggregateBatch(batchIndex, records, true)

		batchesToPublish = append(batchesToPublish, BatchToPublish{
			Batch:      batch,
			RoutingKey: routingKey,
		})

		log.Printf("action: q4_create_batch_to_publish | partition: %d | routing_key: %s | records: %d",
			partition, routingKey, len(records))
	}

	return batchesToPublish, nil
}
