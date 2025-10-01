package aggregates

import (
	"fmt"
	"log"
	"sort"
	"strconv"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q4Aggregate calculates the top 3 customers per store.
// Input: Q4GroupedRecord (store_id, user_id, transaction_count)
// Output: Q4AggregatedRecord (store_id, user_id, purchases_qty) - top 3 per store
type Q4Aggregate struct {
	// Map from store_id to map[user_id]transaction_count
	storeUserCounts map[string]map[string]int
	eofBatches      map[int]struct{}
	maxBatchIndex   int
	hasReceivedEOF  bool
}

// NewQ4Aggregate creates a new Q4Aggregate instance.
func NewQ4Aggregate() *Q4Aggregate {
	return &Q4Aggregate{
		storeUserCounts: make(map[string]map[string]int),
		eofBatches:      make(map[int]struct{}),
		maxBatchIndex:   -1,
		hasReceivedEOF:  false,
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

		// Initialize store map if needed
		if q.storeUserCounts[q4Grouped.StoreID] == nil {
			q.storeUserCounts[q4Grouped.StoreID] = make(map[string]int)
		}

		// Accumulate transaction count for this user in this store
		q.storeUserCounts[q4Grouped.StoreID][q4Grouped.UserID] += transactionCount

		log.Printf("action: q4_accumulate_customer | store: %s | user: %s | total_transactions: %d",
			q4Grouped.StoreID, q4Grouped.UserID, q.storeUserCounts[q4Grouped.StoreID][q4Grouped.UserID])
	}

	return nil
}

// SetEOF marks that we have received EOF for a specific batch.
func (q *Q4Aggregate) SetEOF(batchIndex int) {
	q.eofBatches[batchIndex] = struct{}{}
	if batchIndex > q.maxBatchIndex {
		q.maxBatchIndex = batchIndex
	}
	q.hasReceivedEOF = true

	log.Printf("action: q4_set_eof | batch_index: %d | max_batch_index: %d",
		batchIndex, q.maxBatchIndex)
}

// Finalize generates the top 3 customers per store.
func (q *Q4Aggregate) Finalize() ([]protocol.Record, error) {
	if !q.hasReceivedEOF {
		log.Printf("action: q4_finalize_skip | reason: no_eof_received")
		return nil, nil
	}

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

// IsComplete checks if all expected batches have been received
func (q *Q4Aggregate) IsComplete(maxBatchIndex int) bool {
	if !q.hasReceivedEOF || maxBatchIndex < 0 {
		return false
	}

	// Check if all batches from 0 to maxBatchIndex have been received
	// For this aggregate, we assume completion when we have EOF
	return len(q.eofBatches) > 0 && q.maxBatchIndex >= maxBatchIndex
}
