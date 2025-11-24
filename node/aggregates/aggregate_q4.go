package aggregates

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

type Q4AggregateState struct {
	// Flat map with composite key: "storeID|userID" -> count
	// Much more memory efficient than nested maps: map[store]map[user]count
	// Avoids duplicate entries and reduces map overhead from O(stores * users) to O(unique pairs)
	Counts map[string]int
}

// Q4Aggregate calculates the top 3 customers per store.
// Input: Q4GroupedRecord (store_id, user_id, transaction_count)
// Output: Q4AggregatedRecord (store_id, user_id, purchases_qty) - top 3 per store
type Q4Aggregate struct {
	mu       sync.RWMutex
	state    *Q4AggregateState
	clientID string
}

// NewQ4Aggregate creates a new Q4Aggregate instance.
func NewQ4Aggregate() *Q4Aggregate {
	return &Q4Aggregate{
		state: &Q4AggregateState{
			Counts: make(map[string]int, 100000), // Pre-allocate for expected pairs
		},
	}
}

// Name returns the name of this aggregate.
func (q *Q4Aggregate) Name() string {
	return "Q4Aggregate"
}

// AccumulateBatch processes a batch of Q4GroupedRecord.
// OPTIMIZATION: Uses flat map with composite key "storeID|userID" -> count
// This consolidates duplicates during accumulation, preventing memory explosion
func (q *Q4Aggregate) AccumulateBatch(records []protocol.Record, batchIndex int) error {

	// Build local map for this batch to minimize lock contention
	localCounts := make(map[string]int, len(records))

	for _, record := range records {
		q4Grouped, ok := record.(*protocol.Q4GroupedRecord)
		if !ok {
			log.Printf("action: q4_invalid_record_type | expected: Q4GroupedRecord | got: %T", record)
			continue // Skip invalid record types
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
			log.Printf("action: q4_invalid_transaction_count | user_id: %s | store_id: %s | transaction_count: %s",
				q4Grouped.UserID, q4Grouped.StoreID, q4Grouped.TransactionCount)
			continue // Skip invalid counts
		}

		// Create composite key: "storeID|userID"
		key := q4Grouped.StoreID + "|" + q4Grouped.UserID
		localCounts[key] += transactionCount
	}

	// Empty batches are valid - just skip
	if len(localCounts) == 0 {
		return nil
	}

	// Merge local map into global state with lock
	q.mu.Lock()
	for key, count := range localCounts {
		q.state.Counts[key] += count
	}
	q.mu.Unlock()

	return nil
}

func (q *Q4Aggregate) Finalize(clientID string) ([]protocol.Record, error) {

	if q.clientID == "" {
		q.clientID = clientID
	}

	log.Printf("action: q4_finalize_start | client_id: %s | unique_pairs: %d", clientID, len(q.state.Counts))

	// Group by store: parse composite keys "storeID|userID"
	storeUserCounts := make(map[string]map[string]int)

	for compositeKey, count := range q.state.Counts {
		// Parse composite key: "storeID|userID"
		parts := strings.SplitN(compositeKey, "|", 2)
		if len(parts) != 2 {
			log.Printf("action: q4_invalid_composite_key | key: %s", compositeKey)
			continue
		}

		storeID := parts[0]
		userID := parts[1]

		if storeUserCounts[storeID] == nil {
			storeUserCounts[storeID] = make(map[string]int)
		}
		storeUserCounts[storeID][userID] = count
	}

	log.Printf("action: q4_grouping_complete | client_id: %s | stores: %d", clientID, len(storeUserCounts))

	var results []protocol.Record

	// For each store, get top 3 customers
	for storeId, userCounts := range storeUserCounts {
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

	log.Printf("action: q4_finalize_complete | client_id: %s | total_results: %d", clientID, len(results))

	return results, nil
}

// GetBatchesToPublish performs full finalization with append-only buffer merge
// Implements the RecordAggregate interface
// historicalIncrements: All persisted incremental snapshots (loaded from disk during finalize)
func (q *Q4Aggregate) GetBatchesToPublish(historicalIncrements [][]byte, batchIndex int, clientID string) ([]BatchToPublish, error) {

	// APPEND-ONLY BUFFER STRATEGY:
	// Merge all historical increments (data that was persisted and cleared from memory)
	// Then combine with current in-memory buffer to get complete dataset
	if len(historicalIncrements) > 0 {
		log.Printf("action: q4_merge_start | client_id: %s | increments: %d",
			clientID, len(historicalIncrements))

		for i, incrementData := range historicalIncrements {
			if len(incrementData) > 0 {
				if err := q.RestoreState(incrementData); err != nil {
					log.Printf("action: q4_merge_increment | client_id: %s | increment: %d | result: fail | error: %v",
						clientID, i, err)
					// Continue with other increments
				}
			}
		}

		log.Printf("action: q4_merge_complete | client_id: %s | increments_merged: %d | total_unique_pairs: %d",
			clientID, len(historicalIncrements), len(q.state.Counts))
	}

	cfg := common.GetConfig()
	joinersCount := cfg.GetQ4JoinersCount()

	results, err := q.Finalize(clientID)
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

		batch := protocol.NewAggregateBatch(batchIndex, records, clientID, true)

		batchesToPublish = append(batchesToPublish, BatchToPublish{
			Batch:      batch,
			RoutingKey: routingKey,
		})

		log.Printf("action: q4_create_batch_to_publish | partition: %d | routing_key: %s | records: %d",
			partition, routingKey, len(records))
	}

	return batchesToPublish, nil
}

// ClearBuffer clears the in-memory buffer after successful persistence
// This frees memory while keeping historical data on disk
func (a *Q4Aggregate) ClearBuffer() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Clear the counts map to free memory
	a.state.Counts = make(map[string]int)

	return nil
}

// Cleanup releases all resources held by this aggregate
func (a *Q4Aggregate) Cleanup() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.state.Counts = nil
	a.state = nil

	return nil
}

// Format: composite_key|count\n where composite_key is "storeID|userID"
// Example: "store1|user123|42\n"
// ATOMIC SNAPSHOT: Takes snapshot of current data, clears buffer, then serializes
func (a *Q4Aggregate) SerializeState() ([]byte, error) {
	// CRITICAL: Use WRITE lock and clear buffer atomically with snapshot
	a.mu.Lock()

	// Make snapshot copy of current map
	snapshotCounts := make(map[string]int, len(a.state.Counts))
	for k, v := range a.state.Counts {
		snapshotCounts[k] = v
	}

	// ATOMICALLY clear the buffer (new data can now accumulate during serialization)
	a.state.Counts = make(map[string]int, 100000)

	a.mu.Unlock()

	// Serialize the snapshot WITHOUT holding the lock (can take time)
	var buf bytes.Buffer
	buf.Grow(len(snapshotCounts) * 50) // Pre-allocate

	for compositeKey, count := range snapshotCounts {
		// compositeKey is already "storeID|userID", just append count
		buf.WriteString(compositeKey)
		buf.WriteByte('|')
		buf.WriteString(strconv.Itoa(count))
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

func (a *Q4Aggregate) RestoreState(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Initialize map if not already created
	if a.state.Counts == nil {
		a.state.Counts = make(map[string]int, 100000)
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))
	lineNum := 0
	restoredCount := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		if len(line) == 0 {
			continue
		}

		// Format: "storeID|userID|count"
		parts := strings.Split(line, "|")
		if len(parts) != 3 {
			log.Printf("action: q4_restore_skip_invalid_line | line: %d | parts: %d", lineNum, len(parts))
			continue
		}

		storeID := parts[0]
		userID := parts[1]
		count, err := strconv.Atoi(parts[2])
		if err != nil {
			log.Printf("action: q4_restore_skip_invalid_count | line: %d | count: %s | error: %v",
				lineNum, parts[2], err)
			continue
		}

		// Reconstruct composite key
		compositeKey := storeID + "|" + userID

		// MERGE: Add to existing count (supports append-only persistence)
		// If key doesn't exist, it starts at 0
		a.state.Counts[compositeKey] += count
		restoredCount++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan q4 aggregate snapshot: %w", err)
	}

	return nil
}
