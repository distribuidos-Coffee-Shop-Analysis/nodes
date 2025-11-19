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
	StoreUserCounts map[string]map[string]int
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
			StoreUserCounts: make(map[string]map[string]int),
		},
	}
}

// Name returns the name of this aggregate.
func (q *Q4Aggregate) Name() string {
	return "Q4Aggregate"
}

// AccumulateBatch processes a batch of Q4GroupedRecord.
func (q *Q4Aggregate) AccumulateBatch(records []protocol.Record, batchIndex int) error {

	// Process records locally without lock, then we merge into shared map
	// Structure: store_id -> user_id -> transaction_count
	localStoreUserCounts := make(map[string]map[string]int)

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

		// Initialize store map if needed (in local map)
		if localStoreUserCounts[q4Grouped.StoreID] == nil {
			localStoreUserCounts[q4Grouped.StoreID] = make(map[string]int)
		}

		// Accumulate transaction count in local map
		localStoreUserCounts[q4Grouped.StoreID][q4Grouped.UserID] += transactionCount
	}

	// Empty batches are valid - just skip the merge
	if len(localStoreUserCounts) == 0 {
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for storeID, userCounts := range localStoreUserCounts {
		if q.state.StoreUserCounts[storeID] == nil {
			q.state.StoreUserCounts[storeID] = make(map[string]int)
		}

		for userID, count := range userCounts {
			q.state.StoreUserCounts[storeID][userID] += count
		}
	}

	return nil
}

func (q *Q4Aggregate) Finalize(clientID string) ([]protocol.Record, error) {

	if q.clientID == "" {
		q.clientID = clientID
	}

	log.Printf("action: q4_finalize_start | client_id: %s | stores: %d", clientID, len(q.state.StoreUserCounts))

	var results []protocol.Record

	// For each store, get top 3 customers
	for storeId, userCounts := range q.state.StoreUserCounts {
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

// GetBatchesToPublish returns batches partitioned by user_id for distributed join
// Implements the RecordAggregate interface
func (q *Q4Aggregate) GetBatchesToPublish(batchIndex int, clientID string) ([]BatchToPublish, error) {

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

// Cleanup releases all resources held by this aggregate
func (a *Q4Aggregate) Cleanup() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.state.StoreUserCounts = nil
	a.state = nil

	return nil
}

// Format: store_id|user_id|count\n for each entry
func (a *Q4Aggregate) SerializeState() ([]byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	totalEntries := 0
	for _, userCounts := range a.state.StoreUserCounts {
		totalEntries += len(userCounts)
	}

	var buf bytes.Buffer
	buf.Grow(totalEntries * 50) // Pre-allocate

	for storeID, userCounts := range a.state.StoreUserCounts {
		for userID, count := range userCounts {
			buf.WriteString(storeID)
			buf.WriteByte('|')
			buf.WriteString(userID)
			buf.WriteByte('|')
			buf.WriteString(strconv.Itoa(count))
			buf.WriteByte('\n')
		}
	}

	return buf.Bytes(), nil
}

func (a *Q4Aggregate) RestoreState(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.state.StoreUserCounts = make(map[string]map[string]int)

	scanner := bufio.NewScanner(bytes.NewReader(data))
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		if len(line) == 0 {
			continue
		}

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

		if a.state.StoreUserCounts[storeID] == nil {
			a.state.StoreUserCounts[storeID] = make(map[string]int)
		}

		a.state.StoreUserCounts[storeID][userID] = count
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan q4 aggregate snapshot: %w", err)
	}

	log.Printf("action: q4_restore_complete | entries_restored: %d | stores: %d",
		lineNum, len(a.state.StoreUserCounts))

	return nil
}
