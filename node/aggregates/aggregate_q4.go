package aggregates

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	worker "github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q4Aggregate handles aggregation of Q4 grouped data
// State is only used during Finalize to merge all increments
type Q4Aggregate struct {
	counts   map[string]int // "storeID|userID" -> count
	clientID string

	// In-memory cache of serialized increments to avoid disk reads for already-processed batches
	cachedIncrements map[int][]byte
}

func NewQ4Aggregate() *Q4Aggregate {
	return &Q4Aggregate{
		counts:           make(map[string]int, 100000),
		cachedIncrements: make(map[int][]byte),
	}
}

// CacheIncrement stores a serialized increment in memory to avoid disk reads during finalize
func (q *Q4Aggregate) CacheIncrement(batchIndex int, data []byte) {
	if q.cachedIncrements == nil {
		q.cachedIncrements = make(map[int][]byte)
	}
	q.cachedIncrements[batchIndex] = data
}

// GetCachedBatchIndices returns the set of batch indices currently in memory cache
func (q *Q4Aggregate) GetCachedBatchIndices() map[int]bool {
	result := make(map[int]bool, len(q.cachedIncrements))
	for idx := range q.cachedIncrements {
		result[idx] = true
	}
	return result
}

func (q *Q4Aggregate) Name() string {
	return "Q4Aggregate"
}

// SerializeRecords serializes records with batch index as header
// Format: BATCH|index\n followed by storeID|userID|count\n
// Always returns at least the header to ensure batch is tracked for crash recovery
func (q *Q4Aggregate) SerializeRecords(records []protocol.Record, batchIndex int) ([]byte, error) {
	// Aggregate locally to reduce output size
	localCounts := make(map[string]int, len(records))

	for _, record := range records {
		q4Grouped, ok := record.(*protocol.Q4GroupedRecord)
		if !ok {
			log.Printf("action: q4_serialize_invalid_record | expected: Q4GroupedRecord | got: %T", record)
			continue
		}

		if q4Grouped.UserID == "" || q4Grouped.StoreID == "" {
			continue
		}

		transactionCount, err := strconv.Atoi(q4Grouped.TransactionCount)
		if err != nil {
			continue
		}

		key := q4Grouped.StoreID + "|" + q4Grouped.UserID
		localCounts[key] += transactionCount
	}

	var buf bytes.Buffer
	buf.Grow(len(localCounts)*50 + 20)
	buf.WriteString("BATCH|")
	buf.WriteString(strconv.Itoa(batchIndex))
	buf.WriteByte('\n')

	for compositeKey, count := range localCounts {
		buf.WriteString(compositeKey)
		buf.WriteByte('|')
		buf.WriteString(strconv.Itoa(count))
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

// restoreState restores data from a serialized increment (used during merge)
func (q *Q4Aggregate) restoreState(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if q.counts == nil {
		q.counts = make(map[string]int, 100000)
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) != 3 {
			continue
		}

		storeID := parts[0]
		userID := parts[1]
		count, err := strconv.Atoi(parts[2])
		if err != nil {
			continue
		}

		compositeKey := storeID + "|" + userID
		q.counts[compositeKey] += count
	}

	return scanner.Err()
}

func (q *Q4Aggregate) Finalize(clientID string) ([]protocol.Record, error) {
	if q.clientID == "" {
		q.clientID = clientID
	}

	log.Printf("action: q4_finalize_start | client_id: %s | unique_pairs: %d", clientID, len(q.counts))

	storeUserCounts := make(map[string]map[string]int)

	for compositeKey, count := range q.counts {
		parts := strings.SplitN(compositeKey, "|", 2)
		if len(parts) != 2 {
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

	for storeId, userCounts := range storeUserCounts {
		type Customer struct {
			UserId           string
			TransactionCount int
		}

		var customers []Customer
		for userId, count := range userCounts {
			if userId == "" {
				continue
			}
			customers = append(customers, Customer{
				UserId:           userId,
				TransactionCount: count,
			})
		}

		sort.Slice(customers, func(i, j int) bool {
			return customers[i].TransactionCount > customers[j].TransactionCount
		})

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

// parseBatchIndexFromIncrement extracts the batch index from the header of an increment
func parseBatchIndexFromIncrementQ4(data []byte) int {
	if len(data) == 0 {
		return -1
	}

	newlineIdx := bytes.IndexByte(data, '\n')
	if newlineIdx == -1 {
		return -1
	}

	header := string(data[:newlineIdx])
	if !strings.HasPrefix(header, "BATCH|") {
		return -1
	}

	batchIndexStr := header[6:]
	batchIndex, err := strconv.Atoi(batchIndexStr)
	if err != nil {
		return -1
	}

	return batchIndex
}

// GetBatchesToPublish merges cached + disk increments, filters duplicates, and returns final results.
// Cached increments (from current session) are used first, disk is only read for missing batches (post-crash recovery).
func (q *Q4Aggregate) GetBatchesToPublish(historicalIncrements [][]byte, batchIndex int, clientID string) ([]BatchToPublish, error) {
	// Collect all increments: cached first, then disk (for recovery)
	seenBatches := make(map[int]bool)
	var validIncrements [][]byte
	cachedUsed := 0
	diskUsed := 0
	duplicatesSkipped := 0

	// Phase 1a: Use cached increments first (already in memory, no disk read needed)
	for batchIdx, data := range q.cachedIncrements {
		if len(data) == 0 {
			continue
		}
		seenBatches[batchIdx] = true
		validIncrements = append(validIncrements, data)
		cachedUsed++
	}

	// Phase 1b: Add disk increments only for batches not in cache (recovery scenario)
	for i, incrementData := range historicalIncrements {
		if len(incrementData) == 0 {
			continue
		}

		batchIdx := parseBatchIndexFromIncrementQ4(incrementData)
		if batchIdx == -1 {
			log.Printf("action: q4_merge_skip_invalid | client_id: %s | increment: %d | reason: no_batch_header",
				clientID, i)
			continue
		}

		if seenBatches[batchIdx] {
			duplicatesSkipped++
			continue
		}
		seenBatches[batchIdx] = true
		validIncrements = append(validIncrements, incrementData)
		diskUsed++
	}

	log.Printf("action: q4_merge_start | client_id: %s | cached: %d | from_disk: %d | duplicates_skipped: %d",
		clientID, cachedUsed, diskUsed, duplicatesSkipped)

	// Phase 2: Process valid increments in parallel
	if len(validIncrements) > 0 {
		q.mergeIncrementsParallel(validIncrements)
	}

	log.Printf("action: q4_merge_complete | client_id: %s | total_merged: %d | unique_pairs: %d",
		clientID, len(validIncrements), len(q.counts))

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
	}

	return batchesToPublish, nil
}

// mergeIncrementsParallel processes increments using the common worker pool
func (q *Q4Aggregate) mergeIncrementsParallel(increments [][]byte) {
	worker.ProcessAndMerge(
		increments,
		0, // Use default workers
		parseQ4Increment,
		func(results []map[string]int) {
			for _, localCounts := range results {
				for key, count := range localCounts {
					q.counts[key] += count
				}
			}
		},
	)
}

// parseQ4Increment parses a single increment into a counts map
func parseQ4Increment(data []byte) map[string]int {
	localCounts := make(map[string]int)

	if len(data) == 0 {
		return localCounts
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) != 3 {
			continue
		}

		storeID := parts[0]
		userID := parts[1]
		count, err := strconv.Atoi(parts[2])
		if err != nil {
			continue
		}

		compositeKey := storeID + "|" + userID
		localCounts[compositeKey] += count
	}

	return localCounts
}

func (q *Q4Aggregate) Cleanup() error {
	q.counts = nil
	q.cachedIncrements = nil
	return nil
}
