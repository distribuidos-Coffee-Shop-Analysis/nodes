package aggregates

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"

	worker "github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q2Aggregate handles aggregation of Q2 grouped data to find best selling items and most profitable items per month
type Q2Aggregate struct {
	quantityData map[string]int
	profitData   map[string]float64
	clientID     string

	cachedIncrements map[int][]byte
}

func NewQ2Aggregate() *Q2Aggregate {
	return &Q2Aggregate{
		quantityData:     make(map[string]int),
		profitData:       make(map[string]float64),
		cachedIncrements: make(map[int][]byte),
	}
}

// CacheIncrement stores a serialized increment in memory to avoid disk reads during finalize
func (a *Q2Aggregate) CacheIncrement(batchIndex int, data []byte) {
	if a.cachedIncrements == nil {
		a.cachedIncrements = make(map[int][]byte)
	}
	a.cachedIncrements[batchIndex] = data
}

// GetCachedBatchIndices returns the set of batch indices currently in memory cache
// Only returns indices with non-empty data to avoid excluding batches that would then be skipped
func (a *Q2Aggregate) GetCachedBatchIndices() map[int]bool {
	result := make(map[int]bool, len(a.cachedIncrements))
	for idx, data := range a.cachedIncrements {
		if len(data) > 0 {
			result[idx] = true
		}
	}
	return result
}

func (a *Q2Aggregate) GetCache() map[int][]byte {
	return a.cachedIncrements
}

func (a *Q2Aggregate) ClearCache() {
	a.cachedIncrements = make(map[int][]byte)
}

func (a *Q2Aggregate) Name() string {
	return "q2_aggregate_best_selling_most_profits"
}

// Format: BATCH|index\n followed by Q|year_month|item_id|quantity\n or P|year_month|item_id|profit\n
func (a *Q2Aggregate) SerializeRecords(records []protocol.Record, batchIndex int) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(records)*50 + 20)

	buf.WriteString("BATCH|")
	buf.WriteString(strconv.Itoa(batchIndex))
	buf.WriteByte('\n')

	for _, record := range records {
		switch r := record.(type) {
		case *protocol.Q2GroupWithQuantityRecord:
			quantity, err := strconv.Atoi(r.SellingsQty)
			if err != nil {
				log.Printf("action: q2_serialize_quantity_parse | result: error | "+
					"year_month: %s | item_id: %s | sellings_qty: %s | error: %v",
					r.YearMonth, r.ItemID, r.SellingsQty, err)
				continue
			}
			buf.WriteByte('Q')
			buf.WriteByte('|')
			buf.WriteString(r.YearMonth)
			buf.WriteByte('|')
			buf.WriteString(r.ItemID)
			buf.WriteByte('|')
			buf.WriteString(strconv.Itoa(quantity))
			buf.WriteByte('\n')

		case *protocol.Q2GroupWithSubtotalRecord:
			profit, err := strconv.ParseFloat(r.ProfitSum, 64)
			if err != nil {
				log.Printf("action: q2_serialize_profit_parse | result: error | "+
					"year_month: %s | item_id: %s | profit_sum: %s | error: %v",
					r.YearMonth, r.ItemID, r.ProfitSum, err)
				continue
			}
			buf.WriteByte('P')
			buf.WriteByte('|')
			buf.WriteString(r.YearMonth)
			buf.WriteByte('|')
			buf.WriteString(r.ItemID)
			buf.WriteByte('|')
			buf.WriteString(strconv.FormatFloat(profit, 'f', 2, 64))
			buf.WriteByte('\n')

		default:
			log.Printf("action: q2_serialize_unknown_record | result: warning | record_type: %T", record)
		}
	}

	return buf.Bytes(), nil
}

// Finalize calculates the best selling and most profitable items per year_month
func (a *Q2Aggregate) Finalize(clientID string) ([]protocol.Record, error) {
	if a.clientID == "" {
		a.clientID = clientID
	}

	log.Printf("action: q2_aggregate_finalize_started | client_id: %s | quantity_entries: %d | profit_entries: %d",
		clientID, len(a.quantityData), len(a.profitData))

	quantityByMonth := make(map[string]map[string]int)
	profitByMonth := make(map[string]map[string]float64)

	for key, quantity := range a.quantityData {
		parts := parseAggregateKey(key)
		yearMonth, itemID := parts[0], parts[1]

		if quantityByMonth[yearMonth] == nil {
			quantityByMonth[yearMonth] = make(map[string]int)
		}
		quantityByMonth[yearMonth][itemID] = quantity
	}

	for key, profit := range a.profitData {
		parts := parseAggregateKey(key)
		yearMonth, itemID := parts[0], parts[1]

		if profitByMonth[yearMonth] == nil {
			profitByMonth[yearMonth] = make(map[string]float64)
		}
		profitByMonth[yearMonth][itemID] = profit
	}

	var result []protocol.Record

	for yearMonth, items := range quantityByMonth {
		bestItem, bestQuantity := findBestSellingItem(items)
		if bestItem != "" {
			record := &protocol.Q2BestSellingRecord{
				YearMonth:   yearMonth,
				ItemID:      bestItem,
				SellingsQty: strconv.Itoa(bestQuantity),
			}
			result = append(result, record)
		}
	}

	for yearMonth, items := range profitByMonth {
		bestItem, bestProfit := findMostProfitableItem(items)
		if bestItem != "" {
			record := &protocol.Q2MostProfitsRecord{
				YearMonth: yearMonth,
				ItemID:    bestItem,
				ProfitSum: fmt.Sprintf("%.2f", bestProfit),
			}
			result = append(result, record)
		}
	}

	log.Printf("action: q2_aggregate_finalize_complete | client_id: %s | total_results: %d", clientID, len(result))

	return result, nil
}

// parseAggregateKey splits the composite key "yearMonth|itemID" into parts
func parseAggregateKey(key string) []string {
	parts := make([]string, 2)
	for i := 0; i < len(key); i++ {
		if key[i] == '|' {
			parts[0] = key[:i]
			parts[1] = key[i+1:]
			break
		}
	}
	return parts
}

// findBestSellingItem finds the item with the highest quantity for a given month
func findBestSellingItem(items map[string]int) (string, int) {
	var bestItem string
	var bestQuantity int

	for itemID, quantity := range items {
		if quantity > bestQuantity {
			bestItem = itemID
			bestQuantity = quantity
		}
	}

	return bestItem, bestQuantity
}

// findMostProfitableItem finds the item with the highest profit for a given month
func findMostProfitableItem(items map[string]float64) (string, float64) {
	var bestItem string
	var bestProfit float64

	for itemID, profit := range items {
		if profit > bestProfit {
			bestItem = itemID
			bestProfit = profit
		}
	}

	return bestItem, bestProfit
}

// parseBatchIndexFromIncrement extracts the batch index from the header of an increment
// Returns -1 if the header is not found or invalid
func parseBatchIndexFromIncrement(data []byte) int {
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

type q2WorkerResult struct {
	quantityData map[string]int
	profitData   map[string]float64
}

// GetBatchesToPublish merges cached + disk increments, filters duplicates, and returns final results.
func (a *Q2Aggregate) GetBatchesToPublish(historicalIncrements [][]byte, batchIndex int, clientID string) ([]BatchToPublish, error) {
	seenBatches := make(map[int]bool)
	var validIncrements [][]byte
	cachedUsed := 0
	cachedEmpty := 0
	diskUsed := 0
	diskEmpty := 0
	diskInvalidHeader := 0
	duplicatesSkipped := 0

	// 1. Use cached increments
	for batchIdx, data := range a.cachedIncrements {
		if len(data) == 0 {
			cachedEmpty++
			continue
		}
		seenBatches[batchIdx] = true
		validIncrements = append(validIncrements, data)
		cachedUsed++
	}

	// 2. Add disk increments for batches not in cache
	for i, incrementData := range historicalIncrements {
		if len(incrementData) == 0 {
			diskEmpty++
			continue
		}

		batchIdx := parseBatchIndexFromIncrement(incrementData)
		if batchIdx == -1 {
			diskInvalidHeader++
			log.Printf("action: q2_merge_skip_invalid | client_id: %s | increment: %d | reason: no_batch_header",
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

	log.Printf("action: q2_merge_start | client_id: %s | cached: %d | cached_empty: %d | from_disk: %d | disk_empty: %d | disk_invalid_header: %d | duplicates_skipped: %d",
		clientID, cachedUsed, cachedEmpty, diskUsed, diskEmpty, diskInvalidHeader, duplicatesSkipped)

	// 3. Process valid increments in parallel
	if len(validIncrements) > 0 {
		a.mergeIncrements(validIncrements)
	}

	log.Printf("action: q2_merge_complete | client_id: %s | total_merged: %d | quantity_entries: %d | profit_entries: %d",
		clientID, len(validIncrements), len(a.quantityData), len(a.profitData))

	results, err := a.Finalize(clientID)
	if err != nil {
		return nil, err
	}

	batch := protocol.NewQ2AggregateBatch(batchIndex, results, clientID, true)

	return []BatchToPublish{
		{
			Batch:      batch,
			RoutingKey: "",
		},
	}, nil
}

func (a *Q2Aggregate) mergeIncrements(increments [][]byte) {
	worker.ProcessAndMerge(
		increments,
		0,
		parseQ2Increment,
		func(results []q2WorkerResult) {
			for _, result := range results {
				for key, quantity := range result.quantityData {
					a.quantityData[key] += quantity
				}
				for key, profit := range result.profitData {
					a.profitData[key] += profit
				}
			}
		},
	)
}

func parseQ2Increment(data []byte) q2WorkerResult {
	result := q2WorkerResult{
		quantityData: make(map[string]int),
		profitData:   make(map[string]float64),
	}

	if len(data) == 0 {
		return result
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) != 4 {
			continue
		}

		recordType := parts[0]
		yearMonth := parts[1]
		itemID := parts[2]
		value := parts[3]

		productID := yearMonth + "|" + itemID

		switch recordType {
		case "Q":
			quantity, err := strconv.Atoi(value)
			if err != nil {
				continue
			}
			result.quantityData[productID] += quantity

		case "P":
			profit, err := strconv.ParseFloat(value, 64)
			if err != nil {
				continue
			}
			result.profitData[productID] += profit
		}
	}

	return result
}

func (a *Q2Aggregate) Cleanup() error {
	a.quantityData = nil
	a.profitData = nil
	a.cachedIncrements = nil
	return nil
}
