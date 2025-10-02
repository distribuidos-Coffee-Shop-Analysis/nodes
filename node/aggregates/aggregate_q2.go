package aggregates

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q2Aggregate handles aggregation of Q2 grouped data to find best selling items and most profitable items per month
type Q2Aggregate struct {
	mu sync.RWMutex

	// Maps to accumulate data by year_month + item_id
	quantityData map[string]int     // year_month|item_id -> total quantity
	profitData   map[string]float64 // year_month|item_id -> total profit

	// Track unique batch indices (Q2 has dual datasets but same batch index)
	seenBatchIndices map[int]bool // track which batch indices we've seen
	uniqueBatchCount atomic.Int32 // count of unique batch indices
}

// NewQ2Aggregate creates a new Q2 aggregate processor
func NewQ2Aggregate() *Q2Aggregate {
	return &Q2Aggregate{
		quantityData:     make(map[string]int),
		profitData:       make(map[string]float64),
		seenBatchIndices: make(map[int]bool),
		uniqueBatchCount: atomic.Int32{},
	}
}

func (a *Q2Aggregate) Name() string {
	return "q2_aggregate_best_selling_most_profits"
}

// AccumulateBatch processes and accumulates a batch of Q2 grouped records
func (a *Q2Aggregate) AccumulateBatch(records []protocol.Record, batchIndex int) error {
	// Only count as one batch per batchIndex (Q2 has dual datasets but same batchIndex)
	// We'll track seen batch indices to avoid double counting
	a.trackBatchIndex(batchIndex)

	// Process records locally without lock (no shared state access)
	localQuantity := make(map[string]int)
	localProfit := make(map[string]float64)

	for _, record := range records {
		switch r := record.(type) {
		case *protocol.Q2GroupWithQuantityRecord:
			// Accumulate quantity data in local map
			key := fmt.Sprintf("%s|%s", r.YearMonth, r.ItemID)
			quantity, err := strconv.Atoi(r.SellingsQty)
			if err != nil {
				log.Printf("action: q2_aggregate_quantity_parse | result: error | "+
					"year_month: %s | item_id: %s | sellings_qty: %s | error: %v",
					r.YearMonth, r.ItemID, r.SellingsQty, err)
				continue
			}

			localQuantity[key] += quantity

		case *protocol.Q2GroupWithSubtotalRecord:
			// Accumulate profit data in local map
			key := fmt.Sprintf("%s|%s", r.YearMonth, r.ItemID)
			profit, err := strconv.ParseFloat(r.ProfitSum, 64)
			if err != nil {
				log.Printf("action: q2_aggregate_profit_parse | result: error | "+
					"year_month: %s | item_id: %s | profit_sum: %s | error: %v",
					r.YearMonth, r.ItemID, r.ProfitSum, err)
				continue
			}

			localProfit[key] += profit

		default:
			log.Printf("action: q2_aggregate_unknown_record | result: warning | "+
				"record_type: %T", record)
		}
	}

	// Only lock for the final merge into shared maps
	a.mu.Lock()
	defer a.mu.Unlock()

	for key, quantity := range localQuantity {
		a.quantityData[key] += quantity
	}

	for key, profit := range localProfit {
		a.profitData[key] += profit
	}

	return nil
}

// trackBatchIndex tracks unique batch indices to avoid double counting in dual datasets
func (a *Q2Aggregate) trackBatchIndex(batchIndex int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.seenBatchIndices[batchIndex] {
		a.seenBatchIndices[batchIndex] = true
		a.uniqueBatchCount.Add(1)
	}
}

// Finalize calculates the best selling and most profitable items per year_month
func (a *Q2Aggregate) Finalize() ([]protocol.Record, error) {

	log.Printf("action: q2_aggregate_finalize | quantity_entries: %d | profit_entries: %d",
		len(a.quantityData), len(a.profitData))

	// Group data by year_month for processing
	quantityByMonth := make(map[string]map[string]int)   // year_month -> item_id -> quantity
	profitByMonth := make(map[string]map[string]float64) // year_month -> item_id -> profit

	// Process quantity data
	for key, quantity := range a.quantityData {
		parts := parseAggregateKey(key)
		yearMonth, itemID := parts[0], parts[1]

		if quantityByMonth[yearMonth] == nil {
			quantityByMonth[yearMonth] = make(map[string]int)
		}
		quantityByMonth[yearMonth][itemID] = quantity
	}

	// Process profit data
	for key, profit := range a.profitData {
		parts := parseAggregateKey(key)
		yearMonth, itemID := parts[0], parts[1]

		if profitByMonth[yearMonth] == nil {
			profitByMonth[yearMonth] = make(map[string]float64)
		}
		profitByMonth[yearMonth][itemID] = profit
	}

	var result []protocol.Record

	// Find best selling item per month
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

	// Find most profitable item per month
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

	log.Printf("action: q2_aggregate_finalize_complete | total_results: %d", len(result))

	return result, nil
}

// GetAccumulatedBatchCount returns the number of unique batches received so far
func (a *Q2Aggregate) GetAccumulatedBatchCount() int {
	return int(a.uniqueBatchCount.Load()) // No lock needed for atomic read
}

// IsComplete checks if all expected batches have been received
func (a *Q2Aggregate) IsComplete(maxBatchIndex int) bool {
	expectedBatches := maxBatchIndex // batch indices are 1-based
	received := int(a.uniqueBatchCount.Load())
	isComplete := received == expectedBatches

	log.Printf("action: q2_aggregate_is_complete | received: %d | expected: %d | complete: %v",
		received, expectedBatches, isComplete)

	return isComplete
}

// parseAggregateKey splits the composite key "yearMonth|itemID" into parts
func parseAggregateKey(key string) []string {
	parts := make([]string, 2)
	if idx := len(key); idx > 0 {
		for i := 0; i < len(key); i++ {
			if key[i] == '|' {
				parts[0] = key[:i]
				parts[1] = key[i+1:]
				break
			}
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

// GetBatchesToPublish returns a single batch with all aggregated results
// Q2 doesn't need partitioning, so returns a single batch with empty routing key (uses default from config)
func (a *Q2Aggregate) GetBatchesToPublish(batchIndex int) ([]BatchToPublish, error) {
	results, err := a.Finalize()
	if err != nil {
		return nil, err
	}

	batch := protocol.NewQ2AggregateBatch(batchIndex, results, true)

	return []BatchToPublish{
		{
			Batch:      batch,
			RoutingKey: "", 
		},
	}, nil
}
