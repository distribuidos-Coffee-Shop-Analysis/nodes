package aggregates

import (
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q2Aggregate handles aggregation of Q2 grouped data to find best selling items and most profitable items per month
type Q2Aggregate struct {
	mu sync.RWMutex

	// Maps to accumulate data by year_month + item_id
	quantityData map[string]int     // year_month|item_id -> total quantity
	profitData   map[string]float64 // year_month|item_id -> total profit

	// Track received batches to handle out-of-order EOF
	receivedBatches map[int]bool
	maxBatchIndex   int
	eofReceived     bool
}

// NewQ2Aggregate creates a new Q2 aggregate processor
func NewQ2Aggregate() *Q2Aggregate {
	return &Q2Aggregate{
		quantityData:    make(map[string]int),
		profitData:      make(map[string]float64),
		receivedBatches: make(map[int]bool),
		maxBatchIndex:   -1,
		eofReceived:     false,
	}
}

func (a *Q2Aggregate) Name() string {
	return "q2_aggregate_best_selling_most_profits"
}

// AccumulateBatch processes and accumulates a batch of Q2 grouped records
func (a *Q2Aggregate) AccumulateBatch(records []protocol.Record, batchIndex int) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Mark this batch as received
	a.receivedBatches[batchIndex] = true

	log.Printf("action: q2_aggregate_batch | batch_index: %d | record_count: %d",
		batchIndex, len(records))

	for _, record := range records {
		switch r := record.(type) {
		case *protocol.Q2GroupWithQuantityRecord:
			// Accumulate quantity data
			key := fmt.Sprintf("%s|%s", r.YearMonth, r.ItemID)
			quantity, err := strconv.Atoi(r.SellingsQty)
			if err != nil {
				log.Printf("action: q2_aggregate_quantity_parse | result: error | "+
					"year_month: %s | item_id: %s | sellings_qty: %s | error: %v",
					r.YearMonth, r.ItemID, r.SellingsQty, err)
				continue
			}

			a.quantityData[key] += quantity

			log.Printf("action: q2_aggregate_quantity | year_month: %s | item_id: %s | "+
				"batch_quantity: %d | accumulated_quantity: %d",
				r.YearMonth, r.ItemID, quantity, a.quantityData[key])

		case *protocol.Q2GroupWithSubtotalRecord:
			// Accumulate profit data
			key := fmt.Sprintf("%s|%s", r.YearMonth, r.ItemID)
			profit, err := strconv.ParseFloat(r.ProfitSum, 64)
			if err != nil {
				log.Printf("action: q2_aggregate_profit_parse | result: error | "+
					"year_month: %s | item_id: %s | profit_sum: %s | error: %v",
					r.YearMonth, r.ItemID, r.ProfitSum, err)
				continue
			}

			a.profitData[key] += profit

			log.Printf("action: q2_aggregate_profit | year_month: %s | item_id: %s | "+
				"batch_profit: %.2f | accumulated_profit: %.2f",
				r.YearMonth, r.ItemID, profit, a.profitData[key])

		default:
			log.Printf("action: q2_aggregate_unknown_record | result: warning | "+
				"record_type: %T", record)
		}
	}

	return nil
}

// Finalize calculates the best selling and most profitable items per year_month
func (a *Q2Aggregate) Finalize() ([]protocol.Record, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

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

			log.Printf("action: q2_aggregate_best_selling | year_month: %s | "+
				"item_id: %s | sellings_qty: %d", yearMonth, bestItem, bestQuantity)
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

			log.Printf("action: q2_aggregate_most_profitable | year_month: %s | "+
				"item_id: %s | profit_sum: %.2f", yearMonth, bestItem, bestProfit)
		}
	}

	log.Printf("action: q2_aggregate_finalize_complete | total_results: %d", len(result))

	return result, nil
}

// IsComplete checks if all expected batches have been received
func (a *Q2Aggregate) IsComplete(maxBatchIndex int) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if !a.eofReceived || maxBatchIndex < 0 {
		return false
	}

	// Check if all batches from 0 to maxBatchIndex have been received
	for i := 0; i <= maxBatchIndex; i++ {
		if !a.receivedBatches[i] {
			return false
		}
	}

	return true
}

// SetEOF marks that EOF has been received with the given max batch index
func (a *Q2Aggregate) SetEOF(maxBatchIndex int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.eofReceived = true
	a.maxBatchIndex = maxBatchIndex

	log.Printf("action: q2_aggregate_eof | max_batch_index: %d | received_batches: %d",
		maxBatchIndex, len(a.receivedBatches))
}

// Helper functions

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
