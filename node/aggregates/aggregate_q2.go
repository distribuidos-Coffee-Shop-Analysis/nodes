package aggregates

import (
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

type Q2AggregateState struct {
	QuantityData map[string]int     // year_month|item_id -> total quantity
	ProfitData   map[string]float64 // year_month|item_id -> total profit
}

// Q2Aggregate handles aggregation of Q2 grouped data to find best selling items and most profitable items per month
type Q2Aggregate struct {
	mu          sync.RWMutex
	state       *Q2AggregateState
	persistence *common.StatePersistence
	clientID    string
}

// NewQ2Aggregate creates a new Q2 aggregate processor
func NewQ2Aggregate() *Q2Aggregate {
	return NewQ2AggregateWithPersistence("/app/state")
}

func NewQ2AggregateWithPersistence(stateDir string) *Q2Aggregate {
	persistence, err := common.NewStatePersistence(stateDir)
	if err != nil {
		log.Printf("action: q2_aggregate_init | result: fail | error: %v | fallback: memory_only", err)
		persistence = nil
	}

	return &Q2Aggregate{
		state: &Q2AggregateState{
			QuantityData: make(map[string]int),
			ProfitData:   make(map[string]float64),
		},
		persistence: persistence,
	}
}

func (a *Q2Aggregate) Name() string {
	return "q2_aggregate_best_selling_most_profits"
}

// AccumulateBatch processes and accumulates a batch of Q2 grouped records
func (a *Q2Aggregate) AccumulateBatch(records []protocol.Record, batchIndex int) error {

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

	// Only lock for the final merge into shared state
	a.mu.Lock()
	defer a.mu.Unlock()

	for key, quantity := range localQuantity {
		a.state.QuantityData[key] += quantity
	}

	for key, profit := range localProfit {
		a.state.ProfitData[key] += profit
	}

	if a.persistence != nil && a.clientID != "" {
		if err := a.persistence.SaveState(a.Name(), a.clientID, a.state); err != nil {
			log.Printf("action: q2_save_state | result: fail | client_id: %s | error: %v",
				a.clientID, err)
		}
	}

	return nil
}

// trackBatchIndex is no longer needed as we track inline in AccumulateBatch

// Finalize calculates the best selling and most profitable items per year_month
func (a *Q2Aggregate) Finalize(clientID string) ([]protocol.Record, error) {

	if a.clientID == "" {
		a.clientID = clientID

		if a.persistence != nil {
			var savedState Q2AggregateState
			if err := a.persistence.LoadState(a.Name(), clientID, &savedState); err != nil {
				log.Printf("action: q2_load_state | result: fail | client_id: %s | error: %v",
					clientID, err)
			} else if savedState.QuantityData != nil && savedState.ProfitData != nil {
				a.mu.Lock()
				for key, quantity := range savedState.QuantityData {
					a.state.QuantityData[key] += quantity
				}
				for key, profit := range savedState.ProfitData {
					a.state.ProfitData[key] += profit
				}
				a.mu.Unlock()
				log.Printf("action: q2_load_state | result: success | client_id: %s | "+
					"quantity_entries: %d | profit_entries: %d",
					clientID, len(savedState.QuantityData), len(savedState.ProfitData))
			}
		}
	}

	log.Printf("action: q2_aggregate_finalize | client_id: %s | quantity_entries: %d | profit_entries: %d",
		clientID, len(a.state.QuantityData), len(a.state.ProfitData))

	// Group data by year_month for processing
	quantityByMonth := make(map[string]map[string]int)   // year_month -> item_id -> quantity
	profitByMonth := make(map[string]map[string]float64) // year_month -> item_id -> profit

	// Process quantity data
	for key, quantity := range a.state.QuantityData {
		parts := parseAggregateKey(key)
		yearMonth, itemID := parts[0], parts[1]

		if quantityByMonth[yearMonth] == nil {
			quantityByMonth[yearMonth] = make(map[string]int)
		}
		quantityByMonth[yearMonth][itemID] = quantity
	}

	// Process profit data
	for key, profit := range a.state.ProfitData {
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

	log.Printf("action: q2_aggregate_finalize_complete | client_id: %s | total_results: %d", clientID, len(result))

	return result, nil
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
func (a *Q2Aggregate) GetBatchesToPublish(batchIndex int, clientID string) ([]BatchToPublish, error) {
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

// Cleanup releases all resources held by this aggregate
func (a *Q2Aggregate) Cleanup() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.persistence != nil && a.clientID != "" {
		if err := a.persistence.DeleteState(a.Name(), a.clientID); err != nil {
			log.Printf("action: q2_delete_state | result: fail | client_id: %s | error: %v",
				a.clientID, err)
		}
	}

	// Clear all state to release memory
	a.state.QuantityData = nil
	a.state.ProfitData = nil
	a.state = nil

	return nil
}
