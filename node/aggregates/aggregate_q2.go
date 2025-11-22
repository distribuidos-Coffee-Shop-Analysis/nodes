package aggregates

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

type Q2AggregateState struct {
	QuantityData map[string]int     // year_month|item_id -> total quantity
	ProfitData   map[string]float64 // year_month|item_id -> total profit
}

// Q2Aggregate handles aggregation of Q2 grouped data to find best selling items and most profitable items per month
type Q2Aggregate struct {
	mu       sync.RWMutex
	state    *Q2AggregateState
	clientID string
}

// NewQ2Aggregate creates a new Q2 aggregate processor
func NewQ2Aggregate() *Q2Aggregate {
	return &Q2Aggregate{
		state: &Q2AggregateState{
			QuantityData: make(map[string]int),
			ProfitData:   make(map[string]float64),
		},
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

	return nil
}

// trackBatchIndex is no longer needed as we track inline in AccumulateBatch

// Finalize calculates the best selling and most profitable items per year_month
func (a *Q2Aggregate) Finalize(clientID string) ([]protocol.Record, error) {

	if a.clientID == "" {
		a.clientID = clientID
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

	// Clear all state to release memory
	a.state.QuantityData = nil
	a.state.ProfitData = nil
	a.state = nil

	return nil
}

// Format:
//
//	Q|product_id|quantity\n for quantity data
//	P|product_id|profit\n for profit data
func (a *Q2Aggregate) SerializeState() ([]byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var buf bytes.Buffer
	// Pre-allocate
	buf.Grow(len(a.state.QuantityData)*2*50 + len(a.state.ProfitData)*2*50)

	for productID, quantity := range a.state.QuantityData {
		buf.WriteByte('Q')
		buf.WriteByte('|')
		buf.WriteString(productID)
		buf.WriteByte('|')
		buf.WriteString(strconv.Itoa(quantity))
		buf.WriteByte('\n')
	}

	for productID, profit := range a.state.ProfitData {
		buf.WriteByte('P')
		buf.WriteByte('|')
		buf.WriteString(productID)
		buf.WriteByte('|')
		buf.WriteString(strconv.FormatFloat(profit, 'f', 2, 64))
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

// RestoreState restores Q2 aggregate state
// Format: recordType|year_month|item_id|value
// Note: productID is composite key "year_month|item_id" so we need to reconstruct it
func (a *Q2Aggregate) RestoreState(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.state.QuantityData = make(map[string]int)
	a.state.ProfitData = make(map[string]float64)

	scanner := bufio.NewScanner(bytes.NewReader(data))
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		if len(line) == 0 {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) != 4 {
			log.Printf("action: q2_restore_skip_invalid_line | line: %d | parts: %d | expected: 4", lineNum, len(parts))
			continue
		}

		recordType := parts[0]
		yearMonth := parts[1]
		itemID := parts[2]
		value := parts[3]

		// Reconstruct composite key: "year_month|item_id"
		productID := yearMonth + "|" + itemID

		switch recordType {
		case "Q":
			quantity, err := strconv.Atoi(value)
			if err != nil {
				log.Printf("action: q2_restore_skip_invalid_quantity | line: %d | value: %s | error: %v",
					lineNum, value, err)
				continue
			}
			a.state.QuantityData[productID] = quantity

		case "P":
			profit, err := strconv.ParseFloat(value, 64)
			if err != nil {
				log.Printf("action: q2_restore_skip_invalid_profit | line: %d | value: %s | error: %v",
					lineNum, value, err)
				continue
			}
			a.state.ProfitData[productID] = profit

		default:
			log.Printf("action: q2_restore_skip_unknown_type | line: %d | type: %s", lineNum, recordType)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan q2 aggregate snapshot: %w", err)
	}

	log.Printf("action: q2_restore_complete | quantity_entries: %d | profit_entries: %d",
		len(a.state.QuantityData), len(a.state.ProfitData))

	return nil
}
