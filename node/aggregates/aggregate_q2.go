package aggregates

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q2Aggregate handles aggregation of Q2 grouped data to find best selling items and most profitable items per month
// State is only used during Finalize to merge all increments
type Q2Aggregate struct {
	// State used only during GetBatchesToPublish/Finalize
	quantityData map[string]int
	profitData   map[string]float64
	clientID     string
}

// NewQ2Aggregate creates a new Q2 aggregate processor
func NewQ2Aggregate() *Q2Aggregate {
	return &Q2Aggregate{
		quantityData: make(map[string]int),
		profitData:   make(map[string]float64),
	}
}

func (a *Q2Aggregate) Name() string {
	return "q2_aggregate_best_selling_most_profits"
}

// SerializeRecords directly serializes records without intermediate buffer
// Format: Q|year_month|item_id|quantity\n or P|year_month|item_id|profit\n
func (a *Q2Aggregate) SerializeRecords(records []protocol.Record) ([]byte, error) {
	if len(records) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	buf.Grow(len(records) * 50)

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

// restoreState restores data from a serialized increment (used during merge)
func (a *Q2Aggregate) restoreState(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if a.quantityData == nil {
		a.quantityData = make(map[string]int)
	}
	if a.profitData == nil {
		a.profitData = make(map[string]float64)
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

		// Reconstruct composite key: "year_month|item_id"
		productID := yearMonth + "|" + itemID

		switch recordType {
		case "Q":
			quantity, err := strconv.Atoi(value)
			if err != nil {
				continue
			}
			a.quantityData[productID] += quantity

		case "P":
			profit, err := strconv.ParseFloat(value, 64)
			if err != nil {
				continue
			}
			a.profitData[productID] += profit
		}
	}

	return scanner.Err()
}

// Finalize calculates the best selling and most profitable items per year_month
func (a *Q2Aggregate) Finalize(clientID string) ([]protocol.Record, error) {
	if a.clientID == "" {
		a.clientID = clientID
	}

	log.Printf("action: q2_aggregate_finalize | client_id: %s | quantity_entries: %d | profit_entries: %d",
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

// GetBatchesToPublish loads all increments, merges them, and returns final results
func (a *Q2Aggregate) GetBatchesToPublish(historicalIncrements [][]byte, batchIndex int, clientID string) ([]BatchToPublish, error) {
	if len(historicalIncrements) > 0 {
		log.Printf("action: q2_merge_start | client_id: %s | increments: %d",
			clientID, len(historicalIncrements))

		for i, incrementData := range historicalIncrements {
			if len(incrementData) > 0 {
				if err := a.restoreState(incrementData); err != nil {
					log.Printf("action: q2_merge_increment | client_id: %s | increment: %d | result: fail | error: %v",
						clientID, i, err)
				}
			}
		}

		log.Printf("action: q2_merge_complete | client_id: %s | increments_merged: %d | quantity_entries: %d | profit_entries: %d",
			clientID, len(historicalIncrements), len(a.quantityData), len(a.profitData))
	}

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

func (a *Q2Aggregate) Cleanup() error {
	a.quantityData = nil
	a.profitData = nil
	return nil
}
