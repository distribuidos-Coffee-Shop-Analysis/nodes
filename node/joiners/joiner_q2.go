package joiners

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

type Q2JoinerState struct {
	MenuItems map[string]*protocol.MenuItemRecord // key: item_id, value: menu item record
}

// Q2Joiner handles joining Q2 aggregate data with menu item names
type Q2Joiner struct {
	state    *Q2JoinerState
	mu       sync.RWMutex
	clientID string
}

// NewQ2Joiner creates a new Q2Joiner instance
func NewQ2Joiner() *Q2Joiner {
	return &Q2Joiner{
		state: &Q2JoinerState{
			MenuItems: make(map[string]*protocol.MenuItemRecord),
		},
	}
}

// Name returns the name of this joiner
func (j *Q2Joiner) Name() string {
	return "q2_joiner_menu_items"
}

// StoreReferenceDataset stores menu item reference data for future joins
func (j *Q2Joiner) StoreReferenceDataset(records []protocol.Record) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	for _, record := range records {
		menuItem, ok := record.(*protocol.MenuItemRecord)
		if !ok {
			return fmt.Errorf("expected MenuItemRecord, got %T", record)
		}

		j.state.MenuItems[menuItem.ItemID] = menuItem
	}

	return nil
}

// PerformJoin joins Q2 aggregated data with stored menu items
func (j *Q2Joiner) PerformJoin(aggregatedRecords []protocol.Record, clientId string) ([]protocol.Record, error) {
	if j.clientID == "" {
		j.clientID = clientId
	}

	j.mu.RLock()
	defer j.mu.RUnlock()

	var joinedRecords []protocol.Record

	for _, record := range aggregatedRecords {
		switch aggRecord := record.(type) {
		case *protocol.Q2BestSellingRecord:
			// Join best selling data with menu item names
			menuItem, exists := j.state.MenuItems[aggRecord.ItemID]
			if !exists {
				log.Printf("action: q2_join_warning | client_id: %s | item_id: %s | error: menu_item_not_found", clientId, aggRecord.ItemID)
				continue // Skip records without matching menu items
			}

			joinedRecord := &protocol.Q2BestSellingWithNameRecord{
				YearMonth:   aggRecord.YearMonth,
				ItemName:    menuItem.ItemName,
				SellingsQty: aggRecord.SellingsQty,
			}
			joinedRecords = append(joinedRecords, joinedRecord)

			log.Printf("action: q2_join_best_selling | client_id: %s | item_id: %s | item_name: %s | qty: %s",
				clientId, aggRecord.ItemID, menuItem.ItemName, aggRecord.SellingsQty)

		case *protocol.Q2MostProfitsRecord:
			// Join most profitable data with menu item names
			menuItem, exists := j.state.MenuItems[aggRecord.ItemID]
			if !exists {
				log.Printf("action: q2_join_warning | client_id: %s | item_id: %s | error: menu_item_not_found", clientId, aggRecord.ItemID)
				continue // Skip records without matching menu items
			}

			joinedRecord := &protocol.Q2MostProfitsWithNameRecord{
				YearMonth: aggRecord.YearMonth,
				ItemName:  menuItem.ItemName,
				ProfitSum: aggRecord.ProfitSum,
			}
			joinedRecords = append(joinedRecords, joinedRecord)

			log.Printf("action: q2_join_most_profits | client_id: %s | item_id: %s | item_name: %s | profit: %s",
				clientId, aggRecord.ItemID, menuItem.ItemName, aggRecord.ProfitSum)

		default:
			return nil, fmt.Errorf("unsupported Q2 aggregated record type: %T", record)
		}
	}

	return joinedRecords, nil
}

// GetOutputDatasetType returns the dataset type for Q2 joined output
func (j *Q2Joiner) GetOutputDatasetType() protocol.DatasetType {
	return protocol.DatasetTypeQ2AggWithName
}

// AcceptsReferenceType checks if this joiner accepts menu items as reference data
func (j *Q2Joiner) AcceptsReferenceType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeMenuItems
}

// AcceptsAggregateType checks if this joiner accepts Q2 aggregate data
func (j *Q2Joiner) AcceptsAggregateType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeQ2Agg
}

// Cleanup releases resources
// Note: We keep reference data because multiple EOF batches may arrive from upstream
// Menu items dataset is small (~100 rows, ~10KB) so keeping it in memory is fine
func (j *Q2Joiner) Cleanup() error {
	// No-op: we keep reference data to handle multiple EOF batches
	return nil
}

// SerializeState exports the current joiner state using pipe-delimited format
// Format: item_id|item_name|category|price|is_seasonal|available_from|available_to\n
func (j *Q2Joiner) SerializeState() ([]byte, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	var buf bytes.Buffer
	buf.Grow(len(j.state.MenuItems) * 100)

	for itemID, item := range j.state.MenuItems {
		buf.WriteString(itemID)
		buf.WriteByte('|')
		buf.WriteString(item.ItemName)
		buf.WriteByte('|')
		buf.WriteString(item.Category)
		buf.WriteByte('|')
		buf.WriteString(item.Price)
		buf.WriteByte('|')
		buf.WriteString(item.IsSeasonal)
		buf.WriteByte('|')
		buf.WriteString(item.AvailableFrom)
		buf.WriteByte('|')
		buf.WriteString(item.AvailableTo)
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

// RestoreState restores the joiner state from pipe-delimited format
func (j *Q2Joiner) RestoreState(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	j.state.MenuItems = make(map[string]*protocol.MenuItemRecord)

	scanner := bufio.NewScanner(bytes.NewReader(data))
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		if len(line) == 0 {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) != 7 {
			log.Printf("action: q2_joiner_restore_skip_invalid_line | line: %d | parts: %d", lineNum, len(parts))
			continue
		}

		menuItem := &protocol.MenuItemRecord{
			ItemID:        parts[0],
			ItemName:      parts[1],
			Category:      parts[2],
			Price:         parts[3],
			IsSeasonal:    parts[4],
			AvailableFrom: parts[5],
			AvailableTo:   parts[6],
		}

		j.state.MenuItems[menuItem.ItemID] = menuItem
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan q2 joiner snapshot: %w", err)
	}

	log.Printf("action: q2_joiner_restore_complete | menu_items: %d", len(j.state.MenuItems))

	return nil
}
