package joiners

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

type Q2JoinerState struct {
	MenuItems          map[string]*protocol.MenuItemRecord // key: item_id, value: menu item record
	BufferedAggBatches []protocol.BatchMessage             // Buffered aggregate batches waiting for reference data
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
			MenuItems:          make(map[string]*protocol.MenuItemRecord),
			BufferedAggBatches: make([]protocol.BatchMessage, 0),
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

func (j *Q2Joiner) ShouldCleanupAfterEOF() bool {
	// Q2 has small reference data (menu items), keep in memory
	return false
}

// AddBufferedBatch adds a batch to the buffer (thread-safe)
func (j *Q2Joiner) AddBufferedBatch(batch protocol.BatchMessage) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.state.BufferedAggBatches = append(j.state.BufferedAggBatches, batch)
}

// GetBufferedBatches returns all buffered batches (thread-safe, returns a copy)
func (j *Q2Joiner) GetBufferedBatches() []protocol.BatchMessage {
	j.mu.RLock()
	defer j.mu.RUnlock()
	// Return a copy to avoid race conditions
	batches := make([]protocol.BatchMessage, len(j.state.BufferedAggBatches))
	copy(batches, j.state.BufferedAggBatches)
	return batches
}

// ClearBufferedBatches clears all buffered batches (thread-safe)
func (j *Q2Joiner) ClearBufferedBatches() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.state.BufferedAggBatches = make([]protocol.BatchMessage, 0)
}

// SerializeState exports the current joiner state using pipe-delimited format
// Format:
//
//	Reference data: R|item_id|item_name|category|price|is_seasonal|available_from|available_to\n
//	Separator: ---BUFFERED---\n
//	Buffered batches: B|batch_index|eof|client_id|record_type|record_data...\n
func (j *Q2Joiner) SerializeState() ([]byte, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	var buf bytes.Buffer
	buf.Grow(len(j.state.MenuItems)*100 + len(j.state.BufferedAggBatches)*200)

	// 1. Serialize reference data (menu items)
	for itemID, item := range j.state.MenuItems {
		buf.WriteString("R|")
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

	// 2. Separator
	if len(j.state.BufferedAggBatches) > 0 {
		buf.WriteString("---BUFFERED---\n")

		// 3. Serialize buffered aggregate batches
		for _, batch := range j.state.BufferedAggBatches {
			for _, record := range batch.Records {
				buf.WriteString("B|")
				buf.WriteString(strconv.Itoa(batch.BatchIndex))
				buf.WriteByte('|')
				buf.WriteString(strconv.FormatBool(batch.EOF))
				buf.WriteByte('|')
				buf.WriteString(batch.ClientID)
				buf.WriteByte('|')

				// Serialize specific record type
				switch r := record.(type) {
				case *protocol.Q2BestSellingRecord:
					buf.WriteString("Q2BestSelling|")
					buf.WriteString(r.YearMonth)
					buf.WriteByte('|')
					buf.WriteString(r.ItemID)
					buf.WriteByte('|')
					buf.WriteString(r.SellingsQty)

				case *protocol.Q2MostProfitsRecord:
					buf.WriteString("Q2MostProfits|")
					buf.WriteString(r.YearMonth)
					buf.WriteByte('|')
					buf.WriteString(r.ItemID)
					buf.WriteByte('|')
					buf.WriteString(r.ProfitSum)

				default:
					log.Printf("action: q2_joiner_serialize_skip_unknown_record | type: %T", record)
					continue
				}
				buf.WriteByte('\n')
			}
		}
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
	j.state.BufferedAggBatches = make([]protocol.BatchMessage, 0)

	scanner := bufio.NewScanner(bytes.NewReader(data))
	lineNum := 0
	inBufferedSection := false
	currentBatch := (*protocol.BatchMessage)(nil)
	batchRecords := make(map[int][]protocol.Record) // Group records by batch index

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		if len(line) == 0 {
			continue
		}

		// Check for separator
		if line == "---BUFFERED---" {
			inBufferedSection = true
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) < 2 {
			log.Printf("action: q2_joiner_restore_skip_invalid_line | line: %d | parts: %d", lineNum, len(parts))
			continue
		}

		prefix := parts[0]

		if !inBufferedSection && prefix == "R" {
			// Reference data: R|item_id|item_name|category|price|is_seasonal|available_from|available_to
			if len(parts) != 8 {
				log.Printf("action: q2_joiner_restore_skip_invalid_reference | line: %d | parts: %d", lineNum, len(parts))
				continue
			}

			menuItem := &protocol.MenuItemRecord{
				ItemID:        parts[1],
				ItemName:      parts[2],
				Category:      parts[3],
				Price:         parts[4],
				IsSeasonal:    parts[5],
				AvailableFrom: parts[6],
				AvailableTo:   parts[7],
			}
			j.state.MenuItems[menuItem.ItemID] = menuItem

		} else if inBufferedSection && prefix == "B" {
			// Buffered batch: B|batch_index|eof|client_id|record_type|record_data...
			if len(parts) < 5 {
				log.Printf("action: q2_joiner_restore_skip_invalid_buffered | line: %d | parts: %d", lineNum, len(parts))
				continue
			}

			batchIndex, _ := strconv.Atoi(parts[1])
			eof, _ := strconv.ParseBool(parts[2])
			clientID := parts[3]
			recordType := parts[4]

			var record protocol.Record
			switch recordType {
			case "Q2BestSelling":
				if len(parts) != 8 {
					continue
				}
				record = &protocol.Q2BestSellingRecord{
					YearMonth:   parts[5],
					ItemID:      parts[6],
					SellingsQty: parts[7],
				}

			case "Q2MostProfits":
				if len(parts) != 8 {
					continue
				}
				record = &protocol.Q2MostProfitsRecord{
					YearMonth: parts[5],
					ItemID:    parts[6],
					ProfitSum: parts[7],
				}

			default:
				log.Printf("action: q2_joiner_restore_skip_unknown_record_type | type: %s", recordType)
				continue
			}

			// Group records by batch index
			batchRecords[batchIndex] = append(batchRecords[batchIndex], record)

			// Store batch metadata (will be overwritten by same batch but that's ok)
			currentBatch = &protocol.BatchMessage{
				BatchIndex: batchIndex,
				EOF:        eof,
				ClientID:   clientID,
				Records:    nil, // Will be filled below
			}
		}
	}

	// Reconstruct batches from grouped records
	for batchIndex, records := range batchRecords {
		j.state.BufferedAggBatches = append(j.state.BufferedAggBatches, protocol.BatchMessage{
			BatchIndex: batchIndex,
			EOF:        currentBatch.EOF,
			ClientID:   currentBatch.ClientID,
			Records:    records,
		})
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan q2 joiner snapshot: %w", err)
	}

	log.Printf("action: q2_joiner_restore_complete | menu_items: %d | buffered_batches: %d",
		len(j.state.MenuItems), len(j.state.BufferedAggBatches))

	return nil
}
