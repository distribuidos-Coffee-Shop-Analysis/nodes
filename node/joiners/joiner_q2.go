package joiners

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q2Joiner handles joining Q2 aggregate data with menu item names
// State is only used during PerformJoin to merge all reference increments
type Q2Joiner struct {
	rawReferenceRecords []*protocol.MenuItemRecord
	clientID            string
}

// NewQ2Joiner creates a new Q2Joiner instance
func NewQ2Joiner() *Q2Joiner {
	return &Q2Joiner{
		rawReferenceRecords: make([]*protocol.MenuItemRecord, 0),
	}
}

func (j *Q2Joiner) Name() string {
	return "q2_joiner_menu_items"
}

// SerializeReferenceRecords directly serializes reference records without intermediate buffer
// Format: R|item_id|item_name|category|price|is_seasonal|available_from|available_to\n
func (j *Q2Joiner) SerializeReferenceRecords(records []protocol.Record, batchIndex int) ([]byte, error) {
	if len(records) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	buf.Grow(len(records)*100 + 20)

	buf.WriteString("BATCH|")
	buf.WriteString(strconv.Itoa(batchIndex))
	buf.WriteByte('\n')

	for _, record := range records {
		menuItem, ok := record.(*protocol.MenuItemRecord)
		if !ok {
			log.Printf("action: q2_joiner_serialize_skip | expected: MenuItemRecord | got: %T", record)
			continue
		}

		buf.WriteString("R|")
		buf.WriteString(menuItem.ItemID)
		buf.WriteByte('|')
		buf.WriteString(menuItem.ItemName)
		buf.WriteByte('|')
		buf.WriteString(menuItem.Category)
		buf.WriteByte('|')
		buf.WriteString(menuItem.Price)
		buf.WriteByte('|')
		buf.WriteString(menuItem.IsSeasonal)
		buf.WriteByte('|')
		buf.WriteString(menuItem.AvailableFrom)
		buf.WriteByte('|')
		buf.WriteString(menuItem.AvailableTo)
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

// SerializeBufferedBatch directly serializes a buffered batch without intermediate buffer
// Format: B|batch_index|eof|client_id|record_type|fields...\n
func (j *Q2Joiner) SerializeBufferedBatch(batch *protocol.BatchMessage) ([]byte, error) {
	if batch == nil || len(batch.Records) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	buf.Grow(len(batch.Records) * 100)

	for _, record := range batch.Records {
		buf.WriteString("B|")
		buf.WriteString(strconv.Itoa(batch.BatchIndex))
		buf.WriteByte('|')
		buf.WriteString(strconv.FormatBool(batch.EOF))
		buf.WriteByte('|')
		buf.WriteString(batch.ClientID)
		buf.WriteByte('|')

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
			log.Printf("action: q2_joiner_serialize_buffered_skip | type: %T", record)
			continue
		}
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

// RestoreBufferedBatches restores buffered batches from disk (returns them for processing)
func (j *Q2Joiner) RestoreBufferedBatches(data []byte) ([]protocol.BatchMessage, error) {
	if len(data) == 0 {
		return nil, nil
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))
	batchRecords := make(map[int][]protocol.Record)
	batchMetadata := make(map[int]*protocol.BatchMessage)

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 || !strings.HasPrefix(line, "B|") {
			continue
		}

		parts := strings.Split(line[2:], "|")
		if len(parts) < 5 {
			continue
		}

		batchIndex, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}

		eof, _ := strconv.ParseBool(parts[1])
		clientID := parts[2]
		recordType := parts[3]

		if _, exists := batchMetadata[batchIndex]; !exists {
			batchMetadata[batchIndex] = &protocol.BatchMessage{
				Type:       protocol.MessageTypeBatch,
				BatchIndex: batchIndex,
				EOF:        eof,
				ClientID:   clientID,
			}
		}

		var record protocol.Record
		switch recordType {
		case "Q2BestSelling":
			if len(parts) != 7 {
				continue
			}
			record = &protocol.Q2BestSellingRecord{
				YearMonth:   parts[4],
				ItemID:      parts[5],
				SellingsQty: parts[6],
			}

		case "Q2MostProfits":
			if len(parts) != 7 {
				continue
			}
			record = &protocol.Q2MostProfitsRecord{
				YearMonth: parts[4],
				ItemID:    parts[5],
				ProfitSum: parts[6],
			}

		default:
			continue
		}

		batchRecords[batchIndex] = append(batchRecords[batchIndex], record)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan q2 joiner buffered batches: %w", err)
	}

	var batches []protocol.BatchMessage
	for batchIndex, records := range batchRecords {
		meta := batchMetadata[batchIndex]
		batches = append(batches, protocol.BatchMessage{
			Type:        meta.Type,
			DatasetType: protocol.DatasetTypeQ2Agg,
			BatchIndex:  batchIndex,
			EOF:         meta.EOF,
			ClientID:    meta.ClientID,
			Records:     records,
		})
	}

	log.Printf("action: q2_joiner_restore_buffered_complete | buffered_batches: %d", len(batches))

	return batches, nil
}

// restoreState restores reference data from a serialized increment (used during join)
func (j *Q2Joiner) restoreState(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if j.rawReferenceRecords == nil {
		j.rawReferenceRecords = make([]*protocol.MenuItemRecord, 0)
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 || !strings.HasPrefix(line, "R|") {
			continue
		}

		parts := strings.Split(line[2:], "|")
		if len(parts) != 7 {
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
		j.rawReferenceRecords = append(j.rawReferenceRecords, menuItem)
	}

	return scanner.Err()
}

func (j *Q2Joiner) PerformJoin(aggregatedRecords []protocol.Record, clientId string, historicalIncrements [][]byte) ([]protocol.Record, error) {
	if j.clientID == "" {
		j.clientID = clientId
	}

	// Load all reference data increments
	if len(historicalIncrements) > 0 {
		log.Printf("action: q2_joiner_load_start | client_id: %s | increments: %d",
			clientId, len(historicalIncrements))

		for i, incrementData := range historicalIncrements {
			if len(incrementData) > 0 {
				if err := j.restoreState(incrementData); err != nil {
					log.Printf("action: q2_joiner_load_increment | client_id: %s | increment: %d | result: fail | error: %v",
						clientId, i, err)
				}
			}
		}

		log.Printf("action: q2_joiner_load_complete | client_id: %s | increments_merged: %d | raw_records: %d",
			clientId, len(historicalIncrements), len(j.rawReferenceRecords))
	}

	// Build index
	menuItemIndex := make(map[string]*protocol.MenuItemRecord, len(j.rawReferenceRecords))
	for _, menuItem := range j.rawReferenceRecords {
		menuItemIndex[menuItem.ItemID] = menuItem
	}

	log.Printf("action: q2_joiner_index_built | client_id: %s | menu_items: %d",
		clientId, len(menuItemIndex))

	var joinedRecords []protocol.Record

	for _, record := range aggregatedRecords {
		switch aggRecord := record.(type) {
		case *protocol.Q2BestSellingRecord:
			menuItem, exists := menuItemIndex[aggRecord.ItemID]
			if !exists {
				log.Printf("action: q2_join_warning | client_id: %s | item_id: %s | error: menu_item_not_found", clientId, aggRecord.ItemID)
				continue
			}

			joinedRecord := &protocol.Q2BestSellingWithNameRecord{
				YearMonth:   aggRecord.YearMonth,
				ItemName:    menuItem.ItemName,
				SellingsQty: aggRecord.SellingsQty,
			}
			joinedRecords = append(joinedRecords, joinedRecord)

		case *protocol.Q2MostProfitsRecord:
			menuItem, exists := menuItemIndex[aggRecord.ItemID]
			if !exists {
				log.Printf("action: q2_join_warning | client_id: %s | item_id: %s | error: menu_item_not_found", clientId, aggRecord.ItemID)
				continue
			}

			joinedRecord := &protocol.Q2MostProfitsWithNameRecord{
				YearMonth: aggRecord.YearMonth,
				ItemName:  menuItem.ItemName,
				ProfitSum: aggRecord.ProfitSum,
			}
			joinedRecords = append(joinedRecords, joinedRecord)

		default:
			return nil, fmt.Errorf("unsupported Q2 aggregated record type: %T", record)
		}
	}

	log.Printf("action: q2_joiner_join_complete | client_id: %s | input_records: %d | joined_records: %d",
		clientId, len(aggregatedRecords), len(joinedRecords))

	return joinedRecords, nil
}

func (j *Q2Joiner) GetOutputDatasetType() protocol.DatasetType {
	return protocol.DatasetTypeQ2AggWithName
}

func (j *Q2Joiner) AcceptsReferenceType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeMenuItems
}

func (j *Q2Joiner) AcceptsAggregateType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeQ2Agg
}

func (j *Q2Joiner) Cleanup() error {
	j.rawReferenceRecords = nil
	return nil
}

func (j *Q2Joiner) ShouldCleanupAfterEOF() bool {
	return false
}

func (j *Q2Joiner) CacheIncrement(batchIndex int, data []byte) {
}

func (j *Q2Joiner) GetCachedBatchIndices() map[int]bool {
	return nil
}	
