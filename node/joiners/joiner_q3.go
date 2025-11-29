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

// Q3Joiner handles joining Q3 aggregate data with store names
type Q3Joiner struct {
	rawReferenceRecords []*protocol.StoreRecord
	clientID            string
}

func NewQ3Joiner() *Q3Joiner {
	return &Q3Joiner{
		rawReferenceRecords: make([]*protocol.StoreRecord, 0),
	}
}

func (j *Q3Joiner) Name() string {
	return "q3_joiner_stores"
}

// SerializeReferenceRecords directly serializes reference records without intermediate buffer
func (j *Q3Joiner) SerializeReferenceRecords(records []protocol.Record, batchIndex int) ([]byte, error) {
	if len(records) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	buf.Grow(len(records)*150 + 20)

	buf.WriteString("BATCH|")
	buf.WriteString(strconv.Itoa(batchIndex))
	buf.WriteByte('\n')

	for _, record := range records {
		store, ok := record.(*protocol.StoreRecord)
		if !ok {
			continue
		}

		buf.WriteString("R|")
		buf.WriteString(store.StoreID)
		buf.WriteByte('|')
		buf.WriteString(store.StoreName)
		buf.WriteByte('|')
		buf.WriteString(store.Street)
		buf.WriteByte('|')
		buf.WriteString(store.PostalCode)
		buf.WriteByte('|')
		buf.WriteString(store.City)
		buf.WriteByte('|')
		buf.WriteString(store.State)
		buf.WriteByte('|')
		buf.WriteString(store.Latitude)
		buf.WriteByte('|')
		buf.WriteString(store.Longitude)
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

// SerializeBufferedBatch directly serializes a buffered batch
func (j *Q3Joiner) SerializeBufferedBatch(batch *protocol.BatchMessage) ([]byte, error) {
	if batch == nil || len(batch.Records) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	buf.Grow(len(batch.Records) * 100)

	for _, record := range batch.Records {
		if r, ok := record.(*protocol.Q3AggregatedRecord); ok {
			buf.WriteString("B|")
			buf.WriteString(strconv.Itoa(batch.BatchIndex))
			buf.WriteByte('|')
			buf.WriteString(strconv.FormatBool(batch.EOF))
			buf.WriteByte('|')
			buf.WriteString(batch.ClientID)
			buf.WriteByte('|')
			buf.WriteString(r.YearHalf)
			buf.WriteByte('|')
			buf.WriteString(r.StoreID)
			buf.WriteByte('|')
			buf.WriteString(r.TPV)
			buf.WriteByte('\n')
		}
	}

	return buf.Bytes(), nil
}

// RestoreBufferedBatches restores buffered batches from disk
func (j *Q3Joiner) RestoreBufferedBatches(data []byte) ([]protocol.BatchMessage, error) {
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
		if len(parts) != 6 {
			continue
		}

		batchIndex, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}

		eof, _ := strconv.ParseBool(parts[1])
		clientID := parts[2]

		if _, exists := batchMetadata[batchIndex]; !exists {
			batchMetadata[batchIndex] = &protocol.BatchMessage{
				Type:       protocol.MessageTypeBatch,
				BatchIndex: batchIndex,
				EOF:        eof,
				ClientID:   clientID,
			}
		}

		record := &protocol.Q3AggregatedRecord{
			YearHalf: parts[3],
			StoreID:  parts[4],
			TPV:      parts[5],
		}
		batchRecords[batchIndex] = append(batchRecords[batchIndex], record)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan q3 joiner buffered batches: %w", err)
	}

	var batches []protocol.BatchMessage
	for batchIndex, records := range batchRecords {
		meta := batchMetadata[batchIndex]
		batches = append(batches, protocol.BatchMessage{
			Type:        meta.Type,
			DatasetType: protocol.DatasetTypeQ3Agg,
			BatchIndex:  batchIndex,
			EOF:         meta.EOF,
			ClientID:    meta.ClientID,
			Records:     records,
		})
	}

	return batches, nil
}

// restoreState restores reference data from a serialized increment
func (j *Q3Joiner) restoreState(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if j.rawReferenceRecords == nil {
		j.rawReferenceRecords = make([]*protocol.StoreRecord, 0)
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 || !strings.HasPrefix(line, "R|") {
			continue
		}

		parts := strings.Split(line[2:], "|")
		if len(parts) != 8 {
			continue
		}

		store := &protocol.StoreRecord{
			StoreID:    parts[0],
			StoreName:  parts[1],
			Street:     parts[2],
			PostalCode: parts[3],
			City:       parts[4],
			State:      parts[5],
			Latitude:   parts[6],
			Longitude:  parts[7],
		}
		j.rawReferenceRecords = append(j.rawReferenceRecords, store)
	}

	return scanner.Err()
}

func (j *Q3Joiner) PerformJoin(aggregatedRecords []protocol.Record, clientId string, historicalIncrements [][]byte) ([]protocol.Record, error) {
	if j.clientID == "" {
		j.clientID = clientId
	}

	if len(historicalIncrements) > 0 {
		log.Printf("action: q3_joiner_load_start | client_id: %s | increments: %d", clientId, len(historicalIncrements))

		for i, incrementData := range historicalIncrements {
			if len(incrementData) > 0 {
				if err := j.restoreState(incrementData); err != nil {
					log.Printf("action: q3_joiner_load_increment | client_id: %s | increment: %d | result: fail | error: %v",
						clientId, i, err)
				}
			}
		}

		log.Printf("action: q3_joiner_load_complete | client_id: %s | increments_merged: %d | raw_records: %d",
			clientId, len(historicalIncrements), len(j.rawReferenceRecords))
	}

	storeIndex := make(map[string]*protocol.StoreRecord, len(j.rawReferenceRecords))
	for _, store := range j.rawReferenceRecords {
		storeIndex[store.StoreID] = store
	}

	log.Printf("action: q3_joiner_index_built | client_id: %s | stores: %d", clientId, len(storeIndex))

	var joinedRecords []protocol.Record

	for _, record := range aggregatedRecords {
		aggRecord, ok := record.(*protocol.Q3AggregatedRecord)
		if !ok {
			return nil, fmt.Errorf("expected Q3AggregatedRecord, got %T", record)
		}

		storeRecord, exists := storeIndex[aggRecord.StoreID]
		if !exists {
			continue
		}

		joinedRecord := &protocol.Q3JoinedRecord{
			YearHalf:  aggRecord.YearHalf,
			StoreName: storeRecord.StoreName,
			TPV:       aggRecord.TPV,
		}
		joinedRecords = append(joinedRecords, joinedRecord)
	}

	log.Printf("action: q3_joiner_join_complete | client_id: %s | input_records: %d | joined_records: %d",
		clientId, len(aggregatedRecords), len(joinedRecords))

	return joinedRecords, nil
}

func (j *Q3Joiner) GetOutputDatasetType() protocol.DatasetType {
	return protocol.DatasetTypeQ3AggWithName
}

func (j *Q3Joiner) AcceptsReferenceType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeStores
}

func (j *Q3Joiner) AcceptsAggregateType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeQ3Agg
}

func (j *Q3Joiner) Cleanup() error {
	j.rawReferenceRecords = nil
	return nil
}

func (j *Q3Joiner) ShouldCleanupAfterEOF() bool {
	return false
}

func (j *Q3Joiner) CacheIncrement(batchIndex int, data []byte) {
}

func (j *Q3Joiner) GetCachedBatchIndices() map[int]bool {
	return nil
}

func (j *Q3Joiner) GetCache() map[int][]byte {
	return nil
}
