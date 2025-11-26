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

// Q4StoreJoiner handles the second join in Q4: joining user-joined data with store names
type Q4StoreJoiner struct {
	rawReferenceRecords []*protocol.StoreRecord
	clientID            string
}

func NewQ4StoreJoiner() *Q4StoreJoiner {
	return &Q4StoreJoiner{
		rawReferenceRecords: make([]*protocol.StoreRecord, 0),
	}
}

func (j *Q4StoreJoiner) Name() string {
	return "q4_joiner_stores"
}

// SerializeReferenceRecords directly serializes reference records
func (j *Q4StoreJoiner) SerializeReferenceRecords(records []protocol.Record, batchIndex int) ([]byte, error) {
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
func (j *Q4StoreJoiner) SerializeBufferedBatch(batch *protocol.BatchMessage) ([]byte, error) {
	if batch == nil || len(batch.Records) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	buf.Grow(len(batch.Records) * 100)

	for _, record := range batch.Records {
		if joinedRecord, ok := record.(*protocol.Q4JoinedWithUserRecord); ok {
			buf.WriteString("B|")
			buf.WriteString(strconv.Itoa(batch.BatchIndex))
			buf.WriteByte('|')
			buf.WriteString(strconv.FormatBool(batch.EOF))
			buf.WriteByte('|')
			buf.WriteString(batch.ClientID)
			buf.WriteByte('|')
			buf.WriteString(joinedRecord.StoreID)
			buf.WriteByte('|')
			buf.WriteString(joinedRecord.UserID)
			buf.WriteByte('|')
			buf.WriteString(joinedRecord.PurchasesQty)
			buf.WriteByte('|')
			buf.WriteString(joinedRecord.Birthdate)
			buf.WriteByte('\n')
		}
	}

	return buf.Bytes(), nil
}

// RestoreBufferedBatches restores buffered batches from disk
func (j *Q4StoreJoiner) RestoreBufferedBatches(data []byte) ([]protocol.BatchMessage, error) {
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
		if len(parts) != 7 {
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

		record := &protocol.Q4JoinedWithUserRecord{
			StoreID:      parts[3],
			UserID:       parts[4],
			PurchasesQty: parts[5],
			Birthdate:    parts[6],
		}
		batchRecords[batchIndex] = append(batchRecords[batchIndex], record)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan q4 store joiner buffered batches: %w", err)
	}

	var batches []protocol.BatchMessage
	for batchIndex, records := range batchRecords {
		meta := batchMetadata[batchIndex]
		batches = append(batches, protocol.BatchMessage{
			Type:        meta.Type,
			DatasetType: protocol.DatasetTypeQ4AggWithUser,
			BatchIndex:  batchIndex,
			EOF:         meta.EOF,
			ClientID:    meta.ClientID,
			Records:     records,
		})
	}

	return batches, nil
}

// restoreState restores reference data from a serialized increment
func (j *Q4StoreJoiner) restoreState(data []byte) error {
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

func (j *Q4StoreJoiner) PerformJoin(userJoinedRecords []protocol.Record, clientId string, historicalIncrements [][]byte) ([]protocol.Record, error) {
	if j.clientID == "" {
		j.clientID = clientId
	}

	if len(historicalIncrements) > 0 {
		log.Printf("action: q4_store_joiner_load_start | client_id: %s | increments: %d", clientId, len(historicalIncrements))

		for i, incrementData := range historicalIncrements {
			if len(incrementData) > 0 {
				if err := j.restoreState(incrementData); err != nil {
					log.Printf("action: q4_store_joiner_load_increment | client_id: %s | increment: %d | result: fail | error: %v",
						clientId, i, err)
				}
			}
		}

		log.Printf("action: q4_store_joiner_load_complete | client_id: %s | increments_merged: %d | raw_records: %d",
			clientId, len(historicalIncrements), len(j.rawReferenceRecords))
	}

	storeIndex := make(map[string]*protocol.StoreRecord, len(j.rawReferenceRecords))
	for _, store := range j.rawReferenceRecords {
		storeIndex[store.StoreID] = store
	}

	log.Printf("action: q4_store_joiner_index_built | client_id: %s | stores: %d", clientId, len(storeIndex))

	var joinedRecords []protocol.Record

	for _, record := range userJoinedRecords {
		userJoinedRecord, ok := record.(*protocol.Q4JoinedWithUserRecord)
		if !ok {
			return nil, fmt.Errorf("expected Q4JoinedWithUserRecord, got %T", record)
		}

		storeRecord, exists := storeIndex[userJoinedRecord.StoreID]
		if !exists {
			continue
		}

		joinedRecord := &protocol.Q4JoinedWithStoreAndUserRecord{
			StoreName:    storeRecord.StoreName,
			PurchasesQty: userJoinedRecord.PurchasesQty,
			Birthdate:    userJoinedRecord.Birthdate,
		}
		joinedRecords = append(joinedRecords, joinedRecord)
	}

	log.Printf("action: q4_store_joiner_join_complete | client_id: %s | input_records: %d | joined_records: %d",
		clientId, len(userJoinedRecords), len(joinedRecords))

	return joinedRecords, nil
}

func (j *Q4StoreJoiner) GetOutputDatasetType() protocol.DatasetType {
	return protocol.DatasetTypeQ4AggWithUserAndStore
}

func (j *Q4StoreJoiner) AcceptsReferenceType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeStores
}

func (j *Q4StoreJoiner) AcceptsAggregateType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeQ4AggWithUser
}

func (j *Q4StoreJoiner) Cleanup() error {
	j.rawReferenceRecords = nil
	return nil
}

func (j *Q4StoreJoiner) ShouldCleanupAfterEOF() bool {
	return false
}
