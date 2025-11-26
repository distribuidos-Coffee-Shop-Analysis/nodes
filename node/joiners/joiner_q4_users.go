package joiners

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q4UserRawRecord is a lightweight representation of user data for joins
type Q4UserRawRecord struct {
	UserID    string
	Birthdate string
}

type Q4UserJoiner struct {
	rawReferenceRecords []Q4UserRawRecord
	clientID            string
}

func NewQ4UserJoiner() *Q4UserJoiner {
	return &Q4UserJoiner{
		rawReferenceRecords: make([]Q4UserRawRecord, 0),
	}
}

func (j *Q4UserJoiner) Name() string {
	return "q4_joiner_users"
}

// SerializeReferenceRecords directly serializes reference records
func (j *Q4UserJoiner) SerializeReferenceRecords(records []protocol.Record, batchIndex int) ([]byte, error) {
	if len(records) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	buf.Grow(len(records)*30 + 20)

	buf.WriteString("BATCH|")
	buf.WriteString(strconv.Itoa(batchIndex))
	buf.WriteByte('\n')

	for _, record := range records {
		userRecord, ok := record.(*protocol.UserRecord)
		if !ok {
			continue
		}

		normalizedUserID := common.NormalizeUserID(userRecord.UserID)
		buf.WriteString("R|")
		buf.WriteString(normalizedUserID)
		buf.WriteByte('|')
		buf.WriteString(userRecord.Birthdate)
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

// SerializeBufferedBatch directly serializes a buffered batch
func (j *Q4UserJoiner) SerializeBufferedBatch(batch *protocol.BatchMessage) ([]byte, error) {
	if batch == nil || len(batch.Records) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	buf.Grow(len(batch.Records) * 80)

	for _, record := range batch.Records {
		if aggRecord, ok := record.(*protocol.Q4AggregatedRecord); ok {
			buf.WriteString("B|")
			buf.WriteString(strconv.Itoa(batch.BatchIndex))
			buf.WriteByte('|')
			buf.WriteString(strconv.FormatBool(batch.EOF))
			buf.WriteByte('|')
			buf.WriteString(batch.ClientID)
			buf.WriteByte('|')
			buf.WriteString(aggRecord.UserID)
			buf.WriteByte('|')
			buf.WriteString(aggRecord.StoreID)
			buf.WriteByte('|')
			buf.WriteString(aggRecord.PurchasesQty)
			buf.WriteByte('\n')
		}
	}

	return buf.Bytes(), nil
}

// RestoreBufferedBatches restores buffered batches from disk
func (j *Q4UserJoiner) RestoreBufferedBatches(data []byte) ([]protocol.BatchMessage, error) {
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

		record := &protocol.Q4AggregatedRecord{
			UserID:       parts[3],
			StoreID:      parts[4],
			PurchasesQty: parts[5],
		}
		batchRecords[batchIndex] = append(batchRecords[batchIndex], record)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan q4 user joiner buffered batches: %w", err)
	}

	var batches []protocol.BatchMessage
	for batchIndex, records := range batchRecords {
		meta := batchMetadata[batchIndex]
		batches = append(batches, protocol.BatchMessage{
			Type:        meta.Type,
			DatasetType: protocol.DatasetTypeQ4Agg,
			BatchIndex:  batchIndex,
			EOF:         meta.EOF,
			ClientID:    meta.ClientID,
			Records:     records,
		})
	}

	return batches, nil
}

// restoreState restores reference data from a serialized increment
func (j *Q4UserJoiner) restoreState(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if j.rawReferenceRecords == nil {
		j.rawReferenceRecords = make([]Q4UserRawRecord, 0)
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 || !strings.HasPrefix(line, "R|") {
			continue
		}

		parts := strings.Split(line[2:], "|")
		if len(parts) != 2 {
			continue
		}

		j.rawReferenceRecords = append(j.rawReferenceRecords, Q4UserRawRecord{
			UserID:    parts[0],
			Birthdate: parts[1],
		})
	}

	return scanner.Err()
}

func (j *Q4UserJoiner) PerformJoin(aggregatedRecords []protocol.Record, clientId string, historicalIncrements [][]byte) ([]protocol.Record, error) {
	if j.clientID == "" {
		j.clientID = clientId
	}

	if len(historicalIncrements) > 0 {
		log.Printf("action: q4_user_joiner_load_start | client_id: %s | increments: %d",
			clientId, len(historicalIncrements))

		for i, incrementData := range historicalIncrements {
			if len(incrementData) > 0 {
				if err := j.restoreState(incrementData); err != nil {
					log.Printf("action: q4_user_joiner_load_increment | client_id: %s | increment: %d | result: fail | error: %v",
						clientId, i, err)
				}
			}
		}

		log.Printf("action: q4_user_joiner_load_complete | client_id: %s | increments_merged: %d | raw_records: %d",
			clientId, len(historicalIncrements), len(j.rawReferenceRecords))
	}

	userIndex := make(map[string]string, len(j.rawReferenceRecords))
	for _, rawUser := range j.rawReferenceRecords {
		userIndex[rawUser.UserID] = rawUser.Birthdate
	}

	log.Printf("action: q4_user_joiner_index_built | client_id: %s | unique_users: %d",
		clientId, len(userIndex))

	var joinedRecords []protocol.Record

	for _, record := range aggregatedRecords {
		aggRecord, ok := record.(*protocol.Q4AggregatedRecord)
		if !ok {
			return nil, fmt.Errorf("expected Q4AggregatedRecord, got %T", record)
		}

		normalizedUserID := common.NormalizeUserID(aggRecord.UserID)

		birthdate, exists := userIndex[normalizedUserID]
		if !exists {
			continue
		}

		joinedRecord := &protocol.Q4JoinedWithUserRecord{
			StoreID:      aggRecord.StoreID,
			UserID:       aggRecord.UserID,
			PurchasesQty: aggRecord.PurchasesQty,
			Birthdate:    birthdate,
		}
		joinedRecords = append(joinedRecords, joinedRecord)
	}

	log.Printf("action: q4_user_joiner_join_complete | client_id: %s | input_records: %d | joined_records: %d",
		clientId, len(aggregatedRecords), len(joinedRecords))

	return joinedRecords, nil
}

func (j *Q4UserJoiner) GetOutputDatasetType() protocol.DatasetType {
	return protocol.DatasetTypeQ4AggWithUser
}

func (j *Q4UserJoiner) AcceptsReferenceType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeUsers
}

func (j *Q4UserJoiner) AcceptsAggregateType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeQ4Agg
}

func (j *Q4UserJoiner) Cleanup() error {
	j.rawReferenceRecords = nil
	return nil
}

func (j *Q4UserJoiner) ShouldCleanupAfterEOF() bool {
	return true
}
