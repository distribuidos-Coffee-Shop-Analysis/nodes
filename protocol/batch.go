package protocol

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

// BatchMessage represents multiple records sent together for a specific dataset
type BatchMessage struct {
	Type        int
	DatasetType DatasetType
	BatchIndex  int
	Records     []Record
	EOF         bool
}

// NewBatchMessage creates a new batch message
func NewBatchMessage(datasetType DatasetType, batchIndex int, records []Record, eof bool) *BatchMessage {
	return &BatchMessage{
		Type:        MessageTypeBatch,
		DatasetType: datasetType,
		BatchIndex:  batchIndex,
		Records:     records,
		EOF:         eof,
	}
}

// BatchMessageFromData parses batch message from custom protocol data
func BatchMessageFromData(data []byte) (*BatchMessage, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("invalid batch message: too short")
	}

	if data[0] != MessageTypeBatch {
		return nil, fmt.Errorf("invalid batch message: not a batch message")
	}

	datasetType := DatasetType(data[1])
	content := string(data[2:])
	parts := strings.Split(content, "|")

	log.Printf("action: parse_batch_message | dataset_type: %d | content: %s", datasetType, content)

	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid batch message format: missing BatchIndex, EOF or RecordCount")
	}

	batchIndex, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid batch index: %v", err)
	}

	eof := parts[1] == "1"
	recordCount, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid record count: %v", err)
	}

	recordClass, fieldsPerRecord, err := getRecordClassAndFields(datasetType)
	if err != nil {
		return nil, err
	}

	log.Printf("action: parse_batch_message | batch_index: %d | eof: %t | record_count: %d | fields_per_record: %d",
		batchIndex, eof, recordCount, fieldsPerRecord)

	dataParts := parts[3:] // Skip BatchIndex, EOF and RecordCount

	records := make([]Record, 0, recordCount)
	for i := 0; i < recordCount; i++ {
		startIdx := i * fieldsPerRecord
		endIdx := startIdx + fieldsPerRecord

		if endIdx <= len(dataParts) {
			recordFields := dataParts[startIdx:endIdx]

			log.Printf("action: reconstruct_record | index: %d | fields: %v", i, recordFields)

			record, err := recordClass(recordFields)
			if err != nil {
				log.Printf("action: create_record | index: %d | error: %v", i, err)
				continue
			}

			records = append(records, record)
			log.Printf("action: create_record | index: %d | success: true", i)
		}
	}

	return &BatchMessage{
		Type:        MessageTypeBatch,
		DatasetType: datasetType,
		BatchIndex:  batchIndex,
		Records:     records,
		EOF:         eof,
	}, nil
}

// ResponseMessage represents server response to client
type ResponseMessage struct {
	Type    int
	Success bool
	Error   string
}

// NewResponseMessage creates a new response message
func NewResponseMessage(success bool, errorMsg string) *ResponseMessage {
	return &ResponseMessage{
		Type:    MessageTypeResponse,
		Success: success,
		Error:   errorMsg,
	}
}

// RecordFactory is a function type for creating records from parts
type RecordFactory func([]string) (Record, error)

// getRecordClassAndFields returns the record factory and field count for a dataset type
func getRecordClassAndFields(datasetType DatasetType) (RecordFactory, int, error) {
	switch datasetType {
	case DatasetTypeMenuItems:
		return func(parts []string) (Record, error) {
			return NewMenuItemRecordFromParts(parts)
		}, MenuItemRecordParts, nil
	case DatasetTypeStores:
		return func(parts []string) (Record, error) {
			return NewStoreRecordFromParts(parts)
		}, StoreRecordParts, nil
	case DatasetTypeTransactionItems:
		return func(parts []string) (Record, error) {
			return NewTransactionItemRecordFromParts(parts)
		}, TransactionItemRecordParts, nil
	case DatasetTypeTransactions:
		return func(parts []string) (Record, error) {
			return NewTransactionRecordFromParts(parts)
		}, TransactionRecordParts, nil
	case DatasetTypeUsers:
		return func(parts []string) (Record, error) {
			return NewUserRecordFromParts(parts)
		}, UserRecordParts, nil
	case DatasetTypeQ1:
		return func(parts []string) (Record, error) {
			return NewQ1RecordFromParts(parts)
		}, Q1RecordParts, nil
	case DatasetTypeQ2:
		return func(parts []string) (Record, error) {
			return NewQ2RecordFromParts(parts)
		}, Q2RecordParts, nil
	case DatasetTypeQ3:
		return func(parts []string) (Record, error) {
			return NewQ3RecordFromParts(parts)
		}, Q3RecordParts, nil
	case DatasetTypeQ4:
		return func(parts []string) (Record, error) {
			return NewQ4RecordFromParts(parts)
		}, Q4RecordParts, nil
	default:
		return nil, 0, fmt.Errorf("unknown dataset type: %d", datasetType)
	}
}

// CreateRecordFromString creates the appropriate record type from string data
func CreateRecordFromString(datasetType DatasetType, data string) (Record, error) {
	recordFactory, _, err := getRecordClassAndFields(datasetType)
	if err != nil {
		return nil, err
	}

	parts := strings.Split(data, "|")
	return recordFactory(parts)
}
