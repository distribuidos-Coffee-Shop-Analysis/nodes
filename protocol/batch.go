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

// NewGroupByBatch creates a batch message for grouped data, determining the dataset type from records
func NewGroupByBatch(batchIndex int, records []Record, eof bool) *BatchMessage {
	// Determine dataset type from the first record
	var datasetType DatasetType = DatasetTypeQ4Groups // default

	if len(records) > 0 {
		datasetType = records[0].GetType()
	}

	return &BatchMessage{
		Type:        MessageTypeBatch,
		DatasetType: datasetType,
		BatchIndex:  batchIndex,
		Records:     records,
		EOF:         eof,
	}
}

// NewQ2GroupByBatch creates a batch message specifically for Q2 grouped data with dual subdatasets
func NewQ2GroupByBatch(batchIndex int, records []Record, eof bool) *BatchMessage {
	return &BatchMessage{
		Type:        MessageTypeBatch,
		DatasetType: DatasetTypeQ2Groups,
		BatchIndex:  batchIndex,
		Records:     records,
		EOF:         eof,
	}
}

// NewQ2AggregateBatch creates a batch message specifically for Q2 aggregated data with dual subdatasets
func NewQ2AggregateBatch(batchIndex int, records []Record, eof bool) *BatchMessage {
	return &BatchMessage{
		Type:        MessageTypeBatch,
		DatasetType: DatasetTypeQ2Agg, // Q2Agg for aggregate output
		BatchIndex:  batchIndex,
		Records:     records,
		EOF:         eof,
	}
}

// NewQ3GroupByBatch creates a batch message specifically for Q3 grouped data
func NewQ3GroupByBatch(batchIndex int, records []Record, eof bool) *BatchMessage {
	return &BatchMessage{
		Type:        MessageTypeBatch,
		DatasetType: DatasetTypeQ3Groups,
		BatchIndex:  batchIndex,
		Records:     records,
		EOF:         eof,
	}
}

// NewQ4GroupByBatch creates a batch message specifically for Q4 grouped data
func NewQ4GroupByBatch(batchIndex int, records []Record, eof bool) *BatchMessage {
	return &BatchMessage{
		Type:        MessageTypeBatch,
		DatasetType: DatasetTypeQ4Groups,
		BatchIndex:  batchIndex,
		Records:     records,
		EOF:         eof,
	}
}

// NewAggregateBatch creates a batch message for aggregated data
func NewAggregateBatch(batchIndex int, records []Record, eof bool) *BatchMessage {
	// Determine dataset type from the first record
	var datasetType DatasetType = DatasetTypeQ4Agg // default

	if len(records) > 0 {
		datasetType = records[0].GetType()
	}

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

	// Check if this is a Q2 dual dataset type
	if isQ2DualDatasetType(datasetType) {
		records, err := parseQ2DualDataset(parts[2:], datasetType)
		if err != nil {
			return nil, err
		}
		return &BatchMessage{
			Type:        MessageTypeBatch,
			DatasetType: datasetType,
			BatchIndex:  batchIndex,
			Records:     records,
			EOF:         eof,
		}, nil
	}

	// Regular parsing for non-Q2 datasets
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

// isQ2DualDatasetType checks if the dataset type requires dual subdataset parsing
func isQ2DualDatasetType(dt DatasetType) bool {
	return dt == DatasetTypeQ2Groups ||
		dt == DatasetTypeQ2Agg ||
		dt == DatasetTypeQ2AggWithName
}

// parseQ2DualDataset parses Q2 datasets with two subdatasets
// Format: len(subset1)|records_subset1|len(subset2)|records_subset2
func parseQ2DualDataset(parts []string, datasetType DatasetType) ([]Record, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid Q2 dual dataset format: too short")
	}

	// Get record factories for both groups
	factory1, fields1, factory2, fields2, err := getQ2RecordFactories(datasetType)
	if err != nil {
		return nil, err
	}

	// Parse group 1 count
	count1, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid group1 count: %v", err)
	}

	log.Printf("action: parse_q2_dual | dataset_type: %d | group1_count: %d | group1_fields: %d",
		datasetType, count1, fields1)

	var allRecords []Record
	currentIdx := 1

	// Parse group 1 records
	for i := 0; i < count1; i++ {
		endIdx := currentIdx + fields1
		if endIdx > len(parts) {
			return nil, fmt.Errorf("insufficient data for group1 record %d", i)
		}
		recordFields := parts[currentIdx:endIdx]
		record, err := factory1(recordFields)
		if err != nil {
			log.Printf("action: parse_q2_group1_record | index: %d | error: %v", i, err)
			currentIdx = endIdx
			continue
		}
		allRecords = append(allRecords, record)
		currentIdx = endIdx
	}

	// Parse group 2 count
	if currentIdx >= len(parts) {
		return nil, fmt.Errorf("missing group2 count")
	}
	count2, err := strconv.Atoi(parts[currentIdx])
	if err != nil {
		return nil, fmt.Errorf("invalid group2 count: %v", err)
	}
	currentIdx++

	log.Printf("action: parse_q2_dual | dataset_type: %d | group2_count: %d | group2_fields: %d",
		datasetType, count2, fields2)

	// Parse group 2 records
	for i := 0; i < count2; i++ {
		endIdx := currentIdx + fields2
		if endIdx > len(parts) {
			return nil, fmt.Errorf("insufficient data for group2 record %d", i)
		}
		recordFields := parts[currentIdx:endIdx]
		record, err := factory2(recordFields)
		if err != nil {
			log.Printf("action: parse_q2_group2_record | index: %d | error: %v", i, err)
			currentIdx = endIdx
			continue
		}
		allRecords = append(allRecords, record)
		currentIdx = endIdx
	}

	log.Printf("action: parse_q2_dual_complete | total_records: %d | group1: %d | group2: %d",
		len(allRecords), count1, count2)

	return allRecords, nil
}

// getQ2RecordFactories returns the two record factories for Q2 dual datasets
func getQ2RecordFactories(datasetType DatasetType) (RecordFactory, int, RecordFactory, int, error) {
	switch datasetType {
	case DatasetTypeQ2Groups:
		return func(parts []string) (Record, error) {
				return NewQ2GroupWithQuantityRecordFromParts(parts)
			}, Q2GroupWithQuantityRecordParts,
			func(parts []string) (Record, error) {
				return NewQ2GroupWithSubtotalRecordFromParts(parts)
			}, Q2GroupWithSubtotalRecordParts, nil
	case DatasetTypeQ2Agg:
		return func(parts []string) (Record, error) {
				return NewQ2BestSellingRecordFromParts(parts)
			}, Q2BestSellingRecordParts,
			func(parts []string) (Record, error) {
				return NewQ2MostProfitsRecordFromParts(parts)
			}, Q2MostProfitsRecordParts, nil
	case DatasetTypeQ2AggWithName:
		return func(parts []string) (Record, error) {
				return NewQ2BestSellingWithNameRecordFromParts(parts)
			}, Q2BestSellingWithNameRecordParts,
			func(parts []string) (Record, error) {
				return NewQ2MostProfitsWithNameRecordFromParts(parts)
			}, Q2MostProfitsWithNameRecordParts, nil
	default:
		return nil, 0, nil, 0, fmt.Errorf("not a Q2 dual dataset type: %d", datasetType)
	}
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
	case DatasetTypeQ3Groups:
		return func(parts []string) (Record, error) {
			return NewQ3GroupedRecordFromParts(parts)
		}, Q3GroupedRecordParts, nil
	case DatasetTypeQ3Agg:
		return func(parts []string) (Record, error) {
			return NewQ3AggregatedRecordFromParts(parts)
		}, Q3AggregatedRecordParts, nil
	case DatasetTypeQ3AggWithName:
		return func(parts []string) (Record, error) {
			return NewQ3JoinedRecordFromParts(parts)
		}, Q3JoinedRecordParts, nil
	case DatasetTypeQ4Groups:
		return func(parts []string) (Record, error) {
			return NewQ4GroupedRecordFromParts(parts)
		}, Q4GroupedRecordParts, nil
	case DatasetTypeQ4Agg:
		return func(parts []string) (Record, error) {
			return NewQ4AggregatedRecordFromParts(parts)
		}, Q4AggregatedRecordParts, nil
	case DatasetTypeQ4AggWithUser:
		return func(parts []string) (Record, error) {
			return NewQ4JoinedWithUserRecordFromParts(parts)
		}, Q4JoinedWithUserRecordParts, nil
	case DatasetTypeQ4AggWithUserAndStore:
		return func(parts []string) (Record, error) {
			return NewQ4JoinedWithStoreAndUserRecordFromParts(parts)
		}, Q4JoinedWithStoreAndUserRecordParts, nil
	// Q2 dataset types are handled separately with dual parsing in parseQ2DualDataset
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
