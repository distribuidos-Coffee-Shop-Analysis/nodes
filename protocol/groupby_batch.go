package protocol

import (
	"fmt"
	"strconv"
	"strings"
)

type GroupByBatch struct {
	BatchIndex int
	Records    []Record
	EOF        bool
}

// NewGroupByBatch creates a new GroupByBatch
func NewGroupByBatch(batchIndex int, records []Record, eof bool) *GroupByBatch {
	return &GroupByBatch{
		BatchIndex: batchIndex,
		Records:    records,
		EOF:        eof,
	}
}

// Serialize encodes GroupByBatch to byte array
func (gb *GroupByBatch) Serialize() []byte {
	eofValue := "0"
	if gb.EOF {
		eofValue = "1"
	}

	content := fmt.Sprintf("%d|%s|%d", gb.BatchIndex, eofValue, len(gb.Records))
	for _, record := range gb.Records {
		content += "|" + record.Serialize()
	}

	return []byte(content)
}

// GroupByBatchFromData parses GroupByBatch from byte array
// recordFactory is a function that creates records from string parts 
// fieldsPerRecord is the number of fields each record has
func GroupByBatchFromData(data []byte, recordFactory RecordFactory, fieldsPerRecord int) (*GroupByBatch, error) {
	content := string(data)
	parts := strings.Split(content, "|")

	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid groupby batch format: missing BatchIndex, EOF or RecordCount")
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

	dataParts := parts[3:] 

	records := make([]Record, 0, recordCount)
	for i := 0; i < recordCount; i++ {
		startIdx := i * fieldsPerRecord
		endIdx := startIdx + fieldsPerRecord

		if endIdx <= len(dataParts) {
			recordFields := dataParts[startIdx:endIdx]
			record, err := recordFactory(recordFields)
			if err != nil {
				continue
			}
			records = append(records, record)
		}
	}

	return &GroupByBatch{
		BatchIndex: batchIndex,
		Records:    records,
		EOF:        eof,
	}, nil
}
