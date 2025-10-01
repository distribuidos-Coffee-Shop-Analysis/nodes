package common

import (
	"fmt"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// encodeToByteArray encodes batch message to byte array
func EncodeToByteArray(batchMessage *protocol.BatchMessage) []byte {
	// [MessageType][DatasetType][BatchIndex|EOF|RecordCount|Records...]
	data := make([]byte, 0)
	data = append(data, protocol.MessageTypeBatch)
	data = append(data, byte(batchMessage.DatasetType))

	// Check if this is a Q2 dataset type that needs special encoding
	if isQ2DualDatasetType(batchMessage.DatasetType) {
		content := encodeQ2DualDataset(batchMessage)
		data = append(data, []byte(content)...)
		return data
	}

	// Build content: BatchIndex|EOF|RecordCount|Record1|Record2|...
	eofValue := "0"
	if batchMessage.EOF {
		eofValue = "1"
	}

	content := fmt.Sprintf("%d|%s|%d", batchMessage.BatchIndex, eofValue, len(batchMessage.Records))
	for _, record := range batchMessage.Records {
		content += "|" + record.Serialize()
	}

	data = append(data, []byte(content)...)
	return data
}

// isQ2DualDatasetType checks if the dataset type requires dual subdataset encoding
func isQ2DualDatasetType(dt protocol.DatasetType) bool {
	return dt == protocol.DatasetTypeQ2Groups ||
		dt == protocol.DatasetTypeQ2Agg ||
		dt == protocol.DatasetTypeQ2AggWithName
}

// encodeQ2DualDataset encodes Q2 datasets with two subdatasets
// Format: BatchIndex|EOF|len(subset1)|records_subset1|len(subset2)|records_subset2
func encodeQ2DualDataset(batchMessage *protocol.BatchMessage) string {
	eofValue := "0"
	if batchMessage.EOF {
		eofValue = "1"
	}

	// Separate records into two groups based on their specific type
	group1, group2 := separateQ2Records(batchMessage.Records, batchMessage.DatasetType)

	// Build: BatchIndex|EOF|len(group1)|group1_records|len(group2)|group2_records
	content := fmt.Sprintf("%d|%s|%d", batchMessage.BatchIndex, eofValue, len(group1))

	// Serialize group1 records
	for _, record := range group1 {
		content += "|" + record.Serialize()
	}

	// Add group2 length
	content += fmt.Sprintf("|%d", len(group2))

	// Serialize group2 records
	for _, record := range group2 {
		content += "|" + record.Serialize()
	}

	return content
}

// separateQ2Records separates Q2 records into two groups based on dataset type
func separateQ2Records(records []protocol.Record, datasetType protocol.DatasetType) ([]protocol.Record, []protocol.Record) {
	var group1, group2 []protocol.Record

	for _, record := range records {
		switch datasetType {
		case protocol.DatasetTypeQ2Groups:
			// Group 1: Q2GroupWithQuantityRecord, Group 2: Q2GroupWithSubtotalRecord
			if _, ok := record.(*protocol.Q2GroupWithQuantityRecord); ok {
				group1 = append(group1, record)
			} else if _, ok := record.(*protocol.Q2GroupWithSubtotalRecord); ok {
				group2 = append(group2, record)
			}
		case protocol.DatasetTypeQ2Agg:
			// Group 1: Q2BestSellingRecord, Group 2: Q2MostProfitsRecord
			if _, ok := record.(*protocol.Q2BestSellingRecord); ok {
				group1 = append(group1, record)
			} else if _, ok := record.(*protocol.Q2MostProfitsRecord); ok {
				group2 = append(group2, record)
			}
		case protocol.DatasetTypeQ2AggWithName:
			// Group 1: Q2BestSellingWithNameRecord, Group 2: Q2MostProfitsWithNameRecord
			if _, ok := record.(*protocol.Q2BestSellingWithNameRecord); ok {
				group1 = append(group1, record)
			} else if _, ok := record.(*protocol.Q2MostProfitsWithNameRecord); ok {
				group2 = append(group2, record)
			}
		}
	}

	return group1, group2
}

