package common

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"

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

// GetJoinerPartition calculates the joiner partition for a given user_id using consistent hashing.
//
// This ensures that the same user_id always maps to the same joiner node
//
// Args:
//   - userID: The user ID to hash (string)
//   - joinersCount: Total number of joiner nodes
//
// Returns:
//   - int: The partition number (1 to joinersCount) - 1-based indexing
func GetJoinerPartition(userID string, joinersCount int) int {
	// Create SHA256 hash (same as Python implementation in connection-node)
	hash := sha256.Sum256([]byte(userID))
	hashHex := hex.EncodeToString(hash[:])

	// Convert hex string to big integer
	hashInt := new(big.Int)
	hashInt.SetString(hashHex, 16)

	// Calculate modulo to get partition
	joinersCountBig := big.NewInt(int64(joinersCount))
	partition := new(big.Int).Mod(hashInt, joinersCountBig)

	// Return 1-based partition (1 to joinersCount)
	return int(partition.Int64()) + 1
}

// NodeIDToPartition converts NODE_ID string to partition number (int)
// Both NODE_ID and partitions are 1-based
// Example: "1" -> 1, "2" -> 2, "3" -> 3
// Returns -1 if conversion fails or if nodeID < 1
func NodeIDToPartition(nodeID string) int {
	var nodeNum int
	_, err := fmt.Sscanf(nodeID, "%d", &nodeNum)
	if err != nil || nodeNum < 1 {
		return -1
	}
	return nodeNum
}

// BuildQ4UserJoinerRoutingKey builds the routing key for Q4 User Joiner based on partition
// Format: "joiner.{partition}.q4_agg"
// Example: partition 1 -> "joiner.1.q4_agg", partition 2 -> "joiner.2.q4_agg"
func BuildQ4UserJoinerRoutingKey(partition int) string {
	return fmt.Sprintf("joiner.%d.q4_agg", partition)
}
