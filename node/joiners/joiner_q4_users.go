package joiners

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

type Q4UserJoinerState struct {
	Users              map[string]string       // key: user_id, value: birthdate
	BufferedAggBatches []protocol.BatchMessage // Buffered aggregate batches waiting for reference data
}

// Q4UserJoiner handles joining Q4 aggregate data with user information (birthdate)
type Q4UserJoiner struct {
	state    *Q4UserJoinerState
	mu       sync.RWMutex
	clientID string
}

// NewQ4UserJoiner creates a new Q4UserJoiner instance
func NewQ4UserJoiner() *Q4UserJoiner {
	return &Q4UserJoiner{
		state: &Q4UserJoinerState{
			Users:              make(map[string]string),
			BufferedAggBatches: make([]protocol.BatchMessage, 0),
		},
	}
}

// Name returns the name of this joiner
func (j *Q4UserJoiner) Name() string {
	return "q4_joiner_users"
}

// StoreReferenceDataset stores user reference data for future joins
// Only stores user_id and birthdate to optimize memory usage (users dataset is large)
func (j *Q4UserJoiner) StoreReferenceDataset(records []protocol.Record) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	for _, record := range records {
		userRecord, ok := record.(*protocol.UserRecord)
		if !ok {
			return fmt.Errorf("expected UserRecord, got %T", record)
		}

		normalizedUserID := common.NormalizeUserID(userRecord.UserID)
		j.state.Users[normalizedUserID] = userRecord.Birthdate
	}

	return nil
}

// PerformJoin joins Q4 aggregated data with stored user information
func (j *Q4UserJoiner) PerformJoin(aggregatedRecords []protocol.Record, clientId string) ([]protocol.Record, error) {
	if j.clientID == "" {
		j.clientID = clientId
	}

	j.mu.RLock()
	defer j.mu.RUnlock()

	var joinedRecords []protocol.Record

	for _, record := range aggregatedRecords {
		aggRecord, ok := record.(*protocol.Q4AggregatedRecord)
		if !ok {
			return nil, fmt.Errorf("expected Q4AggregatedRecord, got %T", record)
		}

		// Normalize UserID to handle potential ".0" suffix
		normalizedUserID := common.NormalizeUserID(aggRecord.UserID)

		// Join aggregated data with user birthdate
		birthdate, exists := j.state.Users[normalizedUserID]
		if !exists {
			continue // Skip records without matching users
		}

		joinedRecord := &protocol.Q4JoinedWithUserRecord{
			StoreID:      aggRecord.StoreID,
			UserID:       aggRecord.UserID,
			PurchasesQty: aggRecord.PurchasesQty,
			Birthdate:    birthdate,
		}
		joinedRecords = append(joinedRecords, joinedRecord)
	}

	return joinedRecords, nil
}

// GetOutputDatasetType returns the dataset type for Q4 user joined output
func (j *Q4UserJoiner) GetOutputDatasetType() protocol.DatasetType {
	return protocol.DatasetTypeQ4AggWithUser
}

// AcceptsReferenceType checks if this joiner accepts users as reference data
func (j *Q4UserJoiner) AcceptsReferenceType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeUsers
}

// AcceptsAggregateType checks if this joiner accepts Q4 aggregate data
func (j *Q4UserJoiner) AcceptsAggregateType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeQ4Agg
}

// Cleanup releases resources
// Note: We keep reference data because multiple EOF batches may arrive from upstream
// Even though users dataset is large, we need it to handle all EOF batches from multiple aggregates
func (j *Q4UserJoiner) Cleanup() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	// Clear all reference data to release memory
	// Q4 Users can have millions of entries, so cleanup is important
	j.state.Users = nil
	j.state.BufferedAggBatches = nil
	j.state = nil

	return nil
}

func (j *Q4UserJoiner) ShouldCleanupAfterEOF() bool {
	// Q4 Users has large reference data (can be millions of users), cleanup after EOF
	return true
}

// AddBufferedBatch adds a batch to the buffer (thread-safe)
func (j *Q4UserJoiner) AddBufferedBatch(batch protocol.BatchMessage) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.state.BufferedAggBatches = append(j.state.BufferedAggBatches, batch)
}

// GetBufferedBatches returns all buffered batches (thread-safe, returns a copy)
func (j *Q4UserJoiner) GetBufferedBatches() []protocol.BatchMessage {
	j.mu.RLock()
	defer j.mu.RUnlock()
	// Return a copy to avoid race conditions
	batches := make([]protocol.BatchMessage, len(j.state.BufferedAggBatches))
	copy(batches, j.state.BufferedAggBatches)
	return batches
}

// ClearBufferedBatches clears all buffered batches (thread-safe)
func (j *Q4UserJoiner) ClearBufferedBatches() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.state.BufferedAggBatches = make([]protocol.BatchMessage, 0)
}

// SerializeState exports the current joiner state using pipe-delimited format
// Format:
//
//	Reference data: R|user_id|birthdate\n
//	Separator: ---BUFFERED---\n
//	Buffered batches: B|batch_index|eof|client_id|record_type|record_data...\n
func (j *Q4UserJoiner) SerializeState() ([]byte, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	var buf bytes.Buffer
	estimatedSize := len(j.state.Users)*30 + len(j.state.BufferedAggBatches)*100
	buf.Grow(estimatedSize)

	// Serialize reference data with R| prefix
	for userID, birthdate := range j.state.Users {
		buf.WriteString("R|")
		buf.WriteString(userID)
		buf.WriteByte('|')
		buf.WriteString(birthdate)
		buf.WriteByte('\n')
	}

	// Separator between reference data and buffered batches
	buf.WriteString("---BUFFERED---\n")

	// Serialize buffered aggregate batches with B| prefix
	for _, batch := range j.state.BufferedAggBatches {
		buf.WriteString("B|")
		buf.WriteString(fmt.Sprintf("%d", batch.BatchIndex))
		buf.WriteByte('|')
		buf.WriteString(fmt.Sprintf("%t", batch.EOF))
		buf.WriteByte('|')
		buf.WriteString(batch.ClientID)

		// Serialize each Q4AggregatedRecord in the batch
		for _, record := range batch.Records {
			if aggRecord, ok := record.(*protocol.Q4AggregatedRecord); ok {
				buf.WriteByte('|')
				buf.WriteString("Q4AGG")
				buf.WriteByte('|')
				buf.WriteString(aggRecord.UserID)
				buf.WriteByte('|')
				buf.WriteString(aggRecord.StoreID)
				buf.WriteByte('|')
				buf.WriteString(aggRecord.PurchasesQty)
			}
		}
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

// RestoreState restores the joiner state from pipe-delimited format
// Uses streaming to avoid loading entire dataset in memory at once
func (j *Q4UserJoiner) RestoreState(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	j.state.Users = make(map[string]string)
	j.state.BufferedAggBatches = make([]protocol.BatchMessage, 0)

	scanner := bufio.NewScanner(bytes.NewReader(data))
	lineNum := 0
	inBufferedSection := false

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

		if !inBufferedSection {
			// Parse reference data (R|user_id|birthdate)
			if !strings.HasPrefix(line, "R|") {
				log.Printf("action: q4_user_joiner_restore_skip_invalid_reference | line: %d | expected_prefix: R|", lineNum)
				continue
			}

			parts := strings.Split(line[2:], "|") // Skip "R|" prefix
			if len(parts) != 2 {
				log.Printf("action: q4_user_joiner_restore_skip_invalid_line | line: %d | parts: %d", lineNum, len(parts))
				continue
			}

			userID := parts[0]
			birthdate := parts[1]

			j.state.Users[userID] = birthdate
		} else {
			// Parse buffered batches (B|batch_index|eof|client_id|record_type|record_data...)
			if !strings.HasPrefix(line, "B|") {
				log.Printf("action: q4_user_joiner_restore_skip_invalid_buffered | line: %d | expected_prefix: B|", lineNum)
				continue
			}

			parts := strings.Split(line[2:], "|") // Skip "B|" prefix
			if len(parts) < 3 {
				log.Printf("action: q4_user_joiner_restore_skip_invalid_batch | line: %d | parts: %d", lineNum, len(parts))
				continue
			}

			batchIndex, err := strconv.Atoi(parts[0])
			if err != nil {
				log.Printf("action: q4_user_joiner_restore_skip_invalid_batch_index | line: %d | error: %v", lineNum, err)
				continue
			}

			eof := parts[1] == "true"
			clientID := parts[2]

			// Parse Q4AggregatedRecords
			var records []protocol.Record
			i := 3
			for i < len(parts) {
				if i+4 > len(parts) {
					break
				}

				recordType := parts[i]
				if recordType != "Q4AGG" {
					log.Printf("action: q4_user_joiner_restore_skip_unknown_record_type | line: %d | type: %s", lineNum, recordType)
					i++
					continue
				}

				userID := parts[i+1]
				storeID := parts[i+2]
				purchasesQty := parts[i+3]

				records = append(records, &protocol.Q4AggregatedRecord{
					UserID:       userID,
					StoreID:      storeID,
					PurchasesQty: purchasesQty,
				})

				i += 4
			}

			j.state.BufferedAggBatches = append(j.state.BufferedAggBatches, protocol.BatchMessage{
				Type:        protocol.MessageTypeBatch,
				DatasetType: protocol.DatasetTypeQ4Agg,
				BatchIndex:  batchIndex,
				EOF:         eof,
				ClientID:    clientID,
				Records:     records,
			})
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan q4 user joiner snapshot: %w", err)
	}

	log.Printf("action: q4_user_joiner_restore_complete | users: %d | buffered_batches: %d",
		len(j.state.Users), len(j.state.BufferedAggBatches))

	return nil
}
