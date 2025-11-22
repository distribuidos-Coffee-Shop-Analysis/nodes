package joiners

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

type Q4StoreJoinerState struct {
	Stores             map[string]*protocol.StoreRecord // key: store_id, value: store record
	BufferedAggBatches []protocol.BatchMessage          // Buffered aggregate batches waiting for reference data
}

// Q4StoreJoiner handles the second join in Q4: joining user-joined data with store names
type Q4StoreJoiner struct {
	state    *Q4StoreJoinerState
	mu       sync.RWMutex
	clientID string
}

// NewQ4StoreJoiner creates a new Q4StoreJoiner instance
func NewQ4StoreJoiner() *Q4StoreJoiner {
	return &Q4StoreJoiner{
		state: &Q4StoreJoinerState{
			Stores:             make(map[string]*protocol.StoreRecord),
			BufferedAggBatches: make([]protocol.BatchMessage, 0),
		},
	}
}

// Name returns the name of this joiner
func (j *Q4StoreJoiner) Name() string {
	return "q4_joiner_stores"
}

// StoreReferenceDataset stores store reference data for future joins
func (j *Q4StoreJoiner) StoreReferenceDataset(records []protocol.Record) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	for _, record := range records {
		storeRecord, ok := record.(*protocol.StoreRecord)
		if !ok {
			return fmt.Errorf("expected StoreRecord, got %T", record)
		}

		j.state.Stores[storeRecord.StoreID] = storeRecord
	}

	return nil
}

// PerformJoin joins Q4 user-joined data with stored store information to produce final output
func (j *Q4StoreJoiner) PerformJoin(userJoinedRecords []protocol.Record, clientId string) ([]protocol.Record, error) {
	if j.clientID == "" {
		j.clientID = clientId
	}

	j.mu.RLock()
	defer j.mu.RUnlock()

	var joinedRecords []protocol.Record

	for _, record := range userJoinedRecords {
		userJoinedRecord, ok := record.(*protocol.Q4JoinedWithUserRecord)
		if !ok {
			return nil, fmt.Errorf("expected Q4JoinedWithUserRecord, got %T", record)
		}

		// Join user-joined data with store names
		storeRecord, exists := j.state.Stores[userJoinedRecord.StoreID]
		if !exists {
			continue // Skip records without matching stores
		}

		joinedRecord := &protocol.Q4JoinedWithStoreAndUserRecord{
			StoreName:    storeRecord.StoreName,
			PurchasesQty: userJoinedRecord.PurchasesQty,
			Birthdate:    userJoinedRecord.Birthdate,
		}
		joinedRecords = append(joinedRecords, joinedRecord)
	}

	return joinedRecords, nil
}

// GetOutputDatasetType returns the dataset type for Q4 final joined output
func (j *Q4StoreJoiner) GetOutputDatasetType() protocol.DatasetType {
	return protocol.DatasetTypeQ4AggWithUserAndStore
}

// AcceptsReferenceType checks if this joiner accepts stores as reference data
func (j *Q4StoreJoiner) AcceptsReferenceType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeStores
}

// AcceptsAggregateType checks if this joiner accepts Q4 user-joined data
func (j *Q4StoreJoiner) AcceptsAggregateType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeQ4AggWithUser
}

// Cleanup releases resources
// Note: We keep reference data because multiple EOF batches may arrive from upstream
// (e.g., 5 Q4 User Joiners all send EOF to 1 Q4 Store Joiner)
// Stores dataset is tiny (~10 rows, ~1KB) so keeping it in memory is fine
func (j *Q4StoreJoiner) Cleanup() error {
	// No-op: we keep reference data to handle multiple EOF batches
	return nil
}

func (j *Q4StoreJoiner) ShouldCleanupAfterEOF() bool {
	// Q4 Stores has small reference data, keep in memory
	return false
}

// AddBufferedBatch adds a batch to the buffer (thread-safe)
func (j *Q4StoreJoiner) AddBufferedBatch(batch protocol.BatchMessage) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.state.BufferedAggBatches = append(j.state.BufferedAggBatches, batch)
}

// GetBufferedBatches returns all buffered batches (thread-safe, returns a copy)
func (j *Q4StoreJoiner) GetBufferedBatches() []protocol.BatchMessage {
	j.mu.RLock()
	defer j.mu.RUnlock()
	// Return a copy to avoid race conditions
	batches := make([]protocol.BatchMessage, len(j.state.BufferedAggBatches))
	copy(batches, j.state.BufferedAggBatches)
	return batches
}

// ClearBufferedBatches clears all buffered batches (thread-safe)
func (j *Q4StoreJoiner) ClearBufferedBatches() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.state.BufferedAggBatches = make([]protocol.BatchMessage, 0)
}

// SerializeState exports the current joiner state using pipe-delimited format
// Format:
//
//	Reference data: R|store_id|store_name|street|postal_code|city|state|latitude|longitude\n
//	Separator: ---BUFFERED---\n
//	Buffered batches: B|batch_index|eof|client_id|record_type|record_data...\n
func (j *Q4StoreJoiner) SerializeState() ([]byte, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	var buf bytes.Buffer
	estimatedSize := len(j.state.Stores)*150 + len(j.state.BufferedAggBatches)*100
	buf.Grow(estimatedSize)

	// Serialize reference data with R| prefix
	for storeID, store := range j.state.Stores {
		buf.WriteString("R|")
		buf.WriteString(storeID)
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

		// Serialize each Q4JoinedWithUserRecord in the batch
		for _, record := range batch.Records {
			if joinedRecord, ok := record.(*protocol.Q4JoinedWithUserRecord); ok {
				buf.WriteByte('|')
				buf.WriteString("Q4JOINED")
				buf.WriteByte('|')
				buf.WriteString(joinedRecord.StoreID)
				buf.WriteByte('|')
				buf.WriteString(joinedRecord.UserID)
				buf.WriteByte('|')
				buf.WriteString(joinedRecord.PurchasesQty)
				buf.WriteByte('|')
				buf.WriteString(joinedRecord.Birthdate)
			}
		}
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

// RestoreState restores the joiner state from pipe-delimited format
func (j *Q4StoreJoiner) RestoreState(data []byte) error {
	if len(data) == 0 {
		log.Printf("action: q4_store_joiner_restore_start | result: empty_data")
		return nil
	}

	log.Printf("action: q4_store_joiner_restore_start | data_size: %d", len(data))

	j.mu.Lock()
	defer j.mu.Unlock()

	j.state.Stores = make(map[string]*protocol.StoreRecord)
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
			// Parse reference data (R|store_id|store_name|street|postal_code|city|state|latitude|longitude)
			if !strings.HasPrefix(line, "R|") {
				log.Printf("action: q4_store_joiner_restore_skip_invalid_reference | line: %d | expected_prefix: R|", lineNum)
				continue
			}

			parts := strings.Split(line[2:], "|") // Skip "R|" prefix
			if len(parts) != 8 {
				log.Printf("action: q4_store_joiner_restore_skip_invalid_line | line: %d | parts: %d", lineNum, len(parts))
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

			j.state.Stores[store.StoreID] = store
		} else {
			// Parse buffered batches (B|batch_index|eof|client_id|record_type|record_data...)
			if !strings.HasPrefix(line, "B|") {
				log.Printf("action: q4_store_joiner_restore_skip_invalid_buffered | line: %d | expected_prefix: B|", lineNum)
				continue
			}

			parts := strings.Split(line[2:], "|") // Skip "B|" prefix
			if len(parts) < 3 {
				log.Printf("action: q4_store_joiner_restore_skip_invalid_batch | line: %d | parts: %d", lineNum, len(parts))
				continue
			}

			batchIndex, err := strconv.Atoi(parts[0])
			if err != nil {
				log.Printf("action: q4_store_joiner_restore_skip_invalid_batch_index | line: %d | error: %v", lineNum, err)
				continue
			}

			eof := parts[1] == "true"
			clientID := parts[2]

			// Parse Q4JoinedWithUserRecords
			var records []protocol.Record
			i := 3
			for i < len(parts) {
				if i+5 > len(parts) {
					break
				}

				recordType := parts[i]
				if recordType != "Q4JOINED" {
					log.Printf("action: q4_store_joiner_restore_skip_unknown_record_type | line: %d | type: %s", lineNum, recordType)
					i++
					continue
				}

				storeID := parts[i+1]
				userID := parts[i+2]
				purchasesQty := parts[i+3]
				birthdate := parts[i+4]

				records = append(records, &protocol.Q4JoinedWithUserRecord{
					StoreID:      storeID,
					UserID:       userID,
					PurchasesQty: purchasesQty,
					Birthdate:    birthdate,
				})

				i += 5
			}

			j.state.BufferedAggBatches = append(j.state.BufferedAggBatches, protocol.BatchMessage{
				Type:        protocol.MessageTypeBatch,
				DatasetType: protocol.DatasetTypeQ4AggWithUser,
				BatchIndex:  batchIndex,
				EOF:         eof,
				ClientID:    clientID,
				Records:     records,
			})
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan q4 store joiner snapshot: %w", err)
	}

	log.Printf("action: q4_store_joiner_restore_complete | stores: %d | buffered_batches: %d",
		len(j.state.Stores), len(j.state.BufferedAggBatches))

	return nil
}
