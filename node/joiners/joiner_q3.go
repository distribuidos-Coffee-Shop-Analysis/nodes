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

type Q3JoinerState struct {
	Stores             map[string]*protocol.StoreRecord // key: store_id, value: store record
	BufferedAggBatches []protocol.BatchMessage          // Buffered aggregate batches waiting for reference data
}

// Q3Joiner handles joining Q3 aggregate data with store names
type Q3Joiner struct {
	state    *Q3JoinerState
	mu       sync.RWMutex
	clientID string
}

// NewQ3Joiner creates a new Q3Joiner instance
func NewQ3Joiner() *Q3Joiner {
	return &Q3Joiner{
		state: &Q3JoinerState{
			Stores:             make(map[string]*protocol.StoreRecord),
			BufferedAggBatches: make([]protocol.BatchMessage, 0),
		},
	}
}

// Name returns the name of this joiner
func (j *Q3Joiner) Name() string {
	return "q3_joiner_stores"
}

// StoreReferenceDataset stores store reference data for future joins
func (j *Q3Joiner) StoreReferenceDataset(records []protocol.Record) error {
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

// PerformJoin joins Q3 aggregated data with stored store information
func (j *Q3Joiner) PerformJoin(aggregatedRecords []protocol.Record, clientId string) ([]protocol.Record, error) {
	if j.clientID == "" {
		j.clientID = clientId
	}

	j.mu.RLock()
	defer j.mu.RUnlock()

	var joinedRecords []protocol.Record

	for _, record := range aggregatedRecords {
		aggRecord, ok := record.(*protocol.Q3AggregatedRecord)
		if !ok {
			return nil, fmt.Errorf("expected Q3AggregatedRecord, got %T", record)
		}

		// Join aggregated data with store names
		storeRecord, exists := j.state.Stores[aggRecord.StoreID]
		if !exists {
			continue // Skip records without matching stores
		}

		joinedRecord := &protocol.Q3JoinedRecord{
			YearHalf:  aggRecord.YearHalf,
			StoreName: storeRecord.StoreName,
			TPV:       aggRecord.TPV,
		}
		joinedRecords = append(joinedRecords, joinedRecord)
	}

	return joinedRecords, nil
}

// GetOutputDatasetType returns the dataset type for Q3 joined output
func (j *Q3Joiner) GetOutputDatasetType() protocol.DatasetType {
	return protocol.DatasetTypeQ3AggWithName
}

// AcceptsReferenceType checks if this joiner accepts stores as reference data
func (j *Q3Joiner) AcceptsReferenceType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeStores
}

// AcceptsAggregateType checks if this joiner accepts Q3 aggregate data
func (j *Q3Joiner) AcceptsAggregateType(datasetType protocol.DatasetType) bool {
	return datasetType == protocol.DatasetTypeQ3Agg
}

// Cleanup releases resources
// Note: We keep reference data because multiple EOF batches may arrive from upstream
// Stores dataset is tiny (~10 rows, ~1KB) so keeping it in memory is fine
func (j *Q3Joiner) Cleanup() error {
	// No-op: we keep reference data to handle multiple EOF batches
	return nil
}

func (j *Q3Joiner) ShouldCleanupAfterEOF() bool {
	// Q3 has small reference data (stores), keep in memory
	return false
}

// AddBufferedBatch adds a batch to the buffer (thread-safe)
func (j *Q3Joiner) AddBufferedBatch(batch protocol.BatchMessage) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.state.BufferedAggBatches = append(j.state.BufferedAggBatches, batch)
}

// GetBufferedBatches returns all buffered batches (thread-safe, returns a copy)
func (j *Q3Joiner) GetBufferedBatches() []protocol.BatchMessage {
	j.mu.RLock()
	defer j.mu.RUnlock()
	// Return a copy to avoid race conditions
	batches := make([]protocol.BatchMessage, len(j.state.BufferedAggBatches))
	copy(batches, j.state.BufferedAggBatches)
	return batches
}

// ClearBufferedBatches clears all buffered batches (thread-safe)
func (j *Q3Joiner) ClearBufferedBatches() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.state.BufferedAggBatches = make([]protocol.BatchMessage, 0)
}

// SerializeState exports the current joiner state using pipe-delimited format
// Format:
//
//	Reference data: R|store_id|store_name|street|postal_code|city|state|latitude|longitude\n
//	Separator: ---BUFFERED---\n
//	Buffered batches: B|batch_index|eof|client_id|year_half|store_id|tpv\n
func (j *Q3Joiner) SerializeState() ([]byte, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	var buf bytes.Buffer
	buf.Grow(len(j.state.Stores)*150 + len(j.state.BufferedAggBatches)*200)

	// 1. Serialize reference data (stores)
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

	// 2. Separator
	if len(j.state.BufferedAggBatches) > 0 {
		buf.WriteString("---BUFFERED---\n")

		// 3. Serialize buffered aggregate batches
		for _, batch := range j.state.BufferedAggBatches {
			for _, record := range batch.Records {
				buf.WriteString("B|")
				buf.WriteString(strconv.Itoa(batch.BatchIndex))
				buf.WriteByte('|')
				buf.WriteString(strconv.FormatBool(batch.EOF))
				buf.WriteByte('|')
				buf.WriteString(batch.ClientID)
				buf.WriteByte('|')

				// Q3 only has one record type
				if r, ok := record.(*protocol.Q3AggregatedRecord); ok {
					buf.WriteString(r.YearHalf)
					buf.WriteByte('|')
					buf.WriteString(r.StoreID)
					buf.WriteByte('|')
					buf.WriteString(r.TPV)
				} else {
					log.Printf("action: q3_joiner_serialize_skip_unknown_record | type: %T", record)
					continue
				}
				buf.WriteByte('\n')
			}
		}
	}

	return buf.Bytes(), nil
}

// RestoreState restores the joiner state from pipe-delimited format
func (j *Q3Joiner) RestoreState(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	j.state.Stores = make(map[string]*protocol.StoreRecord)
	j.state.BufferedAggBatches = make([]protocol.BatchMessage, 0)

	scanner := bufio.NewScanner(bytes.NewReader(data))
	lineNum := 0
	inBufferedSection := false
	batchRecords := make(map[int][]protocol.Record)
	batchMetadata := make(map[int]*protocol.BatchMessage)

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		if len(line) == 0 {
			continue
		}

		if line == "---BUFFERED---" {
			inBufferedSection = true
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) < 2 {
			log.Printf("action: q3_joiner_restore_skip_invalid_line | line: %d | parts: %d", lineNum, len(parts))
			continue
		}

		prefix := parts[0]

		if !inBufferedSection && prefix == "R" {
			// Reference data: R|store_id|store_name|street|postal_code|city|state|latitude|longitude
			if len(parts) != 9 {
				log.Printf("action: q3_joiner_restore_skip_invalid_reference | line: %d | parts: %d", lineNum, len(parts))
				continue
			}

			store := &protocol.StoreRecord{
				StoreID:    parts[1],
				StoreName:  parts[2],
				Street:     parts[3],
				PostalCode: parts[4],
				City:       parts[5],
				State:      parts[6],
				Latitude:   parts[7],
				Longitude:  parts[8],
			}
			j.state.Stores[store.StoreID] = store

		} else if inBufferedSection && prefix == "B" {
			// Buffered batch: B|batch_index|eof|client_id|year_half|store_id|tpv
			if len(parts) != 7 {
				log.Printf("action: q3_joiner_restore_skip_invalid_buffered | line: %d | parts: %d", lineNum, len(parts))
				continue
			}

			batchIndex, _ := strconv.Atoi(parts[1])
			eof, _ := strconv.ParseBool(parts[2])
			clientID := parts[3]

			record := &protocol.Q3AggregatedRecord{
				YearHalf: parts[4],
				StoreID:  parts[5],
				TPV:      parts[6],
			}

			batchRecords[batchIndex] = append(batchRecords[batchIndex], record)
			batchMetadata[batchIndex] = &protocol.BatchMessage{
				BatchIndex: batchIndex,
				EOF:        eof,
				ClientID:   clientID,
			}
		}
	}

	// Reconstruct batches from grouped records
	for batchIndex, records := range batchRecords {
		meta := batchMetadata[batchIndex]
		j.state.BufferedAggBatches = append(j.state.BufferedAggBatches, protocol.BatchMessage{
			BatchIndex: batchIndex,
			EOF:        meta.EOF,
			ClientID:   meta.ClientID,
			Records:    records,
		})
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan q3 joiner snapshot: %w", err)
	}

	log.Printf("action: q3_joiner_restore_complete | stores: %d | buffered_batches: %d",
		len(j.state.Stores), len(j.state.BufferedAggBatches))

	return nil
}
