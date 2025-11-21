package joiners

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

type Q3JoinerState struct {
	Stores map[string]*protocol.StoreRecord // key: store_id, value: store record
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
			Stores: make(map[string]*protocol.StoreRecord),
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

// SerializeState exports the current joiner state using pipe-delimited format
// Format: store_id|store_name|street|postal_code|city|state|latitude|longitude\n
func (j *Q3Joiner) SerializeState() ([]byte, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	var buf bytes.Buffer
	buf.Grow(len(j.state.Stores) * 150)

	for storeID, store := range j.state.Stores {
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

	scanner := bufio.NewScanner(bytes.NewReader(data))
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		if len(line) == 0 {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) != 8 {
			log.Printf("action: q3_joiner_restore_skip_invalid_line | line: %d | parts: %d", lineNum, len(parts))
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
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan q3 joiner snapshot: %w", err)
	}

	log.Printf("action: q3_joiner_restore_complete | stores: %d", len(j.state.Stores))

	return nil
}
