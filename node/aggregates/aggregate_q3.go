package aggregates

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Q3RawRecord stores raw data before aggregation
type Q3RawRecord struct {
	YearHalf string
	StoreID  string
	TPV      float64
}

// Q3Aggregate handles aggregation of Q3 grouped data to accumulate TPV by year_half and store
// State is only used during Finalize to merge all increments
type Q3Aggregate struct {
	rawRecords []Q3RawRecord
	clientID   string
}

// NewQ3Aggregate creates a new Q3 aggregate processor
func NewQ3Aggregate() *Q3Aggregate {
	return &Q3Aggregate{
		rawRecords: make([]Q3RawRecord, 0),
	}
}

func (a *Q3Aggregate) Name() string {
	return "q3_aggregate_tpv_by_year_half_store"
}

// SerializeRecords serializes records with batch index as header
// Format: BATCH|index\n followed by year_half|store_id|tpv\n
// Always returns at least the header to ensure batch is tracked for crash recovery
func (a *Q3Aggregate) SerializeRecords(records []protocol.Record, batchIndex int) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(records)*50 + 20)

	buf.WriteString("BATCH|")
	buf.WriteString(strconv.Itoa(batchIndex))
	buf.WriteByte('\n')

	for _, record := range records {
		q3GroupedRecord, ok := record.(*protocol.Q3GroupedRecord)
		if !ok {
			log.Printf("action: q3_serialize_invalid_record | result: warning | "+
				"record_type: %T | expected: Q3GroupedRecord", record)
			continue
		}

		if q3GroupedRecord.YearHalf == "" || q3GroupedRecord.StoreID == "" || q3GroupedRecord.TPV == "" {
			continue
		}

		tpv, err := strconv.ParseFloat(q3GroupedRecord.TPV, 64)
		if err != nil {
			log.Printf("action: q3_serialize_parse_tpv | result: error | "+
				"year_half: %s | store_id: %s | tpv: %s | error: %v",
				q3GroupedRecord.YearHalf, q3GroupedRecord.StoreID, q3GroupedRecord.TPV, err)
			continue
		}

		buf.WriteString(q3GroupedRecord.YearHalf)
		buf.WriteByte('|')
		buf.WriteString(q3GroupedRecord.StoreID)
		buf.WriteByte('|')
		buf.WriteString(strconv.FormatFloat(tpv, 'f', 2, 64))
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

// restoreState restores data from a serialized increment (used during merge)
func (a *Q3Aggregate) restoreState(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if a.rawRecords == nil {
		a.rawRecords = make([]Q3RawRecord, 0)
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) != 3 {
			continue
		}

		yearHalf := parts[0]
		storeID := parts[1]
		tpvValue := parts[2]

		tpv, err := strconv.ParseFloat(tpvValue, 64)
		if err != nil {
			continue
		}

		a.rawRecords = append(a.rawRecords, Q3RawRecord{
			YearHalf: yearHalf,
			StoreID:  storeID,
			TPV:      tpv,
		})
	}

	return scanner.Err()
}

func (a *Q3Aggregate) Finalize(clientId string) ([]protocol.Record, error) {
	if a.clientID == "" {
		a.clientID = clientId
	}

	log.Printf("action: q3_aggregate_finalize_start | client_id: %s | raw_records: %d",
		clientId, len(a.rawRecords))

	tpvData := make(map[string]float64)

	for _, rawRecord := range a.rawRecords {
		key := fmt.Sprintf("%s|%s", rawRecord.YearHalf, rawRecord.StoreID)
		tpvData[key] += rawRecord.TPV
	}

	log.Printf("action: q3_aggregate_finalize_aggregated | client_id: %s | unique_keys: %d",
		clientId, len(tpvData))

	var result []protocol.Record

	for key, tpv := range tpvData {
		parts := parseAggregateKey(key)
		yearHalf, storeID := parts[0], parts[1]

		record := &protocol.Q3AggregatedRecord{
			YearHalf: yearHalf,
			StoreID:  storeID,
			TPV:      fmt.Sprintf("%.2f", tpv),
		}

		result = append(result, record)
	}

	log.Printf("action: q3_aggregate_finalize_complete | client_id: %s | total_results: %d", clientId, len(result))

	return result, nil
}

// parseBatchIndexFromIncrement extracts the batch index from the header of an increment
// Returns -1 if the header is not found or invalid
func parseBatchIndexFromIncrementQ3(data []byte) int {
	if len(data) == 0 {
		return -1
	}

	newlineIdx := bytes.IndexByte(data, '\n')
	if newlineIdx == -1 {
		return -1
	}

	header := string(data[:newlineIdx])
	if !strings.HasPrefix(header, "BATCH|") {
		return -1
	}

	batchIndexStr := header[6:]
	batchIndex, err := strconv.Atoi(batchIndexStr)
	if err != nil {
		return -1
	}

	return batchIndex
}

// GetBatchesToPublish loads all increments, filters duplicates, merges them, and returns final results
func (a *Q3Aggregate) GetBatchesToPublish(historicalIncrements [][]byte, batchIndex int, clientID string) ([]BatchToPublish, error) {
	if len(historicalIncrements) > 0 {
		log.Printf("action: q3_merge_start | client_id: %s | increments: %d",
			clientID, len(historicalIncrements))

		seenBatches := make(map[int]bool)
		duplicatesSkipped := 0

		for i, incrementData := range historicalIncrements {
			if len(incrementData) == 0 {
				continue
			}

			// Parse batch index from header
			batchIdx := parseBatchIndexFromIncrementQ3(incrementData)
			if batchIdx == -1 {
				log.Printf("action: q3_merge_skip_invalid | client_id: %s | increment: %d | reason: no_batch_header",
					clientID, i)
				continue
			}

			// Skip duplicates
			if seenBatches[batchIdx] {
				duplicatesSkipped++
				continue
			}
			seenBatches[batchIdx] = true

			if err := a.restoreState(incrementData); err != nil {
				log.Printf("action: q3_merge_increment | client_id: %s | increment: %d | result: fail | error: %v",
					clientID, i, err)
			}
		}

		log.Printf("action: q3_merge_complete | client_id: %s | increments_merged: %d | duplicates_skipped: %d | raw_records: %d",
			clientID, len(seenBatches), duplicatesSkipped, len(a.rawRecords))
	}

	results, err := a.Finalize(clientID)
	if err != nil {
		return nil, err
	}

	batch := protocol.NewAggregateBatch(batchIndex, results, clientID, true)

	return []BatchToPublish{
		{
			Batch:      batch,
			RoutingKey: "",
		},
	}, nil
}

func (a *Q3Aggregate) Cleanup() error {
	a.rawRecords = nil
	return nil
}
