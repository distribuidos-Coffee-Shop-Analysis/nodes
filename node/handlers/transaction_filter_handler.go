package handlers

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	"github.com/rabbitmq/amqp091-go"
)

type TransactionFilterHandler struct {
	minYear   int
	maxYear   int
	mu        sync.RWMutex
	isRunning bool
}

func NewTransactionFilterHandler() *TransactionFilterHandler {
	return &TransactionFilterHandler{
		minYear:   2024,
		maxYear:   2025,
		isRunning: false,
		
	}
}

func (h *TransactionFilterHandler) Name() string { return "transaction_year_filter" }


// handleTransactionBatch handles a transaction batch - filter by year and route to output queues
func (tfh *TransactionFilterHandler) Handle(batchMessage *protocol.BatchMessage, connection *amqp091.Connection, 
	wiring *common.NodeWiring, clientWG *sync.WaitGroup) (error) {

	clientWG.Add(1)
	defer clientWG.Done()
	
	// Validate dataset type - only process TRANSACTIONS and TRANSACTION_ITEMS
	if batchMessage.DatasetType != protocol.DatasetTypeTransactions &&
		batchMessage.DatasetType != protocol.DatasetTypeTransactionItems {
		log.Printf("action: batch_dropped | result: success | "+
			"dataset_type: %s | reason: invalid_dataset_type | "+
			"record_count: %d", batchMessage.DatasetType, len(batchMessage.Records))
		return nil
	}

	log.Printf("action: transaction_batch_received | result: success | "+
		"dataset_type: %s | record_count: %d | eof: %t",
		batchMessage.DatasetType, len(batchMessage.Records), batchMessage.EOF)

	// Filter records by year (2024-2025)
	filteredRecords := tfh.filterRecordsByYear(batchMessage.Records)

	if len(filteredRecords) == 0 && !batchMessage.EOF {
		return nil
	}
	out := *batchMessage
	out.Records = filteredRecords

	publisher, err := middleware.NewPublisher(connection, wiring)
	if err != nil {
		log.Printf("action: create publisher | result: fail | error: %v", err)
		return err
	}

	if err := publisher.SendToDatasetOutputExchanges(&out); err != nil {
		log.Printf("action: node_publish | result: fail | error: %v", err)
		return err
	}

	return nil
}

// filterRecordsByYear filters records based on created_at field being between 2024-2025
func (tfh *TransactionFilterHandler) filterRecordsByYear(records []protocol.Record) []protocol.Record {
	var filteredRecords []protocol.Record

	for _, record := range records {
		// Extract created_at field from record
		createdAt, err := tfh.extractCreatedAt(record)
		if err != nil {
			log.Printf("action: record_parse_error | result: dropped | "+
				"error: %v", err)
			continue
		}

		// Parse the date - assuming format like "2024-01-15T10:30:00" or "2024-01-15"
		var datePart string
		if strings.Contains(createdAt, "T") {
			datePart = strings.Split(createdAt, "T")[0]
		} else {
			datePart = strings.Split(createdAt, " ")[0] // Handle "YYYY-MM-DD HH:MM:SS" format
		}

		yearStr := strings.Split(datePart, "-")[0]
		year, err := strconv.Atoi(yearStr)
		if err != nil {
			log.Printf("action: record_parse_error | result: dropped | "+
				"created_at: %s | error: %v", createdAt, err)
			continue
		}

		// Filter by year range
		if year >= tfh.minYear && year <= tfh.maxYear {
			filteredRecords = append(filteredRecords, record)
			log.Printf("action: record_accepted | year: %d | "+
				"transaction_id: %s", year, tfh.extractTransactionID(record))
		} else {
			transactionID := tfh.extractTransactionID(record)
			log.Printf("action: record_filtered | year: %d | "+
				"transaction_id: %s", year, transactionID)
		}
	}

	return filteredRecords
}

// extractCreatedAt extracts the created_at field from a record
func (tfh *TransactionFilterHandler) extractCreatedAt(record protocol.Record) (string, error) {
	// Use type assertion to get the specific record type
	switch r := record.(type) {
	case *protocol.TransactionRecord:
		return r.CreatedAt, nil
	case *protocol.TransactionItemRecord:
		return r.CreatedAt, nil
	default:
		return "", fmt.Errorf("unsupported record type for created_at extraction")
	}
}

// extractTransactionID extracts the transaction_id field from a record for logging
func (tfh *TransactionFilterHandler) extractTransactionID(record protocol.Record) string {
	// Use type assertion to get the specific record type
	switch r := record.(type) {
	case *protocol.TransactionRecord:
		return r.TransactionID
	case *protocol.TransactionItemRecord:
		return r.TransactionID
	default:
		return "unknown"
	}
}
