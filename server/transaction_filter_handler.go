package server

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// TransactionFilterHandler handles consuming transactions from RabbitMQ,
// filtering by years 2024-2025, and routing to output queues
type TransactionFilterHandler struct {
	cleanupCallback   func(*TransactionFilterHandler)
	shutdownRequested bool
	queueManager      *middleware.QueueManager
	minYear           int
	maxYear           int
	wg                sync.WaitGroup
	shutdownChan      chan struct{}
	isRunning         bool
	mu                sync.RWMutex
}

// NewTransactionFilterHandler creates a new TransactionFilterHandler instance
func NewTransactionFilterHandler(cleanupCallback func(*TransactionFilterHandler)) *TransactionFilterHandler {
	return &TransactionFilterHandler{
		cleanupCallback:   cleanupCallback,
		shutdownRequested: false,
		minYear:           2024,
		maxYear:           2025,
		shutdownChan:      make(chan struct{}),
		isRunning:         false,
	}
}

// Start begins the transaction filter handler
func (tfh *TransactionFilterHandler) Start() error {
	tfh.mu.Lock()
	defer tfh.mu.Unlock()

	if tfh.isRunning {
		return nil
	}

	tfh.wg.Add(1)
	go tfh.run()
	tfh.isRunning = true

	return nil
}

// RequestShutdown requests graceful shutdown of the handler
func (tfh *TransactionFilterHandler) RequestShutdown() {
	tfh.mu.Lock()
	defer tfh.mu.Unlock()

	if tfh.shutdownRequested {
		return
	}

	tfh.shutdownRequested = true
	log.Println("action: transaction_filter_handler_shutdown | result: requested")

	// Stop consuming to unblock the handler
	if tfh.queueManager != nil {
		tfh.queueManager.StopConsuming()
	}

	close(tfh.shutdownChan)
}

// Wait waits for the handler to finish
func (tfh *TransactionFilterHandler) Wait() {
	tfh.wg.Wait()
}

// IsAlive checks if the handler is still running
func (tfh *TransactionFilterHandler) IsAlive() bool {
	tfh.mu.RLock()
	defer tfh.mu.RUnlock()
	return tfh.isRunning
}

// run is the main handler loop for consuming and filtering transactions
func (tfh *TransactionFilterHandler) run() {
	defer tfh.wg.Done()
	defer tfh.cleanup()

	log.Println("action: transaction_filter_handler_start | result: success")

	tfh.queueManager = middleware.NewQueueManager()

	if err := tfh.queueManager.Connect(); err != nil {
		log.Printf("action: transaction_filter_handler | result: fail | error: Could not connect to RabbitMQ: %v", err)
		return
	}

	// Start consuming transactions with callback (this blocks until shutdown)
	err := tfh.queueManager.StartConsuming(tfh.handleTransactionBatch)
	if err != nil && !tfh.shutdownRequested {
		log.Printf("action: transaction_filter_handler | result: fail | error: %v", err)
	}
}

// handleTransactionBatch handles a transaction batch - filter by year and route to output queues
func (tfh *TransactionFilterHandler) handleTransactionBatch(batchMessage *protocol.BatchMessage) {
	if tfh.shutdownRequested {
		return
	}

	// Validate dataset type - only process TRANSACTIONS and TRANSACTION_ITEMS
	if batchMessage.DatasetType != protocol.DatasetTypeTransactions &&
		batchMessage.DatasetType != protocol.DatasetTypeTransactionItems {
		log.Printf("action: batch_dropped | result: success | "+
			"dataset_type: %s | reason: invalid_dataset_type | "+
			"record_count: %d", batchMessage.DatasetType, len(batchMessage.Records))
		return
	}

	log.Printf("action: transaction_batch_received | result: success | "+
		"dataset_type: %s | record_count: %d | eof: %t",
		batchMessage.DatasetType, len(batchMessage.Records), batchMessage.EOF)

	// Filter records by year (2024-2025)
	filteredRecords := tfh.filterRecordsByYear(batchMessage.Records)
	batchMessage.Records = filteredRecords

	tfh.routeToOutputExchanges(batchMessage)
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
		} else {
			transactionID := tfh.extractTransactionID(record)
			log.Printf("action: record_filtered | year: %d | "+
				"transaction_id: %s", year, transactionID)
		}
	}

	return filteredRecords
}

// routeToOutputExchanges routes filtered records to the appropriate output queues based on dataset type
func (tfh *TransactionFilterHandler) routeToOutputExchanges(batchMessage *protocol.BatchMessage) {
	if err := tfh.queueManager.SendToDatasetOutputExchanges(batchMessage); err != nil {
		log.Printf("action: route_to_output_exchanges | result: fail | error: %v", err)
	}
}

// cleanup cleans up resources and notifies server
func (tfh *TransactionFilterHandler) cleanup() {
	tfh.mu.Lock()
	defer tfh.mu.Unlock()

	tfh.isRunning = false

	if tfh.queueManager != nil {
		tfh.queueManager.Close()
	}

	if tfh.cleanupCallback != nil {
		tfh.cleanupCallback(tfh)
	}

	log.Println("action: transaction_filter_handler_cleanup | result: success")
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
