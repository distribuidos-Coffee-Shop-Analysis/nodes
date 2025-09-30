package handlers

import (
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/filters"
	"github.com/rabbitmq/amqp091-go"

)

type TransactionFilterHandler struct {
	filter filters.RecordFilter
}

func NewTransactionFilterHandler(filter filters.RecordFilter) *TransactionFilterHandler {
	return &TransactionFilterHandler{
		filter: filter,
	}
}

func (h *TransactionFilterHandler) Name() string {
	return "transaction_filter_" + h.filter.Name()
}


// startTransactionFilterHandler starts the transaction filter handler
func (h *TransactionFilterHandler) StartHandler(queueManager *middleware.QueueManager,clientWg *sync.WaitGroup) error {
	
	// Consume with callback (blocks until StopConsuming or error)
	err := queueManager.StartConsuming(func(batchMessage *protocol.BatchMessage) {
	
		go h.Handle(batchMessage, queueManager.Connection, 
			queueManager.Wiring, clientWg)
		
	})
	if err != nil {
		log.Printf("action: node_consume | result: fail | error: %v", err)
		return err
	}
	return nil
}


// handleTransactionBatch handles a transaction batch - filter by year and route to output queues
func (tfh *TransactionFilterHandler) Handle(batchMessage *protocol.BatchMessage, connection *amqp091.Connection,
	wiring *common.NodeWiring, clientWG *sync.WaitGroup) error {

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

	// Filter records using the configured filter
	filteredRecords := tfh.filterRecords(batchMessage.Records)

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

// filterRecords filters records using the configured filter
func (tfh *TransactionFilterHandler) filterRecords(records []protocol.Record) []protocol.Record {
	var filteredRecords []protocol.Record

	for _, record := range records {
		// Use the configured filter to determine if record should be kept
		if tfh.filter.Filter(record) {
			filteredRecords = append(filteredRecords, record)
			log.Printf("action: record_accepted | filter: %s | "+
				"transaction_id: %s", tfh.filter.Name(), filters.ExtractTransactionID(record))
		} else {
			log.Printf("action: record_filtered | filter: %s | "+
				"transaction_id: %s", tfh.filter.Name(), filters.ExtractTransactionID(record))
		}
	}

	return filteredRecords
}
