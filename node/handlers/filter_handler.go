package handlers

import (
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/filters"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	"github.com/rabbitmq/amqp091-go"
)

type RecordTransformer func(protocol.Record) protocol.Record

type FilterHandler struct {
	filter      filters.RecordFilter
	transformer RecordTransformer    // Optional: transforms records after filtering
	outputType  protocol.DatasetType // Optional: override output dataset type
}

func NewFilterHandler(filter filters.RecordFilter) *FilterHandler {
	return &FilterHandler{
		filter:      filter,
		transformer: nil,
		outputType:  0, // 0 means use input type
	}
}

func NewFilterHandlerWithTransform(filter filters.RecordFilter, transformer RecordTransformer, outputType protocol.DatasetType) *FilterHandler {
	return &FilterHandler{
		filter:      filter,
		transformer: transformer,
		outputType:  outputType,
	}
}

func (h *FilterHandler) Name() string {
	return "transaction_filter_" + h.filter.Name()
}

// startFilterHandler starts the transaction filter handler
func (h *FilterHandler) StartHandler(queueManager *middleware.QueueManager, clientWg *sync.WaitGroup) error {

	// Consume with callback (blocks until StopConsuming or error)
	err := queueManager.StartConsuming(func(batchMessage *protocol.BatchMessage, delivery amqp091.Delivery) {

		h.Handle(batchMessage, queueManager.Connection,
			queueManager.Wiring, clientWg, delivery)

	})
	if err != nil {
		log.Printf("action: node_consume | result: fail | error: %v", err)
		return err
	}
	return nil
}

// handleTransactionBatch handles a transaction batch - filter by year and route to output queues
func (tfh *FilterHandler) Handle(batchMessage *protocol.BatchMessage, connection *amqp091.Connection,
	wiring *common.NodeWiring, clientWG *sync.WaitGroup, msg amqp091.Delivery) error {

	clientWG.Add(1)
	defer clientWG.Done()

	// Filter records using the configured filter
	filteredRecords := tfh.filterRecords(batchMessage.Records)

	// Transform records if transformer is provided
	if tfh.transformer != nil {
		transformedRecords := make([]protocol.Record, 0, len(filteredRecords))
		for _, record := range filteredRecords {
			transformedRecords = append(transformedRecords, tfh.transformer(record))
		}
		filteredRecords = transformedRecords
	}

	// if len(filteredRecords) == 0 && !batchMessage.EOF {
	// 	return nil
	// }

	out := *batchMessage
	out.Records = filteredRecords

	// Override dataset type if specified
	if tfh.outputType != 0 {
		out.DatasetType = tfh.outputType
	}

	publisher, err := middleware.NewPublisher(connection, wiring)
	if err != nil {
		log.Printf("action: create publisher | result: fail | error: %v", err)
		msg.Nack(false, true) // Reject and requeue
		return err
	}

	if err := publisher.SendToDatasetOutputExchanges(&out); err != nil {
		log.Printf("action: node_publish | result: fail | error: %v", err)
		msg.Nack(false, true) // Reject and requeue
		return err
	}

	msg.Ack(false) // Acknowledge the message
	publisher.Close() // Close channel after publishing

	return nil
}

// filterRecords filters records using the configured filter
func (tfh *FilterHandler) filterRecords(records []protocol.Record) []protocol.Record {
	var filteredRecords []protocol.Record

	for _, record := range records {
		// Use the configured filter to determine if record should be kept
		if tfh.filter.Filter(record) {
			filteredRecords = append(filteredRecords, record)
		}
	}

	return filteredRecords
}
