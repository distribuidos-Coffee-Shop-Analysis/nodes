package handlers

import (
	"fmt"
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/joiners"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	"github.com/rabbitmq/amqp091-go"
)

// JoinerHandler handles join operations between aggregated data and reference data
type JoinerHandler struct {
	joiner joiners.RecordJoiner
}

// NewJoinerHandler creates a new joiner handler with the specified joiner
func NewJoinerHandler(joiner joiners.RecordJoiner) *JoinerHandler {
	return &JoinerHandler{
		joiner: joiner,
	}
}

// Name returns the handler name
func (h *JoinerHandler) Name() string {
	return "joiner_" + h.joiner.Name()
}

// StartHandler starts the joiner handler - consumes from multiple exchanges
func (h *JoinerHandler) StartHandler(queueManager *middleware.QueueManager, clientWg *sync.WaitGroup) error {
	err := queueManager.StartConsuming(func(batchMessage *protocol.BatchMessage, delivery amqp091.Delivery) {
		h.Handle(batchMessage, queueManager.Connection, queueManager.Wiring, clientWg, delivery)
	})
	if err != nil {
		log.Printf("action: joiner_consume | result: fail | error: %v", err)
		return err
	}
	return nil
}

// Handle processes batches from different sources (reference data or aggregated data)
func (h *JoinerHandler) Handle(batchMessage *protocol.BatchMessage, connection *amqp091.Connection,
	wiring *common.NodeWiring, clientWG *sync.WaitGroup, msg amqp091.Delivery) error {

	clientWG.Add(1)
	defer clientWG.Done()

	log.Printf("action: joiner_batch_received | joiner: %s | dataset_type: %s | record_count: %d | eof: %t",
		h.joiner.Name(), batchMessage.DatasetType, len(batchMessage.Records), batchMessage.EOF)

	// Route message based on dataset type using joiner's acceptance methods
	if h.joiner.AcceptsReferenceType(batchMessage.DatasetType) {
		return h.handleReferenceData(batchMessage, msg)
	} else if h.joiner.AcceptsAggregateType(batchMessage.DatasetType) {
		return h.handleAggregatedData(batchMessage, connection, wiring, msg)
	} else {
		log.Printf("action: joiner_handle | result: fail | error: unsupported dataset type: %s", batchMessage.DatasetType)
		msg.Nack(false, true)
		return fmt.Errorf("unsupported dataset type: %s", batchMessage.DatasetType)
	}
}

// handleReferenceData processes and stores reference data (e.g., menu items)
func (h *JoinerHandler) handleReferenceData(batchMessage *protocol.BatchMessage, msg amqp091.Delivery) error {

	log.Printf("action: store_reference_data | joiner: %s | count: %d", h.joiner.Name(), len(batchMessage.Records))

	// Store reference data using the joiner's specific logic
	err := h.joiner.StoreReferenceData(batchMessage.Records)
	if err != nil {
		log.Printf("action: store_reference_data | result: fail | error: %v", err)
		msg.Nack(false, true)
		return err
	}

	log.Printf("action: reference_data_stored | joiner: %s | result: success", h.joiner.Name())
	msg.Ack(false)
	return nil
}

// handleAggregatedData processes aggregated data and performs joins with stored reference data
func (h *JoinerHandler) handleAggregatedData(batchMessage *protocol.BatchMessage, connection *amqp091.Connection,
	wiring *common.NodeWiring, msg amqp091.Delivery) error {

	log.Printf("action: join_aggregate_data | joiner: %s | records: %d", h.joiner.Name(), len(batchMessage.Records))

	// Perform join using the joiner's specific logic
	joinedRecords, err := h.joiner.PerformJoin(batchMessage.Records)
	if err != nil {
		log.Printf("action: perform_join | result: fail | error: %v", err)
		msg.Nack(false, true)
		return err
	}

	// Create output batch with joined data
	outputBatch := h.createOutputBatch(batchMessage.BatchIndex, joinedRecords, batchMessage.EOF)

	// Publish joined results
	publisher, err := middleware.NewPublisher(connection, wiring)
	if err != nil {
		log.Printf("action: create_publisher | result: fail | error: %v", err)
		msg.Nack(false, true)
		return err
	}
	defer publisher.Close()

	if err := publisher.SendToDatasetOutputExchanges(outputBatch); err != nil {
		log.Printf("action: joiner_publish | result: fail | error: %v", err)
		msg.Nack(false, true)
		return err
	}

	log.Printf("action: joiner_publish | result: success | batch_index: %d | joined_records: %d | eof: %t",
		batchMessage.BatchIndex, len(joinedRecords), batchMessage.EOF)

	msg.Ack(false)
	return nil
}

// createOutputBatch creates the output batch with the correct dataset type
func (h *JoinerHandler) createOutputBatch(batchIndex int, records []protocol.Record, eof bool) *protocol.BatchMessage {
	return &protocol.BatchMessage{
		Type:        protocol.MessageTypeBatch,
		DatasetType: h.joiner.GetOutputDatasetType(),
		BatchIndex:  batchIndex,
		Records:     records,
		EOF:         eof,
	}
}
