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
	filter      filters.Filter
	transformer RecordTransformer    // transforms records after filtering
	outputType  protocol.DatasetType // override output dataset type

	// Reusable publisher to avoid channel exhaustion
	pub   *middleware.Publisher
	pubMu sync.Mutex
}

func NewFilterHandler(filter filters.Filter) *FilterHandler {
	return &FilterHandler{
		filter:      filter,
		transformer: nil,
		outputType:  0, // 0 means use input type
	}
}

func NewFilterHandlerWithTransform(filter filters.Filter, transformer RecordTransformer, outputType protocol.DatasetType) *FilterHandler {
	return &FilterHandler{
		filter:      filter,
		transformer: transformer,
		outputType:  outputType,
	}
}

func (h *FilterHandler) Name() string {
	return "transaction_filter_" + h.filter.Name()
}

// Shutdown for filter handlers (no state to persist)
func (h *FilterHandler) Shutdown() error {
	return nil
}

// startFilterHandler starts the transaction filter handler
func (h *FilterHandler) StartHandler(queueManager *middleware.QueueManager, clientWg *sync.WaitGroup) error {
	// Create ONE publisher/channel for this handler and reuse it
	pub, err := middleware.NewPublisher(queueManager.Connection, queueManager.Wiring)
	if err != nil {
		log.Printf("action: create_publisher | result: fail | error: %v", err)
		return err
	}
	h.pub = pub
	log.Printf("action: create_publisher | result: success | handler: %s", h.Name())

	// Consume with callback (blocks until StopConsuming or error)
	err = queueManager.StartConsuming(func(batchMessage *protocol.BatchMessage, delivery amqp091.Delivery) {

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

	filteredRecords := tfh.filterRecords(batchMessage.Records)

	if tfh.transformer != nil {
		transformedRecords := make([]protocol.Record, 0, len(filteredRecords))
		for _, record := range filteredRecords {
			transformedRecords = append(transformedRecords, tfh.transformer(record))
		}
		filteredRecords = transformedRecords
	}

	out := *batchMessage
	out.Records = filteredRecords

	if tfh.outputType != 0 {
		out.DatasetType = tfh.outputType
	}

	tfh.pubMu.Lock()
	err := tfh.pub.SendToDatasetOutputExchanges(&out)
	tfh.pubMu.Unlock()

	if err != nil {
		log.Printf("action: node_publish | result: fail | error: %v", err)
		msg.Nack(false, true)
		return err
	}

	msg.Ack(false)

	return nil
}

func (tfh *FilterHandler) filterRecords(records []protocol.Record) []protocol.Record {
	var filteredRecords []protocol.Record

	for _, record := range records {
		if tfh.filter.Filter(record) {
			filteredRecords = append(filteredRecords, record)
		}
	}

	return filteredRecords
}
