package handlers

import (
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/aggregates"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	"github.com/rabbitmq/amqp091-go"
)

type AggregateHandler struct {
	aggregate                aggregates.RecordAggregate
	numberOfBatchesRemaining int
	eofReceived              bool
	mu                       sync.Mutex
	oneTimeFinalize          bool
}

func NewAggregateHandler(aggregate aggregates.RecordAggregate) *AggregateHandler {
	return &AggregateHandler{
		aggregate:                aggregate,
		numberOfBatchesRemaining: 0,
		eofReceived:              false,
		oneTimeFinalize:          true,
		mu:                       sync.Mutex{},
	}
}

func (h *AggregateHandler) Name() string {
	return "aggregate_" + h.aggregate.Name()
}

// StartHandler starts the aggregate handler
func (h *AggregateHandler) StartHandler(queueManager *middleware.QueueManager, clientWg *sync.WaitGroup) error {
	err := queueManager.StartConsuming(func(batchMessage *protocol.BatchMessage, delivery amqp091.Delivery) {
		h.Handle(batchMessage, queueManager.Connection,
			queueManager.Wiring, clientWg, delivery)
	})
	if err != nil {
		log.Printf("action: aggregate_consume | result: fail | error: %v", err)
		return err
	}
	return nil
}

// Handle processes a batch by accumulating it, and publishes final results when all batches are complete
func (h *AggregateHandler) Handle(batchMessage *protocol.BatchMessage, connection *amqp091.Connection,
	wiring *common.NodeWiring, clientWG *sync.WaitGroup, msg amqp091.Delivery) error {

	clientWG.Add(1)
	defer clientWG.Done()

	// Accumulate this batch
	err := h.aggregate.AccumulateBatch(batchMessage.Records, batchMessage.BatchIndex)
	if err != nil {
		log.Printf("action: aggregate_accumulate | aggregate: %s | result: fail | error: %v",
			h.aggregate.Name(), err)
		msg.Nack(false, true) // Reject and requeue
		return err
	}

	log.Printf("action: aggregate_accumulate | aggregate: %s | batch_index: %d | records: %d",
		h.aggregate.Name(), batchMessage.BatchIndex, len(batchMessage.Records))

	h.mu.Lock()
	// Check if this batch has EOF flag
	if batchMessage.EOF {

		// Get the current count of accumulated batches
		accumulatedCount := h.aggregate.GetAccumulatedBatchCount()

		// Calculate how many batches we're still waiting for
		// batchIndex starts from 1, so it represents the total expected batches
		expectedTotalBatches := batchMessage.BatchIndex
		h.numberOfBatchesRemaining = expectedTotalBatches - accumulatedCount
		h.eofReceived = true

		log.Printf("action: aggregate_eof_received | aggregate: %s | max_batch_index: %d | "+
			"accumulated_batches: %d | expected_total: %d | batches_remaining: %d",
			h.aggregate.Name(), batchMessage.BatchIndex, accumulatedCount,
			expectedTotalBatches, h.numberOfBatchesRemaining)

	} else if h.eofReceived {
		// If EOF was already received, decrement the remaining batch count
		h.numberOfBatchesRemaining--
		log.Printf("action: aggregate_batch_processed | aggregate: %s | batch_index: %d | "+
			"batches_remaining: %d",
			h.aggregate.Name(), batchMessage.BatchIndex, h.numberOfBatchesRemaining)
	}

	// Check if we should finalize
	shouldFinalize := h.numberOfBatchesRemaining == 0 && batchMessage.EOF && h.oneTimeFinalize

	if shouldFinalize {
		h.oneTimeFinalize = false // Ensure "h.aggregate.finalize()" runs only once
		h.mu.Unlock()             // Now we can unlock
		log.Printf("action: aggregate_finalize | aggregate: %s | result: start", h.aggregate.Name())

		// Finalize aggregation and get results
		finalResults, err := h.aggregate.Finalize()
		if err != nil {
			log.Printf("action: aggregate_finalize | aggregate: %s | result: fail | error: %v",
				h.aggregate.Name(), err)
			msg.Nack(false, true)
			return err
		}

		// Create the final batch to publish
		finalBatch := h.createFinalBatch(finalResults, batchMessage.BatchIndex)

		// Publish final results
		publisher, err := middleware.NewPublisher(connection, wiring)
		if err != nil {
			log.Printf("action: create_publisher | aggregate: %s | result: fail | error: %v",
				h.aggregate.Name(), err)
			msg.Nack(false, true)
			return err
		}

		if err := publisher.SendToDatasetOutputExchanges(finalBatch); err != nil {
			log.Printf("action: aggregate_publish | aggregate: %s | result: fail | error: %v",
				h.aggregate.Name(), err)
			msg.Nack(false, true)
			return err
		}

		publisher.Close()

		log.Printf("action: aggregate_publish | aggregate: %s | result: success | "+
			"final_record_count: %d", h.aggregate.Name(), len(finalResults))
	} else {
		h.mu.Unlock()
	}

	// Acknowledge the message
	msg.Ack(false)

	return nil
}

// createFinalBatch creates the appropriate batch message for the aggregated results
func (h *AggregateHandler) createFinalBatch(results []protocol.Record, batchIndex int) *protocol.BatchMessage {
	// For Q2, we need to create a dual dataset batch
	if h.aggregate.Name() == "q2_aggregate_best_selling_most_profits" {
		return protocol.NewQ2AggregateBatch(batchIndex, results, true)
	}

	// For other aggregates, use regular batch
	return protocol.NewAggregateBatch(batchIndex, results, true)
}
