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
	aggregate aggregates.RecordAggregate
}

func NewAggregateHandler(aggregate aggregates.RecordAggregate) *AggregateHandler {
	return &AggregateHandler{
		aggregate: aggregate,
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

	log.Printf("action: aggregate_batch_received | aggregate: %s | result: success | "+
		"dataset_type: %s | batch_index: %d | record_count: %d | eof: %t",
		h.aggregate.Name(), batchMessage.DatasetType, batchMessage.BatchIndex,
		len(batchMessage.Records), batchMessage.EOF)

	// Accumulate this batch
	err := h.aggregate.AccumulateBatch(batchMessage.Records, batchMessage.BatchIndex)
	if err != nil {
		log.Printf("action: aggregate_accumulate | aggregate: %s | result: fail | error: %v",
			h.aggregate.Name(), err)
		msg.Nack(false, true) // Reject and requeue
		return err
	}

	// If this batch has EOF, mark it in the aggregate
	if batchMessage.EOF {
		// Call SetEOF for aggregates that support it
		switch agg := h.aggregate.(type) {
		case *aggregates.Q2Aggregate:
			agg.SetEOF(batchMessage.BatchIndex)
		case *aggregates.Q3Aggregate:
			agg.SetEOF(batchMessage.BatchIndex)
		case *aggregates.Q4Aggregate:
			agg.SetEOF(batchMessage.BatchIndex)
		}

		log.Printf("action: aggregate_eof_received | aggregate: %s | max_batch_index: %d",
			h.aggregate.Name(), batchMessage.BatchIndex)
	}

	// Check if we have received all expected batches
	var shouldFinalize bool
	if batchMessage.EOF {
		shouldFinalize = h.aggregate.IsComplete(batchMessage.BatchIndex)
	}

	if shouldFinalize {
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
