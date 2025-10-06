package handlers

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/aggregates"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	"github.com/rabbitmq/amqp091-go"
)

type AggregateHandler struct {
	aggregate                aggregates.RecordAggregate
	mu                       sync.Mutex
	numberOfBatchesRemaining int
	eofReceived              bool
	oneTimeFinalize          bool
	// Track unique batch indices
	seenBatchIndices map[int]bool // track which batch indices we've seen
	uniqueBatchCount atomic.Int32 // count of unique batch indices
}

func NewAggregateHandler(aggregate aggregates.RecordAggregate) *AggregateHandler {
	return &AggregateHandler{
		aggregate:                aggregate,
		numberOfBatchesRemaining: 0,
		eofReceived:              false,
		oneTimeFinalize:          true,
		mu:                       sync.Mutex{},
		seenBatchIndices:         make(map[int]bool),
		uniqueBatchCount:         atomic.Int32{},
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
	h.trackBatchIndex(batchMessage.BatchIndex)
	if err != nil {
		log.Printf("action: aggregate_accumulate | aggregate: %s | result: fail | error: %v",
			h.aggregate.Name(), err)
		msg.Ack(false) // Ack to remove empty or bad batches
		return err
	}

	h.mu.Lock()
	// Check if this batch has EOF flag
	if batchMessage.EOF {
		// Get the actual count AFTER processing this batch
		accumulatedCountAfterThisBatch := h.getUniqueBatchCount()
		expectedTotalBatches := batchMessage.BatchIndex 
		h.numberOfBatchesRemaining = expectedTotalBatches - int(accumulatedCountAfterThisBatch)
		h.eofReceived = true

		log.Printf("action: aggregate_eof_received | aggregate: %s | max_batch_index: %d | "+
			"accumulated_batches: %d | expected_total: %d | batches_remaining: %d",
			h.aggregate.Name(), batchMessage.BatchIndex, accumulatedCountAfterThisBatch,
			expectedTotalBatches, h.numberOfBatchesRemaining)

	} else if h.eofReceived {
		// If EOF was already received, decrement the remaining batch count
		h.numberOfBatchesRemaining--
		log.Printf("action: aggregate_batch_processed | aggregate: %s | batch_index: %d | "+
			"batches_remaining: %d",
			h.aggregate.Name(), batchMessage.BatchIndex, h.numberOfBatchesRemaining)
	}

	// Check if we should finalize
	shouldFinalize := h.numberOfBatchesRemaining == 0 && h.eofReceived && h.oneTimeFinalize

	if shouldFinalize {
		h.oneTimeFinalize = false // Ensure finalize runs only once
		h.mu.Unlock()             // Now we can unlock
		log.Printf("action: aggregate_finalize | aggregate: %s | result: start", h.aggregate.Name())

		batchesToPublish, err := h.aggregate.GetBatchesToPublish(batchMessage.BatchIndex)
		if err != nil {
			log.Printf("action: get_batches_to_publish | aggregate: %s | result: fail | error: %v",
				h.aggregate.Name(), err)
			msg.Nack(false, true)
			return err
		}

		publisher, err := middleware.NewPublisher(connection, wiring)
		if err != nil {
			log.Printf("action: create_publisher | aggregate: %s | result: fail | error: %v",
				h.aggregate.Name(), err)
			msg.Nack(false, true)
			return err
		}

		err = h.publishBatches(publisher, batchesToPublish)
		publisher.Close()

		if err != nil {
			log.Printf("action: aggregate_publish | aggregate: %s | result: fail | error: %v",
				h.aggregate.Name(), err)
			msg.Nack(false, true)
			return err
		}

		log.Printf("action: aggregate_publish | aggregate: %s | result: success | batches_published: %d",
			h.aggregate.Name(), len(batchesToPublish))
	} else {
		h.mu.Unlock() // This unlock is necessary in case we dont have to finalize yet
	}

	// Acknowledge the message
	msg.Ack(false)

	return nil
}

// publishBatches publishes all batches, using custom routing keys when provided
func (h *AggregateHandler) publishBatches(publisher *middleware.Publisher, batchesToPublish []aggregates.BatchToPublish) error {
	for i, batchToPublish := range batchesToPublish {

		err := publisher.SendToDatasetOutputExchangesWithRoutingKey(batchToPublish.Batch, batchToPublish.RoutingKey)
		if err != nil {
			return fmt.Errorf("failed to publish batch %d: %w", i+1, err)
		}

		log.Printf("action: publish_batch | batch_num: %d | routing_key: %s | records: %d",
			i+1, batchToPublish.RoutingKey, len(batchToPublish.Batch.Records))
	}

	return nil
}

func (h *AggregateHandler) trackBatchIndex(batchIndex int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.seenBatchIndices[batchIndex] {
		h.seenBatchIndices[batchIndex] = true
		h.uniqueBatchCount.Add(1)
	}
}
func (h *AggregateHandler) getUniqueBatchCount() int32 {
	return h.uniqueBatchCount.Load()
}
