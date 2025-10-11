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

// AggregateClientState holds per-client state for aggregate operations
type AggregateClientState struct {
	mu                       sync.Mutex
	aggregate                aggregates.Aggregate
	eofReceived              bool
	oneTimeFinalize          bool
	numberOfBatchesRemaining int
	seenBatchIndices         map[int]bool
	uniqueBatchCount         atomic.Int32
}

// AggregateHandler manages aggregate operations for multiple clients
type AggregateHandler struct {
	newAggregate func() aggregates.Aggregate // Factory function to create new aggregates per client
	states       sync.Map                    // map[string]*AggregateClientState - keyed by clientID
}

// NewAggregateHandler creates a new aggregate handler with a factory function
func NewAggregateHandler(newAggregate func() aggregates.Aggregate) *AggregateHandler {
	return &AggregateHandler{
		newAggregate: newAggregate,
	}
}

func (h *AggregateHandler) Name() string {
	return "aggregate_" + h.sampleName()
}

// sampleName instantiates a temporary aggregate just to read its name
func (h *AggregateHandler) sampleName() string {
	return h.newAggregate().Name()
}

// getState retrieves or creates the state for a specific client
func (h *AggregateHandler) getState(clientID string) *AggregateClientState {
	if v, ok := h.states.Load(clientID); ok {
		return v.(*AggregateClientState)
	}

	// Create new state for this client
	st := &AggregateClientState{
		aggregate:        h.newAggregate(),
		oneTimeFinalize:  true,
		seenBatchIndices: make(map[int]bool),
	}

	actual, _ := h.states.LoadOrStore(clientID, st)
	return actual.(*AggregateClientState)
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

	clientID := batchMessage.ClientID
	state := h.getState(clientID)

	// Accumulate this batch for this specific client
	err := state.aggregate.AccumulateBatch(batchMessage.Records, batchMessage.BatchIndex)
	state.trackBatchIndex(batchMessage.BatchIndex)
	if err != nil {
		log.Printf("action: aggregate_accumulate | client_id: %s | aggregate: %s | result: fail | error: %v",
			clientID, state.aggregate.Name(), err)
		msg.Ack(false) // Ack to remove empty or bad batches
		return err
	}

	state.mu.Lock()
	// Check if this batch has EOF flag
	if batchMessage.EOF {
		// Get the actual count AFTER processing this batch
		accumulatedCountAfterThisBatch := state.getUniqueBatchCount()
		expectedTotalBatches := batchMessage.BatchIndex
		state.numberOfBatchesRemaining = expectedTotalBatches - int(accumulatedCountAfterThisBatch)
		state.eofReceived = true

		log.Printf("action: aggregate_eof_received | client_id: %s | aggregate: %s | max_batch_index: %d | "+
			"accumulated_batches: %d | expected_total: %d | batches_remaining: %d",
			clientID, state.aggregate.Name(), batchMessage.BatchIndex, accumulatedCountAfterThisBatch,
			expectedTotalBatches, state.numberOfBatchesRemaining)

	} else if state.eofReceived {
		// If EOF was already received, decrement the remaining batch count
		state.numberOfBatchesRemaining--
		log.Printf("action: aggregate_batch_processed | client_id: %s | aggregate: %s | batch_index: %d | "+
			"batches_remaining: %d",
			clientID, state.aggregate.Name(), batchMessage.BatchIndex, state.numberOfBatchesRemaining)
	}

	// Check if we should finalize
	shouldFinalize := state.numberOfBatchesRemaining == 0 && state.eofReceived && state.oneTimeFinalize

	if shouldFinalize {
		state.oneTimeFinalize = false // Ensure finalize runs only once
		state.mu.Unlock()             // Now we can unlock
		log.Printf("action: aggregate_finalize | client_id: %s | aggregate: %s | result: start",
			clientID, state.aggregate.Name())

		batchesToPublish, err := state.aggregate.GetBatchesToPublish(batchMessage.BatchIndex, clientID)
		if err != nil {
			log.Printf("action: get_batches_to_publish | client_id: %s | aggregate: %s | result: fail | error: %v",
				clientID, state.aggregate.Name(), err)
			msg.Nack(false, true)
			return err
		}

		publisher, err := middleware.NewPublisher(connection, wiring)
		if err != nil {
			log.Printf("action: create_publisher | client_id: %s | aggregate: %s | result: fail | error: %v",
				clientID, state.aggregate.Name(), err)
			msg.Nack(false, true)
			return err
		}

		err = h.publishBatches(publisher, batchesToPublish)
		publisher.Close()

		if err != nil {
			log.Printf("action: aggregate_publish | client_id: %s | aggregate: %s | result: fail | error: %v",
				clientID, state.aggregate.Name(), err)
			msg.Nack(false, true)
			return err
		}

		log.Printf("action: aggregate_publish | client_id: %s | aggregate: %s | result: success | batches_published: %d",
			clientID, state.aggregate.Name(), len(batchesToPublish))
	} else {
		state.mu.Unlock() // This unlock is necessary in case we dont have to finalize yet
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

// trackBatchIndex tracks unique batch indices for a specific client state
func (s *AggregateClientState) trackBatchIndex(batchIndex int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.seenBatchIndices[batchIndex] {
		s.seenBatchIndices[batchIndex] = true
		s.uniqueBatchCount.Add(1)
	}
}

// getUniqueBatchCount returns the number of unique batches seen for this client
func (s *AggregateClientState) getUniqueBatchCount() int32 {
	return s.uniqueBatchCount.Load()
}
