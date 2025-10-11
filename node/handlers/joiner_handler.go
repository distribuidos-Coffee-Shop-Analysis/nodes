package handlers

import (
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/joiners"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	"github.com/rabbitmq/amqp091-go"
)

// bufferedBatch holds all necessary information to process a batch later
type bufferedBatch struct {
	batchMessage *protocol.BatchMessage
	connection   *amqp091.Connection
	wiring       *common.NodeWiring
	delivery     amqp091.Delivery
}

// JoinerClientState holds per-client state for joiner operations
type JoinerClientState struct {
	mu                       sync.Mutex
	joiner                   joiners.Joiner
	referenceDatasetComplete bool
	bufferedAggregateBatches []bufferedBatch
}

// JoinerHandler manages join operations for multiple clients
type JoinerHandler struct {
	newJoiner func() joiners.Joiner // Factory function to create new joiners per client
	states    sync.Map              // map[string]*JoinerClientState - keyed by clientID
}

// NewJoinerHandler creates a new joiner handler with a factory function
func NewJoinerHandler(newJoiner func() joiners.Joiner) *JoinerHandler {
	return &JoinerHandler{
		newJoiner: newJoiner,
	}
}

// Name returns the handler name
func (h *JoinerHandler) Name() string {
	return "joiner_" + h.sampleName()
}

// sampleName instantiates a temporary joiner just to read its name
func (h *JoinerHandler) sampleName() string {
	return h.newJoiner().Name()
}

// getState retrieves or creates the state for a specific client
func (h *JoinerHandler) getState(clientID string) *JoinerClientState {
	if v, ok := h.states.Load(clientID); ok {
		return v.(*JoinerClientState)
	}

	// Create new state for this client
	st := &JoinerClientState{
		joiner:                   h.newJoiner(),
		referenceDatasetComplete: false,
		bufferedAggregateBatches: make([]bufferedBatch, 0),
	}

	actual, _ := h.states.LoadOrStore(clientID, st)
	return actual.(*JoinerClientState)
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

	// Get or create client-specific state
	clientID := batchMessage.ClientID
	state := h.getState(clientID)

	// Route message based on dataset type using joiner's acceptance methods
	if state.joiner.AcceptsReferenceType(batchMessage.DatasetType) {
		return h.handleReferenceDataset(state, batchMessage, connection, wiring, msg, clientID)
	} else if state.joiner.AcceptsAggregateType(batchMessage.DatasetType) {
		return h.handleAggregatedData(state, batchMessage, connection, wiring, msg, clientID)
	} else {
		log.Printf("action: joiner_handle | client_id: %s | result: fail | error: unsupported dataset type: %s",
			clientID, batchMessage.DatasetType)
		msg.Ack(false) // Ack to avoid redelivery of unsupported messages
		return nil
	}
}

// handleReferenceDataset processes and stores reference data (e.g., menu items, stores, users)
func (h *JoinerHandler) handleReferenceDataset(state *JoinerClientState, batchMessage *protocol.BatchMessage,
	connection *amqp091.Connection, wiring *common.NodeWiring, msg amqp091.Delivery, clientID string) error {

	// Store reference data using this client's joiner
	err := state.joiner.StoreReferenceDataset(batchMessage.Records)
	if err != nil {
		log.Printf("action: store_reference_data | client_id: %s | joiner: %s | result: fail | error: %v",
			clientID, state.joiner.Name(), err)
		msg.Nack(false, true)
		return err
	}

	log.Printf("action: store_reference_data | client_id: %s | joiner: %s | records: %d | eof: %v",
		clientID, state.joiner.Name(), len(batchMessage.Records), batchMessage.EOF)

	// Check if this is the last batch of reference data (EOF received)
	if batchMessage.EOF {
		state.mu.Lock()
		state.referenceDatasetComplete = true
		bufferedBatches := state.bufferedAggregateBatches
		state.bufferedAggregateBatches = make([]bufferedBatch, 0) // Clear buffer
		state.mu.Unlock()

		log.Printf("action: reference_data_complete | client_id: %s | joiner: %s | buffered_batches: %d",
			clientID, state.joiner.Name(), len(bufferedBatches))

		// Process any buffered aggregate batches that arrived before reference data was complete
		if len(bufferedBatches) > 0 {
			for i, buffered := range bufferedBatches {
				// Process this buffered batch (perform join and publish)
				err := h.processAggregatedData(state, buffered.batchMessage, buffered.connection, buffered.wiring, buffered.delivery, clientID)
				if err != nil {
					log.Printf("action: process_buffered_batch | client_id: %s | joiner: %s | result: fail | batch_num: %d | error: %v",
						clientID, state.joiner.Name(), i+1, err)
					// Continue processing other batches even if one fails
					continue
				}
			}

			log.Printf("action: process_buffered_batches_complete | client_id: %s | joiner: %s | processed: %d",
				clientID, state.joiner.Name(), len(bufferedBatches))
		}
	}

	msg.Ack(false)
	return nil
}

// processAggregatedData performs the actual join and publishes results
func (h *JoinerHandler) processAggregatedData(state *JoinerClientState, batchMessage *protocol.BatchMessage,
	connection *amqp091.Connection, wiring *common.NodeWiring, msg amqp091.Delivery, clientID string) error {

	log.Printf("action: join_aggregate_data | client_id: %s | joiner: %s | records: %d",
		clientID, state.joiner.Name(), len(batchMessage.Records))

	// Perform join using this client's joiner
	joinedRecords, err := state.joiner.PerformJoin(batchMessage.Records)
	if err != nil {
		log.Printf("action: perform_join | client_id: %s | joiner: %s | result: fail | error: %v",
			clientID, state.joiner.Name(), err)
		msg.Nack(false, true)
		return err
	}

	// Create output batch with joined data (propagate ClientID)
	outputBatch := h.createOutputBatch(state, batchMessage.BatchIndex, joinedRecords, batchMessage.EOF, clientID)

	// Publish joined results
	publisher, err := middleware.NewPublisher(connection, wiring)
	if err != nil {
		log.Printf("action: create_publisher | client_id: %s | joiner: %s | result: fail | error: %v",
			clientID, state.joiner.Name(), err)
		msg.Nack(false, true)
		return err
	}
	defer publisher.Close()

	if err := publisher.SendToDatasetOutputExchanges(outputBatch); err != nil {
		log.Printf("action: joiner_publish | client_id: %s | joiner: %s | result: fail | error: %v",
			clientID, state.joiner.Name(), err)
		msg.Nack(false, true)
		return err
	}

	log.Printf("action: joiner_publish | client_id: %s | joiner: %s | result: success | batch_index: %d | joined_records: %d | eof: %t",
		clientID, state.joiner.Name(), batchMessage.BatchIndex, len(joinedRecords), batchMessage.EOF)

	msg.Ack(false)
	return nil
}

// handleAggregatedData processes aggregated data - buffers if reference data not ready, or joins immediately
func (h *JoinerHandler) handleAggregatedData(state *JoinerClientState, batchMessage *protocol.BatchMessage,
	connection *amqp091.Connection, wiring *common.NodeWiring, msg amqp091.Delivery, clientID string) error {

	// Check if reference data is complete for this client
	state.mu.Lock()
	refDataComplete := state.referenceDatasetComplete
	state.mu.Unlock()

	if !refDataComplete {
		// Reference data not ready yet - buffer this aggregate batch
		state.mu.Lock()
		state.bufferedAggregateBatches = append(state.bufferedAggregateBatches, bufferedBatch{
			batchMessage: batchMessage,
			connection:   connection,
			wiring:       wiring,
			delivery:     msg,
		})
		bufferedCount := len(state.bufferedAggregateBatches)
		state.mu.Unlock()

		log.Printf("action: buffer_aggregate_batch | client_id: %s | joiner: %s | batch_index: %d | buffered_count: %d | reason: reference_data_not_ready",
			clientID, state.joiner.Name(), batchMessage.BatchIndex, bufferedCount)

		// Don't ACK yet - will ACK when we process from buffer
		return nil
	}

	// Reference data is ready - process immediately
	return h.processAggregatedData(state, batchMessage, connection, wiring, msg, clientID)
}

// createOutputBatch creates the output batch with the correct dataset type
func (h *JoinerHandler) createOutputBatch(state *JoinerClientState, batchIndex int, records []protocol.Record, eof bool, clientID string) *protocol.BatchMessage {
	return &protocol.BatchMessage{
		Type:        protocol.MessageTypeBatch,
		DatasetType: state.joiner.GetOutputDatasetType(),
		ClientID:    clientID,
		BatchIndex:  batchIndex,
		Records:     records,
		EOF:         eof,
	}
}
