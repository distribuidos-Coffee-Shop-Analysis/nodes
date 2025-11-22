package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/joiners"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/storage"
	"github.com/rabbitmq/amqp091-go"
)

// JoinerClientState holds per-client state for joiner operations
type JoinerClientState struct {
	mu                       sync.Mutex
	joiner                   joiners.Joiner
	referenceDatasetComplete bool
}

// JoinerHandler manages join operations for multiple clients
type JoinerHandler struct {
	newJoiner func() joiners.Joiner         // Factory function to create new joiners per client
	states    map[string]*JoinerClientState // map[string]*JoinerClientState - keyed by clientID
	statesMu  sync.RWMutex                  // Protects states map

	// Reusable publisher to avoid channel exhaustion
	pub   *middleware.Publisher
	pubMu sync.Mutex

	stateStore storage.StateStore
}

type joinerSnapshotMetadata struct {
	ReferenceDatasetComplete bool `json:"reference_dataset_complete"`
}

// NewJoinerHandler creates a new joiner handler with a factory function
func NewJoinerHandler(newJoiner func() joiners.Joiner) *JoinerHandler {
	return &JoinerHandler{
		newJoiner: newJoiner,
		states:    make(map[string]*JoinerClientState),
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

// Shutdown persists all active client states for joiner (stateful)
func (h *JoinerHandler) Shutdown() error {
	log.Printf("action: joiner_shutdown | result: start | persisting_states: true")

	h.statesMu.RLock()
	clientIDs := make([]string, 0, len(h.states))
	for clientID := range h.states {
		clientIDs = append(clientIDs, clientID)
	}
	h.statesMu.RUnlock()

	for _, clientID := range clientIDs {
		state := h.states[clientID]
		if state != nil {
			if err := h.persistClientState(clientID, state); err != nil {
				log.Printf("action: joiner_shutdown_persist | client_id: %s | result: fail | error: %v", clientID, err)
			} else {
				log.Printf("action: joiner_shutdown_persist | client_id: %s | result: success", clientID)
			}
		}
	}

	log.Printf("action: joiner_shutdown | result: success | states_persisted: %d", len(clientIDs))
	return nil
}

// getState retrieves or creates the state for a specific client
func (h *JoinerHandler) getState(clientID string) *JoinerClientState {
	// Try read-only access first (common case)
	h.statesMu.RLock()
	if state, ok := h.states[clientID]; ok {
		h.statesMu.RUnlock()
		return state
	}
	h.statesMu.RUnlock()

	// Need to create new state - acquire write lock
	h.statesMu.Lock()
	defer h.statesMu.Unlock()

	// Double-check after acquiring write lock
	if state, ok := h.states[clientID]; ok {
		return state
	}

	// Create new state for this client
	st := &JoinerClientState{
		joiner:                   h.newJoiner(),
		referenceDatasetComplete: false,
	}
	h.restoreClientState(clientID, st)
	h.states[clientID] = st
	return st
}

// StartHandler starts the joiner handler - consumes from multiple exchanges
func (h *JoinerHandler) StartHandler(queueManager *middleware.QueueManager, clientWg *sync.WaitGroup) error {
	h.initStateStore(queueManager.Wiring.Role)

	pub, err := middleware.NewPublisher(queueManager.Connection, queueManager.Wiring)
	if err != nil {
		log.Printf("action: create_publisher | result: fail | error: %v", err)
		return err
	}
	h.pub = pub

	err = queueManager.StartConsuming(func(batchMessage *protocol.BatchMessage, delivery amqp091.Delivery) {
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
		return h.handleReferenceDataset(state, batchMessage, msg, clientID)
	} else if state.joiner.AcceptsAggregateType(batchMessage.DatasetType) {
		return h.handleAggregatedData(state, batchMessage, msg, clientID)
	} else {
		log.Printf("action: joiner_handle | client_id: %s | result: fail | error: unsupported dataset type: %s",
			clientID, batchMessage.DatasetType)
		msg.Ack(false) // Ack to avoid redelivery of unsupported messages
		return nil
	}
}

// handleReferenceDataset processes and stores reference data (e.g., menu items, stores, users)
func (h *JoinerHandler) handleReferenceDataset(state *JoinerClientState, batchMessage *protocol.BatchMessage, msg amqp091.Delivery, clientID string) error {

	// Store reference data using this client's joiner
	err := state.joiner.StoreReferenceDataset(batchMessage.Records)
	if err != nil {
		log.Printf("action: store_reference_data | client_id: %s | joiner: %s | result: fail | error: %v",
			clientID, state.joiner.Name(), err)
		msg.Nack(false, true)
		return err
	}

	// Check if this is the last batch of reference data (EOF received)
	if batchMessage.EOF {
		state.mu.Lock()
		state.referenceDatasetComplete = true
		state.mu.Unlock()

		// Get buffered batches from joiner and clear them
		bufferedBatches := state.joiner.GetBufferedBatches()
		state.joiner.ClearBufferedBatches()

		log.Printf("action: reference_data_complete | client_id: %s | joiner: %s | buffered_batches: %d",
			clientID, state.joiner.Name(), len(bufferedBatches))

		if err := h.persistClientState(clientID, state); err != nil {
			log.Printf("action: joiner_persist_state | client_id: %s | joiner: %s | result: fail | error: %v",
				clientID, state.joiner.Name(), err)
			msg.Nack(false, true)
			return err
		}

		// Process any buffered aggregate batches that arrived before reference data was complete
		if len(bufferedBatches) > 0 {
			for i, buffered := range bufferedBatches {
				// Process this buffered batch (perform join and publish)
				// Don't pass msg delivery since these batches were already ACK'ed when buffered
				err := h.processAggregatedDataWithoutAck(state, &buffered, clientID)
				if err != nil {
					log.Printf("action: process_buffered_batch | client_id: %s | joiner: %s | result: fail | batch_num: %d | error: %v",
						clientID, state.joiner.Name(), i+1, err)
					// Continue processing other batches even if one fails
					continue
				}
			}
		}
	}

	msg.Ack(false)
	return nil
}

// processAggregatedDataWithoutAck performs the join and publish without ACK/NACK
// Used for processing buffered batches that were already ACK'ed when buffered
func (h *JoinerHandler) processAggregatedDataWithoutAck(state *JoinerClientState, batchMessage *protocol.BatchMessage, clientID string) error {

	log.Printf("action: join_aggregate_data | client_id: %s | joiner: %s | records: %d | source: buffered",
		clientID, state.joiner.Name(), len(batchMessage.Records))

	// Perform join using this client's joiner
	joinedRecords, err := state.joiner.PerformJoin(batchMessage.Records, clientID)
	if err != nil {
		log.Printf("action: perform_join | client_id: %s | joiner: %s | result: fail | error: %v",
			clientID, state.joiner.Name(), err)
		return err
	}

	// Create output batch with joined data (propagate ClientID)
	outputBatch := h.createOutputBatch(state, batchMessage.BatchIndex, joinedRecords, batchMessage.EOF, clientID)

	// Publish joined results using the shared publisher with mutex protection
	h.pubMu.Lock()
	err = h.pub.SendToDatasetOutputExchanges(outputBatch)
	h.pubMu.Unlock()

	if err != nil {
		log.Printf("action: joiner_publish | client_id: %s | joiner: %s | result: fail | error: %v",
			clientID, state.joiner.Name(), err)
		return err
	}

	log.Printf("action: joiner_publish | client_id: %s | joiner: %s | result: success | batch_index: %d | joined_records: %d | eof: %t | source: buffered",
		clientID, state.joiner.Name(), batchMessage.BatchIndex, len(joinedRecords), batchMessage.EOF)

	// Cleanup when EOF received if the joiner requires it (e.g., large datasets like Q4 Users)
	if batchMessage.EOF {
		if state.joiner.ShouldCleanupAfterEOF() {
			log.Printf("action: joiner_eof_received | client_id: %s | joiner: %s | result: cleaning_up",
				clientID, state.joiner.Name())
			h.cleanupClientState(clientID, state)
		} else {
			// Keep reference data to handle multiple EOF batches from different upstream nodes
			// (e.g., 5 Q4 User Joiners all send batches with EOF to 1 Q4 Store Joiner)
			log.Printf("action: joiner_eof_received | client_id: %s | joiner: %s | note: keeping_reference_data_for_multiple_eof_batches",
				clientID, state.joiner.Name())
		}
	}

	// No ACK here - batch was already ACK'ed when it was buffered
	return nil
}

// processAggregatedData performs the actual join and publishes results
func (h *JoinerHandler) processAggregatedData(state *JoinerClientState, batchMessage *protocol.BatchMessage, msg amqp091.Delivery, clientID string) error {

	log.Printf("action: join_aggregate_data | client_id: %s | joiner: %s | records: %d",
		clientID, state.joiner.Name(), len(batchMessage.Records))

	// Perform join using this client's joiner
	joinedRecords, err := state.joiner.PerformJoin(batchMessage.Records, clientID)
	if err != nil {
		log.Printf("action: perform_join | client_id: %s | joiner: %s | result: fail | error: %v",
			clientID, state.joiner.Name(), err)
		msg.Nack(false, true)
		return err
	}

	// Create output batch with joined data (propagate ClientID)
	outputBatch := h.createOutputBatch(state, batchMessage.BatchIndex, joinedRecords, batchMessage.EOF, clientID)

	// Publish joined results using the shared publisher with mutex protection
	h.pubMu.Lock()
	err = h.pub.SendToDatasetOutputExchanges(outputBatch)
	h.pubMu.Unlock()

	if err != nil {
		log.Printf("action: joiner_publish | client_id: %s | joiner: %s | result: fail | error: %v",
			clientID, state.joiner.Name(), err)
		msg.Nack(false, true)
		return err
	}

	log.Printf("action: joiner_publish | client_id: %s | joiner: %s | result: success | batch_index: %d | joined_records: %d | eof: %t",
		clientID, state.joiner.Name(), batchMessage.BatchIndex, len(joinedRecords), batchMessage.EOF)

	// Cleanup when EOF received if the joiner requires it (e.g., large datasets like Q4 Users)
	if batchMessage.EOF {
		if state.joiner.ShouldCleanupAfterEOF() {
			log.Printf("action: joiner_eof_received | client_id: %s | joiner: %s | result: cleaning_up",
				clientID, state.joiner.Name())
			h.cleanupClientState(clientID, state)
		} else {
			// Keep reference data to handle multiple EOF batches from different upstream nodes
			// (e.g., 5 Q4 User Joiners all send batches with EOF to 1 Q4 Store Joiner)
			log.Printf("action: joiner_eof_received | client_id: %s | joiner: %s | note: keeping_reference_data_for_multiple_eof_batches",
				clientID, state.joiner.Name())
		}
	}

	msg.Ack(false)
	return nil
} // handleAggregatedData processes aggregated data - buffers if reference data not ready, or joins immediately
func (h *JoinerHandler) handleAggregatedData(state *JoinerClientState, batchMessage *protocol.BatchMessage, msg amqp091.Delivery, clientID string) error {

	// Check if reference data is complete for this client
	state.mu.Lock()
	refDataComplete := state.referenceDatasetComplete
	state.mu.Unlock()

	if !refDataComplete {
		// Reference data not ready yet - buffer this aggregate batch in the joiner
		state.joiner.AddBufferedBatch(*batchMessage)
		bufferedCount := len(state.joiner.GetBufferedBatches())

		log.Printf("action: buffer_aggregate_batch | client_id: %s | joiner: %s | batch_index: %d | buffered_count: %d | reason: reference_data_not_ready",
			clientID, state.joiner.Name(), batchMessage.BatchIndex, bufferedCount)

		// ACK the message - it's now safely buffered in memory and will be persisted
		// when reference data EOF arrives
		msg.Ack(false)
		return nil
	}

	return h.processAggregatedData(state, batchMessage, msg, clientID)
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

func (h *JoinerHandler) initStateStore(role common.NodeRole) {
	if h.stateStore != nil {
		return
	}

	baseDir := os.Getenv("STATE_BASE_DIR")
	if baseDir == "" {
		baseDir = "/app/state"
	}

	store, err := storage.NewFileStateStore(baseDir, string(role))
	if err != nil {
		log.Printf("action: joiner_state_store_init | role: %s | result: fail | error: %v", role, err)
		return
	}

	h.stateStore = store
}

func (h *JoinerHandler) persistClientState(clientID string, state *JoinerClientState) error {
	if h.stateStore == nil {
		return nil
	}

	data, err := state.joiner.SerializeState()
	if err != nil {
		return fmt.Errorf("serialize joiner state: %w", err)
	}

	meta := state.metadataSnapshot()
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encode joiner metadata: %w", err)
	}

	return h.stateStore.Persist(clientID, &storage.Snapshot{
		Metadata: metaBytes,
		Data:     data,
	})
}

func (h *JoinerHandler) restoreClientState(clientID string, state *JoinerClientState) {
	if h.stateStore == nil {
		return
	}

	snapshot, err := h.stateStore.Load(clientID)
	if err != nil {
		if !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: joiner_restore_state | client_id: %s | result: fail | error: %v", clientID, err)
		}
		return
	}

	if len(snapshot.Data) > 0 {
		if err := state.joiner.RestoreState(snapshot.Data); err != nil {
			log.Printf("action: joiner_restore_state | client_id: %s | joiner: %s | result: fail | error: %v",
				clientID, state.joiner.Name(), err)
			return
		}

	}

	if len(snapshot.Metadata) > 0 {
		var meta joinerSnapshotMetadata
		if err := json.Unmarshal(snapshot.Metadata, &meta); err != nil {
			log.Printf("action: joiner_restore_metadata | client_id: %s | result: fail | error: %v", clientID, err)
		} else {
			state.applyMetadata(meta)
		}
	}

	bufferedCount := len(state.joiner.GetBufferedBatches())
	log.Printf("action: joiner_restore_complete | client_id: %s | buffered_batches: %d",
		clientID, bufferedCount)
}

func (h *JoinerHandler) deleteClientSnapshot(clientID string) {
	if h.stateStore == nil {
		return
	}

	if err := h.stateStore.Delete(clientID); err != nil {
		if !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: joiner_state_delete | client_id: %s | result: fail | error: %v", clientID, err)
		}
	}
}

// cleanupClientState removes a client's state from memory and disk after finalization
// Calls the joiner's Cleanup() method to release resources, then removes the state
func (h *JoinerHandler) cleanupClientState(clientID string, state *JoinerClientState) {
	// Let the joiner clean up its own resources (maps, slices, file descriptors, etc.)
	if err := state.joiner.Cleanup(); err != nil {
		log.Printf("action: joiner_cleanup | client_id: %s | result: fail | error: %v", clientID, err)
	}

	// Delete persisted state files
	h.deleteClientSnapshot(clientID)

	// Remove the entire client state from the map
	h.statesMu.Lock()
	delete(h.states, clientID)
	h.statesMu.Unlock()

	log.Printf("action: joiner_cleanup_complete | client_id: %s", clientID)
}

func (s *JoinerClientState) metadataSnapshot() joinerSnapshotMetadata {
	s.mu.Lock()
	defer s.mu.Unlock()

	return joinerSnapshotMetadata{
		ReferenceDatasetComplete: s.referenceDatasetComplete,
	}
}

func (s *JoinerClientState) applyMetadata(meta joinerSnapshotMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.referenceDatasetComplete = meta.ReferenceDatasetComplete
}
