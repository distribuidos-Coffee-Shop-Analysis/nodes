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
	newJoiner func() joiners.Joiner         // Factory function to create new joiners per client
	states    map[string]*JoinerClientState // map[string]*JoinerClientState - keyed by clientID
	statesMu  sync.RWMutex                  // Protects states map

	// Reusable publisher to avoid channel exhaustion
	pub   *middleware.Publisher
	pubMu sync.Mutex

	stateStore storage.StateStore
}

const joinerSnapshotVersion = 1

type joinerSnapshotMetadata struct {
	Version                  int  `json:"version"`
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
		bufferedAggregateBatches: make([]bufferedBatch, 0),
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

	// Check if this is the last batch of reference data (EOF received)
	if batchMessage.EOF {
		state.mu.Lock()
		state.referenceDatasetComplete = true
		bufferedBatches := state.bufferedAggregateBatches
		state.bufferedAggregateBatches = make([]bufferedBatch, 0) // Clear buffer
		state.mu.Unlock()

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
				err := h.processAggregatedData(state, buffered.batchMessage, buffered.connection, buffered.wiring, buffered.delivery, clientID)
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

// processAggregatedData performs the actual join and publishes results
func (h *JoinerHandler) processAggregatedData(state *JoinerClientState, batchMessage *protocol.BatchMessage,
	_ *amqp091.Connection, _ *common.NodeWiring, msg amqp091.Delivery, clientID string) error {

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

	// Cleanup when EOF received - all joins for this client are complete
	// Note: We don't cleanup immediately because multiple upstream nodes may send EOF
	// (e.g., 5 Q4 User Joiners all send batches with EOF to 1 Q4 Store Joiner)
	// The cleanup happens when the handler decides it's safe (timeout, memory pressure, etc.)
	if batchMessage.EOF {
		// For now, we keep the reference data to handle multiple EOF batches from different upstream nodes
		// Cleanup will happen when the node shuts down or on explicit cleanup command
		log.Printf("action: joiner_eof_received | client_id: %s | joiner: %s | note: keeping_reference_data_for_multiple_eof_batches",
			clientID, state.joiner.Name())
	}

	msg.Ack(false)
	return nil
} // handleAggregatedData processes aggregated data - buffers if reference data not ready, or joins immediately
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

	persistable, ok := state.joiner.(joiners.PersistentJoiner)
	if !ok {
		return nil
	}

	data, err := persistable.SerializeState()
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

	persistable, ok := state.joiner.(joiners.PersistentJoiner)
	if !ok {
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
		if err := persistable.RestoreState(snapshot.Data); err != nil {
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

func (s *JoinerClientState) metadataSnapshot() joinerSnapshotMetadata {
	s.mu.Lock()
	defer s.mu.Unlock()

	return joinerSnapshotMetadata{
		Version:                  joinerSnapshotVersion,
		ReferenceDatasetComplete: s.referenceDatasetComplete,
	}
}

func (s *JoinerClientState) applyMetadata(meta joinerSnapshotMetadata) {
	if meta.Version != joinerSnapshotVersion {
		log.Printf("action: joiner_metadata_version_mismatch | expected: %d | got: %d",
			joinerSnapshotVersion, meta.Version)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.referenceDatasetComplete = meta.ReferenceDatasetComplete
}
