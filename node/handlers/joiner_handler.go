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

func (h *JoinerHandler) Name() string {
	return "joiner_" + h.sampleName()
}

func (h *JoinerHandler) sampleName() string {
	return h.newJoiner().Name()
}

func (h *JoinerHandler) getState(clientID string) *JoinerClientState {
	h.statesMu.RLock()
	if state, ok := h.states[clientID]; ok {
		h.statesMu.RUnlock()
		return state
	}
	h.statesMu.RUnlock()

	h.statesMu.Lock()
	defer h.statesMu.Unlock()

	if state, ok := h.states[clientID]; ok {
		return state
	}

	st := &JoinerClientState{
		joiner:                   h.newJoiner(),
		referenceDatasetComplete: false,
	}
	h.restoreClientState(clientID, st)
	h.states[clientID] = st
	return st
}

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

// Handle processes batches - Serialize → Persist → ACK
func (h *JoinerHandler) Handle(batchMessage *protocol.BatchMessage, connection *amqp091.Connection,
	wiring *common.NodeWiring, clientWG *sync.WaitGroup, msg amqp091.Delivery) error {

	clientWG.Add(1)
	defer clientWG.Done()

	clientID := batchMessage.ClientID
	state := h.getState(clientID)

	if state.joiner == nil {
		msg.Ack(false)
		return nil
	}

	if state.joiner.AcceptsReferenceType(batchMessage.DatasetType) {
		return h.handleReferenceDataset(state, batchMessage, msg, clientID)
	} else if state.joiner.AcceptsAggregateType(batchMessage.DatasetType) {
		return h.handleAggregatedData(state, batchMessage, msg, clientID)
	} else {
		log.Printf("action: joiner_handle | client_id: %s | result: fail | error: unsupported dataset type: %s",
			clientID, batchMessage.DatasetType)
		msg.Ack(false)
		return nil
	}
}

// handleReferenceDataset: Serialize → Persist → ACK → (if EOF) process buffered
func (h *JoinerHandler) handleReferenceDataset(state *JoinerClientState, batchMessage *protocol.BatchMessage, msg amqp091.Delivery, clientID string) error {

	state.mu.Lock()

	// 1. Serialize reference
	data, err := state.joiner.SerializeReferenceRecords(batchMessage.Records)
	if err != nil {
		state.mu.Unlock()
		log.Printf("action: joiner_serialize | client_id: %s | joiner: %s | result: fail | error: %v",
			clientID, state.joiner.Name(), err)
		msg.Nack(false, true)
		return err
	}

	// 2. Persist incremental data
	if len(data) > 0 && h.stateStore != nil {
		if err := h.stateStore.PersistIncremental(clientID, data); err != nil {
			state.mu.Unlock()
			log.Printf("action: joiner_persist_data | client_id: %s | joiner: %s | result: fail | error: %v",
				clientID, state.joiner.Name(), err)
			msg.Nack(false, true)
			return err
		}
	}

	// 3. Handle EOF
	isEOF := batchMessage.EOF
	if isEOF {
		state.referenceDatasetComplete = true

		if err := h.persistMetadata(clientID, state); err != nil {
			state.mu.Unlock()
			log.Printf("action: joiner_persist_metadata | client_id: %s | joiner: %s | result: fail | error: %v",
				clientID, state.joiner.Name(), err)
			msg.Nack(false, true)
			return err
		}

		log.Printf("action: reference_data_complete | client_id: %s | joiner: %s",
			clientID, state.joiner.Name())
	}

	state.mu.Unlock()

	// 4. ACK
	msg.Ack(false)

	// 5. Process buffered batches (outside lock)
	if isEOF {
		h.processBufferedBatchesFromDisk(state, clientID)
	}

	return nil
}

// handleAggregatedData: either buffer (if reference not ready) or join
func (h *JoinerHandler) handleAggregatedData(state *JoinerClientState, batchMessage *protocol.BatchMessage, msg amqp091.Delivery, clientID string) error {

	state.mu.Lock()
	refDataComplete := state.referenceDatasetComplete
	state.mu.Unlock()

	if !refDataComplete {
		// Reference data not ready - serialize and persist as buffered
		state.mu.Lock()

		data, err := state.joiner.SerializeBufferedBatch(batchMessage)
		if err != nil {
			state.mu.Unlock()
			log.Printf("action: joiner_serialize_buffered | client_id: %s | joiner: %s | result: fail | error: %v",
				clientID, state.joiner.Name(), err)
			msg.Nack(false, true)
			return err
		}

		if len(data) > 0 && h.stateStore != nil {
			if err := h.stateStore.PersistBufferedBatches(clientID, data); err != nil {
				state.mu.Unlock()
				log.Printf("action: joiner_persist_buffered | client_id: %s | joiner: %s | result: fail | error: %v",
					clientID, state.joiner.Name(), err)
				msg.Nack(false, true)
				return err
			}
		}

		state.mu.Unlock()

		log.Printf("action: buffer_aggregate_batch | client_id: %s | joiner: %s | batch_index: %d",
			clientID, state.joiner.Name(), batchMessage.BatchIndex)

		msg.Ack(false)
		return nil
	}

	// Reference data is complete - perform join
	return h.performJoinAndPublish(state, batchMessage, msg, clientID)
}

func (h *JoinerHandler) performJoinAndPublish(state *JoinerClientState, batchMessage *protocol.BatchMessage, msg amqp091.Delivery, clientID string) error {

	var allIncrements [][]byte
	if h.stateStore != nil {
		var err error
		allIncrements, err = h.stateStore.LoadAllIncrements(clientID)
		if err != nil && !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: joiner_load_increments | client_id: %s | joiner: %s | result: fail | error: %v",
				clientID, state.joiner.Name(), err)
			allIncrements = nil
		}
	}

	joinedRecords, err := state.joiner.PerformJoin(batchMessage.Records, clientID, allIncrements)
	if err != nil {
		log.Printf("action: perform_join | client_id: %s | joiner: %s | result: fail | error: %v",
			clientID, state.joiner.Name(), err)
		msg.Nack(false, true)
		return err
	}

	outputBatch := &protocol.BatchMessage{
		Type:        protocol.MessageTypeBatch,
		DatasetType: state.joiner.GetOutputDatasetType(),
		ClientID:    clientID,
		BatchIndex:  batchMessage.BatchIndex,
		Records:     joinedRecords,
		EOF:         batchMessage.EOF,
	}

	h.pubMu.Lock()
	err = h.pub.SendToDatasetOutputExchanges(outputBatch)
	h.pubMu.Unlock()

	if err != nil {
		log.Printf("action: joiner_publish | client_id: %s | joiner: %s | result: fail | error: %v",
			clientID, state.joiner.Name(), err)
		msg.Nack(false, true)
		return err
	}

	msg.Ack(false)

	log.Printf("action: joiner_publish | client_id: %s | joiner: %s | result: success | batch_index: %d | joined_records: %d | eof: %t",
		clientID, state.joiner.Name(), batchMessage.BatchIndex, len(joinedRecords), batchMessage.EOF)

	if batchMessage.EOF && state.joiner.ShouldCleanupAfterEOF() {
		log.Printf("action: joiner_eof_received | client_id: %s | joiner: %s | result: cleaning_up",
			clientID, state.joiner.Name())
		h.cleanupClientState(clientID, state)
	}

	return nil
}

func (h *JoinerHandler) processBufferedBatchesFromDisk(state *JoinerClientState, clientID string) {
	if h.stateStore == nil {
		return
	}

	bufferedData, err := h.stateStore.LoadBufferedBatches(clientID)
	if err != nil {
		if !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: joiner_load_buffered | client_id: %s | result: fail | error: %v", clientID, err)
		}
		return
	}

	if len(bufferedData) == 0 {
		return
	}

	batches, err := state.joiner.RestoreBufferedBatches(bufferedData)
	if err != nil {
		log.Printf("action: joiner_restore_buffered | client_id: %s | result: fail | error: %v", clientID, err)
		return
	}

	log.Printf("action: joiner_process_buffered | client_id: %s | joiner: %s | buffered_batches: %d",
		clientID, state.joiner.Name(), len(batches))

	for i, batch := range batches {
		err := h.processBufferedBatch(state, &batch, clientID)
		if err != nil {
			log.Printf("action: process_buffered_batch | client_id: %s | joiner: %s | result: fail | batch_num: %d | error: %v",
				clientID, state.joiner.Name(), i+1, err)
			continue
		}
	}

	// Delete buffered batches from disk
	h.stateStore.DeleteBufferedBatches(clientID)
}

func (h *JoinerHandler) processBufferedBatch(state *JoinerClientState, batchMessage *protocol.BatchMessage, clientID string) error {

	var allIncrements [][]byte
	if h.stateStore != nil {
		var err error
		allIncrements, err = h.stateStore.LoadAllIncrements(clientID)
		if err != nil && !errors.Is(err, storage.ErrSnapshotNotFound) {
			allIncrements = nil
		}
	}

	joinedRecords, err := state.joiner.PerformJoin(batchMessage.Records, clientID, allIncrements)
	if err != nil {
		return err
	}

	outputBatch := &protocol.BatchMessage{
		Type:        protocol.MessageTypeBatch,
		DatasetType: state.joiner.GetOutputDatasetType(),
		ClientID:    clientID,
		BatchIndex:  batchMessage.BatchIndex,
		Records:     joinedRecords,
		EOF:         batchMessage.EOF,
	}

	h.pubMu.Lock()
	err = h.pub.SendToDatasetOutputExchanges(outputBatch)
	h.pubMu.Unlock()

	if err != nil {
		return err
	}

	log.Printf("action: joiner_publish | client_id: %s | joiner: %s | result: success | batch_index: %d | joined_records: %d | eof: %t | source: buffered",
		clientID, state.joiner.Name(), batchMessage.BatchIndex, len(joinedRecords), batchMessage.EOF)

	if batchMessage.EOF && state.joiner.ShouldCleanupAfterEOF() {
		h.cleanupClientState(clientID, state)
	}

	return nil
}

func (h *JoinerHandler) Shutdown() error {
	log.Printf("action: joiner_shutdown | result: start")

	h.statesMu.RLock()
	clientIDs := make([]string, 0, len(h.states))
	for clientID := range h.states {
		clientIDs = append(clientIDs, clientID)
	}
	h.statesMu.RUnlock()

	for _, clientID := range clientIDs {
		state := h.states[clientID]
		if state != nil {
			state.mu.Lock()
			h.persistMetadata(clientID, state)
			state.mu.Unlock()
			log.Printf("action: joiner_shutdown_persist | client_id: %s | result: success", clientID)
		}
	}

	log.Printf("action: joiner_shutdown | result: success | states_persisted: %d", len(clientIDs))
	return nil
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

func (h *JoinerHandler) persistMetadata(clientID string, state *JoinerClientState) error {
	if h.stateStore == nil {
		return nil
	}

	meta := joinerSnapshotMetadata{
		ReferenceDatasetComplete: state.referenceDatasetComplete,
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encode joiner metadata: %w", err)
	}

	if err := h.stateStore.Persist(clientID, &storage.Snapshot{
		Metadata: metaBytes,
		Data:     nil,
	}); err != nil {
		return fmt.Errorf("persist metadata: %w", err)
	}

	return nil
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
	} else if len(snapshot.Metadata) > 0 {
		var meta joinerSnapshotMetadata
		if err := json.Unmarshal(snapshot.Metadata, &meta); err != nil {
			log.Printf("action: joiner_restore_metadata | client_id: %s | result: fail | error: %v", clientID, err)
		} else {
			state.referenceDatasetComplete = meta.ReferenceDatasetComplete
			log.Printf("action: joiner_restore_metadata | client_id: %s | result: success | reference_complete: %t",
				clientID, meta.ReferenceDatasetComplete)
		}
	}

	log.Printf("action: joiner_restore_complete | client_id: %s | reference_data_complete: %t",
		clientID, state.referenceDatasetComplete)
}

func (h *JoinerHandler) cleanupClientState(clientID string, state *JoinerClientState) {
	if state.joiner != nil {
		if err := state.joiner.Cleanup(); err != nil {
			log.Printf("action: joiner_cleanup | client_id: %s | result: fail | error: %v", clientID, err)
		}
	}

	h.deleteClientSnapshot(clientID)

	state.mu.Lock()
	state.joiner = nil
	state.mu.Unlock()
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

	if err := h.stateStore.DeleteBufferedBatches(clientID); err != nil {
		if !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: joiner_buffered_delete | client_id: %s | result: fail | error: %v", clientID, err)
		}
	}

	if err := h.stateStore.DeleteAllIncrements(clientID); err != nil {
		log.Printf("action: joiner_increments_delete | client_id: %s | result: fail | error: %v", clientID, err)
	} else {
		log.Printf("action: joiner_increments_delete | client_id: %s | result: success", clientID)
	}
}
