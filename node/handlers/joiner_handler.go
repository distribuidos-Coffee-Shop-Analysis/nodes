package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	seenBatchIndices         map[int]bool // Batch indices that have been PERSISTED
	finalizeStarted          bool         // Set when finalize begins
	finalizeCompleted        bool         // Set after successful publish
}

// JoinerHandler manages join operations for multiple clients
type JoinerHandler struct {
	StatefulHandlerBase

	newJoiner func() joiners.Joiner         // Factory function to create new joiners per client
	states    map[string]*JoinerClientState // map[string]*JoinerClientState - keyed by clientID
	statesMu  sync.RWMutex                  // Protects states map

	pub   *middleware.Publisher
	pubMu sync.Mutex
}

type joinerSnapshotMetadata struct {
	ReferenceDatasetComplete bool `json:"reference_dataset_complete"`
	FinalizeStarted          bool `json:"finalize_started"`
	FinalizeCompleted        bool `json:"finalize_completed"`
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
		seenBatchIndices:         make(map[int]bool),
	}
	h.restoreClientState(clientID, st)
	h.states[clientID] = st
	return st
}

func (h *JoinerHandler) StartHandler(queueManager *middleware.QueueManager, clientWg *sync.WaitGroup) error {
	h.InitStateStore(queueManager.Wiring.Role)

	pub, err := middleware.NewPublisher(queueManager.Connection, queueManager.Wiring)
	if err != nil {
		log.Printf("action: create_publisher | result: fail | error: %v", err)
		return err
	}
	h.pub = pub

	h.checkPendingFinalizes()

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

	// 1. Check if already processed (idempotency)
	if state.seenBatchIndices[batchMessage.BatchIndex] {
		state.mu.Unlock()
		log.Printf("action: joiner_duplicate_reference | client_id: %s | joiner: %s | batch_index: %d | result: skipped",
			clientID, state.joiner.Name(), batchMessage.BatchIndex)
		msg.Ack(false)
		return nil
	}

	// 2. Serialize reference
	data, err := state.joiner.SerializeReferenceRecords(batchMessage.Records, batchMessage.BatchIndex)
	if err != nil {
		state.mu.Unlock()
		log.Printf("action: joiner_serialize | client_id: %s | joiner: %s | result: fail | error: %v",
			clientID, state.joiner.Name(), err)
		msg.Nack(false, true)
		return err
	}

	// 3. Persist incremental data (file named by batchIndex for uniqueness)
	if len(data) > 0 && h.StateStore() != nil {
		if err := h.StateStore().PersistIncremental(clientID, batchMessage.BatchIndex, data); err != nil {
			state.mu.Unlock()
			log.Printf("action: joiner_persist_data | client_id: %s | joiner: %s | result: fail | error: %v",
				clientID, state.joiner.Name(), err)
			msg.Nack(false, true)
			return err
		}

		// Cache increment in memory to avoid disk reads during join
		state.joiner.CacheIncrement(batchMessage.BatchIndex, data)
	}

	// 4. Mark as seen
	state.seenBatchIndices[batchMessage.BatchIndex] = true

	// 5. Handle EOF
	isEOF := batchMessage.EOF
	if isEOF {
		state.referenceDatasetComplete = true
		state.finalizeStarted = true // Mark finalize started (processing buffered batches)

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

	// 6. ACK
	msg.Ack(false)

	// 7. Process buffered batches (outside lock)
	if isEOF {
		h.processBufferedBatchesFromDisk(state, clientID)

		state.mu.Lock()
		state.finalizeCompleted = true
		state.mu.Unlock()
		h.persistMetadata(clientID, state)
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

		if len(data) > 0 && h.StateStore() != nil {
			if err := h.StateStore().PersistBufferedBatches(clientID, data); err != nil {
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
	if h.StateStore() != nil {
		var err error
		// Get cached batch indices to skip reading those files from disk
		excludeIndices := state.joiner.GetCachedBatchIndices()

		allIncrements, err = h.StateStore().LoadAllIncrementsExcluding(clientID, excludeIndices)
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
	if h.StateStore() == nil {
		return
	}

	bufferedData, err := h.StateStore().LoadBufferedBatches(clientID)
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
	h.StateStore().DeleteBufferedBatches(clientID)
}

func (h *JoinerHandler) processBufferedBatch(state *JoinerClientState, batchMessage *protocol.BatchMessage, clientID string) error {

	var allIncrements [][]byte
	if h.StateStore() != nil {
		var err error
		// Get cached batch indices to skip reading those files from disk
		excludeIndices := state.joiner.GetCachedBatchIndices()

		allIncrements, err = h.StateStore().LoadAllIncrementsExcluding(clientID, excludeIndices)
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

func (h *JoinerHandler) persistMetadata(clientID string, state *JoinerClientState) error {
	if h.StateStore() == nil {
		return nil
	}

	meta := joinerSnapshotMetadata{
		ReferenceDatasetComplete: state.referenceDatasetComplete,
		FinalizeStarted:          state.finalizeStarted,
		FinalizeCompleted:        state.finalizeCompleted,
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encode joiner metadata: %w", err)
	}

	if err := h.StateStore().Persist(clientID, &storage.Snapshot{
		Metadata: metaBytes,
		Data:     nil,
	}); err != nil {
		return fmt.Errorf("persist metadata: %w", err)
	}

	return nil
}

func (h *JoinerHandler) restoreClientState(clientID string, state *JoinerClientState) {
	if h.StateStore() == nil {
		return
	}

	snapshot, err := h.StateStore().Load(clientID)
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
			state.finalizeStarted = meta.FinalizeStarted
			state.finalizeCompleted = meta.FinalizeCompleted
			log.Printf("action: joiner_restore_metadata | client_id: %s | result: success | "+
				"reference_complete: %t | finalize_started: %t | finalize_completed: %t",
				clientID, meta.ReferenceDatasetComplete, meta.FinalizeStarted, meta.FinalizeCompleted)
		}
	}

	batchIndices, err := h.StateStore().LoadBatchIndicesFromIncrements(clientID)
	if err != nil {
		log.Printf("action: joiner_restore_batch_indices | client_id: %s | result: fail | error: %v", clientID, err)
	} else if len(batchIndices) > 0 {
		state.seenBatchIndices = batchIndices
		log.Printf("action: joiner_restore_batch_indices | client_id: %s | result: success | count: %d",
			clientID, len(batchIndices))
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
	if h.StateStore() == nil {
		return
	}

	if err := h.StateStore().Delete(clientID); err != nil {
		if !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: joiner_state_delete | client_id: %s | result: fail | error: %v", clientID, err)
		}
	}

	if err := h.StateStore().DeleteBufferedBatches(clientID); err != nil {
		if !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: joiner_buffered_delete | client_id: %s | result: fail | error: %v", clientID, err)
		}
	}

	if err := h.StateStore().DeleteAllIncrements(clientID); err != nil {
		log.Printf("action: joiner_increments_delete | client_id: %s | result: fail | error: %v", clientID, err)
	} else {
		log.Printf("action: joiner_increments_delete | client_id: %s | result: success", clientID)
	}
}

// checkPendingFinalizes scans for clients that crashed during finalize and resumes them
func (h *JoinerHandler) checkPendingFinalizes() {
	if h.StateStore() == nil {
		return
	}

	clientIDs := h.ScanExistingClients()

	for _, clientID := range clientIDs {
		state := h.getState(clientID)

		state.mu.Lock()
		needsFinalize := state.finalizeStarted && !state.finalizeCompleted
		alreadyCompleted := state.finalizeCompleted
		state.mu.Unlock()

		if needsFinalize {
			log.Printf("action: joiner_resume_finalize | client_id: %s | result: start | "+
				"reason: crash_during_previous_finalize",
				clientID)

			h.processBufferedBatchesFromDisk(state, clientID)

			state.mu.Lock()
			state.finalizeCompleted = true
			state.mu.Unlock()
			h.persistMetadata(clientID, state)

			log.Printf("action: joiner_resume_finalize | client_id: %s | result: success", clientID)
		} else if alreadyCompleted {
			log.Printf("action: joiner_cleanup_completed | client_id: %s | reason: finalize_already_completed",
				clientID)
			h.cleanupClientState(clientID, state)
		}
	}
}
