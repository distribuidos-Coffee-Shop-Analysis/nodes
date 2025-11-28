package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/aggregates"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/storage"

	"github.com/rabbitmq/amqp091-go"
)

// AggregateClientState holds per-client state for aggregate operations
type AggregateClientState struct {
	mu sync.Mutex // Single lock for all state operations

	aggregate            aggregates.Aggregate
	eofReceived          bool
	oneTimeFinalize      bool
	expectedTotalBatches int
	seenBatchIndices     map[int]bool // Batch indices that have been PERSISTED
	uniqueBatchCount     atomic.Int32
	finalizeStarted      bool // Set when finalize begins
	finalizeCompleted    bool // Set after successful publish
}

// AggregateHandler manages aggregate operations for multiple clients
type AggregateHandler struct {
	StatefulHandlerBase

	newAggregate func() aggregates.Aggregate      // Factory function to create new aggregates per client
	states       map[string]*AggregateClientState // map[string]*AggregateClientState - keyed by clientID
	statesMu     sync.RWMutex                     // Protects states map

	pub   *middleware.Publisher
	pubMu sync.Mutex
}

type aggregateSnapshotMetadata struct {
	EOFReceived          bool `json:"eof_received"`
	ExpectedTotalBatches int  `json:"expected_total_batches"`
	OneTimeFinalize      bool `json:"one_time_finalize"`
	FinalizeStarted      bool `json:"finalize_started"`
	FinalizeCompleted    bool `json:"finalize_completed"`
}

// NewAggregateHandler creates a new aggregate handler with a factory function
func NewAggregateHandler(newAggregate func() aggregates.Aggregate) *AggregateHandler {
	h := &AggregateHandler{
		newAggregate: newAggregate,
		states:       make(map[string]*AggregateClientState),
	}

	return h
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
	st := &AggregateClientState{
		aggregate:            h.newAggregate(),
		oneTimeFinalize:      true,
		seenBatchIndices:     make(map[int]bool),
		expectedTotalBatches: 0,
	}
	h.restoreClientState(clientID, st)
	h.states[clientID] = st
	return st
}

// StartHandler starts the aggregate handler
func (h *AggregateHandler) StartHandler(queueManager *middleware.QueueManager, clientWg *sync.WaitGroup) error {
	h.InitStateStore(queueManager.Wiring.Role)

	pub, err := middleware.NewPublisher(queueManager.Connection, queueManager.Wiring)
	if err != nil {
		log.Printf("action: create_publisher | result: fail | error: %v", err)
		return err
	}
	h.pub = pub

	h.checkPendingFinalizes()

	err = queueManager.StartConsuming(func(batchMessage *protocol.BatchMessage, delivery amqp091.Delivery) {
		h.Handle(batchMessage, queueManager.Connection,
			queueManager.Wiring, clientWg, delivery)
	})
	if err != nil {
		log.Printf("action: aggregate_consume | result: fail | error: %v", err)
		return err
	}
	return nil
}

// checkPendingFinalizes scans for clients that crashed during finalize and resumes them
func (h *AggregateHandler) checkPendingFinalizes() {
	if h.StateStore() == nil {
		return
	}

	clientIDs := h.ScanExistingClients()

	for _, clientID := range clientIDs {
		state := h.getState(clientID)

		state.mu.Lock()
		needsFinalize := state.finalizeStarted && !state.finalizeCompleted
		batchIndex := state.expectedTotalBatches - 1
		state.mu.Unlock()

		if needsFinalize {
			log.Printf("action: aggregate_resume_finalize | client_id: %s | result: start | "+
				"reason: crash_during_previous_finalize",
				clientID)

			finalizeBatch := &protocol.BatchMessage{
				ClientID:   clientID,
				BatchIndex: batchIndex,
				EOF:        true,
			}

			if err := h.finalizeClient(clientID, state, finalizeBatch); err != nil {
				log.Printf("action: aggregate_resume_finalize | client_id: %s | result: fail | error: %v",
					clientID, err)
			} else {
				log.Printf("action: aggregate_resume_finalize | client_id: %s | result: success",
					clientID)
			}
		} else if state.finalizeCompleted {
			log.Printf("action: aggregate_cleanup_completed | client_id: %s | reason: finalize_already_completed",
				clientID)
			h.cleanupClientState(clientID, state)
		}
	}
}

// scanExistingClients returns client IDs that have persisted metadata
// Handle processes a batch message with a simple linear flow:
// Lock → Check duplicate → Serialize → Persist → Mark seen → Unlock → ACK
func (h *AggregateHandler) Handle(batchMessage *protocol.BatchMessage, connection *amqp091.Connection,
	wiring *common.NodeWiring, clientWG *sync.WaitGroup, msg amqp091.Delivery) error {

	clientWG.Add(1)
	defer clientWG.Done()

	clientID := batchMessage.ClientID
	state := h.getState(clientID)

	state.mu.Lock()

	// 1. Check if already processed (idempotency)
	if state.seenBatchIndices[batchMessage.BatchIndex] {
		state.mu.Unlock()
		log.Printf("action: aggregate_duplicate_batch | client_id: %s | aggregate: %s | batch_index: %d | result: skipped",
			clientID, state.aggregate.Name(), batchMessage.BatchIndex)
		msg.Ack(false)
		return nil
	}

	// 2. Serialize records
	data, err := state.aggregate.SerializeRecords(batchMessage.Records, batchMessage.BatchIndex)
	if err != nil {
		state.mu.Unlock()
		log.Printf("action: aggregate_serialize | client_id: %s | aggregate: %s | result: fail | error: %v",
			clientID, state.aggregate.Name(), err)
		msg.Nack(false, true)
		return err
	}

	// 3. Persist data (file named by batchIndex for uniqueness)
	if len(data) > 0 && h.StateStore() != nil {
		if err := h.StateStore().PersistIncremental(clientID, batchMessage.BatchIndex, data); err != nil {
			state.mu.Unlock()
			log.Printf("action: aggregate_persist_data | client_id: %s | aggregate: %s | result: fail | error: %v",
				clientID, state.aggregate.Name(), err)
			msg.Nack(false, true)
			return err
		}

		// Cache increment in memory to avoid disk reads during finalize
		state.aggregate.CacheIncrement(batchMessage.BatchIndex, data)
	}

	// 4. Mark as seen in memory
	state.seenBatchIndices[batchMessage.BatchIndex] = true
	state.uniqueBatchCount.Add(1)

	// 5. Handle EOF
	if batchMessage.EOF {
		expectedTotalBatches := batchMessage.BatchIndex + 1
		if expectedTotalBatches > state.expectedTotalBatches {
			state.expectedTotalBatches = expectedTotalBatches
		}
		state.eofReceived = true

		log.Printf("action: aggregate_eof_received | client_id: %s | aggregate: %s | max_batch_index: %d | "+
			"expected_total: %d | accumulated_so_far: %d",
			clientID, state.aggregate.Name(), batchMessage.BatchIndex,
			expectedTotalBatches, state.uniqueBatchCount.Load())

		// Persist metadata with EOF info
		h.persistMetadata(clientID, state)
	}

	// 6. Check if ready to finalize
	shouldFinalize := false
	if state.eofReceived && state.oneTimeFinalize && state.expectedTotalBatches > 0 {
		if state.hasAllBatchesUpTo(state.expectedTotalBatches - 1) {
			shouldFinalize = true
			state.oneTimeFinalize = false
			log.Printf("action: aggregate_ready_to_finalize | client_id: %s | aggregate: %s | "+
				"total_batches: %d",
				clientID, state.aggregate.Name(), state.expectedTotalBatches)
		} else {
			missing := state.countMissingBatches(state.expectedTotalBatches - 1)
			log.Printf("action: aggregate_waiting_for_batches | client_id: %s | aggregate: %s | "+
				"missing_batches: %d | received: %d | expected: %d",
				clientID, state.aggregate.Name(),
				missing, state.uniqueBatchCount.Load(), state.expectedTotalBatches)
		}
	}

	state.mu.Unlock()

	msg.Ack(false)

	if shouldFinalize {
		return h.finalizeClient(clientID, state, batchMessage)
	}

	return nil
}

// finalizeClient performs the complete finalization process for a client
func (h *AggregateHandler) finalizeClient(clientID string, state *AggregateClientState, batchMessage *protocol.BatchMessage) error {
	log.Printf("action: aggregate_finalize_started | client_id: %s | aggregate: %s | result: start | "+
		"total_batches_processed: %d | expected_batches: %d",
		clientID, state.aggregate.Name(), state.uniqueBatchCount.Load(), state.expectedTotalBatches)

	state.mu.Lock()
	state.finalizeStarted = true
	state.mu.Unlock()
	h.persistMetadata(clientID, state)

	var allIncrements [][]byte
	if h.StateStore() != nil {
		var err error
		// Get cached batch indices to skip reading those files from disk
		excludeIndices := state.aggregate.GetCachedBatchIndices()

		allIncrements, err = h.StateStore().LoadAllIncrementsExcluding(clientID, excludeIndices)
		if err != nil && !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: aggregate_load_increments | client_id: %s | aggregate: %s | result: fail | error: %v",
				clientID, state.aggregate.Name(), err)
			allIncrements = nil
		} else if len(allIncrements) > 0 || len(excludeIndices) > 0 {
			log.Printf("action: aggregate_load_increments | client_id: %s | aggregate: %s | "+
				"from_disk: %d | cached: %d | result: success",
				clientID, state.aggregate.Name(), len(allIncrements), len(excludeIndices))
		}
	}

	batchesToPublish, err := state.aggregate.GetBatchesToPublish(allIncrements, batchMessage.BatchIndex, clientID)
	if err != nil {
		log.Printf("action: aggregate_finalize | client_id: %s | aggregate: %s | result: fail | error: %v",
			clientID, state.aggregate.Name(), err)
		return err
	}

	h.pubMu.Lock()
	err = h.publishBatches(h.pub, batchesToPublish)
	h.pubMu.Unlock()

	if err != nil {
		log.Printf("action: aggregate_publish | client_id: %s | aggregate: %s | result: fail | error: %v",
			clientID, state.aggregate.Name(), err)
		return err
	}

	state.mu.Lock()
	state.finalizeCompleted = true
	state.mu.Unlock()
	h.persistMetadata(clientID, state)

	log.Printf("action: aggregate_finalize | client_id: %s | aggregate: %s | result: success",
		clientID, state.aggregate.Name())

	h.cleanupClientState(clientID, state)
	return nil
}

func (h *AggregateHandler) cleanupClientState(clientID string, state *AggregateClientState) {
	if err := state.aggregate.Cleanup(); err != nil {
		log.Printf("action: aggregate_cleanup | client_id: %s | result: fail | error: %v", clientID, err)
	}

	h.deleteClientSnapshot(clientID)

	state.mu.Lock()
	state.seenBatchIndices = nil
	state.mu.Unlock()

	h.statesMu.Lock()
	delete(h.states, clientID)
	h.statesMu.Unlock()
}

func (h *AggregateHandler) publishBatches(publisher *middleware.Publisher, batchesToPublish []aggregates.BatchToPublish) error {
	for i, batchToPublish := range batchesToPublish {
		err := publisher.SendToDatasetOutputExchangesWithRoutingKey(batchToPublish.Batch, batchToPublish.RoutingKey)
		if err != nil {
			return fmt.Errorf("failed to publish batch %d: %w", i+1, err)
		}
	}

	return nil
}

func (h *AggregateHandler) persistMetadata(clientID string, state *AggregateClientState) error {
	if h.StateStore() == nil {
		return nil
	}

	meta := aggregateSnapshotMetadata{
		EOFReceived:          state.eofReceived,
		ExpectedTotalBatches: state.expectedTotalBatches,
		OneTimeFinalize:      state.oneTimeFinalize,
		FinalizeStarted:      state.finalizeStarted,
		FinalizeCompleted:    state.finalizeCompleted,
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encode aggregate metadata: %w", err)
	}

	if err := h.StateStore().Persist(clientID, &storage.Snapshot{
		Metadata: metaBytes,
		Data:     nil,
	}); err != nil {
		return fmt.Errorf("persist metadata: %w", err)
	}

	return nil
}

func (h *AggregateHandler) restoreClientState(clientID string, state *AggregateClientState) {
	if h.StateStore() == nil {
		return
	}

	// Restore metadata
	snapshot, err := h.StateStore().Load(clientID)
	if err != nil {
		if !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: aggregate_restore_state | client_id: %s | result: fail | error: %v", clientID, err)
		}
	} else if len(snapshot.Metadata) > 0 {
		var meta aggregateSnapshotMetadata
		if err := json.Unmarshal(snapshot.Metadata, &meta); err != nil {
			log.Printf("action: aggregate_restore_metadata | client_id: %s | result: fail | error: %v", clientID, err)
		} else {
			state.eofReceived = meta.EOFReceived
			state.expectedTotalBatches = meta.ExpectedTotalBatches
			state.oneTimeFinalize = meta.OneTimeFinalize
			state.finalizeStarted = meta.FinalizeStarted
			state.finalizeCompleted = meta.FinalizeCompleted
			log.Printf("action: aggregate_restore_metadata | client_id: %s | result: success | "+
				"eof_received: %t | expected_batches: %d | finalize_started: %t | finalize_completed: %t",
				clientID, meta.EOFReceived, meta.ExpectedTotalBatches, meta.FinalizeStarted, meta.FinalizeCompleted)
		}
	}

	// Restore batch indices from incremental file headers
	batchIndices, err := h.StateStore().LoadBatchIndicesFromIncrements(clientID)
	if err != nil {
		log.Printf("action: aggregate_restore_batch_indices | client_id: %s | result: fail | error: %v", clientID, err)
	} else if len(batchIndices) > 0 {
		state.seenBatchIndices = batchIndices
		state.uniqueBatchCount.Store(int32(len(batchIndices)))
		log.Printf("action: aggregate_restore_batch_indices | client_id: %s | result: success | count: %d",
			clientID, len(batchIndices))
	}

	log.Printf("action: aggregate_restore_complete | client_id: %s | aggregate: %s | "+
		"metadata: loaded | batch_indices: %d",
		clientID, state.aggregate.Name(), len(batchIndices))
}

func (h *AggregateHandler) deleteClientSnapshot(clientID string) {
	if h.StateStore() == nil {
		return
	}

	if err := h.StateStore().Delete(clientID); err != nil {
		if !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: aggregate_state_delete | client_id: %s | result: fail | error: %v", clientID, err)
		}
	}

	if err := h.StateStore().DeleteAllIncrements(clientID); err != nil {
		log.Printf("action: aggregate_increments_delete | client_id: %s | result: fail | error: %v", clientID, err)
	} else {
		log.Printf("action: aggregate_increments_delete | client_id: %s | result: success", clientID)
	}
}

// Shutdown persists all pending state before stopping
func (h *AggregateHandler) Shutdown() error {
	log.Printf("action: aggregate_shutdown | result: start")

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

			log.Printf("action: aggregate_shutdown_persist | client_id: %s | result: success | "+
				"total_seen: %d", clientID, state.uniqueBatchCount.Load())
		}
	}

	log.Printf("action: aggregate_shutdown | result: success | states_persisted: %d", len(clientIDs))
	return nil
}

// Helper methods for AggregateClientState

func (s *AggregateClientState) hasAllBatchesUpTo(maxIndex int) bool {
	if s.seenBatchIndices == nil {
		return false
	}

	if len(s.seenBatchIndices) < maxIndex+1 {
		return false
	}

	for i := 0; i <= maxIndex; i++ {
		if !s.seenBatchIndices[i] {
			return false
		}
	}

	return true
}

func (s *AggregateClientState) countMissingBatches(maxIndex int) int {
	if s.seenBatchIndices == nil {
		return maxIndex + 1
	}

	missing := 0
	for i := 0; i <= maxIndex; i++ {
		if !s.seenBatchIndices[i] {
			missing++
		}
	}

	return missing
}
