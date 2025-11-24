package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
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
	mu                   sync.Mutex
	aggregate            aggregates.Aggregate
	eofReceived          bool
	oneTimeFinalize      bool
	expectedTotalBatches int
	seenBatchIndices     map[int]bool
	uniqueBatchCount     atomic.Int32

	// Batch tracking for ACK-after-persist strategy
	batchesSinceLastPersist int                // Batches accumulated since last persist
	pendingDeliveries       []amqp091.Delivery // Deliveries waiting for persist + ACK

	// Persist synchronization (prevents multiple concurrent persists)
	isPersisting atomic.Bool // Flag to ensure only one goroutine persists at a time
}

// AggregateHandler manages aggregate operations for multiple clients
type AggregateHandler struct {
	newAggregate func() aggregates.Aggregate      // Factory function to create new aggregates per client
	states       map[string]*AggregateClientState // map[string]*AggregateClientState - keyed by clientID
	statesMu     sync.RWMutex                     // Protects states map

	pub   *middleware.Publisher
	pubMu sync.Mutex

	stateStore storage.StateStore
}

type aggregateSnapshotMetadata struct {
	EOFReceived          bool `json:"eof_received"`
	ExpectedTotalBatches int  `json:"expected_total_batches"`
	OneTimeFinalize      bool `json:"one_time_finalize"`
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
	st := &AggregateClientState{
		aggregate:               h.newAggregate(),
		oneTimeFinalize:         true,
		seenBatchIndices:        make(map[int]bool),
		expectedTotalBatches:    0,
		batchesSinceLastPersist: 0,
		pendingDeliveries:       make([]amqp091.Delivery, 0),
	}
	h.restoreClientState(clientID, st)
	h.states[clientID] = st
	return st
}

// StartHandler starts the aggregate handler
func (h *AggregateHandler) StartHandler(queueManager *middleware.QueueManager, clientWg *sync.WaitGroup) error {
	h.initStateStore(queueManager.Wiring.Role)

	pub, err := middleware.NewPublisher(queueManager.Connection, queueManager.Wiring)
	if err != nil {
		log.Printf("action: create_publisher | result: fail | error: %v", err)
		return err
	}
	h.pub = pub

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

// Handle processes a batch by accumulating it, and publishes final results when all batches are complete
func (h *AggregateHandler) Handle(batchMessage *protocol.BatchMessage, connection *amqp091.Connection,
	wiring *common.NodeWiring, clientWG *sync.WaitGroup, msg amqp091.Delivery) error {

	clientWG.Add(1)
	defer clientWG.Done()

	clientID := batchMessage.ClientID
	state := h.getState(clientID)

	// Check if this batch was already processed (duplicate detection for recovery)
	state.mu.Lock()
	alreadySeen := state.seenBatchIndices[batchMessage.BatchIndex]
	state.mu.Unlock()

	if alreadySeen {
		// This batch was already processed before a crash/restart
		// ACK it to prevent requeue, but don't process again
		log.Printf("action: aggregate_duplicate_batch | client_id: %s | aggregate: %s | batch_index: %d | "+
			"result: skipped | reason: already_processed",
			clientID, state.aggregate.Name(), batchMessage.BatchIndex)

		// However, if this duplicate batch has EOF flag, we still need to register it
		// This handles the case where EOF arrived but wasn't persisted before crash
		if batchMessage.EOF {
			state.mu.Lock()
			expectedTotalBatches := batchMessage.BatchIndex + 1
			if expectedTotalBatches > state.expectedTotalBatches {
				state.expectedTotalBatches = expectedTotalBatches
			}
			state.eofReceived = true

			// Check if we should finalize (all batches already received before crash)
			if state.oneTimeFinalize && state.expectedTotalBatches > 0 {
				allBatchesReceived := state.hasAllBatchesUpTo(state.expectedTotalBatches - 1)
				if allBatchesReceived {
					state.oneTimeFinalize = false
					state.mu.Unlock()

					log.Printf("action: aggregate_eof_received_duplicate | client_id: %s | aggregate: %s | "+
						"max_batch_index: %d | expected_total: %d | ready_to_finalize: true",
						clientID, state.aggregate.Name(), batchMessage.BatchIndex, expectedTotalBatches)

					// Finalize immediately (no new data to persist)
					return h.finalizeClient(clientID, state, batchMessage)
				}
			}
			state.mu.Unlock()

			log.Printf("action: aggregate_eof_received_duplicate | client_id: %s | aggregate: %s | "+
				"max_batch_index: %d | expected_total: %d",
				clientID, state.aggregate.Name(), batchMessage.BatchIndex, expectedTotalBatches)
		}

		msg.Ack(false)
		return nil
	}

	// Accumulate this batch for this specific client
	err := state.aggregate.AccumulateBatch(batchMessage.Records, batchMessage.BatchIndex)
	if err != nil {
		log.Printf("action: aggregate_accumulate | client_id: %s | aggregate: %s | result: fail | error: %v",
			clientID, state.aggregate.Name(), err)
		msg.Ack(false) // Ack to remove empty or bad batches
		return err
	}

	// Only track batch index AFTER successful accumulation
	// This ensures we don't count batches that failed to process
	state.trackBatchIndex(batchMessage.BatchIndex)

	// Track this batch and add delivery to pending list (for ACK-after-persist)
	state.mu.Lock()
	state.batchesSinceLastPersist++
	state.pendingDeliveries = append(state.pendingDeliveries, msg)
	batchesSinceLastPersist := state.batchesSinceLastPersist
	pendingCount := len(state.pendingDeliveries)

	// Check if this batch has EOF flag
	if batchMessage.EOF {
		// Store the maximum batch index - this tells us the expected range is [0, maxBatchIndex]
		expectedTotalBatches := batchMessage.BatchIndex + 1
		if expectedTotalBatches > state.expectedTotalBatches {
			state.expectedTotalBatches = expectedTotalBatches
		}
		state.eofReceived = true

		log.Printf("action: aggregate_eof_received | client_id: %s | aggregate: %s | max_batch_index: %d | "+
			"expected_total: %d | accumulated_so_far: %d",
			clientID, state.aggregate.Name(), batchMessage.BatchIndex,
			expectedTotalBatches, state.getUniqueBatchCount())
	}

	// Check if we should finalize: verify ALL batch indices [0, expectedTotal-1] are present
	// This handles prefetch/in-flight batches correctly - we wait until ALL are tracked
	shouldFinalize := false
	if state.eofReceived && state.oneTimeFinalize && state.expectedTotalBatches > 0 {
		// Check if we have received all batches from 0 to expectedTotalBatches-1
		allBatchesReceived := state.hasAllBatchesUpTo(state.expectedTotalBatches - 1)
		if allBatchesReceived {
			shouldFinalize = true
			state.oneTimeFinalize = false // Ensure finalize runs only once
			log.Printf("action: aggregate_ready_to_finalize | client_id: %s | aggregate: %s | batch_index: %d | "+
				"total_batches: %d | has_eof_flag: %t",
				clientID, state.aggregate.Name(), batchMessage.BatchIndex,
				state.expectedTotalBatches, batchMessage.EOF)
		} else {
			missing := state.countMissingBatches(state.expectedTotalBatches - 1)
			log.Printf("action: aggregate_waiting_for_batches | client_id: %s | aggregate: %s | batch_index: %d | "+
				"missing_batches: %d | received: %d | expected: %d",
				clientID, state.aggregate.Name(), batchMessage.BatchIndex,
				missing, state.getUniqueBatchCount(), state.expectedTotalBatches)
		}
		// Note: We silently wait for missing batches to arrive from prefetch buffer
	}

	state.mu.Unlock()

	// Persistence strategy: persist + ACK periodically OR on EOF/finalize
	// - Persist every BATCH_PERSIST_LIMIT batches (e.g., 3000)
	// - On crash: RabbitMQ requeues unACKed messages, we restore from last persist
	const batchPersistLimit = 3000
	shouldPersist := batchesSinceLastPersist >= batchPersistLimit || batchMessage.EOF || shouldFinalize

	if shouldFinalize && !shouldPersist {
		shouldPersist = true
	}

	// Persist state and ACK pending deliveries
	// IMPORTANT: Persistence happens OUTSIDE the lock to avoid blocking other goroutines
	// Use atomic flag to ensure only ONE goroutine persists at a time (prevents race condition)
	// EXCEPT for finalize - we skip persist and use in-memory buffer directly
	if shouldPersist && !shouldFinalize {
		// NORMAL PERSIST: Try to acquire lock, skip if busy
		if state.isPersisting.CompareAndSwap(false, true) {
			defer state.isPersisting.Store(false)

			// Persist with batch indices
			// NOTE: SerializeState now does snapshot+clear atomically, so no separate ClearBuffer needed
			if err := h.persistClientStateWithBatches(clientID, state); err != nil {
				log.Printf("action: aggregate_persist_state | client_id: %s | aggregate: %s | result: fail | pending: %d | error: %v",
					clientID, state.aggregate.Name(), pendingCount, err)
				h.nackPendingDeliveries(state)
				return err
			}

			// ACK all pending deliveries
			h.ackPendingDeliveries(state, clientID)
		} else {
			// Skip - another persist in progress
			// These batches will remain in memory and be included in next persist or finalize
		}
	}

	if shouldFinalize {
		return h.finalizeClient(clientID, state, batchMessage)
	}

	return nil
}

// finalizeClient performs the complete finalization process for a client
func (h *AggregateHandler) finalizeClient(clientID string, state *AggregateClientState, batchMessage *protocol.BatchMessage) error {
	log.Printf("action: aggregate_finalize | client_id: %s | aggregate: %s | result: start | "+
		"total_batches_processed: %d | expected_batches: %d | all_batches_received: true",
		clientID, state.aggregate.Name(), state.getUniqueBatchCount(), state.expectedTotalBatches)

	// APPEND-ONLY FINALIZE: Load ALL incremental snapshots from disk
	// Memory only contains the current buffer - historical data is on disk
	var allIncrements [][]byte
	if h.stateStore != nil {
		var err error
		allIncrements, err = h.stateStore.LoadAllIncrements(clientID)
		if err != nil && !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: aggregate_load_increments | client_id: %s | aggregate: %s | result: fail | error: %v",
				clientID, state.aggregate.Name(), err)
			// Continue with empty increments - we have current buffer in memory
			allIncrements = nil
		} else if len(allIncrements) > 0 {
			log.Printf("action: aggregate_load_increments | client_id: %s | aggregate: %s | "+
				"increments: %d | result: success",
				clientID, state.aggregate.Name(), len(allIncrements))
		}
	}

	// Delegate finalization to the aggregate (merge all increments + current buffer)
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

	h.ackPendingDeliveries(state, clientID)

	h.cleanupClientState(clientID, state)
	return nil
}

// cleanupClientState removes a client's state from memory after finalization
// Calls the aggregate's Cleanup() method to release resources, then removes the state
func (h *AggregateHandler) cleanupClientState(clientID string, state *AggregateClientState) {
	// Let the aggregate clean up its own resources (maps, slices, file descriptors, etc.)
	if err := state.aggregate.Cleanup(); err != nil {
		log.Printf("action: aggregate_cleanup | client_id: %s | result: fail | error: %v", clientID, err)
	}

	h.deleteClientSnapshot(clientID)

	state.mu.Lock()
	state.seenBatchIndices = nil
	state.pendingDeliveries = nil
	state.mu.Unlock()

	// Remove the entire client state from the map
	h.statesMu.Lock()
	delete(h.states, clientID)
	h.statesMu.Unlock()

	// Force garbage collection to release memory back to the OS
	// This is important for aggregates that accumulated large amounts of data
	// runtime.GC()

	// log.Printf("action: aggregate_cleanup_complete | client_id: %s | gc_triggered: true", clientID)
}

// publishBatches publishes all batches, using custom routing keys when provided
func (h *AggregateHandler) publishBatches(publisher *middleware.Publisher, batchesToPublish []aggregates.BatchToPublish) error {
	for i, batchToPublish := range batchesToPublish {
		err := publisher.SendToDatasetOutputExchangesWithRoutingKey(batchToPublish.Batch, batchToPublish.RoutingKey)
		if err != nil {
			return fmt.Errorf("failed to publish batch %d: %w", i+1, err)
		}
	}

	return nil
}

func (h *AggregateHandler) initStateStore(role common.NodeRole) {
	if h.stateStore != nil {
		return
	}

	baseDir := os.Getenv("STATE_BASE_DIR")
	if baseDir == "" {
		baseDir = "/app/state"
	}

	store, err := storage.NewFileStateStore(baseDir, string(role))
	if err != nil {
		log.Printf("action: aggregate_state_store_init | role: %s | result: fail | error: %v", role, err)
		return
	}

	h.stateStore = store
}

func (h *AggregateHandler) persistClientState(clientID string, state *AggregateClientState) error {
	if h.stateStore == nil {
		return nil
	}

	// APPEND-ONLY PERSISTENCE:
	// Instead of rewriting the entire state file, we append incremental snapshots
	// This makes persist operations fast and allows recovery to merge all increments

	// Serialize current in-memory state (only what's accumulated since last persist)
	data, err := state.aggregate.SerializeState()
	if err != nil {
		return fmt.Errorf("serialize aggregate state: %w", err)
	}

	// Persist metadata (always overwrites - small file)
	meta := state.metadataSnapshot()
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encode aggregate metadata: %w", err)
	}

	if err := h.stateStore.Persist(clientID, &storage.Snapshot{
		Metadata: metaBytes,
		Data:     nil, // We don't use the main data file anymore
	}); err != nil {
		return fmt.Errorf("persist metadata: %w", err)
	}

	// Persist data as incremental snapshot (appends new file)
	if len(data) > 0 {
		if err := h.stateStore.PersistIncremental(clientID, data); err != nil {
			return fmt.Errorf("persist incremental data: %w", err)
		}
	}

	return nil
}

// persistClientStateWithBatches persists state AND batch indices (only called on EOF)
func (h *AggregateHandler) persistClientStateWithBatches(clientID string, state *AggregateClientState) error {
	if err := h.persistClientState(clientID, state); err != nil {
		return err
	}

	if h.stateStore != nil {
		batchIndices := state.getBatchIndices()
		if err := h.stateStore.PersistBatchIndices(clientID, batchIndices); err != nil {
			return fmt.Errorf("persist batch indices: %w", err)
		}
	}

	return nil
}

func (h *AggregateHandler) restoreClientState(clientID string, state *AggregateClientState) {
	if h.stateStore == nil {
		return
	}

	snapshot, err := h.stateStore.Load(clientID)
	if err != nil {
		if !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: aggregate_restore_state | client_id: %s | result: fail | error: %v", clientID, err)
		}
		return
	}

	// APPEND-ONLY BUFFER STRATEGY:
	// We only restore metadata and batch indices during startup
	// Data (incremental snapshots) is NEVER loaded into memory until Finalize()
	// Memory is used ONLY as a temporary buffer between persist operations

	// Load metadata (control flow: EOF status, expected batches)
	if len(snapshot.Metadata) > 0 {
		var meta aggregateSnapshotMetadata
		if err := json.Unmarshal(snapshot.Metadata, &meta); err != nil {
			log.Printf("action: aggregate_restore_metadata | client_id: %s | result: fail | error: %v", clientID, err)
		} else {
			state.applyMetadata(meta)
			log.Printf("action: aggregate_restore_metadata | client_id: %s | result: success | "+
				"eof_received: %t | expected_batches: %d",
				clientID, meta.EOFReceived, meta.ExpectedTotalBatches)
		}
	}

	// Load batch indices (needed to detect duplicates and skip already-processed batches)
	batchIndices, err := h.stateStore.LoadBatchIndices(clientID)
	if err != nil {
		if !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: aggregate_restore_batch_indices | client_id: %s | result: fail | error: %v", clientID, err)
		}
	} else if len(batchIndices) > 0 {
		state.applyBatchIndices(batchIndices)
		log.Printf("action: aggregate_restore_batch_indices | client_id: %s | result: success | count: %d",
			clientID, len(batchIndices))
	}

	log.Printf("action: aggregate_restore_complete | client_id: %s | aggregate: %s | "+
		"metadata: loaded | batch_indices: %d | data: deferred_to_finalize",
		clientID, state.aggregate.Name(), len(batchIndices))
}

func (h *AggregateHandler) deleteClientSnapshot(clientID string) {
	if h.stateStore == nil {
		return
	}

	// Delete main metadata file
	if err := h.stateStore.Delete(clientID); err != nil {
		if !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: aggregate_state_delete | client_id: %s | result: fail | error: %v", clientID, err)
		}
	}

	// Delete batch indices file
	if err := h.stateStore.DeleteBatchIndices(clientID); err != nil {
		if !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: aggregate_batch_indices_delete | client_id: %s | result: fail | error: %v", clientID, err)
		}
	}

	// Delete ALL incremental snapshot files (append-only cleanup)
	if err := h.stateStore.DeleteAllIncrements(clientID); err != nil {
		log.Printf("action: aggregate_increments_delete | client_id: %s | result: fail | error: %v", clientID, err)
	} else {
		log.Printf("action: aggregate_increments_delete | client_id: %s | result: success", clientID)
	}
}

// ackPendingDeliveries acknowledges all pending deliveries after successful persist
func (h *AggregateHandler) ackPendingDeliveries(state *AggregateClientState, clientID string) {
	state.mu.Lock()
	defer state.mu.Unlock()

	for _, delivery := range state.pendingDeliveries {
		delivery.Ack(false)
	}

	state.pendingDeliveries = make([]amqp091.Delivery, 0)
	state.batchesSinceLastPersist = 0
}

// nackPendingDeliveries negatively acknowledges all pending deliveries (requeue on error)
func (h *AggregateHandler) nackPendingDeliveries(state *AggregateClientState) {
	state.mu.Lock()
	defer state.mu.Unlock()

	for _, delivery := range state.pendingDeliveries {
		delivery.Nack(false, true) // requeue=true
	}

	state.pendingDeliveries = make([]amqp091.Delivery, 0)
	state.batchesSinceLastPersist = 0
}

// Shutdown persists all active client states during graceful shutdown
// This is called by node.Shutdown() AFTER all in-flight messages have been processed
func (h *AggregateHandler) Shutdown() error {
	log.Printf("action: aggregate_shutdown | result: start | persisting_states: true")

	h.statesMu.RLock()
	clientIDs := make([]string, 0, len(h.states))
	for clientID := range h.states {
		clientIDs = append(clientIDs, clientID)
	}
	h.statesMu.RUnlock()

	for _, clientID := range clientIDs {
		state := h.states[clientID]
		if state != nil {
			// Persist state
			if err := h.persistClientStateWithBatches(clientID, state); err != nil {
				log.Printf("action: aggregate_shutdown_persist | client_id: %s | result: fail | error: %v", clientID, err)
				// On error, NACK pending deliveries to requeue them
				h.nackPendingDeliveries(state)
			} else {
				// On success, ACK all pending deliveries
				h.ackPendingDeliveries(state, clientID)
				log.Printf("action: aggregate_shutdown_persist | client_id: %s | result: success", clientID)
			}
		}
	}

	log.Printf("action: aggregate_shutdown | result: success | states_persisted: %d", len(clientIDs))
	return nil
}

// trackBatchIndex tracks unique batch indices for a specific client state
func (s *AggregateClientState) trackBatchIndex(batchIndex int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seenBatchIndices == nil {
		s.seenBatchIndices = make(map[int]bool)
	}
	if !s.seenBatchIndices[batchIndex] {
		s.seenBatchIndices[batchIndex] = true
		s.uniqueBatchCount.Add(1)
		return true
	}
	return false
}

// getUniqueBatchCount returns the number of unique batches seen for this client
func (s *AggregateClientState) getUniqueBatchCount() int32 {
	return s.uniqueBatchCount.Load()
}

// hasAllBatchesUpTo checks if all batch indices from 0 to maxIndex (inclusive) have been received
// Must be called with s.mu locked
func (s *AggregateClientState) hasAllBatchesUpTo(maxIndex int) bool {
	if s.seenBatchIndices == nil {
		return false
	}

	// Quick check: if we haven't seen enough batches, we can't have all of them
	if len(s.seenBatchIndices) < maxIndex+1 {
		return false
	}

	// Verify all indices from 0 to maxIndex are present
	for i := 0; i <= maxIndex; i++ {
		if !s.seenBatchIndices[i] {
			return false
		}
	}

	return true
}

// countMissingBatches counts how many batches from 0 to maxIndex (inclusive) are missing
// Must be called with s.mu locked
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

func (s *AggregateClientState) metadataSnapshot() aggregateSnapshotMetadata {
	s.mu.Lock()
	defer s.mu.Unlock()

	return aggregateSnapshotMetadata{
		EOFReceived:          s.eofReceived,
		ExpectedTotalBatches: s.expectedTotalBatches,
		OneTimeFinalize:      s.oneTimeFinalize,
	}
}

// getBatchIndices extracts batch indices from the in-memory map (for persistence)
func (s *AggregateClientState) getBatchIndices() []int {
	s.mu.Lock()
	defer s.mu.Unlock()

	indices := make([]int, 0, len(s.seenBatchIndices))
	for idx := range s.seenBatchIndices {
		indices = append(indices, idx)
	}
	sort.Ints(indices)
	return indices
}

func (s *AggregateClientState) applyMetadata(meta aggregateSnapshotMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.eofReceived = meta.EOFReceived
	s.oneTimeFinalize = meta.OneTimeFinalize
	s.expectedTotalBatches = meta.ExpectedTotalBatches
}

// applyBatchIndices restores batch indices into the in-memory map (from persistence)
func (s *AggregateClientState) applyBatchIndices(indices []int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.seenBatchIndices == nil {
		s.seenBatchIndices = make(map[int]bool, len(indices))
	} else {
		for key := range s.seenBatchIndices {
			delete(s.seenBatchIndices, key)
		}
	}

	for _, idx := range indices {
		s.seenBatchIndices[idx] = true
	}

	s.uniqueBatchCount.Store(int32(len(indices)))
}
