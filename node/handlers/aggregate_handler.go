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
	mu                      sync.Mutex
	aggregate               aggregates.Aggregate
	eofReceived             bool
	oneTimeFinalize         bool
	expectedTotalBatches    int
	seenBatchIndices        map[int]bool
	uniqueBatchCount        atomic.Int32
	batchesSinceLastPersist int // Counter to reduce persistence frequency
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
	return &AggregateHandler{
		newAggregate: newAggregate,
		states:       make(map[string]*AggregateClientState),
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

	state.mu.Lock()
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
		}
		// Note: We silently wait for missing batches to arrive from prefetch buffer
	}

	// Check if we should finalize
	if shouldFinalize {
		state.oneTimeFinalize = false // Ensure finalize runs only once
	}

	// Increment batch counter for persistence frequency control
	state.batchesSinceLastPersist++

	// Determine if we should persist now:
	// 1. Always persist on EOF (to ensure recoverability before finalization)
	// 2. Every 2000 batches (to reduce I/O overhead, especially for Q4 with huge state)
	// 3. Always persist before finalization
	shouldPersist := batchMessage.EOF || state.batchesSinceLastPersist >= 2000 || shouldFinalize

	state.mu.Unlock()

	// Persist state periodically, not on every batch (reduces memory pressure and I/O)
	if shouldPersist {
		if err := h.persistClientState(clientID, state); err != nil {
			log.Printf("action: aggregate_persist_state | client_id: %s | aggregate: %s | result: fail | error: %v",
				clientID, state.aggregate.Name(), err)
			msg.Nack(false, true)
			return err
		}

		state.mu.Lock()
		state.batchesSinceLastPersist = 0
		state.mu.Unlock()
	}

	if shouldFinalize {
		log.Printf("action: aggregate_finalize | client_id: %s | aggregate: %s | result: start | "+
			"total_batches_processed: %d | expected_batches: %d | all_batches_received: true",
			clientID, state.aggregate.Name(), state.getUniqueBatchCount(), state.expectedTotalBatches)

		batchesToPublish, err := state.aggregate.GetBatchesToPublish(batchMessage.BatchIndex, clientID)
		if err != nil {
			log.Printf("action: get_batches_to_publish | client_id: %s | aggregate: %s | result: fail | error: %v",
				clientID, state.aggregate.Name(), err)
			msg.Nack(false, true)
			return err
		}

		h.pubMu.Lock()
		err = h.publishBatches(h.pub, batchesToPublish)
		h.pubMu.Unlock()

		if err != nil {
			log.Printf("action: aggregate_publish | client_id: %s | aggregate: %s | result: fail | error: %v",
				clientID, state.aggregate.Name(), err)
			msg.Nack(false, true)
			return err
		}

		h.cleanupClientState(clientID, state)
	}

	// Acknowledge the message
	msg.Ack(false)

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

	// Clear metadata
	state.mu.Lock()
	state.seenBatchIndices = nil
	state.mu.Unlock()

	// Remove the entire client state from the map
	h.statesMu.Lock()
	delete(h.states, clientID)
	h.statesMu.Unlock()
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

	data, err := state.aggregate.SerializeState()
	if err != nil {
		return fmt.Errorf("serialize aggregate state: %w", err)
	}

	meta := state.metadataSnapshot()
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encode aggregate metadata: %w", err)
	}

	// Persist main snapshot (metadata + data)
	if err := h.stateStore.Persist(clientID, &storage.Snapshot{
		Metadata: metaBytes,
		Data:     data,
	}); err != nil {
		return err
	}

	batchIndices := state.getBatchIndices()
	if err := h.stateStore.PersistBatchIndices(clientID, batchIndices); err != nil {
		return fmt.Errorf("persist batch indices: %w", err)
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

	if len(snapshot.Data) > 0 {
		if err := state.aggregate.RestoreState(snapshot.Data); err != nil {
			log.Printf("action: aggregate_restore_state | client_id: %s | aggregate: %s | result: fail | error: %v",
				clientID, state.aggregate.Name(), err)
			return
		}
	}

	if len(snapshot.Metadata) > 0 {
		var meta aggregateSnapshotMetadata
		if err := json.Unmarshal(snapshot.Metadata, &meta); err != nil {
			log.Printf("action: aggregate_restore_metadata | client_id: %s | result: fail | error: %v", clientID, err)
		} else {
			state.applyMetadata(meta)
		}
	}

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
}

func (h *AggregateHandler) deleteClientSnapshot(clientID string) {
	if h.stateStore == nil {
		return
	}

	if err := h.stateStore.Delete(clientID); err != nil {
		if !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: aggregate_state_delete | client_id: %s | result: fail | error: %v", clientID, err)
		}
	}

	if err := h.stateStore.DeleteBatchIndices(clientID); err != nil {
		if !errors.Is(err, storage.ErrSnapshotNotFound) {
			log.Printf("action: aggregate_batch_indices_delete | client_id: %s | result: fail | error: %v", clientID, err)
		}
	}
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
