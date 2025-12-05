package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/aggregates"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/storage"

	"github.com/rabbitmq/amqp091-go"
)

// AggregateClientState holds per-client state for aggregate operations
type AggregateClientState struct {
	mu sync.Mutex // lock for all state operations

	aggregate            aggregates.Aggregate
	eofReceived          bool
	expectedTotalBatches int
	seenBatchIndices     map[int]bool // Batch indices that have been persisted
	finalizeStarted      bool         // Set when finalize begins
	finalizeCompleted    bool         // Set after successful publish
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

	h.statesMu.Lock()
	defer h.statesMu.Unlock()

	if state, ok := h.states[clientID]; ok {
		return state
	}

	st := &AggregateClientState{
		aggregate:            h.newAggregate(),
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
		seenBatchIndicesEqualsExpectedTotalBatches := state.eofReceived &&
			state.expectedTotalBatches > 0 &&
			len(state.seenBatchIndices) == state.expectedTotalBatches

		needsFinalize := !state.finalizeCompleted && (state.finalizeStarted || seenBatchIndicesEqualsExpectedTotalBatches)
		batchIndex := state.expectedTotalBatches - 1
		state.mu.Unlock()

		if needsFinalize {
			log.Printf("action: aggregate_resume_finalize | client_id: %s | result: start | "+
				"expectedTotalBatches: %d | batches_in_memory: %d",
				clientID, state.expectedTotalBatches, len(state.seenBatchIndices))

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

// Handle processes a batch message
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

	// 3. Persist data
	if err := h.StateStore().PersistIncremental(clientID, batchMessage.BatchIndex, data); err != nil {
		state.mu.Unlock()
		log.Printf("action: aggregate_persist_data | client_id: %s | aggregate: %s | result: fail | error: %v",
			clientID, state.aggregate.Name(), err)
		msg.Nack(false, true)
		return err
	}

	if len(data) == 0 {
		log.Printf("action: aggregate_empty_batch_persisted | client_id: %s | aggregate: %s | batch_index: %d | records: %d",
			clientID, state.aggregate.Name(), batchMessage.BatchIndex, len(batchMessage.Records))
	}

	state.aggregate.CacheIncrement(batchMessage.BatchIndex, data)

	// 4. Mark as seen in memory
	state.seenBatchIndices[batchMessage.BatchIndex] = true

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
			expectedTotalBatches, len(state.seenBatchIndices))

		// Persist metadata with EOF info
		h.persistMetadata(clientID, state)
	}

	// 6. Check if ready to finalize
	shouldFinalize := false
	if state.eofReceived && !state.finalizeStarted && state.expectedTotalBatches > 0 {
		if state.hasAllBatchesUpTo(state.expectedTotalBatches - 1) {
			shouldFinalize = true
			log.Printf("action: aggregate_ready_to_finalize | client_id: %s | aggregate: %s | "+
				"total_batches: %d",
				clientID, state.aggregate.Name(), state.expectedTotalBatches)
		} else {
			missing := state.countMissingBatches(state.expectedTotalBatches - 1)
			log.Printf("action: aggregate_waiting_for_batches | client_id: %s | aggregate: %s | "+
				"missing_batches: %d | received: %d | expected: %d",
				clientID, state.aggregate.Name(),
				missing, len(state.seenBatchIndices), state.expectedTotalBatches)
		}
	}

	state.mu.Unlock()

	msg.Ack(false)

	if shouldFinalize {
		return h.finalizeClient(clientID, state, batchMessage)
	}

	return nil
}

// finalizeClient performs the finalization process for a client
// Hay veces en las cuales se llega al finalizeClient (osea que estamos seguro que recibimos todas las batches y que estas estan persistidas en disco) pero que cuando se checkean las batches persistidas en disco faltan algunas. Esto es porque el sistema operativo no es instantaneo y hay un delay entre el momento en que se persiste la batch y el que se lee para ver si esta se encuentra. Es por esto que hay veces en las cuales LoadAllIncrements(clientID) devuelve menos batches de los que se esperan. Esto es una limitacion que el sistema operativo nos impone porque sabemos que en realidad esas batches existen y estan persistidas. Es por esto que implementamos aca un sistema de retries. Pusimos un numero muy alto: `maxRetries = 15`, aunque nunca vimos que llegue a ser necesario mas de 3 retries, pero es simplemente para hacer al sistema resiliente y asegurarnos de que siempre vamos a dar los resultados correctos.
func (h *AggregateHandler) finalizeClient(clientID string, state *AggregateClientState, batchMessage *protocol.BatchMessage) error {
	state.mu.Lock()
	totalBatchesProcessed := len(state.seenBatchIndices)
	state.finalizeStarted = true
	state.mu.Unlock()

	log.Printf("action: aggregate_finalize_started | client_id: %s | aggregate: %s | result: start | "+
		"total_batches_processed: %d | expected_batches: %d",
		clientID, state.aggregate.Name(), totalBatchesProcessed, state.expectedTotalBatches)

	h.persistMetadata(clientID, state)

	// merge disk and cache
	var allIncrements [][]byte
	if h.StateStore() != nil {
		expectedTotal := state.expectedTotalBatches
		cachedBatches := state.aggregate.GetCache()

		const maxRetries = 15
		const retryBackoff = 3 * time.Second
		var missingIndices []int

		for attempt := 0; attempt < maxRetries; attempt++ {
			// load all batches from disk
			diskBatches, err := h.StateStore().LoadAllIncrements(clientID)
			if err != nil && !errors.Is(err, storage.ErrSnapshotNotFound) {
				log.Printf("action: aggregate_load_increments | client_id: %s | aggregate: %s | attempt: %d | result: fail | error: %v",
					clientID, state.aggregate.Name(), attempt+1, err)
				diskBatches = make(map[int][]byte)
			}

			if attempt == 0 {
				log.Printf("action: aggregate_reconciliation_start | client_id: %s | aggregate: %s | "+
					"expected_total: %d | disk_batches: %d | cached_batches: %d",
					clientID, state.aggregate.Name(), expectedTotal, len(diskBatches), len(cachedBatches))
			}

			allIncrements = make([][]byte, 0, expectedTotal)
			missingIndices = nil
			fromDisk := 0
			fromCache := 0
			emptyBatches := 0

			for i := 0; i < expectedTotal; i++ {
				// disk first
				if data, existsInDisk := diskBatches[i]; existsInDisk {
					allIncrements = append(allIncrements, data)
					fromDisk++
					if len(data) == 0 {
						emptyBatches++
					}
				} else if data, existsInCache := cachedBatches[i]; existsInCache {
					allIncrements = append(allIncrements, data)
					fromCache++
					if len(data) == 0 {
						emptyBatches++
					}
				} else {
					// batch missing from both disk and cache
					missingIndices = append(missingIndices, i)
					allIncrements = append(allIncrements, nil) // Placeholder to maintain ordering
				}
			}

			// check if reconciliation succeeded
			if len(missingIndices) == 0 {
				log.Printf("action: aggregate_reconciliation_success | client_id: %s | aggregate: %s | "+
					"attempt: %d | total_batches: %d | from_disk: %d | from_cache: %d | empty_batches: %d",
					clientID, state.aggregate.Name(), attempt+1, len(allIncrements), fromDisk, fromCache, emptyBatches)
				break
			}

			// reconciliation failed - retry if attempts remain
			if attempt < maxRetries-1 {
				log.Printf("action: aggregate_reconciliation_retry | client_id: %s | aggregate: %s | "+
					"attempt: %d | missing_batches: %d | missing_indices: %v | "+
					"reason: os_delay_listing_files | retry_in: %v",
					clientID, state.aggregate.Name(), attempt+1, len(missingIndices), missingIndices, retryBackoff)
				time.Sleep(retryBackoff)
			}
		}

		if len(missingIndices) > 0 {
			log.Printf("action: aggregate_reconciliation_error | client_id: %s | aggregate: %s | "+
				"result: fail | attempts: %d | missing_batches: %d | missing_indices: %v | "+
				"reason: batches_lost_from_both_disk_and_cache",
				clientID, state.aggregate.Name(), maxRetries, len(missingIndices), missingIndices)
			return fmt.Errorf("missing %d batches (indices: %v) after %d attempts - lost from both disk and cache",
				len(missingIndices), missingIndices, maxRetries)
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
	if state.aggregate != nil {
		if err := state.aggregate.Cleanup(); err != nil {
			log.Printf("action: aggregate_cleanup | client_id: %s | result: fail | error: %v", clientID, err)
		}
	}

	h.deleteClientSnapshot(clientID)

	state.mu.Lock()
	state.seenBatchIndices = nil
	state.aggregate = nil
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
			state.finalizeStarted = meta.FinalizeStarted
			state.finalizeCompleted = meta.FinalizeCompleted
			log.Printf("action: aggregate_restore_metadata | client_id: %s | result: success | "+
				"eof_received: %t | expected_batches: %d | finalize_started: %t | finalize_completed: %t",
				clientID, meta.EOFReceived, meta.ExpectedTotalBatches, meta.FinalizeStarted, meta.FinalizeCompleted)
		}
	}

	// Restore batch indices from file headers
	batchIndices, err := h.StateStore().LoadBatchIndicesFromIncrements(clientID)
	if err != nil {
		log.Printf("action: aggregate_restore_batch_indices | client_id: %s | result: fail | error: %v", clientID, err)
	} else if len(batchIndices) > 0 {
		state.seenBatchIndices = batchIndices
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
			totalSeen := len(state.seenBatchIndices)
			h.persistMetadata(clientID, state)
			state.mu.Unlock()

			log.Printf("action: aggregate_shutdown_persist | client_id: %s | result: success | "+
				"total_seen: %d", clientID, totalSeen)
		}
	}

	log.Printf("action: aggregate_shutdown | result: success | states_persisted: %d", len(clientIDs))
	return nil
}

func (h *AggregateHandler) OnCleanup(clientID string) {
	h.statesMu.Lock()
	state, exists := h.states[clientID]
	if !exists {
		h.statesMu.Unlock()
		h.DeleteClientState(clientID)
		log.Printf("action: aggregate_cleanup | client_id: %s | result: disk_only | reason: client_not_in_memory", clientID)
		return
	}
	
	delete(h.states, clientID)
	h.statesMu.Unlock()

	state.mu.Lock()
	batchesProcessed := len(state.seenBatchIndices)
	eofReceived := state.eofReceived
	finalizeCompleted := state.finalizeCompleted
	aggregateName := ""
	if state.aggregate != nil {
		aggregateName = state.aggregate.Name()
	}
	state.mu.Unlock()

	log.Printf("action: aggregate_cleanup | client_id: %s | aggregate: %s | "+
		"batches_processed: %d | eof_received: %t | finalize_completed: %t",
		clientID, aggregateName, batchesProcessed, eofReceived, finalizeCompleted)

	h.cleanupClientState(clientID, state)

	log.Printf("action: aggregate_cleanup | client_id: %s | result: success", clientID)
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
