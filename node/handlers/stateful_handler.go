package handlers

import (
	"log"
	"os"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/storage"
)

// StatefulHandlerBase provides common functionality for stateful handlers: Aggregate, Joiner
type StatefulHandlerBase struct {
	stateStore storage.StateStore
}

// InitStateStore initializes the state store
func (h *StatefulHandlerBase) InitStateStore(role common.NodeRole) {
	if h.stateStore != nil {
		return
	}

	baseDir := os.Getenv("STATE_BASE_DIR")
	if baseDir == "" {
		baseDir = "/app/state"
	}

	store, err := storage.NewFileStateStore(baseDir, string(role))
	if err != nil {
		log.Printf("action: state_store_init | role: %s | result: fail | error: %v", role, err)
		return
	}

	h.stateStore = store
}

// StateStore returns the state store
func (h *StatefulHandlerBase) StateStore() storage.StateStore {
	return h.stateStore
}

// ScanExistingClients returns client IDs that have persisted metadata
func (h *StatefulHandlerBase) ScanExistingClients() []string {
	if h.stateStore == nil {
		return nil
	}

	store, ok := h.stateStore.(*storage.FileStateStore)
	if !ok {
		return nil
	}

	return store.ListClients()
}

// LoadBatchIndices loads batch indices from file headers
func (h *StatefulHandlerBase) LoadBatchIndices(clientID string) (map[int]bool, error) {
	if h.stateStore == nil {
		return make(map[int]bool), nil
	}

	return h.stateStore.LoadBatchIndicesFromIncrements(clientID)
}

func (h *StatefulHandlerBase) DeleteClientState(clientID string) {
	if h.stateStore == nil {
		return
	}

	if err := h.stateStore.Delete(clientID); err != nil {
		if !isNotFound(err) {
			log.Printf("action: state_delete | client_id: %s | result: fail | error: %v", clientID, err)
		}
	}

	if err := h.stateStore.DeleteAllIncrements(clientID); err != nil {
		log.Printf("action: increments_delete | client_id: %s | result: fail | error: %v", clientID, err)
	}

	if err := h.stateStore.DeleteBufferedBatches(clientID); err != nil {
		if !isNotFound(err) {
			log.Printf("action: buffered_delete | client_id: %s | result: fail | error: %v", clientID, err)
		}
	}
}

// isNotFound checks if error is a "not found" error
func isNotFound(err error) bool {
	return err == storage.ErrSnapshotNotFound
}
