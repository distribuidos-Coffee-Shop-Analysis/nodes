package common

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type StatePersistence struct {
	stateDir string
	mu       sync.Mutex
}

func NewStatePersistence(stateDir string) (*StatePersistence, error) {
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create state directory: %w", err)
	}

	return &StatePersistence{
		stateDir: stateDir,
	}, nil
}

func (sp *StatePersistence) buildFilePath(aggregateName, clientID string) string {
	filename := fmt.Sprintf("%s_%s.gob", aggregateName, clientID)
	return filepath.Join(sp.stateDir, filename)
}

func (sp *StatePersistence) SaveState(aggregateName, clientID string, state interface{}) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	filePath := sp.buildFilePath(aggregateName, clientID)

	tmpPath := filePath + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(state); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to encode state: %w", err)
	}

	if err := file.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to close file: %w", err)
	}

	if err := os.Rename(tmpPath, filePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename file: %w", err)
	}

	log.Printf("action: save_state | aggregate: %s | client_id: %s | file: %s | result: success",
		aggregateName, clientID, filePath)

	return nil
}

func (sp *StatePersistence) LoadState(aggregateName, clientID string, state interface{}) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	filePath := sp.buildFilePath(aggregateName, clientID)

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		log.Printf("action: load_state | aggregate: %s | client_id: %s | result: no_previous_state",
			aggregateName, clientID)
		return nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open state file: %w", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(state); err != nil {
		return fmt.Errorf("failed to decode state: %w", err)
	}

	log.Printf("action: load_state | aggregate: %s | client_id: %s | file: %s | result: success",
		aggregateName, clientID, filePath)

	return nil
}

func (sp *StatePersistence) DeleteState(aggregateName, clientID string) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	filePath := sp.buildFilePath(aggregateName, clientID)

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		log.Printf("action: delete_state | aggregate: %s | client_id: %s | result: file_not_found",
			aggregateName, clientID)
		return nil
	}

	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("failed to delete state file: %w", err)
	}

	log.Printf("action: delete_state | aggregate: %s | client_id: %s | file: %s | result: success",
		aggregateName, clientID, filePath)

	return nil
}

func (sp *StatePersistence) StateExists(aggregateName, clientID string) bool {
	filePath := sp.buildFilePath(aggregateName, clientID)
	_, err := os.Stat(filePath)
	return err == nil
}

func (sp *StatePersistence) ListClientStates(aggregateName string) ([]string, error) {
	pattern := filepath.Join(sp.stateDir, fmt.Sprintf("%s_*.gob", aggregateName))
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list state files: %w", err)
	}

	clientIDs := make([]string, 0, len(files))
	prefix := aggregateName + "_"
	suffix := ".gob"

	for _, file := range files {
		filename := filepath.Base(file)
		if len(filename) > len(prefix)+len(suffix) {
			clientID := filename[len(prefix) : len(filename)-len(suffix)]
			clientIDs = append(clientIDs, clientID)
		}
	}

	return clientIDs, nil
}
