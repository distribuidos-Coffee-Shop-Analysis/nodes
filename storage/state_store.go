package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// ErrSnapshotNotFound is returned when there is no persisted snapshot for the given client.
var ErrSnapshotNotFound = errors.New("state snapshot not found")

// Snapshot holds the metadata and data blobs to persist for a client.
// Both slices are expected to contain UTF-8 encoded text provided by the caller.
type Snapshot struct {
	Metadata []byte
	Data     []byte
}

// StateStore abstracts the persistence mechanism used by stateful handlers.
type StateStore interface {
	Persist(clientID string, snapshot *Snapshot) error
	Load(clientID string) (*Snapshot, error)
	Delete(clientID string) error

	// Incremental persistence (append-only)
	PersistIncremental(clientID string, data []byte) error
	LoadAllIncrements(clientID string) ([][]byte, error)
	DeleteAllIncrements(clientID string) error
	LoadBatchIndicesFromIncrements(clientID string) (map[int]bool, error)

	// Buffered batches persistence (for joiners)
	PersistBufferedBatches(clientID string, data []byte) error
	LoadBufferedBatches(clientID string) ([]byte, error)
	DeleteBufferedBatches(clientID string) error
}

// FileStateStore is a filesystem-backed implementation of StateStore.
// It stores three binary files per client inside <baseDir>/<role>:
//   - meta_<client>.bin (metadata)
//   - data_<client>.bin (handler-defined payload)
//   - batches_<client>.bin (seen batch indices, one per line)
//   - data_<client>_0.bin, data_<client>_1.bin, ... (incremental snapshots)
type FileStateStore struct {
	roleDir         string
	locks           sync.Map // map[string]*clientLock
	incrementalSeqs sync.Map // map[clientID]int - tracks next incremental sequence number
}

type clientLock struct {
	mu sync.RWMutex
}

// NewFileStateStore instantiates a FileStateStore rooted at baseDir/role.
func NewFileStateStore(baseDir, role string) (*FileStateStore, error) {
	if baseDir == "" {
		return nil, errors.New("baseDir is required")
	}
	if role == "" {
		return nil, errors.New("role is required")
	}

	roleDir := filepath.Join(baseDir, role)
	if err := os.MkdirAll(roleDir, 0o755); err != nil {
		return nil, fmt.Errorf("create role state dir %s: %w", roleDir, err)
	}

	store := &FileStateStore{
		roleDir: roleDir,
	}

	// Clean up any orphaned temporary files from previous crashes
	if err := store.cleanupOrphanedTempFiles(); err != nil {
		// Log but don't fail - this is a best-effort cleanup
		fmt.Printf("warning: failed to cleanup orphaned temp files in %s: %v\n", roleDir, err)
	}

	return store, nil
}

// Persist writes the provided snapshot atomically for the given client.
func (s *FileStateStore) Persist(clientID string, snapshot *Snapshot) error {
	if snapshot == nil {
		return errors.New("nil snapshot")
	}
	lock := s.getLock(clientID)
	lock.mu.Lock()
	defer lock.mu.Unlock()

	if err := writeAtomically(s.dataPath(clientID), snapshot.Data); err != nil {
		return err
	}
	if err := writeAtomically(s.metaPath(clientID), snapshot.Metadata); err != nil {
		return err
	}

	return nil
}

// Load retrieves the snapshot for the given client.
func (s *FileStateStore) Load(clientID string) (*Snapshot, error) {
	lock := s.getLock(clientID)
	lock.mu.RLock()
	defer lock.mu.RUnlock()

	metaBytes, err := os.ReadFile(s.metaPath(clientID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrSnapshotNotFound
		}
		return nil, err
	}

	dataBytes, err := os.ReadFile(s.dataPath(clientID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrSnapshotNotFound
		}
		return nil, err
	}

	return &Snapshot{
		Metadata: metaBytes,
		Data:     dataBytes,
	}, nil
}

// Delete removes the snapshot files for the given client.
func (s *FileStateStore) Delete(clientID string) error {
	lock := s.getLock(clientID)
	lock.mu.Lock()
	defer lock.mu.Unlock()

	if err := os.Remove(s.metaPath(clientID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(s.dataPath(clientID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return nil
}

func (s *FileStateStore) getLock(clientID string) *clientLock {
	lock, ok := s.locks.Load(clientID)
	if ok {
		return lock.(*clientLock)
	}
	newLock := &clientLock{}
	actual, _ := s.locks.LoadOrStore(clientID, newLock)
	return actual.(*clientLock)
}

func (s *FileStateStore) metaPath(clientID string) string {
	filename := fmt.Sprintf("meta_%s.bin", clientID)
	return filepath.Join(s.roleDir, filename)
}

func (s *FileStateStore) dataPath(clientID string) string {
	filename := fmt.Sprintf("data_%s.bin", clientID)
	return filepath.Join(s.roleDir, filename)
}

// cleanupOrphanedTempFiles removes leftover temporary files from previous crashes
func (s *FileStateStore) cleanupOrphanedTempFiles() error {
	entries, err := os.ReadDir(s.roleDir)
	if err != nil {
		return err
	}

	cleaned := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		// Remove files matching the temp file pattern: "state-*"
		name := entry.Name()
		if len(name) > 6 && name[:6] == "state-" {
			fullPath := filepath.Join(s.roleDir, name)
			if err := os.Remove(fullPath); err != nil {
				fmt.Printf("warning: failed to remove orphaned temp file %s: %v\n", fullPath, err)
			} else {
				cleaned++
			}
		}
	}

	if cleaned > 0 {
		fmt.Printf("info: cleaned up %d orphaned temp files in %s\n", cleaned, s.roleDir)
	}

	return nil
}

func writeAtomically(targetPath string, data []byte) error {
	dir := filepath.Dir(targetPath)
	tmpFile, err := os.CreateTemp(dir, "state-*")
	if err != nil {
		return fmt.Errorf("create temp file for %s: %w", targetPath, err)
	}

	tmpName := tmpFile.Name()

	// Ensure cleanup in ALL error paths using defer
	success := false
	defer func() {
		if !success {
			tmpFile.Close()
			os.Remove(tmpName)
		}
	}()

	if _, err := tmpFile.Write(data); err != nil {
		return fmt.Errorf("write temp file %s: %w", tmpName, err)
	}

	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("sync temp file %s: %w", tmpName, err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close temp file %s: %w", tmpName, err)
	}

	if err := os.Rename(tmpName, targetPath); err != nil {
		return fmt.Errorf("rename temp file %s to %s: %w", tmpName, targetPath, err)
	}

	success = true
	return nil
}

// PersistIncremental appends data as a new incremental file
// Creates files like: data_<clientID>_0.bin, data_<clientID>_1.bin, etc.
func (s *FileStateStore) PersistIncremental(clientID string, data []byte) error {
	if len(data) == 0 {
		return nil // Nothing to persist
	}

	lock := s.getLock(clientID)
	lock.mu.Lock()
	defer lock.mu.Unlock()

	// Get next sequence number
	seq := s.getNextIncrementalSeq(clientID)

	// Write incremental file
	incrementalPath := s.incrementalPath(clientID, seq)
	if err := writeAtomically(incrementalPath, data); err != nil {
		return fmt.Errorf("persist incremental %d: %w", seq, err)
	}

	return nil
}

// LoadAllIncrements loads all incremental files for a client
// Returns them in order: [data_0, data_1, data_2, ...]
func (s *FileStateStore) LoadAllIncrements(clientID string) ([][]byte, error) {
	lock := s.getLock(clientID)
	lock.mu.RLock()
	defer lock.mu.RUnlock()

	var allData [][]byte
	seq := 0

	// Read incremental files in order until we find a gap
	for {
		incrementalPath := s.incrementalPath(clientID, seq)
		data, err := os.ReadFile(incrementalPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// No more incremental files
				break
			}
			return nil, fmt.Errorf("read incremental %d: %w", seq, err)
		}

		allData = append(allData, data)
		seq++
	}

	if len(allData) == 0 {
		return nil, ErrSnapshotNotFound
	}

	return allData, nil
}

// DeleteAllIncrements removes all incremental files for a client
func (s *FileStateStore) DeleteAllIncrements(clientID string) error {
	lock := s.getLock(clientID)
	lock.mu.Lock()
	defer lock.mu.Unlock()

	seq := 0
	deletedCount := 0

	// Delete incremental files until we find a gap
	for {
		incrementalPath := s.incrementalPath(clientID, seq)
		err := os.Remove(incrementalPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// No more incremental files
				break
			}
			return fmt.Errorf("delete incremental %d: %w", seq, err)
		}
		deletedCount++
		seq++
	}

	// Reset sequence counter
	s.incrementalSeqs.Delete(clientID)

	return nil
}

// incrementalPath returns the path for an incremental snapshot file
func (s *FileStateStore) incrementalPath(clientID string, seq int) string {
	filename := fmt.Sprintf("data_%s_%d.bin", clientID, seq)
	return filepath.Join(s.roleDir, filename)
}

// getNextIncrementalSeq gets and increments the sequence number for a client
// On first call for a client, scans existing files to find the next available sequence
func (s *FileStateStore) getNextIncrementalSeq(clientID string) int {
	// Try to load existing sequence
	if seqVal, ok := s.incrementalSeqs.Load(clientID); ok {
		seq := seqVal.(int)
		s.incrementalSeqs.Store(clientID, seq+1)
		return seq
	}

	// First time for this client - find the next available sequence by scanning existing files
	nextSeq := s.findNextAvailableSeq(clientID)
	s.incrementalSeqs.Store(clientID, nextSeq+1)

	return nextSeq
}

// findNextAvailableSeq scans existing incremental files to find the next available sequence number
func (s *FileStateStore) findNextAvailableSeq(clientID string) int {
	seq := 0
	for {
		incrementalPath := s.incrementalPath(clientID, seq)
		if _, err := os.Stat(incrementalPath); os.IsNotExist(err) {
			// Found a gap - this is the next available sequence
			return seq
		}
		seq++
	}
}

// PersistBufferedBatches persists buffered aggregated batches (for joiners)
// This file is separate from reference data increments and is overwritten on each persist
func (s *FileStateStore) PersistBufferedBatches(clientID string, data []byte) error {
	if len(data) == 0 {
		return nil // Nothing to persist
	}

	lock := s.getLock(clientID)
	lock.mu.Lock()
	defer lock.mu.Unlock()

	finalPath := s.bufferedBatchesPath(clientID)
	tempPath := finalPath + ".tmp"

	// Write to temp file
	if err := os.WriteFile(tempPath, data, 0o644); err != nil {
		return fmt.Errorf("write buffered batches temp file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, finalPath); err != nil {
		os.Remove(tempPath) // Cleanup temp file on error
		return fmt.Errorf("rename buffered batches file: %w", err)
	}

	return nil
}

// LoadBufferedBatches loads buffered aggregated batches for a client
func (s *FileStateStore) LoadBufferedBatches(clientID string) ([]byte, error) {
	lock := s.getLock(clientID)
	lock.mu.RLock()
	defer lock.mu.RUnlock()

	filePath := s.bufferedBatchesPath(clientID)
	data, err := os.ReadFile(filePath)
	if os.IsNotExist(err) {
		return nil, ErrSnapshotNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read buffered batches file: %w", err)
	}

	return data, nil
}

// DeleteBufferedBatches deletes the buffered batches file for a client
func (s *FileStateStore) DeleteBufferedBatches(clientID string) error {
	lock := s.getLock(clientID)
	lock.mu.Lock()
	defer lock.mu.Unlock()

	filePath := s.bufferedBatchesPath(clientID)
	err := os.Remove(filePath)
	if os.IsNotExist(err) {
		return nil // Already deleted
	}
	return err
}

// bufferedBatchesPath returns the path for buffered batches file
func (s *FileStateStore) bufferedBatchesPath(clientID string) string {
	filename := fmt.Sprintf("data_buffered_%s.bin", clientID)
	return filepath.Join(s.roleDir, filename)
}

// LoadBatchIndicesFromIncrements reads batch indices from incremental file headers
// Each incremental file has a header "BATCH|index\n"
func (s *FileStateStore) LoadBatchIndicesFromIncrements(clientID string) (map[int]bool, error) {
	lock := s.getLock(clientID)
	lock.mu.RLock()
	defer lock.mu.RUnlock()

	batchIndices := make(map[int]bool)
	seq := 0

	for {
		incrementalPath := s.incrementalPath(clientID, seq)
		batchIndex, err := s.readBatchIndexFromFile(incrementalPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				break
			}
			seq++
			continue
		}

		batchIndices[batchIndex] = true
		seq++
	}

	return batchIndices, nil
}

// readBatchIndexFromFile reads only the first line (header) of an incremental file
func (s *FileStateStore) readBatchIndexFromFile(filePath string) (int, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return -1, err
	}
	defer f.Close()

	buf := make([]byte, 30)
	n, err := f.Read(buf)
	if err != nil && n == 0 {
		return -1, err
	}

	header := string(buf[:n])
	newlineIdx := strings.Index(header, "\n")
	if newlineIdx == -1 {
		return -1, fmt.Errorf("no newline in header")
	}

	header = header[:newlineIdx]
	if !strings.HasPrefix(header, "BATCH|") {
		return -1, fmt.Errorf("invalid header format")
	}

	batchIndexStr := header[6:] // Skip "BATCH|"
	batchIndex, err := strconv.Atoi(batchIndexStr)
	if err != nil {
		return -1, fmt.Errorf("invalid batch index: %w", err)
	}

	return batchIndex, nil
}

// ListClients returns all client IDs that have metadata files
func (s *FileStateStore) ListClients() []string {
	entries, err := os.ReadDir(s.roleDir)
	if err != nil {
		return nil
	}

	clientSet := make(map[string]bool)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if strings.HasPrefix(name, "meta_") && strings.HasSuffix(name, ".bin") {
			clientID := name[5 : len(name)-4]
			if clientID != "" {
				clientSet[clientID] = true
			}
		}
	}

	clients := make([]string, 0, len(clientSet))
	for clientID := range clientSet {
		clients = append(clients, clientID)
	}

	return clients
}
