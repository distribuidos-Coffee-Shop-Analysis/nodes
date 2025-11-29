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

const MaxConcurrentFileReads = 20

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

	// Incremental persistence - files named by batchIndex for uniqueness
	// File format: data_<clientID>_<batchIndex>.bin
	PersistIncremental(clientID string, batchIndex int, data []byte) error
	LoadAllIncrementsExcluding(clientID string, excludeBatchIndices map[int]bool) ([][]byte, error)
	DeleteAllIncrements(clientID string) error
	LoadBatchIndicesFromIncrements(clientID string) (map[int]bool, error)

	// Buffered batches persistence (for joiners)
	PersistBufferedBatches(clientID string, data []byte) error
	LoadBufferedBatches(clientID string) ([]byte, error)
	DeleteBufferedBatches(clientID string) error
}

// FileStateStore is a filesystem-backed implementation of StateStore.
// It stores files per client inside <baseDir>/<role>:
//   - meta_<client>.bin (metadata)
//   - data_<client>.bin (handler-defined payload)
//   - data_<client>_<batchIndex>.bin (incremental snapshots, named by batchIndex)
type FileStateStore struct {
	roleDir string
	locks   sync.Map // map[string]*clientLock
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

// PersistIncremental saves data to a file named by batchIndex
// File: data_<clientID>_<batchIndex>.bin - batchIndex ensures uniqueness
// Even if data is empty, we persist an empty file to mark the batch as processed
func (s *FileStateStore) PersistIncremental(clientID string, batchIndex int, data []byte) error {
	lock := s.getLock(clientID)
	lock.mu.Lock()
	defer lock.mu.Unlock()

	filePath := s.getPathForClientAndBatchIndex(clientID, batchIndex)
	if err := writeAtomically(filePath, data); err != nil {
		return fmt.Errorf("persist incremental batch %d: %w", batchIndex, err)
	}

	if len(data) == 0 {
		fmt.Printf("info: persisted empty batch marker | client_id: %s | batch_index: %d\n",
			clientID, batchIndex)
	}

	return nil
}

// fileReadResult holds the result of reading a single file
type fileReadResult struct {
	data []byte
	err  error
}

// LoadAllIncrementsExcluding loads incremental files for a client, skipping files
// whose batchIndex is in excludeBatchIndices. This avoids reading files already cached in memory.
func (s *FileStateStore) LoadAllIncrementsExcluding(clientID string, excludeBatchIndices map[int]bool) ([][]byte, error) {
	lock := s.getLock(clientID)
	lock.mu.RLock()
	defer lock.mu.RUnlock()

	prefix := fmt.Sprintf("data_%s_", clientID)
	suffix := ".bin"

	entries, err := os.ReadDir(s.roleDir)
	if err != nil {
		return nil, ErrSnapshotNotFound
	}

	// Collect file paths to read, excluding cached batch indices
	var filePaths []string
	skippedCount := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}

		// Extract batchIndex from filename: data_clientID_BATCHINDEX.bin
		batchIndexStr := name[len(prefix) : len(name)-len(suffix)]
		batchIndex, err := strconv.Atoi(batchIndexStr)
		if err != nil {
			// Can't parse batch index, include it anyway
			fmt.Printf("warning: can't parse batch index from file %s: %v\n", name, err)
			filePaths = append(filePaths, filepath.Join(s.roleDir, name))
			continue
		}

		// Skip if this batch is already cached in memory
		if excludeBatchIndices != nil && excludeBatchIndices[batchIndex] {
			skippedCount++
			continue
		}

		filePaths = append(filePaths, filepath.Join(s.roleDir, name))
	}

	if skippedCount > 0 {
		fmt.Printf("info: skipped %d cached files for client %s (reading %d from disk)\n",
			skippedCount, clientID, len(filePaths))
	}

	if len(filePaths) == 0 {
		if skippedCount > 0 {
			// All files were cached, return empty (not an error)
			return nil, nil
		}
		return nil, ErrSnapshotNotFound
	}

	result := s.readFilesParallel(filePaths)

	if result.readErrors > 0 {
		fmt.Printf("warning: client %s had file read errors: read_errors=%d successfully_read=%d total_attempted=%d\n",
			clientID, result.readErrors, len(result.data), result.totalFiles)
	}

	if result.emptyFiles > 0 {
		fmt.Printf("info: client %s loaded %d empty batch markers (processed batches with no data)\n",
			clientID, result.emptyFiles)
	}

	return result.data, nil
}

// readFilesParallelResult holds the data and statistics from parallel file reads
type readFilesParallelResult struct {
	data       [][]byte
	readErrors int
	emptyFiles int
	totalFiles int
}

// readFilesParallel reads multiple files with limited concurrency using a semaphore pattern.
// Returns the data along with statistics about failed reads.
func (s *FileStateStore) readFilesParallel(filePaths []string) readFilesParallelResult {
	numFiles := len(filePaths)
	if numFiles == 0 {
		return readFilesParallelResult{}
	}

	// Semaphore: buffered channel limits concurrent disk reads
	semaphore := make(chan struct{}, MaxConcurrentFileReads)

	// Results channel to collect file data
	resultsCh := make(chan fileReadResult, numFiles)

	var wg sync.WaitGroup

	for _, path := range filePaths {
		wg.Add(1)
		go func(filePath string) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }() // Release when done

			data, err := os.ReadFile(filePath)
			resultsCh <- fileReadResult{data: data, err: err}
		}(path)
	}

	// Close results channel after all goroutines complete
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Collect results and track failures
	allData := make([][]byte, 0, numFiles)
	readErrors := 0
	emptyFiles := 0

	for result := range resultsCh {
		if result.err != nil {
			readErrors++
			continue
		}
		if len(result.data) == 0 {
			emptyFiles++
			allData = append(allData, result.data)
			continue
		}
		allData = append(allData, result.data)
	}

	return readFilesParallelResult{
		data:       allData,
		readErrors: readErrors,
		emptyFiles: emptyFiles,
		totalFiles: numFiles,
	}
}

// DeleteAllIncrements removes all incremental files for a client
func (s *FileStateStore) DeleteAllIncrements(clientID string) error {
	lock := s.getLock(clientID)
	lock.mu.Lock()
	defer lock.mu.Unlock()

	prefix := fmt.Sprintf("data_%s_", clientID)
	suffix := ".bin"

	entries, err := os.ReadDir(s.roleDir)
	if err != nil {
		return nil
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}

		filePath := filepath.Join(s.roleDir, name)
		if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("delete incremental %s: %w", name, err)
		}
	}

	return nil
}

// getPathForClientAndBatchIndex returns the path for an incremental file
// Format: data_<clientID>_<batchIndex>.bin
func (s *FileStateStore) getPathForClientAndBatchIndex(clientID string, batchIndex int) string {
	filename := fmt.Sprintf("data_%s_%d.bin", clientID, batchIndex)
	return filepath.Join(s.roleDir, filename)
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

	prefix := fmt.Sprintf("data_%s_", clientID)
	suffix := ".bin"

	entries, err := os.ReadDir(s.roleDir)
	if err != nil {
		return make(map[int]bool), nil
	}

	batchIndices := make(map[int]bool)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}

		// Extract batchIndex from filename: data_clientID_BATCHINDEX.bin
		batchIndexStr := name[len(prefix) : len(name)-len(suffix)]
		batchIndex, err := strconv.Atoi(batchIndexStr)
		if err != nil {
			continue
		}

		batchIndices[batchIndex] = true
	}

	return batchIndices, nil
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
