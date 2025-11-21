package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

// BatchIndicesSnapshot holds seen batch indices for aggregates
type BatchIndicesSnapshot struct {
	Indices []int
}

// StateStore abstracts the persistence mechanism used by stateful handlers.
type StateStore interface {
	Persist(clientID string, snapshot *Snapshot) error
	Load(clientID string) (*Snapshot, error)
	Delete(clientID string) error

	// Batch indices persistence (for aggregates with millions of batches)
	PersistBatchIndices(clientID string, indices []int) error
	LoadBatchIndices(clientID string) ([]int, error)
	DeleteBatchIndices(clientID string) error
}

// FileStateStore is a filesystem-backed implementation of StateStore.
// It stores three binary files per client inside <baseDir>/<role>:
//   - meta_<client>.bin (metadata)
//   - data_<client>.bin (handler-defined payload)
//   - batches_<client>.bin (seen batch indices, one per line)
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

	return &FileStateStore{
		roleDir: roleDir,
	}, nil
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

func (s *FileStateStore) batchesPath(clientID string) string {
	filename := fmt.Sprintf("batches_%s.bin", clientID)
	return filepath.Join(s.roleDir, filename)
}

// PersistBatchIndices writes batch indices to a separate file using efficient format
// Format: one integer per line
func (s *FileStateStore) PersistBatchIndices(clientID string, indices []int) error {
	if len(indices) == 0 {
		return nil
	}

	lock := s.getLock(clientID)
	lock.mu.Lock()
	defer lock.mu.Unlock()

	var buf []byte
	capacity := len(indices) * 10
	buf = make([]byte, 0, capacity)

	for _, idx := range indices {
		buf = append(buf, fmt.Sprintf("%d\n", idx)...)
	}

	return writeAtomically(s.batchesPath(clientID), buf)
}

// LoadBatchIndices reads batch indices from file
func (s *FileStateStore) LoadBatchIndices(clientID string) ([]int, error) {
	lock := s.getLock(clientID)
	lock.mu.RLock()
	defer lock.mu.RUnlock()

	data, err := os.ReadFile(s.batchesPath(clientID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrSnapshotNotFound
		}
		return nil, err
	}

	if len(data) == 0 {
		return []int{}, nil
	}

	var indices []int
	var num int
	start := 0
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' {
			if i > start {
				_, err := fmt.Sscanf(string(data[start:i]), "%d", &num)
				if err == nil {
					indices = append(indices, num)
				}
			}
			start = i + 1
		}
	}

	return indices, nil
}

// DeleteBatchIndices removes the batch indices file
func (s *FileStateStore) DeleteBatchIndices(clientID string) error {
	lock := s.getLock(clientID)
	lock.mu.Lock()
	defer lock.mu.Unlock()

	if err := os.Remove(s.batchesPath(clientID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
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
	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write temp file %s: %w", tmpName, err)
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return fmt.Errorf("sync temp file %s: %w", tmpName, err)
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close temp file %s: %w", tmpName, err)
	}

	if err := os.Rename(tmpName, targetPath); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename temp file %s to %s: %w", tmpName, targetPath, err)
	}

	return nil
}
