package engine

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/reqfleet/replay/internal/model"
)

const currentCheckpointVersion = 1

type checkpointData struct {
	Version     int                         `json:"version"`
	UpdatedAt   string                      `json:"updated_at,omitempty"`
	Connections map[model.ConnectionKey]int `json:"connections"`
}

type checkpointStore struct {
	mu         sync.Mutex
	persistMu  sync.Mutex
	closeOnce  sync.Once
	path       string
	data       checkpointData
	dirty      bool
	generation uint64
	persistErr error
}

func checkpointPath(path string, shardIndex, shardCount int) string {
	if path == "" || shardCount <= 1 {
		return path
	}
	return fmt.Sprintf("%s.shard-%d-of-%d", path, shardIndex, shardCount)
}

func newCheckpointStore(path string) (*checkpointStore, error) {
	if path == "" {
		return nil, nil
	}
	store := &checkpointStore{
		path: path,
		data: checkpointData{
			Version:     currentCheckpointVersion,
			Connections: map[model.ConnectionKey]int{},
		},
	}

	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return store, nil
		}
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	if len(b) == 0 {
		return store, nil
	}
	if err := json.Unmarshal(b, &store.data); err != nil {
		return nil, fmt.Errorf("parse checkpoint: %w", err)
	}
	if store.data.Version != currentCheckpointVersion {
		return nil, fmt.Errorf(
			"unsupported checkpoint version %d (want %d)",
			store.data.Version,
			currentCheckpointVersion,
		)
	}
	if store.data.Connections == nil {
		store.data.Connections = map[model.ConnectionKey]int{}
	}
	return store, nil
}

func (c *checkpointStore) alreadyProcessed(connectionKey model.ConnectionKey, sequence int) bool {
	if c == nil || sequence <= 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	last, ok := c.data.Connections[connectionKey]
	return ok && sequence <= last
}

func (c *checkpointStore) lastProcessed(connectionKey model.ConnectionKey) int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data.Connections[connectionKey]
}

func (c *checkpointStore) markProcessed(connectionKey model.ConnectionKey, sequence int) error {
	if c == nil || sequence <= 0 {
		return nil
	}

	c.mu.Lock()
	if c.persistErr != nil {
		err := c.persistErr
		c.mu.Unlock()
		return err
	}
	if sequence > c.data.Connections[connectionKey] {
		c.data.Connections[connectionKey] = sequence
		c.data.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		c.dirty = true
		c.generation++
	}
	c.mu.Unlock()

	// A successful return acknowledges that this watermark is on disk. Calling
	// persist even for an already-seen sequence waits for any concurrent writer.
	return c.persist()
}

func (c *checkpointStore) persist() error {
	if c == nil || c.path == "" {
		return nil
	}

	c.persistMu.Lock()
	defer c.persistMu.Unlock()

	c.mu.Lock()
	if !c.dirty {
		err := c.persistErr
		c.mu.Unlock()
		return err
	}
	generation := c.generation
	copyData := checkpointData{
		Version:     c.data.Version,
		UpdatedAt:   c.data.UpdatedAt,
		Connections: maps.Clone(c.data.Connections),
	}
	c.mu.Unlock()

	payload, err := json.Marshal(copyData)
	if err != nil {
		return c.recordPersistenceError(fmt.Errorf("marshal checkpoint: %w", err))
	}
	if err := writeCheckpointFile(c.path, payload); err != nil {
		return c.recordPersistenceError(err)
	}

	c.mu.Lock()
	if c.generation == generation {
		c.dirty = false
	}
	err = c.persistErr
	c.mu.Unlock()
	return err
}

func writeCheckpointFile(path string, payload []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("prepare checkpoint dir: %w", err)
	}

	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create checkpoint tmp: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod checkpoint tmp: %w", err)
	}
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write checkpoint tmp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync checkpoint tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close checkpoint tmp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace checkpoint: %w", err)
	}

	dirFile, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open checkpoint dir: %w", err)
	}
	defer dirFile.Close()
	if err := dirFile.Sync(); err != nil {
		return fmt.Errorf("sync checkpoint dir: %w", err)
	}
	return nil
}

func (c *checkpointStore) recordPersistenceError(err error) error {
	c.mu.Lock()
	if c.persistErr == nil {
		c.persistErr = err
	}
	storedErr := c.persistErr
	c.mu.Unlock()
	return storedErr
}

// Close completes any outstanding write and returns the first persistence error.
func (c *checkpointStore) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		_ = c.persist()
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.persistErr
}
