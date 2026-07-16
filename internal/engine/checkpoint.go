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
	mu                  sync.Mutex
	persistCond         *sync.Cond
	closeOnce           sync.Once
	path                string
	data                checkpointData
	persisting          bool
	generation          uint64
	persistedGeneration uint64
	persistErr          error
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
	store.persistCond = sync.NewCond(&store.mu)

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
		c.generation++
	}
	targetGeneration := c.generation
	c.mu.Unlock()

	// A successful return acknowledges that this caller's observed generation
	// is on disk. Calls covered by the same snapshot return together instead of
	// serially persisting generations that are already durable.
	return c.persistThrough(targetGeneration)
}

func (c *checkpointStore) persistThrough(targetGeneration uint64) error {
	if c == nil || c.path == "" {
		return nil
	}

	c.mu.Lock()
	for {
		if c.persistErr != nil {
			err := c.persistErr
			c.mu.Unlock()
			return err
		}
		if c.persistedGeneration >= targetGeneration {
			c.mu.Unlock()
			return nil
		}
		if c.persisting {
			c.persistCond.Wait()
			continue
		}

		generation := c.generation
		copyData := checkpointData{
			Version:     c.data.Version,
			UpdatedAt:   c.data.UpdatedAt,
			Connections: maps.Clone(c.data.Connections),
		}
		c.persisting = true
		c.mu.Unlock()

		err := persistCheckpointData(c.path, copyData)

		c.mu.Lock()
		if err != nil && c.persistErr == nil {
			c.persistErr = err
		}
		if err == nil {
			c.persistedGeneration = generation
		}
		c.persisting = false
		c.persistCond.Broadcast()
	}
}

func persistCheckpointData(path string, data checkpointData) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	return writeCheckpointFile(path, payload)
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

// Close completes any outstanding write and returns the first persistence error.
func (c *checkpointStore) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		targetGeneration := c.generation
		c.mu.Unlock()
		_ = c.persistThrough(targetGeneration)
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.persistErr
}
