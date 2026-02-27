package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type checkpointData struct {
	Version     int            `json:"version"`
	UpdatedAt   string         `json:"updated_at,omitempty"`
	Connections map[string]int `json:"connections"`
}

type checkpointStore struct {
	mu   sync.Mutex
	path string
	data checkpointData
}

func newCheckpointStore(path string) (*checkpointStore, error) {
	if path == "" {
		return nil, nil
	}
	store := &checkpointStore{
		path: path,
		data: checkpointData{
			Version:     1,
			Connections: map[string]int{},
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
	if store.data.Connections == nil {
		store.data.Connections = map[string]int{}
	}
	if store.data.Version == 0 {
		store.data.Version = 1
	}
	return store, nil
}

func (c *checkpointStore) alreadyProcessed(connectionID string, sequence int) bool {
	if c == nil || connectionID == "" || sequence <= 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	last, ok := c.data.Connections[connectionID]
	return ok && sequence <= last
}

func (c *checkpointStore) markProcessed(connectionID string, sequence int) error {
	if c == nil || connectionID == "" || sequence <= 0 {
		return nil
	}

	c.mu.Lock()
	last := c.data.Connections[connectionID]
	if sequence <= last {
		c.mu.Unlock()
		return nil
	}
	c.data.Connections[connectionID] = sequence
	c.data.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	payload, err := json.MarshalIndent(c.data, "", "  ")
	c.mu.Unlock()
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(c.path), 0o755); err != nil {
		return fmt.Errorf("prepare checkpoint dir: %w", err)
	}
	tmpPath := c.path + ".tmp"
	if err := os.WriteFile(tmpPath, payload, 0o644); err != nil {
		return fmt.Errorf("write checkpoint tmp: %w", err)
	}
	if err := os.Rename(tmpPath, c.path); err != nil {
		return fmt.Errorf("replace checkpoint: %w", err)
	}
	return nil
}
