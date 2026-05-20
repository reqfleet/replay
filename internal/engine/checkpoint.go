package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/reqfleet/replay/internal/model"
)

type checkpointData struct {
	Version     int                         `json:"version"`
	UpdatedAt   string                      `json:"updated_at,omitempty"`
	Connections map[model.ConnectionKey]int `json:"connections"`
}

type checkpointStore struct {
	mu   sync.Mutex
	path string
	data checkpointData
	// Batched persistence
	flushCh       chan struct{}
	closeCh       chan struct{}
	wg            sync.WaitGroup
	flushInterval time.Duration
	stopOnce      sync.Once
	persistedOnce bool
}

func newCheckpointStore(path string) (*checkpointStore, error) {
	if path == "" {
		return nil, nil
	}
	store := &checkpointStore{
		path: path,
		data: checkpointData{
			Version:     1,
			Connections: map[model.ConnectionKey]int{},
		},
		flushCh:       make(chan struct{}, 1),
		closeCh:       make(chan struct{}),
		flushInterval: time.Second,
	}

	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// start background flusher even if file doesn't exist yet
			store.wg.Add(1)
			go store.flusher()
			return store, nil
		}
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	if len(b) == 0 {
		// start background flusher when file is empty
		store.wg.Add(1)
		go store.flusher()
		return store, nil
	}
	if err := json.Unmarshal(b, &store.data); err != nil {
		return nil, fmt.Errorf("parse checkpoint: %w", err)
	}
	// mark that we loaded an on-disk checkpoint
	store.persistedOnce = true
	if store.data.Connections == nil {
		store.data.Connections = map[model.ConnectionKey]int{}
	}
	if store.data.Version == 0 {
		store.data.Version = 1
	}
	// start background flusher
	store.wg.Add(1)
	go store.flusher()
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

func (c *checkpointStore) markProcessed(connectionKey model.ConnectionKey, sequence int) error {
	if c == nil || sequence <= 0 {
		return nil
	}

	c.mu.Lock()
	last := c.data.Connections[connectionKey]
	if sequence <= last {
		c.mu.Unlock()
		return nil
	}
	c.data.Connections[connectionKey] = sequence
	c.data.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	c.mu.Unlock()

	// If we haven't yet persisted anything to disk, do a synchronous
	// persist on the first update so callers that expect immediate
	// on-disk checkpoints (tests, short-lived runs) see the file.
	c.mu.Lock()
	if !c.persistedOnce {
		c.persistedOnce = true
		c.mu.Unlock()
		if err := c.persist(); err != nil {
			return err
		}
		return nil
	}
	c.mu.Unlock()

	// signal flusher (non-blocking)
	select {
	case c.flushCh <- struct{}{}:
	default:
	}
	return nil
}

// persist writes the current checkpoint data to disk atomically.
func (c *checkpointStore) persist() error {
	if c == nil || c.path == "" {
		return nil
	}
	// make a copy of the data while holding the lock to avoid races.
	c.mu.Lock()
	copyData := checkpointData{
		Version:     c.data.Version,
		UpdatedAt:   c.data.UpdatedAt,
		Connections: make(map[model.ConnectionKey]int, len(c.data.Connections)),
	}
	for k, v := range c.data.Connections {
		copyData.Connections[k] = v
	}
	c.mu.Unlock()

	payload, err := json.MarshalIndent(copyData, "", "  ")
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

// flusher runs in background and periodically persists queued updates.
func (c *checkpointStore) flusher() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.flushCh:
			if err := c.persist(); err != nil {
				fmt.Fprintf(os.Stderr, "checkpoint persist failed: %v\n", err)
			}
		case <-ticker.C:
			if err := c.persist(); err != nil {
				fmt.Fprintf(os.Stderr, "checkpoint persist failed: %v\n", err)
			}
		case <-c.closeCh:
			// final flush before exiting
			if err := c.persist(); err != nil {
				fmt.Fprintf(os.Stderr, "checkpoint final persist failed: %v\n", err)
			}
			return
		}
	}
}

// Close stops the background flusher and performs a final flush.
func (c *checkpointStore) Close() error {
	if c == nil {
		return nil
	}
	c.stopOnce.Do(func() {
		close(c.closeCh)
	})
	c.wg.Wait()
	return nil
}
