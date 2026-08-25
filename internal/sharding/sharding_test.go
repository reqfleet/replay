package sharding

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/reqfleet/replay/internal/model"
)

func TestConnectionBelongsToShardUsesNode(t *testing.T) {
	baseKey := model.ConnectionKey{ConnectionID: 1}
	baseShard := ConnectionBelongsToShard(baseKey, 0, 2)
	for i := range 256 {
		candidate := model.ConnectionKey{Node: fmt.Sprintf("envoy-%d", i), ConnectionID: 1}
		if ConnectionBelongsToShard(candidate, 0, 2) != baseShard {
			return
		}
	}
	t.Fatal("ConnectionBelongsToShard() did not vary by node")
}

func TestConnectionBelongsToShardSupportsFullHashSpace(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("int cannot represent the full 32-bit hash space")
	}

	const shardIndex = 1803821790
	shardCount := int(uint64(1) << 32)
	connectionKey := model.ConnectionKey{ConnectionID: 1}
	if !ConnectionBelongsToShard(connectionKey, shardIndex, shardCount) {
		t.Errorf(
			"ConnectionBelongsToShard(%v, %d, %d) = false, want true",
			connectionKey,
			shardIndex,
			shardCount,
		)
	}
}
