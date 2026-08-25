package sharding

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/reqfleet/replay/internal/model"
)

// MaxShardCount is the number of distinct values in the FNV-32 hash space.
const MaxShardCount = uint64(1) << 32

// ConnectionBelongsToShard reports whether connectionKey is assigned to the
// requested shard. Callers must validate shard parameters before calling.
func ConnectionBelongsToShard(connectionKey model.ConnectionKey, shardIndex, shardCount int) bool {
	if shardCount <= 1 {
		return true
	}

	hasher := fnv.New32a()
	var connectionID [8]byte
	_, _ = hasher.Write([]byte(connectionKey.Node))
	_, _ = hasher.Write([]byte{0})
	binary.LittleEndian.PutUint64(connectionID[:], uint64(connectionKey.ConnectionID))
	_, _ = hasher.Write(connectionID[:])
	return uint64(hasher.Sum32())%uint64(shardCount) == uint64(shardIndex)
}
