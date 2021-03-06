//go:generate msgp -tests=false

package backend

import (
	"fmt"
	"github.com/baudtime/baudtime/msg"
)

type HandshakeStatus byte

const (
	HandshakeStatus_FailedToSync HandshakeStatus = iota
	HandshakeStatus_NoLongerMySlave
	HandshakeStatus_NewSlave
	HandshakeStatus_AlreadyMySlave
)

func (z HandshakeStatus) String() string {
	switch z {
	case HandshakeStatus_FailedToSync:
		return "FailedToSync"
	case HandshakeStatus_NoLongerMySlave:
		return "NoLongerMySlave"
	case HandshakeStatus_NewSlave:
		return "NewSlave"
	case HandshakeStatus_AlreadyMySlave:
		return "AlreadyMySlave"
	}
	return "<Invalid>"
}

type SlaveOfCommand struct {
	MasterAddr string `msg:"masterAddr"`
}

type SyncHandshake struct {
	SlaveAddr    string `msg:"slaveAddr"`
	BlocksMinT   int64  `msg:"blocksMinT"`
	SlaveOfNoOne bool   `msg:"slaveOfNoOne"`
}

type SyncHandshakeAck struct {
	Status  HandshakeStatus `msg:"status"`
	ShardID string          `msg:"shardID"`
	Message string          `msg:"message"`
}

type BlockSyncOffset struct {
	Ulid   string `msg:"ulid"`
	MinT   int64  `msg:"minT"`
	MaxT   int64  `msg:"maxT"`
	Path   string `msg:"path"`
	Offset int64  `msg:"Offset"`
}

func (offset BlockSyncOffset) String() string {
	var empty BlockSyncOffset
	if offset == empty {
		return "Unset"
	}
	return fmt.Sprintf("[%s, %s, %d]", offset.Ulid, offset.Path, offset.Offset)
}

type SyncHeartbeat struct {
	MasterAddr    string           `msg:"masterAddr"`
	SlaveAddr     string           `msg:"slaveAddr"`
	ShardID       string           `msg:"shardID"`
	Epoch         int64            `msg:"epoch"`
	BlkSyncOffset *BlockSyncOffset `msg:"blkSyncOffset"`
}

type SyncHeartbeatAck struct {
	Status        msg.StatusCode   `msg:"status"`
	Message       string           `msg:"message"`
	BlkSyncOffset *BlockSyncOffset `msg:"blkSyncOffset"`
	Data          []byte           `msg:"data"`
}
