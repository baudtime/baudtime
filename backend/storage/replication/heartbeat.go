/*
 * Copyright 2019 The Baudtime Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package replication

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/baudtime/baudtime/msg"
	backendmsg "github.com/baudtime/baudtime/msg/backend"
	"github.com/baudtime/baudtime/tcp/client"
	"github.com/baudtime/baudtime/util/os/fileutil"
	t "github.com/baudtime/baudtime/util/time"
	. "github.com/baudtime/baudtime/vars"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/tsdb"
)

const (
	metaFileName       = "meta.json"
	indexFileName      = "index"
	tombstonesFileName = "tombstones"
)

type Heartbeat struct {
	db                 *tsdb.DB
	masterAddr         string
	shardID            string
	epoch              int64
	masterCli          *client.Client
	closed             uint32
	lastTSendHeartbeat int64
}

func (h *Heartbeat) start() {
	if h.masterCli == nil {
		h.masterCli = client.NewBackendClient("rpl_s2m", h.masterAddr, 1, 0)
	}

	heartbeat := &backendmsg.SyncHeartbeat{
		MasterAddr:    h.masterAddr,
		SlaveAddr:     fmt.Sprintf("%v:%v", LocalIP, Cfg.TcpPort),
		ShardID:       h.shardID,
		Epoch:         h.epoch,
		BlkSyncOffset: getStartSyncOffset(h.db.Blocks()),
	}

	var fileSyncing *os.File
	var sleepTime = time.Second

	for h.isRunning() {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		reply, err := h.masterCli.SyncRequest(ctx, heartbeat)
		if err != nil {
			time.Sleep(sleepTime)
			sleepTime = t.Exponential(sleepTime, time.Second, time.Minute)
			continue
		} else {
			sleepTime = time.Second
		}

		ack, ok := reply.(*backendmsg.SyncHeartbeatAck)
		if !ok {
			level.Error(Logger).Log("error", "unexpected response")
			continue
		}

		atomic.StoreInt64(&h.lastTSendHeartbeat, t.FromTime(time.Now()))

		if ack.Status != msg.StatusCode_Succeed {
			level.Error(Logger).Log("error", ack.Message)
			continue
		}

		if ack.BlkSyncOffset == nil {
			if heartbeat.BlkSyncOffset != nil && heartbeat.BlkSyncOffset.Ulid != "" {
				preBlockDir := filepath.Join(h.db.Dir(), heartbeat.BlkSyncOffset.Ulid)
				fileutil.RenameFile(preBlockDir+".tmp", preBlockDir)
				h.db.CleanTombstones() //h.db.Reload()
			}

			if fileSyncing != nil {
				fileSyncing.Close()
				fileSyncing = nil
			}

			heartbeat.BlkSyncOffset = nil
			time.Sleep(time.Duration(Cfg.Storage.Replication.HeartbeatInterval))
			continue
		} else {
			blockTmpDir := filepath.Join(h.db.Dir(), ack.BlkSyncOffset.Ulid) + ".tmp"
			if heartbeat.BlkSyncOffset != nil && heartbeat.BlkSyncOffset.Ulid != ack.BlkSyncOffset.Ulid {
				preBlockDir := filepath.Join(h.db.Dir(), heartbeat.BlkSyncOffset.Ulid)
				fileutil.RenameFile(preBlockDir+".tmp", preBlockDir)

				chunksDir := filepath.Join(blockTmpDir, "chunks")
				if err := os.MkdirAll(chunksDir, 0777); err != nil {
					level.Error(Logger).Log("msg", "can't create chunks dir", "error", err)
					continue
				}
			}
			if fileSyncing == nil {
				fileSyncing, err = os.Create(filepath.Join(blockTmpDir, ack.BlkSyncOffset.Path))
			} else if !strings.HasSuffix(fileSyncing.Name(), ack.BlkSyncOffset.Path) {
				fileSyncing.Close()
				fileSyncing, err = os.Create(filepath.Join(blockTmpDir, ack.BlkSyncOffset.Path))
			}

			if err != nil { //TODO
				level.Error(Logger).Log("error", err, "block", ack.BlkSyncOffset.Ulid, "path", ack.BlkSyncOffset.Path)
				continue
			}
			heartbeat.BlkSyncOffset = ack.BlkSyncOffset
		}

		for len(ack.Data) > 0 {
			written, err := fileSyncing.Write(ack.Data)
			heartbeat.BlkSyncOffset.Offset += int64(written)
			ack.Data = ack.Data[written:]

			if err != nil {
				level.Error(Logger).Log("error", err, "block", ack.BlkSyncOffset.Ulid, "path", ack.BlkSyncOffset.Path)
			}
		}
	}
}

func (h *Heartbeat) stop() {
	if atomic.CompareAndSwapUint32(&h.closed, 0, 1) {
		if h.masterCli != nil {
			h.masterCli.Close()
		}
	}
}

func (h *Heartbeat) isRunning() bool {
	return atomic.LoadUint32(&h.closed) == 0
}

func getStartSyncOffset(blocks []*tsdb.Block) *backendmsg.BlockSyncOffset {
	if len(blocks) == 0 {
		return &backendmsg.BlockSyncOffset{}
	}

	lastBlock := blocks[len(blocks)-1]
	os.RemoveAll(lastBlock.Dir())

	return &backendmsg.BlockSyncOffset{MinT: lastBlock.MinTime()}
}
