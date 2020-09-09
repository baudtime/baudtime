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
	"github.com/baudtime/baudtime/meta"
	"github.com/baudtime/baudtime/msg"
	backendmsg "github.com/baudtime/baudtime/msg/backend"
	"github.com/baudtime/baudtime/tcp/client"
	t "github.com/baudtime/baudtime/util/time"
	. "github.com/baudtime/baudtime/vars"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	uuid "github.com/satori/go.uuid"
	"io"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ReplicateManager struct {
	meta    meta.ReplicaMeta
	metaMtx sync.RWMutex

	db                 *tsdb.DB
	sampleFeeds        *sync.Map //map[string]*sampleFeed
	snapshot           *Snapshot
	heartbeat          *Heartbeat
	lastTRecvHeartbeat int64

	sync.RWMutex
}

func NewReplicateManager(db *tsdb.DB) *ReplicateManager {
	replicateMgr := &ReplicateManager{
		sampleFeeds: new(sync.Map),
		db:          db,
		snapshot:    newSnapshot(db),
	}

	err := replicateMgr.loadMeta()
	if err != nil {
		panic(err)
	}

	return replicateMgr
}

func (mgr *ReplicateManager) HandleWriteReq(request []byte) {
	var toDelete []string

	mgr.sampleFeeds.Range(func(name, feed interface{}) bool {
		err := feed.(*sampleFeed).Write(request)
		if err != nil {
			if err == io.ErrShortWrite {
				toDelete = append(toDelete, name.(string))
			} else {
				level.Error(Logger).Log("msg", "failed to feed slave", "err", err)
			}
		}
		return true
	})

	if len(toDelete) > 0 {
		for _, name := range toDelete {
			feed, found := mgr.sampleFeeds.Load(name)
			if found {
				feed.(*sampleFeed).Close()
				mgr.sampleFeeds.Delete(name)
			}
			level.Warn(Logger).Log("msg", "slave was removed", "slaveAddr", name)
		}
	}
}

func (mgr *ReplicateManager) HandleHeartbeat(heartbeat *backendmsg.SyncHeartbeat) *backendmsg.SyncHeartbeatAck {
	//level.Info(Logger).Log("msg", "receive heartbeat from slave", "slaveAddr", heartbeat.SlaveAddr)
	atomic.StoreInt64(&mgr.lastTRecvHeartbeat, t.FromTime(time.Now()))

	var feed *sampleFeed

	feedI, found := mgr.sampleFeeds.Load(heartbeat.SlaveAddr)
	if !found {
		feed = NewSampleFeed(heartbeat.SlaveAddr, int(Cfg.Storage.Replication.HandleOffSize))
		mgr.sampleFeeds.Store(heartbeat.SlaveAddr, feed)
	} else {
		feed = feedI.(*sampleFeed)
		feed.FlushIfNeeded()
	}

	if heartbeat.BlkSyncOffset == nil {
		mgr.snapshot.Reset()
		return &backendmsg.SyncHeartbeatAck{
			Status: msg.StatusCode_Succeed,
		}
	}

	err := mgr.snapshot.Init(heartbeat.Epoch)
	if err != nil {
		return &backendmsg.SyncHeartbeatAck{
			Status:  msg.StatusCode_Failed,
			Message: err.Error(),
		}
	}

	syncOffset := heartbeat.BlkSyncOffset

	data, err := mgr.snapshot.FetchData(syncOffset)
	if err != nil {
		return &backendmsg.SyncHeartbeatAck{
			Status:  msg.StatusCode_Failed,
			Message: err.Error(),
		}
	}

	if len(data) > 0 {
		return &backendmsg.SyncHeartbeatAck{
			Status:        msg.StatusCode_Succeed,
			BlkSyncOffset: syncOffset,
			Data:          data,
		}
	}

	syncOffset, err = mgr.snapshot.CutOffset(syncOffset)
	if err != nil {
		return &backendmsg.SyncHeartbeatAck{
			Status:  msg.StatusCode_Failed,
			Message: err.Error(),
		}
	}

	return &backendmsg.SyncHeartbeatAck{
		Status:        msg.StatusCode_Succeed,
		BlkSyncOffset: syncOffset,
	}
}

func (mgr *ReplicateManager) HandleSyncHandshake(handshake *backendmsg.SyncHandshake) *backendmsg.SyncHandshakeAck {
	if handshake.SlaveOfNoOne {
		feed, found := mgr.sampleFeeds.Load(handshake.SlaveAddr)
		if found {
			feed.(*sampleFeed).Close()
			mgr.sampleFeeds.Delete(handshake.SlaveAddr)
			level.Info(Logger).Log("msg", "slave was removed", "slaveAddr", handshake.SlaveAddr)
		}

		return &backendmsg.SyncHandshakeAck{Status: backendmsg.HandshakeStatus_NoLongerMySlave, ShardID: mgr.ShardID()}
	}

	if handshake.BlocksMinT != math.MinInt64 && handshake.BlocksMinT < blocksMinTime(mgr.db) {
		return &backendmsg.SyncHandshakeAck{
			Status:  backendmsg.HandshakeStatus_FailedToSync,
			ShardID: mgr.ShardID(),
			Message: fmt.Sprintf("dirty, not clean, master's minT: %v, slave's minT: %v", blocksMinTime(mgr.db), handshake.BlocksMinT),
		}
	}

	feed, found := mgr.sampleFeeds.Load(handshake.SlaveAddr)
	if !found { //to be new slave
		mgr.JoinCluster()
		feed := NewSampleFeed(handshake.SlaveAddr, int(Cfg.Storage.Replication.HandleOffSize))
		mgr.sampleFeeds.Store(handshake.SlaveAddr, feed)

		return &backendmsg.SyncHandshakeAck{Status: backendmsg.HandshakeStatus_NewSlave, ShardID: mgr.ShardID()}
	} else {
		feed.(*sampleFeed).FlushIfNeeded()
	}

	return &backendmsg.SyncHandshakeAck{Status: backendmsg.HandshakeStatus_AlreadyMySlave, Message: "already my slave", ShardID: mgr.ShardID()}
}

func (mgr *ReplicateManager) HandleSlaveOfCmd(slaveOfCmd *backendmsg.SlaveOfCommand) *msg.GeneralResponse {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.heartbeat != nil {
		if slaveOfCmd.MasterAddr == "" { //slaveof no one
			mgr.heartbeat.stop()
			ack, err := mgr.syncHandshake(mgr.heartbeat.masterAddr, true)
			mgr.heartbeat = nil

			mgr.storeMeta(meta.ReplicaMeta{
				ShardID: mgr.ShardID(),
				Role:    meta.RoleMaster,
			})

			if err != nil {
				return &msg.GeneralResponse{Status: msg.StatusCode_Failed, Message: err.Error()}
			}

			if ack.Status != backendmsg.HandshakeStatus_NoLongerMySlave {
				return &msg.GeneralResponse{Status: msg.StatusCode_Failed, Message: fmt.Sprintf("status:%s, message:%s", ack.Status, ack.Message)}
			}
			return &msg.GeneralResponse{Status: msg.StatusCode_Succeed}
		} else if mgr.heartbeat.masterAddr == slaveOfCmd.MasterAddr {
			return &msg.GeneralResponse{Status: msg.StatusCode_Succeed, Message: "already my slave"}
		} else {
			return &msg.GeneralResponse{Status: msg.StatusCode_Failed, Message: "already an slave of some other master, clean dirty data first"}
		}
	} else {
		if slaveOfCmd.MasterAddr == "" { //slaveof no one
			mgr.storeMeta(meta.ReplicaMeta{
				ShardID: mgr.ShardID(),
				Role:    meta.RoleMaster,
			})
			return &msg.GeneralResponse{Status: msg.StatusCode_Succeed}
		} else {
			ack, err := mgr.syncHandshake(slaveOfCmd.MasterAddr, false)
			if err != nil {
				return &msg.GeneralResponse{Status: msg.StatusCode_Failed, Message: err.Error()}
			}
			switch ack.Status {
			case backendmsg.HandshakeStatus_FailedToSync:
				return &msg.GeneralResponse{Status: msg.StatusCode_Failed, Message: ack.Message}
			case backendmsg.HandshakeStatus_NoLongerMySlave:
				return &msg.GeneralResponse{Status: msg.StatusCode_Failed, Message: fmt.Sprintf("unexpected slave of no one, ack msg: %s", ack.Message)}
			case backendmsg.HandshakeStatus_NewSlave:
				head := mgr.db.Head()
				head.Truncate(t.FromTime(time.Now()) - Cfg.Storage.TSDB.BlockRanges[0]/2) //clear data in mem
			case backendmsg.HandshakeStatus_AlreadyMySlave:
				level.Info(Logger).Log("msg", "already an slave of the master", "masterAddr", slaveOfCmd.MasterAddr)
			}

			mgr.storeMeta(meta.ReplicaMeta{
				ShardID:    ack.ShardID,
				Role:       meta.RoleSlave,
				MasterAddr: slaveOfCmd.MasterAddr,
			})

			mgr.heartbeat = &Heartbeat{
				db:         mgr.db,
				masterAddr: slaveOfCmd.MasterAddr,
				shardID:    ack.ShardID,
				epoch:      t.FromTime(time.Now()),
			}
			go mgr.heartbeat.start()

			return &msg.GeneralResponse{Status: msg.StatusCode_Succeed, Message: ack.Message}
		}
	}
}

func (mgr *ReplicateManager) syncHandshake(masterAddr string, slaveOfNoOne bool) (*backendmsg.SyncHandshakeAck, error) {
	masterCli := client.NewBackendClient("rpl_s2m", masterAddr, 1, 0)
	defer masterCli.Close()

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	reply, err := masterCli.SyncRequest(ctx, &backendmsg.SyncHandshake{
		SlaveAddr:    fmt.Sprintf("%v:%v", LocalIP, Cfg.TcpPort),
		BlocksMinT:   blocksMinTime(mgr.db),
		SlaveOfNoOne: slaveOfNoOne,
	})
	if err != nil {
		return nil, err
	}

	ack, ok := reply.(*backendmsg.SyncHandshakeAck)
	if !ok {
		return nil, errors.New("unexpected response")
	}

	myShardID := mgr.ShardID()
	if myShardID != "" && myShardID != ack.ShardID {
		return nil, errors.New("unexpected relation id")
	}

	return ack, nil
}

func (mgr *ReplicateManager) Master() (found bool, masterAddr string) {
	if mgr.heartbeat != nil {
		return true, mgr.heartbeat.masterAddr
	}

	return false, ""
}

func (mgr *ReplicateManager) Slaves() (slaveAddrs []string) {
	mgr.sampleFeeds.Range(func(key, value interface{}) bool {
		slaveAddrs = append(slaveAddrs, key.(string))
		return true
	})
	return
}

func (mgr *ReplicateManager) Meta() (m meta.ReplicaMeta) {
	mgr.metaMtx.RLock()
	m = mgr.meta
	mgr.metaMtx.RUnlock()
	return
}

func (mgr *ReplicateManager) JoinCluster() {
	shardID := mgr.ShardID()
	if len(shardID) == 0 {
		shardID = strings.Replace(uuid.NewV1().String(), "-", "", -1)
	}
	mgr.storeMeta(meta.ReplicaMeta{
		ShardID: shardID,
		Role:    meta.RoleMaster,
	})
}

func (mgr *ReplicateManager) LeftCluster() {
	if mgr.heartbeat != nil {
		mgr.heartbeat.stop()
		mgr.syncHandshake(mgr.heartbeat.masterAddr, true)

		mgr.Lock()
		mgr.heartbeat = nil
		mgr.Unlock()
	}
	mgr.storeMeta(meta.ReplicaMeta{
		ShardID:    "",
		Role:       meta.RoleUnset,
		MasterAddr: "",
	})
}

func (mgr *ReplicateManager) ShardID() string {
	return mgr.Meta().ShardID
}

func (mgr *ReplicateManager) Close() error {
	if mgr.heartbeat != nil {
		mgr.heartbeat.stop()
	}
	return nil
}

func (mgr *ReplicateManager) LastHeartbeatTime() (recv, send int64) {
	recv = atomic.LoadInt64(&mgr.lastTRecvHeartbeat)
	if mgr.heartbeat != nil {
		send = atomic.LoadInt64(&mgr.heartbeat.lastTSendHeartbeat)
	}
	return
}

func (mgr *ReplicateManager) storeMeta(m meta.ReplicaMeta) error {
	mgr.metaMtx.Lock()
	defer mgr.metaMtx.Unlock()
	mgr.meta = m
	return mgr.meta.StoreToDisk(mgr.db.Dir())
}

func (mgr *ReplicateManager) loadMeta() error {
	mgr.metaMtx.Lock()
	defer mgr.metaMtx.Unlock()
	return mgr.meta.LoadFromDisk(mgr.db.Dir())
}

func blocksMinTime(db *tsdb.DB) int64 {
	blocks := db.Blocks()
	if len(blocks) > 0 {
		return blocks[0].MinTime()
	}

	return math.MinInt64
}
