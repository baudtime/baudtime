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
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/baudtime/baudtime/msg"
	backendmsg "github.com/baudtime/baudtime/msg/backend"
	"github.com/baudtime/baudtime/tcp/client"
	ts "github.com/baudtime/baudtime/util/time"
	. "github.com/baudtime/baudtime/vars"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	uuid "github.com/satori/go.uuid"
)

type ReplicateManager struct {
	id                 *string
	db                 *tsdb.DB
	sampleFeeds        *sync.Map //map[string]*sampleFeed
	heartbeat          *Heartbeat
	lastTRecvHeartbeat atomic.Value
	sync.RWMutex
}

func NewReplicateManager(db *tsdb.DB) *ReplicateManager {
	id, err := loadRelationID(db.Dir())
	if err != nil {
		panic(err)
	}

	return &ReplicateManager{
		id:          id,
		sampleFeeds: new(sync.Map),
		db:          db,
	}
}

func (mgr *ReplicateManager) HandleWriteReq(request []byte) {
	toDelete := getStrSlice()

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

	for _, name := range toDelete {
		feed, found := mgr.sampleFeeds.Load(name)
		if found {
			feed.(*sampleFeed).Close()
			mgr.sampleFeeds.Delete(name)
		}
		level.Warn(Logger).Log("msg", "slave was removed", "slaveAddr", name)
	}

	putStrSlice(toDelete)
}

func (mgr *ReplicateManager) HandleHeartbeat(heartbeat *backendmsg.SyncHeartbeat) *backendmsg.SyncHeartbeatAck {
	//level.Info(Logger).Log("msg", "receive heartbeat from slave", "slaveAddr", heartbeat.SlaveAddr)
	mgr.lastTRecvHeartbeat.Store(time.Now())

	var feed *sampleFeed

	feedI, found := mgr.sampleFeeds.Load(heartbeat.SlaveAddr)
	if !found {
		feed = NewSampleFeed(heartbeat.SlaveAddr, int64(Cfg.Storage.Replication.HandleOffSize))
		mgr.sampleFeeds.Store(heartbeat.SlaveAddr, feed)
	} else {
		feed = feedI.(*sampleFeed)
		feed.FlushIfNeeded()
	}

	if heartbeat.BlkSyncOffset == nil {
		return &backendmsg.SyncHeartbeatAck{
			Status: msg.StatusCode_Succeed,
		}
	}

	block := getBlock(mgr.db.Blocks(), feed.createdTime, heartbeat.BlkSyncOffset.MinT)
	if block == nil {
		mgr.db.EnableCompactions()
		return &backendmsg.SyncHeartbeatAck{
			Status: msg.StatusCode_Succeed,
		}
	}

	ulid := heartbeat.BlkSyncOffset.Ulid
	minT := heartbeat.BlkSyncOffset.MinT
	maxT := heartbeat.BlkSyncOffset.MaxT
	path := heartbeat.BlkSyncOffset.Path
	offset := heartbeat.BlkSyncOffset.Offset

	if block.String() != ulid { //not found
		ulid = block.String()
		minT = block.MinTime()
		maxT = block.MaxTime()
		path = metaFileName
		offset = 0
	}

	f, err := os.Open(filepath.Join(mgr.db.Dir(), ulid, path))
	if err != nil {
		return &backendmsg.SyncHeartbeatAck{
			Status:  msg.StatusCode_Failed,
			Message: err.Error(),
		}
	}
	defer f.Close()

	f.Seek(offset, 0)

	bytes := make([]byte, PageSize)

	m, err := f.Read(bytes)
	if err == nil {
		return &backendmsg.SyncHeartbeatAck{
			Status: msg.StatusCode_Succeed,
			BlkSyncOffset: &backendmsg.BlockSyncOffset{
				Ulid:   ulid,
				MinT:   minT,
				MaxT:   maxT,
				Path:   path,
				Offset: offset,
			},
			Data: bytes[:m],
		}
	}

	if err != io.EOF {
		return &backendmsg.SyncHeartbeatAck{
			Status:  msg.StatusCode_Failed,
			Message: err.Error(),
		}
	}

	path, err = getNextPath(filepath.Join(mgr.db.Dir(), ulid), path)
	if err != nil {
		return &backendmsg.SyncHeartbeatAck{
			Status:  msg.StatusCode_Failed,
			Message: err.Error(),
		}
	}
	offset = 0

	if path == "" {
		block = getBlock(mgr.db.Blocks(), feed.createdTime, maxT)
		if block == nil {
			mgr.db.EnableCompactions()
			return &backendmsg.SyncHeartbeatAck{
				Status: msg.StatusCode_Succeed,
			}
		}

		ulid = block.String()
		minT = block.MinTime()
		maxT = block.MaxTime()
		path = metaFileName
	}

	return &backendmsg.SyncHeartbeatAck{
		Status: msg.StatusCode_Succeed,
		BlkSyncOffset: &backendmsg.BlockSyncOffset{
			Ulid:   ulid,
			MinT:   minT,
			MaxT:   maxT,
			Path:   path,
			Offset: offset,
		},
	}
}

func getBlock(blocks []*tsdb.Block, blockMaxTimeToSync int64, minTimeOfBlock int64) *tsdb.Block {
	for _, block := range blocks {
		if block.MaxTime() >= blockMaxTimeToSync {
			break
		}

		if block.MinTime() >= minTimeOfBlock {
			return block
		}
	}

	return nil
}

func getNextPath(blockDir, currentPath string) (string, error) {
	path := ""

	if currentPath == metaFileName {
		path = indexFileName
	} else if currentPath == indexFileName {
		path = tombstonesFileName
	} else if currentPath == tombstonesFileName {
		path = filepath.Join("chunks", fmt.Sprintf("%0.6d", 1))
	} else {
		i, err := strconv.ParseUint(strings.TrimPrefix(currentPath, "chunks"+string(os.PathSeparator)), 10, 64)
		if err != nil {
			return "", err
		}

		path = filepath.Join("chunks", fmt.Sprintf("%0.6d", i+1))
	}

	if _, err := os.Stat(filepath.Join(blockDir, path)); os.IsNotExist(err) {
		return "", nil
	}

	return path, nil
}

func (mgr *ReplicateManager) HandleSyncHandshake(handshake *backendmsg.SyncHandshake) *backendmsg.SyncHandshakeAck {
	if handshake.SlaveOfNoOne {
		feed, found := mgr.sampleFeeds.Load(handshake.SlaveAddr)
		if found {
			feed.(*sampleFeed).Close()
			mgr.sampleFeeds.Delete(handshake.SlaveAddr)
			level.Info(Logger).Log("msg", "slave was removed", "slaveAddr", handshake.SlaveAddr)
		}

		return &backendmsg.SyncHandshakeAck{Status: backendmsg.HandshakeStatus_NoLongerMySlave, RelationID: mgr.RelationID()}
	}

	if handshake.BlocksMinT != math.MinInt64 && handshake.BlocksMinT != blocksMinTime(mgr.db) {
		return &backendmsg.SyncHandshakeAck{
			Status:     backendmsg.HandshakeStatus_FailedToSync,
			RelationID: mgr.RelationID(),
			Message:    fmt.Sprintf("dirty, not clean, master's minT: %v, slave's minT: %v", blocksMinTime(mgr.db), handshake.BlocksMinT),
		}
	}

	feed, found := mgr.sampleFeeds.Load(handshake.SlaveAddr)
	if !found { //to be new slave
		mgr.JoinCluster()
		mgr.sampleFeeds.Store(handshake.SlaveAddr, NewSampleFeed(handshake.SlaveAddr, int64(Cfg.Storage.Replication.HandleOffSize)))

		mgr.db.DisableCompactions() //TODO multi slave

		return &backendmsg.SyncHandshakeAck{Status: backendmsg.HandshakeStatus_NewSlave, RelationID: mgr.RelationID()}
	} else {
		feed.(*sampleFeed).FlushIfNeeded()
	}

	return &backendmsg.SyncHandshakeAck{Status: backendmsg.HandshakeStatus_AlreadyMySlave, Message: "already my slave", RelationID: mgr.RelationID()}
}

func (mgr *ReplicateManager) HandleSlaveOfCmd(slaveOfCmd *backendmsg.SlaveOfCommand) *msg.GeneralResponse {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.heartbeat != nil {
		if slaveOfCmd.MasterAddr == "" { //slaveof no one
			mgr.heartbeat.stop()
			ack, err := mgr.syncHandshake(mgr.heartbeat.masterAddr, true)
			mgr.heartbeat = nil

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
				head.Truncate(ts.FromTime(time.Now()) - Cfg.Storage.TSDB.BlockRanges[0]/2) //clear data in mem
			case backendmsg.HandshakeStatus_AlreadyMySlave:
				level.Info(Logger).Log("msg", "already an slave of the master", "masterAddr", slaveOfCmd.MasterAddr)
			}

			mgr.heartbeat = &Heartbeat{
				db:         mgr.db,
				masterAddr: slaveOfCmd.MasterAddr,
				relationID: ack.RelationID,
			}
			go mgr.heartbeat.start()

			return &msg.GeneralResponse{Status: msg.StatusCode_Succeed, Message: ack.Message}
		}
	}
}

func (mgr *ReplicateManager) syncHandshake(masterAddr string, slaveOfNoOne bool) (*backendmsg.SyncHandshakeAck, error) {
	masterCli := client.NewBackendClient("rpl_s2m", masterAddr, 2)
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

	relationID := mgr.RelationID()
	if relationID == "" {
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&mgr.id)), nil, unsafe.Pointer(&ack.RelationID)) {
			return ack, nil
		}
	} else if relationID == ack.RelationID {
		return ack, nil
	}

	return nil, errors.New("unexpected relation id")
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
		level.Info(Logger).Log(
			"slave", key.(string),
			"buffered", value.(*sampleFeed).h.Buffered(),
			"cErr", value.(*sampleFeed).h.cErr,
			"allWriten", value.(*sampleFeed).h.bytesWriten,
			"allBuffered", value.(*sampleFeed).h.bytesBuffered,
		)
		return true
	})
	return
}

func (mgr *ReplicateManager) JoinCluster() {
	rid := strings.Replace(uuid.NewV1().String(), "-", "", -1)
	if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&mgr.id)), nil, unsafe.Pointer(&rid)) {
		storeRelationID(mgr.db.Dir(), rid)
	}
}

func (mgr *ReplicateManager) RelationID() (rid string) {
	id := (*string)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&mgr.id))))
	if id != nil {
		return *id
	}
	return ""
}

func (mgr *ReplicateManager) Close() error {
	if mgr.heartbeat != nil {
		mgr.heartbeat.stop()
	}
	return nil
}

func (mgr *ReplicateManager) LastHeartbeat() (recv time.Time, send time.Time) {
	if v := mgr.lastTRecvHeartbeat.Load(); v != nil {
		recv = v.(time.Time)
	}
	if mgr.heartbeat != nil {
		if v := mgr.heartbeat.lastTSendHeartbeat.Load(); v != nil {
			send = v.(time.Time)
		}
	}
	return
}
