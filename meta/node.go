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

package meta

import (
	"context"
	"encoding/json"
	"github.com/baudtime/baudtime/util/os/fileutil"
	"google.golang.org/grpc"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baudtime/baudtime/util"
	"github.com/baudtime/baudtime/util/redo"
	"github.com/baudtime/baudtime/vars"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
)

const replicationMeta = "replication.json"

type Role byte

const (
	RoleUnset Role = iota
	RoleMaster
	RoleSlave
)

func (role Role) String() string {
	switch role {
	case RoleMaster:
		return "Master"
	case RoleSlave:
		return "Slave"
	}
	return "Unset"
}

type ReplicaMeta struct {
	ShardID    string `json:"shard_id"`
	Role       Role   `json:"role"`
	MasterAddr string `json:"master_addr,omitempty"`
}

func (meta *ReplicaMeta) LoadFromDisk(dir string) error {
	b, err := ioutil.ReadFile(filepath.Join(dir, replicationMeta))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		} else {
			return err
		}
	}

	if err := json.Unmarshal(b, meta); err != nil {
		return err
	}

	level.Info(vars.Logger).Log("msg", "replication meta id was loaded",
		"ShardID", meta.ShardID,
		"Role", meta.Role.String(),
		"masterAddr", meta.MasterAddr,
	)
	return nil
}

func (meta *ReplicaMeta) StoreToDisk(dir string) error {
	path := filepath.Join(dir, replicationMeta)
	tmp := path + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "\t")
	err = enc.Encode(meta)
	if err != nil {
		f.Close()
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}

	err = fileutil.RenameFile(tmp, path)
	if err != nil {
		return err
	}

	level.Info(vars.Logger).Log("msg", "replication meta id was stored",
		"ShardID", meta.ShardID,
		"Role", meta.Role.String(),
		"masterAddr", meta.MasterAddr,
	)

	return nil
}

type Node struct {
	Addr string `json:"addr"`
	ReplicaMeta
	DiskFree uint64 `json:"disk_free"`
	IDC      string `json:"idc"`
}

func (node *Node) MayOnline() bool {
	if util.Ping(node.Addr) {
		return true
	}

	nodeExist, err := exist(nodePrefix() + node.Addr)
	if err != nil {
		return true
	}

	return nodeExist
}

type Nodes []Node

func (nodes Nodes) Len() int {
	return len(nodes)
}

func (nodes Nodes) Swap(i, j int) {
	nodes[i], nodes[j] = nodes[j], nodes[i]
}

func (nodes Nodes) Less(i, j int) bool {
	return nodes[i].DiskFree < nodes[j].DiskFree
}

func GetNodes(withSort bool) ([]Node, error) {
	resp, err := etcdGetWithPrefix(nodePrefix())
	if err == ErrKeyNotFound {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	nodes := make(Nodes, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		var node Node
		err = json.Unmarshal(kv.Value, &node)
		if err != nil {
			return nil, err
		}
		nodes[i] = node
	}

	if withSort {
		sort.Sort(nodes)
	}

	return nodes, nil
}

func GetMasters() ([]Node, error) {
	nodes, err := GetNodes(false)
	if err != nil {
		return nil, err
	}

	var masters Nodes
	for _, node := range nodes {
		if node.Role == RoleMaster && node.ShardID != "" {
			masters = append(masters, node)
		}
	}

	sort.Sort(masters)

	return masters, nil
}

type Heartbeat struct {
	cmtx         sync.RWMutex
	client       *clientv3.Client   //maybe reconnect
	leaseTTL     int64              //maybe reGrant
	leaseID      clientv3.LeaseID   //maybe rekeepalive
	cancel       context.CancelFunc //maybe used to maybe
	interval     time.Duration
	f            func() (Node, error)
	lastNodeInfo Node
	registerC    chan struct{}
	exitCh       chan struct{}
	wg           sync.WaitGroup
	started      uint32
}

func NewHeartbeat(leaseTTL time.Duration, reportInterval time.Duration, f func() (Node, error)) *Heartbeat {
	return &Heartbeat{
		leaseTTL:  int64(leaseTTL.Seconds()),
		interval:  reportInterval,
		f:         f,
		registerC: make(chan struct{}),
		exitCh:    make(chan struct{}),
	}
}

func (h *Heartbeat) Start() error {
	if !atomic.CompareAndSwapUint32(&h.started, 0, 1) {
		return nil
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   vars.Cfg.Etcd.Endpoints,
		DialTimeout: time.Duration(vars.Cfg.Etcd.DialTimeout),
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
	})
	if err != nil {
		return errors.Wrap(err, "can't init heartbeat etcd client")
	}
	h.client = cli

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(vars.Cfg.Etcd.RWTimeout))
	defer cancel()

	leaseResp, err := h.client.Grant(ctx, h.leaseTTL)
	if err != nil {
		return errors.Wrap(err, "can't init heartbeat etcd lease")
	}
	h.leaseID = leaseResp.ID

	h.wg.Add(1)
	go func() {
		h.keepLease()
		h.wg.Done()
	}()

	node, err := h.f()
	if err != nil {
		return errors.Wrap(err, "can't get node info")
	}

	err = h.reportInfo(node)
	if err != nil {
		return errors.Wrap(err, "can't register node")
	}

	h.wg.Add(1)
	go func() {
		h.cronReportInfo()
		h.wg.Done()
	}()

	return nil
}

func (h *Heartbeat) Stop() {
	close(h.exitCh)
	h.wg.Wait()

	if h.client != nil {
		redo.Retry(time.Duration(vars.Cfg.Etcd.RetryInterval), vars.Cfg.Etcd.RetryNum, func() (bool, error) {
			ctx, cancel := context.WithTimeout(h.client.Ctx(), time.Duration(vars.Cfg.Etcd.RWTimeout))
			_, err := h.client.Delete(ctx, nodePrefix()+h.lastNodeInfo.Addr)
			cancel()
			if err != nil {
				return true, err
			}
			return false, nil
		})

		h.client.Revoke(context.Background(), h.leaseID)
		h.client.Close()
	}
}

func (h *Heartbeat) keepLease() {
	reGrant := false

	for {
		select {
		case <-h.exitCh:
			level.Warn(vars.Logger).Log("msg", "keep lease for heartbeat exit!!!")
			return
		default:
		}

		if reGrant {
			level.Warn(vars.Logger).Log("msg", "regrant for heartbeat")
			leaseResp, err := h.client.Grant(context.Background(), h.leaseTTL)
			if err != nil {
				time.Sleep(2 * time.Second)
				continue
			}

			h.leaseID = leaseResp.ID
			reGrant = false
		}

		ctx, cancel := context.WithCancel(context.Background())
		keepAlive, err := h.client.KeepAlive(ctx, h.leaseID)
		if err != nil || keepAlive == nil {
			cancel()
			reGrant = true
			continue
		}

		h.cancel = cancel

		go func() {
			h.registerC <- struct{}{}
		}()

	keepAliveLoop:
		for {
			select {
			case _, ok := <-keepAlive:
				if !ok {
					break keepAliveLoop
				}
				// just eat messages
			case <-h.exitCh:
				level.Warn(vars.Logger).Log("msg", "keep lease for heartbeat exit!!!")
				return
			}
		}

		reGrant = true
	}
}

func (h *Heartbeat) reportInfo(node Node) error {
	b, err := json.Marshal(&node)
	if err != nil {
		return err
	}

	h.cmtx.RLock()
	if h.client != nil && h.leaseID != clientv3.NoLease {
		err = redo.Retry(time.Duration(vars.Cfg.Etcd.RetryInterval), vars.Cfg.Etcd.RetryNum, func() (bool, error) {
			ctx, cancel := context.WithTimeout(h.client.Ctx(), time.Duration(vars.Cfg.Etcd.RWTimeout))
			_, er := h.client.Put(ctx, nodePrefix()+node.Addr, string(b), clientv3.WithLease(h.leaseID))
			cancel()
			if er != nil {
				return true, er
			}
			return false, nil
		})
	}
	h.cmtx.RUnlock()

	if err == nil {
		h.lastNodeInfo = node
	} else if h.cancel != nil {
		h.cancel() //let reconnect and keep lease again
	}

	return err
}

func (h *Heartbeat) cronReportInfo() {
	var (
		node Node
		err  error
	)

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if node, err = h.f(); err == nil && node != h.lastNodeInfo {
				err = h.reportInfo(node)
			}
		case <-h.registerC:
			if node, err = h.f(); err == nil {
				err = h.reportInfo(node)
			}
		case <-h.exitCh:
			level.Warn(vars.Logger).Log("msg", "report host info for heartbeat exit!!!")
			return
		}

		if err != nil {
			level.Error(vars.Logger).Log("msg", "error occurred while reporting", "err", err)
		}
	}
}
