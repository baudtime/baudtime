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
	"fmt"
	"github.com/pkg/errors"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baudtime/baudtime/util"
	"github.com/baudtime/baudtime/util/redo"
	"github.com/baudtime/baudtime/vars"
	"github.com/coreos/etcd/clientv3"
	"github.com/go-kit/kit/log/level"
)

type Node struct {
	ShardID    string
	IP         string
	Port       string
	DiskFree   uint64
	IDC        string
	MasterIP   string
	MasterPort string
	addr       string    `json:"-"`
	once       sync.Once `json:"-"`
}

var EmptyNode = Node{}

func (node Node) Addr() string {
	node.once.Do(func() {
		if node.IP != "" && node.Port != "" {
			var b strings.Builder
			b.WriteString(node.IP)
			b.WriteByte(':')
			b.WriteString(node.Port)
			node.addr = b.String()
		}
	})
	return node.addr
}

func (node Node) String() string {
	s := "Shard: " + node.ShardID + "\n" +
		"IP: " + node.IP + "\n" +
		"Port: " + node.Port + "\n" +
		"DiskFree: " + strconv.FormatUint(node.DiskFree, 10) + "GB\n" +
		"IDC: " + node.IDC

	if node.MasterIP != "" && node.MasterPort != "" {
		s += fmt.Sprintf("\nMasterIP: %v", node.MasterIP)
		s += fmt.Sprintf("\nMasterPort: %v", node.MasterPort)
	}

	return s
}

func (node Node) MayOnline() bool {
	addr := node.Addr()
	if util.Ping(addr) {
		return true
	}

	nodeExist, err := exist(nodePrefix() + addr)
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
		if node.MasterIP == "" && node.MasterPort == "" && node.ShardID != "" {
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
		Endpoints:   vars.Cfg.EtcdCommon.Endpoints,
		DialTimeout: time.Duration(vars.Cfg.EtcdCommon.DialTimeout),
	})
	if err != nil {
		return errors.Wrap(err, "can't init heartbeat etcd client")
	}
	h.client = cli

	leaseResp, err := h.client.Grant(context.Background(), h.leaseTTL)
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
		redo.Retry(time.Duration(vars.Cfg.EtcdCommon.RetryInterval), vars.Cfg.EtcdCommon.RetryNum, func() (bool, error) {
			ctx, cancel := context.WithTimeout(h.client.Ctx(), time.Duration(vars.Cfg.EtcdCommon.RWTimeout))
			_, err := h.client.Delete(ctx, nodePrefix()+h.lastNodeInfo.Addr())
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
	reConnect, reGrant := false, false

	clientCfg := clientv3.Config{
		Endpoints:   vars.Cfg.EtcdCommon.Endpoints,
		DialTimeout: time.Duration(vars.Cfg.EtcdCommon.DialTimeout),
	}

	for {
		select {
		case <-h.exitCh:
			level.Warn(vars.Logger).Log("msg", "keep lease for heartbeat exit!!!")
			return
		default:
		}

		if reConnect {
			level.Warn(vars.Logger).Log("msg", "reconnect for heartbeat")
			cli, err := clientv3.New(clientCfg)
			if err != nil {
				time.Sleep(2 * time.Second)
				continue
			}

			h.cmtx.Lock()
			if h.client != nil {
				h.client.Close()
			}
			h.client = cli
			h.cmtx.Unlock()

			reConnect = false
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
			if _, ok := err.(net.Error); ok || err == io.EOF {
				reConnect = true
			} else {
				reGrant = true
			}
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

		reConnect = true
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
		err = redo.Retry(time.Duration(vars.Cfg.EtcdCommon.RetryInterval), vars.Cfg.EtcdCommon.RetryNum, func() (bool, error) {
			ctx, cancel := context.WithTimeout(h.client.Ctx(), time.Duration(vars.Cfg.EtcdCommon.RWTimeout))
			_, er := h.client.Put(ctx, nodePrefix()+node.Addr(), string(b), clientv3.WithLease(h.leaseID))
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
				h.reportInfo(node)
			}
		case <-h.registerC:
			if node, err = h.f(); err == nil {
				h.reportInfo(node)
			}
		case <-h.exitCh:
			level.Warn(vars.Logger).Log("msg", "report host info for heartbeat exit!!!")
			return
		}
	}
}
