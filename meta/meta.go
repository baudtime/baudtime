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
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/baudtime/baudtime/msg"
	backendmsg "github.com/baudtime/baudtime/msg/backend"
	"github.com/baudtime/baudtime/tcp"
	"github.com/baudtime/baudtime/vars"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

type Shard struct {
	Master      *Node
	Slaves      []*Node
	failovering uint32
}

//meta's responsibility is to provide our necessary data
type meta struct {
	sync.Once
	shards     unsafe.Pointer //point to a map[string]*Shard
	routeInfos *sync.Map
	refreshing uint32
}

func (m *meta) getShardIDs(metricName string, day uint64) ([]string, string, error) {
	shardGroup, shardGrpRouteK, found := m.getShardIDsFromCache(metricName, day)
	if found {
		return shardGroup, shardGrpRouteK, nil
	}

	var err error

	routeInfo := m.getRouteInfoFromCache(metricName)
	routeInfo.Lock()

	shardGroup, shardGrpRouteK, found = m.getShardIDsFromCache(metricName, day)
	if !found {
		shardGroup, shardGrpRouteK, err = m.getShardIDsFromEtcd(metricName, day)
		if err == nil {
			routeInfo.ShardGrpRouteK = shardGrpRouteK
			routeInfo.Put(day, shardGroup)
		}
	}

	routeInfo.Unlock()

	return shardGroup, shardGrpRouteK, err
}

func (m *meta) getRouteInfoFromCache(metricName string) *RouteInfo {
	routeInfo, ok := m.routeInfos.Load(metricName)
	if !ok {
		routeInfo, _ = m.routeInfos.LoadOrStore(metricName, NewRouteInfo(metricName))
	}
	return routeInfo.(*RouteInfo)
}

func (m *meta) getShardIDsFromCache(metricName string, day uint64) ([]string, string, bool) {
	routeInfo := m.getRouteInfoFromCache(metricName)

	shardGroup, found := routeInfo.Get(day)
	return shardGroup, routeInfo.ShardGrpRouteK, found
}

func (m *meta) getShardIDsFromEtcd(metricName string, day uint64) ([]string, string, error) {
	level.Info(vars.Logger).Log("msg", "get shards from etcd", "metric", metricName, "day", day)

	sGrpRouteKey := ""
	err := etcdGet(sGrpRoutePrefix()+metricName, &sGrpRouteKey)
	if err != nil && err != ErrKeyNotFound {
		return nil, "", err
	}

	shardGroup := make([]string, 0, vars.Cfg.Gateway.Route.ShardGroupCap)

	key := routeInfoPrefix() + metricName + "/" + strconv.FormatUint(day, 10)
	err = etcdGet(key, &shardGroup)
	if err == nil {
		return shardGroup, sGrpRouteKey, nil
	}

	if err != ErrKeyNotFound {
		return nil, "", err
	}

	masters, err := GetMasters()
	if err != nil {
		return nil, "", err
	}

	if len(masters) < vars.Cfg.Gateway.Route.ShardGroupCap {
		return nil, "", errors.Errorf("not enough shards to init %v", key)
	}

	for i := 0; i < vars.Cfg.Gateway.Route.ShardGroupCap && i < len(masters); i++ {
		shardGroup = append(shardGroup, masters[i].ShardID)
	}

	leaseID, err := getEtcdLease(day)
	if err != nil {
		return nil, "", err
	}

	err = etcdPut(key, shardGroup, leaseID)
	if err != nil {
		return nil, "", err
	}

	return shardGroup, sGrpRouteKey, nil
}

func (m *meta) Refresh() error {
	if !atomic.CompareAndSwapUint32(&m.refreshing, 0, 1) {
		return nil
	}
	defer atomic.StoreUint32(&m.refreshing, 0)

	shards := make(map[string]*Shard)

	nodes, err := GetNodes(false)
	if err != nil {
		return err
	}

	for _, node := range nodes {
		n := node

		shard, found := shards[n.ShardID]
		if !found {
			shard = new(Shard)
			shards[n.ShardID] = shard
		}

		if n.MasterIP == "" && n.MasterPort == "" {
			shard.Master = &n
		} else {
			shard.Slaves = append(shard.Slaves, &n)
		}
	}

	atomic.StorePointer(&m.shards, (unsafe.Pointer)(&shards))
	return nil
}

func (m *meta) GetShard(shardID string) (shard *Shard, found bool) {
	shards := (*map[string]*Shard)(atomic.LoadPointer(&m.shards))
	shard, found = (*shards)[shardID]
	return
}

func (m *meta) watch() {
	m.Do(func() {
		go func() {
			cli, err := clientv3.New(clientv3.Config{
				Endpoints:   vars.Cfg.EtcdCommon.Endpoints,
				DialTimeout: time.Duration(vars.Cfg.EtcdCommon.DialTimeout),
			})
			if err != nil {
				level.Error(vars.Logger).Log("msg", "failed to connect to etcd", "err", err)
				os.Exit(1)
			}
			defer cli.Close()

			rch := cli.Watch(context.Background(), routeInfoPrefix(), clientv3.WithPrefix())
			gch := cli.Watch(context.Background(), sGrpRoutePrefix(), clientv3.WithPrefix())
			nch := cli.Watch(context.Background(), nodePrefix(), clientv3.WithPrefix(), clientv3.WithPrevKV())

			var wresp clientv3.WatchResponse

			level.Info(vars.Logger).Log("msg", "i am watching etcd events now")
			for {
				select {
				case wresp = <-rch:
					for _, ev := range wresp.Events {
						level.Warn(vars.Logger).Log(
							"msg", "get etcd event",
							"type", ev.Type,
							"key", ev.Kv.Key,
							"value", ev.Kv.Value,
						)

						strArray := strings.Split(string(ev.Kv.Key), "/")
						metricName := strings.TrimPrefix(strArray[0], routeInfoPrefix())
						day, err := strconv.ParseUint(strArray[1], 10, 0)
						if err != nil {
							continue
						}

						if ev.Type == mvccpb.DELETE {
							routeInfo := m.getRouteInfoFromCache(metricName)
							routeInfo.Delete(day)
							if day == routeInfo.Timeline {
								m.routeInfos.Delete(metricName)
							}
						} else {
							shardGroup := make([]string, 0, vars.Cfg.Gateway.Route.ShardGroupCap)
							if err = json.Unmarshal(ev.Kv.Value, &shardGroup); err == nil {
								routeInfo := m.getRouteInfoFromCache(metricName)
								routeInfo.Put(day, shardGroup)
							}
						}
					}
				case wresp = <-gch:
					for _, ev := range wresp.Events {
						level.Warn(vars.Logger).Log(
							"msg", "get etcd event",
							"type", ev.Type,
							"key", ev.Kv.Key,
							"value", ev.Kv.Value,
						)

						metricName := strings.TrimPrefix(string(ev.Kv.Key), sGrpRoutePrefix())
						routeInfo := m.getRouteInfoFromCache(metricName)
						if ev.Type == mvccpb.DELETE {
							routeInfo.ShardGrpRouteK = ""
						} else {
							routeInfo.ShardGrpRouteK = string(ev.Kv.Value)
						}
					}
				case wresp = <-nch:
					for _, ev := range wresp.Events {
						if ev.Type == mvccpb.DELETE && ev.PrevKv != nil {
							level.Warn(vars.Logger).Log(
								"msg", "get etcd event",
								"type", ev.Type,
								"key", ev.Kv.Key,
								"value", ev.Kv.Value,
								"preKey", ev.PrevKv.Key,
								"preValue", ev.PrevKv.Value,
							)

							var node Node
							if err = json.Unmarshal(ev.PrevKv.Value, &node); err == nil {
								FailoverIfNeeded(node.ShardID)
							}
						}
					}
					m.Refresh()
				}
			}
		}()
	})
}

var globalMeta *meta

func Watch() error {
	m := &meta{
		routeInfos: new(sync.Map),
	}

	err := m.Refresh()
	if err != nil {
		return err
	}

	m.watch()
	globalMeta = m

	level.Info(vars.Logger).Log("msg", "watching nodes")
	return nil
}

func AllShards() map[string]*Shard {
	shards := (*map[string]*Shard)(atomic.LoadPointer(&globalMeta.shards))
	return *shards
}

func GetMaster(shardID string) *Node {
	shard, found := globalMeta.GetShard(shardID)

	if !found || shard == nil {
		return nil
	}

	return shard.Master
}

func GetSlaves(shardID string) []*Node {
	shard, found := globalMeta.GetShard(shardID)

	if !found || shard == nil {
		return nil
	}

	return shard.Slaves
}

func FailoverIfNeeded(shardID string) {
	shard, found := globalMeta.GetShard(shardID)
	if !found {
		return
	}

	if shard.Master == nil {
		globalMeta.Refresh()
		if shard.Master != nil {
			return
		}
	} else if shard.Master.MayOnline() {
		return
	}

	buf := make([]byte, tcp.MaxMsgSize)
	var msgCodec tcp.MsgCodec

	n, err := msgCodec.Encode(tcp.Message{Message: &backendmsg.SlaveOfCommand{}}, buf) //slaveof no one
	if err != nil {
		level.Error(vars.Logger).Log("err", err)
		return
	}

	if !atomic.CompareAndSwapUint32(&shard.failovering, 0, 1) {
		return
	}
	defer atomic.StoreUint32(&shard.failovering, 0)

	failoverErr := mutexRun("failover", func(session *concurrency.Session) error {
		master := GetMaster(shardID)
		if master != nil && (shard.Master == nil || master.Addr() != shard.Master.Addr()) { //already failover by other gateway
			return nil
		}

		slaves := GetSlaves(shardID)
		if len(slaves) == 0 {
			return errors.New("no available slave to failover")
		}

		chosen := slaves[0]
		if master != nil {
			for _, slave := range slaves {
				if slave.IDC == master.IDC {
					chosen = slave
				}
			}
		}

		slaveConn, err := tcp.Connect(chosen.Addr())
		if err != nil {
			return err
		}
		defer slaveConn.Close()

		err = slaveConn.WriteMsg(buf[:n])
		if err != nil {
			return err
		}

		level.Warn(vars.Logger).Log("msg", "failover triggered", "shard", shardID, "chosen", chosen.Addr())

		c := make(chan struct{})
		go func() {
			defer close(c)

			nn, er := slaveConn.ReadMsg(buf)
			if er != nil {
				return
			}

			reply, er := msgCodec.Decode(buf[:nn])
			if raw := reply.GetRaw(); er == nil && raw != nil {
				reply, ok := raw.(*msg.GeneralResponse)
				if !ok {
					return
				}

				if reply.Status != msg.StatusCode_Succeed {
					err = errors.New(reply.Message)
				} else {
					level.Warn(vars.Logger).Log("msg", "failover succeed", "shard", shardID, "chosen", chosen.Addr())
				}
			}
		}()

		select {
		case <-c:
		case <-time.After(15 * time.Second):
		}

		globalMeta.Refresh()

		return err
	})

	if failoverErr != nil {
		level.Error(vars.Logger).Log("msg", "error occurred when failover ", "shard", shardID, "err", failoverErr)
	}
}
