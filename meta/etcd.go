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
	"sync"
	"time"

	"github.com/baudtime/baudtime/util/redo"
	"github.com/baudtime/baudtime/vars"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

type ClientRefer struct {
	cli *clientv3.Client
	ref int32
	sync.Mutex
}

func (r *ClientRefer) Ref() (*clientv3.Client, error) {
	r.Lock()
	defer r.Unlock()

	if r.cli == nil {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   vars.Cfg.EtcdCommon.Endpoints,
			DialTimeout: time.Duration(vars.Cfg.EtcdCommon.DialTimeout),
		})
		if err != nil {
			return nil, err
		}
		r.cli = cli
	}

	r.ref++
	return r.cli, nil
}

func (r *ClientRefer) UnRef() {
	r.Lock()
	defer r.Unlock()

	r.ref--
	if r.ref == 0 && r.cli != nil {
		r.cli.Close()
		r.cli = nil
	}
}

type lease struct {
	day     uint64
	leaseID clientv3.LeaseID
	sync.Mutex
}

var (
	ErrKeyNotFound = errors.New("key not found in etcd")
	clientRef      ClientRefer
	leases         [366]lease
)

func getEtcdLease(day uint64) (clientv3.LeaseID, error) {
	idx := day % 366
	l := leases[idx]

	l.Lock()
	defer l.Unlock()

	if l.leaseID != clientv3.NoLease && l.day == day {
		return l.leaseID, nil
	}

	ttl := int64(vars.Cfg.Gateway.Route.RouteInfoTTL) / 1e9

	cli, err := clientRef.Ref()
	if err != nil {
		return -1, err
	}
	defer clientRef.UnRef()

	if l.leaseID != clientv3.NoLease {
		cli.Revoke(context.Background(), l.leaseID)
	}

	leaseResp, err := cli.Grant(context.Background(), ttl)
	if err != nil {
		return -1, err
	}

	l.day = day
	l.leaseID = leaseResp.ID

	return l.leaseID, nil
}

func exist(k string) (bool, error) {
	var resp *clientv3.GetResponse

	er := redo.Retry(time.Duration(vars.Cfg.EtcdCommon.RetryInterval), vars.Cfg.EtcdCommon.RetryNum, func() (bool, error) {
		cli, err := clientRef.Ref()
		if err != nil {
			return true, err
		}
		defer clientRef.UnRef()

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(2*vars.Cfg.EtcdCommon.RWTimeout))

		resp, err = cli.Get(ctx, k)
		cancel()
		if err != nil {
			return true, err
		}

		return false, nil
	})

	return resp != nil && len(resp.Kvs) > 0, er
}

func etcdGet(k string, v interface{}) error {
	return redo.Retry(time.Duration(vars.Cfg.EtcdCommon.RetryInterval), vars.Cfg.EtcdCommon.RetryNum, func() (bool, error) {
		cli, err := clientRef.Ref()
		if err != nil {
			return true, err
		}
		defer clientRef.UnRef()

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(vars.Cfg.EtcdCommon.RWTimeout))

		resp, err := cli.Get(ctx, k)
		cancel()
		if err != nil {
			return true, err
		}

		if len(resp.Kvs) == 0 {
			level.Warn(vars.Logger).Log("msg", "key not found in etcd", "key", k)
			return false, ErrKeyNotFound
		}

		if len(resp.Kvs) != 1 {
			return false, errors.Errorf("%v should not be multiple", k)
		}

		if s, ok := v.(*string); ok {
			*s = string(resp.Kvs[0].Value)
			return false, nil
		}

		err = json.Unmarshal(resp.Kvs[0].Value, v)
		if err != nil {
			return true, err
		}
		return false, nil
	})
}

func etcdGetWithPrefix(prefix string) (*clientv3.GetResponse, error) {
	var resp *clientv3.GetResponse
	er := redo.Retry(time.Duration(vars.Cfg.EtcdCommon.RetryInterval), vars.Cfg.EtcdCommon.RetryNum, func() (bool, error) {
		cli, err := clientRef.Ref()
		if err != nil {
			return true, err
		}
		defer clientRef.UnRef()

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(vars.Cfg.EtcdCommon.RWTimeout))

		resp, err = cli.Get(ctx, prefix, clientv3.WithPrefix())
		cancel()
		if err != nil {
			return true, err
		}

		if len(resp.Kvs) == 0 {
			level.Warn(vars.Logger).Log("msg", "keys with prefix not found in etcd", "prefix", prefix)
			return false, ErrKeyNotFound
		}

		return false, nil
	})
	return resp, er
}

func etcdPut(k string, v interface{}, leaseID clientv3.LeaseID) error {
	return redo.Retry(time.Duration(vars.Cfg.EtcdCommon.RetryInterval), vars.Cfg.EtcdCommon.RetryNum, func() (bool, error) {
		var (
			b   []byte
			err error
		)

		if s, ok := v.(*string); ok {
			b = []byte(*s)
		} else {
			b, err = json.Marshal(v)
			if err != nil {
				return false, err
			}
		}

		err = mutexRun(k, func(session *concurrency.Session) error {
			var er error

			cli := session.Client()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(vars.Cfg.EtcdCommon.RWTimeout))

			if leaseID == clientv3.NoLease {
				_, er = cli.Put(ctx, k, string(b))
			} else {
				_, er = cli.Put(ctx, k, string(b), clientv3.WithLease(leaseID))
			}
			cancel()
			return er
		})

		if err != nil {
			return true, err
		}
		return false, nil
	})
}

func mutexRun(lock string, f func(session *concurrency.Session) error) error {
	cli, err := clientRef.Ref()
	if err != nil {
		return err
	}
	defer clientRef.UnRef()

	session, err := concurrency.NewSession(cli, concurrency.WithTTL(20))
	if err != nil {
		return err
	}
	defer session.Close()

	l := concurrency.NewMutex(session, "/l/"+lock)

	ctx, cancel := context.WithTimeout(cli.Ctx(), 10*time.Second)
	err = l.Lock(ctx)
	cancel()
	if err != nil {
		return err
	}
	defer l.Unlock(context.Background())

	return f(session)
}
