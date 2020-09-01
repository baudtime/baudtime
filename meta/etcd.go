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
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"google.golang.org/grpc"
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
			Endpoints:   vars.Cfg.Etcd.Endpoints,
			DialTimeout: time.Duration(vars.Cfg.Etcd.DialTimeout),
			DialOptions: []grpc.DialOption{grpc.WithBlock()},
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

var (
	ErrKeyNotFound = errors.New("key not found in etcd")
	clientRef      ClientRefer
)

func exist(k string) (bool, error) {
	var resp *clientv3.GetResponse

	er := redo.Retry(time.Duration(vars.Cfg.Etcd.RetryInterval), vars.Cfg.Etcd.RetryNum, func() (bool, error) {
		cli, err := clientRef.Ref()
		if err != nil {
			return true, err
		}
		defer clientRef.UnRef()

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(2*vars.Cfg.Etcd.RWTimeout))

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
	return redo.Retry(time.Duration(vars.Cfg.Etcd.RetryInterval), vars.Cfg.Etcd.RetryNum, func() (bool, error) {
		cli, err := clientRef.Ref()
		if err != nil {
			return true, err
		}
		defer clientRef.UnRef()

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(vars.Cfg.Etcd.RWTimeout))

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
	er := redo.Retry(time.Duration(vars.Cfg.Etcd.RetryInterval), vars.Cfg.Etcd.RetryNum, func() (bool, error) {
		cli, err := clientRef.Ref()
		if err != nil {
			return true, err
		}
		defer clientRef.UnRef()

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(vars.Cfg.Etcd.RWTimeout))

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
	return redo.Retry(time.Duration(vars.Cfg.Etcd.RetryInterval), vars.Cfg.Etcd.RetryNum, func() (bool, error) {
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
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(vars.Cfg.Etcd.RWTimeout))

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

func etcdDel(k string) error {
	return redo.Retry(time.Duration(vars.Cfg.Etcd.RetryInterval), vars.Cfg.Etcd.RetryNum, func() (bool, error) {
		cli, err := clientRef.Ref()
		if err != nil {
			return true, err
		}
		defer clientRef.UnRef()

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(vars.Cfg.Etcd.RWTimeout))

		_, err = cli.Delete(ctx, k)
		cancel()

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

	l := NewMutex(session, "/l/"+lock)

	err = l.TryLock(cli.Ctx())
	if err != nil {
		return errors.Wrap(err, "can't acquire lock")
	}
	defer l.Unlock(cli.Ctx())

	return f(session)
}
