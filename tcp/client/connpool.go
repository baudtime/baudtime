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

package client

import (
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type ConnPool interface {
	GetConn() (*Conn, error)
	Destroy(c *Conn) error
	Close() error
}

type serviceConn struct {
	c        *Conn
	lastUsed time.Time
}

type ServiceConnPool struct {
	conns           *sync.Map
	addrProv        ServiceAddrProvider
	new             func(address string, writeOnly bool) (*Conn, error)
	writeOnly       bool
	onConnect       func(*Conn)
	onClose         func(*Conn)
	stopIdleCheckCh chan struct{}
}

func NewServiceConnPool(addrProvider ServiceAddrProvider, writeOnly bool) ConnPool {
	pool := &ServiceConnPool{
		conns:     new(sync.Map),
		addrProv:  addrProvider,
		new:       newConn,
		writeOnly: writeOnly,
	}
	pool.idleCheck()
	return pool
}

func (pool *ServiceConnPool) GetConn() (*Conn, error) {
	address, err := pool.addrProv.GetServiceAddr()
	if err != nil {
		return nil, err
	}

	sc, found := pool.conns.Load(address)
	if !found {
		newc, err := pool.new(address, pool.writeOnly)
		if err != nil {
			pool.addrProv.ServiceDown(address)
			return nil, err
		}

		var loaded bool
		sc, loaded = pool.conns.LoadOrStore(address, &serviceConn{c: newc, lastUsed: time.Now()})

		if loaded {
			newc.Close()
		} else {
			if pool.onConnect != nil {
				pool.onConnect(newc)
			}
		}
	} else {
		sc.(*serviceConn).lastUsed = time.Now()
	}
	return sc.(*serviceConn).c, nil
}

func (pool *ServiceConnPool) Destroy(c *Conn) error {
	pool.conns.Delete(c.address)
	if pool.onClose != nil {
		pool.onClose(c)
	}
	return c.Close()
}

func (pool *ServiceConnPool) Close() error {
	close(pool.stopIdleCheckCh)
	var multiErr error
	pool.conns.Range(func(key, value interface{}) bool {
		err := value.(*serviceConn).c.Close()
		if err != nil {
			multiErr = multierr.Append(multiErr, err)
		}
		return true
	})
	pool.conns = nil
	pool.addrProv.StopWatch()
	return multiErr
}

func (pool *ServiceConnPool) idleCheck() {
	go func() {
		maxIdle := 5 * time.Minute

		ticker := time.NewTicker(maxIdle)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				pool.conns.Range(func(key, value interface{}) bool {
					if time.Since(value.(*serviceConn).lastUsed) > maxIdle {
						value.(*serviceConn).c.Close()
						pool.conns.Delete(key)
					}
					return true
				})
			case <-pool.stopIdleCheckCh:
				return
			}
		}
	}()
}

type HostConnPool struct {
	//do not use sync.Pool
	iter    uint64
	size    int
	conns   []*Conn
	address string

	new       func(address string, writeOnly bool) (*Conn, error)
	writeOnly bool

	onConnect func(*Conn)
	onClose   func(*Conn)
}

func NewHostConnPool(connNum int, address string, writeOnly bool) ConnPool {
	return &HostConnPool{
		conns:     make([]*Conn, connNum),
		size:      connNum,
		address:   address,
		new:       newConn,
		writeOnly: writeOnly,
	}
}

func (pool *HostConnPool) GetConn() (*Conn, error) {
	iter := atomic.AddUint64(&pool.iter, 1) % uint64(pool.size)
	c := (*Conn)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&pool.conns[iter]))))

	if c != nil && !c.Closed() {
		return c, nil
	}

	newc, err := pool.new(pool.address, pool.writeOnly)
	if err != nil {
		return nil, err
	}
	if pool.onConnect != nil {
		pool.onConnect(newc)
	}

	if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&pool.conns[iter])), unsafe.Pointer(c), unsafe.Pointer(newc)) {
		c = newc
	} else {
		newc.Close()
		c = (*Conn)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&pool.conns[iter]))))
	}

	return c, nil
}

func (pool *HostConnPool) Destroy(c *Conn) (err error) {
	if pool.onClose != nil {
		pool.onClose(c)
	}
	return c.Close()
}

func (pool *HostConnPool) Close() error {
	var multiErr error
	for _, c := range pool.conns {
		if c != nil && !c.Closed() {
			if err := c.Close(); err != nil {
				multiErr = multierr.Append(multiErr, err)
			}
		}
	}
	return multiErr
}

var dummyErr error = errors.New("dummy conn, not real")

type dummyPool struct{}

func (pool dummyPool) GetConn() (*Conn, error) {
	return nil, dummyErr
}

func (pool dummyPool) Destroy(c *Conn) (err error) {
	return nil
}

func (pool dummyPool) Close() error {
	return nil
}
