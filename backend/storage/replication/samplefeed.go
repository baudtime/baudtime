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
	"github.com/baudtime/baudtime/tcp/client"
	ts "github.com/baudtime/baudtime/util/time"
	"github.com/baudtime/baudtime/vars"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type sampleFeed struct {
	handOff     [][]byte
	handOffSize int
	handOffCap  int
	handOffMtx  sync.Mutex

	conns       client.ConnPool
	connRefused uint32

	closed      uint32
	createdTime int64
}

func NewSampleFeed(addr string, cap int) *sampleFeed {
	return &sampleFeed{
		handOff:     make([][]byte, 0, 1024),
		handOffCap:  cap,
		conns:       client.NewHostConnPool(vars.Cfg.Storage.Replication.SampleFeedConnsNum, addr, true),
		createdTime: ts.FromTime(time.Now()),
	}
}

func (feed *sampleFeed) Write(msg []byte) (err error) {
	if feed.isClosed() {
		return nil
	}

	if atomic.LoadUint32(&feed.connRefused) == 1 {
		return feed.writeToHandOff(msg)
	} else {
		return feed.writeToConn(msg)
	}
}

func (feed *sampleFeed) writeToHandOff(msg []byte) (err error) {
	bak := make([]byte, len(msg))
	copy(bak, msg)

	feed.handOffMtx.Lock()
	if feed.handOffSize+len(msg) > feed.handOffCap {
		err = io.ErrShortWrite
	} else {
		feed.handOff = append(feed.handOff, bak)
		feed.handOffSize += len(msg)
	}
	feed.handOffMtx.Unlock()

	return
}

func (feed *sampleFeed) writeToConn(msg []byte) (err error) {
	c, err := feed.conns.GetConn()
	if err != nil {
		if connRefused(err) {
			atomic.StoreUint32(&feed.connRefused, 1)
		}
		return err
	}

	err = c.WriteMsg(msg)
	if err != nil {
		feed.conns.Destroy(c)
		return err
	}
	return nil
}

func (feed *sampleFeed) FlushIfNeeded() {
	if atomic.LoadUint32(&feed.connRefused) == 0 {
		return
	}

	p := 0

	for {
		feed.handOffMtx.Lock()

		for i := 0; i < 512 && p < len(feed.handOff); i++ {
			feed.writeToConn(feed.handOff[p])
			feed.handOff[p] = nil
			p++
		}

		if p == len(feed.handOff) {
			feed.handOff = feed.handOff[:0]
			atomic.StoreUint32(&feed.connRefused, 0)

			feed.handOffMtx.Unlock()
			return
		}

		feed.handOffMtx.Unlock()

		runtime.GC()
	}
}

func (feed *sampleFeed) Close() {
	if atomic.CompareAndSwapUint32(&feed.closed, 0, 1) {
		feed.conns.Close()
	}
}

func (feed *sampleFeed) isClosed() bool {
	return atomic.LoadUint32(&feed.closed) == 1
}

func connRefused(err error) bool {
	netErr, ok := err.(net.Error)
	if !ok {
		return false
	}

	if netErr.Timeout() {
		return false
	}

	opErr, ok := netErr.(*net.OpError)
	if !ok {
		return false
	}

	switch t := opErr.Err.(type) {
	case *net.DNSError:
		return false
	case *os.SyscallError:
		if errno, ok := t.Err.(syscall.Errno); ok {
			return errno == syscall.ECONNREFUSED
		}
	}

	return false
}
