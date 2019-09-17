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
	"encoding/binary"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baudtime/baudtime/msg"
	"github.com/baudtime/baudtime/tcp"
	ts "github.com/baudtime/baudtime/util/time"
	"github.com/baudtime/baudtime/vars"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

type sampleFeed struct {
	h           *handOff
	hmu         sync.Mutex
	flushing    uint32
	closed      uint32
	createdTime int64
}

func NewSampleFeed(addr string, size int64) *sampleFeed {
	feed := &sampleFeed{
		h: &handOff{
			buf:  make([]byte, size),
			size: size,
			addr: addr,
		},
		createdTime: ts.FromTime(time.Now()),
	}
	feed.h.reconnect()

	return feed
}

func (feed *sampleFeed) Write(msg []byte) (err error) {
	if !feed.isClosed() {
		buf := GetBytes()
		binary.BigEndian.PutUint32(buf[:4], uint32(len(msg)))

		feed.hmu.Lock()
		if _, err = feed.h.Write(buf[:4]); err == nil {
			_, err = feed.h.Write(msg)
		}
		feed.hmu.Unlock()

		PutBytes(buf)

	}
	return
}

func (feed *sampleFeed) FlushIfNeeded() {
	if atomic.LoadUint32(&feed.flushing) == 1 {
		return
	}

	if feed.h.Buffered() <= 0 {
		return
	}

	level.Warn(vars.Logger).Log("msg", "flush feeds")

	var toFlush *handOff

	feed.hmu.Lock()
	if feed.h.Buffered() > 0 {
		toFlush = feed.h
		feed.h = &handOff{
			buf:  make([]byte, toFlush.size),
			size: toFlush.size,
			addr: toFlush.addr,
		}
		feed.h.reconnect()
	}
	feed.hmu.Unlock()

	if toFlush != nil {
		atomic.CompareAndSwapUint32(&feed.flushing, 0, 1)
		defer atomic.StoreUint32(&feed.flushing, 0)

		var err error

		if toFlush.cErr != nil {
			err = toFlush.reconnect()
			level.Warn(vars.Logger).Log("msg", "reconnect to slave")
		}

		batchSize := int64(os.Getpagesize())

		for err == nil && toFlush.Buffered() > 0 {
			err = toFlush.Flush(batchSize)
		}

		if err == nil {
			toFlush.Close()
		}
	}
}

func (feed *sampleFeed) Close() {
	if atomic.CompareAndSwapUint32(&feed.closed, 0, 1) {
		if feed.h.Conn != nil {
			feed.h.Conn.Close()
		}
	}
	return
}

func (feed *sampleFeed) isClosed() bool {
	return atomic.LoadUint32(&feed.closed) == 1
}

type handOff struct {
	buf      []byte
	head     int64
	readable int64
	size     int64
	addr     string
	cErr     error
	*tcp.Conn
	bytesWriten   uint64
	bytesBuffered uint64
}

func (h *handOff) Buffered() int64 { return atomic.LoadInt64(&h.readable) }

func (h *handOff) Write(p []byte) (nn int, err error) {
	for len(p) > 0 {
		var n int
		if h.readable == 0 && h.cErr == nil && h.Conn != nil {
			// Empty buffer and connection has no error.
			// Write directly from p to avoid copy.
			n, err = h.Conn.Write(p)
			h.bytesWriten += uint64(n)
			if err != nil {
				h.cErr = err
				return n, err
			}
		} else {
			writeCapacity := h.size - h.readable
			if writeCapacity <= 0 {
				// full already
				return nn, io.ErrShortWrite
			}

			if int64(len(p)) > writeCapacity {
				err = io.ErrShortWrite
				// leave err set and
				// keep going, write what we can.
			}

			writeStart := (h.head + h.readable) % h.size
			upperLim := min(writeStart+writeCapacity, h.size)

			n = copy(h.buf[writeStart:upperLim], p)
			h.readable += int64(n)

			h.bytesBuffered += uint64(n)
			//try flush???
		}

		nn += n
		p = p[n:]
	}

	return
}

func (h *handOff) Flush(size int64) error {
	if h.cErr != nil {
		return h.cErr
	}
	if h.Conn == nil {
		return errors.New("conn not initialed")
	}
	if h.readable == 0 {
		return nil
	}

	if size > h.readable {
		size = h.readable
	}

	var n = 0
	var err error

	extent := h.head + size
	if extent <= h.size {
		n, err = h.Conn.Write(h.buf[h.head:extent])
	} else {
		n, err = h.Conn.Write(h.buf[h.head:h.size])
		if int64(n) < size && err == nil {
			nn := 0
			nn, err = h.Conn.Write(h.buf[0:(extent % h.size)])
			n += nn
		}
	}

	if n > 0 {
		h.readable -= int64(n)
		h.head = (h.head + int64(n)) % h.size
	}

	if err != nil {
		h.cErr = err
	}
	return err
}

func (h *handOff) reconnect() error {
	if h.Conn != nil {
		h.Conn.Close()
		h.Conn = nil
	}

	var msgCodec tcp.MsgCodec

	closeWrite := &msg.ConnCtrl{msg.CtrlCode_CloseWrite}
	buf := make([]byte, 1+binary.MaxVarintLen64+closeWrite.Msgsize())

	n, err := msgCodec.Encode(tcp.Message{Message: closeWrite}, buf)
	if err != nil {
		h.cErr = err
		return err
	}

	conn, err := tcp.Connect(h.addr)
	if err != nil {
		h.cErr = err
		return err
	}

	err = conn.WriteMsg(buf[:n])
	if err != nil {
		h.cErr = err
		return err
	}

	err = conn.Flush()
	if err != nil {
		h.cErr = err
		return err
	}

	conn.CloseRead()

	h.cErr = nil
	h.Conn = conn

	return nil
}

func min(a, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

var bytesPool = &sync.Pool{}

func GetBytes() []byte {
	v := bytesPool.Get()
	if v != nil {
		return v.([]byte)
	}
	return make([]byte, 4)
}

func PutBytes(b []byte) {
	bytesPool.Put(b)
}
