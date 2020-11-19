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

package tcp

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"os"
	"syscall"
	"time"

	"golang.org/x/time/rate"
)

type Conn struct {
	*bufio.Reader
	*bufio.Writer
	raw         *net.TCPConn
	rBuf        []byte
	rlBuf       []byte
	wlBuf       []byte
	rateLimiter *rate.Limiter
}

func NewConn(c *net.TCPConn) *Conn {
	return &Conn{
		Reader: bufio.NewReaderSize(c, 6*1e5), // We make a buffered Reader & Writer to reduce syscalls.
		Writer: bufio.NewWriterSize(c, 2*1e5),
		raw:    c,
		rBuf:   make([]byte, 1e6),
		rlBuf:  make([]byte, 4),
		wlBuf:  make([]byte, 4),
	}
}

func newConnWithRate(c *net.TCPConn, rateLimiter *rate.Limiter) *Conn {
	nc := NewConn(c)
	nc.rateLimiter = rateLimiter // Do not juju rate limiter
	return nc
}

func Connect(address string) (*Conn, error) {
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}

	c, err := net.DialTCP("tcp4", nil, addr)
	if err != nil {
		return nil, err
	}

	c.SetNoDelay(true)
	c.SetKeepAlive(true)
	c.SetKeepAlivePeriod(60 * time.Second)

	return NewConn(c), nil
}

func (c *Conn) ReadMsg() ([]byte, error) {
	_, err := io.ReadFull(c.Reader, c.rlBuf)
	if err != nil {
		return nil, err
	}

	//read message length
	msgLen := int(binary.BigEndian.Uint32(c.rlBuf))
	if msgLen < 2 { // one byte is type, one byte is at least for opaque
		return nil, io.ErrUnexpectedEOF
	}

	if c.rateLimiter != nil {
		now := time.Now()
		time.Sleep(c.rateLimiter.ReserveN(now, 4+msgLen).DelayFrom(now))
	}

	rBuf := c.rBuf
	if cap(rBuf) < msgLen {
		rBuf = make([]byte, msgLen)
	}

	_, err = io.ReadFull(c.Reader, rBuf[:msgLen])
	if err != nil {
		return nil, err
	}

	return rBuf[:msgLen], nil
}

func (c *Conn) WriteMsg(msg []byte) error {
	binary.BigEndian.PutUint32(c.wlBuf[:4], uint32(len(msg)))

	//write message length
	_, err := c.Writer.Write(c.wlBuf[:4])
	if err != nil {
		return err
	}

	_, err = c.Writer.Write(msg)
	return err
}

func (c *Conn) CloseRead() error {
	return c.raw.CloseRead()
}

func (c *Conn) CloseWrite() error {
	c.Writer.Flush()
	return c.raw.CloseWrite()
}

func (c *Conn) Close() error {
	c.Writer.Flush()
	return c.raw.Close()
}

type readWriter struct {
	fd int
	f  *os.File
}

func (rw *readWriter) Read(p []byte) (int, error) {
	n, err := syscall.Read(rw.fd, p)
	if err != nil && err == syscall.EAGAIN {
		return 0, nil
	}

	if n == 0 && err == nil {
		return 0, io.EOF
	}

	if n < 0 {
		return 0, io.EOF //reset by peer
	}

	return n, err
}

func (rw *readWriter) Write(p []byte) (n int, err error) {
	var nn int
	for {
		n, err := syscall.Write(rw.fd, p[nn:])
		if n > 0 {
			nn += n
		}
		if nn == len(p) {
			return nn, err
		}
		if err == syscall.EAGAIN {
			return 0, io.EOF
		}
		if err != nil {
			return nn, err
		}
		if n == 0 {
			return nn, io.ErrUnexpectedEOF
		}
	}
}
