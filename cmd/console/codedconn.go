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

package main

import (
	"github.com/baudtime/baudtime/msg"
	"github.com/baudtime/baudtime/tcp"
)

const MaxMsgSize int = 1e7

type CodedConn struct {
	codec tcp.MsgCodec
	*tcp.Conn
	encBuf []byte
}

func NewCodedConn(address string) (*CodedConn, error) {
	c, err := tcp.Connect(address)
	if err != nil {
		return nil, err
	}

	return &CodedConn{
		Conn:   c,
		encBuf: make([]byte, MaxMsgSize),
	}, nil
}

func (c *CodedConn) WriteRaw(msg msg.Message) error {
	n, err := c.codec.Encode(tcp.Message{
		Message: msg,
	}, c.encBuf)

	if err != nil {
		return err
	}
	err = c.WriteMsg(c.encBuf[:n])
	if err != nil {
		return err
	}

	return c.Flush()
}

func (c *CodedConn) ReadRaw() (msg.Message, error) {
	msgBytes, err := c.ReadMsg()
	if err != nil {
		return tcp.EmptyMsg, err
	}

	m, err := c.codec.Decode(msgBytes)
	return m.Message, err
}
