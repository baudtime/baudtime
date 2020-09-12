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
	"github.com/baudtime/baudtime/msg"
	"github.com/pkg/errors"
)

var (
	EmptyMsg        = Message{}
	MsgSizeOverflow = errors.New("message size overflow")
	BadMsgFormat    = errors.New("bad message format")
)

type Message struct {
	msg.Message
	Opaque uint64
}

func (msg *Message) GetOpaque() uint64 {
	return msg.Opaque
}

func (msg *Message) SetOpaque(opaque uint64) {
	msg.Opaque = opaque
}

func (msg *Message) GetRaw() msg.Message {
	return msg.Message
}

func (msg *Message) SetRaw(raw msg.Message) {
	msg.Message = raw
}

func (msg *Message) SizeOfRaw() int {
	if msg.Message != nil {
		return msg.Message.Msgsize()
	}
	return 0
}
