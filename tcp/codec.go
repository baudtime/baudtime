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
	"encoding/binary"
	"github.com/baudtime/baudtime/msg"
	"github.com/baudtime/baudtime/msg/backend"
	"github.com/baudtime/baudtime/msg/gateway"
)

type MsgType uint8

const (
	//gateway
	GatewayAddRequestType MsgType = iota
	GatewayInstantQueryRequestType
	GatewayRangeQueryRequestType
	GatewayQueryResponseType
	GatewaySeriesLabelsRequestType
	GatewaySeriesLabelsResponseType
	GatewayLabelValuesRequestType
	//backend
	BackendAddRequestType
	BackendSelectRequestType
	BackendSelectResponseType
	BackendLabelValuesRequestType
	BackendSlaveOfCommandType
	BackendSyncHandshakeType
	BackendSyncHandshakeAckType
	BackendSyncHeartbeatType
	BackendSyncHeartbeatAckType
	BackendAdminCmdInfoType
	BackendAdminCmdJoinClusterType
	//other
	ConnCtrlType
	GeneralResponseType
	LabelValuesResponseType
	BackendAdminCmdLeftClusterType
	//error
	BadMsgType MsgType = 255
)

func Type(m msg.Message) MsgType {
	switch m.(type) {
	//gateway
	case *gateway.AddRequest:
		return GatewayAddRequestType
	case *gateway.InstantQueryRequest:
		return GatewayInstantQueryRequestType
	case *gateway.RangeQueryRequest:
		return GatewayRangeQueryRequestType
	case *gateway.QueryResponse:
		return GatewayQueryResponseType
	case *gateway.SeriesLabelsRequest:
		return GatewaySeriesLabelsRequestType
	case *gateway.SeriesLabelsResponse:
		return GatewaySeriesLabelsResponseType
	case *gateway.LabelValuesRequest:
		return GatewayLabelValuesRequestType
	//backend
	case *backend.AddRequest:
		return BackendAddRequestType
	case *backend.SelectRequest:
		return BackendSelectRequestType
	case *backend.SelectResponse:
		return BackendSelectResponseType
	case *backend.LabelValuesRequest:
		return BackendLabelValuesRequestType
	case *backend.SlaveOfCommand:
		return BackendSlaveOfCommandType
	case *backend.SyncHandshake:
		return BackendSyncHandshakeType
	case *backend.SyncHandshakeAck:
		return BackendSyncHandshakeAckType
	case *backend.SyncHeartbeat:
		return BackendSyncHeartbeatType
	case *backend.SyncHeartbeatAck:
		return BackendSyncHeartbeatAckType
	case *backend.AdminCmdInfo:
		return BackendAdminCmdInfoType
	case *backend.AdminCmdJoinCluster:
		return BackendAdminCmdJoinClusterType
	case *backend.AdminCmdLeftCluster:
		return BackendAdminCmdLeftClusterType
	//other
	case *msg.ConnCtrl:
		return ConnCtrlType
	case *msg.GeneralResponse:
		return GeneralResponseType
	case *msg.LabelValuesResponse:
		return LabelValuesResponseType
	}

	return BadMsgType
}

type MsgCodec struct {
	reusableGatewayAddReq    *gateway.AddRequest
	gatewayAddReqReusedCount int
	reusableBackendAddReq    *backend.AddRequest
	backendAddReqReusedCount int
}

func (codec *MsgCodec) Encode(msg Message, b []byte) (int, error) {
	raw := msg.GetRaw()
	written := 0

	b[written] = byte(Type(raw))
	written++

	n := binary.PutUvarint(b[written:written+binary.MaxVarintLen64], msg.Opaque)
	written += n

	if raw != nil {
		if cap(b)-written < raw.Msgsize() {
			return 0, MsgSizeOverflow
		}

		o, err := raw.MarshalMsg(b[written:written])
		if err != nil {
			return 0, err
		}
		written += len(o)
	}

	return written, nil
}

func (codec *MsgCodec) Decode(b []byte) (Message, error) {
	var (
		err error
		msg Message
	)

	//get message type
	msgType := MsgType(b[0])

	//get message opaque
	l := 1 + binary.MaxVarintLen64
	if len(b) < l {
		l = len(b)
	}
	opaque, n := binary.Uvarint(b[1:l])
	if n <= 0 {
		return msg, BadMsgFormat
	}

	//get message proto
	raw := codec.get(msgType)
	if raw != nil {
		_, err = raw.UnmarshalMsg(b[1+n:])
	}

	if err != nil {
		return msg, err
	}

	msg.SetOpaque(opaque)
	msg.SetRaw(raw)

	return msg, nil
}

func (codec *MsgCodec) get(t MsgType) msg.Message {
	switch t {
	//gateway
	case GatewayAddRequestType:
		if codec.reusableGatewayAddReq == nil || codec.gatewayAddReqReusedCount > 2048 {
			codec.reusableGatewayAddReq = new(gateway.AddRequest)
			codec.gatewayAddReqReusedCount = 0
		} else {
			codec.gatewayAddReqReusedCount++
		}
		return codec.reusableGatewayAddReq
	case GatewayInstantQueryRequestType:
		return new(gateway.InstantQueryRequest)
	case GatewayRangeQueryRequestType:
		return new(gateway.RangeQueryRequest)
	case GatewayQueryResponseType:
		return new(gateway.QueryResponse)
	case GatewaySeriesLabelsRequestType:
		return new(gateway.SeriesLabelsRequest)
	case GatewaySeriesLabelsResponseType:
		return new(gateway.SeriesLabelsResponse)
	case GatewayLabelValuesRequestType:
		return new(gateway.LabelValuesRequest)
	//backend
	case BackendAddRequestType:
		if codec.reusableBackendAddReq == nil || codec.backendAddReqReusedCount > 2048 {
			codec.reusableBackendAddReq = new(backend.AddRequest)
			codec.backendAddReqReusedCount = 0
		} else {
			codec.backendAddReqReusedCount++
		}
		return codec.reusableBackendAddReq
	case BackendSelectRequestType:
		return new(backend.SelectRequest)
	case BackendSelectResponseType:
		return new(backend.SelectResponse)
	case BackendLabelValuesRequestType:
		return new(backend.LabelValuesRequest)
	case BackendSlaveOfCommandType:
		return new(backend.SlaveOfCommand)
	case BackendSyncHandshakeType:
		return new(backend.SyncHandshake)
	case BackendSyncHandshakeAckType:
		return new(backend.SyncHandshakeAck)
	case BackendSyncHeartbeatType:
		return new(backend.SyncHeartbeat)
	case BackendSyncHeartbeatAckType:
		return new(backend.SyncHeartbeatAck)
	case BackendAdminCmdInfoType:
		return new(backend.AdminCmdInfo)
	case BackendAdminCmdJoinClusterType:
		return new(backend.AdminCmdJoinCluster)
	case BackendAdminCmdLeftClusterType:
		return new(backend.AdminCmdLeftCluster)
	//other
	case ConnCtrlType:
		return new(msg.ConnCtrl)
	case GeneralResponseType:
		return new(msg.GeneralResponse)
	case LabelValuesResponseType:
		return new(msg.LabelValuesResponse)
	}

	return nil
}
