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

package storage

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/baudtime/baudtime/meta"
	backendmsg "github.com/baudtime/baudtime/msg/backend"
	"github.com/baudtime/baudtime/util"
	tm "github.com/baudtime/baudtime/util/time"
)

type OPStat struct {
	SucceedSel      uint64
	FailedSel       uint64
	SucceedLVals    uint64
	FailedLVals     uint64
	ReceivedAdd     uint64
	SucceedAdd      uint64
	FailedAdd       uint64
	OutOfOrder      uint64
	AmendSample     uint64
	OutOfBounds     uint64
	FailedCommit    uint64
	LastOutOfOrder  int64
	LastAmendSample int64
	LastOutOfBounds int64
}

func (stat *OPStat) Reset() {
	atomic.StoreUint64(&stat.SucceedSel, 0)
	atomic.StoreUint64(&stat.FailedSel, 0)
	atomic.StoreUint64(&stat.SucceedLVals, 0)
	atomic.StoreUint64(&stat.FailedLVals, 0)
	atomic.StoreUint64(&stat.ReceivedAdd, 0)
	atomic.StoreUint64(&stat.SucceedAdd, 0)
	atomic.StoreUint64(&stat.FailedAdd, 0)
	atomic.StoreUint64(&stat.OutOfOrder, 0)
	atomic.StoreUint64(&stat.AmendSample, 0)
	atomic.StoreUint64(&stat.OutOfBounds, 0)
	atomic.StoreUint64(&stat.FailedCommit, 0)
	atomic.StoreInt64(&stat.LastOutOfOrder, 0)
	atomic.StoreInt64(&stat.LastAmendSample, 0)
	atomic.StoreInt64(&stat.LastOutOfBounds, 0)
}

type DBStat struct {
	OpStat               OPStat
	SeriesNum            uint64
	BlockNum             int
	HeadMinTime          int64
	HeadMaxTime          int64
	HeadMinValidTime     int64
	AppenderMinValidTime int64
	LastRecvHb           int64
	LastSendHb           int64
	SnapSyncOffset       backendmsg.BlockSyncOffset
}

type Stat struct {
	meta.Node
	*DBStat `json:"omitempty"`
}

func (stat Stat) String() string {
	var buf []byte
	lnBreak := byte('\n')

	buf = append(append(append(buf, "Shard: "...), stat.Node.ShardID...), lnBreak)
	buf = append(append(append(buf, "Role: "...), stat.Node.Role.String()...), lnBreak)
	buf = append(append(append(buf, "Addr: "...), stat.Node.Addr...), lnBreak)
	buf = append(append(append(append(buf, "DiskFree: "...), strconv.FormatUint(stat.Node.DiskFree, 10)...), "GB"...), lnBreak)
	buf = append(append(append(buf, "IDC: "...), stat.Node.IDC...), lnBreak)

	if stat.Node.Role == meta.RoleSlave {
		buf = append(append(append(buf, "MasterAddr: "...), stat.Node.MasterAddr...), lnBreak)
	}

	if stat.DBStat != nil {
		buf = append(append(buf, lnBreak), lnBreak)
		buf = append(append(append(buf, "SucceedSel: "...), strconv.FormatUint(stat.OpStat.SucceedSel, 10)...), lnBreak)
		buf = append(append(append(buf, "FailedSel: "...), strconv.FormatUint(stat.OpStat.FailedSel, 10)...), lnBreak)
		buf = append(append(append(buf, "SucceedLVals: "...), strconv.FormatUint(stat.OpStat.SucceedLVals, 10)...), lnBreak)
		buf = append(append(append(buf, "FailedLVals: "...), strconv.FormatUint(stat.OpStat.FailedLVals, 10)...), lnBreak)
		buf = append(append(append(buf, "ReceivedAdd: "...), strconv.FormatUint(stat.OpStat.ReceivedAdd, 10)...), lnBreak)
		buf = append(append(append(buf, "SucceedAdd: "...), strconv.FormatUint(stat.OpStat.SucceedAdd, 10)...), lnBreak)
		buf = append(append(append(buf, "FailedAdd: "...), strconv.FormatUint(stat.OpStat.FailedAdd, 10)...), lnBreak)
		buf = append(append(append(buf, "OutOfOrder: "...), strconv.FormatUint(stat.OpStat.OutOfOrder, 10)...), lnBreak)
		buf = append(append(append(buf, "AmendSample: "...), strconv.FormatUint(stat.OpStat.AmendSample, 10)...), lnBreak)
		buf = append(append(append(buf, "OutOfBounds: "...), strconv.FormatUint(stat.OpStat.OutOfBounds, 10)...), lnBreak)
		buf = append(append(append(buf, "FailedCommit: "...), strconv.FormatUint(stat.OpStat.FailedCommit, 10)...), lnBreak)
		buf = append(append(append(buf, "LastOutOfOrder: "...), formatTimestamp(stat.OpStat.LastOutOfOrder)...), lnBreak)
		buf = append(append(append(buf, "LastAmendSample: "...), formatTimestamp(stat.OpStat.LastAmendSample)...), lnBreak)
		buf = append(append(append(buf, "LastOutOfBounds: "...), formatTimestamp(stat.OpStat.LastOutOfBounds)...), lnBreak)
		buf = append(append(append(buf, "SeriesNum: "...), strconv.FormatUint(stat.SeriesNum, 10)...), lnBreak)
		buf = append(append(append(buf, "BlockNum: "...), strconv.Itoa(stat.BlockNum)...), lnBreak)
		buf = append(append(append(buf, "HeadMinTime: "...), formatTimestamp(stat.HeadMinTime)...), lnBreak)
		buf = append(append(append(buf, "HeadMaxTime: "...), formatTimestamp(stat.HeadMaxTime)...), lnBreak)
		buf = append(append(append(buf, "HeadMinValidTime: "...), formatTimestamp(stat.HeadMinValidTime)...), lnBreak)
		buf = append(append(append(buf, "AppenderMinValidTime: "...), formatTimestamp(stat.AppenderMinValidTime)...), lnBreak)

		if stat.Node.Role == meta.RoleMaster && stat.LastRecvHb > 0 {
			buf = append(append(append(buf, "LastRecvHb: "...), formatTimestamp(stat.LastRecvHb)...), lnBreak)
			buf = append(append(append(buf, "SnapSyncOffset: "...), stat.SnapSyncOffset.String()...), lnBreak)
		}
		if stat.Node.Role == meta.RoleSlave && stat.LastSendHb > 0 {
			buf = append(append(buf, "LastSendHb: "...), formatTimestamp(stat.LastSendHb)...)
		}
	}

	return util.YoloString(buf)
}

func formatTimestamp(t int64) string {
	if t <= 0 {
		return strconv.FormatInt(t, 10)
	}
	return tm.Time(t).Format(time.RFC3339)
}
