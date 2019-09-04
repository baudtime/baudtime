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
	"github.com/baudtime/baudtime/meta"
	"github.com/baudtime/baudtime/util"
	tm "github.com/baudtime/baudtime/util/time"
	"strconv"
	"time"
)

type AddStat struct {
	Received    uint64
	Succeed     uint64
	Failed      uint64
	OutOfOrder  uint64
	AmendSample uint64
	OutOfBounds uint64
}

type DbStat struct {
	AddStats             AddStat
	SeriesNum            uint64
	BlockNum             int
	HeadMinTime          int64
	HeadMaxTime          int64
	HeadMinValidTime     int64
	AppenderMinValidTime int64
	LastRecvHb           int64
	LastSendHb           int64
}

type Stat struct {
	meta.Node
	*DbStat `json:"omitempty"`
}

func (stat Stat) String() string {
	var buf []byte
	lnBreak := byte('\n')

	buf = append(append(append(buf, "Shard: "...), stat.Node.ShardID...), lnBreak)
	buf = append(append(append(buf, "IP: "...), stat.Node.IP...), lnBreak)
	buf = append(append(append(buf, "Port: "...), stat.Node.Port...), lnBreak)
	buf = append(append(append(append(buf, "DiskFree: "...), strconv.FormatUint(stat.Node.DiskFree, 10)...), "GB"...), lnBreak)
	buf = append(append(append(buf, "IDC: "...), stat.Node.IDC...), lnBreak)

	if stat.Node.MasterIP != "" && stat.Node.MasterPort != "" {
		buf = append(append(append(buf, "MasterIP: "...), stat.Node.MasterIP...), lnBreak)
		buf = append(append(append(buf, "MasterPort: "...), stat.Node.MasterPort...), lnBreak)
	}

	tsToString := func(t int64) string {
		if t <= 0 {
			return strconv.FormatInt(t, 10)
		}
		return tm.Time(t).Format(time.RFC3339)
	}

	if stat.DbStat != nil {
		buf = append(append(append(buf, "Received: "...), strconv.FormatUint(stat.AddStats.Received, 10)...), lnBreak)
		buf = append(append(append(buf, "Succeed: "...), strconv.FormatUint(stat.AddStats.Succeed, 10)...), lnBreak)
		buf = append(append(append(buf, "Failed: "...), strconv.FormatUint(stat.AddStats.Failed, 10)...), lnBreak)
		buf = append(append(append(buf, "OutOfOrder: "...), strconv.FormatUint(stat.AddStats.OutOfOrder, 10)...), lnBreak)
		buf = append(append(append(buf, "AmendSample: "...), strconv.FormatUint(stat.AddStats.AmendSample, 10)...), lnBreak)
		buf = append(append(append(buf, "OutOfBounds: "...), strconv.FormatUint(stat.AddStats.OutOfBounds, 10)...), lnBreak)
		buf = append(append(append(buf, "SeriesNum: "...), strconv.FormatUint(stat.SeriesNum, 10)...), lnBreak)
		buf = append(append(append(buf, "BlockNum: "...), strconv.Itoa(stat.BlockNum)...), lnBreak)
		buf = append(append(append(buf, "HeadMinTime: "...), tsToString(stat.HeadMinTime)...), lnBreak)
		buf = append(append(append(buf, "HeadMaxTime: "...), tsToString(stat.HeadMaxTime)...), lnBreak)
		buf = append(append(append(buf, "HeadMinValidTime: "...), tsToString(stat.HeadMinValidTime)...), lnBreak)
		buf = append(append(append(buf, "AppenderMinValidTime: "...), tsToString(stat.AppenderMinValidTime)...), lnBreak)
		buf = append(append(append(buf, "LastRecvHb: "...), tsToString(stat.LastRecvHb)...), lnBreak)
		buf = append(append(append(buf, "LastSendHb: "...), tsToString(stat.LastSendHb)...), lnBreak)
	}

	return util.YoloString(buf)
}
