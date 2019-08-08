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
	"fmt"
	"github.com/baudtime/baudtime/msg/pb"
	backendpb "github.com/baudtime/baudtime/msg/pb/backend"
	"github.com/baudtime/baudtime/vars"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
	"testing"
	"time"
)

func TestAPI_Bench(t *testing.T) {
	db, err := tsdb.Open("/tmp/tsdb", nil, nil, &tsdb.Options{
		RetentionDuration: uint64(vars.Cfg.Storage.TSDB.RetentionDuration) / 1e6,
		BlockRanges:       vars.Cfg.Storage.TSDB.BlockRanges,
		NoLockfile:        vars.Cfg.Storage.TSDB.NoLockfile,
	})

	if err != nil {
		t.Fatal(err)
		return
	}

	handler := &AddReqHandler{
		appender: db.Appender,
		//lastCommitTime: time.Now().Unix(),
	}

	lb := []pb.Label{
		{"__name__", "test"},
		{"host", "localhost"},
		{"app", "proxy"},
		{"idc", "langfang"},
		{"state", "0"},
	}

	errNo, total := 0, 0
	begin := time.Now()
	req := &backendpb.AddRequest{
		Series: []*pb.Series{{
			Labels: lb,
		}},
	}

	for i := 0; i < 20000000; i++ {
		now := time.Now().UnixNano() / 1e6
		req.Series[0].Points = []pb.Point{
			{now - 2, 5},
		}

		err = handler.HandleAddReq(req)
		if err != nil {
			errNo++
		}
		total++
	}

	fmt.Printf("ops:%d", int64(total)/int64(time.Since(begin).Seconds()))
}

func TestAPI_Bench_Orig(t *testing.T) {
	db, err := tsdb.Open("/tmp/tsdb", nil, nil, &tsdb.Options{
		RetentionDuration: uint64(vars.Cfg.Storage.TSDB.RetentionDuration) / 1e6,
		BlockRanges:       vars.Cfg.Storage.TSDB.BlockRanges,
		NoLockfile:        vars.Cfg.Storage.TSDB.NoLockfile,
	})

	if err != nil {
		t.Fatal(err)
		return
	}

	app := db.Appender()
	lb := []labels.Label{
		{"__name__", "test"},
		{"host", "localhost"},
		{"app", "proxy"},
		{"idc", "langfang"},
		{"state", "0"},
	}

	errNo, total := 0, 0
	begin := time.Now()

	for i := 0; i < 20000000; i++ {
		now := time.Now().UnixNano() / 1e6

		p := pb.Point{now - 2, 5}
		_, err = app.Add(lb, p.T, p.V)
		if err != nil {
			errNo++
		}
		total++
	}

	fmt.Printf("ops:%d", int64(total)/int64(time.Since(begin).Seconds()))
}
