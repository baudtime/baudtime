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
	"bytes"
	"math"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/allegro/bigcache/v2"
	"github.com/baudtime/baudtime/backend/storage/replication"
	"github.com/baudtime/baudtime/meta"
	"github.com/baudtime/baudtime/msg"
	backendmsg "github.com/baudtime/baudtime/msg/backend"
	"github.com/baudtime/baudtime/util"
	tm "github.com/baudtime/baudtime/util/time"
	"github.com/baudtime/baudtime/vars"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
	"github.com/shirou/gopsutil/disk"
	"go.uber.org/multierr"
)

func selectLabelsOnly(q tsdb.Querier, matchers []labels.Matcher) ([]*msg.Series, error) {
	set, err := q.Select(matchers...)
	if err != nil {
		return nil, err
	}

	var series []*msg.Series

	for set.Next() {
		curSeries := set.At()
		series = append(series, &msg.Series{
			Labels: LabelsToProto(curSeries.Labels()),
			Points: nil,
		})
	}

	return series, nil
}

func selectVectors(q tsdb.Querier, matchers []labels.Matcher, tIt *tm.TimestampIter) ([]*msg.Series, error) {
	set, err := q.Select(matchers...)
	if err != nil {
		return nil, err
	}

	var (
		series []*msg.Series
		pIt    *BufferedSeriesIterator
		points []msg.Point
	)

	for set.Next() {
		curSeries := set.At()

		start := len(points)
		if pIt == nil {
			pIt = NewBufferIterator(curSeries.Iterator(), tm.DurationMilliSec(vars.Cfg.LookbackDelta))
		} else {
			pIt.Reset(curSeries.Iterator())
		}

		for tIt.Next() {
			ts := tIt.At()

			var t int64
			var v float64

			ok := pIt.Seek(ts)
			if !ok {
				err = pIt.Err()
				if err != nil {
					return nil, err
				}
			} else {
				t, v = pIt.Values()
			}

			if !ok || t > ts {
				t, v, ok = pIt.PeekBack(1)
				if !ok || t < ts-tm.DurationMilliSec(vars.Cfg.LookbackDelta) {
					continue
				}
			}
			if value.IsStaleNaN(v) {
				continue
			}

			points = append(points, msg.Point{T: t, V: v})
		}

		if len(points[start:]) > 0 {
			series = append(series, &msg.Series{
				Labels: LabelsToProto(curSeries.Labels()),
				Points: points[start:],
			})
		}

		tIt.Reset()
	}

	return series, nil
}

func selectNoInterval(q tsdb.Querier, matchers []labels.Matcher, mint, maxt int64) ([]*msg.Series, error) {
	set, err := q.Select(matchers...)
	if err != nil {
		return nil, err
	}

	var (
		series []*msg.Series
		points []msg.Point
	)

	for set.Next() {
		curSeries := set.At()

		start := len(points)
		it := NewBufferIterator(curSeries.Iterator(), maxt-mint)

		ok := it.Seek(maxt)
		if !ok {
			err = it.Err()
			if err != nil {
				return nil, err
			}
		}

		buf := it.Buffer()
		for buf.Next() {
			t, v := buf.At()
			if value.IsStaleNaN(v) {
				continue
			}
			// Values in the buffer are guaranteed to be smaller than maxt.
			if t >= mint {
				points = append(points, msg.Point{T: t, V: v})
			}
		}

		// The seeked sample might also be in the range.
		if ok {
			t, v := it.Values()
			if t == maxt && !value.IsStaleNaN(v) {
				points = append(points, msg.Point{T: t, V: v})
			}
		}

		if len(points[start:]) > 0 {
			series = append(series, &msg.Series{
				Labels: LabelsToProto(curSeries.Labels()),
				Points: points[start:],
			})
		}
	}

	return series, nil
}

type Storage struct {
	*tsdb.DB
	*AddReqHandler
	ReplicateManager *replication.ReplicateManager
	OpStat           *OPStat
}

func New(db *tsdb.DB) *Storage {
	opStat := new(OPStat)

	symbolsK, err := bigcache.NewBigCache(bigcache.Config{
		Shards:             1024,
		LifeWindow:         24 * time.Hour,
		MaxEntriesInWindow: 1000 * 10 * 60,
		MaxEntrySize:       500,
		Verbose:            false,
		Hasher:             util.NewHasher(),
	})
	if err != nil {
		panic(err)
	}

	symbolsV, err := bigcache.NewBigCache(bigcache.Config{
		Shards:             1 << 16,
		LifeWindow:         24 * time.Hour,
		MaxEntriesInWindow: 1000 * 10 * 60,
		MaxEntrySize:       500,
		Verbose:            false,
		Hasher:             util.NewHasher(),
	})
	if err != nil {
		panic(err)
	}

	return &Storage{
		DB: db,
		AddReqHandler: &AddReqHandler{
			appender: db.Appender,
			opStat:   opStat,
			symbolsK: symbolsK,
			symbolsV: symbolsV,
		},
		ReplicateManager: replication.NewReplicateManager(db),
		OpStat:           opStat,
	}
}

func (storage *Storage) HandleSelectReq(request *backendmsg.SelectRequest) *backendmsg.SelectResponse {
	queryResponse := &backendmsg.SelectResponse{Status: msg.StatusCode_Failed}

	var span opentracing.Span
	wireContext, err := opentracing.GlobalTracer().Extract(opentracing.Binary, bytes.NewBuffer(request.SpanCtx))
	if err != nil {
		span = opentracing.StartSpan("storage_select")
	} else {
		span = opentracing.StartSpan("storage_select", opentracing.ChildOf(wireContext))
	}
	defer func() {
		if queryResponse.Status == msg.StatusCode_Succeed {
			atomic.AddUint64(&storage.opStat.SucceedSel, 1)
			span.SetTag("seriesNum", len(queryResponse.Series))
		} else {
			atomic.AddUint64(&storage.opStat.FailedSel, 1)
			span.SetTag("errorMsg", queryResponse.ErrorMsg)
		}
		span.Finish()
	}()

	matchers, err := ProtoToMatchers(request.Matchers)
	if err != nil {
		queryResponse.ErrorMsg = err.Error()
		return queryResponse
	}

	var (
		q      tsdb.Querier
		series []*msg.Series
	)

	if (request.Mint == request.Maxt && request.Interval == 0) || (request.Mint < request.Maxt && request.Interval > 0) {
		q, err = storage.DB.Querier(request.Mint-tm.DurationMilliSec(vars.Cfg.LookbackDelta), request.Maxt)
		if err != nil {
			queryResponse.ErrorMsg = err.Error()
			return queryResponse
		}
		defer q.Close()

		series, err = selectVectors(q, matchers, tm.NewTimestampIter(request.Mint, request.Maxt, request.Interval))
	} else if request.Mint < request.Maxt && request.Interval == 0 {
		q, err = storage.DB.Querier(request.Mint, request.Maxt)
		if err != nil {
			queryResponse.ErrorMsg = err.Error()
			return queryResponse
		}
		defer q.Close()

		if request.OnlyLabels {
			series, err = selectLabelsOnly(q, matchers)
		} else {
			series, err = selectNoInterval(q, matchers, request.Mint, request.Maxt)
		}
	} else {
		queryResponse.ErrorMsg = "parameter error"
		return queryResponse
	}

	if err != nil {
		queryResponse.ErrorMsg = err.Error()
		return queryResponse
	}

	queryResponse.Status = msg.StatusCode_Succeed
	queryResponse.Series = series
	return queryResponse
}

func (storage *Storage) HandleLabelValuesReq(request *backendmsg.LabelValuesRequest) *msg.LabelValuesResponse {
	queryResponse := &msg.LabelValuesResponse{Status: msg.StatusCode_Failed}

	var span opentracing.Span
	wireContext, err := opentracing.GlobalTracer().Extract(opentracing.Binary, bytes.NewBuffer(request.SpanCtx))
	if err != nil {
		span = opentracing.StartSpan("storage_labelValues")
	} else {
		span = opentracing.StartSpan("storage_labelValues", opentracing.ChildOf(wireContext))
	}
	defer func() {
		if queryResponse.Status == msg.StatusCode_Succeed {
			atomic.AddUint64(&storage.opStat.SucceedLVals, 1)
			span.SetTag("valuesNum", len(queryResponse.Values))
		} else {
			atomic.AddUint64(&storage.opStat.FailedLVals, 1)
			span.SetTag("errorMsg", queryResponse.ErrorMsg)
		}
		span.Finish()
	}()

	q, err := storage.DB.Querier(request.Mint, request.Maxt)
	if err != nil {
		queryResponse.ErrorMsg = err.Error()
		return queryResponse
	}
	defer q.Close()

	var values []string

	if len(request.Matchers) == 0 {
		values, err = q.LabelValues(request.Name)
	} else {
		queryResponse.ErrorMsg = "not implemented"
		return queryResponse
	}

	if err != nil {
		queryResponse.ErrorMsg = err.Error()
		return queryResponse
	}

	queryResponse.Status = msg.StatusCode_Succeed
	queryResponse.Values = values
	return queryResponse
}

func (storage *Storage) Close() error {
	return multierr.Combine(storage.ReplicateManager.Close(), storage.DB.Close())
}

func (storage *Storage) Info(detailed bool) (Stat, error) {
	stat := Stat{}

	diskUsage, err := disk.Usage(vars.Cfg.Storage.TSDB.Path)
	if err != nil {
		return stat, err
	}

	masterIP, masterPort := "", ""
	found, masterAddr := storage.ReplicateManager.Master()
	if found {
		ipPort := strings.Split(masterAddr, ":")
		masterIP = ipPort[0]
		masterPort = ipPort[1]
	}

	stat.Node = meta.Node{
		ShardID:    storage.ReplicateManager.RelationID(),
		IP:         vars.LocalIP,
		Port:       vars.Cfg.TcpPort,
		DiskFree:   uint64(math.Round(float64(diskUsage.Free) / 1073741824.0)), //GB
		MasterIP:   masterIP,
		MasterPort: masterPort,
	}

	if !detailed {
		return stat, nil
	}

	recvTimeHb, sendTimeHb := storage.ReplicateManager.LastHeartbeatTime()
	headMinT := storage.DB.Head().MinTime()
	headMaxT := storage.DB.Head().MaxTime()
	headMinValidTime := reflect.ValueOf(storage.DB.Head()).Elem().FieldByName("minValidTime").Int()
	appMinValidTime := headMinValidTime
	if appMinValidTime < headMaxT-vars.Cfg.Storage.TSDB.BlockRanges[0]/2 {
		appMinValidTime = headMaxT - vars.Cfg.Storage.TSDB.BlockRanges[0]/2
	}

	stat.DBStat = &DBStat{
		OpStat:               *storage.OpStat,
		SeriesNum:            storage.DB.Head().NumSeries(),
		BlockNum:             len(storage.DB.Blocks()),
		HeadMinTime:          headMinT,
		HeadMaxTime:          headMaxT,
		HeadMinValidTime:     headMinValidTime,
		AppenderMinValidTime: appMinValidTime,
		LastRecvHb:           recvTimeHb,
		LastSendHb:           sendTimeHb,
	}

	return stat, nil
}

type AddReqHandler struct {
	appender func() tsdb.Appender
	opStat   *OPStat
	symbolsK *bigcache.BigCache
	symbolsV *bigcache.BigCache
}

func (addReqHandler *AddReqHandler) HandleAddReq(request *backendmsg.AddRequest) error {
	var multiErr error
	var app = addReqHandler.appender()

	for _, series := range request.Series {

		var ref uint64
		for _, p := range series.Points {
			var err error

			if ref != 0 {
				err = app.AddFast(ref, p.T, p.V)
			} else {
				lset := make([]labels.Label, len(series.Labels))

				for i, lb := range series.Labels {
					if symbol, err := addReqHandler.symbolsK.Get(lb.Name); err == nil {
						lset[i].Name = util.YoloString(symbol)
					} else {
						lset[i].Name = lb.Name
						addReqHandler.symbolsK.Set(lset[i].Name, util.YoloBytes(lset[i].Name))
					}

					if symbol, err := addReqHandler.symbolsV.Get(lb.Value); err == nil {
						lset[i].Value = util.YoloString(symbol)
					} else {
						lset[i].Value = lb.Value
						addReqHandler.symbolsV.Set(lset[i].Value, util.YoloBytes(lset[i].Value))
					}
				}

				ref, err = app.Add(lset, p.T, p.V)
			}

			atomic.AddUint64(&addReqHandler.opStat.ReceivedAdd, 1)
			if err == nil {
				atomic.AddUint64(&addReqHandler.opStat.SucceedAdd, 1)
			} else {
				atomic.AddUint64(&addReqHandler.opStat.FailedAdd, 1)
				switch err {
				case tsdb.ErrOutOfOrderSample:
					atomic.AddUint64(&addReqHandler.opStat.OutOfOrder, 1)
					atomic.StoreInt64(&addReqHandler.opStat.LastOutOfOrder, p.T)
				case tsdb.ErrAmendSample:
					atomic.AddUint64(&addReqHandler.opStat.AmendSample, 1)
					atomic.StoreInt64(&addReqHandler.opStat.LastAmendSample, p.T)
				case tsdb.ErrOutOfBounds:
					atomic.AddUint64(&addReqHandler.opStat.OutOfBounds, 1)
					atomic.StoreInt64(&addReqHandler.opStat.LastOutOfBounds, p.T)
				default:
					multiErr = multierr.Append(multiErr, err)
				}
			}
		}
	}

	if err := app.Commit(); err != nil {
		atomic.AddUint64(&addReqHandler.opStat.FailedCommit, 1)
		multiErr = multierr.Append(multiErr, err)
	}

	return multiErr
}
