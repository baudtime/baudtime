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
	"github.com/baudtime/baudtime/backend/storage/replication"
	"github.com/baudtime/baudtime/meta"
	"github.com/baudtime/baudtime/msg"
	backendmsg "github.com/baudtime/baudtime/msg/backend"
	"github.com/baudtime/baudtime/util/syn"
	tm "github.com/baudtime/baudtime/util/time"
	"github.com/baudtime/baudtime/vars"
	"github.com/hashicorp/go-multierror"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
	"github.com/shirou/gopsutil/disk"
	"math"
	"reflect"
	"strings"
	"sync/atomic"
)

func selectVectors(q tsdb.Querier, matchers []*backendmsg.Matcher, tIt *tm.TimestampIter) ([]*msg.Series, error) {
	ms, err := ProtoToMatchers(matchers)
	if err != nil {
		return nil, err
	}

	var (
		series []*msg.Series
		pIt    *BufferedSeriesIterator
	)

	set, err := q.Select(ms...)
	if err != nil {
		return nil, err
	}

	for set.Next() {
		curSeries := set.At()

		if pIt == nil {
			pIt = NewBufferIterator(curSeries.Iterator(), tm.DurationMilliSec(vars.Cfg.Storage.TSDB.LookbackDelta))
		} else {
			pIt.Reset(curSeries.Iterator())
		}

		var points []msg.Point
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
				if !ok || t < ts-tm.DurationMilliSec(vars.Cfg.Storage.TSDB.LookbackDelta) {
					continue
				}
			}
			if value.IsStaleNaN(v) {
				continue
			}

			points = append(points, msg.Point{V: v, T: t})
		}

		series = append(series, &msg.Series{
			Labels: LabelsToProto(curSeries.Labels()),
			Points: points,
		})

		tIt.Reset()
	}

	return series, nil
}

func selectNoInterval(q tsdb.Querier, matchers []*backendmsg.Matcher, mint, maxt int64) ([]*msg.Series, error) {
	ms, err := ProtoToMatchers(matchers)
	if err != nil {
		return nil, err
	}

	set, err := q.Select(ms...)
	if err != nil {
		return nil, err
	}

	series := make([]*msg.Series, 0)
	allPoints := make([]msg.Point, 0)

	for set.Next() {
		start := len(allPoints)
		curSeries := set.At()

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
				allPoints = append(allPoints, msg.Point{T: t, V: v})
			}
		}

		// The seeked sample might also be in the range.
		if ok {
			t, v := it.Values()
			if t == maxt && !value.IsStaleNaN(v) {
				allPoints = append(allPoints, msg.Point{T: t, V: v})
			}
		}

		if len(allPoints[start:]) > 0 {
			series = append(series, &msg.Series{
				Labels: LabelsToProto(curSeries.Labels()),
				Points: allPoints[start:],
			})
		}
	}

	return series, nil
}

type Storage struct {
	*tsdb.DB
	*AddReqHandler
	ReplicateManager *replication.ReplicateManager
}

func New(db *tsdb.DB) *Storage {
	return &Storage{
		DB: db,
		AddReqHandler: &AddReqHandler{
			appender: db.Appender,
			symbolsK: syn.NewMap(1024),
			symbolsV: syn.NewMap(1 << 15),
		},
		ReplicateManager: replication.NewReplicateManager(db),
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
			span.SetTag("seriesNum", len(queryResponse.Series))
		} else {
			span.SetTag("errorMsg", queryResponse.ErrorMsg)
		}
		span.Finish()
	}()

	if (request.Mint == request.Maxt && request.Interval == 0) || (request.Mint < request.Maxt && request.Interval > 0) {
		q, err := storage.DB.Querier(request.Mint-tm.DurationMilliSec(vars.Cfg.Storage.TSDB.LookbackDelta), request.Maxt)
		if err != nil {
			queryResponse.ErrorMsg = err.Error()
			return queryResponse
		}
		defer q.Close()

		series, err := selectVectors(q, request.Matchers, tm.NewTimestampIter(request.Mint, request.Maxt, request.Interval))
		if err != nil {
			queryResponse.ErrorMsg = err.Error()
			return queryResponse
		}

		queryResponse.Status = msg.StatusCode_Succeed
		queryResponse.Series = series
		return queryResponse
	}

	if request.Mint < request.Maxt && request.Interval == 0 {
		q, err := storage.DB.Querier(request.Mint-tm.DurationMilliSec(vars.Cfg.Storage.TSDB.LookbackDelta), request.Maxt)
		if err != nil {
			queryResponse.ErrorMsg = err.Error()
			return queryResponse
		}
		defer q.Close()

		series, err := selectNoInterval(q, request.Matchers, request.Mint, request.Maxt)
		if err != nil {
			queryResponse.ErrorMsg = err.Error()
			return queryResponse
		}

		queryResponse.Status = msg.StatusCode_Succeed
		queryResponse.Series = series
		return queryResponse
	}

	queryResponse.ErrorMsg = "parameter error"
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
			span.SetTag("valuesNum", len(queryResponse.Values))
		} else {
			span.SetTag("errorMsg", queryResponse.ErrorMsg)
		}
		span.Finish()
	}()

	q, err := storage.DB.Querier(math.MinInt64, math.MaxInt64)
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

func (storage *Storage) Close() (err error) {
	err = multierror.Append(err, storage.ReplicateManager.Close(), storage.DB.Close())
	return
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

	stat.DbStat = &DbStat{
		AddStats:             storage.addStat,
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
	addStat  AddStat
	symbolsK *syn.Map
	symbolsV *syn.Map
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
					if symbol, found := addReqHandler.symbolsK.Get(lb.Name); found {
						lset[i].Name = symbol.(string)
					} else {
						lset[i].Name = lb.Name
						addReqHandler.symbolsK.Set(lset[i].Name, lset[i].Name)
					}

					if symbol, found := addReqHandler.symbolsV.Get(lb.Value); found {
						lset[i].Value = symbol.(string)
					} else {
						lset[i].Value = lb.Value
						addReqHandler.symbolsV.Set(lset[i].Value, lset[i].Value)
					}
				}

				ref, err = app.Add(lset, p.T, p.V)
			}

			atomic.AddUint64(&addReqHandler.addStat.Received, 1)
			if err == nil {
				atomic.AddUint64(&addReqHandler.addStat.Succeed, 1)
			} else {
				atomic.AddUint64(&addReqHandler.addStat.Failed, 1)
				switch errors.Cause(err) {
				case tsdb.ErrOutOfOrderSample:
					atomic.AddUint64(&addReqHandler.addStat.OutOfOrder, 1)
				case tsdb.ErrAmendSample:
					atomic.AddUint64(&addReqHandler.addStat.AmendSample, 1)
				case tsdb.ErrOutOfBounds:
					atomic.AddUint64(&addReqHandler.addStat.OutOfBounds, 1)
				default:
					multiErr = multierror.Append(multiErr, err)
				}
			}
		}
	}

	if err := app.Commit(); err != nil {
		multiErr = multierror.Append(multiErr, err)
	}

	return multiErr
}
