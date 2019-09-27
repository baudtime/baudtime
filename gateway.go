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

package baudtime

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/baudtime/baudtime/backend"
	"github.com/baudtime/baudtime/msg"
	gatewaymsg "github.com/baudtime/baudtime/msg/gateway"
	"github.com/baudtime/baudtime/promql"
	"github.com/baudtime/baudtime/util"
	ts "github.com/baudtime/baudtime/util/time"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	lb "github.com/prometheus/prometheus/pkg/labels"
	"github.com/valyala/fasthttp"
	"go.uber.org/multierr"
)

type queryResult struct {
	ResultType promql.ValueType `json:"resultType"`
	Result     promql.Value     `json:"result"`
}

type Gateway struct {
	Backend      backend.Backend
	QueryEngine  *promql.Engine
	appenderPool sync.Pool
}

func (gateway *Gateway) InstantQuery(request *gatewaymsg.InstantQueryRequest) *gatewaymsg.QueryResponse {
	result, err := gateway.instantQuery(request.Time, request.Timeout, request.Query)
	if err != nil {
		return &gatewaymsg.QueryResponse{Status: msg.StatusCode_Failed, ErrorMsg: err.Error()}
	}

	queryRes, err := json.Marshal(result)
	if err != nil {
		return &gatewaymsg.QueryResponse{Status: msg.StatusCode_Failed, ErrorMsg: err.Error()}
	}

	return &gatewaymsg.QueryResponse{Status: msg.StatusCode_Succeed, Result: string(queryRes)}
}

func (gateway *Gateway) RangeQuery(request *gatewaymsg.RangeQueryRequest) *gatewaymsg.QueryResponse {
	result, err := gateway.rangeQuery(request.Start, request.End, request.Step, request.Timeout, request.Query)
	if err != nil {
		return &gatewaymsg.QueryResponse{Status: msg.StatusCode_Failed, ErrorMsg: err.Error()}
	}

	queryRes, err := json.Marshal(result)
	if err != nil {
		return &gatewaymsg.QueryResponse{Status: msg.StatusCode_Failed, ErrorMsg: err.Error()}
	}

	return &gatewaymsg.QueryResponse{Status: msg.StatusCode_Succeed, Result: string(queryRes)}
}

func (gateway *Gateway) LabelValues(request *gatewaymsg.LabelValuesRequest) *msg.LabelValuesResponse {
	values, err := gateway.labelValues(request.Name, request.Constraint, request.Timeout)
	if err != nil {
		return &msg.LabelValuesResponse{Status: msg.StatusCode_Failed, ErrorMsg: err.Error()}
	}
	return &msg.LabelValuesResponse{Status: msg.StatusCode_Succeed, Values: values}
}

func (gateway *Gateway) Ingest(request *gatewaymsg.AddRequest) error {
	var err error
	var appender backend.Appender

	if v := gateway.appenderPool.Get(); v != nil {
		appender = v.(backend.Appender)
	} else {
		appender, err = gateway.Backend.Appender()
		if err != nil {
			return errors.Errorf("no suitable appender: %v", err)
		}
	}

	var hasher = util.NewHasher()
	for _, series := range request.Series {
		hash := hasher.Hash(series.Labels)

		for _, p := range series.Points {
			if er := appender.Add(series.Labels, p.T, p.V, hash); er != nil {
				err = multierr.Append(err, er)
			}
		}
	}

	if er := appender.Flush(); er != nil {
		err = multierr.Append(err, er)
	}

	gateway.appenderPool.Put(appender)
	return err
}

type httpResponse struct {
	Status string      `json:"status"`
	Data   interface{} `json:"data,omitempty"`
	Error  string      `json:"error,omitempty"`
}

func (gateway *Gateway) HttpInstantQuery(c *fasthttp.RequestCtx) {
	exeHttpQuery(c, func() (interface{}, error) {
		var ts, timeout, query string

		if t := c.QueryArgs().Peek("time"); t != nil {
			ts = string(t)
		}

		if to := c.QueryArgs().Peek("timeout"); to != nil {
			timeout = string(to)
		}

		if q := c.QueryArgs().Peek("query"); q != nil {
			query = string(q)
		}

		return gateway.instantQuery(ts, timeout, query)
	})
}

func (gateway *Gateway) HttpRangeQuery(c *fasthttp.RequestCtx) {
	exeHttpQuery(c, func() (interface{}, error) {
		var start, end, step, timeout, query string

		if arg := c.QueryArgs().Peek("start"); arg != nil {
			start = string(arg)
		}

		if arg := c.QueryArgs().Peek("end"); arg != nil {
			end = string(arg)
		}

		if arg := c.QueryArgs().Peek("step"); arg != nil {
			step = string(arg)
		}

		if arg := c.QueryArgs().Peek("timeout"); arg != nil {
			timeout = string(arg)
		}

		if arg := c.QueryArgs().Peek("query"); arg != nil {
			query = string(arg)
		}

		return gateway.rangeQuery(start, end, step, timeout, query)
	})
}

func (gateway *Gateway) HttpLabelValues(c *fasthttp.RequestCtx) {
	exeHttpQuery(c, func() (interface{}, error) {
		name, ok := c.UserValue("name").(string)
		if !ok {
			return nil, errors.New("label name must be provided")
		}

		var constraint, timeout string
		if arg := c.QueryArgs().Peek("constraint"); arg != nil {
			constraint = string(arg)
		}

		if arg := c.QueryArgs().Peek("timeout"); arg != nil {
			timeout = string(arg)
		}

		return gateway.labelValues(name, constraint, timeout)
	})
}

func (gateway *Gateway) instantQuery(t, timeout, query string) (*queryResult, error) {
	span := opentracing.StartSpan("instantQuery", opentracing.Tag{"query", query})
	defer span.Finish()

	var ts time.Time
	if t != "" {
		var err error
		ts, err = ParseTime(t)
		if err != nil {
			return nil, err
		}
	} else {
		ts = time.Now()
	}

	ctx := context.WithValue(context.Background(), "span", span)
	if timeout != "" {
		var cancel context.CancelFunc
		to, err := ParseDuration(timeout)
		if err != nil {
			return nil, err
		}

		ctx, cancel = context.WithTimeout(ctx, to)
		defer cancel()
	}

	if query == "" {
		return nil, errors.New("query must be provided")
	}

	qry, err := gateway.QueryEngine.NewInstantQuery(gateway.Backend, query, ts)
	if err != nil {
		return nil, err
	}

	res := qry.Exec(ctx)
	if res.Err != nil {
		return nil, res.Err
	}

	return &queryResult{
		ResultType: res.Value.Type(),
		Result:     res.Value,
	}, nil
}

func (gateway *Gateway) rangeQuery(startT, endT, step, timeout, query string) (*queryResult, error) {
	span := opentracing.StartSpan("rangeQuery", opentracing.Tag{"query", query})
	defer span.Finish()

	if startT == "" {
		return nil, errors.New("start time must be provided")
	}
	start, err := ParseTime(startT)
	if err != nil {
		return nil, err
	}

	if endT == "" {
		return nil, errors.New("end time must be provided")
	}

	end, err := ParseTime(endT)
	if err != nil {
		return nil, err
	}

	if end.Before(start) {
		return nil, errors.New("end time must not be before start time")
	}

	if step == "" {
		return nil, errors.New("step must be provided")
	}

	interval, err := ParseDuration(step)
	if err != nil {
		return nil, err
	}

	if interval <= 0 {
		return nil, errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if end.Sub(start)/interval > 11000 {
		return nil, errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
	}

	ctx := context.WithValue(context.Background(), "span", span)
	if timeout != "" {
		var cancel context.CancelFunc
		to, err := ParseDuration(timeout)
		if err != nil {
			return nil, err
		}

		ctx, cancel = context.WithTimeout(ctx, to)
		defer cancel()
	}

	if query == "" {
		return nil, errors.New("query must be provided")
	}

	qry, err := gateway.QueryEngine.NewRangeQuery(gateway.Backend, query, start, end, interval)
	if err != nil {
		return nil, err
	}

	res := qry.Exec(ctx)
	if res.Err != nil {
		return nil, res.Err
	}

	return &queryResult{
		ResultType: res.Value.Type(),
		Result:     res.Value,
	}, nil
}

func (gateway *Gateway) labelValues(name, constraint, timeout string) ([]string, error) {
	span := opentracing.StartSpan("labelValues", opentracing.Tag{"name", name}, opentracing.Tag{"constraint", constraint})
	defer span.Finish()

	var (
		mint, maxt int64 = math.MinInt64, math.MaxInt64
		matchers   []*lb.Matcher
	)

	if constraint != "" {
		now := time.Now()

		expr, err := promql.ParseExpr(constraint)
		if err != nil {
			return nil, err
		}

		switch selector := expr.(type) {
		case *promql.VectorSelector:
			mint = ts.FromTime(now.Add(-selector.Offset))
			maxt = mint
			matchers = selector.LabelMatchers
		case *promql.MatrixSelector:
			mint = ts.FromTime(now.Add(-selector.Offset - selector.Range))
			maxt = ts.FromTime(now.Add(-selector.Offset))
			matchers = selector.LabelMatchers
		default:
			return nil, errors.Errorf("invalid expression type %s for constraint, must be Scalar or instant Vector", expr.Type())
		}
	}

	ctx := context.WithValue(context.Background(), "span", span)
	if timeout != "" {
		var cancel context.CancelFunc
		to, err := ParseDuration(timeout)
		if err != nil {
			return nil, err
		}

		ctx, cancel = context.WithTimeout(ctx, to)
		defer cancel()
	}

	q, err := gateway.Backend.Querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	vals, err := q.LabelValues(name, matchers...)
	if err != nil {
		return nil, err
	}

	return vals, nil
}

func exeHttpQuery(c *fasthttp.RequestCtx, f func() (interface{}, error)) {
	c.SetContentType("application/json; charset=utf-8")

	result, err := f()
	if err != nil {
		c.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}

	queryRes, err := json.Marshal(&httpResponse{
		Status: "success",
		Data:   result,
	})
	if err != nil {
		c.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}
	c.SetBody(queryRes)
}

func ParseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func ParseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}
