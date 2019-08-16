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
	"context"
	"github.com/baudtime/baudtime/backend"
	backendmsg "github.com/baudtime/baudtime/msg/backend"
	"github.com/baudtime/baudtime/util"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
)

func QueryableConn(c *CodedConn) backend.Queryable {
	return backend.QueryableFunc(func(ctx context.Context, mint, maxt int64) (backend.Querier, error) {
		return &querier{
			ctx:       ctx,
			mint:      mint,
			maxt:      maxt,
			CodedConn: c,
		}, nil
	})
}

// querier is an adapter to make a Client usable as a Querier.
type querier struct {
	ctx        context.Context
	mint, maxt int64
	*CodedConn
}

// Select implements Querier and uses the given matchers to read series
// sets from the Client.
func (q *querier) Select(selectParams *backend.SelectParams, matchers ...*labels.Matcher) (backend.SeriesSet, error) {
	queryRequest := &backendmsg.SelectRequest{
		Mint:     q.mint,
		Maxt:     q.maxt,
		Interval: selectParams.Step,
		Matchers: util.MatchersToProto(matchers),
	}

	err := q.WriteRaw(queryRequest)
	if err != nil {
		return nil, err
	}

	res, err := q.ReadRaw()
	if err != nil {
		return nil, err
	}

	return backend.FromQueryResult(res.(*backendmsg.SelectResponse)), nil
}

// LabelValues implements Querier and is a noop.
func (q *querier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	return nil, errors.New("not supported")
}

// Close implements Querier and is a noop.
func (q *querier) Close() error {
	return nil
}
