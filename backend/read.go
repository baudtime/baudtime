// Copyright 2017 The Prometheus Authors
// Copyright 2019 The Baudtime Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"context"
	"fmt"
	"sort"

	"github.com/baudtime/baudtime/msg"
	backendmsg "github.com/baudtime/baudtime/msg/backend"
	"github.com/baudtime/baudtime/util"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
)

// QueryableClient returns a Queryable which queries the given
// Client to select series sets.
func QueryableClient(c Client) Queryable {
	return QueryableFunc(func(ctx context.Context, mint, maxt int64) (Querier, error) {
		return &querier{
			ctx:    ctx,
			mint:   mint,
			maxt:   maxt,
			client: c,
		}, nil
	})
}

// querier is an adapter to make a Client usable as a Querier.
type querier struct {
	ctx        context.Context
	mint, maxt int64
	client     Client
}

// Select implements Querier and uses the given matchers to read series
// sets from the Client.
func (q *querier) Select(selectParams *SelectParams, matchers ...*labels.Matcher) (SeriesSet, error) {
	selectRequest := &backendmsg.SelectRequest{
		Mint:     q.mint,
		Maxt:     q.maxt,
		Matchers: util.MatchersToProto(matchers),
	}
	if selectParams == nil {
		selectRequest.Interval = q.maxt - q.mint
	} else {
		selectRequest.Interval = selectParams.Step
	}
	res, err := q.client.Select(q.ctx, selectRequest)
	if err != nil {
		return nil, err
	}
	return FromQueryResult(res), nil
}

// LabelValues implements Querier and is a noop.
func (q *querier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	labelValuesRequest := &backendmsg.LabelValuesRequest{
		Mint: q.mint,
		Maxt: q.maxt,
		Name: name,
	}
	if len(matchers) > 0 {
		labelValuesRequest.Matchers = util.MatchersToProto(matchers)
	}
	res, err := q.client.LabelValues(q.ctx, labelValuesRequest)
	if err != nil {
		return nil, err
	}
	return res.Values, nil
}

// Close implements Querier and is a noop.
func (q *querier) Close() error {
	return nil
}

// FromQueryResult unpacks a QueryResult proto.
func FromQueryResult(res *backendmsg.SelectResponse) SeriesSet {
	series := make([]Series, 0, len(res.Series))
	for _, ts := range res.Series {
		lbls := util.ProtoToLabels(ts.Labels)
		if err := validateLabelsAndMetricName(lbls); err != nil {
			return errSeriesSet{err: err}
		}

		series = append(series, &concreteSeries{
			labels:  lbls,
			samples: ts.Points,
		})
	}
	//TODO
	//sort.Sort(byLabel(series))
	return &concreteSeriesSet{
		series: series,
	}
}

// validateLabelsAndMetricName validates the label names/values and metric names returned from remote read.
func validateLabelsAndMetricName(ls labels.Labels) error {
	for _, l := range ls {
		if l.Name == labels.MetricName && !model.IsValidMetricName(model.LabelValue(l.Value)) {
			return fmt.Errorf("invalid metric name: %v", l.Value)
		}
		if !model.LabelName(l.Name).IsValid() {
			return fmt.Errorf("invalid label name: %v", l.Name)
		}
		if !model.LabelValue(l.Value).IsValid() {
			return fmt.Errorf("invalid label value: %v", l.Value)
		}
	}
	return nil
}

type byLabel []Series

func (a byLabel) Len() int           { return len(a) }
func (a byLabel) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byLabel) Less(i, j int) bool { return labels.Compare(a[i].Labels(), a[j].Labels()) < 0 }

// concreteSeriesSet implements SeriesSet.
type concreteSeriesSet struct {
	cur    int
	series []Series
}

func (c *concreteSeriesSet) Next() bool {
	c.cur++
	return c.cur-1 < len(c.series)
}

func (c *concreteSeriesSet) At() Series {
	return c.series[c.cur-1]
}

func (c *concreteSeriesSet) Err() error {
	return nil
}

// concreteSeries implementes Series.
type concreteSeries struct {
	labels  labels.Labels
	samples []msg.Point
}

func (c *concreteSeries) Labels() labels.Labels {
	return labels.New(c.labels...)
}

func (c *concreteSeries) Iterator() SeriesIterator {
	return newConcreteSeriersIterator(c)
}

// concreteSeriesIterator implements SeriesIterator.
type concreteSeriesIterator struct {
	cur    int
	series *concreteSeries
}

func newConcreteSeriersIterator(series *concreteSeries) SeriesIterator {
	return &concreteSeriesIterator{
		cur:    -1,
		series: series,
	}
}

// Seek implements SeriesIterator.
func (c *concreteSeriesIterator) Seek(t int64) bool {
	c.cur = sort.Search(len(c.series.samples), func(n int) bool {
		return c.series.samples[n].T >= t
	})
	return c.cur < len(c.series.samples)
}

// At implements SeriesIterator.
func (c *concreteSeriesIterator) At() (t int64, v float64) {
	s := c.series.samples[c.cur]
	return s.T, s.V
}

// Next implements SeriesIterator.
func (c *concreteSeriesIterator) Next() bool {
	c.cur++
	return c.cur < len(c.series.samples)
}

// Err implements SeriesIterator.
func (c *concreteSeriesIterator) Err() error {
	return nil
}
