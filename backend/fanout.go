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
	"container/heap"
	"context"
	"strings"
	"sync"

	"github.com/baudtime/baudtime/backend/storage"
	"github.com/baudtime/baudtime/backend/visitor"
	"github.com/baudtime/baudtime/meta"
	"github.com/baudtime/baudtime/msg"
	"github.com/baudtime/baudtime/util/time"
	"github.com/baudtime/baudtime/vars"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"go.uber.org/multierr"
)

type Fanout struct {
	localStorage *storage.Storage
}

// NewFanout returns a new fan-out Backend, which proxies reads and writes
// through to multiple underlying storages.
func NewFanout(localStorage *storage.Storage) *Fanout {
	return &Fanout{
		localStorage: localStorage,
	}
}

func (f *Fanout) Querier(ctx context.Context, mint, maxt int64) (Querier, error) {
	return &fanoutQuerier{
		ctx:          ctx,
		mint:         mint,
		maxt:         maxt,
		localStorage: f.localStorage,
	}, nil
}

// StartTime implements the Backend interface.
func (f *Fanout) StartTime() (int64, error) {
	// StartTime of a fanout should be the earliest StartTime of all its storages,
	// both primary and secondaries.
	return int64(model.Latest), nil
}

// Close closes the storage and all its underlying resources.
func (f *Fanout) Close() (err error) {
	if f.localStorage != nil {
		err = f.localStorage.Close()
	}
	return
}

type fanoutQuerier struct {
	ctx        context.Context
	mint, maxt int64
	Querier
	localStorage *storage.Storage
}

func (q *fanoutQuerier) Select(params *SelectParams, matchers ...*labels.Matcher) (SeriesSet, error) {
	shardIDs, err := meta.Router().GetShardIDsByTimeSpan(time.Time(q.mint), time.Time(q.maxt), matchers...)
	if err != nil {
		return emptySeriesSet, err
	}

	shardVisitor, found := visitor.GetVisitor(vars.Cfg.Gateway.QueryStrategy)
	if !found {
		return emptySeriesSet, errors.Errorf("query strategy %v not found", vars.Cfg.Gateway.QueryStrategy)
	}

	queriers := make([]Querier, 0, len(shardIDs))
	for _, shardID := range shardIDs {
		if shardID == "" {
			continue
		}

		queriers = append(queriers, &querier{
			ctx:  q.ctx,
			mint: q.mint,
			maxt: q.maxt,
			client: &ShardClient{
				shardID:      shardID,
				localStorage: q.localStorage,
				exeQuery:     shardVisitor,
			},
		})
	}

	q.Querier = NewMergeQuerier(queriers)
	return q.Querier.Select(params, matchers...)
}

func (q *fanoutQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	shardIDs, err := meta.Router().GetShardIDsByTimeSpan(time.Time(q.mint), time.Time(q.maxt), matchers...)
	if err != nil {
		return nil, err
	}

	shardVisitor, found := visitor.GetVisitor(vars.Cfg.Gateway.QueryStrategy)
	if !found {
		return nil, errors.Errorf("query strategy %v not found", vars.Cfg.Gateway.QueryStrategy)
	}

	queriers := make([]Querier, 0, len(shardIDs))
	for _, shardID := range shardIDs {
		if shardID == "" {
			continue
		}

		queriers = append(queriers, &querier{
			ctx:  q.ctx,
			mint: q.mint,
			maxt: q.maxt,
			client: &ShardClient{
				shardID:      shardID,
				localStorage: q.localStorage,
				exeQuery:     shardVisitor,
			},
		})
	}

	q.Querier = NewMergeQuerier(queriers)
	return q.Querier.LabelValues(name, matchers...)
}

func (q *fanoutQuerier) Close() error {
	if q.Querier != nil {
		return q.Querier.Close()
	}
	return nil
}

// mergeQuerier implements Querier.
type mergeQuerier struct {
	queriers []Querier
}

// NewMergeQuerier returns a new Querier that merges results of input queriers.
// NB NewMergeQuerier will return NoopQuerier if no queriers are passed to it,
// and will filter NoopQueriers from its arguments, in order to reduce overhead
// when only one querier is passed.
func NewMergeQuerier(queriers []Querier) Querier {
	filtered := make([]Querier, 0, len(queriers))
	for _, querier := range queriers {
		if querier != NoopQuerier() {
			filtered = append(filtered, querier)
		}
	}

	switch len(filtered) {
	case 0:
		return NoopQuerier()
	case 1:
		return filtered[0]
	default:
		return &mergeQuerier{
			queriers: filtered,
		}
	}
}

// Select returns a set of series that matches the given label matchers.
func (q *mergeQuerier) Select(params *SelectParams, matchers ...*labels.Matcher) (SeriesSet, error) {
	var (
		errors     = make([]error, len(q.queriers))
		seriesSets = make([]SeriesSet, len(q.queriers))
		wg         sync.WaitGroup
	)

	for i, querier := range q.queriers {
		wg.Add(1)
		go func(idx int, q Querier) {
			defer wg.Done()

			seriesSets[idx], errors[idx] = q.Select(params, matchers...)
		}(i, querier)
	}
	wg.Wait()

	multiErr := multierr.Combine(errors...)

	if multiErr != nil {
		return nil, multiErr
	}
	return NewMergeSeriesSet(seriesSets), nil
}

// LabelValues returns all potential values for a label name.
func (q *mergeQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	var (
		errors = make([]error, len(q.queriers))
		values = make([][]string, len(q.queriers))
		wg     sync.WaitGroup
	)

	for i, querier := range q.queriers {
		wg.Add(1)
		go func(idx int, q Querier) {
			defer wg.Done()

			values[idx], errors[idx] = q.LabelValues(name, matchers...)
		}(i, querier)
	}
	wg.Wait()

	multiErr := multierr.Combine(errors...)

	if multiErr != nil {
		return nil, multiErr
	}

	return mergeStringSlices(values), nil
}

func mergeStringSlices(ss [][]string) []string {
	switch len(ss) {
	case 0:
		return nil
	case 1:
		return ss[0]
	case 2:
		return mergeTwoStringSlices(ss[0], ss[1])
	default:
		halfway := len(ss) / 2
		return mergeTwoStringSlices(
			mergeStringSlices(ss[:halfway]),
			mergeStringSlices(ss[halfway:]),
		)
	}
}

func mergeTwoStringSlices(a, b []string) []string {
	i, j := 0, 0
	result := make([]string, 0, len(a)+len(b))
	for i < len(a) && j < len(b) {
		switch strings.Compare(a[i], b[j]) {
		case 0:
			result = append(result, a[i])
			i++
			j++
		case -1:
			result = append(result, a[i])
			i++
		case 1:
			result = append(result, b[j])
			j++
		}
	}
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

// Close releases the resources of the Querier.
func (q *mergeQuerier) Close() error {
	// TODO return multiple errors?
	var lastErr error
	for _, querier := range q.queriers {
		if err := querier.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// mergeSeriesSet implements SeriesSet
type mergeSeriesSet struct {
	currentLabels labels.Labels
	currentSets   []SeriesSet
	heap          seriesSetHeap
	sets          []SeriesSet
}

// NewMergeSeriesSet returns a new series set that merges (deduplicates)
// series returned by the input series sets when iterating.
func NewMergeSeriesSet(sets []SeriesSet) SeriesSet {
	if len(sets) == 1 {
		return sets[0]
	}

	// Sets need to be pre-advanced, so we can introspect the label of the
	// series under the cursor.
	var h seriesSetHeap
	for _, set := range sets {
		if set.Next() {
			heap.Push(&h, set)
		}
	}
	return &mergeSeriesSet{
		heap: h,
		sets: sets,
	}
}

func (c *mergeSeriesSet) Next() bool {
	// Firstly advance all the current series sets.  If any of them have run out
	// we can drop them, otherwise they should be inserted back into the heap.
	for _, set := range c.currentSets {
		if set.Next() {
			heap.Push(&c.heap, set)
		}
	}
	if len(c.heap) == 0 {
		return false
	}

	// Now, pop items of the heap that have equal label sets.
	c.currentSets = nil
	c.currentLabels = c.heap[0].At().Labels()
	for len(c.heap) > 0 && labels.Equal(c.currentLabels, c.heap[0].At().Labels()) {
		set := heap.Pop(&c.heap).(SeriesSet)
		c.currentSets = append(c.currentSets, set)
	}
	return true
}

func (c *mergeSeriesSet) At() Series {
	if len(c.currentSets) == 1 {
		return c.currentSets[0].At()
	}
	var series []Series
	for _, seriesSet := range c.currentSets {
		series = append(series, seriesSet.At())
	}
	return &mergeSeries{
		labels: c.currentLabels,
		series: series,
	}
}

func (c *mergeSeriesSet) Err() error {
	for _, set := range c.sets {
		if err := set.Err(); err != nil {
			return err
		}
	}
	return nil
}

type seriesSetHeap []SeriesSet

func (h seriesSetHeap) Len() int      { return len(h) }
func (h seriesSetHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h seriesSetHeap) Less(i, j int) bool {
	a, b := h[i].At().Labels(), h[j].At().Labels()
	return labels.Compare(a, b) < 0
}

func (h *seriesSetHeap) Push(x interface{}) {
	*h = append(*h, x.(SeriesSet))
}

func (h *seriesSetHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type mergeSeries struct {
	labels labels.Labels
	series []Series
}

func (m *mergeSeries) Labels() labels.Labels {
	return m.labels
}

func (m *mergeSeries) Iterator() SeriesIterator {
	iterators := make([]SeriesIterator, 0, len(m.series))
	for _, s := range m.series {
		iterators = append(iterators, s.Iterator())
	}
	return newMergeIterator(iterators)
}

type mergeIterator struct {
	iterators []SeriesIterator
	h         seriesIteratorHeap
}

func newMergeIterator(iterators []SeriesIterator) SeriesIterator {
	return &mergeIterator{
		iterators: iterators,
		h:         nil,
	}
}

func (c *mergeIterator) Seek(t int64) bool {
	c.h = seriesIteratorHeap{}
	for _, iter := range c.iterators {
		if iter.Seek(t) {
			heap.Push(&c.h, iter)
		}
	}
	return len(c.h) > 0
}

func (c *mergeIterator) At() (t int64, v float64) {
	if len(c.h) == 0 {
		panic("mergeIterator.At() called after .Next() returned false.")
	}

	// TODO do I need to dedupe or just merge?
	return c.h[0].At()
}

func (c *mergeIterator) Next() bool {
	if c.h == nil {
		for _, iter := range c.iterators {
			if iter.Next() {
				heap.Push(&c.h, iter)
			}
		}
		return len(c.h) > 0
	}

	if len(c.h) == 0 {
		return false
	}

	iter := heap.Pop(&c.h).(SeriesIterator)
	if iter.Next() {
		heap.Push(&c.h, iter)
	}

	return len(c.h) > 0
}

func (c *mergeIterator) Err() error {
	for _, iter := range c.iterators {
		if err := iter.Err(); err != nil {
			return err
		}
	}
	return nil
}

type seriesIteratorHeap []SeriesIterator

func (h seriesIteratorHeap) Len() int      { return len(h) }
func (h seriesIteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h seriesIteratorHeap) Less(i, j int) bool {
	at, _ := h[i].At()
	bt, _ := h[j].At()
	return at < bt
}

func (h *seriesIteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(SeriesIterator))
}

func (h *seriesIteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (f *Fanout) Appender() (Appender, error) {
	return &fanoutAppender{
		appenders:    make(map[string]*appender),
		localStorage: f.localStorage,
	}, nil
}

type fanoutAppender struct {
	appenders    map[string]*appender
	localStorage *storage.Storage
}

func (fanoutApp *fanoutAppender) Add(l []msg.Label, t int64, v float64, hash uint64) error {
	shardID, err := meta.Router().GetShardIDByLabels(time.Time(t), l, hash)
	if err != nil {
		return err
	}

	app, found := fanoutApp.appenders[shardID]
	if !found {
		app, err = newAppender(shardID, fanoutApp.localStorage)
		if err != nil {
			return err
		}

		fanoutApp.appenders[shardID] = app
	}

	return app.Add(l, t, v, hash)
}

func (fanoutApp *fanoutAppender) Flush() error {
	var multiErr error
	for _, app := range fanoutApp.appenders {
		if err := app.Flush(); err != nil {
			multiErr = multierr.Append(multiErr, err)
		}
	}
	return multiErr
}
