// Copyright 2017 The Prometheus Authors
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
	"github.com/baudtime/baudtime/msg"
	"github.com/prometheus/prometheus/pkg/labels"
)

type Backend interface {
	Queryable

	// StartTime returns the oldest timestamp stored in the storage.
	StartTime() (int64, error)

	// Appender returns a new appender against the storage.
	Appender() (Appender, error)

	// Close closes the storage and all its underlying resources.
	Close() error
}

// A Queryable handles queries against a storage.
type Queryable interface {
	// Querier returns a new Querier on the storage.
	Querier(ctx context.Context, mint, maxt int64) (Querier, error)
}

// Querier provides reading access to time series data.
type Querier interface {
	// Select returns a set of series that matches the given label matchers.
	//Select(*SelectParams, ...*labels.Matcher) (SeriesSet, error)
	Select(*SelectParams, ...*labels.Matcher) (SeriesSet, error)
	// LabelValues returns all potential values for a label name.
	LabelValues(string, ...*labels.Matcher) ([]string, error)

	// Close releases the resources of the Querier.
	Close() error
}

// Appender provides batched appends against a storage.
type Appender interface {
	Add(l []msg.Label, t int64, v float64, hash uint64) error
	Flush() error
}

// SelectParams specifies parameters passed to data selections.
type SelectParams struct {
	Step       int64  // Query step size in milliseconds.
	OnlyLabels bool   // Query meta data only, no samples, only labels
	Func       string // String representation of surrounding function or aggregation.
}

// SeriesSet contains a set of series.
type SeriesSet interface {
	Next() bool
	At() Series
	Err() error
}

// Series represents a single time series.
type Series interface {
	// Labels returns the complete set of labels identifying the series.
	Labels() labels.Labels

	// Iterator returns a new iterator of the data of the series.
	Iterator() SeriesIterator
}

// SeriesIterator iterates over the data of a time series.
type SeriesIterator interface {
	// Seek advances the iterator forward to the value at or after
	// the given timestamp.
	Seek(t int64) bool
	// At returns the current timestamp/value pair.
	At() (t int64, v float64)
	// Next advances the iterator by one.
	Next() bool
	// Err returns the current error.
	Err() error
}

// QueryableFunc is an adapter to allow the use of ordinary functions as
// Queryables. It follows the idea of http.HandlerFunc.
type QueryableFunc func(ctx context.Context, mint, maxt int64) (Querier, error)

// Querier calls f() with the given parameters.
func (f QueryableFunc) Querier(ctx context.Context, mint, maxt int64) (Querier, error) {
	return f(ctx, mint, maxt)
}

// errSeriesSet implements SeriesSet, just returning an error.
type errSeriesSet struct {
	err error
}

func (errSeriesSet) Next() bool {
	return false
}

func (errSeriesSet) At() Series {
	return nil
}

func (e errSeriesSet) Err() error {
	return e.err
}

var emptySeriesSet = errSeriesSet{}

// EmptySeriesSet returns a series set that's always empty.
func EmptySeriesSet() SeriesSet {
	return emptySeriesSet
}
