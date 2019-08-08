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

package backend

import (
	"context"
	"github.com/baudtime/baudtime/backend/storage"
	"github.com/baudtime/baudtime/msg/pb"
	backendpb "github.com/baudtime/baudtime/msg/pb/backend"
	"github.com/pkg/errors"
	"sync"
)

var (
	pointsPool = sync.Pool{
		New: func() interface{} {
			return make([]pb.Point, 0, 60)
		},
	}
	seriesPool = &sync.Pool{
		New: func() interface{} {
			return make([]*pb.Series, 0)
		},
	}
)

type seriesHashMap map[uint64][]*pb.Series

func (m seriesHashMap) get(hash uint64, lset []pb.Label) *pb.Series {
OUTLOOP:
	for _, s := range m[hash] {
		if len(s.Labels) != len(lset) {
			continue OUTLOOP
		}

		for i, l := range lset {
			if s.Labels[i] != l {
				continue OUTLOOP
			}
		}

		return s
	}
	return nil
}

func (m seriesHashMap) set(hash uint64, s *pb.Series) {
	ss, found := m[hash]
	if !found {
		ss = seriesPool.Get().([]*pb.Series)
	}
	m[hash] = append(ss, s)
}

func (m seriesHashMap) del(hash uint64) {
	if ss, found := m[hash]; found {
		delete(m, hash)
		seriesPool.Put(ss[:0])
	}
}

type appender struct {
	client Client
	series seriesHashMap
}

func newAppender(shardID string, localStorage *storage.Storage) (*appender, error) {
	if shardID == "" {
		return nil, errors.New("invalid backend shard id")
	}

	return &appender{
		client: &ShardClient{
			shardID:      shardID,
			localStorage: localStorage,
		},
		series: seriesHashMap{},
	}, nil
}

func (app *appender) Add(l []pb.Label, t int64, v float64, hash uint64) error {
	s := app.series.get(hash, l)
	if s == nil {
		s = &pb.Series{
			Labels: l,
			Points: pointsPool.Get().([]pb.Point),
		}
		app.series.set(hash, s)
	}
	s.Points = append(s.Points, pb.Point{T: t, V: v})
	return nil
}

func (app *appender) Flush() error {
	series := seriesPool.Get().([]*pb.Series)

	for k, ss := range app.series {
		series = append(series, ss...)
		app.series.del(k)
	}
	err := app.client.Add(context.TODO(), &backendpb.AddRequest{Series: series})

	for _, s := range series {
		s.Labels = nil
		pointsPool.Put(s.Points[:0])
	}
	seriesPool.Put(series[:0])

	if err != nil {
		return errors.Wrap(err, "failed to flush series")
	}
	return nil
}
