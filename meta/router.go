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

package meta

import (
	"github.com/baudtime/baudtime/msg"
	"github.com/baudtime/baudtime/vars"
	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"go.uber.org/multierr"
	"time"
)

var (
	baseTime, _  = time.Parse("2006-01-02 15:04:05", "2019-01-01 00:00:00")
	globalRouter = router{meta: &globalMeta}
	routingKey   = "__rk__"
)

//router's responsibility is computing
type router struct {
	meta *meta
}

func Router() *router {
	return &globalRouter
}

//used by write
func (r *router) GetShardIDByLabels(t time.Time, lbls []msg.Label, hash uint64) (string, error) {
	shardGroup, err := r.meta.getShardGroup(tickNO(t))
	if err != nil {
		return "", err
	}

	if len(shardGroup) == 0 {
		return "", errors.New("shard group is empty")
	}

	for _, l := range lbls {
		if l.Name == routingKey {
			idx := xxhash.Sum64String(l.Value) % uint64(len(shardGroup))
			return shardGroup[idx], nil
		}
	}

	idx := hash % uint64(len(shardGroup))
	return shardGroup[idx], nil
}

func (r *router) GetShardIDsByTime(t time.Time, matchers ...*labels.Matcher) ([]string, error) {
	shardGroup, err := r.meta.getShardGroup(tickNO(t))
	if err != nil {
		return nil, err
	}

	if len(shardGroup) == 0 {
		return nil, errors.New("shard group is empty")
	}

	for _, m := range matchers {
		if m.Name == routingKey && m.Type == labels.MatchEqual {
			idx := xxhash.Sum64String(m.Value) % uint64(len(shardGroup))
			return shardGroup[idx : idx+1], nil
		}
	}

	return shardGroup, nil
}

//used by query
func (r *router) GetShardIDsByTimeSpan(from, to time.Time, matchers ...*labels.Matcher) ([]string, error) {
	var multiErr error
	idSet := make(map[string]struct{})

	for t := from; t.Before(to); t = t.Add(24 * time.Hour) {
		if ids, err := r.GetShardIDsByTime(t, matchers...); err != nil {
			multiErr = multierr.Append(multiErr, err)
		} else {
			for _, id := range ids {
				idSet[id] = struct{}{}
			}
		}
	}

	if ids, err := r.GetShardIDsByTime(to, matchers...); err != nil {
		multiErr = multierr.Append(multiErr, err)
	} else {
		for _, id := range ids {
			idSet[id] = struct{}{}
		}
	}

	ids := make([]string, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, id)
	}

	return ids, multiErr
}

func tickNO(t time.Time) uint64 {
	return uint64(t.Sub(baseTime) / time.Duration(vars.Cfg.Gateway.Route.ShardGroupTickInterval))
}
