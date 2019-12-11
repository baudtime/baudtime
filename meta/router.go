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
func (r *router) GetShardIDByLabels(t time.Time, lbls []msg.Label, hash uint64, createIfAbsent bool) (string, error) {
	n := tickNO(t)

	shardGroup, err := r.meta.getShardGroup(n)
	if err != nil {
		return "", err
	}

	if len(shardGroup) == 0 {
		if createIfAbsent && n <= tickNO(time.Now()) {
			shardGroup, err = r.meta.createShardGroup(n)
			if err != nil {
				return "", err
			}
			if len(shardGroup) == 0 {
				return "", errors.New("can't create a new shard group")
			}
		} else {
			return "", nil
		}
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
		return nil, nil
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

	shardNum := len(r.meta.AllShards())
	if shardNum == 0 {
		return nil, nil
	}

	idSet := make(map[string]struct{})

	if ids, err := r.GetShardIDsByTime(to, matchers...); err != nil {
		multiErr = multierr.Append(multiErr, err)
	} else {
		for _, id := range ids {
			idSet[id] = struct{}{}
			if len(idSet) == shardNum {
				return setToSlice(idSet), nil
			}
		}
	}

	for t := from; t.Before(to); t = t.Add(time.Duration(vars.Cfg.Gateway.Route.ShardGroupTickInterval)) {
		if ids, err := r.GetShardIDsByTime(t, matchers...); err != nil {
			multiErr = multierr.Append(multiErr, err)
		} else {
			for _, id := range ids {
				idSet[id] = struct{}{}
				if len(idSet) == shardNum {
					return setToSlice(idSet), nil
				}
			}
		}
	}

	return setToSlice(idSet), multiErr
}

func tickNO(t time.Time) uint64 {
	return uint64(t.Sub(baseTime) / time.Duration(vars.Cfg.Gateway.Route.ShardGroupTickInterval))
}

func setToSlice(set map[string]struct{}) []string {
	slice := make([]string, 0, len(set))
	for elem := range set {
		slice = append(slice, elem)
	}
	return slice
}
