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
	"sync"
)

type RouteInfo struct {
	metricName string
	Timeline   uint64
	*sync.Map
	ShardGrpRouteK string
	sync.Mutex
}

func NewRouteInfo(metricName string) *RouteInfo {
	return &RouteInfo{
		metricName:     metricName,
		Timeline:       0,
		Map:            new(sync.Map),
		ShardGrpRouteK: "",
	}
}

func (r *RouteInfo) Put(day uint64, v []string) {
	if r.Timeline < day {
		r.Timeline = day
	}

	r.Map.Store(day, v)

	var toDelete []interface{}
	r.Map.Range(func(day, value interface{}) bool {
		if r.Timeline-day.(uint64) > 366 {
			toDelete = append(toDelete, day)
		}
		return true
	})

	if len(toDelete) > 0 {
		for d := range toDelete {
			r.Map.Delete(d)
		}
	}
}

func (r *RouteInfo) Get(day uint64) ([]string, bool) {
	v, found := r.Map.Load(day)
	if found {
		return v.([]string), true
	}
	return nil, false
}
