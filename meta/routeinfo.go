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
	"github.com/baudtime/baudtime/vars"
	"sync"
)

type RouteInfo struct {
	Timeline uint64
	sync.Map
}

func (r *RouteInfo) Put(tickNO uint64, v []string) (evicted []uint64) {
	r.Map.Store(tickNO, v)

	if r.Timeline < tickNO {
		r.Timeline = tickNO

		tickNum := uint64(vars.Cfg.Gateway.Route.ShardGroupTTL / vars.Cfg.Gateway.Route.ShardGroupTickInterval)

		r.Map.Range(func(tickNO, value interface{}) bool {
			if r.Timeline-tickNO.(uint64) > tickNum {
				evicted = append(evicted, tickNO.(uint64))
			}
			return true
		})

		if len(evicted) > 0 {
			for t := range evicted {
				r.Map.Delete(t)
			}
		}
	}

	return
}

func (r *RouteInfo) Get(tickNO uint64) ([]string, bool) {
	v, found := r.Map.Load(tickNO)
	if found {
		return v.([]string), true
	}
	return nil, false
}
