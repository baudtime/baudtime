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

package visitor

import (
	"github.com/baudtime/baudtime/meta"
	"github.com/baudtime/baudtime/msg"
	"github.com/hashicorp/go-multierror"
	"math/rand"
	"time"
)

var random = rand.New(rand.NewSource(time.Now().UnixNano()))

func init() {
	registry["random"] = randomVisit
}

func randomVisit(shard *meta.Shard, op func(node *meta.Node) (resp msg.Message, err error)) (resp msg.Message, err error) {
	var multiErr error

	i := random.Intn(len(shard.Slaves) + 1)
	if i > 0 {
		if resp, err = op(shard.Slaves[i-1]); err != nil {
			multiErr = multierror.Append(multiErr, err)
		} else {
			return
		}
	} else {
		if shard.Master != nil {
			if resp, err = op(shard.Master); err != nil {
				multiErr = multierror.Append(multiErr, err)
			} else {
				return
			}
		} else {
			meta.FailoverIfNeeded(shard.ID)
		}
	}

	return nil, multiErr
}
