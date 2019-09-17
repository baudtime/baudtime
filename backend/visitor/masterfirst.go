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
	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

func init() {
	registry["masterfirst"] = masterFirst
}

func masterFirst(shard *meta.Shard, op func(node *meta.Node) (resp msg.Message, err error)) (resp msg.Message, err error) {
	var multiErr error

	if shard.Master != nil {
		if resp, err = op(shard.Master); err != nil {
			multiErr = multierr.Append(multiErr, err)
		} else {
			return
		}
	}

	defer meta.FailoverIfNeeded(shard.ID)

	if len(shard.Slaves) > 0 {
		for _, node := range shard.Slaves {
			if resp, err = op(node); err != nil {
				multiErr = multierr.Append(multiErr, err)
			} else {
				return
			}
		}
	}

	if multiErr != nil {
		return nil, multiErr
	} else {
		return nil, errors.Errorf("shard %v has no available data node", shard.ID)
	}
}
