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

package replication

import (
	"encoding/json"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/baudtime/baudtime/util/os/fileutil"
	. "github.com/baudtime/baudtime/vars"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/tsdb"
)

const replicationMeta = "replication.json"

type Meta struct {
	RelationID string `json:"relation_id,omitempty"`
}

func loadRelationID(dir string) (*string, error) {
	b, err := ioutil.ReadFile(filepath.Join(dir, replicationMeta))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		} else {
			return nil, err
		}
	}

	var m Meta
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}

	if m.RelationID == "" {
		return nil, nil
	}

	level.Info(Logger).Log("msg", "relation id was loaded", "relationID", m.RelationID)
	return &m.RelationID, nil
}

func storeRelationID(dir, relationID string) error {
	path := filepath.Join(dir, replicationMeta)
	tmp := path + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "\t")
	err = enc.Encode(&Meta{relationID})
	if err != nil {
		f.Close()
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}

	err = fileutil.RenameFile(tmp, path)
	if err != nil {
		return err
	}

	level.Info(Logger).Log("msg", "relation id was stored", "relationID", relationID)
	return nil
}

func blocksMinTime(db *tsdb.DB) int64 {
	blocks := db.Blocks()
	if len(blocks) > 0 {
		return blocks[0].MinTime()
	}

	return math.MinInt64
}

var (
	strSlicePool = &sync.Pool{}
)

func getStrSlice() []string {
	v := strSlicePool.Get()
	if v != nil {
		return v.([]string)
	}

	return make([]string, 0, 3)
}

func putStrSlice(slice []string) {
	slice = slice[:0]
	strSlicePool.Put(slice)
}
