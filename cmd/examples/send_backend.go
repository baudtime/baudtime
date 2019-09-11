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

package main

import (
	"fmt"
	"github.com/baudtime/baudtime/msg"
	backendmsg "github.com/baudtime/baudtime/msg/backend"
	"github.com/baudtime/baudtime/tcp/client"
	ts "github.com/baudtime/baudtime/util/time"
	"time"
)

func main() {
	cli := client.NewBackendClient("name", "localhost:8088", 2)

	var t int64

	s := make([]*msg.Series, 100)
	r := &backendmsg.AddRequest{s}

	for j := 0; j < 10000; j++ {
		for i := 0; i < 100; i++ {
			num := fmt.Sprintf("%d", i+j)

			lbs := []msg.Label{
				{"__name__", "test"},
				{"host", "localhost"},
				{"to", "backend"},
				{"idc", "langfang"},
				{"state", "0"},
				{"aaa", "xz"},
				{"bbb", "zz"},
				{"n", num},
				{"m", num + "_"},
			}

			t = ts.FromTime(time.Now())
			points := []msg.Point{{t, float64(i + j*100)}}

			r.Series[i] = &msg.Series{
				Labels: lbs,
				Points: points,
			}
		}

		cli.AsyncRequest(r, nil)
	}

	fmt.Println("complete")
}
