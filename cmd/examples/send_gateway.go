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
	"context"
	"fmt"
	"github.com/baudtime/baudtime/msg"
	"github.com/baudtime/baudtime/msg/gateway"
	"github.com/baudtime/baudtime/tcp/client"
	ts "github.com/baudtime/baudtime/util/time"
	"sync"
	"time"
)

func send_gateway() {
	addrProvider := client.NewStaticAddrProvider("localhost:8088")
	cli := client.NewGatewayClient("name", addrProvider)

	var t int64

	s := make([]*msg.Series, 10)
	r := &gateway.AddRequest{s}

	for j := 0; j < 10000; j++ {
		for i := 0; i < 10; i++ {
			lbs := []msg.Label{
				{"__name__", "test"},
				{"host", "localhost"},
				{"to", "gateway"},
				{"idc", "langfang"},
				{"state", "0"},
				{"aaa", "xz"},
				{"bbb", "zz"},
				{"j", fmt.Sprintf("%d", j)},
				{"i", fmt.Sprintf("%d", i)},
			}

			t = ts.FromTime(time.Now())
			points := []msg.Point{{t, float64(i)}}

			r.Series[i] = &msg.Series{
				Labels: lbs,
				Points: points,
			}
		}

		_, err := cli.SyncRequest(context.Background(), r)
		time.Sleep(2 * time.Second)
		if err != nil {
			fmt.Println(err)
		}

	}
}

func main() {
	var wg sync.WaitGroup

	count := 1

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			send_gateway()
		}()
	}

	wg.Wait()

	fmt.Println("complete")
}
