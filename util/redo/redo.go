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

package redo

import (
	"time"
)

func Repeat(interval time.Duration, stopc <-chan struct{}, f func() error) error {
	tick := time.NewTicker(interval)
	defer tick.Stop()

	for {
		if err := f(); err != nil {
			return err
		}
		select {
		case <-stopc:
			return nil
		case <-tick.C:
		}
	}
}

func RetryWithTimeout(interval time.Duration, timeout time.Duration, f func() (bool, error)) error {
	var retry = true
	var err error
	now := time.Now()
	for {
		if retry, err = f(); !retry {
			return err
		}

		if time.Since(now) > timeout {
			return err
		}
		time.Sleep(interval)
	}
}

func Retry(interval time.Duration, count int, f func() (bool, error)) error {
	var retry = true
	var err error
	for i := 0; i < count; i++ {
		if retry, err = f(); !retry {
			return err
		}
		time.Sleep(interval)
	}
	return err
}
