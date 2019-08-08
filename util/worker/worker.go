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

package worker

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/baudtime/baudtime/vars"
	"github.com/go-kit/kit/log/level"
)

type worker struct {
	id      int
	ch      chan func(id int)
	running uint32
}

func (w *worker) run() {
	for f := range w.ch {
		if !w.isRunning() {
			return
		}
		f(w.id)
	}
}

func (w *worker) exit() {
	if atomic.CompareAndSwapUint32(&w.running, 1, 0) {
		close(w.ch)
	}
}

func (w *worker) isRunning() bool {
	return atomic.LoadUint32(&w.running) == 1
}

type WorkerPool struct {
	name    string
	workers []*worker
	size    int
	iter    uint64
	wg      sync.WaitGroup
	running uint32
}

func NewWorkerPool(name string, size int) *WorkerPool {
	if size > runtime.GOMAXPROCS(0) || size <= 0 {
		size = runtime.GOMAXPROCS(0)
	}

	pool := &WorkerPool{
		name:    name,
		workers: make([]*worker, size),
		size:    size,
		running: 1,
	}

	for i := 0; i < size; i++ {
		pool.workers[i] = &worker{
			id:      i,
			ch:      make(chan func(id int), 1024),
			running: 1,
		}

		pool.wg.Add(1)
		go func(idx int) {
			pool.workers[idx].run()
			pool.wg.Done()
		}(i)
	}

	return pool
}

func (pool *WorkerPool) Submit(task func(id int)) {
	if pool.isRunning() {
		var wk *worker

		iter := atomic.AddUint64(&pool.iter, 1)
		for i := 0; i < pool.size; i++ {
			wk = pool.workers[(iter+uint64(i))%uint64(pool.size)]
			select {
			case wk.ch <- task:
				return
			default:
			}
		}

		wk.ch <- task
	}
}

func (pool *WorkerPool) isRunning() bool {
	return atomic.LoadUint32(&pool.running) == 1
}

func (pool *WorkerPool) Shutdown() {
	if atomic.CompareAndSwapUint32(&pool.running, 1, 0) {
		for _, wk := range pool.workers {
			go wk.exit()
		}
		pool.wg.Wait()
		if vars.Logger != nil {
			level.Debug(vars.Logger).Log("msg", "worker pool exit", "poolName", pool.name)
		}
	}
}
