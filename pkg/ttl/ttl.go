// Copyright 2023 The PromiseDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttl

import (
	"sync/atomic"
	"time"
)

type TTL struct {
	started  *atomic.Bool
	eventCh  chan struct{}
	timeHeap *timeHeap
	deleter  func(key string) error
}

func NewTTL(deleter func(key string) error) *TTL {
	return &TTL{
		started: &atomic.Bool{},
		eventCh: make(chan struct{}),
		timeHeap: &timeHeap{
			h: h{
				heap:  make([]*Job, 0),
				index: make(map[string]int),
			},
		},
		deleter: deleter,
	}
}

func (ttl *TTL) Add(job *Job) {
	ttl.timeHeap.push(job)
	ttl.notify()
}

func (ttl *TTL) Delete(key string) {
	ttl.timeHeap.remove(key)
	ttl.notify()
}

func (ttl *TTL) IsExpired(key string) bool {
	return ttl.timeHeap.isExpired(key)
}

func (ttl *TTL) Start() {
	ttl.started.Store(true)

	for {
		if !ttl.started.Load() {
			break
		}

		ttl.exec()
	}
}

func (ttl *TTL) Stop() {
	ttl.started.Store(false)
	close(ttl.eventCh)
}

const maxDuration time.Duration = 1<<63 - 1

func (ttl *TTL) exec() {
	now := time.Now()
	duration := maxDuration
	job := ttl.timeHeap.peek()
	if job != nil {
		if job.Expiration.After(now) {
			duration = job.Expiration.Sub(now)
		} else {
			duration = 0
		}
	}

	if duration > 0 {
		timer := time.NewTimer(duration)
		defer timer.Stop()

		select {
		case <-ttl.eventCh:
			return
		case <-timer.C:
		}
	}

	job = ttl.timeHeap.pop()
	if job == nil {
		return
	}

	go func() {
		err := ttl.deleter(job.Key)
		if err != nil {

		}
	}()
}

func (ttl *TTL) notify() {
	if ttl.started.Load() {
		ttl.eventCh <- struct{}{}
	}
}
