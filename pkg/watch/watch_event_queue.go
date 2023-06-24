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

package watch

import (
	"sync"
)

type watchEventQueue struct {
	queue  []*WatchEvent
	cond   *sync.Cond
	closed bool
}

func newWatchEventQueue() *watchEventQueue {
	return &watchEventQueue{
		queue:  make([]*WatchEvent, 0),
		cond:   sync.NewCond(&sync.Mutex{}),
		closed: false,
	}
}

func (q *watchEventQueue) read() *WatchEvent {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	for len(q.queue) == 0 && !q.closed {
		q.cond.Wait()
	}

	if len(q.queue) == 0 {
		return nil
	}

	event := q.queue[0]
	q.queue[0] = nil
	q.queue = q.queue[1:]

	return event
}

func (q *watchEventQueue) write(watchEvent *WatchEvent) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	if q.closed {
		return
	}

	q.queue = append(q.queue, watchEvent)
	q.cond.Signal()
}

func (q *watchEventQueue) close() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	if q.closed {
		return
	}

	q.closed = true
	q.cond.Broadcast()
}
