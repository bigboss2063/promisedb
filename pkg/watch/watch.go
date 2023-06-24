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
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type EventType uint8

const (
	Put EventType = iota
	Del
)

type WatcherManager struct {
	lock          *sync.RWMutex
	keyToWatchers map[string]map[*Watcher]struct{}
	queue         *watchEventQueue
	closeCh       chan struct{}
	nextWatcherId *atomic.Uint64
}

func NewWatcherManager() *WatcherManager {
	return &WatcherManager{
		lock:          &sync.RWMutex{},
		keyToWatchers: make(map[string]map[*Watcher]struct{}),
		queue:         newWatchEventQueue(),
		closeCh:       make(chan struct{}),
		nextWatcherId: &atomic.Uint64{},
	}
}

func (wm *WatcherManager) closeWatcherListener(watcher *Watcher) {
	select {
	case <-watcher.ctx.Done():
		wm.lock.Lock()
		wm.UnWatch(watcher)
		wm.lock.Unlock()
	case <-wm.closeCh:
		return
	}
}

func (wm *WatcherManager) Watch(ctx context.Context, key string) <-chan *WatchEvent {

	watcher := &Watcher{
		key:      key,
		ctx:      ctx,
		respCh:   make(chan *WatchEvent, 1024),
		canceled: false,
	}

	wm.lock.Lock()
	defer wm.lock.Unlock()

	if _, ok := wm.keyToWatchers[key]; ok {
		wm.keyToWatchers[key][watcher] = struct{}{}
	} else {
		wm.keyToWatchers[key] = make(map[*Watcher]struct{})
		wm.keyToWatchers[key][watcher] = struct{}{}
	}

	go wm.closeWatcherListener(watcher)

	return watcher.respCh
}

func (wm *WatcherManager) UnWatch(watcher *Watcher) {
	if !watcher.canceled {
		close(watcher.respCh)
		watcher.canceled = true
	}

	delete(wm.keyToWatchers[watcher.key], watcher)
	if len(wm.keyToWatchers[watcher.key]) == 0 {
		delete(wm.keyToWatchers, watcher.key)
	}
}

func (wm *WatcherManager) Notify(watchEvent *WatchEvent) {
	wm.queue.write(watchEvent)
}

func (wm *WatcherManager) Start() {
	for {
		event := wm.queue.read()
		if event == nil {
			break
		}

		wm.lock.RLock()
		for watcher := range wm.keyToWatchers[event.Key] {
			if watcher.canceled {
				continue
			}

			watcher.sendResp(event)
		}
		wm.lock.RUnlock()
	}
}

func (wm *WatcherManager) Close() {
	wm.queue.close()

	close(wm.closeCh)

	wm.lock.Lock()
	defer wm.lock.Unlock()

	for _, watchers := range wm.keyToWatchers {
		for watcher := range watchers {
			wm.UnWatch(watcher)
		}
	}
}

type Watcher struct {
	key      string
	ctx      context.Context
	respCh   chan *WatchEvent
	canceled bool
}

func (w *Watcher) sendResp(event *WatchEvent) {
	timeout := 100 * time.Millisecond
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case w.respCh <- event:
	case <-w.ctx.Done():
	case <-timer.C:
	}
}

type WatchEvent struct {
	Key       string
	Value     []byte
	EventType EventType
}
