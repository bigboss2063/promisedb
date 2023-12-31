// Copyright 2023 The ApexDB Authors
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

package promisedb

import (
	"sync"
)

type HashTable struct {
	index sync.Map
}

func (h *HashTable) Put(key string, dataPost *DataPos) {
	h.index.Store(key, dataPost)
}

func (h *HashTable) Get(key string) *DataPos {
	value, ok := h.index.Load(key)
	if ok {
		return value.(*DataPos)
	}
	return nil
}

func (h *HashTable) Del(key string) {
	h.index.Delete(key)
}

func (h *HashTable) Size() int {
	size := 0
	h.index.Range(func(key, value any) bool {
		size++
		return true
	})
	return size
}
