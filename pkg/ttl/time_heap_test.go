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
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestJobHeap(t *testing.T) {

	jh := &timeHeap{
		h: h{
			heap:  make([]*Job, 0),
			index: make(map[string]int),
		},
	}

	now := time.Now()

	job1 := NewJob("key1", time.Unix(0, now.Add(time.Second*1).UnixNano()))
	job2 := NewJob("key2", time.Unix(0, now.Add(time.Second*2).UnixNano()))
	job3 := NewJob("key3", time.Unix(0, now.Add(time.Second*3).UnixNano()))

	jh.push(job1)
	jh.push(job2)
	jh.push(job3)

	assert.Equal(t, "key1", jh.peek().Key)
	assert.Equal(t, 0, jh.h.index["key1"])

	job4 := NewJob("key4", time.Now().Add(time.Millisecond*500))
	jh.push(job4)

	assert.Equal(t, "key4", jh.peek().Key)
	assert.Equal(t, 0, jh.h.index["key4"])

	jh.push(NewJob("key1", time.Now().Add(50*time.Millisecond)))

	assert.Equal(t, "key1", jh.peek().Key)
	assert.Equal(t, 0, jh.h.index["key1"])

	jh.remove("key2")
	assert.Nil(t, jh.get("key2"))
}
