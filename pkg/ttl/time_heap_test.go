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

	jh := NewJobHeap()

	job1 := NewJob("key1", time.Now().Add(time.Second*10))
	job2 := NewJob("key2", time.Now().Add(time.Second*20))
	job3 := NewJob("key3", time.Now().Add(time.Second*30))

	jh.Push(job1)
	jh.Push(job2)
	jh.Push(job3)

	assert.Equal(t, "key1", jh.Peek().Key)
	assert.Equal(t, 0, jh.heap.index["key1"])

	job4 := NewJob("key4", time.Now().Add(time.Second*5))
	jh.Push(job4)

	assert.Equal(t, "key4", jh.Peek().Key)
	assert.Equal(t, 0, jh.heap.index["key4"])

	jh.Update("key1", time.Now().Add(50*time.Millisecond))

	assert.Equal(t, "key1", jh.Peek().Key)
	assert.Equal(t, 0, jh.heap.index["key1"])

	jh.Remove("key2")
	assert.Nil(t, jh.Get("key2"))
}
