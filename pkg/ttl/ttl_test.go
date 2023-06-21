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
	"log"
	"testing"
	"time"
)

func TestTTL(t *testing.T) {
	ttl := NewTTL(func(key string) error {
		log.Printf("deleted %v\n", key)
		return nil
	})

	go ttl.Start()

	now := time.Now()

	job1 := NewJob("key1", time.Unix(0, now.Add(time.Second*3).UnixNano()))
	job2 := NewJob("key2", time.Unix(0, now.Add(time.Second*5).UnixNano()))
	job3 := NewJob("key3", time.Unix(0, now.Add(time.Second*7).UnixNano()))
	job4 := NewJob("key4", time.Unix(0, now.Add(time.Second*6).UnixNano()))

	ttl.Add(job1)
	ttl.Add(job2)
	ttl.Add(job3)
	ttl.Add(job4)

	time.Sleep(1000 * time.Second)
}
