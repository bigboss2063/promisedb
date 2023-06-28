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

package promisedb

import (
	"github.com/bigboss2063/promisedb/pkg/util"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
	"time"
)

func BenchmarkDB_Put(b *testing.B) {
	opts := DefaultOptions()
	db, err := OpenDB(opts)
	assert.Nil(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		err := db.Put(util.GetTestKey(n), util.RandomBytes(1024))
		assert.Nil(b, err)
	}

	b.StopTimer()

	db.Close()
	os.RemoveAll(opts.Path)
}

func BenchmarkDB_Get(b *testing.B) {
	opts := DefaultOptions()
	db, err := OpenDB(opts)
	assert.Nil(b, err)

	for i := 0; i < 10000; i++ {
		err := db.Put(util.GetTestKey(i), util.RandomBytes(1024))
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	rand.Seed(time.Now().Unix())

	for i := 0; i < b.N; i++ {
		_, err := db.Get(util.GetTestKey(rand.Intn(10000)))
		if err != nil && err != ErrKeyNotExist {
			assert.Fail(b, err.Error())
		}
	}

	b.StopTimer()

	db.Close()
	os.RemoveAll(opts.Path)
}
