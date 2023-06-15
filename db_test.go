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

package ApexDB

import (
	"fmt"
	"github.com/bigboss2063/ApexDB/pkg/util"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func TestDB_Open_Close(t *testing.T) {
	db, err := OpenDB(DefaultOption())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.option.Path)
	assert.Nil(t, err)
}

func TestDB_Put(t *testing.T) {
	db, err := OpenDB(DefaultOption())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("key-1"), []byte("value-1"))
	assert.Nil(t, err)

	err = db.Put(nil, []byte("value-1"))
	assert.NotNil(t, err)

	err = db.Put([]byte("key-2"), nil)
	assert.Nil(t, err)

	for i := 0; i < 100000; i++ {
		err := db.Put([]byte(fmt.Sprintf("%09d", i)), util.RandomBytes(1024))
		assert.Nil(t, err)
	}

	err = db.Close()
	assert.Nil(t, err)

	db, err = OpenDB(DefaultOption())

	for i := 0; i < 100000; i++ {
		err := db.Put([]byte(fmt.Sprintf("%09d", i)), util.RandomBytes(1024))
		assert.Nil(t, err)
	}

	assert.Equal(t, len(db.archivedFiles), 3)
	assert.Equal(t, db.keyDir.Size(), 100002)

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.option.Path)
	assert.Nil(t, err)
}

func TestDB_Get(t *testing.T) {
	db, err := OpenDB(DefaultOption())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("key-1"), []byte("value-1"))
	assert.Nil(t, err)
	err = db.Put([]byte("key-2"), nil)
	assert.Nil(t, err)

	entry, err := db.Get([]byte("key-1"))
	assert.Nil(t, err)
	assert.Equal(t, entry.Value, []byte("value-1"))

	entry, err = db.Get(nil)
	assert.NotNil(t, err)
	assert.Equal(t, err, ErrKeyIsEmpty)

	entry, err = db.Get([]byte("key-2"))
	assert.Nil(t, err)
	assert.Len(t, entry.Value, 0)

	entry, err = db.Get([]byte("key-3"))
	assert.NotNil(t, err)
	assert.Equal(t, err, ErrKeyNotExist)

	for i := 0; i < 200000; i++ {
		err := db.Put([]byte(fmt.Sprintf("%09d", i)), []byte(fmt.Sprintf("%09d", i)))
		assert.Nil(t, err)
	}

	for i := 0; i < 200000; i++ {
		et, err := db.Get([]byte(fmt.Sprintf("%09d", i)))
		assert.Nil(t, err)
		assert.Equal(t, et.Value, []byte(fmt.Sprintf("%09d", i)))
	}

	err = db.Close()
	assert.Nil(t, err)

	db, err = OpenDB(DefaultOption())

	for i := 0; i < 200000; i++ {
		et, err := db.Get([]byte(fmt.Sprintf("%09d", i)))
		assert.Nil(t, err)
		assert.Equal(t, et.Value, []byte(fmt.Sprintf("%09d", i)))
	}

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.option.Path)
	assert.Nil(t, err)
}

func TestDB_Del(t *testing.T) {
	db, err := OpenDB(DefaultOption())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("key-1"), []byte("value-1"))
	assert.Nil(t, err)

	err = db.Del([]byte("key-1"))
	assert.Nil(t, err)

	entry, err := db.Get([]byte("key-1"))
	assert.NotNil(t, err)
	assert.Nil(t, entry)
	assert.Equal(t, err, ErrKeyNotExist)

	err = db.Del([]byte("key-1"))
	assert.NotNil(t, err)
	assert.Equal(t, err, ErrKeyNotExist)

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.option.Path)
	assert.Nil(t, err)
}

func TestDB_Replace_Active_File(t *testing.T) {
	db, err := OpenDB(DefaultOption())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 100000; i++ {
		err := db.Put([]byte(fmt.Sprintf("%09d", i)), util.RandomBytes(1024))
		assert.Nil(t, err)
	}

	assert.NotEqual(t, len(db.archivedFiles), 0)

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.option.Path)
	assert.Nil(t, err)
}

func TestDB_Concurrency(t *testing.T) {
	db, err := OpenDB(DefaultOption())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wg := new(sync.WaitGroup)

	wg.Add(200000)
	for i := 0; i < 200000; i++ {
		go func(i int) {
			err := db.Put([]byte(fmt.Sprintf("%09d", i)), []byte(fmt.Sprintf("%09d", i)))
			assert.Nil(t, err)
			wg.Done()
		}(i)
	}
	wg.Wait()

	wg.Add(200000)
	for i := 0; i < 200000; i++ {
		go func(i int) {
			et, err := db.Get([]byte(fmt.Sprintf("%09d", i)))
			assert.Nil(t, err)
			assert.Equal(t, et.Value, []byte(fmt.Sprintf("%09d", i)))
			wg.Done()
		}(i)
	}
	wg.Wait()

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.option.Path)
	assert.Nil(t, err)
}

func TestDB_Compaction(t *testing.T) {
	db, err := OpenDB(DefaultOption())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 1000000; i++ {
		err := db.Put([]byte(fmt.Sprintf("%09d", i)), util.RandomBytes(1024))
		assert.Nil(t, err)
	}

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 500000; i++ {
		randNum := rand.Intn(500000)
		err = db.Del([]byte(fmt.Sprintf("%09d", randNum)))
		if err != nil && err != ErrKeyNotExist {
			assert.Nil(t, err)
		}
	}

	err = db.Compaction()
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.option.Path)
	assert.Nil(t, err)
}
