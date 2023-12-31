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
	"context"
	"fmt"
	"github.com/bigboss2063/promisedb/pkg/util"
	"github.com/bigboss2063/promisedb/pkg/watch"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func TestDB_Open_Close(t *testing.T) {
	db, err := OpenDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.options.Path)
	assert.Nil(t, err)
}

func TestDB_Put(t *testing.T) {
	db, err := OpenDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// put a normal key-value pair
	err = db.Put([]byte("key-1"), []byte("value-1"))
	assert.Nil(t, err)

	// put a key-value pair with a nil key but a value
	err = db.Put(nil, []byte("value-1"))
	assert.NotNil(t, err)

	// put a key-value pair with a nil value but a key
	err = db.Put([]byte("key-2"), nil)
	assert.Nil(t, err)

	// put a large number of key-value pairs so that the active file is replaced
	for i := 0; i < 100000; i++ {
		err := db.Put([]byte(fmt.Sprintf("%09d", i)), util.RandomBytes(1024))
		assert.Nil(t, err)
	}

	err = db.Close()
	assert.Nil(t, err)

	db, err = OpenDB(DefaultOptions())

	// After restarting, read key-value pairs from multiple data files
	for i := 0; i < 100000; i++ {
		err := db.Put([]byte(fmt.Sprintf("%09d", i)), util.RandomBytes(1024))
		assert.Nil(t, err)
	}

	assert.Equal(t, len(db.archivedFiles), 3)
	assert.Equal(t, db.keyDir.Size(), 100002)

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.options.Path)
	assert.Nil(t, err)
}

func TestDB_Get(t *testing.T) {
	db, err := OpenDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("key-1"), []byte("value-1"))
	assert.Nil(t, err)
	err = db.Put([]byte("key-2"), nil)
	assert.Nil(t, err)

	// get a key-value pair normally
	entry, err := db.Get([]byte("key-1"))
	assert.Nil(t, err)
	assert.Equal(t, entry.Value, []byte("value-1"), 0)

	// get a key-value pair with nil key
	entry, err = db.Get(nil)
	assert.NotNil(t, err)
	assert.Equal(t, err, ErrKeyIsEmpty)

	// get a key-value pair with nil value
	entry, err = db.Get([]byte("key-2"))
	assert.Nil(t, err)
	assert.Len(t, entry.Value, 0)

	// get a key-value pair that do not exist
	entry, err = db.Get([]byte("key-3"))
	assert.NotNil(t, err)
	assert.Equal(t, err, ErrKeyNotExist)

	for i := 0; i < 200000; i++ {
		err := db.Put([]byte(fmt.Sprintf("%09d", i)), []byte(fmt.Sprintf("%09d", i)))
		assert.Nil(t, err)
	}

	// get key-value pairs from multi data files
	for i := 0; i < 200000; i++ {
		et, err := db.Get([]byte(fmt.Sprintf("%09d", i)))
		assert.Nil(t, err)
		assert.Equal(t, et.Value, []byte(fmt.Sprintf("%09d", i)))
	}

	err = db.Close()
	assert.Nil(t, err)

	// get key-value pairs from multi data files after restarting
	db, err = OpenDB(DefaultOptions())

	for i := 0; i < 200000; i++ {
		et, err := db.Get([]byte(fmt.Sprintf("%09d", i)))
		assert.Nil(t, err)
		assert.Equal(t, et.Value, []byte(fmt.Sprintf("%09d", i)))
	}

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.options.Path)
	assert.Nil(t, err)
}

func TestDB_Del(t *testing.T) {
	db, err := OpenDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("key-1"), []byte("value-1"))
	assert.Nil(t, err)

	// Delete a key-value pair normally
	err = db.Del([]byte("key-1"))
	assert.Nil(t, err)

	entry, err := db.Get([]byte("key-1"))
	assert.NotNil(t, err)
	assert.Nil(t, entry)
	assert.Equal(t, err, ErrKeyNotExist)

	// Delete a key-value pair that do not exist
	err = db.Del([]byte("key-1"))
	assert.NotNil(t, err)
	assert.Equal(t, err, ErrKeyNotExist)

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.options.Path)
	assert.Nil(t, err)
}

func TestDB_Replace_Active_File(t *testing.T) {
	db, err := OpenDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 100000; i++ {
		err := db.Put([]byte(fmt.Sprintf("%09d", i)), util.RandomBytes(1024))
		assert.Nil(t, err)
	}

	assert.NotEqual(t, len(db.archivedFiles), 0)

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.options.Path)
	assert.Nil(t, err)
}

func TestDB_Concurrency(t *testing.T) {
	db, err := OpenDB(DefaultOptions())
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

	err = os.RemoveAll(db.options.Path)
	assert.Nil(t, err)
}

func TestDB_Compaction(t *testing.T) {
	db, err := OpenDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Compaction()
	assert.Nil(t, err)

	// put a large amount of data
	for i := 0; i < 1000000; i++ {
		err := db.Put([]byte(fmt.Sprintf("%09d", i)), []byte(fmt.Sprintf("%09d", i)))
		assert.Nil(t, err)
	}

	rand.Seed(time.Now().UnixNano())

	// Randomly delete about half of the data
	for i := 0; i < 500000; i++ {
		randNum := rand.Intn(500000)
		err = db.Del([]byte(fmt.Sprintf("%09d", randNum)))
		if err != nil && err != ErrKeyNotExist {
			assert.Nil(t, err)
		}
	}

	// Execute Compact and Get in parallel, but not Put and Delete
	wg := new(sync.WaitGroup)

	wg.Add(1)
	go func() {
		err = db.Compaction()
		assert.Nil(t, err)
		wg.Done()
	}()

	for i := 0; i < 1000000; i++ {
		wg.Add(1)
		go func(i int) {
			et, err := db.Get([]byte(fmt.Sprintf("%09d", i)))
			if err != nil {
				assert.Equal(t, ErrKeyNotExist, err)
			} else {
				assert.NotNil(t, et)
				assert.Equal(t, et.Value, []byte(fmt.Sprintf("%09d", i)))
			}
			wg.Done()
		}(i)
	}

	wg.Wait()

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.options.Path)
	assert.Nil(t, err)
}

func TestDB_TTL(t *testing.T) {
	db, err := OpenDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.PutWithExpiration([]byte(fmt.Sprintf("%09d", 0)), []byte(fmt.Sprintf("%09d", 0)), 3*time.Second)
	assert.Nil(t, err)

	err = db.PutWithExpiration([]byte(fmt.Sprintf("%09d", 1)), []byte(fmt.Sprintf("%09d", 1)), 1*time.Second)
	assert.Nil(t, err)

	time.Sleep(1050 * time.Millisecond)

	et0, err := db.Get([]byte(fmt.Sprintf("%09d", 0)))
	assert.Equal(t, []byte(fmt.Sprintf("%09d", 0)), et0.Value)
	assert.Nil(t, err)

	et1, err := db.Get([]byte(fmt.Sprintf("%09d", 1)))
	assert.Nil(t, et1)
	assert.NotNil(t, err)

	time.Sleep(2 * time.Second)

	et0, err = db.Get([]byte(fmt.Sprintf("%09d", 0)))
	assert.Nil(t, et0)
	assert.NotNil(t, err)

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.options.Path)
	assert.Nil(t, err)
}

func TestDB_TTL_Restart(t *testing.T) {
	db, err := OpenDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.PutWithExpiration([]byte(fmt.Sprintf("%09d", 0)), []byte(fmt.Sprintf("%09d", 0)), 2*time.Second)
	assert.Nil(t, err)

	et0, err := db.Get([]byte(fmt.Sprintf("%09d", 0)))
	assert.Equal(t, []byte(fmt.Sprintf("%09d", 0)), et0.Value)
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

	db, err = OpenDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	time.Sleep(2 * time.Second)

	et0, err = db.Get([]byte(fmt.Sprintf("%09d", 0)))
	assert.Nil(t, et0)
	assert.NotNil(t, err)

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.options.Path)
	assert.Nil(t, err)
}

func TestDB_Watch(t *testing.T) {
	db, err := OpenDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	defer func() {
		err = db.Close()
		assert.Nil(t, err)

		err = os.RemoveAll(db.options.Path)
		assert.Nil(t, err)
	}()

	key := "key"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	watchCh := db.wm.Watch(ctx, key)

	go func() {
		_ = db.Put([]byte(key), []byte("value1"))
		_ = db.Put([]byte(key), []byte("value2"))
		_ = db.Del([]byte(key))
	}()

	expectedEvents := []*watch.WatchEvent{
		{Key: key, Value: []byte("value1"), EventType: watch.Put},
		{Key: key, Value: []byte("value2"), EventType: watch.Put},
		{Key: key, Value: nil, EventType: watch.Del},
	}

	for _, expectedEvent := range expectedEvents {
		select {
		case <-ctx.Done():
			assert.Fail(t, "Context canceled before receiving all events")
			return
		case event, ok := <-watchCh:
			assert.True(t, ok)
			assert.Equal(t, expectedEvent, event)
		}
	}

	ctx, cancel = context.WithCancel(context.Background())
	watchCh = db.wm.Watch(ctx, key)

	go func() {
		cancel()
	}()

	select {
	case <-ctx.Done():
		break
	case _, _ = <-watchCh:
		assert.Fail(t, "Context should be canceled before receiving all events")
	}
}
