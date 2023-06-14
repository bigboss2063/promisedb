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
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_Open_Close(t *testing.T) {
	db, err := Open(DefaultOption())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.option.Path)
	assert.Nil(t, err)
}

func TestDB_Put(t *testing.T) {
	db, err := Open(DefaultOption())
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("key-1"), []byte("value-1"))
	assert.Nil(t, err)

	err = db.Put(nil, []byte("value-1"))
	assert.NotNil(t, err)

	err = db.Put([]byte("key-2"), nil)
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.option.Path)
	assert.Nil(t, err)
}

func TestDB_Get(t *testing.T) {
	db, err := Open(DefaultOption())
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

	err = db.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(db.option.Path)
	assert.Nil(t, err)
}

func TestDB_Del(t *testing.T) {
	db, err := Open(DefaultOption())
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
}
