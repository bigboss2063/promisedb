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
	"io"
	"log"
	"os"
	"testing"
	"time"
)

func TestNewDataFile(t *testing.T) {

	path := os.TempDir() + "/apexdb"

	err := os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)

	dataFile, err := CreateDataFile(path, 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(path)
	assert.Nil(t, err)
}

func TestDataFile_WriteAt_ReadAt(t *testing.T) {
	path := os.TempDir() + "/apexdb"

	err := os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(path)

	dataFile, err := CreateDataFile(path, 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
	defer dataFile.Close()

	key, value := []byte("key-1"), []byte("value-1")

	entry := &Entry{
		Key:   key,
		Value: value,
		MetaData: &MetaData{
			EntryType: NormalEntry,
			Tstamp:    uint64(time.Now().Unix()),
			Ksz:       uint32(len(key)),
			Vsz:       uint32(len(value)),
		},
	}

	data := entry.EncodeLogEntry()

	_, err = dataFile.WriteAt(data, 0)
	assert.Nil(t, err)

	et, err := dataFile.ReadAt(0)
	assert.Nil(t, err)
	assert.Equal(t, et.Key, entry.Key)
	assert.Equal(t, et.Value, entry.Value)
}

func TestDataFile_ReadBatch(t *testing.T) {
	path := DefaultOption().Path

	err := os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)

	dataFile, err := CreateDataFile(path, 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	off := 0
	for i := 0; i < 32; i++ {
		et := NewEntry([]byte(fmt.Sprintf("%09d", i)), util.RandomBytes(1024), NormalEntry)
		data := et.EncodeLogEntry()
		n, err := dataFile.WriteAt(data, int64(off))
		off += len(data)
		assert.Equal(t, n, len(data))
		assert.Nil(t, err)
	}

	var offset int64
	for {
		entries, _, nextOff, err := dataFile.ReadBatch(offset)
		if err != nil {
			if err == io.EOF {
				if len(entries) != 0 {
					for _, et := range entries {
						log.Printf("%v\n", string(et.Key))
					}
				}
				break
			}
			assert.Fail(t, "")
		}
		for _, et := range entries {
			log.Printf("%v\n", string(et.Key))
		}
		offset = nextOff
	}

	os.RemoveAll(path)
}

func TestDataFile_Sync_Close(t *testing.T) {
	path := os.TempDir() + "/apexdb"

	err := os.MkdirAll(path, os.ModePerm)
	assert.Nil(t, err)

	dataFile, err := CreateDataFile(path, 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Sync()
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)

	err = os.RemoveAll(path)
	assert.Nil(t, err)
}
