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
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestEntry_Codec(t *testing.T) {
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

	et := new(Entry)

	et.DecodeLogEntryMeta(data)

	err := et.DecodeLogEntry(data[4:])

	assert.Nil(t, err)
	assert.Equal(t, entry.Key, et.Key)
	assert.Equal(t, entry.Value, et.Value)
}
