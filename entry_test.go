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
	"github.com/bigboss2063/ApexDB/pkg/binaryx"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDataFile_Entry_Codec(t *testing.T) {
	entry := &Entry{
		Key:   []byte("key-1"),
		Value: []byte("value-1"),
		MetaData: &MetaData{
			EntryType: NormalEntry,
			Tstamp:    uint64(time.Now().Unix()),
		},
	}
	entry.MetaData.Ksz = uint32(len(entry.Key))
	entry.MetaData.Vsz = uint32(len(entry.Value))

	data := entry.EncodeLogEntry()
	entry.MetaData.Crc = binaryx.Uint32(data[:4])

	et, err := DecodeLogEntry(data)

	assert.Nil(t, err)
	assert.Equal(t, entry.Key, et.Key)
	assert.Equal(t, entry.Value, et.Value)
	assert.Equal(t, *entry.MetaData, *et.MetaData)
}
