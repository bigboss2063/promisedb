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
	"errors"
	"github.com/bigboss2063/promisedb/pkg/binaryx"
	"hash/crc32"
)

type EntryType = uint16

const (
	NormalEntry EntryType = iota
	Tombstone
	Begin
	Commit
)

const EntryMetaSize = 2 + 4 + 8 + 8 + 4 + 4

var (
	ErrEntryWrong = errors.New("the data of entry is wrong")
)

type MetaData struct {
	Crc        uint32
	EntryType  EntryType
	TxnId      uint64
	Expiration uint64
	Ksz        uint32
	Vsz        uint32
}

type Entry struct {
	Key      []byte
	Value    []byte
	MetaData *MetaData
}

type DataPos struct {
	FileId uint32
	Vsz    uint32
	Vpos   uint32
}

func NewEntry(key []byte, value []byte, entryType EntryType) *Entry {
	et := &Entry{
		Key:   key,
		Value: value,
		MetaData: &MetaData{
			EntryType: entryType,
			Ksz:       uint32(len(key)),
			Vsz:       uint32(len(value)),
		},
	}
	return et
}

func (et *Entry) DecodeLogEntryMeta(data []byte) {

	metaData := &MetaData{
		Crc:        binaryx.Uint32(data[:4]),
		EntryType:  binaryx.Uint16(data[4:6]),
		TxnId:      binaryx.Uint64(data[6:14]),
		Expiration: binaryx.Uint64(data[14:22]),
		Ksz:        binaryx.Uint32(data[22:26]),
		Vsz:        binaryx.Uint32(data[26:30]),
	}

	et.MetaData = metaData
}

func (et *Entry) DecodeLogEntry(data []byte) error {

	if et.MetaData.Crc != crc32.ChecksumIEEE(data) {
		return ErrEntryWrong
	}

	et.Key = data[EntryMetaSize-4 : EntryMetaSize+et.MetaData.Ksz-4]
	et.Value = data[EntryMetaSize+et.MetaData.Ksz-4:]
	return nil
}

func (et *Entry) EncodeLogEntry() []byte {
	buf := make([]byte, 0, EntryMetaSize+len(et.Key)+len(et.Value))

	buf = append(buf, binaryx.PutUint32(et.MetaData.Crc)...)
	buf = append(buf, binaryx.PutUint16(et.MetaData.EntryType)...)
	buf = append(buf, binaryx.PutUint64(et.MetaData.TxnId)...)
	buf = append(buf, binaryx.PutUint64(et.MetaData.Expiration)...)
	buf = append(buf, binaryx.PutUint32(et.MetaData.Ksz)...)
	buf = append(buf, binaryx.PutUint32(et.MetaData.Vsz)...)
	buf = append(buf, et.Key...)
	buf = append(buf, et.Value...)

	copy(buf[:4], binaryx.PutUint32(crc32.ChecksumIEEE(buf[4:])))

	return buf
}

func (et *Entry) Size() int {
	return EntryMetaSize + len(et.Key) + len(et.Value)
}
