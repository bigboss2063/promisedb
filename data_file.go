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
	"fmt"
	"github.com/bigboss2063/promisedb/pkg/binaryx"
	"github.com/bigboss2063/promisedb/pkg/common"
	"io"
	"os"
)

const (
	DataFileSuffix = ".apex"
)

type DataFile struct {
	path      string
	fileId    uint32
	offset    uint32
	size      uint32
	rwManager RWManager
}

func newDataFilePath(path string, fileId uint32) string {
	return path + "/" + fmt.Sprintf("%09d", fileId) + DataFileSuffix
}

func CreateDataFile(path string, fileId uint32) (*DataFile, error) {
	filePath := newDataFilePath(path, fileId)

	fd, err := NewFd(filePath)
	if err != nil {
		return nil, err
	}

	df := &DataFile{
		path:      filePath,
		fileId:    fileId,
		rwManager: fd,
	}

	_, err = df.WriteAt(binaryx.PutUint32(0), 0)
	if err != nil {
		return nil, err
	}

	return df, nil
}

func OpenDataFile(path string, fileId uint32) (*DataFile, error) {
	dataFilePath := newDataFilePath(path, fileId)

	fd, err := NewFd(dataFilePath)
	if err != nil {
		return nil, err
	}

	df := &DataFile{
		path:      dataFilePath,
		fileId:    fileId,
		rwManager: fd,
	}

	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(df.path)
	if err != nil {
		return nil, err
	}

	df.offset = uint32(stat.Size())
	df.size = uint32(stat.Size())
	return df, nil
}

func (df *DataFile) ReadAt(off int64) (*Entry, error) {
	metaDataBuf := make([]byte, EntryMetaSize)

	_, err := df.rwManager.ReadAt(metaDataBuf, off)
	if err != nil {
		return nil, err
	}

	et := new(Entry)

	et.DecodeLogEntryMeta(metaDataBuf)

	payloadSize := int64(et.MetaData.Ksz) + int64(et.MetaData.Vsz)
	off += EntryMetaSize
	payloadBuf := make([]byte, payloadSize)

	_, err = df.rwManager.ReadAt(payloadBuf, off)
	if err != nil {
		return nil, err
	}

	err = et.DecodeLogEntry(append(metaDataBuf[4:], payloadBuf...))
	if err != nil {
		return nil, err
	}

	return et, nil
}

func (df *DataFile) ReadBatch(off int64) ([]*Entry, []uint32, int64, error) {
	buf := make([]byte, 64*common.KB)

	n, err := df.rwManager.ReadAt(buf, off)
	if err != nil && err != io.EOF {
		return nil, nil, 0, err
	}

	offset := 0
	entries := make([]*Entry, 0)
	vpos := make([]uint32, 0)
	for {
		// if the remaining bytes cannot be parsed out metadata, return
		if n-offset < EntryMetaSize {
			return entries, vpos, off + int64(offset), err
		}

		et := new(Entry)
		metaDataBuf := buf[offset : offset+EntryMetaSize]
		et.DecodeLogEntryMeta(metaDataBuf)

		offset += EntryMetaSize
		if n-offset < int(et.MetaData.Ksz+et.MetaData.Vsz) {
			return entries, vpos, off + int64(offset-EntryMetaSize), err
		}

		payloadBuf := buf[offset : offset+int(et.MetaData.Ksz+et.MetaData.Vsz)]
		err := et.DecodeLogEntry(append(metaDataBuf[4:], payloadBuf...))
		if err == nil {
			entries = append(entries, et)
			vpos = append(vpos, uint32(off+int64(offset-EntryMetaSize)))
		}
		offset += int(et.MetaData.Ksz + et.MetaData.Vsz)
	}
}

func (df *DataFile) ReadEntryAt(off int64, entrySize int) (*Entry, error) {
	buf := make([]byte, entrySize)

	_, err := df.rwManager.ReadAt(buf, off)
	if err != nil {
		return nil, err
	}

	et := new(Entry)

	et.DecodeLogEntryMeta(buf[:EntryMetaSize])
	err = et.DecodeLogEntry(buf[4:])
	if err != nil {
		return nil, err
	}

	return et, nil
}

func (df *DataFile) WriteAt(data []byte, off int64) (int, error) {
	n, err := df.rwManager.WriteAt(data, off)
	if err != nil {
		return 0, err
	}
	df.offset += uint32(len(data))
	df.size += uint32(len(data))
	return n, err
}

func (df *DataFile) ReadGarbageSize(data []byte) error {
	if _, err := df.rwManager.ReadAt(data, 0); err != nil {
		return err
	}
	return nil
}

func (df *DataFile) WriteGarbageSize(length uint32) error {
	buf := make([]byte, 4)
	_, err := df.rwManager.ReadAt(buf, 0)
	if err != nil {
		return err
	}

	_, err = df.rwManager.WriteAt(binaryx.PutUint32(binaryx.Uint32(buf)+length), 0)
	if err != nil {
		return err
	}

	return nil
}

func (df *DataFile) Sync() error {
	return df.rwManager.Sync()
}

func (df *DataFile) Close() error {
	return df.rwManager.Close()
}
