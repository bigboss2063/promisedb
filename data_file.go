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
	"os"
)

const DataFileSuffix = ".apex"

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

func NewDataFile(path string, fileId uint32) (*DataFile, error) {
	dataFilePath := newDataFilePath(path, fileId)

	fd, err := NewFd(dataFilePath)
	if err != nil {
		return nil, err
	}

	datafile := &DataFile{
		path:      dataFilePath,
		fileId:    fileId,
		offset:    0,
		size:      0,
		rwManager: fd,
	}
	return datafile, nil
}

func openDataFile(path string, fileId uint32) (*DataFile, error) {
	df, err := NewDataFile(path, fileId)
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

	err = et.DecodeLogEntry(payloadBuf)
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

func (df *DataFile) Sync() error {
	return df.rwManager.Sync()
}

func (df *DataFile) Close() error {
	return df.rwManager.Close()
}
