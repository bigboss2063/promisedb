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
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrKeyIsEmpty       = errors.New("the key is empty")
	ErrKeyNotExist      = errors.New("the key dose not exist")
	ErrDataFileNotExist = errors.New("the data file dose not exist")
)

type DB struct {
	lock            *sync.RWMutex
	option          *Option
	activeFile      *DataFile
	archivedFiles   map[uint32]*DataFile
	archivedFileIds []uint32
	keyDir          Index
	nextFileId      uint32
}

func Open(option *Option) (*DB, error) {
	if _, err := os.Stat(option.Path); os.IsNotExist(err) {
		if err = os.MkdirAll(option.Path, os.ModePerm); err != nil {
			return nil, err
		}
	}

	db := &DB{
		lock:          &sync.RWMutex{},
		option:        option,
		archivedFiles: make(map[uint32]*DataFile),
		keyDir:        NewIndex(),
	}

	err := db.loadDataFiles()
	if err != nil {
		return nil, err
	}

	err = db.loadIndexFromFiles()
	if err != nil {
		return nil, err
	}

	dataFile, err := NewDataFile(db.option.Path, db.nextFileId)
	if err != nil {
		return nil, err
	}

	db.nextFileId += 1
	db.activeFile = dataFile

	return db, nil
}

func (db *DB) loadDataFiles() error {
	entries, err := os.ReadDir(db.option.Path)

	if err != nil {
		return err
	}

	var maxDataFileId uint32 = 0
	fid := make([]uint32, 0)

	for _, et := range entries {
		if strings.HasSuffix(et.Name(), DataFileSuffix) {
			n, err := strconv.ParseUint(strings.Split(et.Name(), ".")[0], 10, 32)
			if err != nil {
				return err
			}

			dataFileId := uint32(n)

			if dataFileId > maxDataFileId {
				maxDataFileId = dataFileId
			}

			dataFile, err := openDataFile(db.option.Path, dataFileId)
			db.archivedFiles[dataFileId] = dataFile
			fid = append(fid, dataFileId)
		}
	}

	sort.Slice(fid, func(i, j int) bool {
		return fid[i] < fid[j]
	})

	db.archivedFileIds = fid
	db.nextFileId = maxDataFileId + 1

	return nil
}

func (db *DB) loadIndexFromFiles() error {
	for _, id := range db.archivedFileIds {
		df := db.archivedFiles[id]
		offset := 0
		for {
			et, err := df.ReadAt(int64(offset))
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			if et.MetaData.EntryType == Tombstone {
				continue
			}
			entrySize := EntryMetaSize + len(et.Key) + len(et.Value)
			dataPos := &DataPos{
				FileId: id,
				Vsz:    uint32(len(et.Value)),
				Vpos:   uint32(offset),
				Tstamp: et.MetaData.Tstamp,
			}
			db.keyDir.Put(string(et.Key), dataPos)
			offset += entrySize
		}
	}
	return nil
}

func (db *DB) Get(key []byte) (*Entry, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	dataPos := db.keyDir.Get(string(key))

	if dataPos == nil {
		return nil, ErrKeyNotExist
	}

	var datafile *DataFile

	if dataPos.FileId == db.activeFile.fileId {
		datafile = db.activeFile
	} else if df, ok := db.archivedFiles[dataPos.FileId]; ok {
		datafile = df
	}

	if datafile == nil {
		return nil, ErrDataFileNotExist
	}

	et, err := datafile.ReadAt(int64(dataPos.Vpos))
	if err != nil {
		return nil, err
	}

	return et, nil
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	db.lock.Lock()

	et := NewEntry(key, value, NormalEntry)
	vpos := db.activeFile.offset

	err := db.put(et)

	if err != nil {
		return err
	}

	db.lock.Unlock()

	db.keyDir.Put(string(key), &DataPos{
		FileId: db.activeFile.fileId,
		Vsz:    uint32(len(value)),
		Vpos:   vpos,
		Tstamp: et.MetaData.Tstamp,
	})

	return nil
}

func (db *DB) put(et *Entry) error {
	data := et.EncodeLogEntry()

	_, err := db.activeFile.WriteAt(data, int64(db.activeFile.offset))
	if err != nil {
		return err
	}

	if db.activeFile.size >= db.option.MaxDataFileSize {
		db.archivedFileIds = append(db.archivedFileIds, db.activeFile.fileId)
		db.archivedFiles[db.activeFile.fileId] = db.activeFile
		newDataFile, err := NewDataFile(db.option.Path, db.nextFileId)
		if err != nil {
			return err
		}
		db.nextFileId += 1
		db.activeFile = newDataFile
	}

	return nil
}

func (db *DB) Del(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if db.keyDir.Get(string(key)) == nil {
		return ErrKeyNotExist
	}

	db.lock.Lock()

	et := NewEntry(key, nil, Tombstone)

	err := db.put(et)
	if err != nil {
		return err
	}

	db.lock.Unlock()

	db.keyDir.Del(string(key))
	return nil
}

func (db *DB) Close() error {
	err := db.activeFile.Close()
	if err != nil {
		return err
	}

	for _, fd := range db.archivedFiles {
		err = fd.Close()
		if err != nil {
			return err
		}
	}

	return err
}
