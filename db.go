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
	"os"
	"sync"
)

var (
	ErrKeyIsEmpty       = errors.New("the key is empty")
	ErrKeyNotExist      = errors.New("the key dose not exist")
	ErrDataFileNotExist = errors.New("the data file dose not exist")
)

type DB struct {
	lock          *sync.RWMutex
	option        *Option
	activeFile    *DataFile
	archivedFiles map[uint32]*DataFile
	keyDir        Index
	nextFileId    uint32
}

func NewApexDB(option *Option) (*DB, error) {
	if _, err := os.Stat(option.Path); os.IsNotExist(err) {
		if err = os.MkdirAll(option.Path, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// TODO read a file already stored on disk
	dataFile, err := NewDataFile(option.Path, 0)
	if err != nil {
		return nil, err
	}
	db := &DB{
		lock:          &sync.RWMutex{},
		option:        option,
		activeFile:    dataFile,
		archivedFiles: make(map[uint32]*DataFile),
		keyDir:        NewIndex(),
		nextFileId:    1,
	}
	return db, nil
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
