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
	"fmt"
	"github.com/bigboss2063/ApexDB/pkg/binaryx"
	"io"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	ErrKeyIsEmpty       = errors.New("the key is empty")
	ErrKeyNotExist      = errors.New("the key dose not exist")
	ErrDataFileNotExist = errors.New("the data file dose not exist")
	ErrCompacting       = errors.New("compacting in progress")
)

type DB struct {
	lock            *sync.RWMutex
	option          *Option
	activeFile      *DataFile
	archivedFiles   map[uint32]*DataFile
	archivedFileIds []uint32
	keyDir          Index
	nextFileId      uint32
	compacting      bool
}

func OpenDB(option *Option) (*DB, error) {
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

	if db.activeFile == nil {
		dataFile, err := CreateDataFile(db.option.Path, db.nextFileId)
		if err != nil {
			return nil, err
		}
		db.nextFileId += 1
		db.activeFile = dataFile
	}

	err = db.loadIndexFromFiles()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) loadDataFiles() error {
	entries, err := os.ReadDir(db.option.Path)

	if err != nil {
		return err
	}

	fid := make([]uint32, 0)

	for _, et := range entries {
		if strings.HasSuffix(et.Name(), DataFileSuffix) {
			n, err := strconv.ParseUint(strings.Split(et.Name(), ".")[0], 10, 32)
			if err != nil {
				return err
			}

			dataFileId := uint32(n)
			fid = append(fid, dataFileId)
		}
	}

	if len(fid) == 0 {
		return nil
	}

	sort.Slice(fid, func(i, j int) bool {
		return fid[i] < fid[j]
	})

	for _, id := range fid {
		dataFile, err := OpenDataFile(db.option.Path, id)
		if err != nil {
			return err
		}
		if int(id) == len(fid)-1 {
			db.activeFile = dataFile
		} else {
			db.archivedFiles[id] = dataFile
		}
	}

	db.archivedFileIds = fid[:len(fid)-1]
	db.nextFileId = db.activeFile.fileId + 1

	return nil
}

func (db *DB) loadIndexFromFiles() error {
	for _, id := range db.archivedFileIds {
		df := db.archivedFiles[id]
		if err := db.loadIndexFromFile(df); err != nil && err != io.EOF {
			return err
		}
	}

	if err := db.loadIndexFromFile(db.activeFile); err != nil && err != io.EOF {
		return err
	}
	return nil
}

func (db *DB) loadIndexFromFile(df *DataFile) error {
	offset := 4
	for {
		et, err := df.ReadAt(int64(offset))
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if et.MetaData.EntryType == Tombstone {
			db.keyDir.Del(string(et.Key))
		}
		dataPos := &DataPos{
			FileId: df.fileId,
			Vsz:    uint32(len(et.Value)),
			Vpos:   uint32(offset),
			Tstamp: et.MetaData.Tstamp,
		}
		db.keyDir.Put(string(et.Key), dataPos)
		offset += et.Size()
	}
	return nil
}

func (db *DB) Get(key []byte) (*Entry, error) {

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	dataPos := db.keyDir.Get(string(key))

	if dataPos == nil {
		return nil, ErrKeyNotExist
	}

	var datafile *DataFile

	if dataPos.FileId == db.activeFile.fileId {
		db.lock.RLock()
		defer db.lock.RUnlock()
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

	et := NewEntry(key, value, NormalEntry)

	dataPos := db.keyDir.Get(string(key))

	db.lock.Lock()

	if dataPos != nil && dataPos.FileId != db.activeFile.fileId {
		df := db.archivedFiles[dataPos.FileId]
		deletionSize := EntryMetaSize + len(key) + len(value)
		err := df.WriteDeletionSize(uint32(deletionSize))
		if err != nil {
			return err
		}
	}

	vpos := db.activeFile.offset
	err := db.appendLogEntry(et)

	db.lock.Unlock()

	if err != nil {
		return err
	}

	db.keyDir.Put(string(key), &DataPos{
		FileId: db.activeFile.fileId,
		Vsz:    uint32(len(value)),
		Vpos:   vpos,
		Tstamp: et.MetaData.Tstamp,
	})

	return nil
}

func (db *DB) appendLogEntry(et *Entry) error {
	data := et.EncodeLogEntry()

	_, err := db.activeFile.WriteAt(data, int64(db.activeFile.offset))
	if err != nil {
		return err
	}

	if db.activeFile.size >= db.option.MaxDataFileSize {
		if err := db.activeFile.Sync(); err != nil {
			return err
		}

		db.archivedFileIds = append(db.archivedFileIds, db.activeFile.fileId)
		db.archivedFiles[db.activeFile.fileId] = db.activeFile

		newDataFile, err := CreateDataFile(db.option.Path, db.nextFileId)
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

	dataPos := db.keyDir.Get(string(key))
	if dataPos == nil {
		return ErrKeyNotExist
	}

	et := NewEntry(key, nil, Tombstone)

	db.lock.Lock()

	var df *DataFile
	if dataPos.FileId == db.activeFile.fileId {
		df = db.activeFile
	} else {
		df = db.archivedFiles[dataPos.FileId]
	}

	if df == nil {
		return ErrDataFileNotExist
	}

	deletionSize := EntryMetaSize + len(key) + int(dataPos.Vsz)
	err := df.WriteDeletionSize(uint32(deletionSize))
	if err != nil {
		return err
	}

	err = db.appendLogEntry(et)
	db.lock.Unlock()

	if err != nil {
		return err
	}

	db.keyDir.Del(string(key))
	return nil
}

func (db *DB) Compaction() error {
	db.lock.Lock()

	if db.compacting {
		return ErrCompacting
	}

	db.compacting = true
	defer func() {
		db.compacting = false
	}()

	buf := make([]byte, 4)
	err := db.activeFile.ReadDeletionSize(buf)
	if err != nil {
		return err
	}

	deletionSize := float64(binaryx.Uint32(buf))
	deletionRate := deletionSize / float64(db.activeFile.size)
	if deletionRate >= db.option.DeletionRate {
		if err := db.activeFile.Sync(); err != nil {
			return err
		}
		db.archivedFiles[db.activeFile.fileId] = db.activeFile
		db.archivedFileIds = append(db.archivedFileIds, db.activeFile.fileId)

		newDataFile, err := CreateDataFile(db.option.Path, db.nextFileId)
		if err != nil {
			return err
		}

		db.nextFileId += 1
		db.activeFile = newDataFile
	}

	waitingCompactFiles := make([]*DataFile, 0)

	for _, fileId := range db.archivedFileIds {
		df := db.archivedFiles[fileId]
		if err := df.ReadDeletionSize(buf); err != nil {
			return err
		}
		deletionSize = float64(binaryx.Uint32(buf))
		deletionRate = deletionSize / float64(df.size)
		if deletionRate >= db.option.DeletionRate {
			waitingCompactFiles = append(waitingCompactFiles, df)
		}
	}

	db.lock.Unlock()

	if len(waitingCompactFiles) == 0 {
		return nil
	}

	option := MergeOption()
	mergeDB, err := OpenDB(option)
	if err != nil {
		return err
	}
	defer mergeDB.Close()

	for _, file := range waitingCompactFiles {
		var offset int64 = 4
		for {
			et, err := file.ReadAt(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			dataPos := db.keyDir.Get(string(et.Key))
			if dataPos != nil && dataPos.FileId == file.fileId && dataPos.Vpos == uint32(offset) {
				if err := mergeDB.appendLogEntry(et); err != nil {
					return err
				}
			}

			offset += int64(et.Size())
		}
	}

	if err := mergeDB.activeFile.Sync(); err != nil {
		return err
	}

	compactEndFlag, err := CreateCompactEndFlag(mergeDB.option.Path)
	if err != nil {
		return err
	}

	compactedFileIds := make([]uint32, len(waitingCompactFiles))

	for _, file := range waitingCompactFiles {
		compactedFileIds = append(compactedFileIds, file.fileId)
	}

	et := NewEntry([]byte("CompactedFiles"), []byte(strings.Join(strings.Fields(fmt.Sprint(compactedFileIds)), ",")), NormalEntry)
	_, err = compactEndFlag.WriteAt(et.EncodeLogEntry(), 0)
	if err != nil {
		return err
	}

	err = compactEndFlag.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) compactor() {

	if db.option.CompactionInternal == 0 {
		return
	}

	ticker := time.NewTicker(db.option.CompactionInternal)
	defer ticker.Stop()

	stop := make(chan os.Signal)
	signal.Notify(stop, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for {
		select {
		case <-ticker.C:
			err := db.Compaction()
			if err != nil {

			}
		case <-stop:
			return
		}
	}
}

func (db *DB) Close() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	err := db.activeFile.Sync()
	if err != nil {
		return err
	}
	err = db.activeFile.Close()
	if err != nil {
		return err
	}

	for _, fd := range db.archivedFiles {
		err = fd.Sync()
		if err != nil {
			return err
		}
		err = fd.Close()
		if err != nil {
			return err
		}
	}

	return err
}
