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
	"context"
	"errors"
	"github.com/bigboss2063/promisedb/pkg/ttl"
	"github.com/bigboss2063/promisedb/pkg/watch"
	"github.com/samber/lo"
	"io"
	"log"
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
	closed          bool
	lock            *sync.RWMutex
	options         *Options
	activeFile      *DataFile
	archivedFiles   map[uint32]*DataFile
	archivedFileIds []uint32
	keyDir          Index
	nextFileId      uint32
	compacting      bool
	gm              *GarbageManager
	ttl             *ttl.TTL
	wm              *watch.WatcherManager
}

func OpenDB(option *Options) (*DB, error) {
	if _, err := os.Stat(option.Path); os.IsNotExist(err) {
		if err = os.MkdirAll(option.Path, os.ModePerm); err != nil {
			return nil, err
		}
	}

	gm, err := NewGarbageManager(option.Path, option.GarbageManagerBufSize)
	if err != nil {
		return nil, err
	}

	db := &DB{
		closed:        false,
		lock:          &sync.RWMutex{},
		options:       option,
		archivedFiles: make(map[uint32]*DataFile),
		keyDir:        NewIndex(),
		gm:            gm,
		wm:            watch.NewWatcherManager(),
	}

	db.ttl = ttl.NewTTL(func(key string) error {
		return db.Del([]byte(key))
	})

	err = db.loadDataFiles()
	if err != nil {
		return nil, err
	}

	if db.activeFile == nil {
		dataFile, err := CreateDataFile(db.options.Path, db.nextFileId)
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

	go db.ttl.Start()

	go db.wm.Start()

	return db, nil
}

func (db *DB) loadDataFiles() error {
	entries, err := os.ReadDir(db.options.Path)
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
		dataFile, err := OpenDataFile(db.options.Path, id)
		if err != nil {
			return err
		}
		if id == fid[len(fid)-1] {
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
		if err := db.loadIndexFromFileBatch(df); err != nil && err != io.EOF {
			return err
		}
	}

	if err := db.loadIndexFromFileBatch(db.activeFile); err != nil && err != io.EOF {
		return err
	}
	return nil
}

func (db *DB) loadIndexFromFileBatch(df *DataFile) error {
	var offset int64 = 4
	for {
		entries, vpos, nextOff, err := df.ReadBatch(offset)
		if err != nil {
			if err == io.EOF {
				for i, et := range entries {
					db.setKeyDir(et, df.fileId, vpos[i])
				}
				break
			}
			return err
		}
		for i, et := range entries {
			db.setKeyDir(et, df.fileId, vpos[i])
		}
		offset = nextOff
	}
	return nil
}

func (db *DB) setKeyDir(et *Entry, fileId uint32, offset uint32) {
	if et.MetaData.EntryType == Tombstone {
		db.keyDir.Del(string(et.Key))
		return
	}
	if et.MetaData.Expiration != 0 {
		db.ttl.Add(ttl.NewJob(string(et.Key), time.Unix(0, int64(et.MetaData.Expiration))))
	}
	dataPos := &DataPos{
		FileId: fileId,
		Vsz:    et.MetaData.Vsz,
		Vpos:   offset,
	}
	db.keyDir.Put(string(et.Key), dataPos)
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
		datafile = db.activeFile
	} else if df, ok := db.archivedFiles[dataPos.FileId]; ok {
		datafile = df
	}

	if datafile == nil {
		return nil, ErrDataFileNotExist
	}

	et, err := datafile.ReadEntryAt(int64(dataPos.Vpos), EntryMetaSize+len(key)+int(dataPos.Vsz))
	if err != nil {
		return nil, err
	}

	return et, nil
}

func (db *DB) PutWithExpiration(key []byte, value []byte, duration time.Duration) error {
	return db.put(key, value, duration)
}

func (db *DB) Put(key []byte, value []byte) error {
	return db.put(key, value, 0)
}

func (db *DB) put(key []byte, value []byte, duration time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	db.lock.Lock()

	et := NewEntry(key, value, NormalEntry)
	if duration != 0 {
		expiration := time.Now().Add(duration).UnixNano()
		et.MetaData.Expiration = uint64(expiration)
		db.ttl.Add(ttl.NewJob(string(key), time.Unix(0, expiration)))
	}

	dataPos := db.keyDir.Get(string(key))
	if dataPos != nil {
		garbageSize := EntryMetaSize + len(key) + len(value)
		db.gm.sendUpdateInfo(dataPos.FileId, uint32(garbageSize))
	}

	dataPos, err := db.appendLogEntry(et)
	if err != nil {
		return err
	}

	db.Notify(string(key), value, watch.Put)

	db.lock.Unlock()

	db.keyDir.Put(string(key), dataPos)

	return nil
}

func (db *DB) appendLogEntry(et *Entry) (*DataPos, error) {
	data := et.EncodeLogEntry()

	if db.activeFile.size+uint32(len(data)) >= db.options.MaxDataFileSize {
		err := db.replaceActiveFile()
		if err != nil {
			return nil, err
		}
	}

	_, err := db.activeFile.WriteAt(data, int64(db.activeFile.offset))
	if err != nil {
		return nil, err
	}

	if db.options.Sync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	dataPos := &DataPos{
		FileId: db.activeFile.fileId,
		Vsz:    et.MetaData.Vsz,
		Vpos:   db.activeFile.offset - uint32(len(data)),
	}

	return dataPos, nil
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

	var garbageSize int
	if dataPos.FileId == db.activeFile.fileId {
		garbageSize = EntryMetaSize + len(key) + int(dataPos.Vsz) + et.Size()
		db.gm.sendUpdateInfo(dataPos.FileId, uint32(garbageSize))
	} else {
		garbageSize = EntryMetaSize + len(key) + int(dataPos.Vsz)
		db.gm.sendUpdateInfo(dataPos.FileId, uint32(garbageSize))
		db.gm.sendUpdateInfo(db.activeFile.fileId, uint32(et.Size()))
	}

	_, err := db.appendLogEntry(et)
	if err != nil {
		return err
	}

	db.ttl.Delete(string(key))

	db.Notify(string(key), nil, watch.Del)

	db.lock.Unlock()

	db.keyDir.Del(string(key))

	return nil
}

func (db *DB) Compaction() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.compacting {
		return ErrCompacting
	}

	db.compacting = true
	defer func() {
		db.compacting = false
	}()

	waitingCompactFiles := make([]*DataFile, 0)

	garbageSize := db.gm.getGarbageSize(db.activeFile.fileId)
	garbageRate := float64(garbageSize) / float64(db.activeFile.size)
	if garbageRate >= db.options.GarbageRate {
		err := db.replaceActiveFile()
		if err != nil {
			return err
		}
	}

	for _, fileId := range db.archivedFileIds {
		df := db.archivedFiles[fileId]
		garbageSize = db.gm.getGarbageSize(fileId)
		garbageRate = float64(garbageSize) / float64(df.size)
		if garbageRate >= db.options.GarbageRate {
			waitingCompactFiles = append(waitingCompactFiles, df)
		}
	}

	for _, df := range waitingCompactFiles {
		var offset int64 = 4
		for {
			entries, vpos, nextOff, err := df.ReadBatch(offset)
			if err != nil {
				if err == io.EOF {
					for i, et := range entries {
						err := db.resetKeyDir(et, df.fileId, vpos[i])
						if err != nil {
							return err
						}
					}
					break
				}
				return err
			}
			for i, et := range entries {
				err := db.resetKeyDir(et, df.fileId, vpos[i])
				if err != nil {
					return err
				}
			}
			offset = nextOff
		}
	}

	for _, file := range waitingCompactFiles {
		err := db.deleteDataFile(file.fileId)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) resetKeyDir(et *Entry, fileId uint32, offset uint32) error {
	dataPos := db.keyDir.Get(string(et.Key))
	if dataPos != nil && dataPos.FileId == fileId && dataPos.Vpos == offset {

		dataPos, err := db.appendLogEntry(et)
		if err != nil {
			return err
		}

		db.keyDir.Put(string(et.Key), dataPos)
	}
	return nil
}

func (db *DB) compactor() {

	if db.options.CompactionInternal == 0 {
		return
	}

	ticker := time.NewTicker(db.options.CompactionInternal)
	defer ticker.Stop()

	stop := make(chan os.Signal)
	signal.Notify(stop, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for {
		select {
		case <-ticker.C:
			err := db.Compaction()
			if err != nil {
				log.Println(err.Error())
			}
		case <-stop:
			return
		}
	}
}

func (db *DB) deleteDataFile(fileId uint32) error {
	err := db.gm.free(fileId)
	if err != nil {
		return err
	}

	df := db.archivedFiles[fileId]

	delete(db.archivedFiles, fileId)

	lo.Reject(db.archivedFileIds, func(id uint32, _ int) bool {
		return id == fileId
	})

	err = df.Close()
	if err != nil {
		return err
	}

	return os.Remove(df.path)
}

func (db *DB) replaceActiveFile() error {
	if err := db.activeFile.Sync(); err != nil {
		return err
	}
	db.archivedFiles[db.activeFile.fileId] = db.activeFile
	db.archivedFileIds = append(db.archivedFileIds, db.activeFile.fileId)

	newDataFile, err := CreateDataFile(db.options.Path, db.nextFileId)
	if err != nil {
		return err
	}

	db.nextFileId += 1
	db.activeFile = newDataFile

	return nil
}

func (db *DB) Watch(ctx context.Context, key string) {
	db.wm.Watch(ctx, key)
}

func (db *DB) Notify(key string, value []byte, entryType watch.EventType) {
	if db.wm.Watched(key) {
		db.wm.Notify(&watch.WatchEvent{Key: key, Value: value, EventType: entryType})
	}
}

func (db *DB) Close() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.closed = true

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

	db.gm.close()

	db.ttl.Stop()

	db.wm.Close()

	return err
}
