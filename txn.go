// Copyright 2023 The PromiseDB Authors
// Licensed under the Apache License, Version  2.0 (the "License");
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
	"github.com/bigboss2063/promisedb/pkg/ttl"
	"github.com/bigboss2063/promisedb/pkg/watch"
	"github.com/bwmarrin/snowflake"
	"time"
)

var (
	ErrDBClosed     = errors.New("db already closed")
	ErrTxnCommitted = errors.New("txn already committed")
	ErrTxnReadOnly  = errors.New("txn is read only")
)

type Txn struct {
	txnId         int64
	db            *DB
	pendingWrites map[string]*pendingWrite
	readOnly      bool
	committed     bool
}

type pendingWrite struct {
	et       *Entry
	duration time.Duration
}

func (db *DB) Begin(readOnly bool) (*Txn, error) {
	txn := &Txn{
		db:        db,
		readOnly:  readOnly,
		committed: false,
	}

	if !readOnly {
		txn.pendingWrites = make(map[string]*pendingWrite)
	}

	node, err := snowflake.NewNode(1)
	if err != nil {
		return nil, err
	}

	txn.txnId = node.Generate().Int64()

	beginEntry := NewEntry(nil, nil, Begin)
	beginEntry.MetaData.TxnId = uint64(txn.txnId)
	_, err = db.appendLogEntry(beginEntry)
	if err != nil {
		return nil, err
	}

	txn.lock()

	return txn, nil
}

func (txn *Txn) Put(key []byte, value []byte) error {
	return txn.put(key, value, 0)
}

func (txn *Txn) PutWithExpiration(key []byte, value []byte, duration time.Duration) error {
	return txn.put(key, value, duration)
}

func (txn *Txn) put(key []byte, value []byte, duration time.Duration) error {
	if txn.db.closed {
		return ErrDBClosed
	}

	if txn.readOnly {
		return ErrTxnReadOnly
	}

	et := NewEntry(key, value, NormalEntry)
	txn.pendingWrites[string(key)] = &pendingWrite{
		et:       et,
		duration: duration,
	}

	return nil
}

func (txn *Txn) Del(key []byte) error {
	if txn.db.closed {
		return ErrDBClosed
	}

	if txn.readOnly {
		return ErrTxnReadOnly
	}

	et := NewEntry(key, nil, Tombstone)
	txn.pendingWrites[string(key)] = &pendingWrite{
		et: et,
	}

	return nil
}

func (txn *Txn) Get(key []byte) (*Entry, error) {
	return txn.db.Get(key)
}

func (txn *Txn) Commit() error {
	defer txn.unlock()

	if txn.db.closed {
		return ErrDBClosed
	}

	if txn.readOnly || len(txn.pendingWrites) == 0 {
		return nil
	}

	if txn.committed {
		return ErrTxnCommitted
	}

	dataPoses := make(map[string]*DataPos)

	for _, pw := range txn.pendingWrites {
		pw.et.MetaData.TxnId = uint64(txn.txnId)

		if pw.duration != 0 {
			txn.updateExpiration(pw)
		}

		txn.updateGarbageInfo(pw)

		dataPos, err := txn.db.appendLogEntry(pw.et)
		if err != nil {
			return err
		}

		dataPoses[string(pw.et.Key)] = dataPos
	}

	commitEntry := NewEntry(nil, nil, Commit)
	commitEntry.MetaData.TxnId = uint64(txn.txnId)
	_, err := txn.db.appendLogEntry(commitEntry)
	if err != nil {
		return nil
	}

	for _, pw := range txn.pendingWrites {

		if pw.et.MetaData.EntryType == NormalEntry {
			txn.db.Notify(string(pw.et.Key), pw.et.Value, watch.Put)
			txn.db.keyDir.Put(string(pw.et.Key), dataPoses[string(pw.et.Key)])
		} else {
			txn.db.Notify(string(pw.et.Key), pw.et.Value, watch.Del)
			txn.db.keyDir.Del(string(pw.et.Key))
		}
	}

	txn.committed = true
	return nil
}

func (txn *Txn) updateExpiration(pw *pendingWrite) {
	expiration := time.Now().Add(pw.duration).UnixNano()
	pw.et.MetaData.Expiration = uint64(expiration)
	txn.db.ttl.Add(ttl.NewJob(string(pw.et.Key), time.Unix(0, expiration)))
}

func (txn *Txn) updateGarbageInfo(pw *pendingWrite) {
	dataPos := txn.db.keyDir.Get(string(pw.et.Key))

	if pw.et.MetaData.EntryType == NormalEntry {
		garbageSize := EntryMetaSize + len(pw.et.Key) + len(pw.et.Value)
		txn.db.gm.sendUpdateInfo(dataPos.FileId, uint32(garbageSize))
	} else {
		var garbageSize int
		if dataPos.FileId == txn.db.activeFile.fileId {
			garbageSize = EntryMetaSize + len(pw.et.Key) + int(dataPos.Vsz) + pw.et.Size()
			txn.db.gm.sendUpdateInfo(dataPos.FileId, uint32(garbageSize))
		} else {
			garbageSize = EntryMetaSize + len(pw.et.Key) + int(dataPos.Vsz)
			txn.db.gm.sendUpdateInfo(dataPos.FileId, uint32(garbageSize))
			txn.db.gm.sendUpdateInfo(txn.db.activeFile.fileId, uint32(pw.et.Size()))
		}
	}
}

func (txn *Txn) lock() {
	if txn.readOnly {
		txn.db.lock.RLock()
	} else {
		txn.db.lock.Lock()
	}
}

func (txn *Txn) unlock() {
	if txn.readOnly {
		txn.db.lock.RUnlock()
	} else {
		txn.db.lock.Unlock()
	}
}
