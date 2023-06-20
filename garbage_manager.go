package promisedb

import (
	"errors"
	"github.com/bigboss2063/promisedb/pkg/binaryx"
	"path/filepath"
	"sync"
)

var (
	ErrGarbageMetaDataNotExist = errors.New("the garbage meta data not exist")
	ErrGarbageFreeListEmpty    = errors.New("the garbage free list is empty")
)

const (
	GarbageManagerFreeListSize = 1024
	GarbageManagerFileName     = "gm"
	GarbageManagerRecordSize   = 9
)

type updateInfo struct {
	fileId      uint32
	garbageSize uint32
}

type GarbageMetaData struct {
	offset      uint32
	garbageSize uint32
	next        *GarbageMetaData
}

type GarbageManager struct {
	lock         *sync.Mutex
	rwManager    RWManager
	metas        map[uint32]*GarbageMetaData
	freeList     *GarbageMetaData
	updateInfoCh chan *updateInfo
	closeCh      chan struct{}
}

func NewGarbageManager(path string, bufSize int) (*GarbageManager, error) {
	path = filepath.Join(path, GarbageManagerFileName)
	fd, err := NewFd(path)
	if err != nil {
		return nil, err
	}

	stat, err := fd.f.Stat()
	if err != nil {
		return nil, err
	}

	metas := make(map[uint32]*GarbageMetaData)

	var root *GarbageMetaData
	if stat.Size() == 0 {
		gmRecordBuf := make([]byte, GarbageManagerRecordSize*GarbageManagerFreeListSize)
		_, err = fd.WriteAt(gmRecordBuf, 0)
		if err != nil {
			return nil, err
		}
		for offset := uint32(0); offset < GarbageManagerRecordSize*GarbageManagerFreeListSize; offset += GarbageManagerRecordSize {
			cur := &GarbageMetaData{
				garbageSize: 0,
				offset:      offset,
				next:        root,
			}
			root = cur
		}
	} else {
		gmRecordBuf := make([]byte, GarbageManagerRecordSize)
		for offset := uint32(0); offset < GarbageManagerRecordSize*GarbageManagerFreeListSize; offset += GarbageManagerRecordSize {
			_, err = fd.ReadAt(gmRecordBuf, int64(offset))
			if err != nil {
				return nil, err
			}
			if gmRecordBuf[0] == 0 {
				cur := &GarbageMetaData{
					garbageSize: 0,
					offset:      offset,
					next:        root,
				}
				root = cur
			} else {
				metas[binaryx.Uint32(gmRecordBuf[1:5])] = &GarbageMetaData{
					garbageSize: binaryx.Uint32(gmRecordBuf[5:]),
					offset:      offset,
				}
			}
		}
	}

	gm := &GarbageManager{
		lock:         &sync.Mutex{},
		rwManager:    fd,
		metas:        metas,
		freeList:     root,
		updateInfoCh: make(chan *updateInfo, bufSize),
		closeCh:      make(chan struct{}),
	}

	go gm.garbageRecorder()

	return gm, nil
}

func (gm *GarbageManager) garbageRecorder() {
	for {
		select {
		case ui, ok := <-gm.updateInfoCh:
			if !ok {
				return
			}
			if err := gm.updateGarbageInfo(ui); err != nil {
				if err == ErrGarbageFreeListEmpty {
					// Todo db must be compacted now
				}
			}
		case <-gm.closeCh:
			_ = gm.sync()
			_ = gm.rwManager.Close()
			return
		}
	}
}

func (gm *GarbageManager) getGarbageSize(fileId uint32) uint32 {
	gm.lock.Lock()
	defer gm.lock.Unlock()
	gmd, ok := gm.metas[fileId]
	if !ok {
		return 0
	}
	return gmd.garbageSize
}

func (gm *GarbageManager) sendUpdateInfo(fileId uint32, garbageSize uint32) {
	gm.updateInfoCh <- &updateInfo{
		fileId:      fileId,
		garbageSize: garbageSize,
	}
}

func (gm *GarbageManager) updateGarbageInfo(ui *updateInfo) error {
	gmd, ok := gm.metas[ui.fileId]
	if !ok {
		allocated, err := gm.allocate(ui.fileId)
		if err != nil {
			return err
		}
		gmd = allocated
		gm.metas[ui.fileId] = gmd
	}

	buf := make([]byte, 4)
	if _, err := gm.rwManager.ReadAt(buf, int64(gmd.offset)+5); err != nil {
		return err
	}

	garbageSize := ui.garbageSize + binaryx.Uint32(buf)

	if _, err := gm.rwManager.WriteAt(binaryx.PutUint32(ui.garbageSize+binaryx.Uint32(buf)), int64(gmd.offset)+5); err != nil {
		return err
	}

	gmd.garbageSize = garbageSize

	return nil
}

func (gm *GarbageManager) allocate(fileId uint32) (*GarbageMetaData, error) {
	if gm.freeList == nil {
		return nil, ErrGarbageFreeListEmpty
	}

	gmd := gm.freeList

	buf := make([]byte, GarbageManagerRecordSize)
	buf[0] = 1
	copy(buf[1:5], binaryx.PutUint32(fileId))
	copy(buf[5:], binaryx.PutUint32(0))

	_, err := gm.rwManager.WriteAt(buf, int64(gmd.offset))
	if err != nil {
		return nil, err
	}

	gm.freeList = gm.freeList.next

	return gmd, nil
}

func (gm *GarbageManager) free(fileId uint32) error {
	gm.lock.Lock()
	defer gm.lock.Unlock()

	gmd, ok := gm.metas[fileId]
	if !ok {
		return ErrGarbageMetaDataNotExist
	}

	buf := make([]byte, GarbageManagerRecordSize)
	_, err := gm.rwManager.WriteAt(buf, int64(gmd.offset))
	if err != nil {
		return err
	}

	delete(gm.metas, fileId)

	gmd.next = gm.freeList
	gm.freeList = gmd

	return nil
}

func (gm *GarbageManager) sync() error {
	return gm.rwManager.Sync()
}

func (gm *GarbageManager) close() {
	close(gm.closeCh)
}
