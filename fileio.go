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

import "os"

type Fd struct {
	f *os.File
}

func NewFd(path string) (*Fd, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	fd := &Fd{
		f: f,
	}
	return fd, nil
}

func (fd *Fd) ReadAt(data []byte, off int64) (n int, err error) {
	return fd.f.ReadAt(data, off)
}

func (fd *Fd) WriteAt(data []byte, off int64) (n int, err error) {
	return fd.f.WriteAt(data, off)
}

func (fd *Fd) Sync() (err error) {
	return fd.f.Sync()
}

func (fd *Fd) Close() (err error) {
	return fd.f.Close()
}
