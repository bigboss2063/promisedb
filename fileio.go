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

import "os"

type FileIO struct {
	fd *os.File
}

func (f *FileIO) ReadAt(data []byte, off int64) (n int, err error) {
	return f.fd.ReadAt(data, off)
}

func (f *FileIO) WriteAt(data []byte, off int64) (n int, err error) {
	return f.fd.WriteAt(data, off)
}

func (f *FileIO) Sync() (err error) {
	return f.fd.Sync()
}

func (f *FileIO) Close() (err error) {
	return f.fd.Close()
}
