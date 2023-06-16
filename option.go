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
	"os"
	"time"
)

const (
	DBDirectory        = "/apexdb"
	CompactDirectory   = "/apexdb/merge"
	MaxDataFileSize    = 64 * 1024 * 1024
	CompactionInternal = 8 * time.Hour
	DeletionRate       = 0.3
)

type Option struct {
	Path               string
	MaxDataFileSize    uint32
	CompactionInternal time.Duration
	DeletionRate       float64
}

func DefaultOption() *Option {
	return &Option{
		Path:               os.TempDir() + DBDirectory,
		MaxDataFileSize:    MaxDataFileSize,
		CompactionInternal: CompactionInternal,
		DeletionRate:       DeletionRate,
	}
}
