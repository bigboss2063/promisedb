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

package util

import (
	"crypto/rand"
	"fmt"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

func GetTestKey(n int) []byte {
	return []byte(fmt.Sprintf("%09d", n))
}

func RandomBytes(length int) []byte {
	randomBytes := make([]byte, length)

	_, _ = rand.Read(randomBytes)

	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = letters[randomBytes[i]%byte(len(letters))]
	}
	return result
}
