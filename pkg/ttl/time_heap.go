// Copyright 2023 The PromiseDB Authors
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

package ttl

import (
	"container/heap"
	"time"
)

type timeHeap struct {
	h h
}

func (jh *timeHeap) push(job *Job) {
	if _, ok := jh.h.index[job.Key]; ok {
		jh.h.update(job)
	} else {
		heap.Push(&jh.h, job)
	}
}

func (jh *timeHeap) pop() *Job {
	if jh.h.isEmpty() {
		return nil
	}
	return heap.Pop(&jh.h).(*Job)
}

func (jh *timeHeap) get(key string) *Job {
	if i, ok := jh.h.index[key]; ok {
		return jh.h.heap[i]
	}
	return nil
}

func (jh *timeHeap) remove(key string) {
	if i, ok := jh.h.index[key]; ok {
		delete(jh.h.index, key)
		heap.Remove(&jh.h, i)
	}
}

func (jh *timeHeap) isExpired(key string) bool {
	if _, ok := jh.h.index[key]; ok {
		return jh.get(key).Expiration.Before(time.Now())
	}
	return true
}

func (jh *timeHeap) peek() *Job {
	if jh.isEmpty() {
		return nil
	}
	return jh.h.peek().(*Job)
}

func (jh *timeHeap) isEmpty() bool {
	return jh.h.isEmpty()
}

type Job struct {
	Key        string
	Expiration time.Time
}

func NewJob(key string, expiration time.Time) *Job {
	return &Job{
		Key:        key,
		Expiration: expiration,
	}
}

type h struct {
	heap  []*Job
	index map[string]int
}

// Push adds a job to the h.
func (h *h) Push(j interface{}) {
	job := j.(*Job)
	h.heap = append(h.heap, job)
	h.index[job.Key] = len(h.heap) - 1
}

// Pop removes and returns the job with the earliest expiration time from the h.
func (h *h) Pop() interface{} {
	if h.isEmpty() {
		return nil
	}
	old := h.heap
	n := len(old)
	x := old[n-1]
	h.heap = old[0 : n-1]
	delete(h.index, x.Key)
	return x
}

// peek returns the job with the earliest expiration time without removing it from the heap.
func (h *h) peek() interface{} {
	if h.isEmpty() {
		return nil
	}
	return h.heap[0]
}

// Len returns the number of jobs in the h.
func (h *h) Len() int {
	return len(h.heap)
}

// Less reports whether the job with index i should sort before the job with index j.
func (h *h) Less(i, j int) bool {
	return h.heap[i].Expiration.Before(h.heap[j].Expiration)
}

// Swap swaps the jobs with indexes i and j.
func (h *h) Swap(i, j int) {
	h.heap[i], h.heap[j] = h.heap[j], h.heap[i]
	h.index[h.heap[i].Key], h.index[h.heap[j].Key] = i, j
}

// isEmpty checks if the heap is empty.
func (h *h) isEmpty() bool {
	return len(h.heap) == 0
}

// update modifies the expiration time of a job in the heap.
func (h *h) update(job *Job) {
	if index, ok := h.index[job.Key]; ok {
		j := h.heap[index]
		j.Expiration = job.Expiration
		heap.Fix(h, index)
	}
}
