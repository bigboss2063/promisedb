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

type TimeHeap struct {
	h h
}

func NewJobHeap() *TimeHeap {
	return &TimeHeap{
		h: h{
			heap:  make([]*Job, 0),
			index: make(map[string]int),
		},
	}
}

func (jh *TimeHeap) Push(job *Job) {
	if _, ok := jh.h.index[job.Key]; ok {
		jh.h.update(job)
	} else {
		heap.Push(&jh.h, job)
	}
}

func (jh *TimeHeap) Pop() *Job {
	if jh.h.isEmpty() {
		return nil
	}
	return heap.Pop(&jh.h).(*Job)
}

func (jh *TimeHeap) Get(key string) *Job {
	if i, ok := jh.h.index[key]; ok {
		return jh.h.heap[i]
	}
	return nil
}

func (jh *TimeHeap) Remove(key string) {
	if i, ok := jh.h.index[key]; ok {
		delete(jh.h.index, key)
		heap.Remove(&jh.h, i)
	}
}

func (jh *TimeHeap) IsExpired(key string) bool {
	if _, ok := jh.h.index[key]; ok {
		return jh.Get(key).Expiration.Before(time.Now())
	}
	return true
}

func (jh *TimeHeap) Peek() *Job {
	return jh.h.peek().(*Job)
}

func (jh *TimeHeap) IsEmpty() bool {
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
	if len(h.heap) == 0 {
		return nil
	}
	earliestJob := h.heap[0]
	h.heap = h.heap[1:]
	delete(h.index, earliestJob.Key)
	return earliestJob
}

// Peek returns the job with the earliest expiration time without removing it from the heap.
func (h *h) peek() interface{} {
	if len(h.heap) > 0 {
		return h.heap[0]
	}
	return nil
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
