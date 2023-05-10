// *************************************************************************
//
// REFRACTION POINT CONFIDENTIAL
// __________________
//
//  Copyright 2018 Refraction Point Inc.
//  All Rights Reserved.
//
// NOTICE:  All information contained herein is, and remains
// the property of Refraction Point Inc. and its suppliers,
// if any.  The intellectual and technical concepts contained
// herein are proprietary to Refraction Point Inc
// and its suppliers and may be covered by U.S. and Foreign Patents,
// patents in process, and are protected by trade secret or copyright law.
// Dissemination of this information or reproduction of this material
// is strictly forbidden unless prior written permission is obtained
// from Refraction Point Inc.
//

package utils

import (
	"context"
	"errors"
	"sync"

	"golang.org/x/sync/semaphore"
)

type Element = interface{}

type pipelinerRecord struct {
	e   Element
	i   int
	err error
}

type PipelinerGenerator = func() (Element, error)
type PipelinerClose = func()
type PipelinerMapper = func(Element) Element

// The Pipeliner will consume elements from `it()` and process them
// with `proc()` in parallel (`nParallel` at a time). The returned
// generator will receive the processed elements _in the order they
// were generated_. The closer terminates all the pipeline (blocking).
// If the `it()` returns a `nil` element, it signifies the termination
// of the pipeline and the generator will produce a `nil` element.
func Pipeliner(it PipelinerGenerator, nParallel int, proc PipelinerMapper) (PipelinerGenerator, PipelinerClose, error) {
	if nParallel <= 0 {
		return nil, nil, errors.New("negative parallel size invalid")
	}

	wg := &sync.WaitGroup{}
	evDone := NewEvent()
	chGen := make(chan pipelinerRecord, nParallel)
	smGen := semaphore.NewWeighted(int64(nParallel))
	chReady := make(chan pipelinerRecord, nParallel)
	arr := make([]pipelinerRecord, nParallel)
	mArr := &sync.Mutex{}
	evReady := NewEvent()

	// Mark the positions in the array
	// to prime the ordering.
	for i := range arr {
		arr[i].i = i + 1
	}

	// This will consume the elements from
	// the iterator provided.
	wg.Add(1)
	go func() {
		defer wg.Done()
		consume(it, chGen, evDone, smGen)
	}()

	// This will process elements in N
	// parallel goroutines.
	wgProc := &sync.WaitGroup{}
	for i := 0; i < nParallel; i++ {
		wg.Add(1)
		wgProc.Add(1)
		go func() {
			defer wg.Done()
			defer wgProc.Done()
			process(chGen, chReady, proc)
		}()
	}
	go func() {
		// Only close the chReady channel when
		// all the processors are done.
		wgProc.Wait()
		close(chReady)
	}()

	// This will order elements that have been
	// processed so we can consume them in-order.
	// We use a static sized array to be the
	// pipeline of ready-to-consume elements.
	// The user consumes the next ready element
	// always from index 0.
	wg.Add(1)
	go func() {
		defer wg.Done()
		placer(chReady, arr, mArr, evReady)
	}()

	// Make sure that when all goroutines are
	// done executing, the events are all set
	// as needed to avoid deadlocks.
	go func() {
		wg.Wait()
		// We use the mArr lock to ensure a
		// consumer does not have a race when
		// checking if the entire pipeline is
		// done vs just waiting for new item.
		mArr.Lock()
		evReady.Set()
		evDone.Set()
		mArr.Unlock()
	}()

	// Return an in-order consumer from the
	// array that the `placer` goroutine populates.
	return func() (Element, error) {
			arrLen := len(arr)
			evReady.Wait()

			mArr.Lock()
			defer mArr.Unlock()
			smGen.Release(1)

			// Consume index 0, the next ordered element.
			r := arr[0]

			// Shift the contents of the array left
			// (towards index 0) to move on to the
			// next element expected (based on order).
			if arrLen > 1 {
				for i := 1; i < arrLen; i++ {
					arr[i-1] = arr[i]
				}
				arr[arrLen-1].e = nil
				arr[arrLen-1].err = nil
				arr[arrLen-1].i = arr[arrLen-2].i + 1
			} else {
				arr[0].e = nil
				arr[0].err = nil
				arr[0].i = 0
			}

			// If element 0 is nil, it means it's
			// not ready to be consumed so reset
			// the event.
			if !evDone.IsSet() && arr[0].e == nil && arr[0].err == nil {
				evReady.Clear()
			}

			return r.e, r.err
		}, func() {
			evDone.Set()
			go func() {
				// Try to release in case a `consume` is
				// currently blocked on the semaphore.
				defer func() {
					recover()
				}()
				smGen.Release(1)
			}()
			wg.Wait()
		}, nil
}

// Consumes elements from the iterator provided.
func consume(it PipelinerGenerator, chGen chan pipelinerRecord, evDone *Event, smGen *semaphore.Weighted) {
	defer close(chGen)

	ctx := context.Background()
	index := 0
	for !evDone.IsSet() {
		smGen.Acquire(ctx, 1)
		if evDone.IsSet() {
			break
		}
		e, err := it()
		index++
		if err != nil {
			chGen <- pipelinerRecord{
				err: err,
				i:   index,
			}
			evDone.Set()
			break
		}
		chGen <- pipelinerRecord{
			e:   e,
			i:   index,
			err: err,
		}
		if e == nil {
			break
		}
	}
}

// Processes elements generated in parallel.
func process(chGen chan pipelinerRecord, chReady chan pipelinerRecord, proc PipelinerMapper) {
	// chReady is closed by runtime wrapper.
	for rec := range chGen {
		if rec.e != nil {
			rec.e = proc(rec.e)
		}
		chReady <- rec
	}
}

// Places the elements processed in order so they can be given to the user in order.
func placer(chReady chan pipelinerRecord, arr []pipelinerRecord, mArr *sync.Mutex, evReady *Event) {
	arrLen := len(arr)
	for rec := range chReady {
		mArr.Lock()
		// The queuing is only valid if the queue size if greater than 1.
		if arrLen > 1 {
			// Look for the right spot in the array for this element
			// based on the index of the next element in the array.
			isPlaced := false
			for i := 0; i < arrLen-1; i++ {
				if arr[i+1].i != rec.i+1 {
					continue
				}
				isPlaced = true
				if arr[i].e != nil {
					panic("our position in pipeliner is taken")
				}
				arr[i] = rec
				if i == 0 {
					evReady.Set()
				}
				break
			}
			if isPlaced {
				mArr.Unlock()
				continue
			}
			// Not placed, so it's the last element, keep
			// going to the single-element case.
		} else {
			evReady.Set()
		}

		// Then last item must be our spot.
		if arr[arrLen-1].e != nil {
			panic("our last spot in pipeliner is taken")
		}
		arr[arrLen-1] = rec
		mArr.Unlock()
	}
}
