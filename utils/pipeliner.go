package utils

import (
	"errors"
	"sync"
)

type Element = interface{}

type pipelinerRecord struct {
	e Element
	i int
}

type PipelinerGenerator = func() (Element, error)
type PipelinerClose = func()

// The Pipeliner will consume elements from `it()` and process them
// with `proc()` in parallel (`nParallel` at a time). The returned
// generator will receive the processed elements. The closer terminates
// all the pipeline (blocking).
// If the `it()` returns a `nil` element, it signifies the termination
// of the pipeline and the generator will produce a `nil` element.
func Pipeliner(it func() (Element, error), nParallel int, proc func(e Element) Element) (PipelinerGenerator, PipelinerClose, error) {
	if nParallel <= 0 {
		return nil, nil, errors.New("negative parallel size invalid")
	}
	q := make([]pipelinerRecord, nParallel)
	evNextIsReady := NewEvent()
	m := sync.Mutex{}
	var fetchErr error
	lastI := 0
	isEOF := false
	nRunning := 0

	internalWg := sync.WaitGroup{}
	internalStop := NewEvent()

	closer := func() {
		internalStop.Set()
		internalWg.Wait()
		for i, r := range q {
			if r.e == nil {
				continue
			}
			q[i].e = nil
		}
	}

	f := func() {
		defer internalWg.Done()
		if internalStop.IsSet() {
			return
		}

		// Lock in our index.
		m.Lock()
		myI := lastI + 1
		lastI++
		e, err := it()
		m.Unlock()

		defer func() {
			m.Lock()
			nRunning--
			if isEOF && nRunning == 0 {
				// We got all elements and no
				// pipeliner is running, so this
				// is truly all done, signal the
				// reader that they can do their
				// final EOF read.
				evNextIsReady.Set()
			}
			m.Unlock()
		}()

		if err != nil {
			m.Lock()
			fetchErr = err
			evNextIsReady.Set()
			m.Unlock()
			return
		}

		// A nil element indicates there are no longer
		// any elemets to be read or pipelined.
		if e == nil {
			m.Lock()
			isEOF = true
			m.Unlock()
			return
		}

		// Process the element per request.
		e = proc(e)

		// Move the element to where it needs to be.
		m.Lock()
		defer m.Unlock()
		// The queuing is only valid if the queue size if greater than 1.
		if len(q) > 1 {
			for i := 0; i < len(q)-1; i++ {
				if q[i+1].i == myI+1 {
					if q[i].e != nil {
						panic("our position in pipeliner is taken")
					}
					q[i].e = e
					q[i].i = myI
					if i == 0 {
						evNextIsReady.Set()
					}
					return
				}
			}
		} else {
			evNextIsReady.Set()
		}
		// Then last item must be our spot.
		if q[len(q)-1].e != nil {
			panic("our last spot in pipeliner is taken")
		}
		q[len(q)-1].e = e
		q[len(q)-1].i = myI
	}

	// Prime all the expected indexes so new batches
	// that are ready have an idea where to go in
	// the array. We also start the first fetchers.
	m.Lock()
	for i := 0; i < len(q); i++ {
		q[i].i = i + 1
		nRunning++
		internalWg.Add(1)
		go f()
	}
	m.Unlock()

	return func() (Element, error) {
		// Wait for the next to be ready.
		evNextIsReady.Wait()
		m.Lock()
		defer m.Unlock()

		// If any fetcher got an error, report it.
		if fetchErr != nil {
			return nil, fetchErr
		}

		// Get the next value to return.
		r := q[0].e

		// Launch a new fetch.
		if !isEOF {
			nRunning++
			internalWg.Add(1)
			go f()
		}

		// If we have EOF and the first batch is nil
		// it means we're all done.
		if isEOF && r == nil {
			return nil, nil
		}

		// Move all the others one level.
		if len(q) > 1 {
			for i := 1; i < len(q); i++ {
				q[i-1] = q[i]
			}
			q[len(q)-1].e = nil
			q[len(q)-1].i = q[len(q)-2].i + 1
		} else {
			q[0].e = nil
			q[0].i = 0
		}

		// If the next one after this is ready or
		// no other pipeliners are running, leave
		// the event alone, otherwise clear it.
		if nil == q[0].e && nRunning != 0 {
			evNextIsReady.Clear()
		}

		return r, nil
	}, closer, nil
}
