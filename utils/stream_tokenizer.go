package utils

import "errors"

type StreamTokenizer struct {
	MaxSize      int
	ExpectedSize int
	Token        byte
	currentData  []byte
}

var ErrorTooLarge = errors.New("too large")

func (t *StreamTokenizer) Add(data []byte) ([][]byte, error) {
	dataStart := 0

	toReport := [][]byte{}

	for i, b := range data {
		if b == t.Token {
			// Found a newline, so we can use what we
			// have accumulated before plus this as
			// a message.
			if i-1 > dataStart {
				t.currentData = append(t.currentData, data[dataStart:i]...)
				if t.MaxSize != 0 && len(t.currentData) > t.MaxSize {
					t.currentData = nil
					return nil, ErrorTooLarge
				}
			}
			dataStart = i + 1
			toReport = append(toReport, t.currentData)
			t.currentData = make([]byte, 0, t.ExpectedSize)
			continue
		}
		if len(data)-1 == i {
			// This is the end of the buffer and
			// we got no newline, keep it for later.
			t.currentData = append(t.currentData, data[dataStart:i+1]...)
			if t.MaxSize != 0 && len(t.currentData) > t.MaxSize {
				t.currentData = nil
				return nil, ErrorTooLarge
			}
		}
	}
	return toReport, nil
}
