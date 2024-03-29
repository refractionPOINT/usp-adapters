package utils

import (
	"strings"
	"testing"
)

func TestStreamTokenizerBasic(t *testing.T) {
	s := StreamTokenizer{
		MaxSize: 0,
		Token:   0x0a,
	}

	testData := []string{
		"this is line 1",
		"and 2",
		"and a longer one and a longer oneand a longer one and a longer one and a longer one",
	}
	asStream := []byte(strings.Join(testData, "\n"))
	asStream = append(asStream, byte('\n'))
	out := []string{}
	i := 0
	tmp := []byte{}
	for _, b := range asStream {
		i++
		tmp = append(tmp, b)
		if i%3 == 0 {
			elems, err := s.Add(tmp)
			if err != nil {
				t.Errorf("Add(): %v", err)
			}
			for _, elem := range elems {
				out = append(out, string(elem))
			}
			tmp = []byte{}
		}
	}

	if len(testData) != len(out) {
		t.Errorf("unexpected tokenized data: %+v", out)
		return
	}

	for i := range testData {
		if testData[i] != out[i] {
			t.Errorf("output mismatch: %v != %v", testData[i], out[i])
		}
	}
}

func TestStreamTokenizerMaxSize(t *testing.T) {
	s := StreamTokenizer{
		MaxSize: 20,
		Token:   0x0a,
	}

	testData := []string{
		"this is line 1",
		"and 2",
		"and a longer one and a longer oneand a longer one and a longer one and a longer one",
	}
	asStream := []byte(strings.Join(testData, "\n"))
	asStream = append(asStream, byte('\n'))
	out := []string{}
	i := 0
	tmp := []byte{}
	for _, b := range asStream {
		i++
		tmp = append(tmp, b)
		if i%3 == 0 {
			elems, err := s.Add(tmp)
			if err != nil {
				break
			}
			for _, elem := range elems {
				out = append(out, string(elem))
			}
			tmp = []byte{}
		}
	}

	if len(testData[:2]) != len(out) {
		t.Errorf("unexpected tokenized data: %+v", out)
		return
	}

	for i := range testData[:2] {
		if testData[i] != out[i] {
			t.Errorf("output mismatch: %v != %v", testData[i], out[i])
		}
	}
}

func TestStreamTokenizerBounds(t *testing.T) {
	s := StreamTokenizer{
		MaxSize: 1024,
		Token:   0x0a,
	}

	testData := []string{
		"this is line 1\n",
		"and 2\n",
		"and a longer one and a longer oneand a longer one and a longer one and a longer one\n",
	}
	for i, b := range testData {
		elems, err := s.Add([]byte(b))
		if err != nil {
			t.Errorf("Add(): %v", err)
		}
		if len(elems) != 1 {
			t.Errorf("Unexepcted chunks: %+v", elems)
		}
		if string(elems[0]) != testData[i][0:len(testData[i])-1] {
			t.Errorf("Unexpected chunk value: %s", string(elems[0]))
		}
	}
}
