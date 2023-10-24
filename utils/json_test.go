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
	"encoding/json"
	"testing"
)

var testData1 = Dict{
	"event": Dict{
		"val1":    "hello world",
		"another": "aaa",
		"more": List{
			Dict{
				"some": "bbb",
			},
			Dict{
				"some": "ccc",
			},
			Dict{
				"some": "ddd",
			},
		},
		"to": Dict{
			"list": List{
				"alistvalue",
			},
		},
	},
	"ttt": "aaa",
	"routing": Dict{
		"oid":      "bbb",
		"mid":      uint64(42),
		"whatever": `{"more": ["dat","and","stuff"]}`,
	},
}

var testData2 = Dict{
	"packet": []interface{}{
		Dict{
			"DstMAC":       "UlQAEjUC",
			"EthernetType": 2048,
			"LayerName":    "Ethernet",
			"Length":       0,
			"SrcMAC":       "CAAnzqZ7",
		},
		Dict{
			"Checksum":   42155,
			"DstIP":      "35.241.147.125",
			"Flags":      2,
			"FragOffset": 0,
			"IHL":        5,
			"Id":         53777,
			"LayerName":  "IPv4",
			"Length":     190,
			"Options":    nil,
			"Padding":    nil,
			"Protocol":   6,
			"SrcIP":      "10.0.2.15",
			"TOS":        0,
			"TTL":        64,
			"Version":    4,
		},
		Dict{
			"ACK":        true,
			"Ack":        8726580,
			"CWR":        false,
			"Checksum":   50221,
			"DataOffset": 5,
			"DstPort":    443,
			"ECE":        false,
			"FIN":        false,
			"LayerName":  "TCP",
			"NS":         false,
			"Options":    []Dict{},
			"PSH":        true,
			"Padding":    nil,
			"RST":        false,
			"SYN":        false,
			"Seq":        4149993746,
			"SrcPort":    36548,
			"URG":        false,
			"Urgent":     0,
			"Window":     65535,
		},
	},
}

func TestTokenizer(t *testing.T) {
	p := tokenizePath("event/another")
	if len(p) != 2 {
		t.Errorf("unexpected tonization: %v", p)
	}
	if p[0] != "event" || p[1] != "another" {
		t.Errorf("unexpected tokenization: %v", p)
	}
}

func TestExtractorString(t *testing.T) {
	se := MakeExtractorForString("event/another")
	s := se(testData1)
	if len(s) != 1 {
		t.Errorf("unexpected number of strings extracted: %v", s)
	}

	se = MakeExtractorForString("event/nope")
	s = se(testData1)
	if len(s) != 0 {
		t.Errorf("unexpected number of strings extracted: %v", s)
	}

	se = MakeExtractorForString("*/another")
	s = se(testData1)
	if len(s) != 1 {
		t.Errorf("unexpected number of strings extracted: %v", s)
	}

	se = MakeExtractorForString("*/some")
	s = se(testData1)
	if len(s) != 3 {
		t.Errorf("unexpected number of strings extracted: %v", s)
	}

	se = MakeExtractorForString("event/to/list/*")
	s = se(testData1)
	if len(s) != 1 {
		t.Errorf("unexpected number of strings extracted: %v", s)
	}
}

func TestExtractorInt(t *testing.T) {
	ie := MakeExtractorForInt("routing/mid")
	s := ie(testData1)
	if len(s) != 1 {
		t.Errorf("unexpected number of ints extracted: %v", s)
	}
}

func TestExtractorPresence(t *testing.T) {
	pe := MakeExtractorForPresence("event/more")
	p := pe(testData1)
	if !p {
		t.Errorf("unexpected presence extracted: %v", p)
	}

	pe = MakeExtractorForPresence("event/nope")
	p = pe(testData1)
	if p {
		t.Errorf("unexpected presence extracted: %v", p)
	}

	pe = MakeExtractorForPresence("event/val1")
	p = pe(testData1)
	if !p {
		t.Errorf("unexpected presence extracted: %v", p)
	}

	pe = MakeExtractorForPresence("event")
	p = pe(testData1)
	if !p {
		t.Errorf("unexpected presence extracted: %v", p)
	}

	pe = MakeExtractorForPresence("ttt")
	p = pe(testData1)
	if !p {
		t.Errorf("unexpected presence extracted: %v", p)
	}

	pe = MakeExtractorForPresence("/")
	p = pe(testData1)
	if !p {
		t.Errorf("unexpected presence extracted: %v", p)
	}
}

func TestFindString(t *testing.T) {
	s := testData1.FindString("event/another")
	if len(s) != 1 {
		t.Errorf("unexpected number of strings extracted: %v", s)
	}

	s = testData1.FindString("event/nope")
	if len(s) != 0 {
		t.Errorf("unexpected number of strings extracted: %v", s)
	}

	s = testData1.FindString("*/another")
	if len(s) != 1 {
		t.Errorf("unexpected number of strings extracted: %v", s)
	}

	s = testData1.FindString("*/some")
	if len(s) != 3 {
		t.Errorf("unexpected number of strings extracted: %v", s)
	}
}

func TestUnmarshalClean(t *testing.T) {
	test1 := `{"a": "hello", "b": 42, "c": 2.0, "d": [42, 2.0]}`

	out, err := UnmarshalCleanJSON(test1)
	if err != nil {
		t.Errorf("failed to clean unmarshal json: %v", err)
	}
	if len(out) != 4 {
		t.Errorf("clean json unmarshal got wrong output: %#+v", out)
	}
	if out["a"] != "hello" {
		t.Errorf("unexpected data: %v", out["a"])
	}
	if out["b"] != uint64(42) {
		t.Errorf("unexpected data: %#v", out["b"])
	}
	if out["c"] != float64(2) {
		t.Errorf("unexpected data: %#v", out["c"])
	}
	l, ok := Dict(out).GetList("d")
	if !ok {
		t.Errorf("unexpected list value: %#v", out["d"])
	}
	if len(l) != 2 {
		t.Errorf("unexpected list len: %v", out["d"])
	}
	if _, ok := l[0].(uint64); !ok {
		t.Errorf("unexpected list elem type: %#v", l[0])
	}
	if _, ok := l[1].(float64); !ok {
		t.Errorf("unexpected list elem type: %#v", l[1])
	}
}

func TestFindStringList(t *testing.T) {
	s := testData2.FindString("packet/DstIP")
	if len(s) != 1 {
		t.Errorf("unexpected number of strings extracted: %v", s)
	}
}

func TestUnmarshalInts(t *testing.T) {
	x := struct {
		D Dict `json:"x"`
	}{}
	sample := []byte("{\"x\":{\"a\":12392878244839687498}}")
	if err := json.Unmarshal(sample, &x); err != nil {
		t.Errorf("error unmarshaling int: %v", err)
	}
	if _, ok := x.D["a"].(uint64); !ok {
		t.Errorf("unexpected int type: %T", x.D["a"])
	}
}

func TestStructAndBack(t *testing.T) {
	s := struct {
		A bool `msgpack:"a" json:"a"`
	}{
		A: true,
	}
	d := Dict{}
	var err error
	d, err = d.ImportFromStruct(s)
	if err != nil {
		t.Errorf("ImportFromStruct: %v", err)
	}
	if b, ok := d.GetBool("a"); !ok || !b {
		t.Errorf("missing bool: %+v", d)
	}
	js, err := json.Marshal(d)
	if err != nil {
		t.Errorf("json.Marshal: %v", err)
	}
	if string(js) != `{"a":true}` {
		t.Errorf("unexpected json: %v", js)
	}
}

func TestRecursiveWildcards(t *testing.T) {
	d := Dict{
		"a": Dict{
			"c": Dict{
				"d": "aaaa",
				"e": "bbbb",
			},
			"f": 66,
		},
		"b": 42,
	}

	e := MakeExtractorForString("a/*")
	s := e(d)
	if len(s) != 2 {
		t.Error("no strings found")
	}
}

func TestExtractorDictAndList(t *testing.T) {
	de := MakeExtractorForDict("event/more/*")
	d := de(testData1)
	if len(d) != 3 {
		t.Errorf("unexpected number of dict extracted: %v", d)
	}

	le := MakeExtractorForList("event/more")
	l := le(testData1)
	if len(l) != 1 {
		t.Errorf("unexpected number of list extracted: %v", l)
	}
}

func TestListIndex(t *testing.T) {
	d := Dict{
		"a": List{
			Dict{
				"d": "aaaa",
				"e": "bbbb",
			},
			Dict{
				"f": "fff",
				"g": "ggg",
			},
		},
		"b": 42,
	}

	e := MakeExtractorForString("a/[1]/f")
	s := e(d)
	if len(s) != 1 {
		t.Error("no strings found")
		return
	}
	if s[0] != "fff" {
		t.Errorf("unexpected value: %+v", s)
	}
}

func TestListOfDict(t *testing.T) {
	testData := Dict{
		"dat": []interface{}{
			map[interface{}]interface{}{
				"a": 23,
				"b": "c",
			},
			map[interface{}]interface{}{
				"d": 23,
				"e": "f",
			},
		},
	}

	tmp, ok := testData.GetListOfDict("dat")
	if !ok {
		t.Error("missing list of string")
	}
	if len(tmp) != 2 {
		t.Errorf("unexpected list size: %d", len(tmp))
	}
}

func TestExpandableFindString(t *testing.T) {
	s := testData1.ExpandableFindString("routing/whatever/more/*")
	if len(s) != 3 {
		t.Errorf("unexpected number of strings extracted: %v", s)
	}

	s = testData1.ExpandableFindString("routing/whatever/nope")
	if len(s) != 0 {
		t.Errorf("unexpected number of strings extracted: %v", s)
	}

	s = testData1.ExpandableFindString("*/more/*")
	if len(s) != 3 {
		t.Errorf("unexpected number of strings extracted: %v", s)
	}
}
