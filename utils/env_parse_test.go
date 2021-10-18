package utils

import (
	"encoding/json"
	"testing"
)

type testStruct1 struct {
	A1 string      `json:"a1"`
	A2 testStruct2 `json:"b"`
}

type testStruct2 struct {
	B1 string        `json:"b1"`
	B2 int64         `json:"b2"`
	B3 []testStruct3 `json:"b3"`
}

type testStruct3 struct {
	C1 string `json:"c1"`
	C2 string `json:"c2"`
}

func TestParseCLI(t *testing.T) {
	expectedData := testStruct1{
		A1: "aaa",
		A2: testStruct2{
			B1: "bbb",
			B2: 42,
			B3: []testStruct3{
				{
					C1: "zzz",
					C2: "ttt",
				},
				{
					C1: "yyy",
					C2: "ooo",
				},
			},
		},
	}
	testData := []string{
		"a1=aaa",
		"b.b1=bbb",
		"b.b2=42",
		"b.b3[0].c1=zzz",
		"b.b3[0].c2=ttt",
		"b.b3[1].c1=yyy",
		"b.b3[1].c2=ooo",
	}
	actualData := testStruct1{}

	if err := ParseCLI("", testData, &actualData); err != nil {
		t.Errorf("ParseCLI(): %v", err)
	}

	jsonOut, err := json.Marshal(actualData)
	if err != nil {
		t.Errorf("MarshalActual: %v", err)
	}
	expectedOut, err := json.Marshal(expectedData)
	if err != nil {
		t.Errorf("MarshalExpected: %v", err)
	}
	if string(jsonOut) != string(expectedOut) {
		t.Errorf("mismatch:\n%+v\n%+v", string(jsonOut), string(expectedOut))
	}
}
