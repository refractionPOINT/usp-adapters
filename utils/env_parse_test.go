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
	B1 string `json:"b1"`
	B2 int64  `json:"b2"`
}

func TestParseCLI(t *testing.T) {
	expectedData := testStruct1{
		A1: "aaa",
		A2: testStruct2{
			B1: "bbb",
			B2: 42,
		},
	}
	testData := []string{
		"a1=aaa",
		"b.b1=bbb",
		"b.b2=42",
	}
	actualData := testStruct1{}

	if err := ParseCLI(testData, &actualData); err != nil {
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
