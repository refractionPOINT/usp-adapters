package utils

import (
	"encoding/json"
	"strconv"
	"testing"
)

type testStruct1 struct {
	A1 string      `json:"a1"`
	A2 testStruct2 `json:"b"`
	A3 bool        `json:"a3"`
}
type tmpTestStruct1 testStruct1

type testStruct2 struct {
	B1 string        `json:"b1"`
	B2 int64         `json:"b2"`
	B3 []testStruct3 `json:"b3"`
}

type testStruct3 struct {
	C1 string `json:"c1"`
	C2 string `json:"c2"`
}

// Demonstrating the use of a custom Unmarshaler
// to support string versions of bools.
func (ts *testStruct1) UnmarshalJSON(dat []byte) error {
	d := map[string]interface{}{}
	if err := json.Unmarshal(dat, &d); err != nil {
		return err
	}
	var err error
	if a3, ok := d["a3"]; ok {
		if d["a3"], err = strconv.ParseBool(a3.(string)); err != nil {
			return err
		}
	}
	t, err := json.Marshal(d)
	if err != nil {
		return err
	}
	tts := tmpTestStruct1{}
	if err := json.Unmarshal(t, &tts); err != nil {
		return err
	}
	*ts = testStruct1(tts)
	return nil
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
		A3: true,
	}
	testData := []string{
		"a1=aaa",
		"b.b1=bbb",
		"b.b2=42",
		"b.b3[0].c1=zzz",
		"b.b3[0].c2=ttt",
		"b.b3[1].c1=yyy",
		"b.b3[1].c2=ooo",
		"a3=true",
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
