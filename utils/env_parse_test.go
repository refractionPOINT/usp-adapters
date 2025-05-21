package utils

import (
	"encoding/json"
	"reflect"
	"testing"
)

type testStruct1 struct {
	A1 string      `json:"a1"`
	A2 testStruct2 `json:"b"`
	A3 bool        `json:"a3"`
	A4 string      `json:"a4"`
	A5 string      `json:"a5"`
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
		A3: true,
		A4: "42",
		A5: "true",
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
		"a4=42",
		"a5=true",
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

func TestParseCLISimple(t *testing.T) {
	type SimpleStruct struct {
		Name    string `json:"name"`
		Age     int    `json:"age"`
		Enabled bool   `json:"enabled"`
	}

	expectedData := SimpleStruct{
		Name:    "test",
		Age:     25,
		Enabled: true,
	}

	testData := []string{
		"name=test",
		"age=25",
		"enabled=true",
	}

	actualData := SimpleStruct{}
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

func TestParseCLIArrayEdgeCases(t *testing.T) {
	type ArrayStruct struct {
		Items []string `json:"items"`
	}

	expectedData := ArrayStruct{
		Items: []string{"first", "second", "third"},
	}

	// Order the array indices sequentially to avoid out-of-range issues
	testData := []string{
		"items[0]=first",
		"items[1]=second",
		"items[2]=third",
	}

	actualData := ArrayStruct{}
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

func TestParseCLITypeConversion(t *testing.T) {
	type TypeStruct struct {
		IntVal    int     `json:"int_val"`
		FloatVal  float64 `json:"float_val"`
		BoolVal   bool    `json:"bool_val"`
		StringVal string  `json:"string_val"`
		UintVal   uint    `json:"uint_val"`
	}

	expectedData := TypeStruct{
		IntVal:    42,
		FloatVal:  3.14,
		BoolVal:   true,
		StringVal: "test",
		UintVal:   100,
	}

	testData := []string{
		"int_val=42",
		"float_val=3.14",
		"bool_val=true",
		"string_val=test",
		"uint_val=100",
	}

	actualData := TypeStruct{}
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

func TestParseCLIWithPrefix(t *testing.T) {
	type PrefixedStruct struct {
		Config struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		} `json:"config"`
	}

	expectedData := PrefixedStruct{}
	expectedData.Config.Name = "test"
	expectedData.Config.Age = 25

	testData := []string{
		"name=test",
		"age=25",
	}

	actualData := PrefixedStruct{}
	if err := ParseCLI("config", testData, &actualData); err != nil {
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

func TestParseCLIInvalidInput(t *testing.T) {
	type TestStruct struct {
		Name string `json:"name"`
	}

	testCases := []struct {
		name     string
		input    []string
		expected error
	}{
		{
			name:     "empty value",
			input:    []string{"name="},
			expected: nil,
		},
		{
			name:     "invalid format",
			input:    []string{"invalid"},
			expected: nil,
		},
		{
			name:     "invalid array index",
			input:    []string{"items[abc]=value"},
			expected: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualData := TestStruct{}
			err := ParseCLI("", tc.input, &actualData)
			if err != tc.expected {
				t.Errorf("expected error %v, got %v", tc.expected, err)
			}
		})
	}
}

func TestConvertValue(t *testing.T) {
	testCases := []struct {
		name      string
		val       string
		fieldType reflect.Type
		expected  interface{}
	}{
		{
			name:      "string to bool",
			val:       "true",
			fieldType: reflect.TypeOf(true),
			expected:  true,
		},
		{
			name:      "string to bool false",
			val:       "false",
			fieldType: reflect.TypeOf(true),
			expected:  false,
		},
		{
			name:      "string to int",
			val:       "42",
			fieldType: reflect.TypeOf(0),
			expected:  int64(42),
		},
		{
			name:      "string to uint",
			val:       "42",
			fieldType: reflect.TypeOf(uint(0)),
			expected:  uint64(42),
		},
		{
			name:      "string to float",
			val:       "3.14",
			fieldType: reflect.TypeOf(0.0),
			expected:  3.14,
		},
		{
			name:      "string to string",
			val:       "test",
			fieldType: reflect.TypeOf(""),
			expected:  "test",
		},
		{
			name:      "invalid bool",
			val:       "not-a-bool",
			fieldType: reflect.TypeOf(true),
			expected:  "not-a-bool",
		},
		{
			name:      "invalid int",
			val:       "not-an-int",
			fieldType: reflect.TypeOf(0),
			expected:  "not-an-int",
		},
		{
			name:      "nil type",
			val:       "test",
			fieldType: nil,
			expected:  "test",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := convertValue(tc.val, tc.fieldType)
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("expected %v (%T), got %v (%T)", tc.expected, tc.expected, result, result)
			}
		})
	}
}

func TestBuildFieldTypesMap(t *testing.T) {
	type NestedStruct struct {
		Field1 string `json:"field1"`
		Field2 int    `json:"field2"`
	}

	type TestStruct struct {
		Name      string       `json:"name"`
		Age       int          `json:"age"`
		Nested    NestedStruct `json:"nested"`
		Array     []string     `json:"array"`
		NoJSONTag string
	}

	fieldTypes := make(map[string]reflect.Type)
	buildFieldTypesMap(reflect.TypeOf(TestStruct{}), "", fieldTypes)

	testCases := []struct {
		name     string
		path     string
		expected reflect.Type
	}{
		{
			name:     "simple field",
			path:     "name",
			expected: reflect.TypeOf(""),
		},
		{
			name:     "nested field",
			path:     "nested.field1",
			expected: reflect.TypeOf(""),
		},
		{
			name:     "array field",
			path:     "array",
			expected: reflect.TypeOf([]string{}),
		},
		{
			name:     "no json tag",
			path:     "NoJSONTag",
			expected: reflect.TypeOf(""),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualType, exists := fieldTypes[tc.path]
			if !exists {
				t.Errorf("field type not found for path: %s", tc.path)
				return
			}
			if actualType != tc.expected {
				t.Errorf("expected type %v, got %v", tc.expected, actualType)
			}
		})
	}

	// Test with prefix
	fieldTypesWithPrefix := make(map[string]reflect.Type)
	buildFieldTypesMap(reflect.TypeOf(TestStruct{}), "config", fieldTypesWithPrefix)

	prefixedPath := "config.name"
	actualType, exists := fieldTypesWithPrefix[prefixedPath]
	if !exists {
		t.Errorf("field type not found for prefixed path: %s", prefixedPath)
	} else if actualType != reflect.TypeOf("") {
		t.Errorf("expected type %v for prefixed path, got %v", reflect.TypeOf(""), actualType)
	}
}

func TestBuildFieldTypesMapWithComplexTypes(t *testing.T) {
	type DeepNested struct {
		Value string `json:"value"`
	}

	type NestedStruct struct {
		Deep    DeepNested   `json:"deep"`
		Numbers []int        `json:"numbers"`
		Structs []DeepNested `json:"structs"`
	}

	type ComplexStruct struct {
		Simple  string         `json:"simple"`
		Nested  NestedStruct   `json:"nested"`
		Map     map[string]int `json:"map"`
		Pointer *string        `json:"pointer"`
	}

	fieldTypes := make(map[string]reflect.Type)
	buildFieldTypesMap(reflect.TypeOf(ComplexStruct{}), "", fieldTypes)

	testCases := []struct {
		name     string
		path     string
		expected reflect.Type
	}{
		{
			name:     "deep nested field",
			path:     "nested.deep.value",
			expected: reflect.TypeOf(""),
		},
		{
			name:     "nested array of primitives",
			path:     "nested.numbers",
			expected: reflect.TypeOf([]int{}),
		},
		{
			name:     "nested array of structs",
			path:     "nested.structs",
			expected: reflect.TypeOf([]DeepNested{}),
		},
		{
			name:     "map field",
			path:     "map",
			expected: reflect.TypeOf(map[string]int{}),
		},
		{
			name:     "pointer field",
			path:     "pointer",
			expected: reflect.TypeOf((*string)(nil)),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualType, exists := fieldTypes[tc.path]
			if !exists {
				t.Errorf("field type not found for path: %s", tc.path)
				return
			}
			if actualType != tc.expected {
				t.Errorf("expected type %v, got %v", tc.expected, actualType)
			}
		})
	}
}
