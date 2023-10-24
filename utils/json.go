package utils

import (
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/vmihailenco/msgpack/v5"
)

// All the accessor functions assume:
// - JSON data that it accesses has been generated
//   as Dict and List, NOT the native Types.
// - Lists contain a single data Type in all of
//   of its elements, NO mix-and-match.

type Dict map[string]interface{}
type List []interface{}

func effectiveDict(d interface{}) Dict {
	r, ok := d.(Dict)
	if ok {
		return r
	}
	r, ok = d.(map[string]interface{})
	if ok {
		return r
	}
	return nil
}

func effectiveList(d interface{}) List {
	var l List
	switch t := d.(type) {
	case []interface{}:
		return t
	case List:
		return t
	case []Dict:
		for _, v := range t {
			l = append(l, v)
		}
	case []string:
		for _, v := range t {
			l = append(l, v)
		}
	}

	return l
}

func (d Dict) Duplicate() Dict {
	return DuplicateMap(d)
}

func (d Dict) GetString(k string) (string, bool) {
	e, ok := d[k]
	if !ok {
		// Not present.
		return "", false
	}
	s, ok := e.(string)
	if !ok {
		// Wrong type.
		return "", false
	}
	return s, true
}

func (d Dict) GetBuffer(k string) ([]byte, bool) {
	e, ok := d.GetString(k)
	if !ok {
		return nil, ok
	}
	s, err := hex.DecodeString(e)
	if err != nil {
		return nil, false
	}
	return s, true
}

func (d Dict) GetBool(k string) (bool, bool) {
	e, ok := d[k]
	if !ok {
		// Not present.
		return false, false
	}
	s, ok := e.(bool)
	if !ok {
		// Wrong type.
		return false, false
	}
	return s, true
}

func (d Dict) GetInt(k string) (uint64, bool) {
	e, ok := d[k]
	if !ok {
		// Not present.
		return 0, false
	}
	// Normalize numbers.
	return StandardInt(e)
}

func StandardInt(v interface{}) (uint64, bool) {
	var s uint64
	switch t := v.(type) {
	case int:
		s = uint64(t)
	case uint:
		s = uint64(t)
	case uint32:
		s = uint64(t)
	case float64:
		s = uint64(t)
	case float32:
		s = uint64(t)
	case uint64:
		s = t
	case int64:
		s = uint64(t)
	case int32:
		s = uint64(t)
	case int8:
		s = uint64(t)
	case uint8:
		s = uint64(t)
	case int16:
		s = uint64(t)
	case uint16:
		s = uint64(t)
	default:
		return 0, false
	}

	return s, true
}

func (d Dict) GetDict(k string) (Dict, bool) {
	e, ok := d[k]
	if !ok {
		// Not present.
		return nil, false
	}
	s := effectiveDict(e)
	if s == nil {
		return nil, false
	}
	return s, true
}

func (d Dict) GetList(k string) (List, bool) {
	e, ok := d[k]
	if !ok {
		// Not present.
		return nil, false
	}
	s := effectiveList(e)
	if s == nil {
		return nil, false
	}
	return s, true
}

func (d Dict) GetListOfDict(k string) ([]Dict, bool) {
	e, ok := d[k]
	if !ok {
		// Not present.
		return nil, false
	}
	l := effectiveList(e)
	if l == nil {
		// Maybe this was already the
		// right type.
		if lD, ok := e.([]Dict); ok {
			return lD, true
		}
		return nil, false
	}
	s := make([]Dict, len(l), len(l))
	isWrongType := false
	for i, v := range l {
		t, ok := v.(Dict)
		if !ok {
			t, ok = v.(map[string]interface{})
			if !ok {
				isWrongType = true
				break
			}
		}
		s[i] = t
	}
	return s, !isWrongType
}

func (d Dict) GetListOfString(k string) ([]string, bool) {
	e, ok := d[k]
	if !ok {
		// Not present.
		return nil, false
	}
	l := effectiveList(e)
	if l == nil {
		// Maybe this was already the
		// right type.
		if lS, ok := e.([]string); ok {
			return lS, true
		}
		return nil, false
	}
	s := make([]string, len(l), len(l))
	isWrongType := false
	for i, v := range l {
		t, ok := v.(string)
		if !ok {
			isWrongType = true
			break
		}
		s[i] = t
	}
	return s, !isWrongType
}

func (d Dict) UnMarshalToStruct(out interface{}) error {
	tmp, err := msgpack.Marshal(d)
	if err != nil {
		return err
	}

	if err := msgpack.Unmarshal(tmp, out); err != nil {
		return err
	}
	return nil
}

func (d Dict) ImportFromStruct(in interface{}) (Dict, error) {
	tmp, err := msgpack.Marshal(in)
	if err != nil {
		return nil, err
	}

	if err := msgpack.Unmarshal(tmp, &d); err != nil {
		return nil, err
	}
	return d, nil
}

func (d Dict) Keys() []string {
	keys := make([]string, 0, len(d))
	for k, _ := range d {
		keys = append(keys, k)
	}
	return keys
}

func (d Dict) Values() []interface{} {
	values := make([]interface{}, 0, len(d))
	for _, v := range d {
		values = append(values, v)
	}
	return values
}

func (d Dict) FindString(path string) []string {
	e := MakeExtractorForString(path)
	return e(d)
}

func (d Dict) FindInt(path string) []uint64 {
	e := MakeExtractorForInt(path)
	return e(d)
}

func (d Dict) FindBool(path string) []bool {
	e := MakeExtractorForBool(path)
	return e(d)
}

func (d Dict) FindOpaque(path string) []interface{} {
	e := MakeExtractorForOpaque(path)
	return e(d)
}

func (d Dict) FindDict(path string) []Dict {
	e := MakeExtractorForDict(path)
	return e(d)
}

func (d Dict) FindList(path string) []List {
	e := MakeExtractorForList(path)
	return e(d)
}

func (d Dict) FindOneString(path string) string {
	e := MakeExtractorForString(path)
	v := e(d)
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

func (d Dict) FindOneInt(path string) uint64 {
	e := MakeExtractorForInt(path)
	v := e(d)
	if len(v) == 0 {
		return 0
	}
	return v[0]
}

func (d Dict) FindOneBool(path string) bool {
	e := MakeExtractorForBool(path)
	v := e(d)
	if len(v) == 0 {
		return false
	}
	return v[0]
}

func (d Dict) FindOneOpaque(path string) interface{} {
	e := MakeExtractorForOpaque(path)
	v := e(d)
	if len(v) == 0 {
		return nil
	}
	return v[0]
}

func (d Dict) FindOneDict(path string) Dict {
	e := MakeExtractorForDict(path)
	v := e(d)
	if len(v) == 0 {
		return nil
	}
	return v[0]
}

func (d Dict) FindOneList(path string) List {
	e := MakeExtractorForList(path)
	v := e(d)
	if len(v) == 0 {
		return nil
	}
	return v[0]
}

func (d *Dict) UnmarshalJSON(data []byte) error {
	c, err := UnmarshalCleanJSON(string(data))
	if err != nil {
		return err
	}
	*d = c

	return nil
}

func findElem(o interface{}, tokens []string, isWildcardDepth bool, isAutoExpand bool) []interface{} {
	var results []interface{}
	if len(tokens) == 0 {
		if isWildcardDepth {
			if dict := effectiveDict(o); dict != nil {
				for _, v := range dict {
					results = append(results, findElem(v, tokens[:], isWildcardDepth, isAutoExpand)...)
				}
			} else if list := effectiveList(o); list != nil {
				for _, elem := range list {
					results = append(results, findElem(elem, tokens[:], isWildcardDepth, isAutoExpand)...)
				}
			} else {
				return []interface{}{o}
			}
			results = append(results, o)
			return results
		}
		return []interface{}{o}
	}
	curToken := tokens[0]
	if dict := effectiveDict(o); dict != nil {
		if curToken == "*" {
			results = findElem(o, tokens[1:], true, isAutoExpand)
		} else if curToken == "?" {
			for _, v := range dict {
				results = append(results, findElem(v, tokens[1:], false, isAutoExpand)...)
			}
		} else if v, ok := dict[curToken]; ok {
			results = append(results, findElem(v, tokens[1:], false, isAutoExpand)...)
		}

		if isWildcardDepth {
			tmpTokens := make([]string, len(tokens))
			copy(tmpTokens, tokens)
			for _, v := range dict {
				results = append(results, findElem(v, tmpTokens, true, isAutoExpand)...)
			}
		}
	} else if list := effectiveList(o); list != nil {
		if strings.HasPrefix(curToken, "[") && strings.HasSuffix(curToken, "]") {
			if listIndex, err := strconv.Atoi(curToken[1 : len(curToken)-1]); err == nil {
				if listIndex < len(list) && listIndex >= 0 {
					results = append(results, findElem(list[listIndex], tokens[1:], isWildcardDepth, isAutoExpand)...)
				}
			}
		} else {
			for _, elem := range list {
				results = append(results, findElem(elem, tokens, isWildcardDepth, isAutoExpand)...)
			}
		}
	}

	return results
}

func checkForElem(o interface{}, tokens []string, isWildcardDepth bool) bool {
	if len(tokens) == 0 {
		return true
	}
	curToken := tokens[0]
	if dict := effectiveDict(o); dict != nil {
		if curToken == "*" {
			if checkForElem(o, tokens[1:], true) {
				return true
			}
		} else if curToken == "?" {
			for _, v := range dict {
				if checkForElem(v, tokens[1:], false) {
					return true
				}
			}
		} else if v, ok := dict[curToken]; ok {
			if checkForElem(v, tokens[1:], false) {
				return true
			}
		}

		if isWildcardDepth {
			tmpTokens := make([]string, len(tokens))
			copy(tmpTokens, tokens)
			for _, v := range dict {
				if checkForElem(v, tmpTokens, true) {
					return true
				}
			}
		}
	} else if list := effectiveList(o); list != nil {
		if strings.HasPrefix(curToken, "[") && strings.HasSuffix(curToken, "]") {
			if listIndex, err := strconv.Atoi(curToken[1 : len(curToken)-1]); err == nil {
				if listIndex < len(list) && listIndex >= 0 {
					if checkForElem(list[listIndex], tokens[1:], isWildcardDepth) {
						return true
					}
				}
			}
		} else {
			for _, elem := range list {
				if checkForElem(elem, tokens, isWildcardDepth) {
					return true
				}
			}
		}
	}

	return false
}

// Tokenize and filter a path.
func tokenizePath(path string) []string {
	var tokens []string
	for _, token := range strings.Split(path, "/") {
		if token != "" {
			tokens = append(tokens, token)
		}
	}
	return tokens
}

// Generator for closures getting a string at a specific path within
// a JSON object, if present.
type StringExtractor func(evt Dict) []string

func MakeExtractorForString(path string) StringExtractor {
	return MakeExpandableExtractorForString(path, false)
}

func MakeExpandableExtractorForString(path string, isAutoExpand bool) StringExtractor {
	tokenizedPath := tokenizePath(path)
	return func(evt Dict) []string {
		var values []string
		for _, v := range findElem(evt, tokenizedPath, false, false) {
			if i, ok := v.(string); ok {
				values = append(values, i)
			}
		}
		return values
	}
}

// Generator for closures getting an int at a specific path within
// a JSON object, if present.
type IntExtractor func(evt Dict) []uint64

func MakeExtractorForInt(path string) IntExtractor {
	return MakeExpandableExtractorForInt(path, false)
}

func MakeExpandableExtractorForInt(path string, isAutoExpand bool) IntExtractor {
	tokenizedPath := tokenizePath(path)
	return func(evt Dict) []uint64 {
		var values []uint64
		for _, v := range findElem(evt, tokenizedPath, false, false) {
			if i, ok := StandardInt(v); ok {
				values = append(values, i)
			}
		}
		return values
	}
}

// Generator for closures getting a bool at a specific path within
// a JSON object, if present.
type BoolExtractor func(evt Dict) []bool

func MakeExtractorForBool(path string) BoolExtractor {
	return MakeExpandableExtractorForBool(path, false)
}

func MakeExpandableExtractorForBool(path string, isAutoExpand bool) BoolExtractor {
	tokenizedPath := tokenizePath(path)
	return func(evt Dict) []bool {
		var values []bool
		for _, v := range findElem(evt, tokenizedPath, false, false) {
			if i, ok := v.(bool); ok {
				values = append(values, i)
			}
		}
		return values
	}
}

// Generator for closures getting an element at a specific path within
// a JSON object, if present.
type OpaqueExtractor func(evt Dict) []interface{}

func MakeExtractorForOpaque(path string) OpaqueExtractor {
	return MakeExpandableExtractorForOpaque(path, false)
}

func MakeExpandableExtractorForOpaque(path string, isAutoExpand bool) OpaqueExtractor {
	tokenizedPath := tokenizePath(path)
	return func(evt Dict) []interface{} {
		var values []interface{}
		for _, v := range findElem(evt, tokenizedPath, false, false) {
			values = append(values, v)
		}
		return values
	}
}

// Generator for closures getting a Dict at a specific path within
// a JSON object, if present.
type DictExtractor func(evt Dict) []Dict

func MakeExtractorForDict(path string) DictExtractor {
	return MakeExpandableExtractorForDict(path, false)
}

func MakeExpandableExtractorForDict(path string, isAutoExpand bool) DictExtractor {
	tokenizedPath := tokenizePath(path)
	return func(evt Dict) []Dict {
		var values []Dict
		for _, v := range findElem(evt, tokenizedPath, false, false) {
			d, ok := v.(Dict)
			if !ok {
				d, ok = v.(map[string]interface{})
				if !ok {
					continue
				}
			}
			values = append(values, d)
		}
		return values
	}
}

// Generator for closures getting a Dict at a specific path within
// a JSON object, if present.
type ListExtractor func(evt Dict) []List

func MakeExtractorForList(path string) ListExtractor {
	return MakeExpandableExtractorForList(path, false)
}

func MakeExpandableExtractorForList(path string, isAutoExpand bool) ListExtractor {
	tokenizedPath := tokenizePath(path)
	return func(evt Dict) []List {
		var values []List
		for _, v := range findElem(evt, tokenizedPath, false, false) {
			d, ok := v.(List)
			if !ok {
				d, ok = v.([]interface{})
				if !ok {
					continue
				}
			}
			values = append(values, d)
		}
		return values
	}
}

type PresenceExtractor func(evt Dict) bool

func MakeExtractorForPresence(path string) PresenceExtractor {
	tokenizedPath := tokenizePath(path)
	return func(evt Dict) bool {
		return checkForElem(evt, tokenizedPath, false)
	}
}

func DuplicateMap(m map[string]interface{}) map[string]interface{} {
	mapCopy := make(map[string]interface{})
	jsonString := []byte{}
	var err error
	if jsonString, err = msgpack.Marshal(m); err != nil {
		return nil
	}
	if err = msgpack.Unmarshal(jsonString, &mapCopy); err != nil {
		return nil
	}
	return mapCopy
}

func UnmarshalCleanJSON(data string) (map[string]interface{}, error) {
	d := json.NewDecoder(strings.NewReader(data))
	d.UseNumber()

	out := map[string]interface{}{}
	if err := d.Decode(&out); err != nil {
		return nil, err
	}

	if err := unmarshalCleanJSONMap(out); err != nil {
		return nil, err
	}

	return out, nil
}

func unmarshalCleanJSONMap(out map[string]interface{}) error {
	for k, v := range out {
		switch val := v.(type) {
		case map[string]interface{}:
			if err := unmarshalCleanJSONMap(val); err != nil {
				return err
			}
		case []interface{}:
			if err := unmarshalCleanJSONList(val); err != nil {
				return err
			}
		default:
			var err error
			out[k], err = unmarshalCleanJSONElement(val)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func unmarshalCleanJSONList(out []interface{}) error {
	for i, v := range out {
		switch val := v.(type) {
		case map[string]interface{}:
			if err := unmarshalCleanJSONMap(val); err != nil {
				return err
			}
		case []interface{}:
			if err := unmarshalCleanJSONList(val); err != nil {
				return err
			}
		default:
			var err error
			out[i], err = unmarshalCleanJSONElement(val)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func unmarshalCleanJSONElement(out interface{}) (interface{}, error) {
	if val, ok := out.(json.Number); ok {
		// This is a Number, we need to check if it has
		// a float component originally to see if we should
		// type it as an int64 or a float64.
		original := val.String()
		if !strings.Contains(original, ".") {
			// No dot component, return as uint64 so
			// that it gets serialized back to a notation
			// without a dot in it.
			if !strings.HasPrefix(original, "-") {
				// We cannot use val.Int64() because it does not
				// support Unsigned 64 bit ints.
				i, err := strconv.ParseUint(original, 10, 64)
				if err != nil {
					return nil, err
				}
				out = i
			} else {
				// Looks like a signed value.
				i, err := val.Int64()
				if err != nil {
					return nil, err
				}
				out = i
			}
		} else {
			// There is a dot, assume a float.
			i, err := val.Float64()
			if err != nil {
				return nil, err
			}
			out = i
		}
	}
	return out, nil
}
