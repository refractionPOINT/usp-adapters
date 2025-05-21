package utils

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

var arrayIndexRE = regexp.MustCompile(`(.+)\[(\d+)\]$`)

// Parses a list of tokens (like from a CLI) into
// a struct, using json.Unmarshal. Simplifies the
// population of configuration structs via CLI
// of a container without a custom container.
func ParseCLI(prefix string, args []string, out interface{}) error {
	// Get the type information of the output struct
	outType := reflect.TypeOf(out).Elem()

	// Create a map to store field types by JSON tag
	fieldTypes := make(map[string]reflect.Type)
	buildFieldTypesMap(outType, "", fieldTypes)

	data := map[string]interface{}{}
	for _, k := range args {
		tmp := data
		components := strings.SplitN(k, "=", 2)
		if len(components) < 2 {
			continue
		}
		varPath := components[0]
		val := components[1]
		pathElems := strings.Split(varPath, ".")
		if prefix != "" {
			pathElems = append([]string{prefix}, pathElems...)
		}

		// Get the full path for type lookup
		fullPath := strings.Join(pathElems, ".")
		fieldType := fieldTypes[fullPath]

		for i, v := range pathElems {
			if matches := arrayIndexRE.FindStringSubmatch(v); len(matches) == 3 {
				index, err := strconv.Atoi(matches[2])
				if err != nil {
					continue
				}
				actualKey := matches[1]
				existing, ok := tmp[actualKey]
				if !ok {
					tmp[actualKey] = make([]interface{}, index+1)
					existing = tmp[actualKey]
				}
				existingArray := existing.([]interface{})
				if len(existingArray) <= index {
					for x := len(existingArray); x <= index; x++ {
						tmp[actualKey] = append(existingArray, nil)
					}
				}
				if i == len(pathElems)-1 {
					// Convert value based on field type
					convertedVal := convertValue(val, fieldType)
					tmp[actualKey].([]interface{})[index] = convertedVal
				} else {
					if tmp[actualKey].([]interface{})[index] != nil {
						existingDict, ok := tmp[actualKey].([]interface{})[index].(map[string]interface{})
						if !ok {
							return fmt.Errorf("namespace collision: %v", v)
						}
						tmp = existingDict
						continue
					}
					newDict := map[string]interface{}{}
					tmp[actualKey].([]interface{})[index] = newDict
					tmp = newDict
				}
				continue
			}
			if i == len(pathElems)-1 {
				// Convert value based on field type
				convertedVal := convertValue(val, fieldType)
				tmp[v] = convertedVal
				continue
			}
			if existing, ok := tmp[v]; ok {
				existingDict, ok := existing.(map[string]interface{})
				if !ok {
					return fmt.Errorf("namespace collision: %v", v)
				}
				tmp = existingDict
				continue
			}
			newDict := map[string]interface{}{}
			tmp[v] = newDict
			tmp = newDict
		}
	}

	j, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(j, out); err != nil {
		return err
	}
	return nil
}

// buildFieldTypesMap recursively builds a map of JSON tag paths to their corresponding types
func buildFieldTypesMap(t reflect.Type, prefix string, fieldTypes map[string]reflect.Type) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" {
			jsonTag = field.Name
		}
		jsonTag = strings.Split(jsonTag, ",")[0] // Remove any options

		currentPath := jsonTag
		if prefix != "" {
			currentPath = prefix + "." + jsonTag
		}

		if field.Type.Kind() == reflect.Struct {
			buildFieldTypesMap(field.Type, currentPath, fieldTypes)
		} else if field.Type.Kind() == reflect.Slice {
			// For slices, we need to handle the element type
			elemType := field.Type.Elem()
			if elemType.Kind() == reflect.Struct {
				buildFieldTypesMap(elemType, currentPath, fieldTypes)
			}
		}
		fieldTypes[currentPath] = field.Type
	}
}

// convertValue converts a string value to the appropriate type based on the target field type
func convertValue(val string, fieldType reflect.Type) interface{} {
	if fieldType == nil {
		// If we don't know the type, try to guess
		if b, err := strconv.ParseBool(val); val != "0" && val != "1" && err == nil {
			return b
		}
		if num, err := strconv.ParseInt(val, 10, 64); err == nil {
			return num
		}
		return val
	}

	switch fieldType.Kind() {
	case reflect.Bool:
		if b, err := strconv.ParseBool(val); err == nil {
			return b
		}
		return val
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if num, err := strconv.ParseInt(val, 10, 64); err == nil {
			return num
		}
		return val
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if num, err := strconv.ParseUint(val, 10, 64); err == nil {
			return num
		}
		return val
	case reflect.Float32, reflect.Float64:
		if num, err := strconv.ParseFloat(val, 64); err == nil {
			return num
		}
		return val
	default:
		return val
	}
}
