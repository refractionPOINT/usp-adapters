package utils

import (
	"encoding/json"
	"fmt"
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
					if num, err := strconv.ParseInt(val, 10, 64); err == nil {
						tmp[actualKey].([]interface{})[index] = num
					} else {
						tmp[actualKey].([]interface{})[index] = val
					}
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
				if num, err := strconv.ParseInt(val, 10, 64); err == nil {
					tmp[v] = num
				} else {
					tmp[v] = val
				}
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
