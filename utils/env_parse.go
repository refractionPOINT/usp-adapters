package utils

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func ParseCLI(args []string, out interface{}) error {
	data := map[string]interface{}{}
	for _, k := range args {
		tmp := data
		components := strings.Split(k, "=")
		if len(components) != 2 {
			return fmt.Errorf("invalid format: %s", k)
		}
		varPath := components[0]
		val := components[1]
		pathElems := strings.Split(varPath, ".")
		for i, v := range pathElems {
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
