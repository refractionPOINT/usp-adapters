package usp_wiz

import (
	"fmt"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

// validateCursorQuery checks syntax, not a vendor schema. Only the configured
// connection's direct field path is supported for cursor traversal. Never guess
// whether an unrelated argument, comment or string consumes the cursor.
func (a *WizAdapter) validateCursorQuery() error {
	invalid := func() error {
		return fmt.Errorf("Wiz pagination configuration: use a single query declaring $after and passing after: $after on the connection selected by data_path; direct field selections are required")
	}
	doc, err := parser.ParseQuery(&ast.Source{Input: a.conf.Query})
	if err != nil || len(doc.Operations) != 1 || len(a.conf.DataPath) < 3 || a.conf.DataPath[0] != "data" {
		return invalid()
	}
	op := doc.Operations[0]
	if op.Operation != ast.Query || op.VariableDefinitions.ForName("after") == nil {
		return invalid()
	}
	selections := op.SelectionSet
	var connection *ast.Field
	for _, name := range a.conf.DataPath[1 : len(a.conf.DataPath)-1] {
		connection = nil
		for _, selection := range selections {
			f, ok := selection.(*ast.Field)
			if !ok {
				continue
			}
			responseName := f.Alias
			if responseName == "" {
				responseName = f.Name
			}
			if responseName == name {
				if connection != nil {
					return invalid()
				}
				connection = f
			}
		}
		if connection == nil {
			return invalid()
		}
		selections = connection.SelectionSet
	}
	argument := connection.Arguments.ForName("after")
	if argument == nil || argument.Value == nil || argument.Value.Kind != ast.Variable || argument.Value.Raw != "after" {
		return invalid()
	}
	return nil
}

// GraphQL error paths use response names and omit the outer "data" key.
// Unscoped errors and errors on ancestors, records, paging, ID or time cannot
// safely be treated as optional, even if some data was returned.
func (a *WizAdapter) onlyOptionalGraphQLErrors(errs []interface{}) bool {
	if len(a.conf.DataPath) < 2 || a.conf.DataPath[0] != "data" {
		return false
	}
	connection := a.conf.DataPath[1 : len(a.conf.DataPath)-1]
	records := a.conf.DataPath[len(a.conf.DataPath)-1]
	for _, raw := range errs {
		e, ok := raw.(map[string]interface{})
		if !ok {
			return false
		}
		path, ok := e["path"].([]interface{})
		if !ok || len(path) == 0 {
			return false
		}
		unrelated := false
		for i, name := range connection {
			if i >= len(path) {
				return false
			}
			part, ok := path[i].(string)
			if !ok {
				return false
			}
			if part != name {
				unrelated = true
				break
			}
		}
		if unrelated {
			continue
		}
		rest := path[len(connection):]
		if len(rest) == 0 {
			return false
		}
		field, ok := rest[0].(string)
		if !ok {
			return false
		}
		if field == "pageInfo" {
			return false
		}
		if field != records {
			continue
		}
		if len(rest) < 3 {
			return false
		}
		if _, ok := rest[1].(float64); !ok {
			return false
		}
		field, ok = rest[2].(string)
		if !ok || field == a.conf.IDField || field == a.conf.TimeField {
			return false
		}
	}
	return true
}
