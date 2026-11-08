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
// Record field failures drop only that record; legacy paging errors are advisory.
// Unscoped, connection and correct-query paging failures still fail the window.
type graphQLErrorDisposition struct {
	skipRecords       map[int]bool
	warnOptional      bool
	legacyPagingError bool
}

func (a *WizAdapter) classifyGraphQLErrors(errs []interface{}, cursorQueryOK bool) (graphQLErrorDisposition, bool) {
	out := graphQLErrorDisposition{skipRecords: make(map[int]bool)}
	if len(a.conf.DataPath) < 2 || a.conf.DataPath[0] != "data" {
		return out, false
	}
	connection := a.conf.DataPath[1 : len(a.conf.DataPath)-1]
	records := a.conf.DataPath[len(a.conf.DataPath)-1]
	for _, raw := range errs {
		e, ok := raw.(map[string]interface{})
		if !ok {
			return out, false
		}
		path, ok := e["path"].([]interface{})
		if !ok || len(path) == 0 {
			return out, false
		}
		unrelated := false
		for i, name := range connection {
			if i >= len(path) {
				return out, false
			}
			part, ok := path[i].(string)
			if !ok {
				return out, false
			}
			if part != name {
				unrelated = true
				break
			}
		}
		if unrelated {
			out.warnOptional = true
			continue
		}
		rest := path[len(connection):]
		if len(rest) == 0 {
			return out, false
		}
		field, ok := rest[0].(string)
		if !ok {
			return out, false
		}
		if field == "pageInfo" {
			if cursorQueryOK {
				return out, false
			}
			out.legacyPagingError = true
			continue
		}
		if field != records {
			out.warnOptional = true
			continue
		}
		if len(rest) < 3 {
			return out, false
		}
		index, ok := rest[1].(float64)
		if !ok {
			return out, false
		}
		i := int(index)
		if i < 0 || float64(i) != index {
			return out, false
		}
		if _, ok := rest[2].(string); !ok {
			return out, false
		}
		out.skipRecords[i] = true
	}
	return out, true
}
