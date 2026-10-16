package utils

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// Closing an adapter while it is shipping makes Ship return
// uspclient.ErrorClosed. That is a normal shutdown, so no adapter may report it
// through OnError. This guard walks every adapter and requires each OnError call
// that reports a Ship failure ("...Ship(): %v") to sit under an if whose
// condition checks for ErrorClosed. A new adapter that copies the old pattern
// fails here instead of raising an error on every graceful teardown.
func TestShipFailuresAreNotReportedWhenClosed(t *testing.T) {
	root := ".."
	fset := token.NewFileSet()
	nChecked := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		var stack []ast.Node
		ast.Inspect(f, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			stack = append(stack, n)
			call, ok := n.(*ast.CallExpr)
			if !ok || !isOnErrorShipReport(call) {
				return true
			}
			nChecked++
			for _, anc := range stack {
				if ifs, ok := anc.(*ast.IfStmt); ok && mentionsErrorClosed(ifs.Cond) {
					return true
				}
			}
			t.Errorf("%s: OnError reports a Ship failure without excluding uspclient.ErrorClosed", fset.Position(call.Pos()))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if nChecked < 30 {
		t.Fatalf("only %d Ship error reports found, the guard is not looking at the adapters", nChecked)
	}
}

func isOnErrorShipReport(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "OnError" || len(call.Args) != 1 {
		return false
	}
	inner, ok := call.Args[0].(*ast.CallExpr)
	if !ok || len(inner.Args) == 0 {
		return false
	}
	lit, ok := inner.Args[0].(*ast.BasicLit)
	return ok && strings.Contains(lit.Value, "hip(): ")
}

func mentionsErrorClosed(e ast.Expr) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if s, ok := n.(*ast.SelectorExpr); ok && s.Sel.Name == "ErrorClosed" {
			found = true
		}
		return !found
	})
	return found
}
