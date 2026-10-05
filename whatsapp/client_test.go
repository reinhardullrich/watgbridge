package whatsapp

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// Guard startup ordering without connecting either messaging account.
func TestHandlerRegisteredBeforeConnect(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "client.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	registrations, connections := 0, 0
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		obj, ok := sel.X.(*ast.Ident)
		if !ok || obj.Name != "client" {
			return true
		}
		switch sel.Sel.Name {
		case "AddEventHandler":
			registrations++
		case "Connect":
			connections++
			if registrations != 1 {
				t.Error("Connect precedes handler registration")
			}
		}
		return true
	})
	if registrations != 1 || connections != 2 {
		t.Fatalf("unexpected startup layout: %d handlers, %d connections", registrations, connections)
	}
}
