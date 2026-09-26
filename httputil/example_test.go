package httputil_test

import (
	"fmt"
	"html/template"
	"net/http/httptest"

	"go.yaml.in/yaml/v3"

	"github.com/grafana/dskit/httputil"
)

func ExampleWriteYAMLResponse() {
	w := httptest.NewRecorder()
	httputil.WriteYAMLResponse(w, map[string]string{"tenant": "example"}, yaml.Marshal)
	fmt.Print(w.Body.String())
	// Output:
	// tenant: example
}

func ExampleRenderHTTPResponse() {
	page := template.Must(template.New("page").Parse("<p>{{.}}</p>"))
	request := httptest.NewRequest("GET", "/status", nil)
	w := httptest.NewRecorder()
	httputil.RenderHTTPResponse(w, "Ready", page, request, "text/html; charset=utf-8")
	fmt.Println(w.Header().Get("Content-Type"))
	fmt.Println(w.Body.String())
	// Output:
	// text/html; charset=utf-8
	// <p>Ready</p>
}
