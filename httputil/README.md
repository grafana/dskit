# HTTP response helpers

The response helpers preserve the JSON, text, HTML, and YAML response behavior
shared by Mimir, Loki, and Tempo. They do not select status codes for successful
responses; a caller can write a status and headers before invoking a helper.

Pass the application's existing `yaml.Marshal` to `WriteYAMLResponse` and
`StreamWriteYAMLResponse`. This preserves its YAML version, custom marshalers,
and formatting without making this package depend on a YAML library:

```go
func WriteYAMLResponse(w http.ResponseWriter, value any) {
    httputil.WriteYAMLResponse(w, value, yaml.Marshal)
}
```

`RenderHTTPResponse` takes an HTML content type as its final argument:

- Use `"text/html; charset=utf-8"` for Mimir's explicit header behavior.
- Use `""` for Loki and Tempo's existing-header/content-detection behavior.

JSON selection preserves the existing case-sensitive `application/json`
substring check in `Accept`, including its handling of quality values. JSON
responses use `json.Marshal` without an added newline. Streaming YAML adds no
document separators and does not flush; marshal errors are logged and skipped,
and write errors end the stream.

Templates write directly to the response. If rendering fails after output has
started, the error is appended and the committed status cannot be changed.
