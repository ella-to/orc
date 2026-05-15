// Package web serves a small, dependency-free HTML/CSS/JS dashboard for
// the ORC admin HTTP API. The whole UI lives in a single index.html file
// that is compiled into the binary via go:embed and served by Handler.
//
// The UI talks to the AdminHandler exposed by the parent package using
// plain fetch() requests against the API base path supplied to Handler.
//
// Mount example:
//
//	mux := http.NewServeMux()
//	mux.Handle("/admin/", http.StripPrefix("/admin", orc.AdminHandler(ctx)))
//	mux.Handle("/ui/",    http.StripPrefix("/ui",    web.Handler("/admin")))
//	http.ListenAndServe(":8080", mux)
//
// Authentication and authorization are out of scope for this package;
// protect the handler behind a reverse proxy or middleware before
// exposing it externally.
package web

import (
	"bytes"
	_ "embed"
	"net/http"
	"strings"
)

//go:embed ui/index.html
var indexHTML []byte

// apiBasePlaceholder is the literal string the UI looks for at startup.
// We replace it once at handler-construction time so the page itself can
// remain a static asset.
const apiBasePlaceholder = "__API_BASE__"

// Handler returns an http.Handler that serves the embedded ORC admin UI.
//
// apiBase is the URL prefix at which the orc.AdminHandler is mounted
// (for example "/admin"). The UI calls "{apiBase}/workflows", "{apiBase}/info",
// etc. Pass an empty string to default to "/admin".
//
// The handler answers any GET / HEAD request — including unknown sub-paths —
// with the same index.html so that client-side hash routing works without
// any further wiring on the server side.
func Handler(apiBase string) http.Handler {
	if apiBase == "" {
		apiBase = "/admin"
	}
	apiBase = "/" + strings.Trim(apiBase, "/")
	body := bytes.ReplaceAll(indexHTML, []byte(apiBasePlaceholder), []byte(apiBase))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write(body)
	})
}
