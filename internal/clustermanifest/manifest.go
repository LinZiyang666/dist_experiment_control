// Package clustermanifest serves the C2 well-known cluster discovery manifest
// (/.well-known/tether/cluster.json) over a loopback-only HTTP listener. The endpoint is
// UNAUTHENTICATED — it serves an account-SIGNED, discovery-only manifest (public roster + public
// seed endpoints, zero secrets) — so it is bound to loopback and meant to sit behind the operator's
// Caddy/WSS reverse proxy. It is a pure leaf: net/http only, no DB/NATS/seed material.
package clustermanifest

import (
	"context"
	"github.com/LinZiyang666/tether/internal/httplisten"
	"net"
	"net/http"
)

// ManifestPath is the single GET route served.
const ManifestPath = "/.well-known/tether/cluster.json"

// Bind opens a loopback-ONLY TCP listener. A non-loopback addr is REFUSED: public exposure must go
// through the operator's reverse proxy (the endpoint is unauthenticated).
func Bind(addr string) (net.Listener, error) {
	// requireLoopback=true: the manifest is unauthenticated + Caddy-fronted.
	return httplisten.Bind("clustermanifest", addr, true)
}

// ServeListener serves until ctx is canceled. body returns the pre-signed manifest bytes, or
// ok=false when the cluster has no manifest yet (→ 503).
func ServeListener(ctx context.Context, ln net.Listener, body func() ([]byte, bool)) error {
	// Batch-A A12: this used to hard-Close (cutting live requests), compare the
	// terminal error with != instead of errors.Is, and watch ctx in a goroutine
	// that outlived the call. All three are gone with the shared implementation.
	return httplisten.Serve(ctx, ln, Handler(body), "clustermanifest")
}

// Handler builds the GET-only well-known manifest mux (extracted so it is httptest-able).
func Handler(body func() ([]byte, bool)) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(ManifestPath, func(w http.ResponseWriter, r *http.Request) {
		// A panic in body() (e.g. a future bug) must not take the listener down.
		defer func() { _ = recover() }()
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		b, ok := body()
		if !ok {
			http.Error(w, "cluster manifest not available", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	})
	return mux
}
