// Package httplisten owns binding and serving for the broker's small internal
// HTTP surfaces (/sub, /metrics, the cluster manifest).
//
// Batch-A A12. Three packages had grown their own Bind + ServeListener pair.
// Two of them (subhttp, brokermetrics) were byte-identical; the third
// (clustermanifest) had DRIFTED, and nobody decided that it should:
//
//   - it closed with srv.Close() — cutting live requests off mid-response —
//     where the others used Shutdown with a 3s drain;
//   - it compared the terminal error with `!=` instead of errors.Is;
//   - it watched ctx in a bare `go func(){ <-ctx.Done(); … }()`, which outlives
//     the function whenever Serve returns first.
//
// internal/broker/broker.go:742 had already named this exact failure mode about
// a different trio: "three hand-copied copies, which is how the late one gets
// fixed and the early one rots". This is that diagnosis recurring verbatim one
// package over.
//
// The loopback decision stays with the CALLER because the three surfaces differ
// on purpose: /sub and the manifest are unauthenticated and Caddy-fronted, so
// they must never leave loopback; /metrics is expected on a private interface
// for a scraper. Making it an explicit argument means the difference is stated
// at each call site rather than encoded in which package you happened to call.
package httplisten

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// shutdownGrace bounds the drain of in-flight requests on context cancel.
const shutdownGrace = 3 * time.Second

// CheckLoopback is the PURE, side-effect-free half of Bind's loopback gate
// (g75-g78 external review F1): a config preflight (broker.ValidateConfig →
// `serve --config-check`) must reach the SAME verdict a real Bind reaches, so
// both call this — never a second copy that could drift. Returns nil for a
// loopback host, an error otherwise. Empty host (":8090" = all interfaces) is
// NOT loopback.
func CheckLoopback(name, addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s: bad listen addr %q: %w", name, addr, err)
	}
	if !isLoopbackHost(host) {
		return fmt.Errorf("%s: listen addr %q must be loopback (this surface is unauthenticated)", name, addr)
	}
	return nil
}

// Bind listens on addr. When requireLoopback is set, a non-loopback host is
// refused BEFORE the socket is opened — these surfaces are unauthenticated, so
// failing closed is the whole point.
func Bind(name, addr string, requireLoopback bool) (net.Listener, error) {
	if requireLoopback {
		if err := CheckLoopback(name, addr); err != nil {
			return nil, err
		}
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("%s: listen %q: %w", name, addr, err)
	}
	return ln, nil
}

// isLoopbackHost reports whether host names the loopback interface.
//
// An EMPTY host is NOT loopback. `:8080` binds every interface, which is the
// most natural thing an operator writes and the most dangerous thing these
// surfaces can do — /sub vends subscriber tokens and PSKs, the cluster manifest
// vends an account-signed roster, and both are plaintext HTTP fronted by Caddy.
//
// This is called out because consolidating the three copies got it wrong once:
// clustermanifest's original spelled the empty case as
//
//	if host == "localhost" || host == "" { return host == "localhost" }
//
// — a shape that exists precisely to answer false for "", and which reads like a
// redundant expression until you ask why it was written that way. Collapsing it
// to `return true` turned a documented startup refusal into a silent bind on
// 0.0.0.0. Behavioural tests below (not just the AST policy check) now cover it.
func isLoopbackHost(host string) bool {
	if host == "" {
		return false // ":8080" == all interfaces
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Serve runs handler on ln until ctx is cancelled, then drains in-flight
// requests for up to shutdownGrace.
//
// context.AfterFunc is used rather than a watcher goroutine: AfterFunc's
// registration is released when ctx is done OR when the returned stop func
// runs, so nothing outlives Serve. The hand-rolled `go func(){ <-ctx.Done() }()`
// this replaces stayed parked until cancellation even after Serve had returned.
func Serve(ctx context.Context, ln net.Listener, handler http.Handler, name string) error {
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	stop := context.AfterFunc(ctx, func() {
		shCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		_ = srv.Shutdown(shCtx)
	})
	defer stop()
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s: serve: %w", name, err)
	}
	return nil
}
