package clustermanifest

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestManifestGETOnly(t *testing.T) {
	srv := httptest.NewServer(Handler(func() ([]byte, bool) { return []byte(`{"ok":1}`), true }))
	defer srv.Close()

	resp, err := http.Post(srv.URL+ManifestPath, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST must be 405, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	resp2, err := http.Get(srv.URL + ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GET must be 200, got %d", resp2.StatusCode)
	}
	_ = resp2.Body.Close()

	resp3, err := http.Get(srv.URL + "/other")
	if err != nil {
		t.Fatal(err)
	}
	if resp3.StatusCode != http.StatusNotFound {
		t.Fatalf("an unknown path must be 404, got %d", resp3.StatusCode)
	}
	_ = resp3.Body.Close()
}

func TestManifest503WhenNoBody(t *testing.T) {
	srv := httptest.NewServer(Handler(func() ([]byte, bool) { return nil, false }))
	defer srv.Close()
	resp, err := http.Get(srv.URL + ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("a nil body must be 503, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestBindRejectsNonLoopback(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:0", "8.8.8.8:80"} {
		if _, err := Bind(addr); err == nil {
			t.Fatalf("non-loopback bind %q must be refused (unauthenticated + Caddy-fronted)", addr)
		}
	}
	ln, err := Bind("127.0.0.1:0")
	if err != nil {
		t.Fatalf("loopback bind must succeed: %v", err)
	}
	_ = ln.Close()
}
