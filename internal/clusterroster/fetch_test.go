package clusterroster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFetchManifestRejectsNonHTTPScheme(t *testing.T) {
	for _, bad := range []string{"file:///etc/passwd", "gopher://x/", "data:text/plain,hi"} {
		if _, err := FetchManifest(context.Background(), bad, 0); err == nil {
			t.Fatalf("non-http(s) url %q must be rejected", bad)
		}
	}
}

func TestFetchManifestCapsBodySize(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("a", 100)))
	}))
	defer srv.Close()
	if _, err := FetchManifest(context.Background(), srv.URL, 10); err == nil {
		t.Fatal("an oversized body must error (bounded read), not be accepted")
	}
}

func TestFetchManifestParsesValid(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"schema_version":1,"roster":null,"seeds":null}`))
	}))
	defer srv.Close()
	m, err := FetchManifest(context.Background(), srv.URL, 0)
	if err != nil {
		t.Fatal(err)
	}
	if m.SchemaVersion != 1 {
		t.Fatalf("parse failed: %+v", m)
	}
}
