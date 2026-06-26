package clusterroster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// fetch.go (C2) — the ONE hardened HTTP fetcher for the well-known cluster manifest. It only PARSES
// (it does NOT verify — signature/account/expiry/generation verification stays the consumer's single
// authority, AdoptDecision). Hardened against an attacker-controlled bootstrap URL: scheme allowlist
// (http/https only — no file/gopher/data), bounded client timeout, body size cap (io.LimitReader),
// and capped redirects that must stay http/https. Proxy-aware via http.ProxyFromEnvironment (the
// HTTP analogue of the v0.3.6 proxy-aware NATS dial).

const (
	defaultFetchTimeout  = 15 * time.Second
	defaultManifestLimit = 1 << 20 // 1 MiB
	maxFetchRedirects    = 5
)

// FetchManifest GETs + JSON-parses a ClusterManifest from an http(s) URL. limit<=0 ⇒ 1 MiB cap.
func FetchManifest(ctx context.Context, rawURL string, limit int64) (*proto.ClusterManifest, error) {
	if limit <= 0 {
		limit = defaultManifestLimit
	}
	if err := requireHTTPScheme(rawURL); err != nil {
		return nil, err
	}
	client := &http.Client{
		Timeout:   defaultFetchTimeout,
		Transport: &http.Transport{Proxy: http.ProxyFromEnvironment},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxFetchRedirects {
				return fmt.Errorf("clusterroster: too many redirects (>%d)", maxFetchRedirects)
			}
			return requireHTTPScheme(req.URL.String())
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("clusterroster: build manifest request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("clusterroster: fetch manifest: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clusterroster: manifest http status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("clusterroster: read manifest: %w", err)
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("clusterroster: manifest exceeds %d-byte cap", limit)
	}
	var m proto.ClusterManifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("clusterroster: parse manifest: %w", err)
	}
	return &m, nil
}

func requireHTTPScheme(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("clusterroster: bad manifest url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("clusterroster: refusing non-http(s) manifest url scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return fmt.Errorf("clusterroster: manifest url missing host")
	}
	return nil
}
