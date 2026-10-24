// download.go owns bounded artifact fetching and permanent/transient mirror classification.
// origin: hot-update S4 plan §10.
package binaryupgrade

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// urlAllowed mirrors broker.URLAllowed; duplicated locally so the
// agent package doesn't import broker (would be a layering
// violation: agent is a leaf in production deployments). Empty
// allow → reject.
func URLAllowed(url string, allow []string) bool {
	if len(allow) == 0 {
		return false
	}
	for _, p := range allow {
		if p == "" {
			continue
		}
		if strings.HasPrefix(url, p) {
			return true
		}
	}
	return false
}

// fetchURL downloads body bytes via HTTP GET with a hard timeout
// AND a hard size cap. The cap defends against a malicious URL
// (broker compromise, MITM on a non-https mirror) that would
// otherwise stream gigabytes into the agent's memory; with `--all`
// fan-out, that could OOM every agent in a session at once.
// Returns an error for any non-2xx, transport failure, or oversize
// response.
// origin: line-2 §12 Y2. fetchURL's four failure modes used to arrive at the caller as one
// indistinguishable error, which the caller reported as the single code `download_failed`. Two of the
// four can never succeed on a retry — a 404 mirror URL and a tarball over the size ceiling — and
// docs/usage.md §9.13 tells automation to retry the class they landed in. These sentinels let the
// caller say which one happened, so the ones that are physically hopeless stop being retried forever.
// THE STATUS SPACE SPLITS IN TWO, NOT ONE. origin: line-2 INDEPENDENT EXTERNAL REVIEW M1.
//
// The first version of this split wrapped EVERY non-2xx in ErrUpgradeHTTPStatus, mapped that to
// download_http_status / exit 64, and taught `node upgrade --all` to abort the whole fleet on it. That is
// correct for 404 (a typo'd --url is wrong on every node) and wrong for 408, 421, 425, 429, 500, 502,
// 503 and 504, all of which can clear with no operator change at all — a different connection, an
// explicit retry request, a mirror restart, a rate limit, or a load balancer draining.
//
// So the increment that existed to STOP collapsing distinct retry semantics into one code did exactly
// that, one layer down, and its own comment said "non-2xx ... Not retryable" as if the status space had
// one member. A fleet rollout hitting a 503 on node 1 aborted the remaining N-1 nodes and told automation
// never to retry.
//
// The boundary is a NAMED SET, not `status/100`: retryability is a policy statement about specific codes,
// and `5xx` alone would sweep in 501 Not Implemented (a mirror that will never serve this) while missing
// 408, 421, 425 and 429 entirely.
var (
	// ErrUpgradeHTTPStatus is a PERMANENT non-2xx: the URL or the mirror is wrong and will be wrong on
	// every node. 404, 403, 401, 410, 501… Not retryable.
	ErrUpgradeHTTPStatus = errors.New("upgrade: mirror returned a permanent non-2xx status")
	// ErrUpgradeHTTPRetryable is a TRANSIENT non-2xx — see upgradeRetryableStatuses. The artifact and the
	// URL are fine; the mirror is not available right now.
	ErrUpgradeHTTPRetryable = errors.New("upgrade: mirror is temporarily unavailable")
	// ErrUpgradeTooLarge means the body exceeded upgradeMaxTarballBytes. Not retryable — the same URL
	// will be the same size next time.
	ErrUpgradeTooLarge = errors.New("upgrade: tarball exceeds the size ceiling")
)

// upgradeRetryableStatuses are the HTTP statuses that can succeed later without anyone changing the
// request. Enumerated rather than expressed as a range so that adding one is a decision someone makes,
// and so 501 (permanent) does not ride in on "5xx".
var upgradeRetryableStatuses = map[int]bool{
	http.StatusRequestTimeout:      true, // 408 — the mirror gave up waiting; the next attempt may not
	http.StatusMisdirectedRequest:  true, // 421 — RFC 9110 permits retry over a different connection
	http.StatusTooEarly:            true, // 425 — RFC 8470 defines this specifically to trigger a retry
	http.StatusTooManyRequests:     true, // 429 — rate limited, explicitly a "later" signal
	http.StatusInternalServerError: true, // 500 — mirror-side fault, routinely transient
	http.StatusBadGateway:          true, // 502 — upstream restarting behind a proxy
	http.StatusServiceUnavailable:  true, // 503 — draining / maintenance; the canonical retry status
	http.StatusGatewayTimeout:      true, // 504 — upstream slow, not absent
}

func FetchURL(url string, timeout time.Duration) ([]byte, error) {
	client := &http.Client{Timeout: timeout}
	//nolint:noctx // Reached from a nats.go MsgHandler (func(*nats.Msg)) -- there is no ctx to thread,
	// and the download is bounded by client.Timeout above. Inline rather than a config exclusion
	// because a path+text rule would exempt every future Client.Get in this file too (measured).
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("http get: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		if upgradeRetryableStatuses[resp.StatusCode] {
			// Retry-After, when the mirror sends one, is the only part of this the operator cannot
			// re-derive. Surfaced in the message rather than parsed into a policy: this agent does not
			// schedule the retry, the caller does.
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				return nil, fmt.Errorf("%w: %s (Retry-After: %s)", ErrUpgradeHTTPRetryable, resp.Status, ra)
			}
			return nil, fmt.Errorf("%w: %s", ErrUpgradeHTTPRetryable, resp.Status)
		}
		return nil, fmt.Errorf("%w: %s", ErrUpgradeHTTPStatus, resp.Status)
	}
	// LimitReader + 1 trick: read maxN+1 bytes; if the read
	// returned exactly maxN+1 bytes the actual stream is larger
	// than maxN. Anything <= maxN is fine.
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxTarballBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if int64(len(body)) > MaxTarballBytes {
		return nil, fmt.Errorf("%w: > %d bytes", ErrUpgradeTooLarge, MaxTarballBytes)
	}
	return body, nil
}
