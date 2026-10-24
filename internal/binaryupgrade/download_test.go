package binaryupgrade

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// download_test.go checks classification at the real bounded HTTP fetch.
// origin: hot-update S4 plan §10.

// TestFetchURLWrapsStatusAndSizeSentinels drives the real fetchURL against a real HTTP server, because
// the defect being guarded is in the error WRAPPING and a hand-built error would prove nothing about it.
func TestFetchURLWrapsStatusAndSizeSentinels(t *testing.T) {
	oversize := strings.Repeat("x", int(MaxTarballBytes)+1)

	tests := []struct {
		name    string
		status  int
		body    string
		wantErr error  // the sentinel errors.Is must match, or nil for success
		notErr  error  // a sentinel that must NOT match (keeps the two from being interchangeable)
		wantMsg string // substring the operator-visible message must carry
	}{
		{
			name:    "404 is a PERMANENT status failure, never a size failure",
			status:  http.StatusNotFound,
			body:    "no such artifact",
			wantErr: ErrUpgradeHTTPStatus,
			notErr:  ErrUpgradeHTTPRetryable,
			wantMsg: "404",
		},
		// origin: line-2 INDEPENDENT EXTERNAL REVIEW M1. This row used to assert the opposite — "500 is
		// also a status failure — the split is 2xx vs not, not 4xx vs 5xx" — and it was WRONG in the way
		// that mattered: it pinned the very collapse Y2 existed to undo. A 500 is a mirror-side fault that
		// routinely clears; classifying it with a typo'd URL aborted the whole fleet and told automation
		// never to retry. The test agreed with the code, so nothing in the tree disagreed with either.
		//
		// Kept as a row rather than deleted, inverted, so the reversal is visible to the next reader.
		{
			name:    "500 is RETRYABLE — a mirror-side fault, not a wrong argument",
			status:  http.StatusInternalServerError,
			body:    "mirror on fire",
			wantErr: ErrUpgradeHTTPRetryable,
			notErr:  ErrUpgradeHTTPStatus,
			wantMsg: "500",
		},
		{
			name:    "503 is RETRYABLE — the canonical drain/maintenance status",
			status:  http.StatusServiceUnavailable,
			body:    "draining",
			wantErr: ErrUpgradeHTTPRetryable,
			notErr:  ErrUpgradeHTTPStatus,
			wantMsg: "503",
		},
		{
			// 501 is the boundary the named set exists for: a `5xx` range test would call it retryable.
			name:    "501 is PERMANENT — a mirror that will never serve this, despite being 5xx",
			status:  http.StatusNotImplemented,
			body:    "nope",
			wantErr: ErrUpgradeHTTPStatus,
			notErr:  ErrUpgradeHTTPRetryable,
			wantMsg: "501",
		},
		{
			name:    "a 200 body one byte over the ceiling is a size failure",
			status:  http.StatusOK,
			body:    oversize,
			wantErr: ErrUpgradeTooLarge,
			notErr:  ErrUpgradeHTTPStatus,
			wantMsg: "bytes",
		},
		{
			name:   "a 200 body at the ceiling is NOT a failure (the +1 trick must not be off by one)",
			status: http.StatusOK,
			body:   strings.Repeat("y", int(MaxTarballBytes)),
		},
		{
			name:   "an ordinary small 200 succeeds",
			status: http.StatusOK,
			body:   "tarball",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			body, err := FetchURL(srv.URL, 30*time.Second)

			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("want success, got %v", err)
				}
				if len(body) != len(tc.body) {
					t.Fatalf("body length %d, want %d", len(body), len(tc.body))
				}
				return
			}
			if err == nil {
				t.Fatalf("want an error wrapping %v, got nil", tc.wantErr)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("errors.Is(err, %v) is false — err = %v\n\n"+
					"This is the mapping the Y2 split exists to provide. Without the sentinel the caller "+
					"falls through to download_failed, which cmd/tether classifies exitTransient, which "+
					"tells automation to retry a permanently-failing URL forever. Check that the wrap uses "+
					"%%w and not %%v.", tc.wantErr, err)
			}
			if tc.notErr != nil && errors.Is(err, tc.notErr) {
				t.Errorf("errors.Is(err, %v) is ALSO true — the two sentinels are not distinguishable, so "+
					"the switch in handleUpgradeForwarded would pick by clause order rather than by cause",
					tc.notErr)
			}
			if tc.wantMsg != "" && !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("message %q does not contain %q — the operator sees this string and it has to say "+
					"WHICH status or WHICH limit", err.Error(), tc.wantMsg)
			}
		})
	}
}
