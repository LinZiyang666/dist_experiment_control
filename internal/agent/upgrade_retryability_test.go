package agent

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// origin: line-2 independent external review. The status split must distinguish a permanently
// wrong artifact URL from a temporarily unavailable mirror. ErrUpgradeHTTPStatus is mapped to
// download_http_status / exitUsage, which stops `node upgrade --all` and tells automation not to retry.
func TestUpgradeTransientHTTPStatusIsNotPermanent(t *testing.T) {
	for _, status := range []int{
		http.StatusRequestTimeout,
		http.StatusMisdirectedRequest,
		http.StatusTooEarly,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
			defer srv.Close()

			_, err := fetchURL(srv.URL, time.Second)
			if err == nil {
				t.Fatalf("HTTP %d unexpectedly succeeded", status)
			}
			if errors.Is(err, ErrUpgradeHTTPStatus) {
				t.Fatalf("HTTP %d wraps ErrUpgradeHTTPStatus, which the caller classifies as a permanent "+
					"usage/configuration error. This status can clear without any operator change and "+
					"must remain retryable (or be classified by a separate transient-status sentinel).",
					status)
			}
		})
	}
}
