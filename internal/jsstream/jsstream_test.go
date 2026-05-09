package jsstream

import (
	"context"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// startJSNATS is a thin alias over testharness.StartJSNATS so existing
// call sites in this file (and tests inside this package historically)
// don't have to change. The shared implementation handles the
// JetStream-ready wait that prevents flakes under load.
func startJSNATS(t *testing.T) string { return testharness.StartJSNATS(t) }

func newJS(t *testing.T, url string) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

func TestEnsureEventsStreamIsIdempotent(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	ctx := context.Background()

	if err := EnsureEventsStream(ctx, js); err != nil {
		t.Fatal(err)
	}
	// Second call must succeed (already-exists path).
	if err := EnsureEventsStream(ctx, js); err != nil {
		t.Fatalf("re-create: %v", err)
	}
}

func TestEnsureHistoryStreamCreatesPerSession(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	ctx := context.Background()

	if err := EnsureHistoryStream(ctx, js, "lab"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureHistoryStream(ctx, js, "lab"); err != nil {
		t.Fatalf("re-create same sid: %v", err)
	}
	if err := EnsureHistoryStream(ctx, js, "prod"); err != nil {
		t.Fatal(err)
	}

	sids, err := ListHistorySIDs(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"lab": true, "prod": true}
	for _, s := range sids {
		delete(want, s)
	}
	if len(want) != 0 {
		t.Errorf("missing history streams: %v", want)
	}
}

func TestDeleteHistoryStreamIdempotent(t *testing.T) {
	js := newJS(t, startJSNATS(t))
	ctx := context.Background()

	if err := EnsureHistoryStream(ctx, js, "lab"); err != nil {
		t.Fatal(err)
	}
	if err := DeleteHistoryStream(ctx, js, "lab"); err != nil {
		t.Fatal(err)
	}
	// Re-deleting a missing stream must not error — H.3 phase ②
	// must be safe to retry after a crash.
	if err := DeleteHistoryStream(ctx, js, "lab"); err != nil {
		t.Errorf("re-delete: %v", err)
	}
}

func TestSIDFromHistoryStream(t *testing.T) {
	cases := []struct {
		stream  string
		wantSID string
		wantOK  bool
	}{
		{"history-lab", "lab", true},
		{"history-lab-1", "lab-1", true},
		{"events", "", false},
		{"random", "", false},
	}
	for _, c := range cases {
		gotSID, gotOK := SIDFromHistoryStream(c.stream)
		if gotSID != c.wantSID || gotOK != c.wantOK {
			t.Errorf("SIDFromHistoryStream(%q) = (%q, %v); want (%q, %v)",
				c.stream, gotSID, gotOK, c.wantSID, c.wantOK)
		}
	}
}
