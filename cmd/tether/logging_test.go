// logging_test.go — resolveLogSink's #75 visibility breadcrumb. When slog goes
// to the capped file, journal (where the deployed unit routes stderr) shows
// NOTHING from the broker — the live incident read exactly like that
// ambiguity: "is the file sink working, or did my observability block never
// take effect?". One stderr line at boot answers it from journalctl alone.
// origin: docs/deploy-tier-gotchas.md #75
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/logrotate"
)

// captureStderr runs fn with os.Stderr swapped to a pipe and returns what fn
// wrote to it.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = orig }()
	fn()
	_ = w.Close()
	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	_ = r.Close()
	return string(buf[:n])
}

func TestResolveLogSinkBreadcrumb(t *testing.T) {
	t.Run("file sink prints the breadcrumb to stderr", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "broker.log")
		var sink any
		out := captureStderr(t, func() { sink = resolveLogSink(path, 50, 2) })
		if _, isFile := sink.(*logrotate.Writer); !isFile {
			t.Fatalf("usable file path must yield the capped writer; stderr said %q", out)
		}
		if !strings.Contains(out, "tether: log sink "+path) ||
			!strings.Contains(out, "cap 50MB x 2 backups") {
			t.Fatalf("file sink must announce itself on stderr (the journal-visible channel); got %q", out)
		}
	})

	t.Run("stderr sink stays silent", func(t *testing.T) {
		for _, p := range []string{"", "-"} {
			out := captureStderr(t, func() { _ = resolveLogSink(p, 0, 0) })
			if out != "" {
				// The zero-config / dev path must stay byte-identical (h1
				// byte-equivalence anchor): no new boot chatter.
				t.Fatalf("stderr sink (path=%q) must print nothing, got %q", p, out)
			}
		}
	})

	t.Run("unusable file announces DEGRADED before the breadcrumb", func(t *testing.T) {
		// A directory path cannot be opened as a log file. logrotate.Open
		// never fails — it returns a birth-degraded Writer that spills to
		// stderr and retries — so the sink is STILL a *logrotate.Writer,
		// and the truth about delivery is the Writer's own DEGRADED line,
		// which must precede the neutral breadcrumb so journal readers see
		// the correction first.
		dir := t.TempDir()
		var sink any
		out := captureStderr(t, func() { sink = resolveLogSink(dir, 50, 2) })
		if _, isFile := sink.(*logrotate.Writer); !isFile {
			t.Fatal("even an unusable path yields the self-healing capped writer")
		}
		degraded := strings.Index(out, "DEGRADED (spilling to stderr)")
		crumb := strings.Index(out, "tether: log sink ")
		if degraded < 0 || crumb < 0 || degraded > crumb {
			t.Fatalf("stderr must carry the DEGRADED notice before the breadcrumb; got %q", out)
		}
	})
}
