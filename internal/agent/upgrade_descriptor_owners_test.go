//go:build linux

package agent

import (
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/logrotate"
	"github.com/LinZiyang666/tether/internal/pty"
)

// origin: hot-update S3 plan §5.2 / RS-4. A census can only protect owners
// that exist in the fixture. These stay open until the exec-hook audit ends.
func cutOpenDescriptorOwners(t *testing.T, a *Agent) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agent.log")
	writer, err := logrotate.Open(path, 4096, 1, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	if _, err := writer.Write([]byte("descriptor audit\n")); err != nil {
		t.Fatal(err)
	}
	// Open can deliberately degrade to stderr, which would make this audit
	// vacuous. Require the real backing file and its still-open descriptor.
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range entries {
		name, _ := os.Readlink("/proc/self/fd/" + entry.Name())
		found = found || name == path
	}
	if !found {
		t.Fatal("logrotate backing descriptor missing")
	}
	previous := a.cfg.Logger
	a.cfg.Logger = slog.New(slog.NewTextHandler(writer, nil))
	t.Cleanup(func() { a.cfg.Logger = previous })
	release, err := cluster.AcquireDataDirLock(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	server, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	// Allocation covers the PTY owner. Admitting a PTY workload would veto
	// the cut; that different invariant belongs to the existing busy tests.
	terminal, err := pty.Allocate(80, 24)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = terminal.Close() })
}
