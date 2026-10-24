package cli

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats-server/v2/server"
)

// The N-1 inbox refusal path through the REAL composite (proxydial bounded
// dialer + natsinbox refusal probe + fallback dial + FinishWithin). The P20
// deep-dial/WSS subtests still run against the contractstub adapter, so this
// is the only place the fallback's second dial meets the real admission
// budget and the sticky-deadline detach.
// origin: hot-update S6 internal review rv-ctl
func TestConnectWithinRefusalFallbackSharesRealBudget(t *testing.T) {
	id, err := EnsureIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// A pre-cutover server grant: only the shared _INBOX space.
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		Nkeys: []*server.NkeyUser{{Nkey: id.PublicKey, Permissions: &server.Permissions{
			Subscribe: &server.SubjectPermission{Allow: []string{"_INBOX.>"}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(upgradebudget.RequestLatency) {
		t.Fatal("NATS fixture not ready")
	}
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	t.Setenv(DevNoAuthEnv, "")
	if err := os.Unsetenv(DevNoAuthEnv); err != nil {
		t.Fatal(err)
	}
	sink, path := retrySink(t)
	observer := BindControlObserver(sink, "fallback-connect")
	deadline := retryDeadline(t, 800*time.Millisecond)
	nc, err := ConnectWithin(s.ClientURL(), id, deadline, observer)
	if err != nil {
		t.Fatalf("legacy-inbox broker refused admission: %v", err)
	}
	defer nc.Close()
	if nc.Opts.InboxPrefix != "" {
		t.Fatalf("fallback kept the refused private prefix %q", nc.Opts.InboxPrefix)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"inbox_probe", "inbox_fallback"} {
		if !strings.Contains(string(body), `"phase":"`+phase+`"`) {
			t.Fatalf("fallback lost receipt %s: %s", phase, body)
		}
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err == nil {
		<-time.After(remaining + 50*time.Millisecond)
	}
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatalf("fallback connection kept the admission deadline: %v", err)
	}
}
