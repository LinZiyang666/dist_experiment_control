package xferserve

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/nats-io/nkeys"
)

// The production source must follow a reloaded nats.conf (rotation adds or
// replaces the managed $SYS users), fail closed on a broadened or unreadable
// file without flooding the log per CONNECT, and stay nil for in-process
// brokers that have no config file at all.
// origin: hot-update S9 plan §3.3 / P12 / T27; S7 lead integration R8
func TestLoadedSystemPrincipalsFollowReloadAndFailClosed(t *testing.T) {
	if LoadedSystemPrincipals("", nil) != nil {
		t.Fatal("an in-process broker without a config file installed a SYS source")
	}
	key := func() string {
		kp, err := nkeys.CreateUser()
		if err != nil {
			t.Fatal(err)
		}
		pub, err := kp.PublicKey()
		if err != nil {
			t.Fatal(err)
		}
		return pub
	}
	block := func(read, step string) string {
		b, err := natsconf.SystemPrincipalBlock(natsconf.SystemPrincipals{ReadNkey: read, StepNkey: step})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "nats.conf")
	// Each rewrite gets a distinct mtime so the identity check observes it even
	// on coarse filesystem timestamps.
	stamp := time.Now().Add(-time.Hour)
	write := func(body string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		stamp = stamp.Add(time.Second)
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	var logs bytes.Buffer
	source := LoadedSystemPrincipals(path, slog.New(slog.NewTextHandler(&logs, nil)))
	if source == nil {
		t.Fatal("configured broker installed no SYS source")
	}
	if got := source(); len(got) != 0 {
		t.Fatalf("a missing config yielded SYS principals: %+v", got)
	}
	write("port: -1\n")
	if got := source(); len(got) != 0 {
		t.Fatalf("a config without the managed block yielded SYS principals: %+v", got)
	}
	read, step := key(), key()
	write("port: -1\n" + block(read, step))
	got := source()
	if len(got) != 2 || got[0].Nkey == got[1].Nkey || (got[0].Nkey != read && got[0].Nkey != step) || (got[1].Nkey != read && got[1].Nkey != step) {
		t.Fatalf("loaded principals do not name the managed users: %+v", got)
	}
	for _, p := range got {
		if len(p.Publish) == 0 || len(p.Subscribe) == 0 || !strings.HasPrefix(p.Subscribe[0], authcallout.SystemInboxRoot+"."+p.Nkey+".") {
			t.Fatalf("loaded principal lost its exact authority: %+v", p)
		}
	}
	// A caller mutating its copy cannot widen the cached authority, whether
	// the copy came from the load or from a later cache hit.
	got[0].Publish[0] = "$SYS.>"
	hit := source()
	hit[0].Publish[0] = "$SYS.>"
	for _, p := range source() {
		for _, s := range p.Publish {
			if s == "$SYS.>" {
				t.Fatal("caller mutation reached the cached authority")
			}
		}
	}
	// P03 rotation: the reloaded file replaces the identities.
	rotated := key()
	write("port: -1\n" + block(rotated, step))
	found := false
	for _, p := range source() {
		found = found || p.Nkey == rotated
		if p.Nkey == read {
			t.Fatal("rotated-out SYS identity remained admitted")
		}
	}
	if !found {
		t.Fatal("rotated SYS identity was not recognised after reload")
	}
	// A broadened block fails closed and logs once, not once per CONNECT.
	write("port: -1\n" + strings.Replace(block(read, step), "$SYS.REQ.SERVER.*.JSZ", "$SYS.>", 1))
	logs.Reset()
	for i := 0; i < 5; i++ {
		if got := source(); len(got) != 0 {
			t.Fatalf("broadened SYS authority was admitted: %+v", got)
		}
	}
	if n := strings.Count(logs.String(), "SYS admission fails closed"); n != 1 {
		t.Fatalf("broken config logged %d times across five CONNECTs, want once", n)
	}
}
