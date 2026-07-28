package agent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// g5_reexec_test.go — G5 #13 W3: the co-located-agent re-exec-only leg. sha256OfFile is the on-disk
// binary verification the ReExecOnly path uses to refuse re-execing a stale/unstaged image. The full
// forwarded-handler path (reply + real syscall.Exec) is exercised by the N=3 sim drill.
func TestSha256OfFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "bin")
	if err := os.WriteFile(p, []byte("abc"), 0o755); err != nil {
		t.Fatal(err)
	}
	// sha256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
	got, err := sha256OfFile(p)
	if err != nil {
		t.Fatalf("sha256OfFile: %v", err)
	}
	const want = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got != want {
		t.Fatalf("sha256OfFile = %s, want %s", got, want)
	}
}

func TestSha256OfFileMissing(t *testing.T) {
	if _, err := sha256OfFile(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("hashing a missing file must error (so ReExecOnly refuses rather than re-execs blind)")
	}
}

// reExecOnlyReply drives handleReExecOnly over a real conn and returns the forwarded reply. UpgradeNoExit
// keeps the go-test binary from being syscall.Exec'd; UpgradeExecutablePath points at a hermetic temp file.
func reExecOnlyReply(t *testing.T, req proto.UpgradeForwardedReq, exePath string) proto.UpgradeForwardedResp {
	t.Helper()
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	a := &Agent{}
	a.cfg.UpgradeNoExit = true
	a.cfg.UpgradeExecutablePath = exePath
	sub, err := nc.Subscribe("test.reexec", func(msg *nats.Msg) { a.handleReExecOnly(msg, req) })
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	reply, err := nc.Request("test.reexec", []byte("{}"), 2*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	var resp proto.UpgradeForwardedResp
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	return resp
}

// TestReExecOnlyRefusesEmptySHA pins the External-review fail-closed symmetry fix: an unguarded re-exec (no
// sha256) must be refused, never launch whatever is on disk.
func TestReExecOnlyRefusesEmptySHA(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bin")
	if err := os.WriteFile(p, []byte("staged"), 0o755); err != nil {
		t.Fatal(err)
	}
	resp := reExecOnlyReply(t, proto.UpgradeForwardedReq{ReExecOnly: true, SHA256: ""}, p)
	if resp.OK || resp.Code != "sha256_required" {
		t.Fatalf("an unguarded re-exec (empty sha) must be refused sha256_required: %+v", resp)
	}
}

// TestReExecOnlyRefusesShaMismatch: a wrong staged digest must refuse (staging did not land the target).
func TestReExecOnlyRefusesShaMismatch(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bin")
	if err := os.WriteFile(p, []byte("staged"), 0o755); err != nil {
		t.Fatal(err)
	}
	resp := reExecOnlyReply(t, proto.UpgradeForwardedReq{ReExecOnly: true,
		SHA256: "0000000000000000000000000000000000000000000000000000000000000000"}, p)
	if resp.OK || resp.Code != "sha256_mismatch" {
		t.Fatalf("a stale/unstaged on-disk binary must refuse sha256_mismatch: %+v", resp)
	}
}
