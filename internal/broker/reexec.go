package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// reloadReplyDrain is how long the reload handler waits AFTER returning its OK reply before arming the
// re-exec, so the ack reaches the caller over the admin socket before the restart drops the channel.
const reloadReplyDrain = 300 * time.Millisecond

// reexec.go — G5 #13: the privilege-free broker-daemon RELOAD primitive. Once the target binary is on
// disk (staging is the §0.1 PRIVILEGED precondition — /usr/local/bin/tether is root-owned, User=tether
// cannot write it), the broker loads it by unwinding Run through its EXISTING ordered shutdown and then
// letting serve.go syscall.Exec the on-disk binary IN PLACE (same PID). systemd Type=simple sees no exit,
// so this is structurally immune to the #23 clean-exit-not-restarted trap; on an exec failure serve.go
// exits non-zero and Restart=always (G1) revives with the new binary. No sudo, no systemctl — execute
// permission on the binary is all User=tether needs.

// SetReloadTrigger stores the run-context cancel (serve.go injects it before Run) so RequestReExec can
// unwind Run through its ordered teardown instead of a bespoke shutdown path.
func (b *Broker) SetReloadTrigger(cancel context.CancelFunc) { b.reloadTrigger = cancel }

// RequestReExec marks the broker for a post-shutdown self-re-exec and unwinds Run. Idempotent and
// safe to call from an admin-handler goroutine. The CALLER (the reload admin-op) is responsible for the
// gates — leader-refusal, on-disk sha verification, reply-drain — before invoking this; RequestReExec
// itself only arms the trampoline and triggers the graceful stop.
func (b *Broker) RequestReExec() {
	b.reExecRequested.Store(true)
	if t := b.reloadTrigger; t != nil {
		t()
	}
}

// ReExecRequested reports whether a re-exec was armed (serve.go checks it after Run returns).
func (b *Broker) ReExecRequested() bool { return b.reExecRequested.Load() }

// handleBrokerUpgradeReload (G5 #13, OpBrokerUpgradeReload) is the local-socket reload op. Gate order:
// (1) idempotency skip if already running ToVersion; (2) REFUSE if this broker is the raft leader (the
// orchestrator transfers leadership off first); (3) verify the on-disk binary sha matches the staged
// target so a stale/unstaged image is never re-exec'd; (4) reply OK, then arm the re-exec after a short
// drain. Dispatched BEFORE the leader gate (a to-be-restarted broker must NOT be the leader).
func (b *clusterAdminBackend) handleBrokerUpgradeReload(req adminsock.Request) adminsock.Response {
	op := req.Op
	// proto.SameRelease: the two ends disagree on the leading "v" (goreleaser stamps it, a source build does not), and this is a CONFIRMATION check — a false negative reports a successful upgrade as unconfirmed (prerelease audit DRD-F2/F3).
	if req.ToVersion != "" && proto.SameRelease(proto.ReleaseVersion, req.ToVersion) {
		return adminsock.Response{Op: op, OK: true, AlreadyAtVersion: true}
	}
	if b.admin != nil && b.admin.node != nil && b.admin.node.IsLeader() {
		return adminsock.Response{Op: op, Code: adminsock.CodeIsLeader,
			Error: "this broker is the raft leader — transfer leadership off it before reloading (the orchestrator does this)"}
	}
	// Stage-C M4: refuse a reload while a cluster operation (grow/retire/force-single) is in flight — a
	// restart mid-membership-change could race the reconfiguration. Each host self-refuses (belt-and-
	// suspenders with the orchestrator's own pre-roll check).
	if b.admin != nil && b.admin.node != nil {
		if ops, oerr := cluster.NonTerminalOperations(b.admin.node.RODB()); oerr == nil && len(ops) > 0 {
			return adminsock.Response{Op: op, Code: adminsock.CodeBadRequest,
				Error: "a cluster operation is in flight — retry the reload after it completes"}
		}
	}
	// External-review doubt: the sha guard is MANDATORY, not optional — an unguarded reload would re-exec
	// whatever happens to be on disk (a stale/half-staged image). Every legitimate caller knows the staged
	// digest (the CLI requires --expect-sha256; the orchestrator threads it through), so fail-closed on an
	// empty digest rather than silently skipping the check.
	if req.ExpectSHA256 == "" {
		return adminsock.Response{Op: op, Code: adminsock.CodeBadRequest,
			Error: "reload requires expect_sha256 (the staged-binary digest guard) — refusing an unguarded reload"}
	}
	got, err := onDiskBinarySHA256()
	if err != nil {
		return adminsock.Response{Op: op, Code: adminsock.CodeStoreError, Error: "cannot hash the on-disk binary: " + err.Error()}
	}
	if !strings.EqualFold(got, req.ExpectSHA256) {
		return adminsock.Response{Op: op, Code: adminsock.CodeShaMismatch,
			Error: "on-disk binary sha256 does not match the staged target (staging did not land — the privileged precondition); refusing to reload"}
	}
	if b.requestReExec == nil {
		return adminsock.Response{Op: op, Code: adminsock.CodeClusterNotEnabled, Error: "broker reload not available (single mode / not wired)"}
	}
	// Reply first, then arm: the ack must reach the caller before the restart drops the channel.
	go func() {
		time.Sleep(reloadReplyDrain)
		b.requestReExec()
	}()
	return adminsock.Response{Op: op, OK: true, Reloading: true}
}

// onDiskBinarySHA256 hashes the running executable's on-disk image (the freshly-staged target binary).
func onDiskBinarySHA256() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	exe = strings.TrimSuffix(exe, " (deleted)")
	f, err := os.Open(exe)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
