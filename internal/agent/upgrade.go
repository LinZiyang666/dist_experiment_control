// Agent side of architecture J.4 (b) — `tether node upgrade`.
//
// Pre-conditions enforced by the broker (see broker/upgrade.go):
// owner-only, URL allowlist, sha256 sanity, proto match. This file
// re-checks the URL allowlist on the agent side as defense in
// depth (J.4 § 安全约束: "agent 收到 url 后本地再验一次白名单"),
// then downloads, verifies, and atomically replaces the running
// binary. Restart is the supervisor's job (systemd unit Restart=
// on-failure / setsid wrapper); architecture J.4 step 5 says the
// agent disconnects NATS so the new process's G.1 reconcile picks
// up the slack.
package agent

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// upgradeMaxTarballBytes caps the upgrade download size. v1 tether
// release tarballs are ~10MiB; 64MiB gives 6× headroom while
// preventing a malicious or misconfigured URL from OOM-ing the
// agent (audit Sec F1: io.ReadAll has no size limit, `--all`
// could OOM the entire fleet at once).
const upgradeMaxTarballBytes = 64 * 1024 * 1024

// defaultAgentURLAllowlist is the agent-side defense-in-depth
// allowlist used when Config.UpgradeURLAllowlist is empty; the broker
// ALWAYS double-gates the same allowlist, so this is belt-and-
// suspenders for an attacker who somehow reached the forwarded
// subject directly.
var defaultAgentURLAllowlist = []string{
	"https://github.com/LinZiyang666/dist_experiment_control/releases/",
}

// upgradeFetchTimeout bounds the HTTP GET round-trip. v1 ships
// ~10MB binaries; 30s covers reasonable broadband + connection setup.
const upgradeFetchTimeout = 30 * time.Second

func (a *Agent) handleUpgradeForwarded(nc *nats.Conn, msg *nats.Msg) {
	if msg.Reply == "" {
		a.cfg.Logger.Warn("agent: upgrade.req.forwarded without Reply")
		return
	}
	var req proto.UpgradeForwardedReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{
			Code: "json_parse", Error: err.Error(),
		})
		return
	}

	// G5 #13: a co-located broker-host agent RE-EXECS the already-staged shared binary — it does NOT
	// download/install (it cannot write the root-owned bin dir; staging is the privileged precondition).
	if req.ReExecOnly {
		a.handleReExecOnly(msg, req)
		return
	}

	// origin: upgrade-safety internal review S2. One install at a time: the
	// forwarded-message handler runs one goroutine per message, and the
	// pending-marker entry gate below only sees markers that have already
	// been WRITTEN — a second install arriving while the first is still
	// downloading/smoking would pass the gate and clobber the prev slot.
	if !a.upgradeInstallMu.TryLock() {
		a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{
			Code:  "upgrade_in_progress",
			Error: "another upgrade install is running on this agent right now; retry after it finishes",
		})
		return
	}
	defer a.upgradeInstallMu.Unlock()
	// External review F1: the in-process TryLock above cannot exclude a
	// SIBLING AGENT PROCESS sharing this binary path — the host-wide flock
	// can. Non-blocking for the same reason as the TryLock: a loser replies
	// upgrade_in_progress instead of queueing behind a ~35s download.
	exePathForLock, lockErr := a.upgradeExePath()
	if lockErr != nil {
		a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{Code: "self_path", Error: lockErr.Error()})
		return
	}
	installErr := withUpgradeFileLock(exePathForLock, false, func() error {
		a.handleUpgradeInstallLocked(msg, req, exePathForLock)
		return nil
	})
	if errors.Is(installErr, errUpgradeLockBusy) {
		a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{
			Code:  "upgrade_in_progress",
			Error: "another agent process on this host is installing an upgrade of the shared binary; retry after it finishes",
		})
	} else if installErr != nil {
		a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{Code: "install_failed", Error: installErr.Error()})
	}
}

// handleUpgradeInstallLocked is the install pipeline body, entered holding
// BOTH the in-process install mutex and the host-wide upgrade flock.
func (a *Agent) handleUpgradeInstallLocked(msg *nats.Msg, req proto.UpgradeForwardedReq, exePath string) {
	// upgrade-safety plan §3.1 install entry gate: a pending marker means a
	// prior upgrade staged its binary but has not committed or rolled back —
	// installing over it would clobber the only known-good binary (the prev
	// slot). Bounded: pending resolves at the register deadline, and a STALE
	// pending marker (deadline passed with no process alive to resolve it —
	// e.g. a failed install flip) stops blocking here so the operator can
	// simply retry. A corrupt marker is idle (loud, never a block).
	if m, merr := readUpgradeMarker(upgradeMarkerPath(exePath)); merr != nil {
		a.cfg.Logger.Warn("agent: upgrade marker unreadable — treating as idle", "err", merr)
	} else if m != nil && m.State == upgradeStatePending && a.upgradeNow().Before(m.Deadline) {
		a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{
			Code: "upgrade_in_progress",
			Error: fmt.Sprintf("an upgrade to %s is pending its register deadline (%s); retry after it commits or rolls back",
				m.NewVersion, m.Deadline.UTC().Format(time.RFC3339)),
		})
		return
	}

	allow := a.cfg.UpgradeURLAllowlist
	if len(allow) == 0 {
		allow = defaultAgentURLAllowlist
	}
	if !urlAllowed(req.URL, allow) {
		a.cfg.Logger.Warn("agent: upgrade URL rejected by local allowlist", "url", req.URL)
		a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{
			Code:  "url_not_allowed_local",
			Error: req.URL,
		})
		return
	}

	body, err := fetchURL(req.URL, upgradeFetchTimeout)
	if err != nil {
		// origin: line-2 §12 Y2. One code became three, because the three do not share a remedy: a
		// non-2xx mirror and an oversize tarball will fail identically on every retry, while a
		// transport or read error is exactly the kind that clears. Reporting all three as
		// `download_failed` sent an unclassified code (exit 70), which docs/usage.md §9.13 instructs
		// automation to retry — so `tether node upgrade` against a typo'd URL retried forever.
		//
		// Three literal branches rather than a `code` variable: the wire-code coverage gate resolves
		// codes statically, so a variable here would become an "unresolved site" needing an exemption —
		// and an exemption that can be avoided by writing the code out should not exist.
		switch {
		case errors.Is(err, ErrUpgradeHTTPStatus):
			a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{
				Code: "download_http_status", Error: err.Error(),
			})
		case errors.Is(err, ErrUpgradeHTTPRetryable):
			// origin: external review M1. Its own code rather than folding into download_failed: an
			// operator reading "transport or read failure" for a 503 would look at the network, and the
			// fleet loop needs to tell "skip this node" from "the mirror is down for everyone".
			a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{
				Code: "download_http_retryable", Error: err.Error(),
			})
		case errors.Is(err, ErrUpgradeTooLarge):
			a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{
				Code: "download_too_large", Error: err.Error(),
			})
		default:
			a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{
				Code: "download_failed", Error: err.Error(),
			})
		}
		return
	}
	got := sha256OfBytes(body)
	if got != strings.ToLower(req.SHA256) {
		a.cfg.Logger.Warn("agent: upgrade sha256 mismatch",
			"want", req.SHA256, "got", got, "url", req.URL)
		a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{
			Code:  "sha256_mismatch",
			Error: fmt.Sprintf("got %s want %s", got, req.SHA256),
		})
		return
	}

	newVersion, err := a.installNewBinary(body, exePath)
	if err != nil {
		// Literal branches rather than a code variable — the wire-code
		// coverage gate resolves codes statically (same shape as the
		// fetchURL split above).
		if errors.Is(err, errUpgradeEpochMismatch) {
			// external review F5: a cross-epoch ARTIFACT is the same refusal
			// as a cross-epoch peer — reinstall, never `node upgrade`. The
			// disk is untouched (the smoke gate runs before any mutation).
			a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{
				Code: "proto_bump_requires_reinstall", Error: err.Error(),
			})
			return
		}
		if errors.Is(err, errUpgradeSmokeFailed) {
			a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{
				Code: "smoke_failed", Error: err.Error(),
			})
			return
		}
		a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{
			Code: "install_failed", Error: err.Error(),
		})
		return
	}

	a.cfg.Logger.Info("agent: upgrade installed; re-execing into new binary",
		"new_version", newVersion, "exe", exePath)
	a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{
		OK: true, BinaryReplaced: true, NewVersion: newVersion,
	})

	// Step 5 of J.4: in-place process replacement via syscall.Exec.
	// The old NATS connection drops automatically (kernel closes
	// fds across exec); the new binary runs as the SAME PID, so
	// systemd / setsid-nohup never see the old agent die — there's
	// nothing to "restart". G.1 reconcile then runs against the
	// broker from the new binary's first register.
	//
	// We DON'T fall back to os.Exit + supervisor: clean exits don't
	// trigger Restart=on-failure (agent stays down) and the
	// non-systemd setsid-nohup path has no supervisor at all.
	// Tests pin UpgradeNoExit=true so the in-process harness
	// doesn't replace the go-test binary.
	a.reExecInPlace(exePath)
}

// handleReExecOnly (G5 #13) is the co-located-agent leg of a `cluster upgrade`: the shared binary is
// already staged on disk, so the agent SKIPS download/install and only re-execs. SHA256, when set, is
// the expected ON-DISK BINARY digest — the agent refuses to re-exec a stale/unstaged image.
func (a *Agent) handleReExecOnly(msg *nats.Msg, req proto.UpgradeForwardedReq) {
	exePath := a.cfg.UpgradeExecutablePath
	if exePath == "" {
		var err error
		exePath, err = os.Executable()
		if err != nil {
			a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{Code: "self_path", Error: err.Error()})
			return
		}
	}
	// Stage-C M7: after a rename-replace staging the running inode is gone, so os.Executable() can return
	// the path with a " (deleted)" suffix — trim it or both the sha hash AND the syscall.Exec would target
	// a nonexistent path. (The broker reload path already trims this; the agent leg must too.)
	exePath = strings.TrimSuffix(exePath, " (deleted)")
	// External-review doubt (symmetry with the broker reload primitive): the sha guard is MANDATORY. An
	// unguarded re-exec would launch whatever is on disk; every legitimate trigger threads the staged digest,
	// so fail-closed on an empty digest rather than skipping the check.
	if req.SHA256 == "" {
		a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{Code: "sha256_required",
			Error: "re-exec requires sha256 (the staged-binary digest guard) — refusing an unguarded re-exec"})
		return
	}
	got, err := sha256OfFile(exePath)
	if err != nil {
		a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{Code: "self_path", Error: err.Error()})
		return
	}
	if got != strings.ToLower(req.SHA256) {
		a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{Code: "sha256_mismatch",
			Error: fmt.Sprintf("on-disk binary %s != staged target %s (staging did not land)", got, req.SHA256)})
		return
	}
	a.cfg.Logger.Info("agent: re-exec-only into the staged on-disk binary", "exe", exePath)
	a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{OK: true})
	a.reExecInPlace(exePath)
}

// reExecInPlace replaces this agent process image with the on-disk binary at exePath (same PID; the old
// NATS conn drops as the kernel closes fds across exec). A tiny delay lets the OK reply drain first.
//
// On exec failure with a PENDING upgrade marker (the `node upgrade` path), this process — the OLD image,
// the only surviving copy of the old code — restores the prev slot IN PLACE and keeps running (plan §3.2
// F3). It must NOT exit: os.Exit would hand a clean-exit to Restart=on-failure (agent stays down) and the
// setsid-nohup path has no supervisor at all, while the rollback code needed at next boot would be living
// inside a binary that just proved it cannot exec. Without a pending marker (the ReExecOnly / cluster
// upgrade leg, which stages no marker) the old exit-non-zero contract is preserved unchanged.
// Tests pin UpgradeNoExit so the in-process harness does not replace the go-test binary.
func (a *Agent) reExecInPlace(exePath string) {
	if a.cfg.UpgradeNoExit {
		return
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		argv := append([]string(nil), os.Args...)
		argv[0] = exePath
		if err := syscall.Exec(exePath, argv, os.Environ()); err != nil {
			a.cfg.Logger.Error("agent: re-exec into the new binary failed", "err", err, "exe", exePath)
			if a.recoverFromFailedExec(exePath, err) {
				return
			}
			a.cfg.Logger.Error("agent: re-exec failed with no pending upgrade marker; exiting non-zero for supervisor restart",
				"exe", exePath)
			os.Exit(1)
		}
	}()
}

// recoverFromFailedExec is the F3 closer (upgrade-safety plan §3.2): when
// syscall.Exec of the just-installed binary fails, the still-running OLD
// process image is the only surviving copy of the old code — restore the prev
// slot in place and keep running (reexec=false: we already ARE the restored
// code). Returns true when a pending marker was found and handled; false
// means this exec was not a marker-tracked upgrade (the ReExecOnly leg) and
// the caller keeps its historical exit-non-zero contract.
func (a *Agent) recoverFromFailedExec(exePath string, execErr error) bool {
	a.upgradeMu.Lock()
	defer a.upgradeMu.Unlock()
	handled := false
	// Host-wide flock (external review F1): exclude sibling processes from
	// the marker/prev/dst read-modify-write.
	lerr := withUpgradeFileLock(exePath, true, func() error {
		m, merr := readUpgradeMarker(upgradeMarkerPath(exePath))
		if merr != nil || m == nil || m.State != upgradeStatePending {
			//nolint:nilerr // The closure's error return propagates LOCK failures only; a corrupt
			// marker is by contract "idle" (loud elsewhere, never a block) and means: nothing to recover.
			return nil
		}
		executeRollback(exePath, m, fmt.Sprintf("syscall.Exec of the new binary failed: %v", execErr),
			a.cfg.Logger, false, nil)
		handled = true
		return nil
	})
	if lerr != nil {
		// We cannot safely prove that this was the untracked re-exec-only
		// path without the lock. Keep the only known-live old process rather
		// than exit and risk leaving an unsupervised node permanently down.
		a.cfg.Logger.Error("agent: re-exec recovery: host-wide lock unavailable; keeping the old process alive",
			"err", lerr)
		return true
	}
	return handled
}

func (a *Agent) replyUpgradeForwarded(msg *nats.Msg, resp proto.UpgradeForwardedResp) {
	if msg.Reply == "" {
		return
	}
	body, _ := json.Marshal(&resp)
	_ = msg.Respond(body)
}

// urlAllowed mirrors broker.URLAllowed; duplicated locally so the
// agent package doesn't import broker (would be a layering
// violation: agent is a leaf in production deployments). Empty
// allow → reject.
func urlAllowed(url string, allow []string) bool {
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
	// errUpgradeSmokeFailed marks a smoke-gate refusal (upgrade-safety plan
	// §3.1): the sha-verified artifact could not exec or printed no parsable
	// release tag. The artifact is bad for every node — maps to the
	// `smoke_failed` wire code, which aborts a fleet rollout.
	errUpgradeSmokeFailed = errors.New("upgrade: staged binary failed the smoke gate")
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

func fetchURL(url string, timeout time.Duration) ([]byte, error) {
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
	body, err := io.ReadAll(io.LimitReader(resp.Body, upgradeMaxTarballBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if int64(len(body)) > upgradeMaxTarballBytes {
		return nil, fmt.Errorf("%w: > %d bytes", ErrUpgradeTooLarge, upgradeMaxTarballBytes)
	}
	return body, nil
}

func sha256OfBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// sha256OfFile streams the hex sha256 of a file on disk (G5 #13 re-exec-only: verify the staged binary
// before re-execing it, without loading the whole binary into memory).
func sha256OfFile(path string) (string, error) {
	f, err := os.Open(path)
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

// installNewBinary stages and atomically installs the `tether` binary
// contained in the just-downloaded gzipped tar. The release tarball
// layout (build/goreleaser.yaml § archives) carries a single file
// named `tether` at the archive root plus README/LICENSE; we
// extract just `tether` into a sibling tmp file, chmod +x, then
// (upgrade-safety plan §3.1) run the SMOKE GATE, hardlink the current
// binary into the prev slot, write the pending marker, and finally
// rename onto dst. Rename is atomic on the same fs; running
// processes keep their open inode handle (the new binary only
// loads on the next exec), so this is safe to do while the agent
// is still serving the upgrade reply. dst itself is IN PLACE at every
// interruption point — the prev slot is a hardlink plus a single
// rename, never a remove-then-replace window (plan §10.2: the
// two-rename variant would strand a crashed host on ENOENT).
//
// Uses archive/tar + compress/gzip directly (audit Sec F1: shelling
// out to host tar opens path-traversal exposure under non-GNU tar
// implementations). Each tar header.Name is checked: must be
// exactly "tether" — anything with a path separator, leading slash,
// or "..", or any other filename, is refused.
//
// Returns the release tag the smoke gate parsed out of `<staged> version` —
// GUARANTEED non-empty and in the same format the new binary will report as
// ReleaseVersion on register (ctl --wait compares the two). A failure
// wrapping errUpgradeSmokeFailed means the artifact is bad everywhere and the
// disk was untouched; any other failure is a local IO problem
// (install_failed).
func (a *Agent) installNewBinary(tarball []byte, dst string) (string, error) {
	tmpDir, err := os.MkdirTemp(filepath.Dir(dst), ".tether-upgrade-*")
	if err != nil {
		return "", fmt.Errorf("mkdir tmp: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	binPath := filepath.Join(tmpDir, "tether")
	if err := extractTetherBinary(tarball, binPath); err != nil {
		return "", err
	}

	// Smoke gate: the staged binary must exec and answer `version` with a
	// parsable release tag BEFORE anything on disk changes. Catches wrong
	// arch, truncation, and not-a-tether-binary — the failure classes that
	// used to be discovered only by syscall.Exec, after the flip.
	newVersion, err := smokeVersion(binPath)
	if err != nil {
		return "", fmt.Errorf("staged binary failed the smoke gate (disk unchanged): %w: %w", errUpgradeSmokeFailed, err)
	}

	// Prev slot: hardlink the CURRENT binary aside (same dir ⇒ same fs ⇒
	// zero-copy; a hardlink shares the old inode, so the later rename of the
	// new binary over dst cannot touch it). Some filesystems refuse links —
	// fall back to a copy. Old prev from a previous upgrade is replaced.
	prevPath := upgradePrevPath(dst)
	prevSHA, err := sha256OfFile(dst)
	if err != nil {
		return "", fmt.Errorf("hash current binary: %w", err)
	}
	newSHA, err := sha256OfFile(binPath)
	if err != nil {
		return "", fmt.Errorf("hash staged binary: %w", err)
	}
	var upgradeNonce [16]byte
	if _, err := rand.Read(upgradeNonce[:]); err != nil {
		return "", fmt.Errorf("generate upgrade transaction id: %w", err)
	}
	if err := os.Remove(prevPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("clear old prev slot: %w", err)
	}
	if err := os.Link(dst, prevPath); err != nil {
		if cerr := copyFile(dst, prevPath); cerr != nil {
			return "", fmt.Errorf("prev slot: link failed (%v) and copy failed: %w", err, cerr)
		}
	}
	// F6 ordered durability: the prev slot's directory entry must be on
	// stable storage BEFORE the marker that references it exists — a power
	// cut between marker and prev would otherwise leave a rollback promise
	// with nothing to roll back to.
	if err := syncDirE(filepath.Dir(dst)); err != nil {
		_ = os.Remove(prevPath)
		return "", fmt.Errorf("sync dir after prev slot: %w", err)
	}

	// Marker BEFORE the flip: if the flip below fails, the error path clears
	// it back to a terminal state; if that clearing is itself lost (process
	// death mid-install), the pending marker goes stale at the deadline and
	// the install entry gate stops honoring it.
	deadline := a.upgradeNow().Add(upgradeRegisterDeadline)
	marker := &upgradeMarker{
		State:       upgradeStatePending,
		PrevSHA:     prevSHA,
		NewSHA:      newSHA,
		PrevVersion: proto.ReleaseVersion,
		NewVersion:  newVersion,
		Deadline:    deadline,
		BootBudget:  upgradeBootBudget,
		// external review F1: pin the upgrade to the instance `node upgrade
		// <nid>` targeted — a co-hosted sibling must neither claim nor clear
		// it. UpgradeID is also the process-local boot proof, so it must be
		// unique even for rapid same-artifact retries.
		TargetSID: a.cfg.SID,
		TargetNID: a.cfg.NID,
		UpgradeID: hex.EncodeToString(upgradeNonce[:]),
	}
	// The caller holds the host-wide flock, which serializes this marker write
	// against commit/watchdog in this process and every sibling process. Do
	// not take upgradeMu here: those transitions take upgradeMu before the
	// flock, and reversing that order here creates an ABBA deadlock.
	markerPath := upgradeMarkerPath(dst)
	werr := writeUpgradeMarker(markerPath, marker)
	if werr != nil {
		// external re-review F11: writeUpgradeMarker can fail AFTER its
		// rename landed (the directory-fsync step) — the transaction is
		// aborting, but a visible pending marker would make this healthy
		// old process reject every retry as upgrade_in_progress until the
		// 120s deadline. Compensate under the same locks (host flock is
		// held by the caller): remove whatever landed and re-sync.
		if landed, rerr := readUpgradeMarker(markerPath); rerr == nil && landed != nil &&
			landed.UpgradeID == marker.UpgradeID {
			_ = os.Remove(markerPath)
			syncDir(filepath.Dir(markerPath))
		}
	}
	if werr != nil {
		_ = os.Remove(prevPath)
		syncDir(filepath.Dir(prevPath))
		return "", fmt.Errorf("write upgrade marker: %w", werr)
	}
	if err := os.Rename(binPath, dst); err != nil {
		// dst is untouched (the rename failed) — record the aborted attempt
		// so the entry gate does not block on a flip that never happened.
		marker.State = upgradeStateRolledBack
		marker.Detail = fmt.Sprintf("install aborted before the flip: %v", err)
		werr := writeUpgradeMarker(markerPath, marker)
		if werr != nil {
			a.cfg.Logger.Warn("agent: could not record aborted install; pending marker will go stale at its deadline", "err", werr)
		}
		_ = os.Remove(prevPath)
		return "", fmt.Errorf("atomic rename: %w", err)
	}
	// F6: make the flip itself durable. Past this point the transaction is
	// complete on stable storage; a failure here is logged, not unwound —
	// the runtime state (reply, re-exec) matches the disk either way.
	if err := syncDirE(filepath.Dir(dst)); err != nil {
		a.cfg.Logger.Warn("agent: dir fsync after the flip failed — a power cut may resurface the old binary (marker converges it at boot)", "err", err)
	}
	return newVersion, nil
}

// copyFile is the prev-slot fallback for filesystems that refuse hardlinks.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	_, cpErr := io.Copy(out, in)
	chmodErr := out.Chmod(0o755) // fd-chmod before the fsync (F6): OpenFile's mode is umask-clipped
	syncErr := syncFile(out)     // S13: the prev slot must survive a power cut — it IS the rollback
	closeErr := out.Close()
	if cpErr != nil {
		return cpErr
	}
	if chmodErr != nil {
		return chmodErr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// extractTetherBinary scans the gzipped tar for a single file
// entry literally named "tether" and writes its contents to
// outPath. Refuses any other path. Refuses files larger than
// upgradeMaxTarballBytes (defense in depth — the network read is
// already capped, but a maliciously-crafted small tar could
// declare a huge file size).
//
// outPath is opened with O_EXCL | O_NOFOLLOW so a local attacker
// can't race a symlink swap into the tmp dir between MkdirTemp
// and our open (audit shard 02 F2). MkdirTemp uses 0700 by
// default on Linux but a non-root local who already owns the
// containing directory could pre-plant.
func extractTetherBinary(tarball []byte, outPath string) error {
	gz, err := gzip.NewReader(bytes.NewReader(tarball))
	if err != nil {
		return fmt.Errorf("gzip open: %w", err)
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("tarball missing tether binary entry")
		}
		if err != nil {
			return fmt.Errorf("tar read: %w", err)
		}
		// Strict allowlist: the literal name "tether" and nothing
		// else. base()-ing or filepath.Clean()-ing first would
		// silently accept "./tether", "subdir/tether", or worse,
		// "../something".
		if hdr.Name != "tether" {
			continue
		}
		// archive/tar collapses the legacy TypeRegA into TypeReg
		// since Go 1.11; one check suffices.
		if hdr.Typeflag != tar.TypeReg {
			return fmt.Errorf("tether entry has wrong type: %v", hdr.Typeflag)
		}
		if hdr.Size > upgradeMaxTarballBytes {
			return fmt.Errorf("tether entry too large: %d bytes", hdr.Size)
		}
		f, err := os.OpenFile(outPath,
			os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
		if err != nil {
			return fmt.Errorf("create binary: %w", err)
		}
		_, err = io.CopyN(f, tr, hdr.Size)
		// chmod on the SAME fd, BEFORE the fsync (external review F6): a
		// chmod after the sync would leave the executable bit unpersisted —
		// after a power cut the new dst exists but cannot exec, which no
		// rollback path detects.
		chmodErr := f.Chmod(0o755)
		// fsync before the later rename onto the live binary path (internal
		// review S13): a post-rename power cut must not leave a truncated
		// dst — that is the one artifact the rollback machinery cannot fix.
		syncErr := syncFile(f)
		closeErr := f.Close()
		if err != nil {
			return fmt.Errorf("write binary: %w", err)
		}
		if chmodErr != nil {
			return fmt.Errorf("chmod binary: %w", chmodErr)
		}
		if syncErr != nil {
			return fmt.Errorf("sync binary: %w", syncErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close binary: %w", closeErr)
		}
		return nil
	}
}

// upgradeSmokeTimeout bounds the smoke gate's `version` probe. A healthy
// static binary answers in milliseconds; 5s absorbs a cold page cache.
const upgradeSmokeTimeout = 5 * time.Second

// errUpgradeEpochMismatch marks a smoke-gate refusal because the CANDIDATE
// binary belongs to a different ProtoVersion epoch (external review F5): the
// request's ProtoVersion only proves ctl/broker/this-agent agree — it says
// nothing about the downloaded bytes. Installing a cross-epoch binary would
// smuggle an epoch change past `proto_bump_requires_reinstall`, and the new
// process would be exactly rejected by every same-epoch broker.
var errUpgradeEpochMismatch = errors.New("upgrade: staged binary belongs to a different proto epoch")

// smokeOutputCap bounds how much of the candidate's stdout we will hold
// (external review 疑惑 2): a hostile/broken candidate spraying output must
// not OOM the agent. The real line is <100 bytes.
const smokeOutputCap = 64 * 1024

// smokeVersion is the smoke gate (upgrade-safety plan §3.1): run
// `<staged> version` and STRICTLY parse its first non-empty line against the
// frozen format `tether <release> (proto v<N>)` (proto.VersionLine — the
// cross-version seam both emitter and this parser derive from). Refuses on
// exec failure, unparsable output, or an epoch N different from this
// agent's own ProtoVersion (external review F5). The returned tag is
// normalized to EXACTLY what the new binary reports as ReleaseVersion on
// register — ctl's --wait equality check depends on that (pinned by test).
func smokeVersion(binPath string) (string, error) {
	// ctx-none: reached from a nats.go MsgHandler (func(*nats.Msg)) — no ctx
	// to thread; the probe is bounded by its own timeout.
	ctx, cancel := context.WithTimeout(context.Background(), upgradeSmokeTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, binPath, "version")
	// 疑惑 2 hardening: cap captured output, and don't let an inherited pipe
	// (a candidate that forks children) hold Wait open past the timeout.
	var buf bytes.Buffer
	cmd.Stdout = &capWriter{w: &buf, n: smokeOutputCap}
	cmd.WaitDelay = 2 * time.Second
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("exec `version` on the staged binary: %w", err)
	}
	for _, line := range strings.Split(buf.String(), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		// Frozen shape: exactly `tether <release> (proto v<N>)`.
		if len(fields) != 4 || fields[0] != "tether" || fields[2] != "(proto" || !strings.HasSuffix(fields[3], ")") {
			return "", fmt.Errorf("staged binary's version line %q is not `tether <release> (proto vN)`", line)
		}
		epoch := strings.TrimSuffix(fields[3], ")")
		if epoch != proto.SubjectVersionToken {
			return "", fmt.Errorf("%w: candidate reports %q, this agent is %q — an epoch change must go through reinstall",
				errUpgradeEpochMismatch, epoch, proto.SubjectVersionToken)
		}
		return fields[1], nil
	}
	return "", fmt.Errorf("staged binary printed no version line")
}

// capWriter drops bytes past n (the parser only needs the first line).
type capWriter struct {
	w *bytes.Buffer
	n int
}

func (c *capWriter) Write(p []byte) (int, error) {
	if room := c.n - c.w.Len(); room > 0 {
		if len(p) > room {
			c.w.Write(p[:room])
		} else {
			c.w.Write(p)
		}
	}
	return len(p), nil
}
