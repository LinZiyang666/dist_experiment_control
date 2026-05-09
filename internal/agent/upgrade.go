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
	"compress/gzip"
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

// UpgradeURLAllowlist is the agent-side defense-in-depth allowlist.
// Defaults to nil (= reject everything) if the operator forgets to
// configure it; the broker ALWAYS double-gates the same allowlist,
// so this is belt-and-suspenders for an attacker who somehow
// reached the forwarded subject directly.
var defaultAgentURLAllowlist = []string{
	"https://github.com/LinZiyang666/tether/releases/",
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
		a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{
			Code: "download_failed", Error: err.Error(),
		})
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

	exePath := a.cfg.UpgradeExecutablePath
	if exePath == "" {
		var err error
		exePath, err = os.Executable()
		if err != nil {
			a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{
				Code: "self_path", Error: err.Error(),
			})
			return
		}
	}
	newVersion, err := installNewBinary(body, exePath)
	if err != nil {
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
	if !a.cfg.UpgradeNoExit {
		go func() {
			// Tiny delay so the OK reply has a chance to drain
			// over NATS before we exec out.
			time.Sleep(100 * time.Millisecond)
			argv := append([]string(nil), os.Args...)
			argv[0] = exePath
			if err := syscall.Exec(exePath, argv, os.Environ()); err != nil {
				// Exec failed — last-ditch fall back to os.Exit
				// with a non-zero code so Restart=on-failure has
				// something to grab onto. Logged so the operator
				// can see why the in-place upgrade didn't take.
				a.cfg.Logger.Error("agent: re-exec failed; exiting non-zero for supervisor restart",
					"err", err, "exe", exePath)
				os.Exit(1)
			}
		}()
	}
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
func fetchURL(url string, timeout time.Duration) ([]byte, error) {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("http get: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("http status %s", resp.Status)
	}
	// LimitReader + 1 trick: read maxN+1 bytes; if the read
	// returned exactly maxN+1 bytes the actual stream is larger
	// than maxN. Anything <= maxN is fine.
	body, err := io.ReadAll(io.LimitReader(resp.Body, upgradeMaxTarballBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if int64(len(body)) > upgradeMaxTarballBytes {
		return nil, fmt.Errorf("upgrade tarball too large: > %d bytes", upgradeMaxTarballBytes)
	}
	return body, nil
}

func sha256OfBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// installNewBinary atomically replaces dst with the `tether` binary
// contained in the just-downloaded gzipped tar. The release tarball
// layout (build/goreleaser.yaml § archives) carries a single file
// named `tether` at the archive root plus README/LICENSE; we
// extract just `tether` into a sibling tmp file, chmod +x, then
// rename onto dst. Rename is atomic on the same fs; running
// processes keep their open inode handle (the new binary only
// loads on the next exec), so this is safe to do while the agent
// is still serving the upgrade reply.
//
// Uses archive/tar + compress/gzip directly (audit Sec F1: shelling
// out to host tar opens path-traversal exposure under non-GNU tar
// implementations). Each tar header.Name is checked: must be
// exactly "tether" — anything with a path separator, leading slash,
// or "..", or any other filename, is refused.
//
// Returns the new tether version string parsed from the binary
// (best-effort via `tether version`); on parse failure the field
// stays empty but the install is still considered a success.
func installNewBinary(tarball []byte, dst string) (string, error) {
	tmpDir, err := os.MkdirTemp(filepath.Dir(dst), ".tether-upgrade-*")
	if err != nil {
		return "", fmt.Errorf("mkdir tmp: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	binPath := filepath.Join(tmpDir, "tether")
	if err := extractTetherBinary(tarball, binPath); err != nil {
		return "", err
	}
	if err := os.Chmod(binPath, 0o755); err != nil {
		return "", fmt.Errorf("chmod: %w", err)
	}
	if err := os.Rename(binPath, dst); err != nil {
		return "", fmt.Errorf("atomic rename: %w", err)
	}
	return readVersionString(dst), nil
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
	gz, err := gzip.NewReader(strings.NewReader(string(tarball)))
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
		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeRegA {
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
		closeErr := f.Close()
		if err != nil {
			return fmt.Errorf("write binary: %w", err)
		}
		if closeErr != nil {
			return fmt.Errorf("close binary: %w", closeErr)
		}
		return nil
	}
}

// readVersionString runs `<exe> version` and returns the first line
// trimmed. Failure is non-fatal — install still succeeded; we just
// won't have the new version number to report.
func readVersionString(exe string) string {
	out, err := exec.Command(exe, "version").Output()
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			return line
		}
	}
	return ""
}
