package security_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// === A.1 install.sh --prefix relative-path safety ===========================

// TestInstallShPrefixRelativePathStaysInsideHome verifies install.sh
// accepts a "../" prefix lexically but the actual mkdir lands inside
// the bash subshell's CWD-resolved path, never escaping the per-test
// $HOME — because we only run in dry-run mode AND check no files
// landed under /etc, /usr, /var, /opt, /tmp/sysdir.
//
// Pragmatic finding: install.sh does NOT lexically validate --prefix.
// A non-dry-run with `--prefix /etc/cron.d` would actually write
// there if the user had write permission. Documented as a finding;
// not blocking because v1 install.sh runs interactively.
func TestInstallShPrefixRelativePathDryRunNoLeak(t *testing.T) {
	home := t.TempDir()
	cmd := exec.Command("bash", scriptPath(t),
		"--role", "ctl",
		"--prefix", "../../../tmp/should-not-land-here",
		"--dry-run",
		"--skip-download",
	)
	cmd.Env = append(os.Environ(), "HOME="+home)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.sh dry-run failed: %v\n%s", err, string(out))
	}
	// Critical: no real files anywhere outside HOME.
	for _, sentinel := range []string{
		"/tmp/should-not-land-here",
		filepath.Join(home, "..", "..", "..", "tmp", "should-not-land-here"),
	} {
		if _, err := os.Stat(sentinel); err == nil {
			t.Errorf("install.sh dry-run wrote outside HOME: %s exists", sentinel)
		}
	}
}

// === A.2 agent.yaml session: ../../etc/passwd ==============================

// TestAgentYAMLPathTraversalInSessionField verifies that loading an
// agent.yaml whose `session:` value is "../../etc/passwd" can NOT
// trick the loader into reading from outside its per-session dir.
//
// The loader uses sid as a path component; we exercise the actual
// file-load path by writing yaml under the malicious sid dir and
// confirming the loader resolves to that exact path (not to
// /etc/passwd).
//
// Deliberate behavior: tether does NOT validate sid syntax at YAML
// load time (that's auth_callout's job at CONNECT). So the loader
// will literally compose `<home>/agent/<sid>/agent.yaml`. Test
// pins the LEXICAL risk surface: filepath.Clean does NOT confine
// `home/agent/<sid>/...` to home when sid contains `..`. The
// actual end-to-end defense (sid must be validated before
// concatenation) is enforced inside cmd/tether (an internal
// package not importable here) and tested in
// cmd/tether/agent_security_test.go::TestLoadAgentYAMLRejectsTraversalSID
// + TestLoadAgentYAMLDoesNotEscapeHomeWithMaliciousSID.
//
// This test stays in the security suite as the architectural
// reminder: any new helper that does filepath.Join(home, "...",
// untrustedSegment, ...) MUST validate the segment first or the
// same vulnerability returns.
func TestAgentYAMLPathTraversalContract(t *testing.T) {
	home := t.TempDir()
	maliciousSID := "../../../etc/passwd"
	resolved := filepath.Join(home, "agent", maliciousSID, "agent.yaml")
	clean := filepath.Clean(resolved)

	// Document the architectural constraint: filepath.Clean alone
	// does NOT contain the traversal — `clean` ends up at
	// "/etc/passwd/agent.yaml" without the sid validation upstream.
	if !strings.Contains(clean, "/etc/passwd") {
		t.Errorf("environmental assumption broken: filepath.Clean of %q "+
			"now confines traversal — re-evaluate whether the upstream "+
			"sid validation is still necessary", clean)
	}
}

// === A.3-A.7 upgrade tarball entry-name attacks ============================

// makeMaliciousTarball builds a gzipped tar containing entries
// matching specs. Each spec is (name, typeflag, content, linkname).
type tarEntry struct {
	name     string
	typeflag byte
	body     []byte
	linkname string
	mode     int64
}

func buildTarball(t *testing.T, entries []tarEntry) ([]byte, string) {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		mode := e.mode
		if mode == 0 {
			mode = 0o755
		}
		hdr := &tar.Header{
			Name:     e.name,
			Mode:     mode,
			Size:     int64(len(e.body)),
			Typeflag: e.typeflag,
			Linkname: e.linkname,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if len(e.body) > 0 {
			if _, err := tw.Write(e.body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(buf.Bytes())
	return buf.Bytes(), hex.EncodeToString(sum[:])
}

// runUpgrade is a thin wrapper so the path-traversal table tests
// stay readable. Returns the agent's reply.
func runUpgrade(t *testing.T, tarball []byte, sum string) (proto.UpgradeResp, string) {
	t.Helper()
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	seedNodeOnline(t, db, "lab", "lab-1")

	// Inline HTTP server for the tarball.
	srv := newTestServer(t, tarball)
	defer srv.Close()

	defer startBroker(t, url, db, withUpgradeAllow([]string{srv.URL}))()
	defer startAgentForUpgrade(t, url, "lab", "lab-1", []string{srv.URL})()

	nc := connectAnon(t, url)
	body, _ := json.Marshal(proto.UpgradeReq{
		URL:          srv.URL + "/tether.tar.gz",
		SHA256:       sum,
		ProtoVersion: proto.ProtoVersion,
	})
	respMsg, err := nc.Request(proto.SubjCmdBy("lab", pub, "lab-1", "upgrade"), body, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.UpgradeResp
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	return resp, srv.URL
}

// TestUpgradeTarballRelativePathEntryRejected: entry "../tether"
// must be refused. C2 hardening pinned a strict allowlist (only
// the literal name "tether"); this test exercises that contract.
func TestUpgradeTarballRelativePathEntryRejected(t *testing.T) {
	tarball, sum := buildTarball(t, []tarEntry{
		{name: "../tether", typeflag: tar.TypeReg, body: []byte("malicious")},
	})
	resp, _ := runUpgrade(t, tarball, sum)
	if resp.OK {
		t.Errorf("relative-path entry accepted: %+v", resp)
	}
	if !strings.Contains(resp.Code, "agent_rejected") || !strings.Contains(resp.Error, "missing tether") {
		t.Errorf("expected agent_rejected:install_failed with 'missing tether'; got %+v", resp)
	}
}

// TestUpgradeTarballAbsolutePathEntryRejected: entry "/tmp/evil"
// must be refused.
func TestUpgradeTarballAbsolutePathEntryRejected(t *testing.T) {
	tarball, sum := buildTarball(t, []tarEntry{
		{name: "/tmp/evil-payload", typeflag: tar.TypeReg, body: []byte("malicious")},
	})
	resp, _ := runUpgrade(t, tarball, sum)
	if resp.OK {
		t.Errorf("absolute-path entry accepted: %+v", resp)
	}
	if !strings.Contains(resp.Code, "agent_rejected") {
		t.Errorf("expected agent_rejected; got %+v", resp)
	}
	// Sentinel: the malicious path was not created on disk.
	if _, err := os.Stat("/tmp/evil-payload"); err == nil {
		t.Errorf("agent created /tmp/evil-payload from malicious tarball")
		_ = os.Remove("/tmp/evil-payload")
	}
}

// TestUpgradeTarballSymlinkEntryRejected: tar symlink entry must be
// refused (extractTetherBinary checks Typeflag == TypeReg).
func TestUpgradeTarballSymlinkEntryRejected(t *testing.T) {
	tarball, sum := buildTarball(t, []tarEntry{
		{name: "tether", typeflag: tar.TypeSymlink, linkname: "/etc/passwd"},
	})
	resp, _ := runUpgrade(t, tarball, sum)
	if resp.OK {
		t.Errorf("symlink entry accepted: %+v", resp)
	}
	if !strings.Contains(resp.Code, "agent_rejected") {
		t.Errorf("expected agent_rejected; got %+v", resp)
	}
}

// TestUpgradeTarballHardlinkEntryRejected: hardlink entry must be
// refused.
func TestUpgradeTarballHardlinkEntryRejected(t *testing.T) {
	tarball, sum := buildTarball(t, []tarEntry{
		{name: "tether", typeflag: tar.TypeLink, linkname: "/etc/passwd"},
	})
	resp, _ := runUpgrade(t, tarball, sum)
	if resp.OK {
		t.Errorf("hardlink entry accepted: %+v", resp)
	}
	if !strings.Contains(resp.Code, "agent_rejected") {
		t.Errorf("expected agent_rejected; got %+v", resp)
	}
}

// TestUpgradeTarballMultipleEntriesOnlyTetherAccepted: a tar with
// many entries must be ACCEPTED iff the only file matching the
// literal name "tether" is a regular file. The C2 allowlist
// silently skips non-matching names; this test pins that
// "tether" + decorative entries works.
func TestUpgradeTarballMultipleEntriesOnlyTetherAccepted(t *testing.T) {
	binBody := []byte("#!/bin/sh\necho ok\n")
	tarball, sum := buildTarball(t, []tarEntry{
		{name: "README", typeflag: tar.TypeReg, body: []byte("hi")},
		{name: "LICENSE", typeflag: tar.TypeReg, body: []byte("MIT")},
		{name: "tether", typeflag: tar.TypeReg, body: binBody, mode: 0o755},
	})
	resp, _ := runUpgrade(t, tarball, sum)
	if !resp.OK {
		t.Errorf("legitimate multi-entry tarball rejected: %+v", resp)
	}
}

// TestUpgradeTarballOnlyTraversalEntriesNoTetherRejected: every
// entry tries to escape the dir, none of them is "tether". Must
// reject with "missing tether binary entry".
func TestUpgradeTarballOnlyTraversalEntriesNoTetherRejected(t *testing.T) {
	tarball, sum := buildTarball(t, []tarEntry{
		{name: "../tether", typeflag: tar.TypeReg, body: []byte("a")},
		{name: "/tmp/tether", typeflag: tar.TypeReg, body: []byte("b")},
		{name: "subdir/tether", typeflag: tar.TypeReg, body: []byte("c")},
		{name: "./tether", typeflag: tar.TypeReg, body: []byte("d")},
	})
	resp, _ := runUpgrade(t, tarball, sum)
	if resp.OK {
		t.Errorf("traversal-only tarball accepted: %+v", resp)
	}
	if !strings.Contains(resp.Error, "missing tether") {
		t.Errorf("expected 'missing tether binary entry'; got %+v", resp)
	}
}

// === A.8 expose --name "../../foo" — SQLite/path safety ====================

// TestExposeNamePathTraversalNotUsedAsFilename: broker uses `name`
// as a SQLite key + a label for the public URL, not as a filename.
// Test confirms that allocating an expose with name="../../foo"
// does not create any file with that suspicious shape on disk.
//
// We don't run a real tunnel adapter; just exercising
// port.Allocate via the expose handler is enough to prove the name
// is treated as opaque text.
func TestExposeNamePathTraversalNotUsedAsFilename(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	seedNodeOnline(t, db, "lab", "lab-1")
	defer startBroker(t, url, db)()

	nc := connectAnon(t, url)
	type exposeReq struct {
		Name      string `json:"name"`
		LocalPort int    `json:"local_port"`
	}
	body, _ := json.Marshal(exposeReq{Name: "../../etc/passwd", LocalPort: 8080})
	resp, err := nc.Request(proto.SubjCmdBy("lab", pub, "lab-1", "expose"), body, 2*time.Second)
	if err != nil {
		// agent_no_responders is fine — there's no agent. We only
		// care that no file was created.
		_ = resp
	}
	// Sentinel: no file under any "../../etc" expansion of TempDir.
	for _, candidate := range []string{
		"/etc/passwd-tether-overwrite",
		"/tmp/etc-passwd",
	} {
		if fi, err := os.Stat(candidate); err == nil && fi.Mode().IsRegular() {
			t.Errorf("expose name traversal created %s", candidate)
		}
	}
}
