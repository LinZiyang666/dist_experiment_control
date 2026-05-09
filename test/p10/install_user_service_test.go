package p10_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// tetherBinary builds the tether CLI into a temp dir and returns
// the absolute path. Reused across the install-user-service tests
// so we exercise the actual cobra flag wiring instead of mocking
// it. Build is one-shot per test run via sync.Once equivalent
// (caller passes the result around).
func tetherBinary(t *testing.T) string {
	t.Helper()
	tmp, err := os.MkdirTemp("", "tether-bin-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tmp) })
	bin := filepath.Join(tmp, "tether")

	_, thisFile, _, _ := runtime.Caller(0)
	repo := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/tether")
	cmd.Dir = repo
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build tether: %v\n%s", err, string(out))
	}
	return bin
}

// TestAgentInstallUserServiceWritesUnit: --install-user-service
// drops a template unit at ~/.config/systemd/user/tether-agent@<sid>.service
// and exits without starting anything.
func TestAgentInstallUserServiceWritesUnit(t *testing.T) {
	bin := tetherBinary(t)
	home := t.TempDir()

	cmd := exec.Command(bin, "agent",
		"--install-user-service",
		"--session", "lab",
		"--nid", "lab-1",
	)
	cmd.Env = append(os.Environ(), "HOME="+home)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install-user-service: %v\n%s", err, string(out))
	}

	unit := filepath.Join(home, ".config", "systemd", "user", "tether-agent@lab.service")
	body, err := os.ReadFile(unit)
	if err != nil {
		t.Fatalf("unit file not written: %v", err)
	}
	for _, want := range []string{
		"[Unit]",
		"Description=tether agent (session=lab nid=lab-1)",
		"ExecStart=" + bin + " agent --session lab --nid lab-1",
		"Restart=on-failure",
		"WantedBy=default.target",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("unit missing %q; got:\n%s", want, string(body))
		}
	}
}

// TestAgentInstallUserServiceRejectsMissingSession: install path
// requires --session and --nid (the same as the run path), and
// surfaces a clean error rather than silently writing a unit
// pointing nowhere.
func TestAgentInstallUserServiceRejectsMissingSession(t *testing.T) {
	bin := tetherBinary(t)
	home := t.TempDir()

	cmd := exec.Command(bin, "agent", "--install-user-service")
	cmd.Env = append(os.Environ(), "HOME="+home)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected install-user-service to fail without --session\n%s", out)
	}
	if !strings.Contains(string(out), "--session and --nid are required") {
		t.Errorf("expected required-flag error; got:\n%s", out)
	}
}

// TestAgentUninstallUserServiceRemovesUnit: --uninstall removes
// the per-session unit file (no-op when absent).
func TestAgentUninstallUserServiceRemovesUnit(t *testing.T) {
	bin := tetherBinary(t)
	home := t.TempDir()

	// Pre-stage a unit file.
	unitDir := filepath.Join(home, ".config", "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o755); err != nil {
		t.Fatal(err)
	}
	unit := filepath.Join(unitDir, "tether-agent@lab.service")
	if err := os.WriteFile(unit, []byte("[Service]\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(bin, "agent", "--uninstall", "--session", "lab")
	cmd.Env = append(os.Environ(), "HOME="+home)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("uninstall: %v\n%s", err, string(out))
	}
	if _, err := os.Stat(unit); !os.IsNotExist(err) {
		t.Errorf("unit file not removed: %v", err)
	}
}
