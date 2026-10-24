//go:build linux && amd64

// Copy into test/p10. Executes only installer shell functions in private paths.
// Network supply and a bounded file-size failure are injected; no service runs.
package p10_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"text/template"
	"time"

	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/testharness"
	"gopkg.in/yaml.v3"
)

func boundaryScript(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../../scripts/install.sh")
	if err != nil {
		t.Fatal(err)
	}
	pre, _, ok := strings.Cut(string(b), "# -- dispatch --------------------------------------------------------------")
	if !ok {
		t.Fatal("installer dispatch marker missing")
	}
	return pre
}

func boundaryTemp(t *testing.T) string {
	t.Helper()
	base := t.TempDir()
	t.Cleanup(func() {
		// Immutable installer slots are read-only even to the sandbox owner.
		_ = filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
			if err == nil && entry.IsDir() {
				return os.Chmod(path, 0700)
			}
			return err
		})
	})
	return base
}

func boundaryRun(t *testing.T, base, body string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-s")
	cmd.Stdin = strings.NewReader(boundaryScript(t) + "\n" + body)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + base, "TMPDIR=" + base, "TETHER_INSTALL_ROOT=" + base + "/root", "CASE_DIR=" + base, "LC_ALL=C"}
	out, err := testharness.CombinedOutput(cmd)
	if ctx.Err() != nil {
		t.Fatalf("shell did not finish: %v %s", ctx.Err(), out)
	}
	return out, err
}

func boundaryPut(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
}

func boundarySupport(t *testing.T, dir string) {
	t.Helper()
	// Install validation reads the ELF header and digest; these bytes are NEVER executed.
	elf := make([]byte, 64)
	copy(elf, []byte{0x7f, 'E', 'L', 'F', 2, 1, 1})
	elf[18] = 0x3e
	files := []string{"tether-infra-helper", "tether-infra-launch", "infra-catalog.json"}
	manifest := "format 1\nrelease v0.0.0-review\ngoos linux\ngoarch amd64\nprotocol 1\nprevious-reader 1\nsource-sha256 " + strings.Repeat("1", 64) + "\n"
	for _, name := range files {
		data := elf
		if name == "infra-catalog.json" {
			data = []byte("{}")
		}
		boundaryPut(t, filepath.Join(dir, name), data, 0755)
		manifest += fmt.Sprintf("sha256 %x %s\n", sha256.Sum256(data), name)
	}
	boundaryPut(t, filepath.Join(dir, "infra-support.manifest"), []byte(manifest), 0644)
}

// origin: S9 privilege review — a legacy host has no support executable to activate staged support.
// origin: S9 internal review rv-priv F4
func TestLegacyInstallPublishesMissingSupportEntrypoints(t *testing.T) {
	base := boundaryTemp(t)
	boundarySupport(t, filepath.Join(base, "supply"))
	boundaryPut(t, filepath.Join(base, "root/usr/local/bin/tether"), []byte("v0.6.0 fixture; never executed"), 0755)
	out, err := boundaryRun(t, base, `ROLE=broker
VERSION=v0.0.0-review
ARCH=amd64
infra_install_admission
infra_install_directories
stage_infra_support "$CASE_DIR/supply"
write_infra_files
`)
	if err != nil {
		t.Fatalf("staging failed: %v %s", err, out)
	}
	for _, name := range []string{"tether-infra-helper", "tether-infra-launch"} {
		path := filepath.Join(base, "root/usr/local/libexec", name)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("legacy host cannot invoke %s after successful stage: %v", name, err)
		}
	}
	for _, name := range []string{"tether-infra-helper.socket", "tether-infra-helper@.service"} {
		if _, err := os.Stat(filepath.Join(base, "root/etc/tether/infra/templates", name)); err != nil {
			t.Fatal("legacy bootstrap cannot read its first management template", err)
		}
	}
	entry := filepath.Join(base, "root/usr/local/libexec/tether-infra-helper")
	before, err := os.Stat(entry)
	if err != nil {
		t.Fatal(err)
	}
	out, err = boundaryRun(t, base, `ROLE=broker
VERSION=v0.0.0-review
ARCH=amd64
infra_install_admission
infra_install_directories
stage_infra_support "$CASE_DIR/supply"
write_infra_files
`)
	if err != nil {
		t.Fatal("repeat staging failed", err, string(out))
	}
	after, err := os.Stat(entry)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("stage-only replaced an existing support inode", err)
	}
	old, err := os.ReadFile(filepath.Join(base, "root/usr/local/bin/tether"))
	if err != nil || string(old) != "v0.6.0 fixture; never executed" {
		t.Fatal("legacy business entry changed", err)
	}

}

// origin: S9 privilege review — bootstrap socket and the broker client must agree.
// origin: S9 internal review rv-priv F6
func TestInstalledSocketMatchesPrivilegeClient(t *testing.T) {
	base := boundaryTemp(t)
	out, err := boundaryRun(t, base, `ROLE=broker
broker_paths
infra_install_directories
write_infra_files
`)
	if err != nil {
		t.Fatalf("templates failed: %v %s", err, out)
	}
	b, err := os.ReadFile(filepath.Join(base, "root/etc/tether/infra/templates/tether-infra-helper.socket"))
	if err != nil {
		t.Fatal(err)
	}
	want := "ListenStream=" + filepath.Join(base, "root", infrapriv.DefaultSocketPath)
	if !strings.Contains(string(b), want+"\n") {
		t.Fatalf("client uses %s; installed unit:\n%s", infrapriv.DefaultSocketPath, b)
	}
}

// origin: S9 privilege review — policy publication can precede journal creation.
// origin: S9 internal review rv-priv F10
func TestInstalledPolicyAloneBlocksColdReplacement(t *testing.T) {
	base := boundaryTemp(t)
	boundaryPut(t, filepath.Join(base, "root/var/lib/tether-infra/policy.json"), []byte("root authority already published"), 0600)
	// This is the loader's exact directory; contents are intentionally irrelevant
	// because even an unreadable/damaged authority must refuse the shell cold path.
	out, err := boundaryRun(t, base, `ROLE=broker
broker_paths
infra_has_authority
`)
	if err != nil {
		t.Fatalf("installer failed to recognize the actual installed policy path: %v %s", err, out)
	}
	for _, state := range []string{"policy-only", "journal-only", "unreadable-policy", "policy-symlink", "stale-socket"} {
		t.Run(state, func(t *testing.T) {
			base := boundaryTemp(t)
			path := filepath.Join(base, "root/var/lib/tether-infra/policy.json")
			if state == "journal-only" {
				path = filepath.Join(base, "root/var/lib/tether-infra/journal.json")
			}
			if state == "stale-socket" {
				path = filepath.Join(base, "root/run/tether-infra/control.sock")
			}
			boundaryPut(t, path, []byte("unknown authority"), 0600)
			if state == "unreadable-policy" {
				if err := os.Chmod(path, 0000); err != nil {
					t.Fatal(err)
				}
			}
			if state == "policy-symlink" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(base, "missing"), path); err != nil {
					t.Fatal(err)
				}
			}
			if state == "stale-socket" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
				if err != nil {
					t.Fatal(err)
				}
				directory, openErr := os.Open(filepath.Dir(path))
				if openErr != nil {
					// origin: hot-update S10 lint L3a / errcheck
					if err := syscall.Close(fd); err != nil {
						t.Error(err)
					}
					t.Fatal(openErr)
				}
				// Bind through the pinned directory to stay within sockaddr_un's
				// name limit even when testing adds a long private directory name.
				err = syscall.Bind(fd, &syscall.SockaddrUnix{Name: fmt.Sprintf("/proc/self/fd/%d/control.sock", directory.Fd())})
				// origin: hot-update S10 lint L3a / errcheck
				err = errors.Join(err, directory.Close(), syscall.Close(fd))
				if err != nil {
					t.Fatal(err)
				}
			}
			for _, action := range []string{"FORCE_CONFIG=1\ninfra_install_admission", "uninstall_broker"} {
				out, err := boundaryRun(t, base, "ROLE=broker\n"+action+"\n")
				if err == nil || !bytes.Contains(out, []byte("infra_busy")) {
					t.Fatal("authority did not block action", err, string(out))
				}
				if _, err := os.Lstat(path); err != nil {
					t.Fatal("interlock changed authority", err)
				}
				if _, err := os.Lstat(filepath.Join(base, "root/var/lib/tether-infra/ownership")); !os.IsNotExist(err) {
					t.Fatal("refusal came after filesystem effects", err)
				}
			}
		})
	}

}

// origin: S9 privilege review — match release archive layout to the installer consumer.
// origin: S9 internal review rv-priv F2
func TestReleaseSupportLayoutAcceptedByInstaller(t *testing.T) {
	base := boundaryTemp(t)
	// The original counterexample reconstructed the broken declarations and
	// required regeneration after a layout change. Consume every declared file
	// and the real post-build hooks now, without supplementing missing members.
	archive := boundaryReleaseArchive(t, base)
	out, err := boundaryRun(t, base, `ROLE=broker
VERSION=v0.0.0-review
ARCH=amd64
broker_paths
infra_install_directories
mkdir -p "$BIN_DIR"
place_binary "`+archive+`" "$BIN_DIR"
`)
	if err != nil {
		t.Fatalf("configured release layout rejected by installer: %v %s", err, out)
	}
	for _, name := range []string{"tether-infra-helper", "tether-infra-launch"} {
		published := filepath.Join(base, "root/usr/local/libexec", name)
		data, err := os.ReadFile(published)
		if err != nil {
			t.Fatal(err)
		}
		built, err := os.ReadFile(filepath.Join(base, "release", name+"_linux_amd64_v1", name))
		if err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(published)
		if err != nil || info.Mode().Perm() != 0755 || !bytes.Equal(data, built) {
			t.Fatal("installed support bytes/mode differ from the actual build", name, err)
		}
	}

}

// origin: S9 privilege review — preserve the old canonical executable on failed copy.
// origin: S9 internal review rv-priv F9
func TestCaddyInstallFailurePreservesOldBinary(t *testing.T) {
	for _, fault := range []string{"copy", "rename", "directory-sync", "none"} {
		t.Run(fault, func(t *testing.T) { exerciseCaddyPublication(t, fault) })
	}
}

func exerciseCaddyPublication(t *testing.T, fault string) {
	base := boundaryTemp(t)
	var archive bytes.Buffer
	z := gzip.NewWriter(&archive)
	tw := tar.NewWriter(z)
	payload := bytes.Repeat([]byte("new-caddy-payload"), 1024)
	if err := tw.WriteHeader(&tar.Header{Name: "caddy", Mode: 0755, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	boundaryPut(t, filepath.Join(base, "caddy.tgz"), archive.Bytes(), 0600)
	sum := fmt.Sprintf("%x  caddy_2.7.6_linux_amd64.tar.gz\n", sha256.Sum256(archive.Bytes()))
	boundaryPut(t, filepath.Join(base, "checksums"), []byte(sum), 0600)
	old := []byte("old-caddy-must-remain")
	target := filepath.Join(base, "bin/caddy")
	boundaryPut(t, target, old, 0755)
	before, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	injection := ""
	switch fault {
	case "copy":
		injection = `install() { (ulimit -c 0; ulimit -f 1; /usr/bin/install "$@"); }`
	case "rename":
		injection = `mv() { case "$2" in */.caddy.new.*) return 1 ;; esac; /bin/mv "$@"; }`
	case "directory-sync":
		injection = `failed=0
sync() { if [ "$#" -eq 1 ] && [ "$1" = "$CASE_DIR/bin" ] && [ "$failed" -eq 0 ]; then failed=1; return 1; fi; /bin/sync "$@"; }`
	}
	out, err := boundaryRun(t, base, `ROLE=ctl
OS=linux
ARCH=amd64
FORCE_CONFIG=1
CADDY_VERSION=2.7.6
curl() {
    src="$CASE_DIR/caddy.tgz"
    dest=
    while [ "$#" -gt 0 ]; do
        case "$1" in *checksums.txt) src="$CASE_DIR/checksums" ;; -o) dest="$2"; shift ;; esac
        shift
    done
    /bin/cp "$src" "$dest"
}

# Bound only the final copy. The kernel returns EFBIG/SIGXFSZ, simulating a
# failed binary write without requiring a full disk or touching any host file.
`+injection+`
install_caddy "$CASE_DIR/bin"
`)
	if fault == "none" {
		if err != nil {
			t.Fatal("successful publication failed", err, string(out))
		}
		previous := filepath.Join(base, "bin", fmt.Sprintf(".caddy.previous.%x.%d.%d", sha256.Sum256(old), before.Sys().(*syscall.Stat_t).Dev, before.Sys().(*syscall.Stat_t).Ino))
		info, err := os.Stat(previous)
		if err != nil || !os.SameFile(before, info) {
			t.Fatal("rollback inode not retained", err)
		}
		// A later startup failure can restore this exact inode without a download.
		if err := os.Rename(previous, target); err != nil {
			t.Fatal(err)
		}
	} else if err == nil {
		t.Fatalf("injected copy failure unexpectedly succeeded: %s", out)
	}
	after, readErr := os.ReadFile(target)
	if readErr != nil || !bytes.Equal(after, old) {
		t.Fatalf("failed copy destroyed old binary: remaining=%d bytes readErr=%v; command=%v %s", len(after), readErr, err, out)
	}
	info, err := os.Stat(target)
	if err != nil || !os.SameFile(before, info) || before.Mode() != info.Mode() {
		t.Fatal("publication failure/rollback changed the original inode or mode", err)
	}
}

// origin: S9 privilege review — nats.d is deliberately writable by the broker UID.
// Pause after the real validation, then change the name just as a concurrent UID
// could. Every production function body stays unchanged; only return scheduling
// is controlled. The root victim is represented by a private sandbox canary.
// origin: S9 internal review rv-priv F1
func TestNATSConfigWriteRejectsPostValidationSymlink(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseNATSConfigWrite(t)
	}
	t.Run("publication-failure", func(t *testing.T) {
		base := boundaryTemp(t)
		target := filepath.Join(base, "root/etc/tether/nats.d/nats.conf")
		boundaryPut(t, target, []byte("operator config"), 0644)
		out, err := boundaryRun(t, base, `ROLE=broker
FORCE_CONFIG=1
broker_paths
infra_install_directories
mkdir -p "$SYSTEMD_DIR"
mv() { return 1; }
write_systemd_units "$SYSTEMD_DIR" "$BIN_DIR" "$ETC_DIR" "$LIB_DIR" "$LOG_DIR"
`)
		if err == nil || !bytes.Contains(out, []byte("atomic config publication failed")) {
			t.Fatal("failed config rename was not rejected at publication", err, string(out))
		}
		entries, err := os.ReadDir(filepath.Join(base, "root/var/lib/tether-infra/ownership"))
		if err != nil || len(entries) != 0 {
			t.Fatal("failed publication claimed ownership of old content", err)
		}
	})
	testharness.AssertNoGoroutineLeak(t, "installer config publication", beforeG)
	testharness.AssertNoFDLeak(t, "installer config publication", beforeFD, 0)
}

func exerciseNATSConfigWrite(t *testing.T) {
	base := boundaryTemp(t)
	victim := filepath.Join(base, "privileged-canary")
	old := []byte("privileged-file-must-not-change")
	boundaryPut(t, victim, old, 0600)
	before, err := os.Stat(victim)
	if err != nil {
		t.Fatal(err)
	}
	readyR, readyW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := readyR.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Error(err)
		}
	}()
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := readyW.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Error(err)
		}
	}()
	releaseR, releaseW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := releaseR.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Error(err)
		}
	}()
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := releaseW.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	script := boundaryScript(t)
	start := strings.Index(script, "config_dest() {")
	if start < 0 {
		t.Fatal("missing config_dest")
	}
	end := strings.Index(script[start:], "\n}\n")
	if end < 0 {
		t.Fatal("missing function end")
	}
	original := strings.Replace(script[start:start+end+3], "config_dest() {", "boundary_original_config_dest() {", 1)
	body := `
ROLE=broker
INSTALL_STAGE_ONLY=1
broker_paths
infra_install_directories
mkdir -p "$ETC_DIR/nats.d" "$SYSTEMD_DIR"
config_dest() {
    boundary_original_config_dest "$1"
    case "$1" in */nats.d/nats.conf) printf r >&4; read -r resume <&3 ;; esac
}
write_systemd_units "$SYSTEMD_DIR" "$BIN_DIR" "$ETC_DIR" "$LIB_DIR" "$LOG_DIR"
`
	cmd := exec.CommandContext(ctx, "/bin/sh", "-s")
	cmd.Stdin = strings.NewReader(script + "\n" + original + "\n" + body)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + base, "TMPDIR=" + base, "TETHER_INSTALL_ROOT=" + base + "/root", "LC_ALL=C"}
	cmd.ExtraFiles = []*os.File{releaseR, readyW}
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	wait, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	_ = readyW.Close()
	_ = releaseR.Close()
	if err := readyR.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		cancel()
		_ = wait()
		t.Fatal(err)
	}
	var signal [1]byte
	if _, err := io.ReadFull(readyR, signal[:]); err != nil {
		cancel()
		_ = wait()
		t.Fatalf("validation barrier failed: %v %s", err, output.Bytes())
	}
	link := filepath.Join(base, "root/etc/tether/nats.d/nats.conf.new")
	if err := os.Symlink(victim, link); err != nil {
		cancel()
		_ = wait()
		t.Fatal(err)
	}
	if _, err := io.WriteString(releaseW, "go\n"); err != nil {
		cancel()
		_ = wait()
		t.Fatal(err)
	}
	_ = releaseW.Close()
	runErr := wait()
	after, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, old) {
		t.Fatalf("root-side writer followed symlink after validation, overwrote %d bytes; rc=%v output=%s", len(after), runErr, output.Bytes())
	}
	info, err := os.Stat(victim)
	if err != nil {
		t.Fatal(err)
	}
	a, b := before.Sys().(*syscall.Stat_t), info.Sys().(*syscall.Stat_t)
	if a.Ino != b.Ino || a.Uid != b.Uid || a.Gid != b.Gid || before.Mode() != info.Mode() {
		t.Fatal("post-validation symlink changed victim identity or permissions")
	}

}

// Build the selected archive's actual binaries and execute its declared hooks.
// This is hermetic and never executes an installed tether or a support candidate.
func boundaryReleaseArchive(t *testing.T, base string) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "build/goreleaser.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	type archiveFile struct {
		Src, Dst string
		Info     struct{ Mode int }
	}
	var config struct {
		Builds []struct {
			ID, Main, Binary string
			Flags, Ldflags   []string
			Hooks            struct{ Post []string }
		}
		Archives []struct {
			ID    string
			IDs   []string
			Files []yaml.Node
		}
	}
	if err := yaml.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	dist := filepath.Join(base, "release")
	if err := os.Mkdir(dist, 0700); err != nil {
		t.Fatal(err)
	}
	values := map[string]string{"Dist": dist, "Version": "0.0.0-review", "Os": "linux", "Arch": "amd64"}
	render := func(text string) string {
		v, err := template.New("release").Option("missingkey=error").Parse(text)
		if err != nil {
			t.Fatal(err)
		}
		var b bytes.Buffer
		if err := v.Execute(&b, values); err != nil {
			t.Fatal(err)
		}
		return b.String()
	}
	run := func(args ...string) []byte {
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH=amd64", "PYTHONDONTWRITEBYTECODE=1")
		out, err := testharness.CombinedOutput(cmd)
		if err != nil {
			t.Fatalf("release build/hook failed: %v %s", err, out)
		}
		return out
	}
	var files map[string]string
	for _, archive := range config.Archives {
		if archive.ID != "tether-linux" {
			continue
		}
		files = make(map[string]string)
		for _, id := range archive.IDs {
			found := false
			for _, build := range config.Builds {
				if build.ID != id {
					continue
				}
				found = true
				binary := filepath.Join(dist, build.ID+"_linux_amd64_v1", build.Binary)
				args := []string{"go", "build"}
				args = append(args, build.Flags...)
				args = append(args, "-ldflags="+render(strings.Join(build.Ldflags, " ")), "-o", binary, build.Main)
				run(args...)
				values["Path"] = binary
				for _, hook := range build.Hooks.Post {
					run(strings.Fields(render(hook))...)
				}
				files[build.Binary] = binary
			}
			if !found {
				t.Fatal("archive refers to absent build", id)
			}
		}
		for _, node := range archive.Files {
			entry := archiveFile{}
			if node.Kind == yaml.ScalarNode {
				entry.Src, entry.Dst = node.Value, node.Value
			} else if err := node.Decode(&entry); err != nil {
				t.Fatal(err)
			}
			source := render(entry.Src)
			if strings.HasPrefix(source, "dist/") {
				source = filepath.Join(dist, strings.TrimPrefix(source, "dist/"))
			}
			if !filepath.IsAbs(source) {
				source = filepath.Join(root, source)
			}
			if entry.Src == "completions/_tether" {
				// Equivalent to the release before-hook, using this test's newly
				// compiled executable by absolute path and a private HOME.
				source = filepath.Join(dist, "_tether")
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				cmd := exec.CommandContext(ctx, files["tether"], "completion", "zsh")
				cmd.Env = []string{"HOME=" + base, "PATH=/usr/bin:/bin"}
				data, err := testharness.CombinedOutput(cmd)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				boundaryPut(t, source, data, 0644)
			}
			files[entry.Dst] = source
		}
	}
	if files == nil {
		t.Fatal("Linux archive missing")
	}
	archive := filepath.Join(base, "release.tar.gz")
	out, err := os.Create(archive)
	if err != nil {
		t.Fatal(err)
	}
	z := gzip.NewWriter(out)
	tw := tar.NewWriter(z)
	for name, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal("declared archive member missing", name, err)
		}
		mode := int64(0644)
		if name == "tether" || strings.HasPrefix(name, "tether-infra-") {
			mode = 0755
		}
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: mode, Size: int64(len(data))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	return archive
}
