//go:build linux && amd64 && infraprivilege_integration

package infraprivilege

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/test/infraharness"
)

// The filesystem oracle walks the entire redirected root, not a handpicked
// /etc subtree. Reading hashes deliberately excludes atime: the observer itself
// can update atime. Inode, uid/gid, type/mode, mtime/ctime and bytes remain exact.
type installedFile struct {
	Mode          uint32
	UID, GID      uint32
	Device, Inode uint64
	Size          int64
	MTime, CTime  syscall.Timespec
	SHA256        c.Digest
	Link          string
}

func treeManifest(t *testing.T, root string) map[string]installedFile {
	t.Helper()
	result := make(map[string]installedFile)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("numeric filesystem metadata unavailable")
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		row := installedFile{Mode: stat.Mode, UID: stat.Uid, GID: stat.Gid, Device: uint64(stat.Dev), Inode: stat.Ino, Size: stat.Size, MTime: stat.Mtim, CTime: stat.Ctim}
		if info.Mode().IsRegular() {
			row.SHA256 = sha256.Sum256(readFile(t, path))
		}
		if info.Mode()&os.ModeSymlink != 0 {
			row.Link, err = os.Readlink(path)
			if err != nil {
				return err
			}
		}
		result[relative] = row
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func assertManifestEqual(t *testing.T, label string, before, after map[string]installedFile) {
	t.Helper()
	changed := make([]string, 0)
	for path, row := range before {
		if other, ok := after[path]; !ok || other != row {
			changed = append(changed, path)
		}
	}
	for path := range after {
		if _, ok := before[path]; !ok {
			changed = append(changed, path)
		}
	}
	if len(changed) != 0 {
		sort.Strings(changed)
		t.Errorf("%s: dry-run changed content or numeric metadata at %v", label, changed)
	}
}

// The receiver must still hold the permanent lock inode after launcher exec;
// a pre-exec ACK or close/re-lock gap cannot supply this physical receipt.
func assertInheritedStoreLock(t *testing.T, process c.Process, store c.Store) {
	t.Helper()
	if store.LockDevice == 0 || store.LockInode == 0 {
		t.Fatal("launcher receipt omitted lifetime lock identity")
	}
	directory := fmt.Sprintf("/proc/%d/fd", process.PID)
	files, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, file := range files {
		info, err := os.Stat(filepath.Join(directory, file.Name()))
		if err != nil {
			continue
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if ok && uint64(stat.Dev) == store.LockDevice && stat.Ino == store.LockInode {
			matched++
		}
	}
	if matched != 1 {
		t.Fatalf("stock server inherited %d lifetime lock references", matched)
	}
}

type installerSandbox struct {
	root, home, tmp, tools, audit, downloads, script string
}

func newInstallerSandbox(t *testing.T) installerSandbox {
	t.Helper()
	base := t.TempDir()
	t.Cleanup(func() { makeInstallerTreeRemovable(t, base) })
	s := installerSandbox{root: filepath.Join(base, "root"), home: filepath.Join(base, "home"), tmp: filepath.Join(base, "tmp"), tools: filepath.Join(base, "tools"), audit: filepath.Join(base, "effects"), downloads: filepath.Join(base, "downloads")}
	s.script = filepath.Join(repository(t), "scripts/install.sh")
	// Shell scanners cannot see Go overlays. Mutation runs use an on-disk copy
	// in a private directory, preserving the owned production installer bytes.
	// origin: hot-update S9 plan §9.3 / T54 / M27
	if mutant := os.Getenv("TETHER_S9_INSTALLER_MUTATION"); mutant != "" {
		if !filepath.IsAbs(mutant) || filepath.Clean(mutant) != mutant || filepath.Dir(mutant) == os.TempDir() {
			t.Fatal("invalid private installer mutation path")
		}
		info, err := os.Lstat(mutant)
		if err != nil || !info.Mode().IsRegular() {
			t.Fatal("installer mutation is not a regular private file")
		}
		s.script = mutant
	}
	for _, path := range []string{s.root, s.home, s.tmp, s.tools, s.downloads} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(s.audit, nil, 0600); err != nil {
		t.Fatal(err)
	}
	// These guards observe attempted process/kernel effects and always fail.
	// They never delegate to host binaries, including anything named tether.
	for _, name := range []string{"tether", "tether-infra-helper", "tether-infra-launch", "nats-server", "caddy", "nft", "iptables", "ip6tables", "systemctl", "service", "sudo", "useradd", "groupadd", "nk", "openssl", "ssh", "docker"} {
		writeExecutable(t, filepath.Join(s.tools, name), "#!/bin/sh\nprintf '%s\\n' '"+name+"' >> \"$PRIVILEGE_EFFECT_AUDIT\"\nexit 98\n")
	}
	writeExecutable(t, filepath.Join(s.tools, "curl"), `#!/bin/sh
output=; url=
while [ "$#" -gt 0 ]; do
    case "$1" in
        -o) output="$2"; shift 2 ;;
        --retry) shift 2 ;;
        -*) shift ;;
        *) url="$1"; shift ;;
    esac
done
[ -n "$output" ] && [ -n "$url" ] || exit 97
case "$output" in "$TMPDIR"/*) ;; *) exit 97 ;; esac
name=${url##*/}
case "$name" in *[!a-zA-Z0-9._-]*|'') exit 97 ;; esac
source=
while IFS="$(printf '\t')" read -r candidate file; do
    if [ "$candidate" = "$url" ]; then source="$file"; break; fi
done < "$PRIVILEGE_DOWNLOADS/url-index"
case "$source" in "$PRIVILEGE_DOWNLOADS"/*) ;; *) exit 97 ;; esac
[ -f "$source" ] || exit 97
printf 'download:%s\n' "$name" >> "$PRIVILEGE_EFFECT_AUDIT"
exec /bin/cp "$source" "$output"
`)
	return s
}

func writeExecutable(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
}

func (s installerSandbox) command(t *testing.T, role string, args ...string) *exec.Cmd {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), upgradebudget.ComponentHandover)
	t.Cleanup(cancel)
	argv := []string{s.script, "--role", role, "--version", "v0.0.0-privilege", "--source-base", "https://fixture.invalid", "--prefix", filepath.Join(s.root, "usr/local/bin")}
	switch role {
	case "broker":
		argv = append(argv, "--domain", "fixture.invalid", "--acme-email", "fixture@example.invalid", "--no-enable")
	case "agent":
		argv = append(argv, "--broker", "wss://fixture.invalid:443", "--session", "private-session", "--nid", "private-agent")
	case "ctl":
	default:
		t.Fatal("unknown installer role")
	}
	cmd := exec.CommandContext(ctx, "/bin/sh", append(argv, args...)...)
	cmd.Env = []string{"PATH=" + s.tools + ":/usr/bin:/bin", "HOME=" + s.home, "TMPDIR=" + s.tmp, "LANG=C", "TETHER_INSTALL_ROOT=" + s.root, "PRIVILEGE_EFFECT_AUDIT=" + s.audit, "PRIVILEGE_DOWNLOADS=" + s.downloads}
	return cmd
}

// origin: S9 internal review INFRA-SUITE-SUPPLIES F2 / F3; hot-update S9 plan §6.4 / T54 / M27 / M28
func TestInfraInstallerNeverStartsOrMutatesDryRun(t *testing.T) {
	// This lane needs no release supply. Directory creation, chmod, ctime or
	// any new host command makes it red, even if the installer exits zero.
	t.Run("dry-run-all-roots", func(t *testing.T) {
		for _, role := range []string{"ctl", "agent", "broker"} {
			t.Run(role, func(t *testing.T) {
				s := newInstallerSandbox(t)
				for _, mode := range []string{"fresh", "existing", "uninstall", "retrofit"} {
					if mode == "retrofit" && role != "broker" {
						continue
					}
					t.Run(mode, func(t *testing.T) {
						if mode == "existing" {
							seedExistingInstall(t, s, role)
						}
						beforeRoot, beforeHome, beforeTmp := treeManifest(t, s.root), treeManifest(t, s.home), treeManifest(t, s.tmp)
						args := []string{"--dry-run"}
						if mode == "uninstall" {
							args = append(args, "--uninstall")
						}
						if mode == "retrofit" {
							args = append(args, "--hot-update-retrofit")
						}
						output, rc := runCommand(t, s.command(t, role, args...))
						if rc != 0 {
							t.Fatalf("dry-run rc=%d: %s", rc, output)
						}
						assertManifestEqual(t, "install root", beforeRoot, treeManifest(t, s.root))
						assertManifestEqual(t, "private home", beforeHome, treeManifest(t, s.home))
						assertManifestEqual(t, "private tmp", beforeTmp, treeManifest(t, s.tmp))
						if len(readFile(t, s.audit)) != 0 {
							t.Fatal("dry-run executed a process/kernel/download effect")
						}
					})
				}
			})
		}
	})
	t.Run("verified-real-archive-all-roles", func(t *testing.T) {
		supply, root := installerSupply(t)
		current := supply.Releases[len(supply.Releases)-1]
		for _, role := range []string{"ctl", "agent", "broker"} {
			t.Run(role, func(t *testing.T) {
				s := newInstallerSandbox(t)
				supplyDownloads(t, s, supply, root)
				members := archiveMembers(t, safeSupplyPath(t, root, current.Archive.Path))
				output, rc := runCommand(t, s.command(t, role, "--version", current.Version))
				if rc != 0 {
					t.Fatalf("real-archive install rc=%d: %s", rc, output)
				}
				audit := string(readFile(t, s.audit))
				assertOnlyDownloads(t, audit)
				if !strings.Contains(audit, "download:"+filepath.Base(current.Archive.Path)) {
					t.Fatal("real archive branch was not exercised")
				}
				binary := filepath.Join(s.root, "usr/local/bin/tether")
				if !bytes.Equal(readFile(t, binary), members["tether"]) {
					t.Fatal("installed tether bytes differ from verified archive")
				}
				if role == "broker" {
					for _, name := range []string{"tether-infra-helper", "tether-infra-launch"} {
						path := filepath.Join(s.root, "usr/local/libexec", name)
						if !bytes.Equal(readFile(t, path), members[name]) {
							t.Fatal("installed support bytes differ from release")
						}
						info, err := os.Stat(path)
						if err != nil || info.Mode().Perm() != 0755 {
							t.Fatal("support executable permissions incorrect")
						}
					}
					for _, relative := range []string{"etc/systemd/system/tether-infra-helper.socket", "etc/systemd/system/tether-infra-helper@.service"} {
						if len(readFile(t, filepath.Join(s.root, relative))) == 0 {
							t.Fatal("missing management unit")
						}
					}
					// Installation alone must not produce root policy, credentials,
					// a live socket or a journal claiming runtime readiness.
					for path, row := range treeManifest(t, s.root) {
						if row.Mode&syscall.S_IFMT == syscall.S_IFSOCK || strings.HasSuffix(path, ".nk") || strings.HasSuffix(path, "journal.json") {
							t.Fatal("fresh installer activated infrastructure management")
						}
					}
				}
			})
		}
	})
	t.Run("skip-download-is-still-validation", func(t *testing.T) {
		s := newInstallerSandbox(t)
		before := treeManifest(t, s.root)
		_, rc := runCommand(t, s.command(t, "broker", "--skip-download"))
		if rc == 0 {
			t.Fatal("missing helper/launcher accepted by skip-download")
		}
		if !reflect.DeepEqual(before, treeManifest(t, s.root)) {
			t.Fatal("invalid skip-download install changed selectors/files")
		}
		if len(readFile(t, s.audit)) != 0 {
			t.Fatal("skip-download invoked a supporting process")
		}
	})
}

func seedExistingInstall(t *testing.T, s installerSandbox, role string) {
	t.Helper()
	paths := []string{filepath.Join(s.root, "usr/local/bin/tether"), filepath.Join(s.home, ".zshrc")}
	if role == "agent" {
		paths = append(paths, filepath.Join(s.home, ".tether/agent/private-session/agent.yaml"), filepath.Join(s.home, ".config/systemd/user/tether-agent-private-session.service"))
	}
	if role == "broker" {
		for _, relative := range []string{"etc/tether/broker.yaml", "etc/tether/Caddyfile", "etc/tether/nats.d/nats.conf", "etc/systemd/system/tether-broker.service", "etc/systemd/system/nats-server.service", "etc/systemd/system/caddy.service", "etc/systemd/system/tether-infra-helper.socket", "usr/local/libexec/tether-infra-helper", "usr/local/libexec/tether-infra-launch"} {
			paths = append(paths, filepath.Join(s.root, relative))
		}
	}
	for _, path := range paths {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("operator-owned original content"), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func supplyDownloads(t *testing.T, s installerSandbox, supply privilegeSupply, root string) {
	t.Helper()
	files := append([]downloadSupply(nil), supply.Downloads...)
	release := supply.Releases[len(supply.Releases)-1]
	files = append(files, downloadSupply{"https://fixture.invalid/" + filepath.Base(release.Archive.Path), release.Archive}, downloadSupply{"https://fixture.invalid/SHA256SUMS", release.Checksums})
	var index strings.Builder
	seen := map[string]bool{}
	for _, row := range files {
		file := row.File
		verifySuppliedFile(t, root, file)
		if seen[row.URL] || strings.ContainsAny(row.URL, "\t\r\n") {
			t.Fatal("ambiguous download URL")
		}
		seen[row.URL] = true
		if strings.HasSuffix(file.Path, ".tar.gz") || strings.HasSuffix(file.Path, ".tgz") {
			archiveMembers(t, safeSupplyPath(t, root, file.Path))
		}
		dest := filepath.Join(s.downloads, file.SHA256+".blob")
		if err := os.WriteFile(dest, readFile(t, safeSupplyPath(t, root, file.Path)), 0600); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&index, "%s\t%s\n", row.URL, dest)
	}
	if err := os.WriteFile(filepath.Join(s.downloads, "url-index"), []byte(index.String()), 0600); err != nil {
		t.Fatal(err)
	}
}

func assertOnlyDownloads(t *testing.T, audit string) {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(audit), "\n") {
		if line != "" && !strings.HasPrefix(line, "download:") {
			t.Fatal("installer attempted a process/kernel effect")
		}
	}
}

// origin: hot-update S9 plan §6.4 / T73 / M27
func TestInfraActiveInstallerInterlock(t *testing.T) {
	supply, supplyRoot := loadSupply(t)
	exercised := 0
	runInstalledScenario(t, "active-installer", func(event installedEvent) {
		if event.Name != "busy-ready" {
			t.Fatal("unexpected active installer event")
		}
		exercised++
		assertRunning(t, event.Before)
		s := newInstallerSandbox(t)
		s.root = event.sandbox
		supplyDownloads(t, s, supply, supplyRoot)
		before := treeManifest(t, s.root)
		var original infrahost.JournalImage
		if err := strictJSON(readFile(t, safeSupplyPath(t, s.root, event.Journal)), &original); err != nil || len(original.Events) == 0 {
			t.Fatal("invalid busy-domain baseline")
		}
		originalState := original.Events[len(original.Events)-1].State
		for _, args := range [][]string{nil, {"--force-config"}, {"--hot-update-retrofit"}, {"--uninstall"}} {
			_, rc := runCommand(t, s.command(t, "broker", append([]string{"--version", supply.Releases[1].Version}, args...)...))
			if len(args) > 0 && args[0] == "--uninstall" && rc == 0 {
				t.Fatal("busy uninstall succeeded")
			}
			assertRunning(t, event.Before)
			after := treeManifest(t, s.root)
			for path, row := range before {
				if strings.Contains(path, "/staging/") || strings.HasSuffix(path, "journal.json") || path == "." {
					continue
				}
				observed := after[path]
				if row.Mode&syscall.S_IFMT == syscall.S_IFDIR {
					observed.MTime, observed.CTime, observed.Size = row.MTime, row.CTime, row.Size
				}
				if observed != row {
					t.Fatalf("active installer changed protected path %s", path)
				}
			}
			var journal infrahost.JournalImage
			if err := strictJSON(readFile(t, safeSupplyPath(t, s.root, event.Journal)), &journal); err != nil || len(journal.Events) == 0 {
				t.Fatal("installer damaged active journal")
			}
			state := journal.Events[len(journal.Events)-1].State
			for kind := c.NATSBinary; kind <= c.LauncherBinary; kind++ {
				state.Slots[kind].Pending = originalState.Slots[kind].Pending
			}
			if state != originalState {
				t.Fatal("active reinstall took the busy domain or changed selectors")
			}
			assertOnlyDownloads(t, string(readFile(t, s.audit)))
		}
		// Operator files are outside ownership markers and permanent locks are
		// never unlinked, including when uninstall is denied before effect.
		for _, relative := range []string{"etc/systemd/system/operator.service", "var/lib/tether-infra/journal.lock"} {
			if _, ok := before[relative]; !ok {
				t.Fatal("active fixture omitted operator/permanent-lock control")
			}
		}
	})
	if exercised != 1 {
		t.Fatal("active reinstall did not run on one original live fixture")
	}
}

func cleanEnvironment(private string) []string {
	// Explicit allowlist excludes inherited tether handoff/authority/agent keys.
	return []string{"PATH=/usr/bin:/bin", "HOME=" + private, "TMPDIR=" + private, "LANG=C", "TETHER_INSTALL_ROOT=" + private}
}

func cleanCommand(t *testing.T, name string, args ...string) *exec.Cmd {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), upgradebudget.ComponentHandover)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = cleanEnvironment(t.TempDir())
	return cmd
}

func runCommand(t *testing.T, cmd *exec.Cmd) ([]byte, int) {
	t.Helper()
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	process, err := infraharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	err = process.Wait()
	if os.Getenv("TETHER_S9_INSTALLER_MUTATION") != "" {
		for _, line := range strings.Split(output.String(), "\n") {
			if strings.HasPrefix(line, "S9_P31_MUTATION=") {
				t.Log(line)
			}
		}
	}
	if err == nil {
		return output.Bytes(), 0
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatal(err)
	}
	return output.Bytes(), exit.ExitCode()
}

func strictJSON(data []byte, dst any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	// Canonical re-encoding also rejects duplicate keys and alternate spellings.
	canonical, err := json.Marshal(dst)
	if err != nil || !bytes.Equal(canonical, data) {
		return errors.New("noncanonical JSON")
	}
	return nil
}

func safeSupplyPath(t *testing.T, root, relative string) string {
	t.Helper()
	if relative == "" || filepath.IsAbs(relative) || filepath.Clean(relative) != relative || relative == "." || relative == ".." || strings.HasPrefix(relative, "../") {
		t.Fatal("SETUP-RED: supply path escapes private root")
	}
	path := filepath.Join(root, relative)
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		t.Fatal("SETUP-RED: supply contains a symlink or missing path")
	}
	return path
}

func parseSHA(t *testing.T, value string) c.Digest {
	t.Helper()
	b, err := hex.DecodeString(value)
	if err != nil || len(b) != 32 || strings.ToLower(value) != value {
		t.Fatal("SETUP-RED: invalid SHA256")
	}
	var result c.Digest
	copy(result[:], b)
	if result == (c.Digest{}) {
		t.Fatal("SETUP-RED: empty SHA256")
	}
	return result
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("fixture file unavailable: %v", err)
	}
	return data
}

func verifySuppliedFile(t *testing.T, root string, file suppliedFile) {
	t.Helper()
	path := safeSupplyPath(t, root, file.Path)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() == 0 || info.Size() > 1<<30 {
		t.Fatal("SETUP-RED: invalid supplied regular file")
	}
	if sha256.Sum256(readFile(t, path)) != parseSHA(t, file.SHA256) {
		t.Fatal("SETUP-RED: supplied bytes differ from manifest")
	}
}

func copyTree(t *testing.T, source, target string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		dest := filepath.Join(target, relative)
		if entry.IsDir() {
			return os.MkdirAll(dest, info.Mode().Perm())
		}
		if !info.Mode().IsRegular() {
			return errors.New("fixture contains unsupported file type")
		}
		return os.WriteFile(dest, readFile(t, path), info.Mode().Perm())
	})
	if err != nil {
		t.Fatal(err)
	}
}
