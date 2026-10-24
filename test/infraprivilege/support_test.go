//go:build linux && amd64 && infraprivilege_integration

package infraprivilege

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/infraartifact"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/test/infraharness"
)

// This is a test supply interface, not a product wire/CLI extension. P26 supplies
// archives, sources and inner race binaries; P40 supplies the nonroot syscall
// fixture executable. Missing supply is setup-red. Neither test builds/downloads
// a historical release, invokes a host root command, or replaces a failed leaf.
// Paths in the JSON are relative to its private parent and may not escape it.
type privilegeSupply struct {
	Version        int
	Downloads      []downloadSupply
	Releases       []releaseSupply
	Runner         suppliedFile
	RunnerSource   suppliedFile
	Installer      suppliedFile
	ImageContext   string
	ImageRoot      string
	ImageReceipt   suppliedFile
	SupportCatalog suppliedFile
}
type suppliedFile struct{ Path, SHA256 string }
type downloadSupply struct {
	URL  string
	File suppliedFile
}
type releaseSupply struct {
	Version                    string
	Archive, Checksums, Source suppliedFile
	RaceArchive                suppliedFile
	Binaries                   []supportBinary
}
type supportBinary struct {
	Name   string
	SHA256 string
	Race   suppliedFile
}

func init() {
	installedAuthorityCheck = func(t *testing.T) { runInstalledScenario(t, "authority", nil) }
	installedRotationCheck = func(t *testing.T) { runInstalledScenario(t, "rotation", nil) }
}

func repository(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("repository source location unavailable")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}

func loadSupply(t *testing.T) (privilegeSupply, string) {
	t.Helper()
	path := os.Getenv("TETHER_S9_PRIVILEGE_SUPPLY")
	if path == "" {
		t.Fatal("SETUP-RED: NOT_RUN pending P20/P25/P26/P40: TETHER_S9_PRIVILEGE_SUPPLY must name the pre-supplied private manifest")
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		t.Fatal("SETUP-RED: supply path must be canonical and absolute")
	}
	root := filepath.Dir(path)
	if root == "/" || root == os.TempDir() {
		t.Fatal("SETUP-RED: supply requires a private directory")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("SETUP-RED: supply manifest unavailable")
	}
	var supply privilegeSupply
	if err := strictJSON(data, &supply); err != nil || supply.Version != 1 || len(supply.Releases) != 2 {
		t.Fatal("SETUP-RED: invalid adjacent-release supply")
	}
	for _, file := range []suppliedFile{supply.Runner, supply.RunnerSource, supply.Installer, supply.ImageReceipt, supply.SupportCatalog} {
		verifySuppliedFile(t, root, file)
	}
	if supply.Releases[0].Version == supply.Releases[1].Version || supply.Releases[0].Source.SHA256 == supply.Releases[1].Source.SHA256 {
		t.Fatal("SETUP-RED: adjacent support supply is a same-source relabel")
	}
	for _, release := range supply.Releases {
		for _, file := range []suppliedFile{release.Archive, release.Checksums, release.Source, release.RaceArchive} {
			verifySuppliedFile(t, root, file)
		}
		seen := map[string]bool{}
		for _, binary := range release.Binaries {
			if seen[binary.Name] {
				t.Fatal("SETUP-RED: duplicate support binary")
			}
			seen[binary.Name] = true
			if binary.Name == "tether" {
				continue
			}
			if binary.Name != "tether-infra-helper" && binary.Name != "tether-infra-launch" {
				t.Fatal("SETUP-RED: unknown support binary")
			}
			verifySuppliedFile(t, root, binary.Race)
			path := safeSupplyPath(t, root, binary.Race.Path)
			if err := infraharness.VerifyArtifact(root, path, parseSHA(t, binary.Race.SHA256), true); err != nil {
				t.Fatal(err)
			}
			info, err := buildinfo.ReadFile(path)
			if err != nil || info.Path != "github.com/LinZiyang666/tether/cmd/"+binary.Name {
				t.Fatal("SETUP-RED: race support is not the product entry")
			}
		}
		if len(seen) != 3 || !seen["tether"] || !seen["tether-infra-helper"] || !seen["tether-infra-launch"] {
			t.Fatal("SETUP-RED: incomplete release executables")
		}
	}
	if err := infraharness.VerifyArtifact(root, safeSupplyPath(t, root, supply.Runner.Path), parseSHA(t, supply.Runner.SHA256), true); err != nil {
		t.Fatalf("SETUP-RED: syscall fixture must be an inner race binary: %v", err)
	}
	if !bytes.Equal(readFile(t, safeSupplyPath(t, root, supply.Installer.Path)), readFile(t, filepath.Join(repository(t), "scripts/install.sh"))) {
		t.Fatal("SETUP-RED: supplied installer differs from this candidate")
	}
	return supply, root
}

// origin: hot-update S9 plan §6.5 / §9.4 / T55 / M28
func TestInfraReleaseContainsSupportingBinaries(t *testing.T) {
	supply, root := loadSupply(t)
	if supply.Releases[0].Version == supply.Releases[1].Version || supply.Releases[0].Source.SHA256 == supply.Releases[1].Source.SHA256 {
		t.Fatal("previous support release is a same-source relabel")
	}
	previous := make(map[string]string)
	for _, release := range supply.Releases {
		for _, file := range []suppliedFile{release.Archive, release.Checksums, release.Source} {
			verifySuppliedFile(t, root, file)
		}
		members := archiveMembers(t, safeSupplyPath(t, root, release.Archive.Path))
		checksumRows := strings.Fields(string(readFile(t, safeSupplyPath(t, root, release.Checksums.Path))))
		matches := 0
		if len(checksumRows)%2 != 0 {
			t.Fatal("malformed release checksum list")
		}
		for i := 0; i < len(checksumRows); i += 2 {
			if strings.TrimPrefix(checksumRows[i+1], "*") == filepath.Base(release.Archive.Path) {
				matches++
				if checksumRows[i] != release.Archive.SHA256 {
					t.Fatal("release checksum does not bind archive")
				}
			}
		}
		if matches != 1 {
			t.Fatal("archive requires exactly one official checksum row")
		}
		if len(release.Binaries) != 3 {
			t.Fatal("release must supply tether, helper and launcher")
		}
		seen := make(map[string]bool)
		for _, binary := range release.Binaries {
			if seen[binary.Name] || (binary.Name != "tether" && binary.Name != "tether-infra-helper" && binary.Name != "tether-infra-launch") {
				t.Fatal("invalid supporting executable set")
			}
			seen[binary.Name] = true
			data, found := members[binary.Name]
			if !found || sha256.Sum256(data) != parseSHA(t, binary.SHA256) {
				t.Fatal("archive omitted or relabeled supporting executable")
			}
			if binary.Name == "tether" {
				continue
			} // Installed tether is never executed.
			if old, found := previous[binary.Name]; found && old == binary.SHA256 {
				t.Fatal("adjacent supporting releases contain identical executable bytes")
			}
			previous[binary.Name] = binary.SHA256
			verifySuppliedFile(t, root, binary.Race)
			path := safeSupplyPath(t, root, binary.Race.Path)
			if err := infraharness.VerifyArtifact(root, path, parseSHA(t, binary.Race.SHA256), true); err != nil {
				t.Fatal(err)
			}
			// The race build must retain the production executable's package path;
			// a test fixture with a bumped version string cannot fill this slot.
			info, err := buildinfo.ReadFile(path)
			if err != nil || info.Path != "github.com/LinZiyang666/tether/cmd/"+binary.Name {
				t.Fatal("inner race binary is not the supporting product entry")
			}
		}
	}
	// Running identity is checked during the installed six-hop scenario, while
	// the workers are alive; exit/version output cannot sign an old worker's exit.
	runInstalledScenario(t, "release-runtime", nil)
}

func archiveMembers(t *testing.T, path string) map[string][]byte {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	z, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = z.Close() }()
	reader := tar.NewReader(z)
	members := make(map[string][]byte)
	total := int64(0)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		name := strings.TrimSuffix(header.Name, "/")
		if name == "" || filepath.IsAbs(name) || filepath.Clean(name) != name || name == ".." || strings.HasPrefix(name, "../") || strings.Contains(name, "\\") {
			t.Fatal("unsafe release archive path")
		}
		if _, exists := members[name]; exists {
			t.Fatal("duplicate archive member")
		}
		if header.Typeflag == tar.TypeDir {
			members[name] = nil
			continue
		}
		if header.Typeflag != tar.TypeReg || header.Size < 0 || header.Size > 512<<20 {
			t.Fatal("archive contains a link, device or oversized member")
		}
		total += header.Size
		if total > 1<<30 {
			t.Fatal("release archive exceeds aggregate budget")
		}
		data, err := io.ReadAll(io.LimitReader(reader, header.Size+1))
		if err != nil || int64(len(data)) != header.Size {
			t.Fatal("truncated archive")
		}
		members[name] = data
		if name == "tether" || name == "tether-infra-helper" || name == "tether-infra-launch" {
			if header.Mode&0777 != 0755 || len(data) < 20 || !bytes.Equal(data[:4], []byte{0x7f, 'E', 'L', 'F'}) || data[4] != 2 || data[5] != 1 || data[18] != 62 || data[19] != 0 {
				t.Fatal("support archive requires executable linux/amd64 ELF members")
			}
		}
	}
	return members
}

// origin: hot-update S9 plan §9.4 / T56 / M28
func TestInfraImageGuardChecksEveryCopiedInput(t *testing.T) {
	supply, root := loadSupply(t)
	contextRoot := safeSupplyPath(t, root, supply.ImageContext)
	inputs := copiedInputs(t, contextRoot)
	if len(inputs) == 0 {
		t.Fatal("empty COPY set cannot prove an image guard")
	}
	guard := filepath.Join(repository(t), "test/simcluster/lib/infra-image-manifest.py")
	if _, err := os.Stat(guard); err != nil {
		t.Fatal("SETUP-RED: NOT_RUN pending P26: complete image guard missing")
	}
	imageRoot := safeSupplyPath(t, root, supply.ImageRoot)
	receipt := safeSupplyPath(t, root, supply.ImageReceipt.Path)
	check := func(t *testing.T, directory string, expected int) {
		output, rc := runImageSupplyGuard(t, directory, receipt, imageRoot)
		if rc != expected {
			t.Fatalf("image guard rc=%d want=%d: %s", rc, expected, output)
		}
	}
	check(t, contextRoot, 0)
	// Every mutation is on disk; overlay cannot change a source-scanning guard.
	for _, input := range inputs {
		for _, defect := range []string{"bytes", "mode", "missing", "symlink"} {
			t.Run(input+"/"+defect, func(t *testing.T) {
				copyRoot := filepath.Join(t.TempDir(), "context")
				copyTree(t, contextRoot, copyRoot)
				path := filepath.Join(copyRoot, input)
				switch defect {
				case "bytes":
					f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
					if err != nil {
						t.Fatal(err)
					}
					_, err = f.Write([]byte("stale-input"))
					closeErr := f.Close()
					if err != nil || closeErr != nil {
						t.Fatal("mutation write failed")
					}
				case "mode":
					info, err := os.Stat(path)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.Chmod(path, info.Mode().Perm()^0100); err != nil {
						t.Fatal(err)
					}
				case "missing":
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
				case "symlink":
					data := readFile(t, path)
					alternate := filepath.Join(t.TempDir(), "same-bytes")
					if err := os.WriteFile(alternate, data, 0600); err != nil {
						t.Fatal(err)
					}
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink(alternate, path); err != nil {
						t.Fatal(err)
					}
				}
				check(t, copyRoot, 1)
			})
		}
	}
	check(t, contextRoot, 0)
}

func copiedInputs(t *testing.T, root string) []string {
	t.Helper()
	set := map[string]bool{"Dockerfile": true}
	scanner := bufio.NewScanner(bytes.NewReader(readFile(t, filepath.Join(root, "Dockerfile"))))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		fields := strings.Fields(line)
		if len(fields) == 0 || !strings.EqualFold(fields[0], "COPY") {
			continue
		}
		if len(fields) < 3 || strings.ContainsAny(line, "*?[$\\") || strings.HasPrefix(fields[1], "--") {
			t.Fatal("COPY syntax requires explicit independent parser support")
		}
		for _, source := range fields[1 : len(fields)-1] {
			path := safeSupplyPath(t, root, source)
			err := filepath.WalkDir(path, func(p string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() {
					return nil
				}
				if !entry.Type().IsRegular() {
					return errors.New("COPY includes nonregular input")
				}
				relative, err := filepath.Rel(root, p)
				if err == nil {
					set[relative] = true
				}
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if scanner.Err() != nil {
		t.Fatal(scanner.Err())
	}
	result := make([]string, 0, len(set))
	for path := range set {
		result = append(result, path)
	}
	sort.Strings(result)
	return result
}

// Scenario events are acknowledged only after independent procfs/file reads.
// The P40 fixture must keep all named current/before workers alive at the event
// barrier. It drives actual entries on a private syscall backend, never host
// systemd/nft. Frozen Request/Receipt/Status DTOs retain their product meanings.
type installedEvent struct {
	Name                      string
	Step                      uint32
	Kind                      c.ArtifactKind
	Request                   c.HostRequest
	Receipt                   c.HostReceipt
	Refusal                   *infrapriv.Refusal
	Status                    c.Status
	Before, Current           c.Process
	Journal                   string
	Active, Previous, Pending c.Artifact
	PublicOutput              string
	Policy                    string
	sandbox                   string
}

// origin: hot-update S9 plan §6.5 / T74 / M06 / M08 / M22 / M28
func TestInfraSupportingRuntimeCycle(t *testing.T) {
	loadSupply(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSupportingRuntimeCycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "supporting runtime", g)
	testharness.AssertNoFDLeak(t, "supporting runtime", fd, 0)
}

func exerciseSupportingRuntimeCycle(t *testing.T) {
	t.Helper()
	supply, _ := loadSupply(t)
	completed := map[c.ArtifactKind]int{}
	before := map[c.ArtifactKind]c.Artifact{}
	oldWorker := map[c.ArtifactKind]c.Process{}
	phase := map[c.ArtifactKind]int{}
	launcher := c.Process{}
	launched := 0
	staged, pending, third := 0, 0, 0
	runInstalledScenario(t, "support-cycle", func(event installedEvent) {
		if event.Kind != c.HelperBinary && event.Kind != c.LauncherBinary {
			t.Fatal("unexpected support kind")
		}
		switch event.Name {
		case "support-staged":
			if phase[event.Kind] != 0 || completed[event.Kind] >= 3 {
				t.Fatal("support stage did not follow prior retirement")
			}
			phase[event.Kind] = 1
			staged++
			if event.Pending != (event.Request.Candidate) || event.Active == event.Pending || event.Receipt.Op != c.Stage {
				t.Fatal("busy support stage changed active selection")
			}
			before[event.Kind] = event.Active
			wanted := supportTarget(t, supply, event.Kind, completed[event.Kind])
			if event.Pending != wanted {
				t.Fatal("support cycle did not select upgrade-rollback-upgrade artifacts")
			}
		case "third-generation-rejected":
			if phase[event.Kind] != 1 {
				t.Fatal("third-generation control is out of order")
			}
			phase[event.Kind] = 2
			third++
			if event.Refusal == nil || event.Refusal.Code != "infra_busy" || event.Active != before[event.Kind] {
				t.Fatal("third generation evicted current/rollback slots")
			}
		case "support-switch-pending":
			if phase[event.Kind] != 2 {
				t.Fatal("support switched before stage/third-generation controls")
			}
			phase[event.Kind] = 3
			oldWorker[event.Kind] = event.Before
			pending++
			if event.Receipt.Op != c.UpdateSupport || event.Status.Phase != c.Retiring || event.Receipt.Code != "infra_recovery_required" || event.Previous != before[event.Kind] || event.Active != event.Request.Candidate {
				t.Fatal("switching worker claimed its own retirement")
			}
			assertRunning(t, event.Before)
		case "launcher-before-exec":
			if event.Kind != c.LauncherBinary || phase[event.Kind] != 3 || event.Current.ExeSHA256 != event.Active.BinarySHA256 {
				t.Fatal("launcher image was not observed before exec")
			}
			phase[event.Kind] = 4
			assertRunning(t, event.Current)
			launcher = event.Current
		case "launcher-exec":
			if event.Kind != c.LauncherBinary || phase[event.Kind] != 4 || event.Receipt.Op != c.LaunchAck || event.Current.PID != launcher.PID || event.Current.StartTicks != launcher.StartTicks || event.Current.ExeSHA256 == launcher.ExeSHA256 {
				t.Fatal("launcher did not exec in place")
			}
			phase[event.Kind] = 5
			launched++
			catalog, err := infraartifact.New(infraartifact.Config{})
			if err != nil {
				t.Fatal(err)
			}
			stock, err := catalog.Resolve(c.NATSBinary, "2.14.6", "linux", "amd64")
			if err != nil || event.Current.ExeSHA256 != stock.BinarySHA256 {
				t.Fatal("launcher did not enter verified stock NATS")
			}
			assertRunning(t, event.Current)
			assertInheritedStoreLock(t, event.Current, event.Request.Store)
		case "support-completed":
			wantedPhase := 3
			if event.Kind == c.LauncherBinary {
				wantedPhase = 5
			}
			if phase[event.Kind] != wantedPhase || event.Before != oldWorker[event.Kind] {
				t.Fatal("retirement did not bind the switching worker and launcher proof")
			}
			phase[event.Kind] = 0
			completed[event.Kind]++
			if event.Status.Phase != c.Completed || event.Receipt.Op != c.RecoverHost || event.Current != event.Status.Current || !event.Receipt.Census.Complete || len(event.Receipt.Census.Processes) != 1 || event.Receipt.Census.OldOwnerPending != 0 {
				t.Fatal("support completed before new-worker retirement proof")
			}
			assertExited(t, event.Before)
			assertRunning(t, event.Current)
		default:
			t.Fatalf("unexpected support event %q", event.Name)
		}
	})
	if completed[c.HelperBinary] != 3 || completed[c.LauncherBinary] != 3 || staged != 6 || pending != 6 || third != 6 || launched != 3 {
		t.Fatal("incomplete helper/launcher upgrade-rollback-upgrade cycle")
	}
}

func supportTarget(t *testing.T, supply privilegeSupply, kind c.ArtifactKind, hop int) c.Artifact {
	t.Helper()
	release := supply.Releases[1]
	if hop == 1 {
		release = supply.Releases[0]
	}
	name := "tether-infra-helper"
	if kind == c.LauncherBinary {
		name = "tether-infra-launch"
	}
	for _, binary := range release.Binaries {
		if binary.Name == name {
			return c.Artifact{Kind: kind, Version: release.Version, GOOS: "linux", GOARCH: "amd64", ArchiveSHA256: parseSHA(t, release.RaceArchive.SHA256), BinarySHA256: parseSHA(t, binary.Race.SHA256), CatalogSHA256: parseSHA(t, supply.SupportCatalog.SHA256)}
		}
	}
	t.Fatal("missing support target")
	return c.Artifact{}
}

func assertRunning(t *testing.T, expected c.Process) {
	t.Helper()
	actual, err := infrahost.ReadProcess(expected.PID, deadline(t))
	if err != nil || actual != expected {
		t.Fatalf("running incarnation differs from receipt: %v", err)
	}
}

func assertExited(t *testing.T, before c.Process) {
	t.Helper()
	id := infraharness.Identity{BootID: before.BootID, PID: int(before.PID), StartTicks: before.StartTicks}
	alive, err := infraharness.ProcessExists(id)
	if err != nil || alive {
		t.Fatalf("old supporting worker remains alive/unreaped: %v", err)
	}
}

func runInstalledScenario(t *testing.T, scenario string, check func(installedEvent)) {
	t.Helper()
	supply, root := loadSupply(t)
	sandbox := t.TempDir()
	var canary []byte
	checkCanaries := func() {}
	if scenario == "authority" || scenario == "rotation" {
		var closeCanaries func()
		canary, checkCanaries, closeCanaries = watchBusinessCanaries(t, sandbox)
		defer closeCanaries()
	}
	ctx, cancel := context.WithTimeout(t.Context(), upgradebudget.ComponentHandover)
	defer cancel()
	cmd := exec.CommandContext(ctx, safeSupplyPath(t, root, supply.Runner.Path), "--scenario", scenario, "--root", sandbox, "--supply", filepath.Join(root, filepath.Base(os.Getenv("TETHER_S9_PRIVILEGE_SUPPLY"))))
	cmd.Env = cleanEnvironment(sandbox)
	inRead, inWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inRead.Close(); _ = inWrite.Close() }()
	outRead, outWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = outRead.Close(); _ = outWrite.Close() }()
	cmd.Stdin, cmd.Stdout = inRead, outWrite
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	process, err := infraharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = process.Stop() }()
	_ = inRead.Close()
	_ = outWrite.Close()
	events := 0
	names := map[string]int{}
	scanner := bufio.NewScanner(outRead)
	scanner.Buffer(make([]byte, 4096), c.MaxCensusBytes)
	for scanner.Scan() {
		var event installedEvent
		if err := strictJSON(scanner.Bytes(), &event); err != nil {
			t.Fatal("invalid installed-entry event")
		}
		events++
		names[event.Name]++
		event.sandbox = sandbox
		if event.Step == 0 || event.Step != event.Request.Step {
			t.Fatal("event is not bound to one host effect")
		}
		requestBytes, err := c.EncodeRequest(event.Request)
		if err != nil || len(event.Request.Signature) != 64 {
			t.Fatal("receipt is not bound to canonical request")
		}
		if event.Refusal != nil {
			refused := event.Refusal
			if !reflect.DeepEqual(event.Receipt, c.HostReceipt{}) || refused.Version != 1 || refused.Tx != event.Request.Tx || refused.Sequence != event.Request.Sequence || refused.RequestDigest != sha256.Sum256(requestBytes) {
				t.Fatal("before-effect refusal was fabricated as a journal receipt")
			}
		} else if sha256.Sum256(requestBytes) != event.Receipt.RequestDigest || event.Status.Step != event.Step || event.Receipt.Tx != event.Request.Tx || event.Status.Tx != event.Request.Tx || event.Receipt.Op != event.Request.Op {
			t.Fatal("host receipt/status are not bound to the actual request")
		} else if _, err := c.EncodeReceipt(event.Receipt); err != nil {
			t.Fatal("invalid host receipt")
		}
		if event.Receipt.Applied {
			journalPath := safeSupplyPath(t, sandbox, event.Journal)
			var journal infrahost.JournalImage
			if err := strictJSON(readFile(t, journalPath), &journal); err != nil || len(journal.Events) == 0 {
				t.Fatal("invalid actual root journal")
			}
			last := journal.Events[len(journal.Events)-1]
			if last.Digest != event.Receipt.Journal || !reflect.DeepEqual(last.Receipt, event.Receipt) {
				t.Fatal("root/status journal mismatch")
			}
		}
		if check != nil {
			check(event)
		} else {
			checkAuthorityEvent(t, scenario, event)
		}
		if len(canary) != 0 {
			assertNoCanary(t, string(scanner.Bytes()), canary)
			checkCanaries()
		}
		if _, err := io.WriteString(inWrite, "continue\n"); err != nil {
			t.Fatal(err)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if err := process.Wait(); err != nil {
		t.Fatalf("installed scenario failed: %v", err)
	}
	if events == 0 {
		t.Fatal("empty installed scenario cannot prove a boundary")
	}
	if scenario == "authority" || scenario == "rotation" {
		requireAuthorityEvents(t, scenario, names)
		assertNoCanary(t, stderr.String(), canary)
		checkCanaries()
	}
	if stderr.Len() != 0 {
		t.Fatal("installed scenario leaked unexpected diagnostic output")
	}
}

func checkAuthorityEvent(t *testing.T, scenario string, event installedEvent) {
	t.Helper()
	if scenario == "release-runtime" {
		assertRunning(t, event.Current)
		return
	}
	if strings.HasSuffix(event.Name, "-denied") || strings.HasSuffix(event.Name, "-revoked") || event.Name == "pending-needs-reauthorization" {
		if event.Refusal == nil || (event.Refusal.Code != "infra_permission_denied" && event.Refusal.Code != "infra_policy_stale" && event.Refusal.Code != "infra_busy" && event.Refusal.Code != "infra_config_invalid") {
			t.Fatal("unauthorized installed request applied an effect")
		}
		return
	}
	if event.Name != "authorized-service" && event.Name != "authorized-root" && event.Name != "authorized-query" && event.Name != "policy-rotated" && event.Name != "reauthorized" {
		t.Fatal("unexpected authority event")
	}
	if !event.Receipt.Applied {
		t.Fatal("authorized installed request did not apply")
	}
	policy, err := infrapriv.DecodePolicy(readFile(t, safeSupplyPath(t, event.sandbox, event.Policy)))
	if err != nil || (event.Name == "policy-rotated" && (event.Request.Op != c.RotatePolicy || policy.Revision <= event.Request.PolicyRevision)) {
		t.Fatal("rotation did not durably replace the authority epoch")
	}
}

func requireAuthorityEvents(t *testing.T, scenario string, names map[string]int) {
	t.Helper()
	required := []string{"authorized-service", "authorized-root", "ordinary-uid-denied", "same-uid-launch-denied", "same-uid-catalog-denied", "legacy-admin-denied", "unix-admin-denied", "unit-capabilities-denied", "unit-prefix-denied", "unit-environment-denied", "caller-fd-replaced-denied", "acme-before-exit-denied", "sys-read-stepdown-denied", "authority-crossboot-denied"}
	if scenario == "rotation" {
		required = []string{"policy-rotated", "signed-unsent-revoked", "lost-reply-revoked", "old-key-new-revision-denied", "rotation-busy-denied", "partial-policy-denied", "pending-needs-reauthorization", "authorized-query", "reauthorized"}
	}
	if len(names) != len(required) {
		t.Fatal("authority scenario omitted or added a required cell")
	}
	for _, name := range required {
		if names[name] != 1 {
			t.Fatalf("authority scenario requires exactly one %s cell", name)
		}
	}
}
