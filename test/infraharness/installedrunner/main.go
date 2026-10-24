// installedrunner checks the actual installed-entry prerequisites in a private
// unprivileged tree. It never runs host services or substitutes an authority,
// cgroup or conntrack double for the installed-event success protocol.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F4 / hot-update S9 plan §6
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

type suppliedFile struct{ Path, SHA256 string }
type binary struct {
	Name, SHA256 string
	Race         suppliedFile
}
type release struct {
	Version                                 string
	Archive, Checksums, Source, RaceArchive suppliedFile
	Binaries                                []binary
}
type manifest struct {
	Version   int
	Downloads []struct {
		URL  string
		File suppliedFile
	}
	Releases       []release
	Runner         suppliedFile
	RunnerSource   suppliedFile
	Installer      suppliedFile
	ImageContext   string
	ImageRoot      string
	ImageReceipt   suppliedFile
	SupportCatalog suppliedFile
}
type entryCheck struct {
	Name, SHA256, Command, OutputSHA256 string
	NativeRC                            int
}

func main() { os.Exit(run(os.Args[1:], os.Stderr)) }

func run(args []string, diagnostic io.Writer) int {
	if err := execute(args); err != nil {
		_, _ = fmt.Fprintln(diagnostic, "SETUP-RED:", err)
		return 2
	}
	return 0
}

func execute(args []string) error {
	flags := flag.NewFlagSet("installed-runner", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	scenario, root, path := flags.String("scenario", "", ""), flags.String("root", "", ""), flags.String("supply", "", "")
	if flags.Parse(args) != nil || flags.NArg() != 0 || len(args) != 6 {
		return errors.New("P40: --scenario/--root/--supply are required exactly once")
	}
	allowed := map[string]bool{"authority": true, "rotation": true, "release-runtime": true, "support-cycle": true, "active-installer": true}
	if !allowed[*scenario] || *root == "" || *path == "" {
		return errors.New("P40: unknown or incomplete installed scenario")
	}
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" || os.Geteuid() == 0 {
		return errors.New("P40: unsupported; installed sandbox runner requires an unprivileged linux/amd64 caller")
	}
	if err := privateDirectory(*root); err != nil {
		return err
	}
	if !filepath.IsAbs(*path) || filepath.Clean(*path) != *path {
		return errors.New("P26: canonical absolute supply path required")
	}
	data, err := os.ReadFile(*path)
	if err != nil || len(data) > 4<<20 {
		return errors.New("P26: bounded private privilege supply required")
	}
	var supply manifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&supply) != nil || decoder.Decode(new(any)) != io.EOF || supply.Version != 1 || len(supply.Releases) != 2 {
		return errors.New("P26: complete adjacent release supply required")
	}
	if supply.Releases[0].Version == supply.Releases[1].Version || supply.Releases[0].Source.SHA256 == supply.Releases[1].Source.SHA256 {
		return errors.New("P26: previous support cannot be a same-source relabel")
	}
	base := filepath.Dir(*path)
	if err := privateDirectory(base); err != nil {
		return err
	}
	var checks []entryCheck
	// These commands enter the actual product main. The UID gate must refuse
	// worker/bootstrap before OpenInstalled/OpenBootstrap can touch the host.
	// A version-only process is never reported as an active support worker.
	// origin: hot-update S9 plan §6.1 / T92; S9 internal review INFRA-SUITE-SUPPLIES F4
	for _, image := range supply.Releases {
		for _, item := range image.Binaries {
			if item.Name != "tether-infra-helper" && item.Name != "tether-infra-launch" {
				continue // Never execute a supplied or installed tether here.
			}
			binary, err := verifiedEntry(base, item.Race, item.Name)
			if err != nil {
				return err
			}
			commands := []string{"version"}
			if item.Name == "tether-infra-helper" {
				commands = append(commands, "worker", "bootstrap")
			}
			for _, command := range commands {
				check, err := checkEntry(*root, binary, item, command)
				if err != nil {
					return err
				}
				checks = append(checks, check)
			}
		}
	}
	if len(checks) != 8 {
		return errors.New("P26: both genuine support generations must supply helper and launcher")
	}
	encoded, err := json.Marshal(checks)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(*root, "entry-checks.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errors.New("P40: fresh entry-check evidence required")
	}
	_, writeErr := file.Write(encoded)
	err = errors.Join(writeErr, file.Sync(), file.Close())
	if err != nil {
		return errors.New("P40: entry-check evidence could not be persisted")
	}
	// There is no public installed syscall sandbox with three kernel peer UIDs,
	// private managed cgroups and conntrack in the current P03/P02/P05 graph.
	// Return no installedEvent: an entry refusal is not a signed host effect.
	return errors.New("P40/P03/P02/P05/P07: trusted installed multi-UID/syscall sandbox, managed-unit census and ingress witnesses required; real systemd/nft receipts belong to P31/86")
}

// origin: hot-update S9 plan §9.4 / M28; S9 internal review INFRA-SUITE-SUPPLIES F4
func privateDirectory(path string) error {
	info, err := os.Stat(path)
	resolved, resolveErr := filepath.EvalSymlinks(path)
	if err != nil || resolveErr != nil || !filepath.IsAbs(path) || filepath.Clean(path) != path || resolved != path ||
		path == "/" || path == os.TempDir() || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("P40: private canonical directory required")
	}
	return nil
}

// origin: hot-update S9 plan §6.5 / T55 / M28
func verifiedEntry(root string, record suppliedFile, name string) (string, error) {
	if filepath.IsAbs(record.Path) || filepath.Clean(record.Path) != record.Path || record.Path == "." || record.Path == ".." || strings.HasPrefix(record.Path, "../") {
		return "", errors.New("P26: supporting entry escapes private supply")
	}
	path := filepath.Join(root, record.Path)
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return "", errors.New("P26: supporting entry contains a symlink")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 512<<20 || info.Mode().Perm()&0111 == 0 {
		return "", errors.New("P26: bounded supporting executable required")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, readErr := io.Copy(hash, file)
	if err := errors.Join(readErr, file.Close()); err != nil || hex.EncodeToString(hash.Sum(nil)) != record.SHA256 {
		return "", errors.New("P26: supporting executable digest mismatch")
	}
	build, err := buildinfo.ReadFile(path)
	if err != nil || build.Path != "github.com/LinZiyang666/tether/cmd/"+name {
		return "", errors.New("P26: supporting executable is not the actual product entry")
	}
	for _, setting := range build.Settings {
		if setting.Key == "-race" && setting.Value == "true" {
			return path, nil
		}
	}
	return "", errors.New("P26: supporting executable lacks inner race instrumentation")
}

// checkEntry records a product command's native result without inventing a
// running-worker identity. Callers keep this preflight separate from HostOp
// journal/effect barriers and successful installedEvents.
// origin: hot-update S9 plan §6.1 / T92
func checkEntry(root, path string, item binary, command string) (entryCheck, error) {
	if os.Geteuid() == 0 {
		return entryCheck{}, errors.New("P40: root execution is outside the unprivileged entry checks")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // ctx-root: bounded offline installed-entry check.
	cmd := exec.CommandContext(ctx, path, command)
	cmd.Dir, cmd.Env = root, []string{"PATH=/usr/bin:/bin", "HOME=" + root, "TETHER_HOME=" + root}
	output, err := testharness.CombinedOutput(cmd)
	cancel()
	rc := 0
	if err != nil {
		var exited *exec.ExitError
		if !errors.As(err, &exited) {
			return entryCheck{}, errors.New("P40: supervised product entry did not join")
		}
		rc = exited.ExitCode()
	}
	wanted, text := 0, item.Name+" protocol 1\n"
	if command != "version" {
		wanted, text = 77, "infra_permission_denied\n"
	}
	if rc != wanted || string(output) != text {
		return entryCheck{}, errors.New("P40: actual product entry disagrees with its unprivileged boundary")
	}
	return entryCheck{item.Name, item.Race.SHA256, command, fmt.Sprintf("%x", sha256.Sum256(output)), rc}, nil
}
