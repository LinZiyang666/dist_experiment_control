// evidencecmd is the single offline JSON boundary for fullstack predicates.
// Archives are data: command fields, argv and product diagnostics are never
// executed. A confined directory supplies the exact bytes cited by the result.
// origin: hot-update S10 plan §5.1 / P01
package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/LinZiyang666/tether/test/hotupdate/acceptance"
	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || (args[0] != "validate" && args[0] != "judge" && args[0] != "quantiles") {
		return setupFailure(stdout, stderr, errors.New("expected validate, judge or quantiles"))
	}
	flags := flag.NewFlagSet("evidencecmd", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	profilePath := flags.String("profile", "", "sealed profile JSON file")
	evidenceDir := flags.String("evidence", "", "confined archive containing evidence.json")
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 || *profilePath == "" || *evidenceDir == "" {
		return setupFailure(stdout, stderr, errors.New("profile and evidence directory are required"))
	}
	e, err := readEvidence(*profilePath, *evidenceDir)
	if err != nil {
		return setupFailure(stdout, stderr, err)
	}
	switch args[0] {
	case "validate":
		// GREEN here certifies archive/schema validity, not a deployment result.
		return writeResult(stdout, stderr, c.Result{Verdict: "GREEN", RC: 0, Inputs: e.Raw})
	case "judge":
		result, err := acceptance.Judge(e)
		if err != nil {
			return setupFailure(stdout, stderr, err)
		}
		return writeResult(stdout, stderr, result)
	case "quantiles":
		// P02 owns the implementation; there is no fallback to a failure stub
		// or a second quantile algorithm maintained by this command.
		distribution, err := acceptance.Quantiles(e.Attempts, e.Slots)
		if err != nil {
			return setupFailure(stdout, stderr, err)
		}
		data, err := c.EncodeJSON("distribution", distribution)
		if err != nil {
			return setupFailure(stdout, stderr, errors.New("cannot encode fullstack distribution"))
		}
		if _, err := fmt.Fprintln(stdout, string(data)); err != nil {
			return 2
		}
		return 0
	default:
		return setupFailure(stdout, stderr, errors.New("unknown fullstack command"))
	}
}

func readEvidence(profilePath, evidenceDir string) (e c.Evidence, err error) {
	var profile c.Profile
	data, err := os.ReadFile(profilePath)
	if err != nil || c.DecodeJSON("profile", data, &profile) != nil {
		return e, errors.New("cannot read valid fullstack profile")
	}
	root, err := os.OpenRoot(evidenceDir)
	if err != nil {
		return e, errors.New("cannot open fullstack evidence directory")
	}
	// Close errors must not turn a failed archive read into valid evidence.
	// origin: hot-update S10 plan §5.1 / LINT-s10
	defer closeArchiveResource(&err, root, "cannot close fullstack evidence directory")
	data, err = readArchiveFile(root, "evidence.json")
	if err != nil || c.DecodeJSON("evidence", data, &e) != nil {
		return e, errors.New("cannot read valid fullstack evidence")
	}
	want, err := c.EncodeJSON("profile", profile)
	if err != nil {
		return e, errors.New("invalid fullstack profile")
	}
	got, err := c.EncodeJSON("profile", e.Profile)
	if err != nil || !bytes.Equal(want, got) {
		return e, errors.New("fullstack archive is bound to a different profile")
	}
	refs := append([]c.RawRef{profile.CommandBindings}, e.Raw...)
	for _, view := range e.Views {
		refs = append(refs, view.Raw, view.Control.Evidence, view.Resources.Evidence, view.Released.Evidence)
	}
	for _, sample := range e.Samples {
		refs = append(refs, sample.Raw)
	}
	for _, attempt := range e.Attempts {
		refs = append(refs, attempt.Raw)
	}
	for _, interval := range e.Intervals {
		refs = append(refs, interval.Raw)
	}
	archive := make(map[c.RawRef][]byte)
	absRoot, err := filepath.Abs(evidenceDir)
	if err != nil {
		return e, errors.New("invalid fullstack archive root")
	}
	for index := 0; index < len(refs); index++ {
		ref := refs[index]
		if _, seen := archive[ref]; seen {
			continue
		}
		name := ref.Path
		if filepath.IsAbs(name) {
			name, err = filepath.Rel(absRoot, name)
			if err != nil {
				return e, errors.New("invalid fullstack raw path")
			}
		}
		// OpenRoot refuses traversal and symlink escapes. Hash the opened file,
		// not an unchecked pathname subsequently reopened by a shell command.
		info, statErr := root.Stat(name)
		if statErr != nil || !info.Mode().IsRegular() {
			return e, errors.New("invalid fullstack raw file type")
		}
		f, err := root.Open(name)
		if err != nil {
			return e, errors.New("cannot open confined fullstack raw bytes")
		}
		info, statErr = f.Stat()
		if statErr != nil || !info.Mode().IsRegular() || info.Size() != ref.Bytes || ref.Bytes <= 0 {
			_ = f.Close()
			return e, errors.New("invalid fullstack raw byte count or file type")
		}
		raw, readErr := io.ReadAll(f)
		hash := sha256.Sum256(raw)
		closeErr := f.Close()
		if readErr != nil || closeErr != nil || int64(len(raw)) != ref.Bytes || hash != ref.SHA {
			return e, errors.New("fullstack raw digest mismatch")
		}
		// A digest authenticates bytes, not their meaning. Unknown upstream
		// schemas (including pending adapters) must fail before any predicate runs.
		// origin: S10 internal review rv-honesty F1
		if c.ValidateJSON(ref.Schema, raw) != nil {
			return e, errors.New("invalid or unregistered fullstack raw schema")
		}
		archive[ref] = raw
		if ref == profile.CommandBindings {
			help, err := checkCommandBindings(profile, raw)
			if err != nil {
				return e, err
			}
			refs = append(refs, help...)
		}
	}
	if err := checkProjections(e, archive); err != nil {
		return e, err
	}
	return e, nil
}

func readArchiveFile(root *os.Root, name string) (data []byte, err error) {
	info, err := root.Stat(name)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("invalid archive document")
	}
	f, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	// Keep close diagnostics static: a PathError can expose archive paths.
	// origin: hot-update S10 plan §5.1 / LINT-s10
	defer closeArchiveResource(&err, f, "cannot close fullstack archive document")
	info, err = f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("invalid archive document")
	}
	return io.ReadAll(f)
}

// A close failure invalidates evidence even after a successful read. Joining a
// static diagnostic preserves prior guard failures without exposing paths from
// the closer's error.
// origin: hot-update S10 plan §5.1 / LINT-s10
func closeArchiveResource(err *error, resource io.Closer, diagnostic string) {
	if closeErr := resource.Close(); closeErr != nil {
		*err = errors.Join(*err, errors.New(diagnostic))
	}
}

func setupFailure(stdout, stderr io.Writer, err error) int {
	// Product raw diagnostics may contain sensitive data. These errors describe
	// only local guards and never echo JSON fields, command text or read paths.
	_, _ = fmt.Fprintln(stderr, err)
	return writeResult(stdout, stderr, c.Result{Verdict: "SETUP-RED", RC: 2, Failures: []string{err.Error()}})
}

func writeResult(stdout, stderr io.Writer, result c.Result) int {
	data, err := c.EncodeJSON("result", result)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "cannot encode fullstack result")
		return 2
	}
	if _, err := fmt.Fprintln(stdout, string(data)); err != nil {
		return 2
	}
	return result.RC
}
