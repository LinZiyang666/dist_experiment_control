// capability.go owns the SHA-bound image reader declarations and compatibility
// checks used before staging and at each final re-export.
// origin: hot-update S4 plan §5.2 / T30 / T32 / T46.
package agentimage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradecompat"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// ErrABIUnsupported refuses a reader that cannot consume the retained image.
var ErrABIUnsupported = errors.New("upgrade: candidate image ABI is unsupported")

// ErrEpochMismatch refuses a candidate from another protocol epoch.
var ErrEpochMismatch = errors.New("upgrade: staged binary belongs to a different proto epoch")

// ErrRunningImageDiffers is the prev-slot admissibility refusal (hot-update
// S3 plan §2.1): the on-disk binary is neither this running image nor the
// candidate, so no rollback could safely hand it an image. The text is the
// operator-facing `install_failed` Error verbatim.
var ErrRunningImageDiffers = errors.New("running image differs from the on-disk binary; restart the agent before upgrading")

// Capabilities records the declarations from the candidate probe.
// imageCapabilities belongs to the probed bytes, never merely to a pathname.
// origin: hot-update S4 plan §5.2 / T46.
type Capabilities struct {
	ABI     int
	Domains []string
	SHA256  string
	// ReadEpochs is the explicit reader set; nil is an older reader that can
	// read only its own ABI epoch. Features name callable S8 adapters.
	// origin: hot-update S8 plan §6.1 / §11.2 P17; S8 review rv-n1 F5
	ReadEpochs []uint64
	Features   []string
}

// Reader projects these SHA-bound declarations into the S8 admission input.
// Epoch is the ABI epoch, independent of process generation and release.
func (c Capabilities) Reader(release string, component upgradetx.Component) upgradecompat.Reader {
	return upgradecompat.Reader{SHA256: c.SHA256, Release: release, Component: component, Epoch: uint64(max(c.ABI, 0)),
		ReadEpochs: slices.Clone(c.ReadEpochs), Domains: slices.Clone(c.Domains), Features: slices.Clone(c.Features)}
}

type imageEpochError struct{ cause error }

func (e imageEpochError) Error() string   { return "upgrade: candidate image epoch is unreadable" }
func (e imageEpochError) Unwrap() []error { return []error{ErrABIUnsupported, e.cause} }

// EmptyRuntimeImage includes all retained request and revocation metadata.
func EmptyRuntimeImage(img execsession.Image) bool {
	return len(img.Registry.Records)+len(img.Tasks)+len(img.Tombstones)+len(img.Revocations) == 0
}

type imageDomainError struct{ domain string }

func (e imageDomainError) Error() string {
	return "upgrade: candidate image domain is unsupported: requires " + e.domain
}
func (e imageDomainError) Unwrap() error { return ErrABIUnsupported }

// CheckDomains requires an accepted ABI and every domain in the actual image.
func CheckDomains(candidate Capabilities, required []string) error {
	if candidate.ABI < 1 {
		return fmt.Errorf("%w: candidate image-abi v%d, agent supports v%d", ErrABIUnsupported, candidate.ABI, proto.ImageABIVersion)
	}
	// origin: hot-update S8 plan §6.1 / §11.2 P17; S8 review rv-n1 F5. The
	// declared reader set and the one-epoch distance bind both directions:
	// the reader must consume this image, and this image must read its
	// current re-export on rollback. A numeric ceiling alone could neither
	// refuse a candidate excluding our epoch nor admit a real adjacent reader.
	current := CurrentCapabilities("")
	for _, pair := range [][2]Capabilities{{current, candidate}, {candidate, current}} {
		if err := upgradecompat.CheckEpochs(pair[0].Reader("", ""), pair[1].Reader("", "")); err != nil {
			return imageEpochError{cause: err}
		}
	}
	for _, domain := range required {
		found := false
		for _, supported := range candidate.Domains {
			found = found || domain == supported
		}
		if !found {
			return imageDomainError{domain: domain}
		}
	}
	return nil
}

// The second and final checks consume the bundle that will actually travel.
// No state is collected or discarded to accommodate a weaker reader.
// Retained Flow/SS responsibility requires a reader even with zero live sockets.
// origin: hot-update S5 internal review rv-image-3 / rv-authority-5
func CheckBundleCapabilities(b Bundle, readers ...Capabilities) error {
	for _, reader := range readers {
		if reader.ABI == 0 {
			if !EmptyRuntimeImage(b.Runtime) || len(b.Courier.Events) != 0 || b.Flow != nil || b.SS != nil || b.Transfers != nil {
				return ErrABIUnsupported
			}
			continue
		}
		if err := CheckDomains(reader, b.RequiredDomains()); err != nil {
			return err
		}
	}
	return nil
}

// The rollback reader is determined by bytes, including the shared sibling
// where disk already contains the candidate while this exporter is older.
// origin: hot-update S4 plan §5.2 / T30.
func SelectPreviousCapabilities(runningSHA, prevSHA string, candidate Capabilities) (Capabilities, error) {
	if runningSHA != "" && prevSHA != runningSHA && prevSHA != candidate.SHA256 {
		return Capabilities{}, ErrRunningImageDiffers
	}
	if prevSHA == candidate.SHA256 {
		return candidate, nil
	}
	return CurrentCapabilities(prevSHA), nil
}

// CurrentCapabilities binds this binary's reader declarations to its SHA.
// They are parsed from the exact lines `version` emits, so the advertised and
// enforced reader sets cannot drift; a malformed emitter claims no reader.
// origin: hot-update S8 plan §6.1 / §11.2 P17
func CurrentCapabilities(sha string) Capabilities {
	output := strings.Join(append([]string{proto.VersionLine()}, proto.ImageVersionLines()...), "\n")
	_, caps, err := ParseCapabilities(output)
	if err != nil {
		return Capabilities{SHA256: sha}
	}
	caps.SHA256 = sha
	return caps
}

// VerifyCapabilitySHA rejects stale capabilities after executable replacement.
func VerifyCapabilitySHA(path string, caps Capabilities) error {
	sha, err := SHA256OfFile(path)
	if err != nil || sha != caps.SHA256 || caps.SHA256 == "" {
		return errors.New("upgrade: image capability SHA no longer matches executable")
	}
	return nil
}

// ParseCapabilities checks the frozen version shape and ordered declarations.
// The grammar (including the strict image-read-epochs and image-features
// lines) lives with the S8 admission checks that consume it; this adapter keeps
// the agent's epoch-mismatch class and operator wording.
// origin: hot-update S8 plan §6.1 / §11.2 P17; S8 review rv-n1 F5
func ParseCapabilities(output string) (string, Capabilities, error) {
	d, err := upgradecompat.ParseDeclarations(output, proto.SubjectVersionToken)
	var mismatch *upgradecompat.EpochMismatchError
	if errors.As(err, &mismatch) {
		return "", Capabilities{}, fmt.Errorf("%w: candidate reports %q, this agent is %q — an epoch change must go through reinstall", ErrEpochMismatch, mismatch.Got, mismatch.Want)
	}
	if err != nil {
		return "", Capabilities{}, err
	}
	return d.Release, Capabilities{ABI: d.ABI, Domains: d.Domains, ReadEpochs: d.ReadEpochs, Features: d.Features}, nil
}

// SHA256OfFile hashes the executable without buffering its contents.
// sha256OfFile streams the hex sha256 of a file on disk (G5 #13 re-exec-only: verify the staged binary
// before re-execing it, without loading the whole binary into memory).
func SHA256OfFile(path string) (string, error) {
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

// A legacy marker is probed through the same bounded, isolated supervisor.
// A stored declaration with a mismatching SHA is corrupt, not a cache miss.
// executeRollback independently rechecks PrevSHA immediately before rename.
// origin: hot-update S4 plan §5.2 / T30 / T46.
// PreviousCapabilities verifies the bytes selected for rollback. A declaration
// cache is never authority for a different executable at the same pathname.
// origin: hot-update S7 plan §8.4 / P21; moved from agent.previousImageCapabilities
func PreviousCapabilities(path string, caps Capabilities, declarationSHA string, probe func(string) (string, Capabilities, error)) (Capabilities, error) {
	if err := VerifyCapabilitySHA(path, caps); err != nil {
		return Capabilities{}, err
	}
	if declarationSHA != "" {
		if declarationSHA != caps.SHA256 {
			return Capabilities{}, errors.New("upgrade: previous image domain declaration has a mismatched SHA")
		}
		return caps, nil
	}
	_, probed, err := probe(path)
	if err != nil {
		return Capabilities{}, err
	}
	if probed.SHA256 != caps.SHA256 {
		return Capabilities{}, errors.New("upgrade: previous image changed during capability probe")
	}
	return probed, nil
}
