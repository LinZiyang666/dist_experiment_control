// Registered component keys share the transaction validator. Agent identity
// is SID/configured NID/UID, with an optional canonical legacy config path.
// origin: S8 internal review rv-txn F07
package upgradelock

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/LinZiyang666/tether/internal/upgradetx"
	"golang.org/x/sys/unix"
)

// Registration is supplied by the deployment configuration, never inferred
// from the currently executing immutable slot or an environment override.
// Participants must include every sibling, including those running as another
// UID. ServiceID is the registered supervisor/unit identity, not a live PID.
// origin: hot-update S8 plan §3.1 / §5.1
type Registration struct {
	CanonicalPath string        `json:"canonical_path"`
	ControlDir    string        `json:"control_dir"`
	DeploymentID  string        `json:"deployment_id"`
	Aliases       []string      `json:"aliases"`
	Participants  []Participant `json:"participants"`
}

type Participant struct {
	Key       upgradetx.Key `json:"key"`
	ServiceID string        `json:"service_id"`
}

type domain struct {
	registration Registration
	id           string
	parent       os.FileInfo
	control      os.FileInfo
	encoded      []byte
}

var domains = struct {
	sync.Mutex
	byID map[string]*domain
}{byID: make(map[string]*domain)}

// RegisterDomain persists the complete deployment declaration and returns its
// stable ID. Startup registers the same declaration again before using a Key;
// a missing registration never creates an alternative namespace on acquisition.
// Updating a deployment requires explicit integration policy, rather than an
// automatic overwrite that might hide a pending sibling.
func RegisterDomain(reg Registration) (string, error) {
	d, err := normalizeDomain(reg)
	if err != nil {
		return "", err
	}
	domains.Lock()
	defer domains.Unlock()
	registryGuard, err := lockRegistry(d)
	if err != nil {
		return "", err
	}
	defer func() { _ = registryGuard.Close() }()
	if previous := domains.byID[d.id]; previous != nil && (!bytes.Equal(previous.encoded, d.encoded) || !os.SameFile(previous.control, d.control)) {
		return "", refusal(upgradetx.CodeRejected, "domain registration changed")
	}
	for _, previous := range domains.byID {
		if previous.id != d.id && installationsOverlap(previous.registration, d.registration) {
			return "", refusal(upgradetx.CodeRejected, "installation is already registered in another domain")
		}
	}
	if err := checkOtherRegistrations(d); err != nil {
		return "", err
	}
	path := filepath.Join(d.registration.ControlDir, d.id+".domain")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err == nil {
		// Keep incomplete registration evidence on any durability failure. A
		// later start must refuse it rather than assume no registration existed.
		_, writeErr := f.Write(d.encoded)
		syncErr := f.Sync()
		closeErr := f.Close()
		if writeErr != nil || syncErr != nil || closeErr != nil {
			return "", refusal(upgradetx.CodeDeferred, "domain registration is not durable")
		}
		parent, openErr := os.Open(d.registration.ControlDir)
		if openErr != nil {
			return "", refusal(upgradetx.CodeDeferred, "domain directory is unavailable")
		}
		syncErr = parent.Sync()
		closeErr = parent.Close()
		if syncErr != nil || closeErr != nil {
			return "", refusal(upgradetx.CodeDeferred, "domain directory is not durable")
		}
	} else if !os.IsExist(err) {
		return "", refusal(upgradetx.CodeDeferred, "domain registration is not writable")
	}
	if err := checkRegistration(d); err != nil {
		return "", err
	}
	domains.byID[d.id] = d
	return d.id, nil
}

func normalizeDomain(reg Registration) (*domain, error) {
	if !boundedIdentity(reg.DeploymentID) || len(reg.Participants) == 0 || len(reg.Participants) > 1024 || len(reg.Aliases) > 1024 {
		return nil, refusal(upgradetx.CodeRejected, "incomplete domain registration")
	}
	canonical, err := canonicalPath(reg.CanonicalPath)
	if err != nil {
		return nil, err
	}
	control, err := canonicalPath(reg.ControlDir)
	if err != nil || control != reg.ControlDir {
		return nil, refusal(upgradetx.CodeRejected, "control directory must be canonical")
	}
	parent, err := os.Stat(filepath.Dir(canonical))
	if err != nil {
		return nil, refusal(upgradetx.CodeRejected, "installation parent is unavailable")
	}
	controlInfo, err := os.Stat(control)
	if err != nil || !controlInfo.IsDir() || controlInfo.Mode().Perm()&0o002 != 0 {
		return nil, refusal(upgradetx.CodeRejected, "control directory is not controlled")
	}
	st, ok := parent.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, refusal(upgradetx.CodeRejected, "installation parent identity is unavailable")
	}
	// The binary inode is deliberately absent: atomic installation changes it.
	// Length framing also prevents deployment/path concatenation collisions.
	// origin: hot-update S8 plan §5.1
	h := sha256.New()
	for _, part := range []string{canonical, fmt.Sprint(st.Dev), fmt.Sprint(st.Ino), reg.DeploymentID} {
		_ = binary.Write(h, binary.BigEndian, uint32(len(part)))
		_, _ = h.Write([]byte(part))
	}
	id := hex.EncodeToString(h.Sum(nil)[:16])
	reg.CanonicalPath, reg.ControlDir = canonical, control
	reg.Aliases = append([]string(nil), reg.Aliases...)
	sort.Strings(reg.Aliases)
	if err := checkAliases(reg); err != nil {
		return nil, err
	}
	reg.Participants = append([]Participant(nil), reg.Participants...)
	seenKeys, seenServices := make(map[upgradetx.Key]bool), make(map[string]bool)
	for i := range reg.Participants {
		p := &reg.Participants[i]
		if p.Key.DomainID != "" && p.Key.DomainID != id {
			return nil, refusal(upgradetx.CodeRejected, "participant domain mismatch")
		}
		p.Key.DomainID = id
		if err := validateKey(p.Key); err != nil {
			return nil, err
		}
		if !boundedIdentity(p.ServiceID) || seenKeys[p.Key] || seenServices[p.ServiceID] {
			return nil, refusal(upgradetx.CodeRejected, "duplicate or unknown participant")
		}
		seenKeys[p.Key], seenServices[p.ServiceID] = true, true
	}
	sort.Slice(reg.Participants, func(i, j int) bool { return reg.Participants[i].ServiceID < reg.Participants[j].ServiceID })
	encoded, err := json.Marshal(reg)
	if err != nil || len(encoded) > 1<<20 {
		return nil, refusal(upgradetx.CodeRejected, "invalid domain registration")
	}
	return &domain{registration: reg, id: id, parent: parent, control: controlInfo, encoded: encoded}, nil
}

func checkAliases(reg Registration) error {
	resolved, resolveErr := filepath.EvalSymlinks(reg.CanonicalPath)
	if resolveErr != nil || resolved != reg.CanonicalPath {
		return refusal(upgradetx.CodeRejected, "canonical installation path became an alias")
	}
	file, err := os.Stat(reg.CanonicalPath)
	if err != nil || !file.Mode().IsRegular() {
		return refusal(upgradetx.CodeRejected, "canonical installation is not a regular file")
	}
	st, ok := file.Sys().(*syscall.Stat_t)
	if !ok {
		return refusal(upgradetx.CodeRejected, "installation identity is unavailable")
	}
	links := uint64(1)
	seen := map[string]bool{reg.CanonicalPath: true}
	for _, alias := range reg.Aliases {
		if !filepath.IsAbs(alias) || filepath.Clean(alias) != alias || seen[alias] {
			return refusal(upgradetx.CodeRejected, "invalid installation alias")
		}
		seen[alias] = true
		actual, statErr := os.Stat(alias)
		entry, entryErr := os.Lstat(alias)
		if statErr != nil || entryErr != nil || !os.SameFile(file, actual) {
			return refusal(upgradetx.CodeRejected, "unknown alias or clone family")
		}
		// A symlink to an already-counted pathname is not another hard link.
		if entry.Mode().IsRegular() {
			resolvedParent, resolveErr := filepath.EvalSymlinks(filepath.Dir(alias))
			if resolveErr != nil || filepath.Join(resolvedParent, filepath.Base(alias)) != alias {
				return refusal(upgradetx.CodeRejected, "alias parent must be canonical")
			}
			links++
		}
	}
	if links != uint64(st.Nlink) {
		return refusal(upgradetx.CodeRejected, "installation has unregistered hard links")
	}
	return nil
}

func resolveDomain(key upgradetx.Key) (*domain, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	domains.Lock()
	d := domains.byID[key.DomainID]
	domains.Unlock()
	if d == nil {
		return nil, refusal(upgradetx.CodeRejected, "domain is not registered")
	}
	known := false
	for _, p := range d.registration.Participants {
		known = known || p.Key == key
	}
	if !known {
		return nil, refusal(upgradetx.CodeRejected, "component is not registered")
	}
	parent, parentErr := os.Stat(filepath.Dir(d.registration.CanonicalPath))
	control, controlErr := os.Stat(d.registration.ControlDir)
	resolved, resolveErr := filepath.EvalSymlinks(d.registration.ControlDir)
	if parentErr != nil || controlErr != nil || resolveErr != nil || resolved != d.registration.ControlDir || !os.SameFile(d.parent, parent) || !os.SameFile(d.control, control) {
		return nil, refusal(upgradetx.CodeRejected, "domain directory identity changed")
	}
	if err := checkRegistration(d); err != nil {
		return nil, err
	}
	if err := checkOtherRegistrations(d); err != nil {
		return nil, err
	}
	if err := checkAliases(d.registration); err != nil {
		return nil, err
	}
	return d, nil
}

func checkRegistration(d *domain) error {
	path := filepath.Join(d.registration.ControlDir, d.id+".domain")
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return refusal(upgradetx.CodeStateCorrupt, "domain registration is unavailable")
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != int64(len(d.encoded)) {
		return refusal(upgradetx.CodeStateCorrupt, "domain registration is invalid")
	}
	actual := make([]byte, len(d.encoded))
	if _, err := f.ReadAt(actual, 0); err != nil || !bytes.Equal(actual, d.encoded) {
		return refusal(upgradetx.CodeStateCorrupt, "domain registration differs from deployment")
	}
	return nil
}

// A changed deployment ID must not create a second lock for the same install,
// including after a cold restart has emptied the in-process registry. The
// control directory is fixed by deployment configuration; it is not searched
// in HOME or replaced with a fallback when its evidence is missing.
// origin: hot-update S8 plan §3.1 / §5.1
func checkOtherRegistrations(d *domain) error {
	entries, err := os.ReadDir(d.registration.ControlDir)
	if err != nil || len(entries) > 4096 {
		return refusal(upgradetx.CodeStateCorrupt, "domain registry cannot be enumerated")
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".domain") || entry.Name() == d.id+".domain" {
			continue
		}
		path := filepath.Join(d.registration.ControlDir, entry.Name())
		f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return refusal(upgradetx.CodeStateCorrupt, "sibling domain registration is unavailable")
		}
		info, statErr := f.Stat()
		var data []byte
		var readErr error
		if statErr == nil && info.Mode().IsRegular() && info.Size() > 0 && info.Size() <= 1<<20 {
			data = make([]byte, info.Size())
			_, readErr = f.ReadAt(data, 0)
		}
		_ = f.Close()
		var reg Registration
		if len(data) == 0 || readErr != nil || json.Unmarshal(data, &reg) != nil {
			return refusal(upgradetx.CodeStateCorrupt, "sibling domain registration is invalid")
		}
		if installationsOverlap(reg, d.registration) {
			return refusal(upgradetx.CodeRejected, "installation is already registered in another domain")
		}
	}
	return nil
}

func installationsOverlap(a, b Registration) bool {
	if a.CanonicalPath == b.CanonicalPath {
		return true
	}
	left, leftErr := os.Stat(a.CanonicalPath)
	right, rightErr := os.Stat(b.CanonicalPath)
	return leftErr == nil && rightErr == nil && os.SameFile(left, right)
}

// Serialize declaration discovery and O_EXCL publication across processes.
// Different proposed IDs otherwise both observe an empty registry and can
// create independent domains for the very same installation.
// origin: hot-update S8 plan §5.1
func lockRegistry(d *domain) (*os.File, error) {
	path := filepath.Join(d.registration.ControlDir, "domain-registry.guard")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, refusal(upgradetx.CodeDeferred, "domain registry guard is unavailable")
	}
	info, statErr := f.Stat()
	named, nameErr := os.Lstat(path)
	valid := false
	if statErr == nil {
		st, ok := info.Sys().(*syscall.Stat_t)
		valid = ok && st.Nlink == 1 && info.Mode().IsRegular()
	}
	if !valid || nameErr != nil || !os.SameFile(info, named) {
		_ = f.Close()
		return nil, refusal(upgradetx.CodeRejected, "domain registry guard identity is invalid")
	}
	if err := flock(f, unix.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, refusal(upgradetx.CodeDeferred, "domain registry guard cannot be locked")
	}
	return f, nil
}

func canonicalPath(path string) (string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > 4096 {
		return "", refusal(upgradetx.CodeRejected, "path must be absolute and clean")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", refusal(upgradetx.CodeRejected, "canonical path is unavailable")
	}
	return resolved, nil
}

func validateKey(key upgradetx.Key) error {
	decoded, err := hex.DecodeString(key.DomainID)
	if err != nil || len(decoded) != 16 || key.DomainID != strings.ToLower(key.DomainID) {
		return refusal(upgradetx.CodeRejected, "invalid domain identity")
	}
	// Registration identifies S9's journal and shared reservation; it does
	// not authorize an S8 executor to run an infrastructure component.
	// Keep the role in the exact key so a broker or agent cannot borrow it.
	// origin: hot-update S8 plan §2.1 / §5.1; S9 plan §8 / T44
	return upgradetx.ValidateKey(key)
}

func boundedIdentity(value string) bool {
	return value != "" && len(value) <= 256 && !strings.ContainsAny(value, "\x00\r\n")
}

func refusal(code, detail string) error {
	return &upgradetx.Error{Code: code, Detail: detail}
}

// RegistrationFor returns the already registered declaration, after checking
// its on-disk identity. Publication must not invent a second control directory.
// origin: S8 internal review rv-txn F06 F07
func RegistrationFor(key upgradetx.Key) (Registration, error) {
	d, err := resolveDomain(key)
	if err != nil {
		return Registration{}, err
	}
	r := d.registration
	r.Aliases = append([]string(nil), r.Aliases...)
	r.Participants = append([]Participant(nil), r.Participants...)
	return r, nil
}
