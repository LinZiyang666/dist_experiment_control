// path.go owns filesystem containment and directory-pinned transfer operations.
// origin: hot-update S4 plan §10.
package transferpath

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/imageref"
	"github.com/LinZiyang666/tether/internal/xfercontract/xferport"

	"golang.org/x/sys/unix"
)

// These policy values match the agent's raw-config decision; canonical roots never select a mode.
const (
	ModeOpen     = 0
	ModeNarrow   = 1
	ModeDisabled = 2
)

// ─── Path validation (allow_roots + symlink defence) ──────────────────

// PathValidationError carries both a human message and a machine-readable
// code that lands in the proto.Push/PullPrepareResp.Code field.
//
// The code names come from file-transfer-plan §"Refusing dangerous paths"
// step list and the §Audit code column. Any new code added here must
// also be documented in the plan.
type PathValidationError struct {
	Code string
	Msg  string
}

func (e *PathValidationError) Error() string { return e.Code + ": " + e.Msg }

// IsPathValidationError unwraps to bool so callers can do
// `if errors.As(err, &pve) { ... }` without per-call helper noise.
func IsPathValidationError(err error) bool {
	var pve *PathValidationError
	return errors.As(err, &pve)
}

// pathErr is a tiny constructor — keeps call sites short.
func pathErr(code, format string, a ...any) *PathValidationError {
	return &PathValidationError{Code: code, Msg: fmt.Sprintf(format, a...)}
}

// CanonAllowRoots cleans + de-duplicates the allow_roots list and
// returns each entry in EvalSymlinks-resolved form. Any allow_root that
// fails EvalSymlinks (does not exist, is not a directory, contains an
// unresolvable symlink) is silently dropped. Dropping shrinks the
// narrow-mode root list but NEVER widens reach: transferMode is decided
// upstream from the RAW config presence (resolveTransferMode), so a
// narrow config whose entries all drop stays modeNarrow with an empty
// list and rejects every path (fail-closed) — it does not collapse to the
// whole-FS open mode. The result is always sorted by length descending so
// the longest match wins (so /srv/local/alice does not get shadowed by
// /srv).
//
// Callers should invoke this ONCE at startup (Agent.New stores the
// result in a.canonAllowRoots) and pass the result to ValidateForRead /
// ValidateForWrite — those funcs no longer canonicalize per-call.
// Audit shard P11 F3: previously these helpers re-evaluated symlinks
// on every push/pull request, which was both a perf hit and a TOCTOU
// risk if an operator's allow_root happened to be symlinked.
func CanonAllowRoots(roots []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(roots))
	for _, r := range roots {
		if r == "" {
			continue
		}
		if !filepath.IsAbs(r) {
			continue
		}
		clean := filepath.Clean(r)
		resolved, err := filepath.EvalSymlinks(clean)
		if err != nil {
			continue
		}
		// Must actually be a directory — a regular file as an
		// allow_root would let the caller "write to /any/path/under/it"
		// which makes no sense.
		st, err := os.Stat(resolved)
		if err != nil || !st.IsDir() {
			continue
		}
		if _, dup := seen[resolved]; dup {
			continue
		}
		seen[resolved] = struct{}{}
		out = append(out, resolved)
	}
	// Longest-prefix-wins ordering.
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if len(out[j]) > len(out[i]) {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// containedIn returns the matched allow_root or "" if path is not inside
// any of them. filepath.Rel handles the filesystem root correctly (`/`
// contains `/etc/hosts`) while still preventing `/srv/localfoo` from
// matching `/srv/local`. Both sides are EvalSymlinks-resolved already.
func containedIn(resolvedPath string, roots []string) string {
	for _, r := range roots {
		rel, err := filepath.Rel(r, resolvedPath)
		if err != nil || filepath.IsAbs(rel) {
			continue
		}
		if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
			return r
		}
	}
	return ""
}

// ValidatedPath is what ValidateForRead / ValidateForWrite return on
// success. It carries both the absolute path the agent should open
// AND the matched allow_root (so the caller can attribute audit
// "tier=" / "root=" cleanly).
type ValidatedPath struct {
	Abs string // EvalSymlinks-resolved-parent + clean leaf; safe to open with O_NOFOLLOW
	// Parent and Base preserve the canonical parent/leaf split so the
	// openat pipeline can pin the directory and never re-traverse an
	// attacker-swappable parent path.
	Parent string
	Base   string
	// AllowRoot is the matched allow_root in narrow mode; "" in open mode
	// (no narrowing configured). Informational for audit/log attribution
	// only — it has zero readers today.
	AllowRoot string
}

// ValidateForWrite is the push-side check: agent is going to create a
// file at this path and write bytes. Steps follow file-transfer-plan
// §"Refusing dangerous paths":
//
//  1. mode != modeDisabled (explicit `allow_roots: []` → transfer_disabled).
//  2. absolute path required.
//  3. EvalSymlinks-resolve the parent dir; ENOENT → path_parent_missing
//     (parent dirs are NOT auto-created in v2.0).
//  4. In modeNarrow, <resolved-parent>/<base> must be inside one
//     allow_root (path_outside_roots on miss). modeOpen skips this step.
//  5. If destination already exists, must be a regular file (not a
//     symlink, dir, device, fifo) — symlink → not_a_regular_file.
//
// Caller still opens with O_NOFOLLOW|O_EXCL|O_CREAT to defeat any
// race-window symlink swap between this validation and open. dst-
// exists is NOT considered an error here (the caller's `--force`
// logic decides what to do); the leaf-symlink check still happens
// before that decision so a symlink dest can never silently dereference.
//
// `canonRoots` MUST be the result of CanonAllowRoots (called once at
// agent startup); this function does NOT re-canonicalize.
func ValidateForWrite(rawPath string, canonRoots []string, mode int) (*ValidatedPath, error) {
	if mode == ModeDisabled {
		return nil, pathErr("transfer_disabled",
			"file transfer disabled on this agent (allow_roots: [])")
	}
	if !filepath.IsAbs(rawPath) {
		return nil, pathErr("path_not_absolute",
			"%s: must be absolute", rawPath)
	}
	clean := filepath.Clean(rawPath)
	parent := filepath.Dir(clean)
	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, pathErr("path_parent_missing",
				"%s: parent directory does not exist (run `tether exec ... mkdir -p` first)", parent)
		}
		return nil, pathErr("io_error",
			"resolve parent of %s: %v", rawPath, err)
	}
	abs := filepath.Join(resolvedParent, filepath.Base(clean))
	// Containment applies only in narrow mode. In open mode matched stays
	// "" (whole-FS reach); every mechanism check below still runs.
	matched := ""
	if mode == ModeNarrow {
		matched = containedIn(abs, canonRoots)
		if matched == "" {
			return nil, pathErr("path_outside_roots",
				"%s: not under any allow_root (%v)", abs, canonRoots)
		}
	}
	// If the leaf already exists, it must be a regular file. A symlink
	// at the leaf is rejected here so a follow-up O_NOFOLLOW open
	// failure isn't the operator's only signal.
	if st, err := os.Lstat(abs); err == nil {
		if err := rejectLeafSymlink(abs, st); err != nil {
			return nil, err
		}
		if !st.Mode().IsRegular() {
			return nil, pathErr("not_a_regular_file",
				"%s: not a regular file (mode=%s)", abs, st.Mode())
		}
	} else if !os.IsNotExist(err) {
		return nil, pathErr("io_error", "lstat %s: %v", abs, err)
	}
	return &ValidatedPath{
		Abs: abs, Parent: resolvedParent, Base: filepath.Base(clean),
		AllowRoot: matched,
	}, nil
}

// ValidateForRead is the pull-side check: agent is going to open this
// path read-only and stream its bytes. Steps mirror ValidateForWrite,
// plus:
//
//   - leaf must exist (else path_not_found);
//   - leaf must NOT be a symlink (lstat check; O_NOFOLLOW on the open
//     would also catch it, but lstat gives a clean error code);
//   - leaf must be a regular file (not a dir / device / fifo /
//     socket).
//
// TOCTOU: the caller is expected to open(O_RDONLY|O_NOFOLLOW) and
// fstat the resulting fd, comparing dev+inode against the lstat
// captured here. The race window between lstat and open is small but
// not zero on adversarial filesystems; dev+inode mismatch surfaces as
// `path_race` at the open site.
//
// `canonRoots` MUST be the result of CanonAllowRoots (called once at
// agent startup); this function does NOT re-canonicalize.
func ValidateForRead(rawPath string, canonRoots []string, mode int) (*ValidatedPath, error) {
	if mode == ModeDisabled {
		return nil, pathErr("transfer_disabled",
			"file transfer disabled on this agent (allow_roots: [])")
	}
	if !filepath.IsAbs(rawPath) {
		return nil, pathErr("path_not_absolute",
			"%s: must be absolute", rawPath)
	}
	clean := filepath.Clean(rawPath)
	parent := filepath.Dir(clean)
	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, pathErr("path_not_found",
				"%s: parent directory does not exist", parent)
		}
		return nil, pathErr("io_error",
			"resolve parent of %s: %v", rawPath, err)
	}
	abs := filepath.Join(resolvedParent, filepath.Base(clean))
	// Containment applies only in narrow mode. In open mode matched stays
	// "" (whole-FS reach); the leaf checks below still run.
	matched := ""
	if mode == ModeNarrow {
		matched = containedIn(abs, canonRoots)
		if matched == "" {
			return nil, pathErr("path_outside_roots",
				"%s: not under any allow_root (%v)", abs, canonRoots)
		}
	}
	st, err := os.Lstat(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, pathErr("path_not_found", "%s: not found", abs)
		}
		return nil, pathErr("io_error", "lstat %s: %v", abs, err)
	}
	if err := rejectLeafSymlink(abs, st); err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, pathErr("not_a_regular_file",
			"%s: not a regular file (mode=%s)", abs, st.Mode())
	}
	return &ValidatedPath{
		Abs: abs, Parent: resolvedParent, Base: filepath.Base(clean),
		AllowRoot: matched,
	}, nil
}

// rejectLeafSymlink owns the symlink admission decision independently of the
// broader regular-file gate. Its metadata is the caller's existing Lstat result;
// obtaining it again here would introduce another filesystem race.
// origin: hot-update S4 evidence audit (AGENT-SLIM-2 survivor leaf-symlink-validation).
func rejectLeafSymlink(abs string, st os.FileInfo) error {
	if st.Mode()&os.ModeSymlink != 0 {
		return pathErr("not_a_regular_file",
			"%s: refuses to follow symlink", abs)
	}
	return nil
}

func validatedParentAndBase(vp *ValidatedPath) (string, string, error) {
	parent, base := vp.Parent, vp.Base
	if parent == "" {
		var err error
		parent, err = filepath.EvalSymlinks(filepath.Dir(vp.Abs))
		if err != nil {
			return "", "", err
		}
	}
	if base == "" {
		base = filepath.Base(vp.Abs)
	}
	return parent, base, nil
}

// openDirNoSymlinks opens an already-EvalSymlinks-resolved absolute
// directory one component at a time. Each component uses O_NOFOLLOW, so a
// hostile local process cannot replace a validated parent component with a
// symlink between validation and the eventual file operation.
func openDirNoSymlinks(absDir string) (int, error) {
	clean := filepath.Clean(absDir)
	if !filepath.IsAbs(clean) {
		return -1, fmt.Errorf("directory is not absolute: %s", absDir)
	}
	fd, err := unix.Open(string(filepath.Separator),
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	if clean == string(filepath.Separator) {
		return fd, nil
	}
	parts := strings.Split(strings.TrimPrefix(clean, string(filepath.Separator)), string(filepath.Separator))
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		next, openErr := unix.Openat(fd, part,
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		_ = unix.Close(fd)
		if openErr != nil {
			return -1, openErr
		}
		fd = next
	}
	return fd, nil
}

func unixModeIsRegular(mode uint32) bool {
	return mode&unix.S_IFMT == unix.S_IFREG
}

func unixModeIsSymlink(mode uint32) bool {
	return mode&unix.S_IFMT == unix.S_IFLNK
}

func dirFDStillNamesPath(dirFD int, parent string) (bool, error) {
	currentFD, err := openDirNoSymlinks(parent)
	if err != nil {
		return false, err
	}
	defer func() { _ = unix.Close(currentFD) }()
	var pinned, current unix.Stat_t
	if err := unix.Fstat(dirFD, &pinned); err != nil {
		return false, err
	}
	if err := unix.Fstat(currentFD, &current); err != nil {
		return false, err
	}
	return pinned.Dev == current.Dev && pinned.Ino == current.Ino, nil
}

// OpenForReadAtomic pins the canonical parent directory with openat,
// lstats and opens the leaf relative to that fd, then compares dev+inode.
// Parent and leaf swaps therefore fail closed instead of being followed.
func OpenForReadAtomic(vp *ValidatedPath) (*os.File, error) {
	parent, base, err := validatedParentAndBase(vp)
	if err != nil {
		return nil, pathErr("path_race", "resolve validated parent of %s: %v", vp.Abs, err)
	}
	dirFD, err := openDirNoSymlinks(parent)
	if err != nil {
		return nil, pathErr("path_race", "open validated parent %s: %v", parent, err)
	}
	defer func() { _ = unix.Close(dirFD) }()

	return openForReadAtomicAt(vp, parent, base, dirFD)
}

// openForReadAtomicAt borrows the pinned directory; the caller owns its close.
// Keeping the traversal separate lets tests replace the parent after pinning,
// rather than having traversal's O_NOFOLLOW mask the final identity check.
// origin: hot-update S4 evidence audit (AGENT-SLIM-2 survivor open-parent-current-identity).
func openForReadAtomicAt(vp *ValidatedPath, parent, base string, dirFD int) (*os.File, error) {
	var pre unix.Stat_t
	if err := unix.Fstatat(dirFD, base, &pre, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil, pathErr("path_not_found", "%s: vanished before open", vp.Abs)
		}
		return nil, pathErr("io_error", "lstat-pre %s: %v", vp.Abs, err)
	}
	if err := rejectReadSymlink(vp.Abs, pre); err != nil {
		return nil, err
	}
	if !unixModeIsRegular(uint32(pre.Mode)) {
		return nil, pathErr("not_a_regular_file",
			"%s: not regular before open (mode=%#o)", vp.Abs, pre.Mode)
	}

	fd, err := unix.Openat(dirFD, base, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, unix.ELOOP) {
			return nil, pathErr("not_a_regular_file",
				"%s: refused to follow symlink (O_NOFOLLOW)", vp.Abs)
		}
		if errors.Is(err, unix.ENOENT) {
			return nil, pathErr("path_not_found", "%s: vanished between lstat and open", vp.Abs)
		}
		return nil, pathErr("io_error", "open %s: %v", vp.Abs, err)
	}
	f := os.NewFile(uintptr(fd), vp.Abs)
	if f == nil {
		_ = unix.Close(fd)
		return nil, pathErr("io_error", "open %s: invalid file descriptor", vp.Abs)
	}
	var post unix.Stat_t
	if err := unix.Fstat(fd, &post); err != nil {
		_ = f.Close()
		return nil, pathErr("io_error", "fstat %s: %v", vp.Abs, err)
	}
	if pre.Dev != post.Dev || pre.Ino != post.Ino {
		_ = f.Close()
		return nil, pathErr("path_race",
			"%s: dev/inode changed between lstat and open (lstat=%d/%d open=%d/%d)",
			vp.Abs, pre.Dev, pre.Ino, post.Dev, post.Ino)
	}
	if !unixModeIsRegular(uint32(post.Mode)) {
		_ = f.Close()
		return nil, pathErr("not_a_regular_file",
			"%s: not regular after open (mode=%#o)", vp.Abs, post.Mode)
	}
	if same, err := dirFDStillNamesPath(dirFD, parent); err != nil || !same {
		_ = f.Close()
		return nil, pathErr("path_race",
			"%s: parent changed while opening (same=%v err=%v)", vp.Abs, same, err)
	}
	return f, nil
}

// rejectReadSymlink checks the pinned-directory metadata before any leaf open.
// It remains separately testable from the regular-file and O_NOFOLLOW layers;
// an end-to-end read cannot distinguish which of those layers denied a symlink.
// origin: hot-update S4 evidence audit (AGENT-SLIM-2 survivor open-symlink-guard).
func rejectReadSymlink(abs string, pre unix.Stat_t) error {
	if unixModeIsSymlink(uint32(pre.Mode)) {
		return pathErr("not_a_regular_file",
			"%s: refused to follow symlink", abs)
	}
	return nil
}

// AtomicWrite retains the validated parent fd from tmp creation through
// commit. Abort removes the tmp relative to that same fd, so cleanup cannot
// be redirected by swapping a parent path.
type AtomicWrite struct {
	dirFD   int
	tmpName string
	dstName string
	tmpPath string
	dstPath string
	parent  string
	done    bool

	// carryMode is the destination's existing permission bits, captured at OPEN time
	// (while the destination is known to exist and be regular) and applied at COMMIT
	// time. origin: prerelease audit round 2, I-F4 — see NewAtomicWrite's comment for
	// why the two must be separated.
	carryMode     os.FileMode
	haveCarryMode bool
}

func (w *AtomicWrite) Abort() {
	if w == nil || w.done {
		return
	}
	_ = unix.Unlinkat(w.dirFD, w.tmpName, 0)
	_ = unix.Close(w.dirFD)
	w.done = true
}

func (w *AtomicWrite) finish() {
	if w.done {
		return
	}
	_ = unix.Close(w.dirFD)
	w.done = true
}

// OpenForWriteAtomic creates a tmp sibling relative to a pinned canonical
// parent fd. The caller writes, fsyncs, closes, then commits through
// RenameForWriteAtomic. A deferred pending.Abort() is required.
func OpenForWriteAtomic(vp *ValidatedPath, randSuffix string) (*os.File, *AtomicWrite, error) {
	parent, base, err := validatedParentAndBase(vp)
	if err != nil {
		return nil, nil, pathErr("path_race",
			"resolve validated parent of %s: %v", vp.Abs, err)
	}
	dirFD, err := openDirNoSymlinks(parent)
	if err != nil {
		return nil, nil, pathErr("path_race",
			"open validated parent %s: %v", parent, err)
	}
	tmpName := base + ".tmp." + randSuffix
	tmpPath := filepath.Join(parent, tmpName)
	fd, err := unix.Openat(dirFD, tmpName,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		0o600)
	if err != nil {
		_ = unix.Close(dirFD)
		if errors.Is(err, unix.EEXIST) {
			return nil, nil, pathErr("io_error",
				"%s: tmp file exists (stale write?)", tmpPath)
		}
		if errors.Is(err, unix.ELOOP) {
			return nil, nil, pathErr("not_a_regular_file",
				"%s: refused symlink tmp leaf (O_NOFOLLOW)", tmpPath)
		}
		return nil, nil, pathErr("io_error", "open %s: %v", tmpPath, err)
	}
	f := os.NewFile(uintptr(fd), tmpPath)
	if f == nil {
		_ = unix.Close(fd)
		_ = unix.Unlinkat(dirFD, tmpName, 0)
		_ = unix.Close(dirFD)
		return nil, nil, pathErr("io_error", "open %s: invalid file descriptor", tmpPath)
	}

	// CARRY THE EXISTING FILE'S PERMISSION BITS ONTO THE REPLACEMENT.
	//
	// origin: prerelease audit agent-transfer/L3-F1. An atomic replace creates the tmp
	// at 0600 and renames it over the destination, so `push --force` onto an existing
	// file silently reset its mode: a 0755 script became non-executable, a 0644 config
	// became unreadable to the group that had been reading it. The operator asked to
	// replace the CONTENT.
	//
	// Done HERE, on the fd, and not by name after the rename. This function is the one
	// place that holds the parent dirFD, the destination name and a still-open handle at
	// the same time, so fstatat-then-fchmod has nothing to race against; a path-based
	// chmod after the rename is a TOCTOU window on a directory the caller does not
	// control.
	//
	// MASKED TO 0o777, deliberately not 0o7777: setuid, setgid and the sticky bit are
	// properties of the file that WAS there, and re-applying them to bytes that just
	// arrived over the network would be handing an uploader whatever privilege the old
	// file carried.
	//
	// HONEST LIMIT: this restores the mode, NOT the owner. A root agent replacing a file
	// owned by another user still ends up with a root-owned file, because fchown needs
	// privileges the agent may not have and silently guessing is worse than leaving the
	// ownership visible. A no-force push cannot hit either case: linkat refuses when the
	// destination exists.
	// CAPTURED HERE, APPLIED AT COMMIT — origin: prerelease audit round 2, I-F4. The
	// first version chmod-ed the tmp right here, i.e. BEFORE a single byte of content had
	// been written. For the whole duration of a multi-gigabyte transfer a
	// group- or world-writable file therefore sat in the destination directory under a
	// predictable name, and because the sha256 is computed over the STREAM and not over
	// the file, bytes substituted in that window commit as verified. Carrying the mode
	// forward and setting it at rename time preserves the property the fix is for
	// (the operator's 0755 script stays executable) without opening the window.
	//
	// The READ stays here: this is where the destination is known to exist, and after the
	// rename it is gone.
	w := &AtomicWrite{
		dirFD: dirFD, tmpName: tmpName, dstName: base,
		tmpPath: tmpPath, dstPath: vp.Abs, parent: parent,
	}
	var dst unix.Stat_t
	if serr := unix.Fstatat(dirFD, base, &dst, unix.AT_SYMLINK_NOFOLLOW); serr == nil &&
		unixModeIsRegular(uint32(dst.Mode)) {
		w.carryMode, w.haveCarryMode = os.FileMode(dst.Mode&0o777), true
	}
	return f, w, nil
}

// RenameForWriteAtomic commits a populated tmp. No-force uses linkat as
// an atomic create-if-absent operation; unlike Lstat+Rename it cannot
// overwrite a destination created in the check/rename window.
func RenameForWriteAtomic(vp *ValidatedPath, pending *AtomicWrite, force bool) error {
	if pending == nil || pending.done || pending.dstPath != vp.Abs {
		return pathErr("io_error", "invalid or completed atomic write for %s", vp.Abs)
	}
	if same, err := dirFDStillNamesPath(pending.dirFD, pending.parent); err != nil || !same {
		return pathErr("path_race",
			"%s: parent changed before commit (same=%v err=%v)", vp.Abs, same, err)
	}
	// Apply the carried mode to the TMP, immediately before it becomes the destination
	// (round 2, I-F4). Fchmodat against the pinned dirFD and our own tmp name — not a
	// path-based chmod after the rename, which would be a TOCTOU window on a directory
	// the caller does not control, and which is why the original put this on the fd.
	//
	// Not fatal if it fails: the CONTENT is what the operator asked for, and a filesystem
	// that refuses the chmod is not a reason to fail the transfer.
	if pending.haveCarryMode {
		_ = unix.Fchmodat(pending.dirFD, pending.tmpName, uint32(pending.carryMode), 0)
	}
	if force {
		if err := unix.Renameat(
			pending.dirFD, pending.tmpName,
			pending.dirFD, pending.dstName,
		); err != nil {
			return pathErr("io_error", "rename %s -> %s: %v",
				pending.tmpPath, vp.Abs, err)
		}
		pending.finish()
		return nil
	}

	if err := unix.Linkat(
		pending.dirFD, pending.tmpName,
		pending.dirFD, pending.dstName, 0,
	); err != nil {
		if errors.Is(err, unix.EEXIST) {
			return pathErr("dst_exists",
				"%s: destination exists; pass --force to overwrite", vp.Abs)
		}
		return pathErr("io_error", "link %s -> %s: %v",
			pending.tmpPath, vp.Abs, err)
	}
	// Destination is committed. A failed tmp unlink only leaves a second
	// hardlink to the same inode; it must not turn a successful commit into
	// a retry that would report dst_exists.
	_ = unix.Unlinkat(pending.dirFD, pending.tmpName, 0)
	pending.finish()
	return nil
}

// The image-shape leaf validates only values; filesystem custody stays here.
// origin: hot-update S7 plan §3 / §8.2 / S7 internal review rv-wiring F9
const inlineAtomicMaxBytes = imageref.InlineAtomicMaxBytes

var validAtomicGrant = imageref.ValidAtomicGrant
var validateAtomicDecision = imageref.ValidateAtomicDecision

// The default port refuses before Take; the Linux syscall owner installs the
// original-OFD importer during package initialization. This shared facade keeps
// the non-Linux refusal in the same package without another platform twin.
// The former !linux build constraint is the default refusal until Linux init.
// origin: hot-update S7 plan §9 / S7 internal review rv-wiring F9
var importAtomicReceiver ImportFactory = unsupportedAtomicImport

func unsupportedAtomicImport(xferport.AtomicState, FileSource) (AtomicReceiver, xferport.Salvage[handoff.Resource], error) {
	return nil, xferport.Salvage[handoff.Resource]{}, handoff.ErrUnsupported
}

// Refuse before Take on platforms without the original-OFD import contract.
// Ordinary transfers still use the common path and atomic commit operations.
// origin: hot-update S7 plan §9 / T10
func ImportAtomic(state xferport.AtomicState, files FileSource) (AtomicReceiver, xferport.Salvage[handoff.Resource], error) {
	return importAtomicReceiver(state, files)
}
