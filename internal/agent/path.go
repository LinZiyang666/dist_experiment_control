package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

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

// canonicalAllowRoots cleans + de-duplicates the allow_roots list and
// returns each entry in EvalSymlinks-resolved form. Any allow_root that
// fails EvalSymlinks (does not exist, is not a directory, contains an
// unresolvable symlink) is silently dropped — operator misconfiguration
// must not turn an allow_root accident into a "no roots → allow
// everything" footgun. The result is always sorted by length descending
// so the longest match wins (so /srv/local/alice does not get shadowed
// by /srv).
func canonicalAllowRoots(roots []string) []string {
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

// containedIn returns the matched allow_root or "" if path is not
// inside any of them. Comparison is "<resolved-leaf>/" vs "<root>/" so
// /srv/localfoo doesn't match /srv/local. Both sides are
// EvalSymlinks-resolved already (caller's responsibility).
func containedIn(resolvedPath string, roots []string) string {
	needle := resolvedPath + "/"
	for _, r := range roots {
		hay := r + "/"
		if strings.HasPrefix(needle, hay) {
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
	Abs       string // EvalSymlinks-resolved-parent + clean leaf; safe to open with O_NOFOLLOW
	AllowRoot string
}

// ValidateForWrite is the push-side check: agent is going to create a
// file at this path and write bytes. Steps follow file-transfer-plan
// §"Refusing dangerous paths":
//
//  1. allow_roots non-empty (else `transfer_disabled`).
//  2. absolute path required.
//  3. EvalSymlinks-resolve the parent dir; ENOENT → path_parent_missing
//     (parent dirs are NOT auto-created in v2.0).
//  4. <resolved-parent>/<base> must be inside one allow_root.
//  5. If destination already exists, must be a regular file (not a
//     symlink, dir, device, fifo) — symlink → not_a_regular_file.
//
// Caller still opens with O_NOFOLLOW|O_EXCL|O_CREAT to defeat any
// race-window symlink swap between this validation and open. dst-
// exists is NOT considered an error here (the caller's `--force`
// logic decides what to do); the leaf-symlink check still happens
// before that decision so a symlink dest can never silently dereference.
func ValidateForWrite(rawPath string, allowRoots []string) (*ValidatedPath, error) {
	if len(allowRoots) == 0 {
		return nil, pathErr("transfer_disabled",
			"file transfer disabled on this agent (allow_roots is empty)")
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
	roots := canonicalAllowRoots(allowRoots)
	matched := containedIn(abs, roots)
	if matched == "" {
		return nil, pathErr("path_outside_roots",
			"%s: not under any allow_root (%v)", abs, roots)
	}
	// If the leaf already exists, it must be a regular file. A symlink
	// at the leaf is rejected here so a follow-up O_NOFOLLOW open
	// failure isn't the operator's only signal.
	if st, err := os.Lstat(abs); err == nil {
		if st.Mode()&os.ModeSymlink != 0 {
			return nil, pathErr("not_a_regular_file",
				"%s: refuses to follow symlink", abs)
		}
		if !st.Mode().IsRegular() {
			return nil, pathErr("not_a_regular_file",
				"%s: not a regular file (mode=%s)", abs, st.Mode())
		}
	} else if !os.IsNotExist(err) {
		return nil, pathErr("io_error", "lstat %s: %v", abs, err)
	}
	return &ValidatedPath{Abs: abs, AllowRoot: matched}, nil
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
func ValidateForRead(rawPath string, allowRoots []string) (*ValidatedPath, error) {
	if len(allowRoots) == 0 {
		return nil, pathErr("transfer_disabled",
			"file transfer disabled on this agent (allow_roots is empty)")
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
	roots := canonicalAllowRoots(allowRoots)
	matched := containedIn(abs, roots)
	if matched == "" {
		return nil, pathErr("path_outside_roots",
			"%s: not under any allow_root (%v)", abs, roots)
	}
	st, err := os.Lstat(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, pathErr("path_not_found", "%s: not found", abs)
		}
		return nil, pathErr("io_error", "lstat %s: %v", abs, err)
	}
	if st.Mode()&os.ModeSymlink != 0 {
		return nil, pathErr("not_a_regular_file",
			"%s: refuses to follow symlink", abs)
	}
	if !st.Mode().IsRegular() {
		return nil, pathErr("not_a_regular_file",
			"%s: not a regular file (mode=%s)", abs, st.Mode())
	}
	return &ValidatedPath{Abs: abs, AllowRoot: matched}, nil
}

// OpenForReadAtomic is the open half of the pull-side TOCTOU defence:
// after ValidateForRead returns vp, call this to get a regular-file fd.
// It re-stats via the fd and verifies dev+inode match what lstat
// captured; on mismatch the file changed type / was swapped underneath
// us between lstat and open → path_race.
func OpenForReadAtomic(vp *ValidatedPath) (*os.File, error) {
	preLstat, err := os.Lstat(vp.Abs)
	if err != nil {
		return nil, pathErr("io_error", "lstat-pre %s: %v", vp.Abs, err)
	}
	preSys, ok := preLstat.Sys().(*syscall.Stat_t)
	if !ok {
		// Non-Linux fallback: just open with NOFOLLOW; we lose
		// dev+inode TOCTOU evidence but the open itself still
		// refuses to follow a symlink at the leaf.
		return os.OpenFile(vp.Abs, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	}
	f, err := os.OpenFile(vp.Abs, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		// O_NOFOLLOW on a symlink leaf returns ELOOP on Linux.
		var perr *os.PathError
		if errors.As(err, &perr) && errors.Is(perr.Err, syscall.ELOOP) {
			return nil, pathErr("not_a_regular_file",
				"%s: refused to follow symlink (O_NOFOLLOW)", vp.Abs)
		}
		if os.IsNotExist(err) {
			return nil, pathErr("path_not_found", "%s: vanished between lstat and open", vp.Abs)
		}
		return nil, pathErr("io_error", "open %s: %v", vp.Abs, err)
	}
	postStat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, pathErr("io_error", "fstat %s: %v", vp.Abs, err)
	}
	postSys, ok := postStat.Sys().(*syscall.Stat_t)
	if !ok {
		return f, nil
	}
	if preSys.Dev != postSys.Dev || preSys.Ino != postSys.Ino {
		_ = f.Close()
		return nil, pathErr("path_race",
			"%s: dev/inode changed between lstat and open (lstat=%d/%d open=%d/%d)",
			vp.Abs, preSys.Dev, preSys.Ino, postSys.Dev, postSys.Ino)
	}
	if !postStat.Mode().IsRegular() {
		_ = f.Close()
		return nil, pathErr("not_a_regular_file",
			"%s: not regular after open (mode=%s)", vp.Abs, postStat.Mode())
	}
	return f, nil
}

// OpenForWriteAtomic creates a tmp sibling under the same parent dir
// using O_NOFOLLOW|O_EXCL|O_CREAT|O_WRONLY (mode 0600). The caller
// writes bytes, fsyncs, then RenameForWriteAtomic replaces the final
// destination atomically. The tmp filename uses a "<base>.tmp.<rand>"
// pattern so a partial write left behind by a crashed agent is easy to
// spot and clean up. If the destination already exists, callers
// decide via `force` whether to rename-overwrite or fail.
func OpenForWriteAtomic(vp *ValidatedPath, randSuffix string) (*os.File, string, error) {
	tmp := vp.Abs + ".tmp." + randSuffix
	f, err := os.OpenFile(tmp,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW,
		0o600)
	if err != nil {
		// O_EXCL on an existing tmp (collision with a stale crash
		// remainder, or an attacker pre-creating the path) → bubble.
		var perr *os.PathError
		if errors.As(err, &perr) && errors.Is(perr.Err, syscall.EEXIST) {
			return nil, "", pathErr("io_error",
				"%s: tmp file exists (stale write?)", tmp)
		}
		if errors.As(err, &perr) && errors.Is(perr.Err, syscall.ELOOP) {
			return nil, "", pathErr("not_a_regular_file",
				"%s: tmp parent traversed a symlink (O_NOFOLLOW)", tmp)
		}
		return nil, "", pathErr("io_error", "open %s: %v", tmp, err)
	}
	return f, tmp, nil
}

// RenameForWriteAtomic moves the populated tmp into vp.Abs. If
// `force` is false and vp.Abs exists, returns dst_exists without
// touching anything. Otherwise calls os.Rename which is POSIX-atomic
// on the same filesystem (the tmp is a sibling so this always holds).
func RenameForWriteAtomic(vp *ValidatedPath, tmpPath string, force bool) error {
	if !force {
		if _, err := os.Lstat(vp.Abs); err == nil {
			return pathErr("dst_exists",
				"%s: destination exists; pass --force to overwrite", vp.Abs)
		} else if !os.IsNotExist(err) {
			return pathErr("io_error", "lstat %s: %v", vp.Abs, err)
		}
	}
	if err := os.Rename(tmpPath, vp.Abs); err != nil {
		return pathErr("io_error", "rename %s -> %s: %v", tmpPath, vp.Abs, err)
	}
	return nil
}
