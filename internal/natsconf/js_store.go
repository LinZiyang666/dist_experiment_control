package natsconf

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// JSStoreHasData reports whether a JetStream store dir holds REAL data (a returning node's stale streams /
// clustered meta) rather than being absent, empty, or only the STRUCTURAL directory skeleton a freshly-booted
// JS-enabled nats-server lays down with no streams. Heuristic: the tree contains at least one regular file
// with content (>0 bytes) — stream message blocks / meta snapshots are non-empty; the boot skeleton is empty
// dirs (and any 0-byte markers). Used by the R16 A1 grow joiner reset so a truly-fresh joiner is a no-op (no
// false data-loss HALT, no drill-10/11 byte-equivalence break) while a returning node's stale store is reset.
// A walk error fails CLOSED (treat as data-bearing) — a store we cannot enumerate must not be silently skipped.
func JSStoreHasData(storeDir string) (bool, error) {
	if storeDir == "" {
		return false, nil
	}
	// EXTERNAL REVIEW F4a: the doc comment promised "a walk error fails CLOSED", but EVERY root Stat
	// error returned false — permission denied, ELOOP, I/O error all read as "absent, nothing to reset".
	// A returning joiner then skipped the reset and booted its stale clustered meta, re-opening the grow
	// wedge R16 exists to close. Only IsNotExist means absent; everything else is fail-closed.
	fi, err := os.Stat(storeDir)
	switch {
	case os.IsNotExist(err):
		return false, nil // genuinely absent — nothing to reset
	case err != nil:
		return true, fmt.Errorf("natsconf: cannot stat JetStream store %q: %w", storeDir, err)
	}
	// EXTERNAL REVIEW F4b: os.Stat FOLLOWS a symlink but filepath.WalkDir does NOT descend through the
	// root symlink, so a store reached via an ordinary symlink walked as an empty tree and a real
	// data-bearing store reported "no data". Resolve the root before walking.
	if lst, lerr := os.Lstat(storeDir); lerr == nil && lst.Mode()&os.ModeSymlink != 0 {
		resolved, rerr := filepath.EvalSymlinks(storeDir)
		if rerr != nil {
			return true, fmt.Errorf("natsconf: cannot resolve JetStream store symlink %q: %w", storeDir, rerr)
		}
		storeDir = resolved
		if fi, err = os.Stat(storeDir); err != nil {
			return true, fmt.Errorf("natsconf: cannot stat resolved JetStream store %q: %w", storeDir, err)
		}
	}
	if !fi.IsDir() {
		return true, fmt.Errorf("natsconf: JetStream store %q is not a directory", storeDir)
	}
	found := false
	walkErr := filepath.WalkDir(storeDir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			info, ierr := d.Info()
			if ierr != nil {
				return ierr // F4: an entry we cannot stat must fail CLOSED, not be skipped as empty
			}
			if info.Size() > 0 {
				found = true
				return filepath.SkipAll
			}
		}
		return nil
	})
	if walkErr != nil {
		// fail closed — an un-enumerable store is treated as data-bearing (never silently skip)
		return true, fmt.Errorf("natsconf: cannot enumerate JetStream store %q: %w", storeDir, walkErr)
	}
	return found, nil
}

// ErrJSStoreNeedsAck marks the ONE refusal from MoveAsideJSStore that the caller's reset flag actually
// clears: "this store holds data, acknowledge it". Every other refusal — a permission denial, a broken
// symlink, an I/O error, a rename failure — is an INSPECTION or WRITE failure that the flag does nothing
// about, because the flag only supplies `ack`.
//
// origin: line-2 closure verification m9. All three CLI wrappers appended "re-run with --reset-js"
// (or --reset-former-js) to EVERY error from this function. On a permission error that instruction is
// worse than silence: the operator re-runs the exact command they were told to, it fails identically, and
// the real cause — a root-owned data dir — is never named. Callers must now branch on this sentinel and
// give the flag advice only when the flag is the remedy.
var ErrJSStoreNeedsAck = errors.New("JetStream store reset needs acknowledgement")

// MoveAsideJSStore renames a JetStream store dir to backupPath (NEVER deletes) so the node boots with a
// fresh empty store, then recreates the empty store dir. A NON-EMPTY (data-bearing) store refuses without
// ack (loud, operator-gated): the store carries real history/audit/in-flight OBJ_xfer state. That one
// refusal — and only that one — wraps ErrJSStoreNeedsAck.
//
// Idempotency is BACKUP-DIR-FIRST (M3): the durable evidence a move already happened is the backup dir
// itself, checked before anything else — keying idempotency on the (last-written, crash-losable) sentinel
// alone wedged a resume with ENOTEMPTY (rename of the recreated-empty store over the data-bearing backup).
// The sentinel is an advisory last-write marker; if it is present but backupPath is GONE (the operator
// moved the backup away after a successful move) the move is treated as already-done (no-op, "").
//
// An EACCES on the rename fails LOUD with the broker-ops #6 chown hint (a root-owned data dir after a
// root-run restore/install leaves the User=tether daemon unable to move its own store) — never skip.
//
// It is the ONE move-aside path shared by the former-N1 grow cutover, the grow joiner reset (R16 A1), the
// offline force-single reset (A3), and reconcile-nats --to-standalone --reset-js (A4). Returns the backup
// path actually in place ("" if nothing was moved).
func MoveAsideJSStore(storeDir, backupPath, sentinelPath string, ack bool) (string, error) {
	if storeDir == "" {
		return "", nil // no explicit store dir — nothing to move (render already refused an implicit one)
	}
	// EXTERNAL REVIEW F5: prior evidence (a backup dir, or a sentinel) proves only that SOME earlier move
	// happened — never that the CURRENT store is still the empty directory that move left behind. Two real
	// windows made that difference matter:
	//
	//   1. --to-standalone's backup name is second-precision. First move succeeds, Apply then fails, the
	//      still-running nats-server writes into the recreated dir, and a retry inside the same second
	//      matches the old backup and SKIPS the now-live data.
	//   2. The operator archives the backup after a force-single/grow reset; the stale sentinel then
	//      exempts a store that has since become data-bearing again, permanently.
	//
	// So old evidence is honoured ONLY while the current store is genuinely empty. If it has data, we fall
	// through to the normal path, which requires the operator ack and moves it aside for real.
	// M3: the BACKUP DIR is the durable, EPOCH-SCOPED evidence — its name carries the operation's epoch
	// (e.g. `jetstream.grow-bak.<epoch>`), so its presence can only mean "THIS operation already moved the
	// store aside". A resume must then be a no-op even though the recreated store now holds a fresh
	// clustered skeleton, or the resume would move the FRESH store aside and lose the real backup.
	if _, berr := os.Stat(backupPath); berr == nil {
		return backupPath, nil
	}
	// The SENTINEL is different, and external review F5 is right about it: with the backup gone it has no
	// surviving artifact at all, so it proves only that SOME earlier move happened — never that the
	// current store is still the empty directory that move left behind. Honouring it unconditionally let a
	// store that has since become data-bearing again (operator archived the backup, nats-server wrote new
	// data) be exempted from reset PERMANENTLY. So it may only excuse an EMPTY store; a data-bearing one
	// falls through to the normal ack-gated path below.
	if sentinelPath != "" {
		if _, serr := os.Stat(sentinelPath); serr == nil {
			empty, emptyErr := jsStoreDirIsEmpty(storeDir)
			if emptyErr != nil {
				return "", emptyErr // fail closed: an unreadable store is never assumed already-reset
			}
			if empty {
				return "", nil
			}
		}
	}
	// This stat used to be `if fi, serr := os.Stat(storeDir); serr != nil || !fi.IsDir()` — ANY stat
	// error meant "nothing to reset". That is the one fail-OPEN left in a function whose other two
	// disk checks both fail CLOSED (the sentinel branch above, and m4's ReadDir branch below), and the
	// asymmetry was not deliberate: os.Stat fails for permission denial, a broken symlink and I/O
	// errors as well as for absence, and in every one of those the store may exist and hold data. The
	// early return also jumps PAST m4's protection, so the "treat an unenumerable store as
	// data-bearing" rule never got a chance to apply.
	fi, serr := os.Stat(storeDir)
	switch {
	case os.IsNotExist(serr):
		return "", nil // no store dir on disk yet — genuinely nothing to reset
	case errors.Is(serr, os.ErrPermission):
		// origin: line-2 closure verification m9. The generic branch below is honest about WHY it refuses
		// but gives the operator nothing to do about it — and the overwhelmingly common cause of a stat
		// failure on this path is exactly the one the Rename branch 20 lines down already knows how to
		// explain: the data dir was created or restored as root. Repeating that remedy here is not
		// duplication, it is the difference between a diagnosis and an instruction.
		//
		// Kept as its own branch rather than folded into the generic one so that the advice is only given
		// when it is TRUE: telling someone to chown after an EIO would send them down the wrong path.
		return "", fmt.Errorf("natsconf: cannot stat JetStream store %q: %w — refusing to assume it is "+
			"absent (a store that cannot be inspected must be treated as data-bearing). This is a "+
			"PERMISSION error, not a missing store: the data dir is not readable by this user. If you "+
			"restored or installed as root, chown it back (broker-ops #6: `chown -R tether:tether` the "+
			"data dir), then re-run", storeDir, serr)
	case serr != nil:
		return "", fmt.Errorf("natsconf: cannot stat JetStream store %q: %w — refusing to assume it is "+
			"absent (a store that cannot be inspected must be treated as data-bearing)", storeDir, serr)
	case !fi.IsDir():
		// FAIL CLOSED. origin: line-2 INDEPENDENT EXTERNAL REVIEW M5.
		//
		// This returned ("", nil) — "nothing to reset" — for a path that EXISTS with the wrong filesystem
		// shape. That is not an absent store; it is corruption or a misconfigured store_dir, and the
		// callers of this function are force-single, the grow cutover and reconcile --to-standalone. All
		// three then proceeded through a destructive lifecycle having neither inspected, moved, nor
		// protected the thing sitting at that path.
		//
		// It was also inconsistent with its own neighbours: the sentinel branch above and m4's ReadDir
		// branch below both fail closed, and JSStoreHasData errors on a non-directory. The one fail-open
		// left in the function was the one nobody had a test for.
		//
		// Only ENOENT may be a no-op. Everything else — non-directory, permission, I/O, broken symlink —
		// gets a path and a remedy.
		return "", fmt.Errorf("natsconf: JetStream store path %q exists but is not a directory (mode %s) — "+
			"refusing to continue. A configured store_dir with the wrong shape is corruption or a "+
			"misconfiguration, NOT an absent store, and treating it as absent would let a destructive "+
			"lifecycle step (force-single / grow cutover / reconcile --to-standalone) run without "+
			"inspecting or preserving whatever is there. Inspect the path by hand: if it is a stray file, "+
			"move it aside; if store_dir points at the wrong place, fix the conf", storeDir, fi.Mode())
	}
	// m4: fail CLOSED on a ReadDir error — a store we cannot enumerate must be treated as POTENTIALLY
	// data-bearing (require the operator ack), never silently as empty (a gate whose job is to fail loud).
	entries, rerr := os.ReadDir(storeDir)
	if (rerr != nil || len(entries) > 0) && !ack {
		reason := "is NON-EMPTY (data-bearing)"
		if rerr != nil {
			reason = "could not be enumerated (" + rerr.Error() + ") — treating it as data-bearing"
		}
		return "", fmt.Errorf("%w: JetStream store %q %s — refusing to reset it without acknowledgement; "+
			"re-run with the reset flag (the store is MOVED aside to %s; it is NEVER deleted, and you can restore it by hand)",
			ErrJSStoreNeedsAck, storeDir, reason, backupPath)
	}
	if err := os.Rename(storeDir, backupPath); err != nil {
		if errors.Is(err, os.ErrPermission) {
			return "", fmt.Errorf("move JS store aside to %s: %w — the data dir is not writable by this user; "+
				"if you restored or installed as root, chown it back (broker-ops #6: `chown -R tether:tether` the data dir)",
				backupPath, err)
		}
		return "", fmt.Errorf("move JS store aside: %w", err)
	}
	if err := os.MkdirAll(storeDir, 0o700); err != nil {
		return backupPath, fmt.Errorf("recreate empty JS store dir: %w", err)
	}
	if sentinelPath != "" {
		// Advisory only — the backup dir is the durable idempotency guard, so a lost sentinel is harmless.
		_ = os.WriteFile(sentinelPath, []byte("backup="+backupPath+"\n"), 0o600)
	}
	return backupPath, nil
}

// jsStoreDirIsEmpty reports whether the store dir currently holds nothing (external review F5). Absent
// counts as empty. Any error is returned rather than swallowed: an unreadable store must never be
// mistaken for "already reset by an earlier move".
func jsStoreDirIsEmpty(storeDir string) (bool, error) {
	entries, err := os.ReadDir(storeDir)
	switch {
	case os.IsNotExist(err):
		return true, nil
	case err != nil:
		return false, fmt.Errorf("natsconf: cannot enumerate JetStream store %q to verify a prior reset: %w", storeDir, err)
	}
	return len(entries) == 0, nil
}
