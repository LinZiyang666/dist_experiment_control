package agent

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// canonDir resolves symlinks in a test dir path. t.TempDir() is not
// guaranteed canonical (macOS: /var → /private/var), while
// CanonAllowRoots / ValidateFor* return EvalSymlinks-resolved paths —
// expectations must compare against the resolved form.
func canonDir(t *testing.T, dir string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", dir, err)
	}
	return resolved
}

// validateFn is the shared shape of ValidateForRead / ValidateForWrite,
// used by the table tests that exercise both directions.
type validateFn func(string, []string, transferMode) (*ValidatedPath, error)

func bothValidators() []validateFn { return []validateFn{ValidateForRead, ValidateForWrite} }

// resolveTransferMode is the load-bearing security invariant: mode is
// decided from RAW config presence, NEVER from len(canonAllowRoots), so a
// narrow config whose roots all drop during canonicalization stays
// modeNarrow (reject-all) and never collapses to whole-FS open.
func TestResolveTransferMode(t *testing.T) {
	tests := []struct {
		name            string
		rootsConfigured bool
		rawRoots        []string
		want            transferMode
	}{
		{"absent key (fresh install)", false, nil, modeOpen},
		{"absent key, empty slice (not configured)", false, []string{}, modeOpen},
		{"explicit empty list", true, []string{}, modeDisabled},
		{"non-empty, configured", true, []string{"/x"}, modeNarrow},
		{"non-empty, unflagged still narrow", false, []string{"/x"}, modeNarrow},
		// The fail-closed guarantee: a narrow config is decided on the RAW
		// list, so even roots that will all drop in CanonAllowRoots resolve
		// to modeNarrow here — never modeOpen.
		{"non-empty but will-all-drop stays narrow", true, []string{"/no/such/dir"}, modeNarrow},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveTransferMode(tc.rootsConfigured, tc.rawRoots); got != tc.want {
				t.Errorf("resolveTransferMode(%v, %v) = %d, want %d", tc.rootsConfigured, tc.rawRoots, got, tc.want)
			}
		})
	}
}

// End-to-end fail-closed: a narrow config whose roots ALL drop during
// canonicalization must, when run through agent.New, still reject every path
// (modeNarrow + empty canon), never fall open. Ties resolveTransferMode →
// CanonAllowRoots → Validate in the single flow New actually wires (Round-1
// review: the two halves were pinned separately but never together).
func TestNew_AllDroppedNarrowFailsClosed(t *testing.T) {
	a, err := New(Config{
		NATSURL:         "nats://127.0.0.1:4222",
		SID:             "lab",
		NID:             "a100",
		RootsConfigured: true,
		AllowRoots:      []string{"/no/such/dir", "relative", ""},
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.transferMode != modeNarrow {
		t.Fatalf("transferMode = %d, want modeNarrow (all-dropped narrow must not fall open)", a.transferMode)
	}
	if len(a.canonAllowRoots) != 0 {
		t.Fatalf("canonAllowRoots = %v, want empty (all dropped)", a.canonAllowRoots)
	}
	for _, fn := range bothValidators() {
		_, err := fn("/etc/hosts", a.canonAllowRoots, a.transferMode)
		var pve *PathValidationError
		if !errors.As(err, &pve) || pve.Code != "path_outside_roots" {
			t.Errorf("got %v, want path_outside_roots (fail-closed)", err)
		}
	}
}

// Open mode must reject a DIRECTORY at the leaf on both push and pull. With
// containment gone, the regular-file guard is the load-bearing reject in open
// mode (Round-1 disposition #14). Assert the directory is untouched.
func TestValidate_OpenMode_RejectsDirLeaf(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "a-dir")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, fn := range bothValidators() {
		_, err := fn(dir, nil, modeOpen)
		var pve *PathValidationError
		if !errors.As(err, &pve) || pve.Code != "not_a_regular_file" {
			t.Errorf("open-mode dir leaf: got %v, want not_a_regular_file", err)
		}
	}
	if fi, statErr := os.Lstat(dir); statErr != nil || !fi.IsDir() {
		t.Errorf("dir leaf altered: %v", statErr)
	}
}

// Partial-drop narrow: a config with SOME valid + SOME dropping roots must
// keep the survivors working AND still reject paths outside them — neither
// fall open nor over-reject. This is the typo'd / transiently-unmounted-root
// footgun the RAW-vs-canon design exists to handle (plan §"Rejected 1a").
func TestNew_PartialDropNarrow(t *testing.T) {
	good := t.TempDir()
	a, err := New(Config{
		NATSURL:         "nats://127.0.0.1:4222",
		SID:             "lab",
		NID:             "a100",
		RootsConfigured: true,
		AllowRoots:      []string{good, "/no/such/dir", "relative"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.transferMode != modeNarrow {
		t.Fatalf("transferMode = %d, want modeNarrow", a.transferMode)
	}
	if len(a.canonAllowRoots) != 1 {
		t.Fatalf("canonAllowRoots = %v, want exactly the surviving good root", a.canonAllowRoots)
	}
	// Survivor root still works.
	src := filepath.Join(good, "f.bin")
	if err := os.WriteFile(src, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if vp, err := ValidateForRead(src, a.canonAllowRoots, a.transferMode); err != nil {
		t.Errorf("survivor root path rejected: %v", err)
	} else if vp.AllowRoot == "" {
		t.Errorf("survivor AllowRoot empty, want the good root")
	}
	// A real path OUTSIDE the survivor is still rejected by containment.
	other := t.TempDir()
	otherFile := filepath.Join(other, "f.bin")
	if err := os.WriteFile(otherFile, []byte("y"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = ValidateForRead(otherFile, a.canonAllowRoots, a.transferMode)
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "path_outside_roots" {
		t.Errorf("path outside survivor: got %v, want path_outside_roots", err)
	}
}

func TestPushCommitCacheExpiresAndStaysBounded(t *testing.T) {
	now := time.Now()
	// Expiry is PER ENTRY, keyed on that entry's own size, so a tiny transfer's prep is short-lived.
	a := &Agent{pushCommitCache: map[string]pushCommitEntry{
		"stale": {added: now.Add(-pushCommitEntryTTL(0) - time.Second)},
	}}
	if !a.rememberPushCommit("fresh", pushCommitEntry{}) {
		t.Fatal("remember refused with a nearly-empty cache")
	}
	if _, ok := a.pushCommitCache["stale"]; ok {
		t.Fatal("stale push commit entry was not purged")
	}
	if _, ok := a.pushCommitCache["fresh"]; !ok {
		t.Fatal("fresh push commit entry missing")
	}
}

// TestPushCommitCacheNeverEvictsALiveLargeTransfer pins the batch-C behaviour change, and it is the
// test that has to be BEHAVIOURAL rather than a constant inequality.
//
// The pre-batch-C cache had TWO eviction paths: the TTL sweep, and "at capacity, delete the entry
// with the earliest `added`". Making a large transfer's prep entry live longer — which the size-
// derived budget REQUIRES, since the ctl now takes ~17 minutes to upload 2 GiB — makes that second
// path fire on precisely the entry being protected: the oldest live entry is by construction the
// longest-running, largest transfer. A test asserting only `TTL > uploadBudget` would stay green
// while the capacity path silently killed the transfer.
//
// Mutations: (a) restore evict-oldest instead of refusing; (b) make pushCommitEntryTTL a single
// global constant again. Either reddens.
func TestPushCommitCacheNeverEvictsALiveLargeTransfer(t *testing.T) {
	now := time.Now()
	a := &Agent{pushCommitCache: map[string]pushCommitEntry{}}

	// The entry we must not lose: a maximum-size transfer whose upload has been running for a quarter
	// of an hour. Under the old flat 6-minute TTL it would already be gone.
	big := pushCommitEntry{size: proto.XferMaxBytes, added: now.Add(-15 * time.Minute)}
	a.pushCommitCache["big"] = big
	if pushCommitEntryExpired(big, now) {
		t.Fatalf("a %d-byte transfer's prep expired after 15m; its TTL is %s and the ctl needs %s just "+
			"to upload it", proto.XferMaxBytes, pushCommitEntryTTL(big.size), proto.XferLegBudget(big.size))
	}

	// Now flood the cache to capacity with small, still-live entries.
	for i := 0; len(a.pushCommitCache) < pushCommitCacheMaxEntries; i++ {
		a.pushCommitCache[fmt.Sprintf("id-%04d", i)] = pushCommitEntry{added: now}
	}
	if a.rememberPushCommit("newest", pushCommitEntry{}) {
		t.Fatal("remember accepted an entry while the cache was full of LIVE entries — the only way to " +
			"make room is to evict one, and the victim would be the oldest, i.e. the biggest transfer " +
			"in flight")
	}
	if _, ok := a.pushCommitCache["big"]; !ok {
		t.Fatal("the long-running large transfer's prep entry was evicted to make room for a new one")
	}
	if _, ok := a.pushCommitCache["newest"]; ok {
		t.Fatal("the refused entry was stored anyway")
	}

	// Once the small entries age out, the cache accepts again — the refusal is retriable, not a wedge.
	for id, e := range a.pushCommitCache {
		if id == "big" {
			continue
		}
		e.added = now.Add(-pushCommitEntryTTL(0) - time.Second)
		a.pushCommitCache[id] = e
	}
	if !a.rememberPushCommit("newest", pushCommitEntry{}) {
		t.Fatal("the cache never recovers: expired entries were not swept on the next insert")
	}
	if _, ok := a.pushCommitCache["big"]; !ok {
		t.Fatal("the sweep took the live large entry with it")
	}
}

// TestPushCommitTTLCoversTheUploadItWaitsOn: the prep entry has to outlive the ctl's upload of that
// exact file, or push-commit arrives to a cache miss and the agent answers transfer_unknown.
//
// Mutation: drop pushCommitCacheSlack, or key the TTL on something other than size — reddens.
func TestPushCommitTTLCoversTheUploadItWaitsOn(t *testing.T) {
	for _, size := range []int64{0, 1, 8 * 1024 * 1024, 512 * 1024 * 1024, proto.XferMaxBytes} {
		ttl, upload := pushCommitEntryTTL(size), proto.XferLegBudget(size)
		if ttl <= upload {
			t.Errorf("size=%d: prep TTL %s does not exceed the ctl's own upload budget %s — the entry "+
				"can expire before push-commit is even sent", size, ttl, upload)
		}
	}
}

// Disabled mode (explicit allow_roots: []) short-circuits both directions
// to transfer_disabled before any path work.
func TestValidate_DisabledMode(t *testing.T) {
	root := t.TempDir()
	for _, fn := range bothValidators() {
		_, err := fn(filepath.Join(root, "f"), nil, modeDisabled)
		var pve *PathValidationError
		if !errors.As(err, &pve) || pve.Code != "transfer_disabled" {
			t.Errorf("got %v, want transfer_disabled", err)
		}
	}
}

// Open mode (no allow_roots configured) reaches arbitrary paths — but every
// mechanism check still bites. This is the headline behavior flip: the old
// "empty roots → transfer_disabled" became "no roots → whole-FS, hardened".
func TestValidate_OpenMode_ArbitraryPathButMechanismKept(t *testing.T) {
	root := t.TempDir()

	// (1) A system path OUTSIDE any configured root is allowed in open mode.
	// /etc/hosts is a regular file on linux + darwin (darwin: /etc →
	// /private/etc, resolved by EvalSymlinks-of-parent). Skip on the rare
	// minimal image without it rather than hard-fail.
	if _, statErr := os.Lstat("/etc/hosts"); statErr == nil {
		if vp, err := ValidateForRead("/etc/hosts", nil, modeOpen); err != nil {
			t.Errorf("open-mode read of /etc/hosts: got %v, want OK", err)
		} else if vp.AllowRoot != "" {
			t.Errorf("open-mode AllowRoot = %q, want empty", vp.AllowRoot)
		}
	}

	// (2) Open-mode write to a brand-new leaf under an existing parent OK.
	if vp, err := ValidateForWrite(filepath.Join(root, "new.bin"), nil, modeOpen); err != nil {
		t.Errorf("open-mode write: got %v, want OK", err)
	} else if vp.AllowRoot != "" {
		t.Errorf("open-mode AllowRoot = %q, want empty", vp.AllowRoot)
	}

	// (3) Relative path still rejected in open mode.
	for _, fn := range bothValidators() {
		_, err := fn("relative/path", nil, modeOpen)
		var pve *PathValidationError
		if !errors.As(err, &pve) || pve.Code != "path_not_absolute" {
			t.Errorf("open-mode relative: got %v, want path_not_absolute", err)
		}
	}

	// (4) Leaf symlink still refused in open mode (push AND pull). This is
	// the PRIMARY non-member defense once containment is gone: a hostile
	// co-resident process planting /scratch/out -> /etc/passwd must not
	// redirect a member's write/read.
	target := filepath.Join(root, "real.txt")
	if err := os.WriteFile(target, []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "leaf-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	for _, fn := range bothValidators() {
		_, err := fn(link, nil, modeOpen)
		var pve *PathValidationError
		if !errors.As(err, &pve) || pve.Code != "not_a_regular_file" {
			t.Errorf("open-mode leaf symlink: got %v, want not_a_regular_file", err)
		}
	}
}

// Open mode does NOT auto-create parent dirs (push) — a member must not be
// able to materialize arbitrary directory trees (e.g. /etc/cron.d/...) as
// the agent UID. Assert both the error AND that nothing was created.
func TestValidateForWrite_OpenMode_NoAutoMkdir(t *testing.T) {
	root := t.TempDir()
	missingParent := filepath.Join(root, "no-such-tree", "deep")
	_, err := ValidateForWrite(filepath.Join(missingParent, "f.bin"), nil, modeOpen)
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "path_parent_missing" {
		t.Fatalf("got %v, want path_parent_missing", err)
	}
	if _, statErr := os.Stat(missingParent); !os.IsNotExist(statErr) {
		t.Errorf("parent dir was created (stat err=%v); open mode must not auto-mkdir", statErr)
	}
}

// Open mode refuses non-regular targets (fifo/device/socket) on both sides;
// assert the special file is not consumed.
func TestValidate_OpenMode_RejectsFifo(t *testing.T) {
	root := t.TempDir()
	fifo := filepath.Join(root, "pipe")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo unsupported: %v", err)
	}
	for _, fn := range bothValidators() {
		_, err := fn(fifo, nil, modeOpen)
		var pve *PathValidationError
		if !errors.As(err, &pve) || pve.Code != "not_a_regular_file" {
			t.Errorf("open-mode fifo: got %v, want not_a_regular_file", err)
		}
	}
	if fi, statErr := os.Lstat(fifo); statErr != nil || fi.Mode()&os.ModeNamedPipe == 0 {
		t.Errorf("fifo was consumed/altered: lstat=%v mode=%v", statErr, fi.Mode())
	}
}

// Narrow mode whose canon root list is empty (all dropped) must reject
// EVERY path (fail-closed), never fall through to open. This pins the
// Validate-layer half of the invariant TestResolveTransferMode pins above.
func TestValidate_NarrowMode_EmptyCanonRejectsAll(t *testing.T) {
	for _, fn := range bothValidators() {
		_, err := fn("/etc/hosts", nil, modeNarrow) // nil canon == all dropped
		var pve *PathValidationError
		if !errors.As(err, &pve) || pve.Code != "path_outside_roots" {
			t.Errorf("narrow+empty-canon: got %v, want path_outside_roots", err)
		}
	}
}

func TestValidate_RejectsNonAbsolute(t *testing.T) {
	roots := CanonAllowRoots([]string{t.TempDir()})
	for _, fn := range bothValidators() {
		_, err := fn("relative/path", roots, modeNarrow)
		var pve *PathValidationError
		if !errors.As(err, &pve) || pve.Code != "path_not_absolute" {
			t.Errorf("got %v, want path_not_absolute", err)
		}
	}
}

// Push: ../-style escapes get caught by EvalSymlinks(parent) +
// containment check. filepath.Clean alone DOES strip the .. so by
// itself it's not enough; this exercises the full chain.
func TestValidateForWrite_RejectsDotDotEscape(t *testing.T) {
	root := t.TempDir()
	// /tmp/<root>/sub/../../escape — Clean gives /tmp/escape which is
	// outside <root>. Containment check rejects.
	_, err := ValidateForWrite(filepath.Join(root, "sub", "..", "..", "escape"), CanonAllowRoots([]string{root}), modeNarrow)
	var pve *PathValidationError
	if !errors.As(err, &pve) {
		t.Fatalf("got %v, want PathValidationError", err)
	}
	// Could be path_outside_roots OR path_parent_missing (if the parent
	// doesn't exist). Either is correct; both prevent the write.
	if pve.Code != "path_outside_roots" && pve.Code != "path_parent_missing" {
		t.Errorf("got code=%q, want path_outside_roots|path_parent_missing", pve.Code)
	}
}

// Pull: a symlink at the leaf must be rejected by lstat, before any
// open with O_NOFOLLOW (which would also catch it but with worse error
// attribution).
func TestValidateForRead_RejectsSymlinkAtLeaf(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "real.txt")
	if err := os.WriteFile(target, []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "leaf-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	_, err := ValidateForRead(link, CanonAllowRoots([]string{root}), modeNarrow)
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "not_a_regular_file" {
		t.Errorf("got %v, want not_a_regular_file", err)
	}
}

// Push: same — symlink at leaf rejected even when target is regular.
func TestValidateForWrite_RejectsSymlinkAtLeaf(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "real.txt")
	if err := os.WriteFile(target, []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "leaf-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	_, err := ValidateForWrite(link, CanonAllowRoots([]string{root}), modeNarrow)
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "not_a_regular_file" {
		t.Errorf("got %v, want not_a_regular_file", err)
	}
}

// A symlink in the dir CHAIN is followed (EvalSymlinks of parent), but
// the resolved path must still land inside an allow_root. If the link
// points OUT of allow_roots, containment rejects.
func TestValidateForWrite_RejectsDirSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	// /<root>/escape -> /<outside>
	link := filepath.Join(root, "escape")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	// Try to write /<root>/escape/x — resolves parent to /<outside> →
	// outside the allow_root.
	_, err := ValidateForWrite(filepath.Join(link, "x"), CanonAllowRoots([]string{root}), modeNarrow)
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "path_outside_roots" {
		t.Errorf("got %v, want path_outside_roots", err)
	}
}

// A symlink in the dir CHAIN that points INSIDE allow_roots must
// succeed (the leaf is what matters; intermediate symlinks just
// resolve to a real path inside the root).
func TestValidateForWrite_AcceptsDirSymlinkInsideRoot(t *testing.T) {
	root := t.TempDir()
	innerReal := filepath.Join(root, "real-dir")
	if err := os.Mkdir(innerReal, 0o755); err != nil {
		t.Fatal(err)
	}
	innerLink := filepath.Join(root, "alias")
	if err := os.Symlink(innerReal, innerLink); err != nil {
		t.Fatal(err)
	}
	vp, err := ValidateForWrite(filepath.Join(innerLink, "newfile.bin"), CanonAllowRoots([]string{root}), modeNarrow)
	if err != nil {
		t.Fatalf("got %v, want OK", err)
	}
	if want := canonDir(t, innerReal); !strings.HasPrefix(vp.Abs, want+"/") {
		t.Errorf("vp.Abs=%q does not start with resolved %q/", vp.Abs, want)
	}
}

// Push: parent dir missing → path_parent_missing (we don't auto-mkdir).
func TestValidateForWrite_ParentMissing(t *testing.T) {
	root := t.TempDir()
	_, err := ValidateForWrite(filepath.Join(root, "no-such-dir", "file"), CanonAllowRoots([]string{root}), modeNarrow)
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "path_parent_missing" {
		t.Errorf("got %v, want path_parent_missing", err)
	}
}

// Pull: source missing → path_not_found.
func TestValidateForRead_NotFound(t *testing.T) {
	root := t.TempDir()
	_, err := ValidateForRead(filepath.Join(root, "no-such-file"), CanonAllowRoots([]string{root}), modeNarrow)
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "path_not_found" {
		t.Errorf("got %v, want path_not_found", err)
	}
}

// Pull: directory at the leaf → not_a_regular_file.
func TestValidateForRead_RejectsDirectoryLeaf(t *testing.T) {
	root := t.TempDir()
	d := filepath.Join(root, "subdir")
	if err := os.Mkdir(d, 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := ValidateForRead(d, CanonAllowRoots([]string{root}), modeNarrow)
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "not_a_regular_file" {
		t.Errorf("got %v, want not_a_regular_file", err)
	}
}

// CanonAllowRoots: longest-prefix-wins ordering so /srv/local/alice
// shadows /srv when both are listed.
func TestCanonicalAllowRoots_LongestWins(t *testing.T) {
	srv := t.TempDir()
	deeper := filepath.Join(srv, "alice")
	if err := os.Mkdir(deeper, 0o755); err != nil {
		t.Fatal(err)
	}
	roots := CanonAllowRoots([]string{srv, deeper})
	if len(roots) != 2 {
		t.Fatalf("want 2 roots, got %v", roots)
	}
	if want := canonDir(t, deeper); roots[0] != want {
		t.Errorf("longest first violation: roots[0]=%q want %q", roots[0], want)
	}
}

func TestCanonicalAllowRoots_FilesystemRootContainsPaths(t *testing.T) {
	roots := CanonAllowRoots([]string{string(filepath.Separator)})
	if len(roots) != 1 || roots[0] != string(filepath.Separator) {
		t.Fatalf("canonical root = %v, want [/]", roots)
	}
	root := t.TempDir()
	src := filepath.Join(root, "src.bin")
	if err := os.WriteFile(src, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateForRead(src, roots, modeNarrow); err != nil {
		t.Fatalf("root allow-list rejected read %s: %v", src, err)
	}
	if _, err := ValidateForWrite(filepath.Join(root, "new.bin"), roots, modeNarrow); err != nil {
		t.Fatalf("root allow-list rejected write: %v", err)
	}
}

// CanonAllowRoots silently drops non-existent + non-directory + relative.
func TestCanonicalAllowRoots_DropsBad(t *testing.T) {
	root := t.TempDir()
	regular := filepath.Join(root, "file.txt")
	if err := os.WriteFile(regular, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	roots := CanonAllowRoots([]string{
		"", "relative", "/no/such/path",
		regular, // file, not a dir
		root,    // good
	})
	if want := canonDir(t, root); len(roots) != 1 || roots[0] != want {
		t.Errorf("got %v, want exactly [%q]", roots, want)
	}
}

// Round-trip: ValidateForWrite + OpenForWriteAtomic + RenameForWriteAtomic
// produces a regular file in place; refusing --force when dest exists
// returns dst_exists.
func TestWriteAtomic_DstExistsHonored(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(root, "out.bin")
	if err := os.WriteFile(dst, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	vp, err := ValidateForWrite(dst, CanonAllowRoots([]string{root}), modeNarrow)
	if err != nil {
		t.Fatal(err)
	}
	f, tmp, err := OpenForWriteAtomic(vp, "abc")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("new"))
	_ = f.Close()

	// force=false → dst_exists, tmp left for caller cleanup.
	err = RenameForWriteAtomic(vp, tmp, false)
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "dst_exists" {
		t.Errorf("got %v, want dst_exists", err)
	}

	// force=true → overwrite.
	if err := RenameForWriteAtomic(vp, tmp, true); err != nil {
		t.Fatalf("force rename: %v", err)
	}
	got, _ := os.ReadFile(dst)
	if string(got) != "new" {
		t.Errorf("dst contents = %q, want %q", got, "new")
	}
}

// The dst_exists/--force clobber gate is mode-INDEPENDENT: in open mode
// (AllowRoot=="") an existing destination is still protected without
// --force. Guards against a future refactor gating the clobber check on
// AllowRoot presence — now the only guard once whole-FS is writable.
func TestWriteAtomic_OpenMode_ClobberGate(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(root, "exists.bin")
	if err := os.WriteFile(dst, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	vp, err := ValidateForWrite(dst, nil, modeOpen)
	if err != nil {
		t.Fatal(err)
	}
	if vp.AllowRoot != "" {
		t.Fatalf("open-mode AllowRoot = %q, want empty", vp.AllowRoot)
	}
	f, tmp, err := OpenForWriteAtomic(vp, "abc")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("new"))
	_ = f.Close()

	// No --force → dst_exists even in open mode.
	err = RenameForWriteAtomic(vp, tmp, false)
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "dst_exists" {
		t.Errorf("open-mode no-force: got %v, want dst_exists", err)
	}
	// --force → overwrite.
	if err := RenameForWriteAtomic(vp, tmp, true); err != nil {
		t.Fatalf("open-mode force rename: %v", err)
	}
	if got, _ := os.ReadFile(dst); string(got) != "new" {
		t.Errorf("dst = %q, want %q", got, "new")
	}
}

func TestWriteAtomic_NoForceConcurrentCommitHasSingleWinner(t *testing.T) {
	root := t.TempDir()
	dst := filepath.Join(root, "winner.bin")
	vp, err := ValidateForWrite(dst, nil, modeOpen)
	if err != nil {
		t.Fatal(err)
	}

	type candidate struct {
		pending *AtomicWrite
		content string
	}
	var candidates []candidate
	for i, content := range []string{"first", "second"} {
		f, pending, err := OpenForWriteAtomic(vp, fmt.Sprintf("candidate-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		candidates = append(candidates, candidate{pending: pending, content: content})
	}
	defer candidates[0].pending.Abort()
	defer candidates[1].pending.Abort()

	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, c := range candidates {
		c := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- RenameForWriteAtomic(vp, c.pending, false)
		}()
	}
	wg.Wait()
	close(errs)

	successes, exists := 0, 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		var pve *PathValidationError
		if errors.As(err, &pve) && pve.Code == "dst_exists" {
			exists++
			continue
		}
		t.Fatalf("unexpected commit error: %v", err)
	}
	if successes != 1 || exists != 1 {
		t.Fatalf("successes=%d dst_exists=%d, want 1/1", successes, exists)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "first" && string(got) != "second" {
		t.Fatalf("destination contains %q", got)
	}
}

func TestAtomicOpenRejectsParentSwapToSymlink(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "parent")
	outside := filepath.Join(root, "outside")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(parent, "src.bin")
	if err := os.WriteFile(src, []byte("GOOD"), 0o600); err != nil {
		t.Fatal(err)
	}
	readVP, err := ValidateForRead(src, nil, modeOpen)
	if err != nil {
		t.Fatal(err)
	}
	writeVP, err := ValidateForWrite(filepath.Join(parent, "dst.bin"), nil, modeOpen)
	if err != nil {
		t.Fatal(err)
	}

	held := filepath.Join(root, "parent-held")
	if err := os.Rename(parent, held); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, parent); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "src.bin"), []byte("EVIL"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := OpenForReadAtomic(readVP); err == nil {
		t.Fatal("read followed a parent swapped to symlink")
	} else {
		var pve *PathValidationError
		if !errors.As(err, &pve) || pve.Code != "path_race" {
			t.Fatalf("read parent swap: got %v, want path_race", err)
		}
	}
	if _, pending, err := OpenForWriteAtomic(writeVP, "swap"); err == nil {
		pending.Abort()
		t.Fatal("write followed a parent swapped to symlink")
	} else {
		var pve *PathValidationError
		if !errors.As(err, &pve) || pve.Code != "path_race" {
			t.Fatalf("write parent swap: got %v, want path_race", err)
		}
	}
	if _, err := os.Lstat(filepath.Join(outside, "dst.bin")); !os.IsNotExist(err) {
		t.Fatalf("outside destination was created: %v", err)
	}
}

func TestAtomicWriteRejectsParentSwapAfterTempOpen(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "parent")
	outside := filepath.Join(root, "outside")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	vp, err := ValidateForWrite(filepath.Join(parent, "dst.bin"), nil, modeOpen)
	if err != nil {
		t.Fatal(err)
	}
	f, pending, err := OpenForWriteAtomic(vp, "late-swap")
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Abort()
	if _, err := f.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	held := filepath.Join(root, "parent-held")
	if err := os.Rename(parent, held); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, parent); err != nil {
		t.Fatal(err)
	}
	err = RenameForWriteAtomic(vp, pending, true)
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "path_race" {
		t.Fatalf("late parent swap: got %v, want path_race", err)
	}
	if _, err := os.Lstat(filepath.Join(outside, "dst.bin")); !os.IsNotExist(err) {
		t.Fatalf("outside destination was created: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(held, "dst.bin")); !os.IsNotExist(err) {
		t.Fatalf("moved parent received a falsely successful commit: %v", err)
	}
}

// OpenForReadAtomic returns ELOOP / not_a_regular_file when the path
// is a leaf symlink (defence-in-depth on top of the lstat check in
// ValidateForRead).
func TestOpenForReadAtomic_RejectsSymlinkLeaf(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "real.txt")
	if err := os.WriteFile(target, []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "leaf-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	// Spoof a ValidatedPath as if validation had not seen the symlink
	// (simulates TOCTOU: an attacker swaps file→symlink between
	// lstat and open). OpenForReadAtomic should still refuse.
	vp := &ValidatedPath{Abs: link, AllowRoot: root}
	_, err := OpenForReadAtomic(vp)
	var pve *PathValidationError
	if !errors.As(err, &pve) || pve.Code != "not_a_regular_file" {
		t.Errorf("got %v, want not_a_regular_file", err)
	}
}

// TOCTOU symlink arm: while one goroutine swaps the leaf between a regular
// hardlink to GOOD and a symlink to EVIL, the reader's OpenForReadAtomic
// must NEVER return a successful read of the symlink target — O_NOFOLLOW
// refuses the symlink leaf. Open mode (AllowRoot=="") so this also proves
// the mechanism survives with containment off. Run under
// `go test -race ./internal/agent/`.
func TestOpenForReadAtomic_TOCTOU_SymlinkArm(t *testing.T) {
	root := t.TempDir()
	good := filepath.Join(root, "good.txt")
	if err := os.WriteFile(good, []byte("GOOD"), 0o600); err != nil {
		t.Fatal(err)
	}
	evil := filepath.Join(root, "evil.txt")
	if err := os.WriteFile(evil, []byte("EVIL"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "race-target")

	const iters = 400
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			_ = os.Remove(path)
			if i%2 == 0 {
				_ = os.Link(good, path) // regular file, content GOOD
			} else {
				_ = os.Symlink(evil, path) // symlink → EVIL
			}
		}
		_ = os.Remove(path)
	}()
	go func() {
		defer wg.Done()
		for j := 0; j < iters; j++ {
			f, err := OpenForReadAtomic(&ValidatedPath{Abs: path})
			if err != nil {
				continue // path absent / not_a_regular_file all fine
			}
			b, _ := io.ReadAll(f)
			_ = f.Close()
			if string(b) != "GOOD" {
				t.Errorf("TOCTOU breach: read %q, want GOOD (symlink leaf followed)", b)
				return
			}
		}
	}()
	wg.Wait()
}

// TOCTOU dev/inode arm: swap the leaf between two DISTINCT regular files
// (GOOD/EVIL) via atomic os.Rename, flipping the inode underneath the
// reader. THIS is the swap that exercises OpenForReadAtomic's dev+inode
// recheck (the lstat→open race → path_race); the symlink arm above only
// hits the O_NOFOLLOW path (Round-1 review proved path_race was otherwise
// 0/8000). Under -race this validates the recheck code. The assertion is
// the safety invariant: a successful read is always a COMPLETE regular
// file's content (GOOD or EVIL), never torn/zero, and no error is ever
// not_a_regular_file (no symlink is involved). path_race is timing-
// dependent, so we do not assert a hit count.
func TestOpenForReadAtomic_TOCTOU_RegularSwapArm(t *testing.T) {
	root := t.TempDir()
	goodSrc := filepath.Join(root, "good.src")
	if err := os.WriteFile(goodSrc, []byte("GOOD"), 0o600); err != nil {
		t.Fatal(err)
	}
	evilSrc := filepath.Join(root, "evil.src")
	if err := os.WriteFile(evilSrc, []byte("EVIL"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "race-target")
	if err := os.Link(goodSrc, path); err != nil { // start as a regular file
		t.Fatal(err)
	}

	const iters = 600
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			src := goodSrc
			if i%2 == 1 {
				src = evilSrc
			}
			stage := filepath.Join(root, fmt.Sprintf("stage-%d", i))
			if err := os.Link(src, stage); err != nil {
				continue
			}
			_ = os.Rename(stage, path) // atomic inode flip; path stays a complete regular file
		}
	}()
	go func() {
		defer wg.Done()
		for j := 0; j < iters; j++ {
			f, err := OpenForReadAtomic(&ValidatedPath{Abs: path})
			if err != nil {
				var pve *PathValidationError
				if errors.As(err, &pve) && pve.Code == "not_a_regular_file" {
					t.Errorf("unexpected not_a_regular_file in regular-only swap: %v", err)
					return
				}
				continue // path_race / io_error acceptable
			}
			b, _ := io.ReadAll(f)
			_ = f.Close()
			if s := string(b); s != "GOOD" && s != "EVIL" {
				t.Errorf("torn/wrong read under inode swap: %q", s)
				return
			}
		}
	}()
	wg.Wait()
}
