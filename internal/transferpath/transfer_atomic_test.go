//go:build linux

package transferpath

// Real regular-file IO, directory Sync, rename/link and kernel OFD witnesses.
// Per-receiver syscall faults cover the post-effect boundary; no actor is faked.
// Full same-PID worker/image/custody integration belongs to P28/P32.

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"golang.org/x/sys/unix"
)

func atomicDeadline(t *testing.T) controlbudget.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func newTestAtomic(t *testing.T, force bool, old []byte) (*Atomic, string) {
	t.Helper()
	dst := filepath.Join(canonDir(t, t.TempDir()), "destination")
	if old != nil {
		if err := os.WriteFile(dst, old, 0o751); err != nil {
			t.Fatal(err)
		}
	}
	vp, err := ValidateForWrite(dst, nil, ModeOpen)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := NewAtomicReceiver(vp, "private-nonce", force)
	if err != nil {
		t.Fatal(err)
	}
	r := receiver.(*Atomic)
	t.Cleanup(func() {
		if err := r.Stop(atomicDeadline(t)); err != nil {
			t.Errorf("atomic cleanup: %v", err)
		}
	})
	return r, dst
}

func writeSyncedAtomic(t *testing.T, r AtomicReceiver, body []byte, offset int64) {
	t.Helper()
	if n, err := r.WriteAt(body, offset); err != nil || n != len(body) {
		t.Fatalf("WriteAt: n=%d err=%v", n, err)
	}
	if err := r.Sync(); err != nil {
		t.Fatal(err)
	}
}

func atomicGrant() xfercontract.CommitGrant {
	return xfercontract.CommitGrant{
		Identity:   xfercontract.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", IntentDigest: "intent", ObjectVersion: "version"},
		GrantNonce: "grant", ReceiverInstance: "receiver", TerminalID: "terminal",
	}
}

func atomicOffset(t *testing.T, file *os.File) int64 {
	t.Helper()
	fd, err := atomicDescriptor(file)
	if err != nil {
		t.Fatal(err)
	}
	offset, err := unix.Seek(fd, 0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	return offset
}

// Query KCMP_FILE first. Restricted kernels can deny self-kcmp; in that case
// observe a reversible F_SETFL change through BOTH descriptions. This proves
// kernel OFD coupling, unlike comparing equal dev/ino/offset/flags snapshots.
// The test changes flags only AFTER the pristine-import checks and restores
// them before the owner resumes. A reopened same-inode control must fail too.
func atomicSameOFD(t *testing.T, a, b *os.File) bool {
	t.Helper()
	aFD, err := atomicDescriptor(a)
	if err != nil {
		t.Fatal(err)
	}
	bFD, err := atomicDescriptor(b)
	if err != nil {
		t.Fatal(err)
	}
	result, _, errno := unix.Syscall6(unix.SYS_KCMP, uintptr(os.Getpid()), uintptr(os.Getpid()), 0, uintptr(aFD), uintptr(bFD), 0)
	if errno == 0 {
		return result == 0
	}
	if errno != unix.EPERM && errno != unix.ENOSYS && errno != unix.EACCES {
		t.Fatalf("KCMP_FILE: %v", errno)
	}
	flags, err := unix.FcntlInt(uintptr(aFD), unix.F_GETFL, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(uintptr(aFD), unix.F_SETFL, flags^unix.O_NONBLOCK); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := unix.FcntlInt(uintptr(aFD), unix.F_SETFL, flags); err != nil {
			t.Fatal(err)
		}
	}()
	observed, err := unix.FcntlInt(uintptr(bFD), unix.F_GETFL, 0)
	if err != nil {
		t.Fatal(err)
	}
	return observed&unix.O_NONBLOCK == (flags^unix.O_NONBLOCK)&unix.O_NONBLOCK
}

func closeAtomicSalvage(t *testing.T, salvage xfercontract.Salvage) {
	t.Helper()
	for _, resource := range salvage.Resources {
		for _, file := range resource.Files {
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// origin: hot-update S7 plan §3 / T10 / M09
func TestTransferAtomicReceiverPreservesOriginalOFDs(t *testing.T) {
	t.Run("pristine-originals-and-short-prefix", func(t *testing.T) {
		r, dst := newTestAtomic(t, true, []byte("complete old destination"))
		prefix := []byte("synced prefix")
		writeSyncedAtomic(t, r, prefix, 0)
		// Actual Pwrite returns a short n followed by a real error. Freeze
		// must keep the synced prefix and remove only the old owner's suffix.
		r.ops.write = func(f *os.File, body []byte, off int64) (int, error) {
			n, err := f.WriteAt(body[:3], off)
			return n, errors.Join(err, unix.EIO)
		}
		if n, err := r.WriteAt([]byte("unsynced tail"), int64(len(prefix))); n != 3 || !errors.Is(err, unix.EIO) {
			t.Fatalf("short syscall: n=%d err=%v", n, err)
		}
		if r.written != int64(len(prefix)+3) || r.state.DurableOffset != int64(len(prefix)) {
			t.Fatal("short IO fabricated a durable checkpoint")
		}
		sourcePath := filepath.Join(filepath.Dir(dst), "source")
		if err := os.WriteFile(sourcePath, []byte("source is pinned through import"), 0o600); err != nil {
			t.Fatal(err)
		}
		sourceVP, err := ValidateForRead(sourcePath, nil, ModeOpen)
		if err != nil {
			t.Fatal(err)
		}
		r.source, err = OpenForReadAtomic(sourceVP)
		if err != nil {
			t.Fatal(err)
		}
		r.state.Source = xfercontract.ResourceRef{ResourceID: "pinned-source", Kind: xfercontract.ResourceSource, Name: xfercontract.SourceFileName}
		for _, f := range []*os.File{r.tmp, r.source} {
			fd, _ := atomicDescriptor(f)
			if _, err := unix.Seek(fd, 7, io.SeekStart); err != nil {
				t.Fatal(err)
			}
		}
		state, resources, err := r.Freeze(atomicDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if state.DurableOffset != int64(len(prefix)) || len(resources) != 3 {
			t.Fatalf("wrong checkpoint/FD closure: offset=%d resources=%d", state.DurableOffset, len(resources))
		}
		if n, err := r.WriteAt([]byte("must remain frozen"), state.DurableOffset); n != 0 || err == nil {
			t.Fatal("frozen old owner accepted bytes")
		}
		got, err := os.ReadFile(filepath.Join(filepath.Dir(dst), state.TmpName))
		if err != nil || !bytes.Equal(got, prefix) || state.TmpIdentity.Flags&unix.O_ACCMODE != unix.O_WRONLY {
			t.Fatalf("export was not the synced WRONLY prefix: %q %v", got, err)
		}
		originals := []*os.File{r.parent, r.tmp, r.source}
		offsets := []int64{atomicOffset(t, r.parent), atomicOffset(t, r.tmp), atomicOffset(t, r.source)}
		imported, salvage, err := ImportAtomic(state, handofftest.DupResources(t, resources))
		if err != nil || len(salvage.Resources) != 0 {
			t.Fatalf("ImportAtomic: %v salvage=%d", err, len(salvage.Resources))
		}
		current := imported.(*Atomic)
		defer func() { _ = current.Stop(atomicDeadline(t)) }()
		for i, file := range []*os.File{current.parent, current.tmp, current.source} {
			identity, _, err := atomicIdentity(file)
			originalIdentity, _, _ := atomicIdentity(originals[i])
			if err != nil || identity != originalIdentity || atomicOffset(t, file) != offsets[i] {
				t.Fatalf("pristine import changed flags, identity or offset: resource=%d err=%v", i, err)
			}
			if !atomicSameOFD(t, originals[i], file) {
				t.Fatalf("import reopened original OFD: resource=%d", i)
			}
		}
		if n, err := current.WriteAt([]byte("must sleep"), state.DurableOffset); err == nil || n != 0 {
			t.Fatal("sleeping import accepted bytes")
		}
		if err := DiscardAtomic(r, atomicDeadline(t)); err != nil {
			t.Fatal(err)
		}
		if err := ThawAtomic(current, atomicDeadline(t)); err != nil {
			t.Fatal(err)
		}
		writeSyncedAtomic(t, current, []byte(" continued"), state.DurableOffset)
		out := current.Commit(atomicGrant(), atomicDeadline(t))
		if out.Outcome != xfercontract.OutcomeLanded || !out.ParentSynced {
			t.Fatalf("continued commit: %+v", out)
		}
		got, err = os.ReadFile(dst)
		if err != nil || string(got) != "synced prefix continued" {
			t.Fatalf("continued bytes: %q %v", got, err)
		}
	})
	t.Run("reopen-same-inode-control", func(t *testing.T) {
		r, dst := newTestAtomic(t, false, nil)
		for _, f := range []*os.File{r.tmp, r.parent} {
			fd, _ := atomicDescriptor(f)
			flags, _ := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
			path := filepath.Dir(dst)
			if f == r.tmp {
				path = filepath.Join(path, r.state.TmpName)
			}
			reopenedFD, err := unix.Open(path, flags|unix.O_CLOEXEC, 0)
			if err != nil {
				t.Fatal(err)
			}
			reopened := os.NewFile(uintptr(reopenedFD), "same-inode-control")
			if atomicSameOFD(t, f, reopened) {
				t.Fatal("OFD oracle accepted an equal-inode reopen")
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("refuse-without-repair-and-salvage", func(t *testing.T) {
		r, _ := newTestAtomic(t, false, nil)
		writeSyncedAtomic(t, r, []byte("prefix"), 0)
		state, resources, err := r.Freeze(atomicDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		for _, mutate := range []string{"size", "inode", "mode", "flags", "resource-kind", "name", "phase", "open-phase", "primitive-kind", "inflight", "orphan-identity", "duplicate-id", "grant"} {
			t.Run(mutate, func(t *testing.T) {
				bad := state
				switch mutate {
				case "size":
					bad.DurableOffset--
				case "inode":
					bad.TmpIdentity.Ino++
				case "mode":
					bad.TmpIdentity.Mode = unix.S_IFDIR | 0o600
				case "flags":
					bad.TmpIdentity.Flags |= unix.O_NONBLOCK
				case "resource-kind":
					bad.Tmp.Kind = xfercontract.ResourceSource
				case "name":
					bad.TmpName = "../escape"
				case "phase":
					bad.Phase = xfercontract.PhaseSettled
				case "open-phase":
					bad.Phase = xfercontract.PhaseOpen
				case "primitive-kind":
					bad.Phase, bad.CommitGrant = xfercontract.PhaseDecided, atomicGrant()
					bad.PrimitiveOutcome = xfercontract.ReceiverOutcome{Grant: bad.CommitGrant, Primitive: "foreign", Outcome: xfercontract.OutcomeNotEntered, Bytes: bad.DurableOffset, Code: "io_error"}
				case "inflight":
					bad.PrimitiveOutcome.Outcome = xfercontract.OutcomeInflight
				case "orphan-identity":
					bad.SourceIdentity = bad.TmpIdentity
				case "duplicate-id":
					bad.Tmp.ResourceID = bad.Parent.ResourceID
				case "grant":
					bad.CommitGrant.GrantNonce = "incomplete"
				}
				beforeIdentity, beforeSize, _ := atomicIdentity(r.tmp)
				beforeOffset := atomicOffset(t, r.tmp)
				imported, salvage, err := ImportAtomic(bad, handofftest.DupResources(t, resources))
				if err == nil || imported != nil {
					t.Fatal("invalid inherited closure was accepted")
				}
				for _, resource := range salvage.Resources {
					for _, file := range resource.Files {
						if _, err := file.Stat(); err != nil {
							t.Fatalf("salvage file was closed: %v", err)
						}
					}
				}
				closeAtomicSalvage(t, salvage)
				afterIdentity, afterSize, _ := atomicIdentity(r.tmp)
				if beforeIdentity != afterIdentity || beforeSize != afterSize || beforeOffset != atomicOffset(t, r.tmp) {
					t.Fatal("rejected import repaired a shared OFD")
				}
			})
		}
		for failAt := 1; failAt <= 2; failAt++ {
			src := &atomicPartialSource{source: handofftest.DupResources(t, resources), failAt: failAt}
			_, salvage, err := ImportAtomic(state, src)
			if !errors.Is(err, unix.EIO) || len(salvage.Resources) != failAt {
				t.Fatalf("partial Take dropped ownership: at=%d resources=%d err=%v", failAt, len(salvage.Resources), err)
			}
			closeAtomicSalvage(t, salvage)
		}
	})
}

type atomicPartialSource struct {
	source        FileSource
	calls, failAt int
}

func (s *atomicPartialSource) TakeFile(id, name string) (*os.File, error) {
	s.calls++
	f, err := s.source.TakeFile(id, name)
	if s.calls == s.failAt {
		err = errors.Join(err, unix.EIO)
	}
	return f, err
}

// origin: hot-update S7 plan §3–§4 / T11 / M10
func TestTransferAtomicLinkCleanupAndDirectorySync(t *testing.T) {
	for _, name := range []string{"rename", "link", "exists", "unlink-fails", "directory-sync-fails", "close-fails", "sync-and-close-fail", "primitive-fails"} {
		t.Run(name, func(t *testing.T) {
			force := name != "link" && name != "exists" && name != "unlink-fails"
			old := []byte("old destination is complete")
			if name == "link" || name == "unlink-fails" {
				old = nil
			}
			r, dst := newTestAtomic(t, force, old)
			body := bytes.Repeat([]byte("complete new bytes;"), 4096)
			writeSyncedAtomic(t, r, body, 0)
			attempts, effects, syncs := 0, 0, 0
			realRename, realLink, realSync, realClose, realUnlink := r.ops.rename, r.ops.link, r.ops.sync, r.ops.close, r.ops.unlink
			r.ops.rename = func(a int, b string, c int, d string) error {
				attempts++
				if name == "primitive-fails" {
					return unix.EIO
				}
				err := realRename(a, b, c, d)
				if err == nil {
					effects++
				}
				return err
			}
			r.ops.link = func(a int, b string, c int, d string, flags int) error {
				attempts++
				err := realLink(a, b, c, d, flags)
				if err == nil {
					effects++
				}
				return err
			}
			failCleanup, failSync, failClose := name == "unlink-fails", name == "directory-sync-fails" || name == "sync-and-close-fail", name == "close-fails" || name == "sync-and-close-fail"
			r.ops.unlink = func(fd int, leaf string, flags int) error {
				if failCleanup {
					return unix.EACCES
				}
				return realUnlink(fd, leaf, flags)
			}
			r.ops.sync = func(f *os.File) error {
				if f == r.parent {
					syncs++
					if failSync {
						return unix.EIO
					}
				}
				return realSync(f)
			}
			r.ops.close = func(f *os.File) error {
				err := realClose(f)
				if failClose {
					return errors.Join(err, unix.EIO)
				}
				return err
			}
			var observed []xfercontract.Observation
			if err := SetAtomicObserver(r, func(event xfercontract.Observation) {
				observed = append(observed, event)
				if event.Kind == xfercontract.ObservePrimitiveEnter || event.Kind == xfercontract.ObservePrimitiveExit {
					// Independent opens at BOTH primitive boundaries. Final SHA
					// alone would allow an in-place half-write followed by repair.
					got, err := os.ReadFile(dst)
					if err != nil && (old != nil || !errors.Is(err, os.ErrNotExist)) {
						t.Errorf("target oracle: %v", err)
					}
					if err == nil && !bytes.Equal(got, old) && !bytes.Equal(got, body) {
						t.Errorf("target exposed a partial file: bytes=%d", len(got))
					}
				}
			}); err != nil {
				t.Fatal(err)
			}
			grant := atomicGrant()
			out := r.Commit(grant, atomicDeadline(t))
			if name == "exists" || name == "primitive-fails" {
				wantCode := "dst_exists"
				if name == "primitive-fails" {
					wantCode = "io_error"
				}
				if out.Outcome != xfercontract.OutcomeNotEntered || out.Code != wantCode || effects != 0 || syncs != 0 {
					t.Fatalf("failed primitive fabricated success: %+v effects=%d syncs=%d", out, effects, syncs)
				}
			} else {
				if out.Outcome != xfercontract.OutcomeLanded || effects != 1 || syncs != 1 || out.ParentSynced == failSync || out.TmpCleanupPending != failCleanup {
					t.Fatalf("landed/durability/cleanup collapsed: %+v effects=%d syncs=%d", out, effects, syncs)
				}
				if (failSync && out.Code != xfercontract.CodeDurabilityUnknown) || (!failSync && (failCleanup || failClose) && out.Code != xfercontract.CodeReconcileRequired) {
					t.Fatalf("post-effect failure hidden: %+v", out)
				}
			}
			if attempts != 1 || len(observed) < 2 || observed[0].Kind != xfercontract.ObservePrimitiveEnter || observed[1].Kind != xfercontract.ObservePrimitiveExit {
				t.Fatalf("missing primitive receipt: attempts=%d events=%v", attempts, observed)
			}
			state, resources, err := r.Freeze(atomicDeadline(t))
			if failSync && failClose {
				if err == nil || len(resources) != 0 || r.parent == nil {
					t.Fatal("cut discarded close evidence behind pending durability")
				}
				failSync, failClose = false, false
				if err := ThawAtomic(r, atomicDeadline(t)); err != nil {
					t.Fatal(err)
				}
				settled := r.Commit(grant, atomicDeadline(t))
				if settled.Outcome != xfercontract.OutcomeLanded || !settled.ParentSynced || settled.Code != xfercontract.CodeReconcileRequired || effects != 1 {
					t.Fatalf("old owner forgot independent close failure: %+v effects=%d", settled, effects)
				}
				r.closeErr = nil // The injected error consumed the real file; fixture cleanup is explicit.
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			wantFDs := 0
			if failCleanup || name == "exists" || name == "primitive-fails" {
				wantFDs = 2
			} else if failSync {
				wantFDs = 1
			}
			if len(resources) != wantFDs || state.PrimitiveOutcome != out {
				t.Fatalf("wrong phase FD closure: got=%d want=%d state=%+v", len(resources), wantFDs, state)
			}
			imported, salvage, err := ImportAtomic(state, handofftest.DupResources(t, resources))
			if err != nil || len(salvage.Resources) != 0 {
				t.Fatalf("post-primitive import: %v", err)
			}
			current := imported.(*Atomic)
			if err := DiscardAtomic(r, atomicDeadline(t)); err != nil && !failClose {
				t.Fatal(err)
			}
			if err := ThawAtomic(current, atomicDeadline(t)); err != nil {
				t.Fatal(err)
			}
			// Install the primitive counter in the new owner too: otherwise a
			// repeat after import could escape the exactly-once assertion.
			current.ops.rename, current.ops.link = r.ops.rename, r.ops.link
			for range 3 {
				out = current.Commit(grant, atomicDeadline(t))
			}
			if attempts != 1 {
				t.Fatalf("commit retry reentered primitive: attempts=%d effects=%d", attempts, effects)
			}
			if failCleanup || failSync {
				if !out.ParentSynced || out.TmpCleanupPending || out.Outcome != xfercontract.OutcomeLanded {
					t.Fatalf("settlement did not preserve landed: %+v", out)
				}
			}
			if failClose && out.Code != xfercontract.CodeReconcileRequired {
				t.Fatal("import forgot close uncertainty")
			}
			if !failSync && !failCleanup && out != state.PrimitiveOutcome {
				t.Fatalf("retry changed the immutable decision: before=%+v after=%+v", state.PrimitiveOutcome, out)
			}
			changedGrant := grant
			changedGrant.GrantNonce = "different-grant"
			if conflict := current.Commit(changedGrant, atomicDeadline(t)); conflict.Code != xfercontract.CodeIntentConflict || attempts != 1 {
				t.Fatalf("changed Grant reused commit: %+v attempts=%d", conflict, attempts)
			}
			if err := current.Stop(atomicDeadline(t)); err != nil {
				t.Fatal(err)
			}
			failCleanup, failSync, failClose = false, false, false
			// The original is already discarded; acknowledge its deliberately
			// injected consumed-close error so fixture cleanup can assert success.
			r.closeErr = nil
			got, err := os.ReadFile(dst)
			want := body
			if name == "exists" || name == "primitive-fails" {
				want = old
			}
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("wrong target content: bytes=%d err=%v", len(got), err)
			}
			if force && effects == 1 {
				st, _ := os.Stat(dst)
				if st.Mode().Perm() != 0o751 {
					t.Fatalf("force lost destination permissions: %v", st.Mode())
				}
			}
		})
	}
	t.Run("landed-outcome-survives-closed-gates-and-expiry", func(t *testing.T) {
		r, _ := newTestAtomic(t, true, []byte("old complete"))
		writeSyncedAtomic(t, r, []byte("new complete"), 0)
		realSync := r.ops.sync
		r.ops.sync = func(f *os.File) error {
			if f == r.parent {
				return unix.EIO
			}
			return realSync(f)
		}
		grant := atomicGrant()
		out := r.Commit(grant, atomicDeadline(t))
		if out.Outcome != xfercontract.OutcomeLanded || out.ParentSynced {
			t.Fatalf("pending durability was not landed: %+v", out)
		}
		if late := r.Commit(grant, 1); late != out {
			t.Fatalf("expired settlement erased landed evidence: %+v", late)
		}
		if _, _, err := r.Freeze(atomicDeadline(t)); err != nil {
			t.Fatal(err)
		}
		if frozen := r.Commit(grant, atomicDeadline(t)); frozen != out {
			t.Fatalf("frozen gate erased landed evidence: %+v", frozen)
		}
		if err := ThawAtomic(r, atomicDeadline(t)); err != nil {
			t.Fatal(err)
		}
		if err := r.Stop(atomicDeadline(t)); err == nil {
			t.Fatal("Stop hid pending directory durability")
		}
		if stopped := r.Commit(grant, atomicDeadline(t)); stopped != out {
			t.Fatalf("cancellation erased landed evidence: %+v", stopped)
		}
		r.ops.sync = realSync
		if err := r.Stop(atomicDeadline(t)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("grant-and-path-admission", func(t *testing.T) {
		for _, name := range []string{"no-grant", "expired", "canceled", "parent-swap", "tmp-swap", "destination-symlink", "unsynced"} {
			t.Run(name, func(t *testing.T) {
				r, dst := newTestAtomic(t, true, []byte("old complete"))
				writeSyncedAtomic(t, r, []byte("new complete"), 0)
				grant, deadline := atomicGrant(), atomicDeadline(t)
				switch name {
				case "no-grant":
					grant = xfercontract.CommitGrant{}
				case "expired":
					deadline = 1
				case "canceled":
					if err := r.Stop(deadline); err != nil {
						t.Fatal(err)
					}
				case "parent-swap":
					parent := filepath.Dir(dst)
					if err := os.Rename(parent, parent+"-held"); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = os.RemoveAll(parent + "-held") })
					if err := os.Mkdir(parent, 0o700); err != nil {
						t.Fatal(err)
					}
				case "tmp-swap":
					tmp := filepath.Join(filepath.Dir(dst), r.state.TmpName)
					if err := os.Remove(tmp); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(tmp, []byte("new complete"), 0o600); err != nil {
						t.Fatal(err)
					}
				case "destination-symlink":
					if err := os.Remove(dst); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink("victim", dst); err != nil {
						t.Fatal(err)
					}
				case "unsynced":
					if _, err := r.WriteAt([]byte("suffix"), r.written); err != nil {
						t.Fatal(err)
					}
				}
				out := r.Commit(grant, deadline)
				wantOutcome := xfercontract.OutcomeNotEntered
				if name == "no-grant" {
					wantOutcome = ""
				}
				if out.Outcome != wantOutcome || out.Code == "" {
					t.Fatalf("invalid commit admission landed: %s %+v", name, out)
				}
				if name == "expired" && r.Commit(grant, atomicDeadline(t)) != out {
					t.Fatal("late retry renewed expired byte admission")
				}
			})
		}
	})
}
