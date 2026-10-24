//go:build linux

package infrahost

// Real private filesystem trees, X.509 renewals, supervised writer processes,
// pidfds and flock OFDs run here. Policy/domain authorization is P04's boundary.
// Stock Caddy, real ACME protocol and systemd restart are P29/P31/86.I evidence.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// origin: hot-update S9 plan §5.3 / T20 / M11
func TestACMEFinalDeltaFollowsOldExit(t *testing.T) {
	if path := os.Getenv(acmeWriterEnv); path != "" {
		acmeWriterChild(t, path)
		return
	}
	t.Run("current_delta_and_rollback", exerciseACME)
	t.Run("exit_proof_is_complete", checkACMEExitProof)
	t.Run("descriptor_confinement", checkACMEConfinement)
	t.Run("bounded_tree", checkACMEBounds)
	t.Run("backend_admission", checkACMEAdmission)
}

// origin: hot-update S9 plan §12 P09 / §9.5
func TestACMELifecycle(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseACME(t)
	}
	testharness.AssertNoGoroutineLeak(t, "ACME storage", beforeG)
	testharness.AssertNoFDLeak(t, "ACME storage", beforeFD, 0)
}

func exerciseACME(t *testing.T) {
	t.Helper()
	f := newACMEFixture(t)
	// Released per round for the leak oracle; the fixture's Cleanup close is the
	// idempotent backstop, so this close's outcome is not the subject here.
	defer func() { _ = f.storage.Close() }()
	warm, err := WarmACME(f.storage, acmeDeadline(t, 5*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = warm.Directory.Close() }()
	defer func() {
		if err := RetireACMEWarm(f.storage, warm); err != nil {
			t.Error(err)
		}
	}()
	assertACMERevision(t, warm.Directory, "A", true)
	if body, err := os.ReadFile(filepath.Join(f.initial, "locks", "renew.lock")); err != nil || string(body) != "old-writer-lock" {
		t.Fatal("warm copy removed the live writer's lock")
	}
	f.writer.renew(t) // Real late write by the still-running original writer.
	if receipt, err := f.storage.Do(f.request, acmeDeadline(t, 5*time.Second), nil); err == nil || receipt.Applied {
		t.Fatal("final delta accepted an alive writer")
	}
	if state := acmeStateFixture(t, f.storage); state.Current != "" || !state.Fenced {
		t.Fatal("warm snapshot changed the final selector")
	}
	f.writer.close()
	receipt, err := f.storage.Do(f.request, acmeDeadline(t, 5*time.Second), nil)
	if err != nil || !receipt.Applied || receipt.Revision <= f.fenceRevision {
		t.Fatalf("final delta failed: %v", err)
	}
	replay, err := f.storage.Do(f.request, acmeDeadline(t, 5*time.Second), nil)
	if err != nil || !reflect.DeepEqual(receipt, replay) {
		t.Fatal("final delta replay changed its durable receipt")
	}
	state := acmeStateFixture(t, f.storage)
	selected, err := f.storage.current(state)
	if err != nil {
		t.Fatal(err)
	}
	assertACMERevision(t, selected, "B", false)
	_ = selected.Close()
	assertACMERevision(t, warm.Directory, "A", true)
	for _, field := range []string{"tx", "domain"} {
		tx, domain := f.request.Tx, f.request.Domain
		if field == "tx" {
			tx = infracontract.ID{90}
		} else {
			domain = infracontract.ID{90}
		}
		if _, err := ReleaseACMEStart(f.storage, tx, domain, receipt.Revision, acmeDeadline(t, time.Second), nil); err == nil {
			t.Fatal("foreign transaction/domain released startup")
		}
	}
	released, err := ReleaseACMEStart(f.storage, f.request.Tx, f.request.Domain, receipt.Revision, acmeDeadline(t, time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.storage.Do(f.request, acmeDeadline(t, time.Second), nil); !errors.Is(err, errACMENotReady) {
		t.Fatal("finalization was admitted after startup had been released")
	}
	for _, field := range []string{"tx", "domain", "revision"} {
		tx, domain, revision := f.request.Tx, f.request.Domain, released
		switch field {
		case "tx":
			tx = infracontract.ID{90}
		case "domain":
			domain = infracontract.ID{90}
		case "revision":
			revision++
		}
		if foreign, err := ClaimACMEWriter(f.storage, tx, domain, revision, acmeDeadline(t, 30*time.Millisecond), nil); err == nil {
			_ = foreign.Lease.Close()
			_ = foreign.Directory.Close()
			t.Fatal("foreign transaction/domain/revision claimed renewal ownership")
		}
	}
	writer, err := ClaimACMEWriter(f.storage, f.request.Tx, f.request.Domain, released, acmeDeadline(t, time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Directory.Close() }()
	if duplicate, err := ClaimACMEWriter(f.storage, f.request.Tx, f.request.Domain, released, acmeDeadline(t, time.Second), nil); err == nil {
		_ = duplicate.Lease.Close()
		_ = duplicate.Directory.Close()
		t.Fatal("allowed two renewal writers")
	}
	// The new writer inherits the same lease OFD. A real later renewal is written
	// into the selected tree, then rollback must follow this current C revision.
	currentPath := filepath.Join(f.root, state.Current)
	renewal := t.TempDir()
	writeACMERevision(t, renewal, "C")
	newWriter := startACMEWriter(t, currentPath, renewal, writer.Lease)
	_ = writer.Lease.Close()
	if lease, err := acmeLock(f.storage.root, "writer.lock", acmeDeadline(t, time.Second), nil, false); err == nil {
		_ = lease.Close()
		t.Fatal("parent close released child's final lock reference")
	}
	newWriter.renew(t)
	newWriter.close()
	f.storage.config.Exit = newWriter.exitCensus
	rollback := acmeRequestFixture(newWriter.process)
	rollback.Tx, rollback.Step = infracontract.ID{9}, 2
	fenceACMEFixture(t, f.storage, rollback)
	if _, err := f.storage.Do(rollback, acmeDeadline(t, 5*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	state = acmeStateFixture(t, f.storage)
	selected, err = f.storage.current(state)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = selected.Close() }()
	assertACMERevision(t, selected, "C", false)
	if entries, err := os.ReadDir(f.root); err != nil {
		t.Fatal(err)
	} else {
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "tree-") && entry.Name() != state.Current {
				t.Fatal("retained an obsolete writable storage tree")
			}
		}
	}
	assertACMENoSecret(t, receipt, replay, state)
	// Cleanup runs inside each exercise, before the direct five-round leak oracle.
	f.writer.close()
	newWriter.close()
	_ = f.writer.pidfd.Close()
	_ = newWriter.pidfd.Close()
}

func checkACMEExitProof(t *testing.T) {
	f := newACMEFixture(t)
	f.writer.close()
	for _, name := range []string{"unknown", "remaining_writer", "unknown_digest", "pending_owner"} {
		t.Run(name, func(t *testing.T) {
			f.storage.config.Exit = func(infracontract.Process, infracontract.Deadline, infracontract.Stop) (infracontract.Census, error) {
				c := infracontract.Census{Complete: true, Digest: infracontract.Digest{1}}
				switch name {
				case "unknown":
					c.Complete = false
				case "remaining_writer":
					c.Processes = []infracontract.Process{f.writer.process}
				case "unknown_digest":
					c.Digest = infracontract.Digest{}
				case "pending_owner":
					c.OldOwnerPending = 1
				}
				return c, nil
			}
			if result, err := f.storage.Do(f.request, acmeDeadline(t, time.Second), nil); err == nil || result.Applied {
				t.Fatal("accepted incomplete old-writer exit proof")
			}
		})
	}
	if _, err := f.storage.Inspect(f.request.Tx, acmeDeadline(t, time.Second)); !errors.Is(err, infracontract.ErrUnsupported) {
		t.Fatal("ACME backend signed a host census")
	}
	r := f.request
	r.Op = infracontract.SignalStop
	if _, err := f.storage.Do(r, acmeDeadline(t, time.Second), nil); !errors.Is(err, infracontract.ErrUnsupported) {
		t.Fatal("ACME backend accepted another owner's verb")
	}
}

func checkACMEConfinement(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "fifo", "writable", "state_symlink"} {
		t.Run(kind, func(t *testing.T) {
			f := newACMEFixture(t)
			f.writer.close()
			outside := filepath.Join(t.TempDir(), "private-canary")
			acmeWrite(t, outside, []byte(acmeCanary))
			path := filepath.Join(f.initial, "malicious")
			var err error
			switch kind {
			case "symlink":
				err = os.Symlink(outside, path)
			case "hardlink":
				err = os.Link(outside, path)
			case "fifo":
				err = unix.Mkfifo(path, 0600)
			case "writable":
				acmeWrite(t, path, []byte(acmeCanary))
				err = os.Chmod(path, 0666)
			case "state_symlink":
				err = os.Symlink(outside, filepath.Join(f.root, "tree-"+strings.Repeat("a", 32)))
				state := acmeStateFixture(t, f.storage)
				state.Pending, state.Payload, state.RequestDigest = "tree-"+strings.Repeat("a", 32), f.request.Payload, infracontract.Digest{}
				state.RequestDigest, _ = acmeRequest(f.request)
				if err == nil {
					err = f.storage.writeState(state)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if receipt, err := f.storage.Do(f.request, acmeDeadline(t, time.Second), nil); err == nil || receipt.Applied {
				t.Fatal("migration escaped the approved filesystem tree")
			} else {
				assertACMENoSecret(t, err)
			}
			body, err := os.ReadFile(outside)
			if err != nil || !bytes.Equal(body, []byte(acmeCanary)) {
				t.Fatal("migration modified an unrelated private inode")
			}
		})
	}
}

func checkACMEAdmission(t *testing.T) {
	f := newACMEFixture(t)
	if backend, err := NewACMEStorage(ACMEConfig{}); err == nil {
		_ = backend.Close()
		t.Fatal("missing root policy/exit observer advertised a backend")
	}
	for _, kind := range []string{"exit_observer", "uid", "gid", "uid_overflow", "gid_overflow"} {
		t.Run(kind, func(t *testing.T) {
			config := f.config
			switch kind {
			case "exit_observer":
				config.Exit = nil
			case "uid":
				config.UID = -1
			case "gid":
				config.GID = -1
			case "uid_overflow":
				uid := uint64(^uint32(0))
				config.UID = int(uid)
			case "gid_overflow":
				gid := uint64(^uint32(0))
				config.GID = int(gid)
			}
			if backend, err := NewACMEStorage(config); err == nil {
				_ = backend.Close()
				t.Fatal("missing proof/ownership policy advertised a backend")
			}
		})
	}
	for _, kind := range []string{"regular_file", "readable_directory"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "unapproved-root")
			if kind == "regular_file" {
				acmeWrite(t, path, nil)
			} else {
				if err := os.Mkdir(path, 0755); err != nil {
					t.Fatal(err)
				}
			}
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = file.Close() }()
			config := f.config
			config.Root = file
			if backend, err := NewACMEStorage(config); err == nil {
				_ = backend.Close()
				t.Fatal("unapproved descriptor became a storage root")
			}
		})
	}
}

func checkACMEBounds(t *testing.T) {
	for _, bound := range []string{"file", "total", "depth", "entries", "total_entries", "prefix"} {
		t.Run(bound, func(t *testing.T) {
			f := newACMEFixture(t)
			f.writer.close()
			switch bound {
			case "file", "total":
				n, size := 1, int64((16<<20)+1)
				if bound == "total" {
					n, size = 5, 16<<20
				}
				for i := 0; i < n; i++ {
					file, err := os.OpenFile(filepath.Join(f.initial, fmt.Sprintf("large-%d", i)), os.O_CREATE|os.O_WRONLY, 0600)
					if err != nil {
						t.Fatal(err)
					}
					err = file.Truncate(size)
					_ = file.Close()
					if err != nil {
						t.Fatal(err)
					}
				}
			case "depth":
				path := f.initial
				for i := 0; i < 34; i++ {
					path = filepath.Join(path, "nested")
					if err := os.Mkdir(path, 0700); err != nil {
						t.Fatal(err)
					}
				}
			case "entries":
				for i := 0; i < 4097; i++ {
					if err := os.WriteFile(filepath.Join(f.initial, fmt.Sprintf("entry-%04d", i)), nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
			case "total_entries":
				for _, child := range []string{"first", "second"} {
					path := filepath.Join(f.initial, child)
					if err := os.Mkdir(path, 0700); err != nil {
						t.Fatal(err)
					}
					for i := 0; i < 2049; i++ {
						if err := os.WriteFile(filepath.Join(path, fmt.Sprintf("entry-%04d", i)), nil, 0600); err != nil {
							t.Fatal(err)
						}
					}
				}
			case "prefix":
				dir, err := os.Open(f.initial)
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 18; i++ {
					name := strings.Repeat("d", 250)
					if err := unix.Mkdirat(int(dir.Fd()), name, 0700); err != nil {
						_ = dir.Close()
						t.Fatal(err)
					}
					next, err := acmeOpenDirectory(dir, name)
					_ = dir.Close()
					if err != nil {
						t.Fatal(err)
					}
					dir = next
				}
				_ = dir.Close()
			}
			var result infracontract.HostReceipt
			var err error
			// A large tree may need several bounded host actions. Checkpoint reuse
			// must eventually reach the inventory guard without selecting a partial.
			for action := 0; action < 4; action++ {
				result, err = f.storage.Do(f.request, acmeDeadline(t, 8*time.Second), nil)
				if !errors.Is(err, errACMEBudget) {
					break
				}
			}
			if !errors.Is(err, infracontract.ErrIncomplete) || result.Applied {
				t.Fatal("oversized ACME tree escaped its inventory bound")
			}
			if state := acmeStateFixture(t, f.storage); !state.Fenced || state.Finalized {
				t.Fatal("an incomplete tree released startup")
			}
		})
	}
}
