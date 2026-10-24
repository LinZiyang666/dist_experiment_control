//go:build linux && amd64

// Copy into internal/infraroot. Uses private temporary directories and the real
// ACME selector. The unit adapter injects the crash boundary before unit CAS.
package infraroot

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"golang.org/x/sys/unix"
)

type reviewReleaseState struct{ fence infrahost.StartupFence }

func (s *reviewReleaseState) Load(c.Component, c.Deadline) (infrahost.StartupFence, error) {
	return s.fence, nil
}
func (s *reviewReleaseState) CompareAndSwap(infrahost.StartupFence, infrahost.StartupFence, c.HostRequest, c.Deadline) (c.HostReceipt, error) {
	return c.HostReceipt{}, errors.New("unused")
}

type reviewReleaseUnits struct{ calls int }

func (u *reviewReleaseUnits) Inspect(c.ID, c.Deadline) (c.Census, error) {
	return c.Census{}, errors.New("unused")
}
func (u *reviewReleaseUnits) Do(c.HostRequest, c.Deadline, c.Stop) (c.HostReceipt, error) {
	u.calls++
	if u.calls == 1 {
		return c.HostReceipt{}, errors.New("injected failure before unit state CAS")
	}
	return c.HostReceipt{Applied: true}, nil
}

// origin: S9 internal review rv-infra F09
func TestACMEReleaseResumesAfterUnitCommitFailure(t *testing.T) {
	dl, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rootPath, initialPath := t.TempDir(), t.TempDir()
	for _, path := range []string{rootPath, initialPath} {
		if err := os.Chmod(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(initialPath, "account-key"), []byte("current-B"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.Open(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, root)
	initial, err := os.Open(initialPath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, initial)
	storage, err := infrahost.NewACMEStorage(infrahost.ACMEConfig{Root: root, Initial: initial, UID: os.Geteuid(), GID: os.Getegid(), Exit: func(c.Process, c.Deadline, c.Stop) (c.Census, error) {
		return c.Census{Complete: true, Generation: 1, Digest: c.Digest{9}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	storageClosed := false
	defer func() {
		if !storageClosed {
			closeTestResource(t, storage)
		}
	}()
	old := c.Process{BootID: "boot", PID: 100, StartTicks: 1, InvocationID: "old", ExeDevice: 1, ExeInode: 2, ExeSHA256: c.Digest{1}, Cgroup: "/system.slice/caddy.service"}
	r := c.HostRequest{Version: 1, Op: c.FenceStart, Component: c.Caddy, Tx: c.ID{1}, Domain: c.ID{2}, Step: 1, Sequence: 1, PolicyRevision: 1, KeyID: "test-policy", HostBootID: old.BootID, Nonce: c.ID{3}, Expected: old, ExpectedJournal: c.Digest{4}, BeforeCensus: c.Digest{5}, Payload: c.Digest{6}, RemainingNS: uint64(time.Second)}
	if _, err := infrahost.FenceACME(storage, r, dl, nil); err != nil {
		t.Fatal(err)
	}
	r.Op = c.FinalizeStorage
	final, err := storage.Do(r, dl, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Op = c.ReleaseStart
	r.Candidate = c.Artifact{Kind: c.CaddyBinary, Version: "2.7.6", GOOS: "linux", GOARCH: "amd64", ArchiveSHA256: c.Digest{1}, BinarySHA256: c.Digest{2}, CatalogSHA256: c.Digest{3}}
	r.Config = c.Digest{7}
	state := &reviewReleaseState{fence: infrahost.StartupFence{Tx: r.Tx, Domain: r.Domain, Step: r.Step, Component: r.Component, Revision: 1, Closed: true, OldExited: true, StorageFinalized: r.Payload, Deadline: dl}}
	units := &reviewReleaseUnits{}
	h := &storageUnits{units: units, storage: storage, state: state, revision: func(c.HostRequest, c.Deadline) (uint64, error) { return final.Revision, nil }}
	if _, err := h.Do(r, dl, nil); err == nil || units.calls != 1 {
		t.Fatal("did not reach failure after durable ACME release")
	}
	// Reconstruct the storage owner exactly at the worker-death boundary; no
	// in-memory success flag may authorize the second unit CAS.
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	storageClosed = true
	reopened, err := infrahost.NewACMEStorage(infrahost.ACMEConfig{Root: root, Initial: initial, UID: os.Geteuid(), GID: os.Getegid(), Exit: func(c.Process, c.Deadline, c.Stop) (c.Census, error) {
		return c.Census{}, errors.New("old exit must not be rerun during release")
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, reopened)
	h = &storageUnits{units: units, storage: reopened, state: state, revision: h.revision}
	for _, mutate := range []func(*c.HostRequest){
		func(r *c.HostRequest) { r.Tx[0]++ },
		func(r *c.HostRequest) { r.Domain[0]++ },
		func(r *c.HostRequest) { r.Expected.StartTicks++ },
		func(r *c.HostRequest) { r.Payload[0]++ },
	} {
		bad := r
		mutate(&bad)
		if _, err := h.Do(bad, dl, nil); err == nil || units.calls != 1 {
			t.Fatal("foreign release reached unit CAS")
		}
	}
	if _, err := infrahost.ReleaseACMEStart(reopened, r.Tx, r.Domain, final.Revision-1, dl, nil); err == nil {
		t.Fatal("stale final permit accepted")
	}
	if _, err := h.Do(r, dl, nil); err != nil || units.calls != 2 {
		t.Fatalf("retry cannot complete unit release after storage selector advanced: unit calls=%d err=%v", units.calls, err)
	}
	view, err := infrahost.CurrentACMEView(reopened, r.Tx, r.Domain, dl)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, view.Directory)
	writer, err := infrahost.ClaimACMEWriter(reopened, r.Tx, r.Domain, view.Revision, dl, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, writer.Directory)
	defer closeTestResource(t, writer.Lease)
	fd, err := unix.Openat(int(writer.Directory.Fd()), "account-key", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal(err)
	}
	key := os.NewFile(uintptr(fd), "test-account-key")
	contents, err := io.ReadAll(key)
	closeTestResource(t, key)
	if err != nil || string(contents) != "current-B" {
		t.Fatal("release recovery regressed the finalized credentials")
	}
	otherDeadline, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, err := infrahost.ClaimACMEWriter(reopened, r.Tx, r.Domain, view.Revision, otherDeadline, nil); err == nil {
		closeTestResource(t, duplicate.Directory)
		closeTestResource(t, duplicate.Lease)
		t.Fatal("release recovery admitted two writers")
	}
}
