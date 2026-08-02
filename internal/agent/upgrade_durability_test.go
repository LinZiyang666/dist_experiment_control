package agent

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// origin: upgrade-safety external review F6 — rename gives crash ATOMICITY,
// not durability: without file+directory fsyncs in the right order, a power
// cut after the flip can resurface a truncated dst, a marker without its
// prev slot, or an unpersisted executable bit. These tests pin the ORDER of
// the sync protocol via the upgradeSyncObserver injection point, and prove
// the install pipeline fails closed when a sync step fails.

func testTarball(t *testing.T, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "tether", Mode: 0o755, Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func installFixtureAgent(t *testing.T) (*Agent, string) {
	t.Helper()
	dir := t.TempDir()
	dst := filepath.Join(dir, "tether")
	if err := os.WriteFile(dst, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}
	a := &Agent{cfg: Config{
		Logger:                slog.New(slog.DiscardHandler),
		SID:                   "lab",
		NID:                   "n1",
		UpgradeExecutablePath: dst,
		UpgradeNoExit:         true,
	}}
	return a, dst
}

func TestInstallSyncProtocolOrder(t *testing.T) {
	var events []string
	upgradeSyncObserver = func(kind, path string) error {
		events = append(events, kind+":"+filepath.Base(path))
		return nil
	}
	t.Cleanup(func() { upgradeSyncObserver = nil })

	a, dst := installFixtureAgent(t)
	newVersion, err := a.installNewBinary(testTarball(t, fakeVersionScript(t)), dst)
	if err != nil {
		t.Fatalf("install: %v (events=%v)", err, events)
	}
	if newVersion == "" {
		t.Fatal("install returned an empty version")
	}

	// The protocol, in order (plan §3.1 + F6):
	//   1. candidate file fsync (post-chmod, same fd) — inside extract
	//   2. dir fsync after the prev slot exists
	//   3. marker tmp file fsync, then 4. dir fsync after the marker rename
	//   5. dir fsync after the dst flip
	wantOrder := []string{"file:tether", "dir:", "file:.tether-upgrade-marker-", "dir:", "dir:"}
	idx := 0
	for _, ev := range events {
		if idx < len(wantOrder) && strings.HasPrefix(ev, wantOrder[idx]) {
			idx++
		}
	}
	if idx != len(wantOrder) {
		t.Fatalf("sync protocol order not observed: matched %d/%d of %v in\n  %v",
			idx, len(wantOrder), wantOrder, events)
	}
}

// A failed sync BEFORE the marker exists must abort the install with the
// disk in its pre-install state: no marker, no flip. (The prev slot itself
// is cleaned up by the failure path.)
func TestInstallSyncFailureFailsClosed(t *testing.T) {
	upgradeSyncObserver = func(kind, path string) error {
		if kind == "dir" {
			return fmt.Errorf("injected power-adjacent dir sync failure")
		}
		return nil
	}
	t.Cleanup(func() { upgradeSyncObserver = nil })

	a, dst := installFixtureAgent(t)
	_, err := a.installNewBinary(testTarball(t, fakeVersionScript(t)), dst)
	if err == nil || !strings.Contains(err.Error(), "injected") {
		t.Fatalf("install must fail on a dir sync failure; got %v", err)
	}
	if got, _ := os.ReadFile(dst); string(got) != "OLD" {
		t.Errorf("dst must be untouched after a failed install; got %q", string(got))
	}
	if _, err := os.Stat(upgradeMarkerPath(dst)); !os.IsNotExist(err) {
		t.Errorf("no marker may exist after a pre-marker sync failure (err=%v)", err)
	}
}

// origin: upgrade-safety external re-review F11 — writeUpgradeMarker can
// complete its rename and then fail the directory fsync. Returning an install
// error is correct, but the visible pending marker must be compensated before
// the host lock is released; otherwise the old, healthy process rejects every
// retry as upgrade_in_progress for 120s even though no flip occurred and the
// prev slot was already removed.
func TestMarkerDirSyncFailureDoesNotLeavePendingTransaction(t *testing.T) {
	var dirSyncs int
	upgradeSyncObserver = func(kind, _ string) error {
		if kind == "dir" {
			dirSyncs++
			if dirSyncs == 2 { // marker rename, after the prev slot's successful sync
				return fmt.Errorf("injected marker directory sync failure")
			}
		}
		return nil
	}
	t.Cleanup(func() { upgradeSyncObserver = nil })

	a, dst := installFixtureAgent(t)
	_, err := a.installNewBinary(testTarball(t, fakeVersionScript(t)), dst)
	if err == nil || !strings.Contains(err.Error(), "marker directory sync") {
		t.Fatalf("install must surface marker directory sync failure; got %v", err)
	}
	if got, _ := os.ReadFile(dst); string(got) != "OLD" {
		t.Fatalf("dst changed before the failed marker durability point: %q", got)
	}
	m, rerr := readUpgradeMarker(upgradeMarkerPath(dst))
	if rerr != nil {
		t.Fatal(rerr)
	}
	if m != nil && m.State == upgradeStatePending {
		t.Fatalf("failed pre-flip transaction left a live pending marker: %+v", m)
	}
}

// fakeVersionScript emits the frozen version line for the CURRENT epoch —
// the smoke gate (F5) rejects anything else.
func fakeVersionScript(t *testing.T) []byte {
	t.Helper()
	return []byte("#!/bin/sh\necho 'tether v9.9.9-test (proto v2)'\n")
}
