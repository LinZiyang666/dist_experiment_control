package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/proto"
)

func TestBExternalReviewBadRequestIsUsage(t *testing.T) {
	if got := brokerCodeExitClass(adminsock.CodeBadRequest); got != exitUsage {
		t.Fatalf("bad_request should map to exit %d (usage/operator input), got %d", exitUsage, got)
	}
}

func TestBExternalReviewRestoreAbortIsUsage(t *testing.T) {
	cmd := newClusterRestoreCmd()
	cmd.SetArgs([]string{"/tmp/nonexistent-bundle", "--confirm-node-id", "brk-a"})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("restore without typed confirmation must abort")
	}
	if !strings.Contains(err.Error(), "aborted") {
		t.Fatalf("restore should abort at the confirmation gate, got %v", err)
	}
	if got := classifyExit(err); got != exitUsage {
		t.Fatalf("restore confirmation abort should map to exit %d (usage/operator input), got %d", exitUsage, got)
	}
}

func TestF9IncidentWriteRefusesSymlinkAndClobber(t *testing.T) {
	dir := t.TempDir()
	blob := []byte(`{"ok":true}`)

	// fresh path: O_EXCL create succeeds.
	p := filepath.Join(dir, "incident.json")
	if err := writeIncidentFile(p, blob, false); err != nil {
		t.Fatalf("fresh write must succeed: %v", err)
	}
	// existing file without --force: refused (no clobber).
	if err := writeIncidentFile(p, blob, false); err == nil {
		t.Fatal("must refuse to clobber an existing --out without --force")
	}
	// --force overwrites a regular file.
	if err := writeIncidentFile(p, blob, true); err != nil {
		t.Fatalf("--force must overwrite a regular file: %v", err)
	}
	// a symlink at the path is NEVER followed, even with --force.
	target := filepath.Join(dir, "sensitive")
	if err := os.WriteFile(target, []byte("DO NOT CLOBBER"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if err := writeIncidentFile(link, blob, true); err == nil {
		t.Fatal("must refuse to follow a symlink (O_NOFOLLOW) even with --force")
	}
	if b, _ := os.ReadFile(target); string(b) != "DO NOT CLOBBER" {
		t.Fatal("the symlink target must be untouched")
	}
}

func TestBExternalReviewClusterNodeIDRejectsOptionLikeIDs(t *testing.T) {
	for _, id := range []string{"-brk-a", "--help"} {
		if err := proto.ValidateClusterNodeID(id); err == nil {
			t.Fatalf("cluster node_id %q must be rejected: it is rendered into copy-paste CLI commands and would be parsed as an option", id)
		}
	}
}
