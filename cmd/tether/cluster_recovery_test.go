package main

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// cluster_recovery_test.go — C6 建议7: the `cluster recovery` group aliases the existing offline
// commands without removing the top-level ones, and `recover`/`recovery` coexist unambiguously.

func findChild(parent *cobra.Command, name string) *cobra.Command {
	for _, c := range parent.Commands() {
		if c.Name() == name {
			return c
		}
	}
	return nil
}

func TestRecoveryTreeShapeAndGroups(t *testing.T) {
	root := newClusterCmd()
	rec := findChild(root, "recovery")
	if rec == nil {
		t.Fatal("cluster recovery group missing")
	}
	// The three documented aliases.
	for _, name := range []string{"diagnose", "force-single", "rejoin"} {
		if findChild(rec, name) == nil {
			t.Errorf("cluster recovery %s missing", name)
		}
	}
	if rejoin := findChild(rec, "rejoin"); rejoin != nil && findChild(rejoin, "prepare") == nil {
		t.Error("cluster recovery rejoin prepare missing")
	}
	// The TOP-LEVEL escape commands MUST still exist (the aliases are additive, not a move).
	for _, name := range []string{"force-single", "recover", "restore"} {
		if findChild(root, name) == nil {
			t.Errorf("top-level cluster %s must still exist after adding the recovery aliases", name)
		}
	}
	// recovery is in the escape group.
	if rec.GroupID != "escape" {
		t.Errorf("recovery group must be in 'escape', got %q", rec.GroupID)
	}
}

func TestRecoverVsRecoveryNoPrefixAmbiguity(t *testing.T) {
	root := newClusterCmd()
	// EnablePrefixMatching must stay OFF so "recover" resolves to the top-level command, not the
	// "recovery" group (a prefix of it). Assert both resolve to distinct commands.
	rec := findChild(root, "recover")
	recy := findChild(root, "recovery")
	if rec == nil || recy == nil {
		t.Fatal("both recover and recovery must exist as distinct commands")
	}
	if rec == recy {
		t.Fatal("recover and recovery must be distinct commands")
	}
}

func TestRecoveryDiagnoseRequiresSelfID(t *testing.T) {
	root := newClusterCmd()
	var buf strings.Builder
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetArgs([]string{"recovery", "diagnose"}) // no --self-id
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "self-id") {
		t.Fatalf("recovery diagnose must require --self-id, got err=%v\n%s", err, buf.String())
	}
}
