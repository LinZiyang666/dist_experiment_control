package main

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// TestClusterSubcommandsHaveExamplesAndGroups (B3 review M5 / item 5): every cluster subcommand
// has a non-empty Example, and the 4 context groups are registered + assigned.
func TestClusterSubcommandsHaveExamplesAndGroups(t *testing.T) {
	root := newClusterCmd()
	for _, c := range root.Commands() {
		if c.Name() == "help" || c.Name() == "completion" {
			continue
		}
		// Hidden deprecated aliases (e.g. takeover-natsconf → reconcile nats --manual, C3) are not part
		// of the documented surface and legitimately carry no Example/group.
		if c.Hidden {
			continue
		}
		if strings.TrimSpace(c.Example) == "" {
			t.Errorf("cluster %s has no Example block", c.Name())
		}
		if c.GroupID == "" {
			t.Errorf("cluster %s is not assigned to a group", c.Name())
		}
	}
	if g := root.Groups(); len(g) != 3 {
		t.Errorf("want 3 cluster groups (C8 dropped `local`), got %d", len(g))
	}
}

// TestClusterSafetyWording (B3 review M5 / item 8): the dangerous commands lead with the danger /
// safety net, and the add Example shows the node-pub prerequisite + sign-join.
func TestClusterSafetyWording(t *testing.T) {
	root := newClusterCmd()
	find := func(name string) *cobra.Command {
		for _, c := range root.Commands() {
			if c.Name() == name {
				return c
			}
		}
		t.Fatalf("subcommand %q not found", name)
		return nil
	}
	if s := find("force-single").Short; !strings.Contains(s, "split-brain") {
		t.Errorf("force-single Short should lead with split-brain risk: %q", s)
	}
	// C3: takeover-natsconf is now a HIDDEN DEPRECATED alias for `reconcile nats --manual` — its Short
	// must point users at the replacement. The safety-net wording moved to the reconcile nats command.
	if s := find("takeover-natsconf").Short; !strings.Contains(s, "DEPRECATED") || !strings.Contains(s, "reconcile nats --manual") {
		t.Errorf("takeover-natsconf Short should point at the replacement: %q", s)
	}
	// C8: `add`/`sign-join` are DELETED; `cluster join prepare` is the replacement. Its Example shows the
	// required identity flags (raft-addr/nats-route/tunnel-addr).
	join := find("join")
	var prepare *cobra.Command
	for _, c := range join.Commands() {
		if c.Name() == "prepare" {
			prepare = c
		}
	}
	if prepare == nil || !strings.Contains(prepare.Example, "nats-route") || !strings.Contains(prepare.Example, "tunnel-addr") {
		t.Errorf("join prepare Example should show the required identity flags")
	}
	// init carries the migrate-to-cluster alias (item 3).
	if al := find("init").Aliases; len(al) == 0 || al[0] != "migrate-to-cluster" {
		t.Errorf("init should alias migrate-to-cluster, got %v", al)
	}
}
