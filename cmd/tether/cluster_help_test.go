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
		if strings.TrimSpace(c.Example) == "" {
			t.Errorf("cluster %s has no Example block", c.Name())
		}
		if c.GroupID == "" {
			t.Errorf("cluster %s is not assigned to a group", c.Name())
		}
	}
	if g := root.Groups(); len(g) != 4 {
		t.Errorf("want 4 cluster groups, got %d", len(g))
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
	if s := find("takeover-natsconf").Short; !strings.Contains(s, "nats-server -t") || !strings.Contains(s, ".bak") {
		t.Errorf("takeover-natsconf Short should lead with the safety net: %q", s)
	}
	if ex := find("add").Example; !strings.Contains(ex, "node-pub") || !strings.Contains(ex, "sign-join") {
		t.Errorf("add Example should show the node-pub prereq + sign-join: %q", ex)
	}
	// init carries the migrate-to-cluster alias (item 3).
	if al := find("init").Aliases; len(al) == 0 || al[0] != "migrate-to-cluster" {
		t.Errorf("init should alias migrate-to-cluster, got %v", al)
	}
}
