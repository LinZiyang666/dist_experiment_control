package main

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/execclient"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// exec_guidance_test.go — the operator guidance `tether exec` prints at
// FAILURE time must name commands that exist in this binary's command tree.
//
// A 74 ("accepted, result unknown") is the one exit where the operator has to
// act by hand — the ctl has given up on a task that keeps running on the
// agent — so the line it prints is the line that gets copied into a
// terminal. A dead-end command name there is exactly the trap
// cluster_guidance_guard_test.go exists for; this test walks the REAL cobra
// tree instead of a spelling list, so a command that is later added or
// removed changes the verdict mechanically.

// backtickTetherCmd matches `tether <sub> [<sub2> ...]` inside backticks, the
// spelling every guidance string in cmd/tether uses.
var backtickTetherCmd = regexp.MustCompile("`tether ([a-z][a-z0-9-]*(?: [a-z][a-z0-9-]*)*)")

// origin: hot-update S1 internal review, ctl lane — exec.go's post-create
// timeout message tells the operator to `use tether kill`; usage.md §4 (the
// FAQ table) says "当前没有独立 `tether kill` 命令", and the plan's own §1.1
// / architecture §22.4 item 9 say "没有 ctl 侧 kill 动词". The tree cannot be
// right on both counts.
func TestExecFailureGuidanceNamesExistingCommands(t *testing.T) {
	root := newRootCmd()
	handle := proto.ProcessHandle{RequestID: "req-1"}
	after := execclient.Result{Handle: handle, CreateSent: true}
	messages := []error{
		classifyReliableFailure("agt1", 2*time.Second, after, context.DeadlineExceeded),
		classifyReliableFailure("agt1", 2*time.Second, after, context.Canceled),
		classifyReliableFailure("agt1", 2*time.Second, after, nats.ErrConnectionClosed),
		classifyReliableFailure("agt1", 2*time.Second, after,
			&execclient.ProtocolError{Code: proto.CodeProcessInstanceLost, Message: "process request: lease was issued by another runtime instance", AfterCreate: true}),
		classifyReliableFailure("agt1", 2*time.Second, after,
			&execclient.SettledError{Result: execclient.Result{Handle: handle, Executed: true, StdoutAbort: "input/output error", StdoutBytes: 1}}),
	}
	for _, err := range messages {
		if err == nil {
			t.Fatal("nil error")
		}
		for _, m := range backtickTetherCmd.FindAllStringSubmatch(err.Error(), -1) {
			words := strings.Fields(m[1])
			cmd, rest, ferr := root.Find(words)
			if ferr != nil || cmd == nil || cmd == root || len(rest) != 0 {
				t.Errorf("guidance names a command this binary does not have: `tether %s` (in %q)", m[1], err.Error())
				continue
			}
			if got := strings.TrimPrefix(cmd.CommandPath(), "tether "); got != m[1] {
				t.Errorf("guidance `tether %s` resolves to %q, not to itself", m[1], cmd.CommandPath())
			}
		}
	}
	// Self-check: the walker sees a spelling that does exist, so an empty
	// FindAllStringSubmatch cannot be mistaken for a clean bill.
	if m := backtickTetherCmd.FindStringSubmatch("run `tether ps --json` first"); m == nil || m[1] != "ps" {
		t.Fatalf("the guidance matcher is broken: %v", m)
	}
	if cmd, _, err := root.Find([]string{"ps"}); err != nil || cmd == nil || cmd == root {
		t.Fatal("the command tree walker cannot resolve `tether ps`")
	}
}
