package cluster_test

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/session"
)

func assertNoArgs(t *testing.T, name string, cmd *cluster.Command) {
	t.Helper()
	if cmd == nil {
		t.Fatalf("%s returned nil command", name)
	}
	if len(cmd.Body) == 0 {
		t.Fatalf("%s returned empty command body", name)
	}
	for i, st := range cmd.Body {
		if len(st.Args) != 0 {
			t.Fatalf("%s statement %d used Statement.Args=%v; D2 real ops must be all-literal SQL", name, i, st.Args)
		}
		if st.SQL == "" {
			t.Fatalf("%s statement %d has empty SQL", name, i)
		}
	}
}

// origin: d2_command_shape_review_test.go (renamed in B6) — docs/reviews/d2-external-review.md
func TestD2RealOpsDoNotUseStatementArgs_Review(t *testing.T) {
	now := time.Date(2026, 6, 21, 13, 14, 15, 123456789, time.FixedZone("CST", 8*3600))
	cfg := fixedClock(now)

	db := freshDB(t)
	cmd, err := session.PlanCreate(db, "lab", "lab", "SHA256:owner", "pinhash", now)
	if err != nil {
		t.Fatal(err)
	}
	assertNoArgs(t, "SessionCreate", cmd)
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}

	reg := node.RegisterInput{SID: "lab", NID: "lab-1", BootID: "boot1", ReleaseVersion: "v1", ProtoVersion: 2, ProxyCapable: true}
	cmd, err = node.PlanRegister(db, reg, now)
	if err != nil {
		t.Fatal(err)
	}
	assertNoArgs(t, "NodeRegister", cmd)
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}

	p := proc.Process{PID: "01reviewpid00000000000000aa", SID: "lab", NID: "lab-1", Argv: []string{"sleep", "1"}, Cwd: "/tmp", StartedAt: now, StartedByFP: "SHA256:actor", BootID: "boot1", StartTimeTicks: 1750512345123456789}
	cmd, err = proc.PlanInsert(db, p)
	if err != nil {
		t.Fatal(err)
	}
	assertNoArgs(t, "ProcCreate", cmd)
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}

	cmd, err = proc.PlanMarkExited(db, p.PID, p.SID, 0, now)
	if err != nil {
		t.Fatal(err)
	}
	assertNoArgs(t, "ProcMarkExited", cmd)
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}

	// SID is REQUIRED since external review B-2: markExitedSQL refuses to render an
	// unscoped `WHERE pid=?`, which is the predicate that let one session retire another
	// session's process. Production reconcile is per-session and always has it.
	cmd, err = proc.PlanReconcileBatch(proc.ReconcileBatchInput{
		SID: "lab", Marks: []proc.ExitMark{{PID: p.PID, ExitCode: -1, When: now}}})
	if err != nil {
		t.Fatal(err)
	}
	assertNoArgs(t, "ReconcileBatch", cmd)

	_, cmd, err = port.PlanAllocate(db, "lab", "lab-1", "jupyter", 8888, 0, "SHA256:actor", "", false, cfg)
	if err != nil {
		t.Fatal(err)
	}
	assertNoArgs(t, "PortAllocate", cmd)
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}
	var publicPort int
	if err := db.QueryRow(`SELECT port FROM port_allocations WHERE name='jupyter'`).Scan(&publicPort); err != nil {
		t.Fatal(err)
	}

	cmd, err = port.PlanRevoke(db, publicPort, now)
	if err != nil {
		t.Fatal(err)
	}
	assertNoArgs(t, "PortRevoke", cmd)
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}

	_, cmd, err = port.PlanAllocate(db, "lab", "lab-1", "jupyter2", 9999, 0, "SHA256:actor", "", false, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT port FROM port_allocations WHERE name='jupyter2'`).Scan(&publicPort); err != nil {
		t.Fatal(err)
	}
	cmd, err = port.PlanFree(db, publicPort, now)
	if err != nil {
		t.Fatal(err)
	}
	assertNoArgs(t, "PortFree", cmd)

	cmd, err = agentprov.PlanProvisionWithPIN(db, "lab", "lab-1", "SHA256:agent", "1234", acceptPIN, now)
	if err != nil {
		t.Fatal(err)
	}
	assertNoArgs(t, "AgentProvision", cmd)
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}

	cmd, err = session.PlanJoinWithPIN(db, "lab", "SHA256:member", "1234", acceptPIN, now)
	if err != nil {
		t.Fatal(err)
	}
	assertNoArgs(t, "MemberJoin", cmd)
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}

	cmd, err = node.PlanEvict("lab", "lab-1")
	if err != nil {
		t.Fatal(err)
	}
	assertNoArgs(t, "NodeEvict", cmd)

	cmd, err = session.PlanTombstone(db, "lab", now)
	if err != nil {
		t.Fatal(err)
	}
	assertNoArgs(t, "SessionTombstone", cmd)
	if err := cluster.ExecCommand(db, cmd); err != nil {
		t.Fatal(err)
	}

	cmd, err = session.PlanHardDelete(db, "lab")
	if err != nil {
		t.Fatal(err)
	}
	assertNoArgs(t, "SessionHardDelete", cmd)
}
