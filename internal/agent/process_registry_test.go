package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: docs/reviews/hot-update-plan.md §4.2. Real child and NATS bytes;
// no image replacement or reliable-output protocol is claimed by these tests.
func TestExecExplicitStdioPreservesInputAndEOF(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	for _, safe := range []bool{false, true} {
		for _, input := range [][]byte{nil, bytes.Repeat([]byte{0, 1, 2, '\n', 255}, 40000)} {
			name := "ordinary"
			mounts := fakeMountinfo([2]string{"/", "ext4"})
			if safe {
				name, mounts = "isolated", fakeMountinfo([2]string{"/nfs", "nfs4"}, [2]string{"/", "ext4"})
			}
			if len(input) == 0 {
				name += "-empty"
			}
			t.Run(name, func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				a := newTestAgent(t, mounts, nil)
				inbox := nc.NewInbox()
				sub, err := nc.SubscribeSync(inbox)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = sub.Unsubscribe() }()
				if err := nc.FlushTimeout(time.Second); err != nil {
					t.Fatal(err)
				}
				body, err := json.Marshal(proto.ExecReq{Cwd: t.TempDir(), Argv: []string{"/bin/sh", "-c", "cat; printf stderr-tail >&2; exit 23"}, Stdin: input})
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan struct{})
				go func() {
					a.handleExecForwarded(nc, &nats.Msg{Reply: inbox, Data: body})
					close(done)
				}()
				t.Cleanup(func() {
					a.reapManagedChildren()
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Error("exec handler did not finish cleanup")
					}
				})
				var stdout, stderr []byte
				deadline := time.Now().Add(5 * time.Second)
				for {
					msg, err := sub.NextMsg(time.Until(deadline))
					if err != nil {
						t.Fatal(err)
					}
					var c proto.ExecChunk
					if err := json.Unmarshal(msg.Data, &c); err != nil {
						t.Fatal(err)
					}
					switch c.Kind {
					case "stdout":
						stdout = append(stdout, c.Data...)
					case "stderr":
						stderr = append(stderr, c.Data...)
					case "error":
						t.Fatalf("exec error: %s", c.Error)
					case "exit":
						if c.ExitCode != 23 || !bytes.Equal(stdout, input) || string(stderr) != "stderr-tail" {
							t.Fatalf("stdio/exit mismatch: stdout=%d/%d stderr=%q rc=%d", len(stdout), len(input), stderr, c.ExitCode)
						}
						return
					}
				}
			})
		}
	}
}

// origin: docs/reviews/hot-update-plan.md §4.2. An ordinary exec must stay in
// the reconnect census and remain killable through the existing control verb.
func TestExecOwnershipSnapshotAndControlKill(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	url := startNATS(t)
	a := courierAgent(t, url)
	nc := a.ncBox.Load()
	inbox := nc.NewInbox()
	out, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = out.Unsubscribe() }()
	killSubject := "test.owned.kill"
	killSub, err := nc.Subscribe(killSubject, func(msg *nats.Msg) { a.handleKillForwarded(nc, msg) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = killSub.Unsubscribe() }()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	const id = "owned-exec"
	type result struct {
		code int
		err  error
	}
	done := make(chan result, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		rc, err := a.runChild(nc, inbox, id, &proto.ExecReq{Cwd: t.TempDir(),
			Argv: []string{"/bin/sh", "-c", "printf child-ready; exec sleep 30"},
		})
		done <- result{rc, err}
	}()
	t.Cleanup(func() {
		a.reapManagedChildren()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("exec handler did not finish cleanup")
		}
	})
	msg, err := out.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var chunk proto.ExecChunk
	if err := json.Unmarshal(msg.Data, &chunk); err != nil || chunk.Kind != "stdout" || string(chunk.Data) != "child-ready" {
		t.Fatalf("child readiness: %+v %v", chunk, err)
	}
	rows := localProcessSnapshot(a)
	if len(rows) != 1 || rows[0].PID != id || rows[0].State != "running" {
		t.Fatalf("live exec missing from reconnect snapshot: %+v", rows)
	}
	body, err := json.Marshal(proto.KillReq{PID: id, Signal: int(syscall.SIGKILL)})
	if err != nil {
		t.Fatal(err)
	}
	msg, err = nc.Request(killSubject, body, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var response proto.KillResp
	if err := json.Unmarshal(msg.Data, &response); err != nil || !response.OK {
		t.Fatalf("ordinary exec kill response: %+v %v", response, err)
	}
	select {
	case result := <-done:
		if result.code != -1 || result.err != nil {
			t.Fatalf("signal result: %+v", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ordinary exec did not exit after control kill")
	}
	rows = localProcessSnapshot(a)
	if len(rows) != 1 || rows[0].PID != id || rows[0].State != "exited" || rows[0].RC == nil || *rows[0].RC != -1 {
		t.Fatalf("real exit not retained by courier: %+v", rows)
	}
	if rows := a.workloads.Snapshot(); len(rows) != 0 {
		t.Fatalf("manager still owns retired task: %+v", rows)
	}
}

// origin: docs/reviews/hot-update-plan.md §4.2. Transfer to the exit courier
// deliberately overlaps the PTY table; the union must expose one terminal row.
func TestProcessSnapshotRetirementOverlapKeepsRealExit(t *testing.T) {
	a := newTestAgent(t, fakeMountinfo([2]string{"/", "ext4"}), nil)
	a.registerProc("overlap", &procRec{startedAt: time.Now().UTC()})
	a.courier.enqueueExit("overlap", 23)
	for i := 0; i < 2; i++ {
		rows := localProcessSnapshot(a)
		if len(rows) != 1 || rows[0].PID != "overlap" || rows[0].State != "exited" || rows[0].RC == nil || *rows[0].RC != 23 {
			t.Fatalf("retirement phase %d: %+v", i, rows)
		}
		a.unregisterProc("overlap")
	}
}

// origin: docs/reviews/hot-update-plan.md §4.2. Listing ordinary exec in G.1
// must also connect its DropProcesses action to the real child owner.
func TestReconciliationDropsOrdinaryExecWithBoundedEscalation(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if runIsolatedAgentTest(t, "TestReconciliationDropsOrdinaryExecWithBoundedEscalation") {
		return
	}
	for _, cancelEscalation := range []bool{false, true} {
		name := "escalates"
		if cancelEscalation {
			name = "cancellation-stops-escalation"
		}
		t.Run(name, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := newTestAgent(t, fakeMountinfo([2]string{"/", "ext4"}), nil)
			ready := filepath.Join(t.TempDir(), "ready")
			const id = "orphan-exec"
			type result struct {
				code int
				err  error
			}
			done := make(chan result, 1)
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				rc, err := a.runChild(nil, "", id, &proto.ExecReq{Cwd: t.TempDir(), Argv: []string{
					"/bin/sh", "-c", `trap '' TERM; printf ready > "$1"; exec sleep 30`, "probe", ready,
				}})
				done <- result{rc, err}
			}()
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(func() {
				cancel()
				a.reapManagedChildren()
				select {
				case <-finished:
				case <-time.After(3 * time.Second):
					t.Error("orphan fixture did not finish cleanup")
				}
			})
			if !testharness.WaitFor(t, 3*time.Second, 10*time.Millisecond, func() bool {
				_, err := os.Stat(ready)
				return err == nil && len(a.workloads.Snapshot()) == 1
			}) {
				t.Fatal("TERM-ignoring child never became ready")
			}
			a.applyReconciliation(ctx, proto.NodeRegisterResp{DropProcesses: []string{id}})
			if cancelEscalation {
				cancel()
				// Observe beyond the actual five-second product grace. This is
				// a forbidden-action window, not a guessed readiness delay.
				select {
				case got := <-done:
					t.Fatalf("canceled reconciliation still killed child: %+v", got)
				case <-time.After(5500 * time.Millisecond):
				}
				if err := a.workloads.Signal(id, 0); err != nil {
					t.Fatalf("original child did not survive cancellation: %v", err)
				}
				if err := a.workloads.Signal(id, syscall.SIGKILL); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case got := <-done:
				if got.code != -1 || got.err != nil {
					t.Fatalf("orphan cleanup result: %+v", got)
				}
			case <-time.After(8 * time.Second):
				t.Fatal("DropProcesses never escalated to the ordinary exec owner")
			}
		})
	}
}
