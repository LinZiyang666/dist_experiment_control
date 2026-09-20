package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: simcluster-speed plan §5.3 A (lever A: joinerBootGrace only on a resume).
//
// The rule is a pure function so the table below IS the contract: the invocation that ran the local
// init gets no grace (nothing can be up), every other invocation gets the whole window. A mutation that
// keys the grace on anything else (the render branch, the join-op id) cannot satisfy both rows.
func TestJoinerStartGraceSkipsOnlyWhenInitRan(t *testing.T) {
	cases := []struct {
		name    string
		initRan bool
		nats    natsLocalState
		proc    brokerProcState
		want    time.Duration
	}{
		{name: "fresh joiner: this process created raft/ → no wait", initRan: true, nats: natsLocalUnknown, proc: brokerProcUnknown, want: 0},
		{name: "fresh joiner, nats clustered + a broker process somehow → still no wait (init is decisive)", initRan: true, nats: natsLocalClustered, proc: brokerProcPresent, want: 0},
		{name: "resume: nats clustered, broker process booting → full joinerBootGrace", initRan: false, nats: natsLocalClustered, proc: brokerProcPresent, want: joinerBootGrace},
		{name: "resume: both facts unknown → full joinerBootGrace (conservative)", initRan: false, nats: natsLocalUnknown, proc: brokerProcUnknown, want: joinerBootGrace},
		{name: "resume: nats clustered, /proc unreadable → full joinerBootGrace (conservative)", initRan: false, nats: natsLocalClustered, proc: brokerProcUnknown, want: joinerBootGrace},
		// gotcha #83: a returning node's FIRST invocation — nothing that could ever answer the boundary
		// exists yet; waiting here only spent the join op's catch-up deadline.
		{name: "returning node: nats still standalone → no wait (#83)", initRan: false, nats: natsLocalStandalone, proc: brokerProcPresent, want: 0},
		{name: "returning node: nats not listening → no wait (#83)", initRan: false, nats: natsLocalDown, proc: brokerProcUnknown, want: 0},
		{name: "returning node: nats still on its OLD clustered conf but the broker unit is stopped → no wait (#83 ⑤, image #4)", initRan: false, nats: natsLocalClustered, proc: brokerProcAbsent, want: 0},
		{name: "returning node: nats unknown, no broker process → no wait", initRan: false, nats: natsLocalUnknown, proc: brokerProcAbsent, want: 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := joinerStartGrace(c.initRan, c.nats, c.proc); got != c.want {
				t.Fatalf("joinerStartGrace(%v, %s, %s) = %s, want %s", c.initRan, c.nats, c.proc, got, c.want)
			}
		})
	}
	if joinerBootGrace <= 0 {
		t.Fatalf("joinerBootGrace must stay positive for the resume row to mean anything: %s", joinerBootGrace)
	}
	// The resume grace must COVER the joiner's own clustered-JetStream boot wait (the joiner serves the
	// admin socket this grace polls only after that wait), plus margin. A grace below the boot wait
	// HALTs a joiner that is still legitimately booting — the first deploy-tier run of lever A did
	// exactly that at 60 s vs 90 s. Pinned as a relationship (T7), not as a number.
	if joinerBootGrace <= broker.ClusteredJetStreamBootWait() {
		t.Fatalf("joinerBootGrace %s must exceed broker.ClusteredJetStreamBootWait %s", joinerBootGrace, broker.ClusteredJetStreamBootWait())
	}
}

// clusteredStatus / singleStatus are the two admin-socket answers awaitJoinerBrokerUpLocal can see: a
// broker running in CLUSTER mode (OK + Cluster set) and one running SINGLE mode (external review B1:
// answers the socket, but OK=false / Cluster=nil), which must never count as "up".
func clusteredStatus() *adminsock.Response {
	return &adminsock.Response{Op: adminsock.OpClusterStatus, OK: true, Cluster: &adminsock.ClusterStatusReport{}}
}

func singleStatus() *adminsock.Response {
	return &adminsock.Response{Op: adminsock.OpClusterStatus, OK: false, Code: "cluster_not_enabled"}
}

// origin: simcluster-speed plan §5.3 A / mutation A-2.
//
// grace == 0 must probe EXACTLY once and return within the probe's own cost — the fresh-joiner path is
// only faster than before if the loop is truly skipped. The resume rows prove the window still works:
// a joiner that answers clustered on its third sample is accepted, and a cancelled ctx ends the wait
// honestly (false) instead of banking a not-yet-up joiner.
func TestAwaitJoinerBrokerUpLocalZeroGraceProbesOnce(t *testing.T) {
	t.Run("grace=0: one probe, immediate false", func(t *testing.T) {
		probes := 0
		stubCallAdmin(t, func(_ string, req adminsock.Request) (*adminsock.Response, error) {
			if req.Op != adminsock.OpClusterStatus {
				t.Fatalf("unexpected op %q", req.Op)
			}
			probes++
			return nil, errors.New("connect: no such file or directory")
		})
		var out bytes.Buffer
		start := time.Now()
		got := awaitJoinerBrokerUpLocal(context.Background(), "/nonexistent.sock", "brk2", 0, &out)
		if got {
			t.Fatal("a joiner that never answers must not be reported up")
		}
		if probes != 1 {
			t.Fatalf("grace=0 probed %d times, want exactly 1", probes)
		}
		if took := time.Since(start); took > time.Second {
			t.Fatalf("grace=0 took %s — the wait loop ran", took)
		}
		if strings.Contains(out.String(), "waiting up to") {
			t.Fatalf("grace=0 printed a wait line it never honoured:\n%s", out.String())
		}
	})

	t.Run("grace=0: an already-clustered joiner is still accepted", func(t *testing.T) {
		stubCallAdmin(t, func(string, adminsock.Request) (*adminsock.Response, error) { return clusteredStatus(), nil })
		var out bytes.Buffer
		if !awaitJoinerBrokerUpLocal(context.Background(), "/x.sock", "brk2", 0, &out) {
			t.Fatal("a clustered joiner must be accepted regardless of grace")
		}
	})

	t.Run("resume grace: third sample clustered → true", func(t *testing.T) {
		probes := 0
		stubCallAdmin(t, func(string, adminsock.Request) (*adminsock.Response, error) {
			probes++
			if probes < 3 {
				return singleStatus(), nil // single-mode answer must NOT count (external review B1)
			}
			return clusteredStatus(), nil
		})
		var out bytes.Buffer
		// The poll sleeps 2 s between samples; 3 samples need > 4 s of grace.
		if !awaitJoinerBrokerUpLocal(context.Background(), "/x.sock", "brk2", 10*time.Second, &out) {
			t.Fatalf("joiner answered clustered on sample 3 but was reported down; probes=%d\n%s", probes, out.String())
		}
		if probes != 3 {
			t.Fatalf("probes = %d, want 3 (returned on the first clustered answer)", probes)
		}
		if !strings.Contains(out.String(), "waiting up to 10s") {
			t.Fatalf("resume path must announce its wait:\n%s", out.String())
		}
	})

	t.Run("resume grace: cancelled ctx → false, no banked joiner", func(t *testing.T) {
		stubCallAdmin(t, func(string, adminsock.Request) (*adminsock.Response, error) { return singleStatus(), nil })
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var out bytes.Buffer
		if awaitJoinerBrokerUpLocal(ctx, "/x.sock", "brk2", joinerBootGrace, &out) {
			t.Fatal("a cancelled wait must report the joiner as not up")
		}
	})
}

// origin: simcluster-speed plan §5.3 A′ (the boundary is announced before the wait, not after it).
//
// Two things about the extra line are load-bearing: it names `systemctl restart nats-server` (a bare
// `start` is a no-op on a nats that is already running the STANDALONE conf, and the clustered conf then
// never loads — simcluster's grow comment records that exact failure), and it appears BEFORE the
// "waiting up to" line so an operator watching the terminal knows what the wait is for. It must NOT say
// "PAUSED at start-joiner": provisioning recognises the HALT by that signature over the whole stdout.
func TestStartJoinerHintSaysRestartNotStart(t *testing.T) {
	hint := startJoinerHint("brk2")
	if !strings.Contains(hint, "systemctl restart nats-server && systemctl start tether-broker") {
		t.Fatalf("HALT hint lost the restart-not-start instruction:\n%s", hint)
	}
	if !strings.Contains(hint, "PAUSED at start-joiner") {
		t.Fatalf("HALT hint must keep the PAUSED signature provisioning keys on:\n%s", hint)
	}
	// origin: simcluster-speed G1 (drill 91 at 8 concurrent grows): the OTHER pause. A joiner whose daemons
	// are running but whose broker is still forming its clustered JS meta group must not be told to
	// restart nats-server — that restarts the boot being waited for. Same PAUSED signature, no restart line.
	booting := startJoinerBootingHint("brk2")
	if !strings.Contains(booting, "PAUSED at start-joiner") {
		t.Fatalf("booting hint must keep the PAUSED signature provisioning keys on:\n%s", booting)
	}
	if strings.Contains(booting, "systemctl restart") || !strings.Contains(booting, "do not restart") {
		t.Fatalf("booting hint must not ask for a daemon restart:\n%s", booting)
	}
	// round-2 review R2-F7: the two pauses share their first line by design, so each carries a distinct
	// machine-readable PAUSE-KIND line on its second line — the only greppable discriminator between
	// "start the daemons" and "do NOT touch the daemons".
	if l := strings.Split(hint, "\n"); len(l) < 2 || strings.TrimSpace(l[1]) != pauseKindStartDaemons {
		t.Fatalf("start-daemons pause must carry %q on its second line:\n%s", pauseKindStartDaemons, hint)
	}
	if l := strings.Split(booting, "\n"); len(l) < 2 || strings.TrimSpace(l[1]) != pauseKindBooting {
		t.Fatalf("booting pause must carry %q on its second line:\n%s", pauseKindBooting, booting)
	}
	if strings.Contains(hint, pauseKindBooting) || strings.Contains(booting, pauseKindStartDaemons) {
		t.Fatal("the two pauses must not carry each other's PAUSE-KIND")
	}
	for _, c := range []struct {
		name  string
		init  bool
		grace time.Duration
		nats  natsLocalState
		proc  brokerProcState
		want  bool
	}{
		{"daemons running, waited the full grace", false, joinerBootGrace, natsLocalClustered, brokerProcPresent, true},
		{"init ran this invocation (fresh joiner)", true, 0, natsLocalUnknown, brokerProcUnknown, false},
		// round-2 review R4-2-F12: the row that makes the !initRan term load-bearing — every other
		// term true, init ran here ⇒ never the booting pause (a fresh joiner's daemons are ours to start).
		{"init ran here, daemons look alive: still the start pause", true, joinerBootGrace, natsLocalClustered, brokerProcPresent, false},
		{"grace 0: nothing could come up", false, 0, natsLocalClustered, brokerProcAbsent, false},
		{"process present but nats standalone", false, joinerBootGrace, natsLocalStandalone, brokerProcPresent, false},
		{"nats clustered but process unknown (procfs unreadable)", false, joinerBootGrace, natsLocalClustered, brokerProcUnknown, false},
	} {
		if got := joinerIsBooting(c.init, c.grace, c.nats, c.proc); got != c.want {
			t.Fatalf("%s: joinerIsBooting = %v, want %v", c.name, got, c.want)
		}
	}

	stubCallAdmin(t, func(string, adminsock.Request) (*adminsock.Response, error) { return singleStatus(), nil })
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // print the boundary lines, then leave immediately
	var out bytes.Buffer
	_ = awaitJoinerBrokerUpLocal(ctx, "/x.sock", "brk2", joinerBootGrace, &out)
	s := out.String()
	bi := strings.Index(s, "start-joiner boundary")
	wi := strings.Index(s, "waiting up to")
	if bi < 0 || wi < 0 {
		t.Fatalf("expected both the boundary line and the wait line:\n%s", s)
	}
	if bi > wi {
		t.Fatalf("the boundary line must precede the wait line:\n%s", s)
	}
	if !strings.Contains(s, "systemctl restart nats-server && systemctl start tether-broker") {
		t.Fatalf("the boundary line must carry the restart-not-start command:\n%s", s)
	}
	if strings.Contains(s, "PAUSED") {
		t.Fatalf("the boundary line must not pre-empt the PAUSED signature:\n%s", s)
	}
}

// origin: simcluster-speed plan §5.3 #70① (a lost reply is not a confirmation).
//
// The ladder's three end states, each pinned by the reply sequence that produces it. The mutation this
// guards against is the old `return nil` after six transport errors: row "silence×7" turns green under
// it, and only under it.
func TestCutoverBrokerNeverAssumesSuccessAfterSilence(t *testing.T) {
	transport := errors.New("nats: timeout")
	refused := &proto.ClusterGrowResp{OK: false, Code: "former_store_not_empty", Error: "pass --reset-former-js"}
	done := &proto.ClusterGrowResp{AlreadyDone: true}
	ok := &proto.ClusterGrowResp{OK: true}

	type step struct {
		resp *proto.ClusterGrowResp
		err  error
	}
	cases := []struct {
		name      string
		replies   []step
		wantErr   string // substring; "" == nil
		wantSends int
	}{
		{name: "ok on first try", replies: []step{{ok, nil}}, wantSends: 1},
		{name: "six lost replies then AlreadyDone → success", replies: []step{{nil, transport}, {nil, transport}, {nil, transport}, {nil, transport}, {nil, transport}, {nil, transport}, {done, nil}}, wantSends: 7},
		{name: "silence×7 → NOT confirmed (never assumed)", replies: []step{{nil, transport}, {nil, transport}, {nil, transport}, {nil, transport}, {nil, transport}, {nil, transport}, {nil, transport}}, wantErr: "cutover NOT confirmed after 7 attempts", wantSends: 7},
		{name: "stable refusal survives the ladder → refusal", replies: []step{{refused, nil}, {refused, nil}, {refused, nil}, {refused, nil}, {refused, nil}, {refused, nil}}, wantErr: "refused the cutover: former_store_not_empty", wantSends: 6},
		{name: "refusal then the SIGKILL fired (silence) then done → success", replies: []step{{refused, nil}, {nil, transport}, {done, nil}}, wantSends: 3},
		{name: "silence×6 then the confirming probe is refused → refusal", replies: []step{{nil, transport}, {nil, transport}, {nil, transport}, {nil, transport}, {nil, transport}, {nil, transport}, {refused, nil}}, wantErr: "refused the cutover: former_store_not_empty", wantSends: 7},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sends := 0
			send := func(*proto.ClusterGrowReq) (*proto.ClusterGrowResp, error) {
				if sends >= len(c.replies) {
					t.Fatalf("send #%d beyond the scripted %d replies", sends+1, len(c.replies))
				}
				s := c.replies[sends]
				sends++
				return s.resp, s.err
			}
			var out bytes.Buffer
			err := cutoverBrokerWithPoll(context.Background(), send, "brk1", &proto.ClusterGrowReq{Op: "mesh-cutover"}, &out, 0, func() string { return "brk1 did NOT answer the cluster-health probe" })
			if c.wantErr == "cutover NOT confirmed after 7 attempts" && (err == nil || !strings.Contains(err.Error(), "did NOT answer the cluster-health probe")) {
				t.Fatalf("the NOT-confirmed HALT must carry the former-N1 state summary, got %v", err)
			}
			if c.wantErr == "" && err != nil {
				t.Fatalf("want success, got %v\n%s", err, out.String())
			}
			if c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)) {
				t.Fatalf("want error containing %q, got %v\n%s", c.wantErr, err, out.String())
			}
			if sends != c.wantSends {
				t.Fatalf("sends = %d, want %d", sends, c.wantSends)
			}
			if strings.Contains(c.name, "silence") && !strings.Contains(out.String(), "transport error, attempt ") {
				t.Fatalf("every lost reply must be printed, got:\n%s", out.String())
			}
		})
	}
}

// origin: simcluster-speed A/B, drill 42 baseline-vs-image#2 (plan §8.2/§8.5). A join that reached
// CATCHING_UP and then went BLOCKED on its catch-up deadline — because the boundary wait outlived
// opCatchupTimeout while the returning joiner's daemons were not yet started — has passed AddNonvoter.
// The barrier must accept it (else a resume waits two more minutes for a state the op never re-enters),
// but ONLY when the leader's timeline vouches for it; a BLOCKED op that never was CATCHING_UP, or an older
// leader that does not set NonvoterCommitted, keeps the old wait.
func TestCatchupBarrierAcceptsABlockedJoinOnlyPastAddNonvoter(t *testing.T) {
	cases := []struct {
		name    string
		resp    *proto.ClusterGrowResp
		wantMet bool
		wantErr bool
	}{
		{name: "CATCHING_UP", resp: &proto.ClusterGrowResp{OK: true, OpState: "CATCHING_UP"}, wantMet: true},
		{name: "SERVING", resp: &proto.ClusterGrowResp{OK: true, OpState: "SERVING", Terminal: true}, wantMet: true},
		{name: "BLOCKED after CATCHING_UP (timeline vouches)", resp: &proto.ClusterGrowResp{OK: true, OpState: "BLOCKED", NonvoterCommitted: true, LastError: "catch-up exceeded the deadline"}, wantMet: true},
		{name: "BLOCKED, never CATCHING_UP", resp: &proto.ClusterGrowResp{OK: true, OpState: "BLOCKED", LastError: "AddNonvoter failed 3 times"}, wantMet: false},
		{name: "BLOCKED from an older leader (field absent)", resp: &proto.ClusterGrowResp{OK: true, OpState: "BLOCKED"}, wantMet: false},
		{name: "RAFT_ADDING", resp: &proto.ClusterGrowResp{OK: true, OpState: "RAFT_ADDING"}, wantMet: false},
		{name: "terminal non-SERVING", resp: &proto.ClusterGrowResp{OK: true, OpState: "ABORTED", Terminal: true}, wantErr: true},
		{name: "not OK", resp: &proto.ClusterGrowResp{Code: "cluster_not_ready"}, wantMet: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			met, err := catchupBarrier(c.resp)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, c.wantErr)
			}
			if met != c.wantMet {
				t.Fatalf("met = %v, want %v", met, c.wantMet)
			}
		})
	}
}

// origin: simcluster-speed A/B, drill 42 (plan §8.2/§8.5). After the start-joiner boundary the joiner is
// up, so a join BLOCKED by the boundary itself is re-entered with exactly one confirm — the documented
// "fix and re-run" recovery. Nothing is sent for any other state; a refused confirm HALTs with the
// refusal; a lost confirm reply is deferred to the catch-up wait, never treated as landed.
func TestResumeBlockedJoinConfirmsExactlyOnceAndOnlyPastAddNonvoter(t *testing.T) {
	blockedPast := &proto.ClusterGrowResp{OK: true, OpState: "BLOCKED", NonvoterCommitted: true, LastError: broker.OpBlockedCatchupDeadlineMsg + " — check the joining broker, then `cluster ops confirm op-1` to retry"}
	blockedEarly := &proto.ClusterGrowResp{OK: true, OpState: "BLOCKED", LastError: "AddNonvoter failed"}
	// past AddNonvoter but blocked on exhausted AddVoter attempts: a RUNNING joiner's stall, the budget's business
	blockedPromote := &proto.ClusterGrowResp{OK: true, OpState: "BLOCKED", NonvoterCommitted: true, LastError: "AddVoter (promote) failed 3 times (raft: timeout) — fix the target, then `cluster ops confirm op-1` to retry"}
	catching := &proto.ClusterGrowResp{OK: true, OpState: "CATCHING_UP", NonvoterCommitted: true}
	landed := &proto.ClusterGrowResp{OK: true}
	refused := &proto.ClusterGrowResp{OK: false, Code: "op_not_blocked", Error: "operation is not awaiting a confirm"}
	type step struct {
		resp *proto.ClusterGrowResp
		err  error
	}
	cases := []struct {
		name     string
		replies  []step
		wantOps  []string
		wantErr  string
		wantLine string
	}{
		{name: "CATCHING_UP → nothing to resume", replies: []step{{catching, nil}}, wantOps: []string{"join-status"}},
		{name: "BLOCKED before AddNonvoter → left to the catch-up policy", replies: []step{{blockedEarly, nil}}, wantOps: []string{"join-status"}},
		{name: "BLOCKED on exhausted AddVoter (past AddNonvoter) → NOT the boundary's doing, no confirm (R2-F1/R4-F7)", replies: []step{{blockedPromote, nil}}, wantOps: []string{"join-status"}},
		{name: "status lost → nothing sent (the catch-up wait re-reads)", replies: []step{{nil, errors.New("nats: timeout")}}, wantOps: []string{"join-status"}},
		{name: "BLOCKED past AddNonvoter → one confirm, landed", replies: []step{{blockedPast, nil}, {landed, nil}}, wantOps: []string{"join-status", "confirm-op"}, wantLine: "re-entering catch-up (resume confirm"},
		{name: "confirm refused → HALT with the refusal", replies: []step{{blockedPast, nil}, {refused, nil}}, wantOps: []string{"join-status", "confirm-op"}, wantErr: "resume confirm was refused: op_not_blocked"},
		{name: "confirm reply lost → deferred, not an error", replies: []step{{blockedPast, nil}, {nil, errors.New("nats: no responders")}}, wantOps: []string{"join-status", "confirm-op"}, wantLine: "resume confirm did not land"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var ops []string
			send := func(r *proto.ClusterGrowReq) (*proto.ClusterGrowResp, error) {
				if len(ops) >= len(c.replies) {
					t.Fatalf("send #%d (%s) beyond the scripted %d replies", len(ops)+1, r.Op, len(c.replies))
				}
				s := c.replies[len(ops)]
				ops = append(ops, r.Op)
				return s.resp, s.err
			}
			var out bytes.Buffer
			err := resumeBlockedJoinWith(send, "brk1", "op-1", &out)
			if c.wantErr == "" && err != nil {
				t.Fatalf("want nil error, got %v\n%s", err, out.String())
			}
			if c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)) {
				t.Fatalf("want error containing %q, got %v", c.wantErr, err)
			}
			if strings.Join(ops, ",") != strings.Join(c.wantOps, ",") {
				t.Fatalf("ops = %v, want %v", ops, c.wantOps)
			}
			if c.wantLine != "" && !strings.Contains(out.String(), c.wantLine) {
				t.Fatalf("want output containing %q, got:\n%s", c.wantLine, out.String())
			}
		})
	}
}

// origin: gotcha #83 (simcluster-speed §8.5). The boundary's "has provisioning done its half" probe reads
// the nats-server INFO line — sent before any CONNECT — and classifies the joiner's local nats as
// clustered (INFO.cluster set), standalone (no cluster name), down (nothing listening) or unknown (no URL /
// garbage). A fake listener per row; no nats-server, no credentials.
func TestJoinerNatsStateReadsTheInfoLine(t *testing.T) {
	serve := func(t *testing.T, line string) string {
		t.Helper()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				_, _ = c.Write([]byte(line))
				_ = c.Close()
			}
		}()
		return "nats://" + ln.Addr().String()
	}
	clustered := serve(t, `INFO {"server_id":"X","server_name":"brk2","version":"2.14.6","cluster":"tether","port":4222}`+"\r\n")
	standalone := serve(t, `INFO {"server_id":"X","server_name":"brk2","version":"2.14.6","port":4222}`+"\r\n")
	garbage := serve(t, "HTTP/1.1 400 Bad Request\r\n")
	down, _ := net.Listen("tcp", "127.0.0.1:0")
	downURL := "nats://" + down.Addr().String()
	_ = down.Close()

	cases := []struct {
		name string
		url  string
		want natsLocalState
	}{
		{"clustered conf loaded", clustered, natsLocalClustered},
		{"standalone conf", standalone, natsLocalStandalone},
		{"nothing listening", downURL, natsLocalDown},
		{"not a nats server", garbage, natsLocalUnknown},
		{"no URL configured", "", natsLocalUnknown},
		{"bare host:port form", strings.TrimPrefix(clustered, "nats://"), natsLocalClustered},
		{"comma list: first entry wins", clustered + "," + standalone, natsLocalClustered},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := joinerNatsState(c.url, 2*time.Second); got != c.want {
				t.Fatalf("joinerNatsState(%q) = %s, want %s", c.url, got, c.want)
			}
		})
	}
}

// origin: gotcha #83 ⑤ (simcluster-speed §8.5). The "is any broker booting here" fact is read from
// /proc cmdlines — a synthetic procfs per row, no real processes. The repeated samples are what makes a
// crash-looping broker (absent in sample 1, present in sample 2) read as present; an unreadable procfs
// is Unknown, never Absent; this process's own pid is ignored; `tether serve` is matched by argv[0]
// basename + first non-flag argument, so `tether cluster add` (this command) and `tether-next serve`
// (a staged binary with another name) do not count.
func TestJoinerBrokerProcessSamplesProcfs(t *testing.T) {
	mk := func(t *testing.T, procs map[int][]string) string {
		t.Helper()
		root := t.TempDir()
		for pid, argv := range procs {
			d := root + "/" + strconv.Itoa(pid)
			if err := os.MkdirAll(d, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(d+"/cmdline", []byte(strings.Join(argv, "\x00")+"\x00"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return root
	}
	noSleep := func(time.Duration) {}
	t.Run("a serving broker is present", func(t *testing.T) {
		root := mk(t, map[int][]string{1: {"/sbin/init"}, 400: {"/usr/local/bin/tether", "serve", "--config", "/etc/tether/broker.yaml"}})
		if got := joinerBrokerProcessIn(root, 999, 3, 0, noSleep); got != brokerProcPresent {
			t.Fatalf("got %s, want present", got)
		}
	})
	t.Run("only this command and unrelated processes → absent", func(t *testing.T) {
		root := mk(t, map[int][]string{1: {"/sbin/init"}, 999: {"/usr/local/bin/tether", "cluster", "add", "brk2"}, 500: {"/usr/local/bin/nats-server", "-c", "/etc/tether/nats.d/nats.conf"}})
		if got := joinerBrokerProcessIn(root, 999, 3, 0, noSleep); got != brokerProcAbsent {
			t.Fatalf("got %s, want absent", got)
		}
	})
	t.Run("another tether verb and a differently named binary do not count", func(t *testing.T) {
		root := mk(t, map[int][]string{7: {"/usr/local/bin/tether", "--json", "cluster", "status"}, 8: {"/usr/local/bin/tether-next", "serve"}, 9: {"/usr/local/bin/tether", "serve"}})
		if got := joinerBrokerProcessIn(root, 9, 1, 0, noSleep); got != brokerProcAbsent {
			t.Fatalf("got %s, want absent (pid 9 is self)", got)
		}
	})
	t.Run("crash-looping: absent in sample 1, present in sample 2 → present", func(t *testing.T) {
		root := mk(t, map[int][]string{1: {"/sbin/init"}})
		n := 0
		sleep := func(time.Duration) {
			n++
			if n == 1 {
				_ = os.MkdirAll(root+"/401", 0o755)
				_ = os.WriteFile(root+"/401/cmdline", []byte("/usr/local/bin/tether\x00serve\x00"), 0o644)
			}
		}
		if got := joinerBrokerProcessIn(root, 999, 3, 0, sleep); got != brokerProcPresent {
			t.Fatalf("got %s, want present (revived between samples)", got)
		}
	})
	t.Run("unreadable procfs → unknown, not absent", func(t *testing.T) {
		if got := joinerBrokerProcessIn(t.TempDir()+"/nope", 999, 3, 0, noSleep); got != brokerProcUnknown {
			t.Fatalf("got %s, want unknown", got)
		}
		if got := joinerBrokerProcessIn(t.TempDir(), 999, 1, 0, noSleep); got != brokerProcUnknown {
			t.Fatalf("an empty procfs (no cmdlines at all) must be unknown, got %s", got)
		}
	})
}

// origin: internal review round 1 R2-F4. A caller cancelled during the ladder must get its own ctx
// error from the confirming probe too — never a "cutover NOT confirmed … every reply was lost" verdict
// about a probe that was cancelled rather than lost.
func TestCutoverBrokerCancelledCtxIsNotNotConfirmed(t *testing.T) {
	// Two cancellation points. The first row's cancel lands during the last silent ladder round, where
	// the select on ctx.Done vs time.After(0) usually — not always — returns before either ctx check
	// runs, so on its own it caught a deleted check only ~60 % of the time (round-2 review R4-2-F6). The
	// second row cancels INSIDE the confirming probe: the only exit from there is the post-probe check,
	// so that check is exercised on every run and deleting it is red 30/30.
	for _, c := range []struct {
		name     string
		cancelAt int
	}{
		{"Ctrl-C during the last silent round", cutoverAttempts},
		{"Ctrl-C during the confirming probe", cutoverAttempts + 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			sends := 0
			send := func(*proto.ClusterGrowReq) (*proto.ClusterGrowResp, error) {
				sends++
				if sends == c.cancelAt {
					cancel()
				}
				return nil, context.Canceled
			}
			var out bytes.Buffer
			err := cutoverBrokerWithPoll(ctx, send, "brk1", &proto.ClusterGrowReq{Op: "mesh-cutover"}, &out, 0, func() string { t.Fatal("the summary probe must not run for a cancelled caller"); return "" })
			if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "NOT confirmed") {
				t.Fatalf("want the caller's context.Canceled, got %v", err)
			}
			if sends > c.cancelAt {
				t.Fatalf("the ladder kept sending after the cancel: %d sends, cancelled at %d", sends, c.cancelAt)
			}
		})
	}
}
