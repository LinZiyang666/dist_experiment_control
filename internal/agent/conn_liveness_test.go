package agent

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// origin: simcluster-speed plan §5.4 H — the agent's NATS liveness probe (client half).

// TestConnOptionsSetTheLivenessProbe pins the wiring: buildConnOptions must carry the exported
// constants into nats.Options. Deleting the two options leaves nats.go's 2 min × 2 in place and the
// silent-link detection window back at 4–6 minutes; this row is what notices.
//
// The options are applied over SENTINEL values, not over nats.GetDefaultOptions(): AgentMaxPingsOut (2)
// happens to equal nats.go's DefaultMaxPingOut, so against the defaults the MaxPingsOut assertion was an
// equivalent-mutant check — deleting `nats.MaxPingsOutstanding(AgentMaxPingsOut)` left it green
// (internal review round 1 R4-F8). A value no option would ever produce is what makes "the option is
// wired" distinguishable from "the default happens to agree".
func TestConnOptionsSetTheLivenessProbe(t *testing.T) {
	a := &Agent{}
	got := nats.GetDefaultOptions()
	got.PingInterval = -1
	got.MaxPingsOut = -1
	for _, opt := range a.buildConnOptions() {
		if err := opt(&got); err != nil {
			t.Fatal(err)
		}
	}
	if got.PingInterval != AgentPingInterval {
		t.Fatalf("PingInterval = %s, want AgentPingInterval %s", got.PingInterval, AgentPingInterval)
	}
	if got.MaxPingsOut != AgentMaxPingsOut {
		t.Fatalf("MaxPingsOut = %d, want AgentMaxPingsOut %d", got.MaxPingsOut, AgentMaxPingsOut)
	}
	if AgentPingInterval >= 2*time.Minute {
		t.Fatalf("AgentPingInterval %s is not shorter than nats.go's default; the constant exists to be", AgentPingInterval)
	}
}

// TestOldClientSurvivesServerPingSettings is the hermetic N-1 receipt for the SERVER half: a
// nats-server running install.sh's new ping_interval/ping_max must not drop a client that still
// runs nats.go's defaults (an un-upgraded agent pings every 2 min). The client answers the server's
// PINGs passively from its read loop, so 90 s of client silence — longer than the server's
// (ping_max+1)×interval = 60 s kill window (64 s with the first-tick jitter) — keeps the connection open. Mutation H-N1: set the
// server's MaxPingsOut to 0 (kill on the first unanswered PING) and the old client is still fine,
// because it never fails to answer; set the CLIENT to not answer (impossible in nats.go) is the
// only thing that would — which is the point: the old client is safe by construction.
func TestOldClientSurvivesServerPingSettings(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.PingInterval = 2 * time.Second // scaled: 20s → 2s, MaxPingsOut unchanged; the RATIO is the contract
	opts.MaxPingsOut = AgentMaxPingsOut
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	// The un-upgraded client: nats.go defaults — no PingInterval/MaxPingsOut set at all.
	disconnected := make(chan error, 1)
	nc, err := nats.Connect(ns.ClientURL(), nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
		select {
		case disconnected <- err:
		default:
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	// Stay silent for 4.5 server intervals — past two full server kill windows — and prove the
	// server still answers a request over the same connection afterwards.
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	select {
	case err := <-disconnected:
		t.Fatalf("an old client was disconnected by the new server ping settings: %v", err)
	case <-time.After(9 * time.Second):
	}
	if err := nc.FlushWithContext(ctx); err != nil {
		t.Fatalf("connection dead after silence: %v", err)
	}
	if nc.Status() != nats.CONNECTED {
		t.Fatalf("status = %s after 9s of silence, want CONNECTED", nc.Status())
	}
}

// TestServerDropsASilentRawClient is the positive control for the row above: the same server
// settings DO drop a client that never answers PING. A raw TCP client that completes CONNECT and then
// goes mute is closed by the server; without this row the "old client survives" test would pass against
// a server whose ping settings were silently ignored.
//
// What it pins, exactly (internal review round 1 R5-F4 — the first version said "(ping_max+1)×interval"
// in its comment and allowed (ping_max+3)×interval in its deadline, so a server running ping_max+1 passed):
//   - COUNT: nats-server's processPingTimer closes when `ping.out+1 > MaxPingsOut`, i.e. the mute client
//     sees EXACTLY MaxPingsOut PINGs and the next tick closes it instead of pinging. The count is
//     jitter-free, so it is asserted with `==` — a server on ping_max±1 turns this row red.
//   - TIME: the first tick is randomized by up to 20 % of the interval (setFirstPingTimer), so the close
//     lands within (MaxPingsOut+1)×interval + 0.2×interval of CONNECT; the read deadline is
//     (MaxPingsOut+2)×interval, which is the bound the operator docs quote (20 s × 2 → ≤ 80 s worst case,
//     ≈ 64 s in practice), not a "generous slack" that would also admit a wrong ping_max.
func TestServerDropsASilentRawClient(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.PingInterval = 2 * time.Second
	opts.MaxPingsOut = AgentMaxPingsOut
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	conn, err := net.Dial("tcp", ns.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	r := bufio.NewReader(conn)
	if _, err := r.ReadString('\n'); err != nil { // INFO
		t.Fatalf("read INFO: %v", err)
	}
	if _, err := conn.Write([]byte("CONNECT {\"verbose\":false,\"pedantic\":false,\"name\":\"mute\"}\r\n")); err != nil {
		t.Fatal(err)
	}
	// Never answer PING. The server must close us within (MaxPingsOut+1)×interval plus the ≤20 % first-tick
	// jitter; (MaxPingsOut+2)×interval is the documented worst case.
	deadline := time.Duration(AgentMaxPingsOut+2) * opts.PingInterval
	_ = conn.SetReadDeadline(time.Now().Add(deadline))
	pings := 0
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatalf("server never dropped a mute client within %s (saw %d PINGs) — the ping settings are not live", deadline, pings)
			}
			if pings == 0 {
				t.Fatalf("connection ended before any PING (%v) — not the liveness path", err)
			}
			// EOF/reset: the server closed a client that would not answer. Exactly MaxPingsOut PINGs
			// precede the close (ping.out+1 > MaxPingsOut on the following tick); one more or one fewer
			// is a server running a different ping_max than the one install.sh writes.
			if pings != AgentMaxPingsOut {
				t.Fatalf("server closed the mute client after %d PINGs, want exactly AgentMaxPingsOut = %d — the server's ping_max is not the contract's", pings, AgentMaxPingsOut)
			}
			return
		}
		if strings.HasPrefix(line, "PING") {
			pings++
		}
	}
}
