package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/serveconf"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// cluster_join.go (C4) — the ergonomic ONE-prepare-ONE-approve join. `prepare` runs on the JOINING
// machine (not yet a cluster member, no leader call): it self-mints a nonce, signs the PoP over the
// unchanged bytes, and emits a base64 bundle. `approve` runs on the leader: it hands the bundle to the
// operation controller, which drives the recoverable join to SERVING.

func newClusterJoinCmd(socketPath *string) *cobra.Command {
	parent := &cobra.Command{
		Use:   "join",
		Short: "Recoverable cluster join: `prepare` (on the joiner) then `approve` (on the leader)",
		Long: "Two steps, ONE each: `cluster join prepare` on the NEW broker emits a self-signed bundle;\n" +
			"`cluster join approve <bundle>` on the leader admits + drives the join to SERVING (resumable).",
		Example: "  joiner$ tether cluster join prepare --raft-addr 10.0.0.2:7400 --nats-route nats://10.0.0.2:6222 --tunnel-addr brk-b:7000 --public-host brk-b\n" +
			"  leader$ tether cluster join approve tether-join:v1:… --wait",
	}
	parent.AddCommand(newClusterJoinPrepareCmd(), newClusterJoinApproveCmd(socketPath))
	return parent
}

func newClusterJoinPrepareCmd() *cobra.Command {
	var seedPath, nodeID, name, natsServerID, raftAddr, natsRoute, tunnelAddr, publicHost, certFP, secretsDir string
	cmd := &cobra.Command{
		Use:   "prepare",
		Short: "On the JOINING broker: mint a self-signed join bundle (no leader call needed)",
		Example: "  tether cluster join prepare --node-id brk-b --raft-addr 10.0.0.2:7400 \\\n" +
			"      --nats-route nats://10.0.0.2:6222 --tunnel-addr brk-b:7000 --public-host brk-b",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			rawSeed, err := os.ReadFile(seedPath)
			if err != nil {
				return fmt.Errorf("read node-ident seed %s: %w", seedPath, err)
			}
			// C8 (D12): tolerate a trailing newline on a hand-edited node-ident.nk (the deleted
			// sign-join/node-pub trimmed; join prepare must too — keygen writes none, but operators edit).
			seed := []byte(strings.TrimSpace(string(rawSeed)))
			pub, err := auth.PublicKeyFromSeed(seed)
			if err != nil {
				return fmt.Errorf("derive node-ident pub: %w", err)
			}
			if nodeID == "" {
				nodeID = natsServerID // fall back to the nats server_name if not given
			}
			if err := proto.ValidateClusterNodeID(nodeID); err != nil {
				return usageErr("cluster join prepare: --node-id %q invalid: %v", nodeID, err)
			}
			if raftAddr == "" {
				return usageErr("cluster join prepare: --raft-addr is required (host:7400)")
			}
			// C8 (D10): require the expose-home/NATS-peer identity that the deleted `cluster add`
			// enforced — else the admitted voter can never serve as an expose home or join the mesh.
			// cert_fp + bus_nkey are derived fail-closed below (audit A / v0.4.4 review F1), not D6-backfilled.
			if tunnelAddr == "" || natsRoute == "" {
				return usageErr("cluster join prepare: --tunnel-addr (host:7000) and --nats-route (nats://host:6222) are required (a voter must be able to serve exposes + join the NATS mesh)")
			}
			// Self-mint a nonce (a consistency property, not a secret — §18.2.4): 16 random bytes.
			nb := make([]byte, 16)
			if _, err := rand.Read(nb); err != nil {
				return fmt.Errorf("mint nonce: %w", err)
			}
			nonce := hex.EncodeToString(nb)
			sig, err := auth.SignWithSeed(seed, cluster.JoinSignBytes(nodeID, pub, nonce))
			if err != nil {
				return fmt.Errorf("sign join PoP: %w", err)
			}
			// audit A (+ v0.4.4 review F1): derive the bus nkey (from broker.nk) + cert_fp (from the tunnel
			// cert) at PREPARE so the leader bakes cluster_nodes.bus_nkey_pub + cert_fp AT ADMISSION. This
			// breaks the learner bus_nkey self-backfill deadlock (the write needs a mesh that needs the value)
			// and the empty-cert_fp wireClusterEarly crash-loop, and keeps the new voter home-eligible +
			// topology-renderable. FAIL-CLOSED: an empty bus_nkey/cert_fp bundle re-introduces the EXACT
			// deadlock + crash this fix removes (cert_fp='' trips wireClusterEarly BEFORE nats.Connect →
			// permanent crash-loop on the grown leader), so a derivation failure must ABORT, never WARN-and-emit.
			if natsServerID == "" {
				natsServerID = nodeID // §6.5 SSOT: server_name == node_id
			}
			raw, e := os.ReadFile(filepath.Join(secretsDir, "broker.nk"))
			if e != nil {
				return fmt.Errorf("cluster join prepare: cannot read %s/broker.nk: %w — pass --secrets-dir pointing at this broker's secrets (the leader needs the bus nkey at admission)", secretsDir, e)
			}
			busNkey, e := auth.PublicKeyFromSeed([]byte(strings.TrimSpace(string(raw))))
			if e != nil || busNkey == "" {
				return fmt.Errorf("cluster join prepare: cannot derive bus nkey from %s/broker.nk: %v (an empty bus_nkey re-arms the learner self-backfill deadlock)", secretsDir, e)
			}
			if certFP == "" { // --cert-fp is the explicit override escape hatch (stays non-empty)
				fp, ferr := clusteroffline.TunnelCertFingerprint(secretsDir)
				if ferr != nil || fp == "" {
					return fmt.Errorf("cluster join prepare: cannot derive cert_fp from %s tunnel cert: %v — pass --secrets-dir or --cert-fp (an empty cert_fp crash-loops the joiner: wireClusterEarly fails before nats.Connect)", secretsDir, ferr)
				}
				certFP = fp
			}
			bundle, err := cluster.EncodeJoinBundle(cluster.JoinBundle{
				NodeID: nodeID, Name: name, NodeIdentPub: pub, NatsServerID: natsServerID,
				RaftAddr: raftAddr, NatsRoute: natsRoute, TunnelAddr: tunnelAddr, PublicHost: publicHost,
				CertFP: certFP, BusNkey: busNkey, JoinNonce: nonce, JoinSigHex: hex.EncodeToString(sig),
				// B4: declare what this joiner speaks, so the leader can refuse a proto-skewed
				// join in 50ms with a named reason instead of admitting it and letting the
				// catch-up poll time out 2-30 minutes later with "check the joining broker".
				// Advisory + unsigned (see JoinBundle's doc); the PoP remains the trust boundary.
				ProtoVer: proto.ProtoVersion, ReleaseVersion: proto.ReleaseVersion,
			})
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			_, _ = fmt.Fprintln(out, bundle)
			_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "\nrun on the LEADER: tether cluster join approve <the bundle above> --wait")
			return nil
		},
	}
	cmd.Flags().StringVar(&seedPath, "seed", defaultSeed, "node-identity seed file")
	cmd.Flags().StringVar(&nodeID, "node-id", "", "this broker's node_id (== nats server_name; defaults to --nats-server-id)")
	cmd.Flags().StringVar(&name, "name", "", "human name (defaults to node_id)")
	cmd.Flags().StringVar(&natsServerID, "nats-server-id", "", "this broker's deterministic nats server_name")
	cmd.Flags().StringVar(&raftAddr, "raft-addr", "", "this joiner's raft address (host:7400) — required")
	cmd.Flags().StringVar(&natsRoute, "nats-route", "", "this joiner's NATS route URL (nats://host:6222)")
	cmd.Flags().StringVar(&tunnelAddr, "tunnel-addr", "", "this joiner's public tunnel addr (host:7000)")
	cmd.Flags().StringVar(&publicHost, "public-host", "", "this joiner's public host")
	cmd.Flags().StringVar(&certFP, "cert-fp", "", "this joiner's tunnel-cert fingerprint pin (default: auto-derived from <secrets-dir>/tunnel-cert.pem)")
	cmd.Flags().StringVar(&secretsDir, "secrets-dir", defaultClusterSecretsDir, "§15 secrets dir — read broker.nk (bus nkey) + tunnel-cert.pem (cert_fp) to carry at admission (audit A)")
	return cmd
}

func newClusterJoinApproveCmd(socketPath *string) *cobra.Command {
	var wait bool
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:     "approve <bundle>",
		Short:   "On the LEADER: admit + drive a join bundle to SERVING (recoverable)",
		Args:    cobra.ExactArgs(1),
		Example: "  tether cluster join approve tether-join:v1:… --wait",
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpClusterJoinApprove, JoinBundle: args[0]})
			if err != nil {
				return err
			}
			if err := leaderRedirect(cmd, resp); err != nil { // m7: a follower names the leader host + the right exit class
				return err
			}
			if resp.Error != "" {
				return clusterAdminError("join approve", resp)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "join operation %s created (driving to SERVING).\n", resp.OpID)
			if wait {
				return waitForOp(cmd, *socketPath, resp.OpID, timeout)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  watch: tether cluster ops show %s\n", resp.OpID)
			return nil
		},
	}
	cmd.Flags().BoolVar(&wait, "wait", false, "block until the join reaches SERVING (or --timeout), naming the laggard state")
	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Minute, "with --wait: give up after this long")
	return cmd
}

// waitForOp polls `cluster ops` for the op_id until it is terminal (done = exit 0; failed/aborted =
// loud non-zero) or the timeout (exit 75 transient). Reuses the cluster status socket round-trip.
func waitForOp(cmd *cobra.Command, socketPath, opID string, timeout time.Duration) error {
	var lastState string
	err := pollUntilStep(cmd.Context(), 2*time.Second, time.Now().Add(timeout), func() (bool, error) {
		resp, err := callAdmin(socketPath, adminsock.Request{Op: adminsock.OpClusterOps, OpsNode: opID})
		if err != nil {
			return false, err
		}
		var op *adminsock.ClusterOpEntry
		for i := range resp.Ops {
			if resp.Ops[i].OpID == opID {
				op = &resp.Ops[i]
			}
		}
		if op == nil {
			return false, fmt.Errorf("operation %s not found", opID)
		}
		lastState = op.OpState
		if op.Terminal {
			if op.State == "done" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "operation %s reached %s.\n", opID, op.OpState)
				return true, nil
			}
			return false, &ExitError{Class: exitInternal, Err: fmt.Errorf("operation %s ended %s: %s", opID, op.OpState, op.LastError)}
		}
		// C4-M7: a BLOCKED op needs operator action — do NOT hang to the timeout; surface it promptly.
		if op.State == "stalled" {
			return false, &ExitError{Class: exitTransient, Err: fmt.Errorf("operation %s is BLOCKED (%s): %s — `cluster ops confirm %s` to proceed, or `cluster ops abort %s`", opID, op.OpState, op.LastError, opID, opID)}
		}
		return false, nil
	})
	if errors.Is(err, errPollDeadline) {
		return &ExitError{Class: exitTransient, Err: fmt.Errorf("operation %s still in flight (%s) after %s — `cluster ops show %s`", opID, lastState, timeout, opID)}
	}
	return err
}

// Local joiner readiness and provisioning support the resumable grow driver.
// origin: hot-update S6 plan §9.3 / T53
// joinerBrokerUpLocal reports whether the joiner's OWN broker is up AND running in CLUSTER mode, via its local
// admin socket (we run ON the joiner). It must NOT use NATS: an unmeshed joiner's health responder is
// unreachable from the leader's nats until the cutover forms the mesh.
//
// External review B1: a broker that booted in SINGLE mode (its broker.yaml cluster seam never took effect —
// root-owned config, wrong seam placement) ALSO answers this socket, but with {OK:false, Code:cluster_not_enabled,
// Cluster:nil}. Accepting any reply misjudged that single-mode broker as "cluster-up + joined": the driver would
// skip re-rendering the clustered conf and wait for a SERVING that can never happen. Require a genuine clustered
// status report — OK AND a non-nil Cluster — so a single-mode boot fails this gate and the driver HALTs at the
// start-joiner boundary with the resume hint (its cluster seam must be fixed first).
func joinerBrokerUpLocal(socketPath string) bool {
	resp, err := callAdmin(socketPath, adminsock.Request{Op: adminsock.OpClusterStatus})
	return err == nil && adminStatusIsClustered(resp)
}

// joinerBootGrace bounds how long the start-joiner boundary waits for a joiner whose daemons provisioning has
// ALREADY started. It is not a guess at "how slow is slow": it covers the two REAL transients a just-restarted
// joiner goes through — the broker serving its admin socket at the END of Run (after the cluster backend wires),
// and, during a grow specifically, a short crash-restart cycle while its clustered JetStream still has no quorum
// (the former-N1's nats is meshing) and the broker fail-stops on the lone-clustered-JS guard until systemd's
// Restart=always brings it back.
//
// "Both resolve in seconds; a minute is generous" was the sentence here until simcluster-speed A, and it was
// wrong by construction: the joiner's broker WAITS for its clustered JetStream for up to
// broker.ClusteredJetStreamBootWait (90 s) before it serves the admin socket this probe reads, so a 60 s grace
// could expire on a joiner that was still inside its own legitimate boot. Nobody saw it because the fresh-grow
// invocation used to sit out the same 60 s BEFORE printing the resume hint, and that dead time gave the
// former-N1's JetStream a head start; remove it (A) and the first deploy-tier run HALTed the resume at
// CATCHING_UP/reachable:false on a loaded host. The grace is therefore derived from the boot wait plus one
// systemd restart cycle of margin, never a number of its own — and it is the RESUME grace only; the fresh-grow
// invocation gets zero (joinerStartGrace).
var joinerBootGrace = broker.ClusteredJetStreamBootWait() + joinerBootGraceMargin

// joinerBootGraceMargin covers the joiner's process start + one Restart=always cycle (RestartSec=2) around the
// boot wait, so a joiner that fail-stopped once at the very end of its wait is still caught on the way back.
const joinerBootGraceMargin = 30 * time.Second

// joinerStartGrace is how long the start-joiner boundary waits for the joiner's broker before HALTing
// with the resume hint. It is the WHOLE joinerBootGrace on a resume and ZERO on the invocation that ran
// the local init — the only two cases, decided by one fact this process established itself:
//
//   - initRan == true: THIS process created raft/ moments ago (P2). Cluster mode is switched on by the
//     presence of on-disk raft state (distributed-broker-architecture.md §8.3 (e); the #I1 fail-closed
//     invariant drill 11 pins: a cluster-mode serve with no raft state refuses), so before P2 no broker
//     on this host could have been running clustered, and adminStatusIsClustered would have answered
//     false for any that was running single. Nothing has asked provisioning to start anything since —
//     the resume hint is printed AFTER this decision. Waiting here waits for an event that cannot
//     happen; measured on the deploy tier it was a flat 60 s per fresh grow (2026-09-18: invocation 1
//     took 97 s / 80 s, 60 s of which was this window). An operator running `cluster add` by hand sat
//     through the same minute before being told to start the daemons.
//   - initRan == false: raft/ already existed when we started — EITHER a RESUME after provisioning
//     started the daemons (the boot / crash-restart transient joinerBootGrace exists for, below), OR the
//     FIRST invocation for a RETURNING node (drill 42: rejoin-prepare + `init --from-manifest` left raft/
//     behind, nothing has been asked to start yet). The second fact tells them apart: the joiner's
//     LOCAL nats-server. Provisioning's half of the boundary is `systemctl restart nats-server` — loading
//     the clustered conf this invocation just rendered — and until that has happened the broker cannot
//     mesh whatever it does (a broker restarting against the standalone conf fail-stops on cluster nkeys
//     the conf lacks, forever). So: nats running CLUSTERED → provisioning has done its half → the full
//     window; nats standalone or not listening → nothing can come up → zero, HALT at once with the hint.
//     gotcha #83: the 120 s spent here on a returning node was the whole reason its join op went BLOCKED
//     (the leader's opCatchupTimeout runs from AddNonvoter, i.e. from before this wait).
//   - natsClustered unknown (no broker.yaml to read the nats URL from, or the INFO read failed): keep
//     the full window — the conservative direction, and the pre-#83 behaviour.
//
// NOT the "render branch" (!joinerBrokerUpLocal at P5) and NOT "no join op yet": both are re-evaluated
// on every invocation and are true mid crash-restart on a resume too, which would reintroduce the 3-in-4
// failure the grace was added to cure. A pure function so the rule is table-testable on its own.
//
// The third fact is whether a `tether serve` PROCESS exists on this host at all (joinerBrokerProcess):
// the admin socket this boundary polls is served only at the END of the broker's Run, after its
// clustered-JetStream boot wait, so "socket not answering" cannot tell a broker still booting from no
// broker whatsoever. A returning node whose unit was stopped for the rejoin (drill 42 stops it before
// `rejoin prepare`) has nothing that could ever answer — and its nats-server may well still run its OLD
// clustered conf, so the nats fact alone reads "clustered" there (measured, image #4). No process after
// several samples (bridging systemd's RestartSec gap) → zero grace.
func joinerStartGrace(initRan bool, nats natsLocalState, proc brokerProcState) time.Duration {
	if initRan {
		return 0
	}
	if nats == natsLocalDown || nats == natsLocalStandalone {
		return 0
	}
	if proc == brokerProcAbsent {
		return 0
	}
	return joinerBootGrace // nats clustered/unknown AND a broker process present/unknown
}

// brokerProcState is what joinerBrokerProcess learned about a `tether serve` process on this host.
type brokerProcState int

const (
	brokerProcUnknown brokerProcState = iota // /proc not readable (not Linux, or no permission)
	brokerProcAbsent                         // no `tether serve` process in any sample
	brokerProcPresent                        // a `tether serve` process was seen in at least one sample
)

func (s brokerProcState) String() string {
	switch s {
	case brokerProcAbsent:
		return "absent"
	case brokerProcPresent:
		return "present"
	case brokerProcUnknown:
		return "unknown"
	}
	return "unknown"
}

// joinerBrokerProcess scans /proc for a `tether serve` process other than this one, `samples` times
// `gap` apart, and reports present as soon as one sample sees it. The repeated samples exist for systemd's
// Restart=always: between a fail-stop and the revival (RestartSec=2 in install.sh's unit) there is a short
// window with no process, and a single probe landing in it would misread a crash-looping broker as
// absent. An unreadable /proc (non-Linux, or an unreadable procfs) is Unknown, never Absent.
func joinerBrokerProcess(samples int, gap time.Duration) brokerProcState {
	return joinerBrokerProcessIn("/proc", os.Getpid(), samples, gap, time.Sleep)
}

func joinerBrokerProcessIn(procRoot string, selfPID, samples int, gap time.Duration, sleep func(time.Duration)) brokerProcState {
	if samples < 1 {
		samples = 1
	}
	sawAnyProc := false
	for i := 0; i < samples; i++ {
		if i > 0 {
			sleep(gap)
		}
		entries, err := os.ReadDir(procRoot)
		if err != nil {
			return brokerProcUnknown
		}
		for _, e := range entries {
			pid, perr := strconv.Atoi(e.Name())
			if perr != nil || pid == selfPID {
				continue
			}
			raw, rerr := os.ReadFile(filepath.Join(procRoot, e.Name(), "cmdline"))
			if rerr != nil {
				continue
			}
			sawAnyProc = true
			if isTetherServeArgv(bytes.Split(raw, []byte{0})) {
				return brokerProcPresent
			}
		}
	}
	if !sawAnyProc {
		return brokerProcUnknown // a procfs with no readable cmdlines is not evidence of anything
	}
	return brokerProcAbsent
}

// isTetherServeArgv recognises the broker daemon's argv: argv[0] basename `tether` (any install path)
// and `serve` as the first non-flag argument, i.e. install.sh's `ExecStart=… tether serve --config …`.
func isTetherServeArgv(argv [][]byte) bool {
	if len(argv) < 2 || filepath.Base(string(argv[0])) != "tether" {
		return false
	}
	for _, a := range argv[1:] {
		s := string(a)
		if s == "" || strings.HasPrefix(s, "-") {
			continue
		}
		return s == "serve"
	}
	return false
}

// natsLocalState is what joinerNatsState learned about the joiner's own nats-server.
type natsLocalState int

const (
	natsLocalUnknown    natsLocalState = iota // could not ask (no config path / no URL / read error)
	natsLocalDown                             // nothing listening on the configured client address
	natsLocalStandalone                       // listening, INFO carries no cluster name (standalone conf)
	natsLocalClustered                        // listening, INFO carries a cluster name (clustered conf loaded)
)

func (s natsLocalState) String() string {
	switch s {
	case natsLocalDown:
		return "down"
	case natsLocalStandalone:
		return "standalone"
	case natsLocalClustered:
		return "clustered"
	case natsLocalUnknown:
		return "unknown"
	}
	return "unknown"
}

// joinerNatsState reads the joiner's nats-server INFO line — the server sends it before any CONNECT, so
// no credentials are needed — and reports whether it is up and whether it runs a CLUSTERED configuration
// (nats-server sets INFO.cluster from the `cluster {}` block; a standalone conf has none). natsURL comes
// from the joiner's broker.yaml (broker.nats.url, the address its own broker dials); an empty URL or an
// unreadable answer is Unknown, never a guess.
func joinerNatsState(natsURL string, timeout time.Duration) natsLocalState {
	addr := natsInfoDialAddr(natsURL)
	if addr == "" {
		return natsLocalUnknown
	}
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return natsLocalDown
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	line, err := bufio.NewReaderSize(conn, 64*1024).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "INFO ") {
		return natsLocalUnknown
	}
	var info struct {
		Cluster string `json:"cluster"`
	}
	if json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "INFO "))), &info) != nil {
		return natsLocalUnknown
	}
	if info.Cluster != "" {
		return natsLocalClustered
	}
	return natsLocalStandalone
}

// natsInfoDialAddr turns a broker.nats.url (nats://host:port, host:port, or a comma list — the first
// entry wins) into a dial address; "" when there is nothing usable.
func natsInfoDialAddr(natsURL string) string {
	first := strings.TrimSpace(strings.Split(natsURL, ",")[0])
	if first == "" {
		return ""
	}
	if !strings.Contains(first, "://") {
		first = "nats://" + first
	}
	u, err := url.Parse(first)
	if err != nil || u.Hostname() == "" {
		return ""
	}
	port := u.Port()
	if port == "" {
		port = "4222"
	}
	return net.JoinHostPort(u.Hostname(), port)
}

// joinerNatsURL is the joiner's own broker.nats.url from its broker.yaml, or "" when there is no config
// to read (then the boundary keeps its full grace — Unknown).
func joinerNatsURL(configPath string) string {
	if configPath == "" {
		return ""
	}
	c, err := serveconf.Load(configPath)
	if err != nil {
		return ""
	}
	return c.Broker.NATS.URL
}

// awaitJoinerBrokerUpLocal polls joinerBrokerUpLocal for a bounded window instead of probing ONCE.
//
// A one-shot probe is wrong at exactly this boundary. Provisioning has just restarted the joiner's nats +
// broker, so "not up in cluster mode" right now is far more often "still coming up" than "misconfigured" —
// and the one-shot HALT told the operator to start daemons that were already running, then aborted a grow
// whose joiner became healthy seconds later. Observed on the deploy tier: drill 42's returning-node re-grow
// failed this way in 3 of 4 runs (invocation 2, rc=75) while brk2 was mid boot/crash-restart, even though the
// grow itself was correct — the former-N1 had already been cut over and was sitting clustered-alone waiting.
// Polling turns that transient back into what it is; a joiner that is genuinely single-mode or truly not
// started still fails the whole window and gets the same actionable HALT.
//
// grace is the window (joinerStartGrace). grace <= 0 keeps the single probe and returns at once: the
// caller has established that nothing can come up, so the honest answer is the immediate HALT.
func awaitJoinerBrokerUpLocal(ctx context.Context, socketPath, joiner string, grace time.Duration, out interface{ Write([]byte) (int, error) }) bool {
	if joinerBrokerUpLocal(socketPath) {
		return true
	}
	if grace <= 0 {
		return false
	}
	// The operator sees the boundary BEFORE the wait, not only after it: the old order printed "waiting
	// up to 60s" and then, a minute later, the PAUSED block that named the command to run. The word
	// "PAUSED" is deliberately absent here — provisioning (simcluster:311) recognises the HALT by the
	// `PAUSED at start-joiner|RESUME` signature over the whole stdout, and an early match would blind it.
	_, _ = fmt.Fprintf(out, "→ start-joiner boundary: if %s's daemons are not running yet, on %s: systemctl restart nats-server && systemctl start tether-broker (this command waits up to %s, then pauses with a resume hint)\n", joiner, joiner, grace)
	_, _ = fmt.Fprintf(out, "  … waiting up to %s for %s's broker to serve cluster status (provisioning just restarted it)\n", grace, joiner)
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Second):
		}
		if joinerBrokerUpLocal(socketPath) {
			return true
		}
	}
	return false
}

// adminStatusIsClustered reports whether an OpClusterStatus reply proves the broker is up AND running in CLUSTER
// mode. A SINGLE-mode broker answers the socket too, but with OK=false / Cluster=nil / Code=cluster_not_enabled;
// accepting any reply misjudged it as clustered (external review B1).
func adminStatusIsClustered(resp *adminsock.Response) bool {
	return resp != nil && resp.OK && resp.Cluster != nil
}

// renderJoinerClusteredConf renders the joiner's OWN clustered nats.conf on disk BEFORE its broker boots — a
// fresh joiner has no replicated cluster_nodes yet and cannot boot cluster-mode with a standalone conf. It
// fetches the route-mesh peer triples from the leader (mesh-peers), then shells out to `cluster reconcile nats
// --manual` (the same render an operator would run) with self + peers. Idempotent: re-rendering writes the
// same bytes. It runs as the tether user (nats.d/ is tether-owned per G1), so no root is needed.
func renderJoinerClusteredConf(cmd *cobra.Command, nc *nats.Conn, actor string, accountSeed []byte, leader string, jp joinerParams, out interface{ Write([]byte) (int, error) }) error {
	resp, err := sendGrowTrigger(cmd.Context(), nc, actor, accountSeed, &proto.ClusterGrowReq{Op: "mesh-peers", TargetNode: leader, JoinerNode: jp.Joiner})
	if err != nil {
		return fmt.Errorf("fetch mesh peers: %w", err)
	}
	if resp == nil || !resp.OK {
		code, msg := "", ""
		if resp != nil {
			code, msg = resp.Code, resp.Error
		}
		return fmt.Errorf("fetch mesh peers refused: %s %s", code, msg)
	}
	var selfBusNkey string
	var peers []string
	for _, t := range resp.MeshPeers {
		parts := strings.SplitN(t, ",", 3)
		if len(parts) != 3 {
			continue
		}
		if parts[0] == jp.Joiner {
			selfBusNkey = parts[2]
		} else {
			peers = append(peers, t)
		}
	}
	if selfBusNkey == "" {
		return fmt.Errorf("joiner %s not yet in the leader's mesh roster (bus nkey absent) — retry", jp.Joiner)
	}
	if len(peers) == 0 {
		return fmt.Errorf("no route-mesh peers returned by the leader (roster not converged) — retry")
	}
	acctPub, err := auth.PublicKeyFromSeed(accountSeed)
	if err != nil {
		return fmt.Errorf("derive account pub: %w", err)
	}
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve self binary: %w", err)
	}
	args := []string{"cluster", "reconcile", "nats", "--manual", "--secrets-dir", jp.SecretsDir,
		"--server-name", jp.Joiner, "--route-url", jp.NatsRoute,
		"--account-issuer", acctPub, "--broker-nkey", selfBusNkey, "--conf", defaultNatsConfPath}
	for _, p := range peers {
		args = append(args, "--peer", p)
	}
	c := exec.CommandContext(cmd.Context(), self, args...)
	var stderr bytes.Buffer
	c.Stderr = &stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("reconcile nats --manual: %v: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// resetJoinerJSStore moves aside the JOINER's own JetStream store before it boots clustered (R16 A1). A
// returning node's dead-epoch clustered JS meta would otherwise wedge the 1->2 meta and crash-loop the
// joiner (n1ClusteredJetStreamFatal). Discovery: preflight the joiner's freshly-rendered nats.conf for its
// JS store dir. A non-empty store is operator-gated (--reset-former-js / --preserve-js-data); an empty
// store (a fresh joiner) is a no-op. The move is backup-dir-first idempotent (keyed on the op id).
func resetJoinerJSStore(out interface{ Write([]byte) (int, error) }, jp joinerParams, opID string) error {
	own, err := natsconf.Preflight(defaultNatsConfPath)
	if err != nil {
		// minor-1: fail CLOSED. The render step just rewrote the joiner's clustered conf, so a preflight
		// failure here is abnormal — surfacing it (rather than silently disarming the drill-42 fix) matches
		// the m4 fail-closed posture in MoveAsideJSStore one layer down.
		return fmt.Errorf("preflight the joiner's nats.conf to resolve its JS store: %w", err)
	}
	storeDir := own.JSStoreDir()
	// M3: only a DATA-BEARING store (a returning node's stale streams/clustered meta) needs resetting. A
	// truly-fresh joiner's store is absent, empty, or only the structural skeleton a booted JS nats lays
	// down — treat it as a no-op so a fresh grow (drills 10/11) is byte-equivalent (no backup, no sentinel,
	// no spurious "moved aside" line, no false residue evidence) and never a false data-loss HALT.
	hasData, hasErr := natsconf.JSStoreHasData(storeDir)
	if hasErr != nil {
		// F4: an unreadable store is DATA-BEARING by contract — proceeding as if it were empty is what
		// re-opens the grow wedge. Surface it instead of silently skipping the reset.
		return fmt.Errorf("cannot determine whether the joiner's JetStream store holds data: %w", hasErr)
	}
	if storeDir == "" || !hasData {
		return nil
	}
	// The op id is the STABLE per-grow epoch (set at P4, always non-empty by P5). Fail LOUD rather than mint
	// a fixed collision-prone "grow-bak.join" that a second grow would treat as a same-epoch no-op.
	if opID == "" {
		return fmt.Errorf("reset joiner JS store: no op id (grow epoch) in hand — refusing a fixed-name backup that a later grow could collide with")
	}
	backup := storeDir + ".grow-bak." + opID
	sentinel := filepath.Join(jp.DataDir, ".grow-joiner-reset-"+opID+".done")
	moved, err := natsconf.MoveAsideJSStore(storeDir, backup, sentinel, jp.ResetFormerJS || jp.PreserveJSData)
	if err != nil {
		// origin: line-2 closure verification m9 — see the sibling branch in cluster_offline.go.
		if !errors.Is(err, natsconf.ErrJSStoreNeedsAck) {
			return fmt.Errorf("the JOINER %s has a JetStream store that could not be inspected or moved, and it "+
				"must be reset before the node can boot clustered — %w\n"+
				"  --reset-former-js / --preserve-js-data will NOT help here: they only acknowledge a "+
				"data-bearing store, and this is a failure to read or write it. Fix the condition named above "+
				"on %s, then re-run", jp.Joiner, err, jp.Joiner)
		}
		// Widen the refusal to name the JOINER end of the grow + the exact re-run (M2 stable-refusal).
		return fmt.Errorf("the JOINER %s carries a pre-existing JetStream store that must be reset before it can "+
			"boot clustered — %w\n  re-run `tether cluster add %s … --reset-former-js` (or --preserve-js-data): "+
			"the flags move ANY pre-existing JS store on EITHER end of the grow aside (grow-bak, NEVER deleted)",
			jp.Joiner, err, jp.Joiner)
	}
	if moved != "" {
		_, _ = fmt.Fprintf(out, "→ reset joiner %s JetStream store (moved aside to %s; a returning node's stale clustered meta would wedge the grow)\n", jp.Joiner, moved)
	}
	return nil
}

func joinerIsVoter(nc *nats.Conn, actor, joiner string) bool {
	for _, h := range probeClusterHealthAdvisory(nc, actor) {
		if h.NodeID == joiner && h.IsVoter {
			return true
		}
	}
	return false
}

func formerN1Note(votersBefore int) string {
	if votersBefore == 1 {
		return "former-N1 cutover (JS reset)"
	}
	return "existing voters SIGHUP-reload (no former-N1 cutover)"
}

func routeCertSANPreflight(secretsDir, natsRoute string) error {
	return cluster.RouteCertSANMatches(secretsDir, natsRoute)
}

func dirExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

// verifyClusterSeam confirms the joiner's broker.yaml carries a broker.cluster seam whose EVERY cluster-mode
// field MATCHES this `cluster add`'s parameters — not merely present/non-empty. External re-review
// R2-B1/R3-B1/R4-B1 walked this in: raft_addr must equal the roster's raft addr (R2), the cluster-mode-trigger
// fields must be present (R3 — serve keys cluster mode on a non-empty data_dir, cutover.go), and they must be
// the RIGHT values (R4 — a stale-but-full-looking data_dir would decode + pass a presence check yet boot the
// joiner against the wrong raft path). So the 2-arg presence check was the root cause; this takes the full
// expected tuple and fail-closes on ANY mismatch, printing actual vs want.
func verifyClusterSeam(configPath, wantRaftAddr, wantDataDir, wantSecretsDir, wantNatsConfPath string) error {
	c, err := serveconf.Load(configPath)
	if err != nil {
		// review Mi10: since #75 the most common Load failure is a strict-decode
		// error (a typo'd / mis-nested key), NOT a missing seam — pointing the
		// operator at "apply the seam" would send them the wrong way and loop.
		// Fix the named key first (the wrapped error carries key + line).
		return fmt.Errorf("broker.yaml %s does not parse (strict since #75) — fix the named key, then re-run: %w", configPath, err)
	}
	cs := c.Broker.Cluster
	if cs.RaftAddr == "" && cs.DataDir == "" && cs.SecretsDir == "" && cs.NatsConfPath == "" {
		return fmt.Errorf("broker.cluster seam is NOT set in %s — the joiner would boot in SINGLE mode. Apply it as root "+
			"(set broker.cluster.{data_dir,raft_addr,secrets_dir,nats_conf_path}, e.g. raft_addr: %s), then re-run", configPath, wantRaftAddr)
	}
	for _, m := range []struct{ name, got, want string }{
		{"raft_addr", cs.RaftAddr, wantRaftAddr},
		{"data_dir", cs.DataDir, wantDataDir},
		{"secrets_dir", cs.SecretsDir, wantSecretsDir},
		{"nats_conf_path", cs.NatsConfPath, wantNatsConfPath},
	} {
		if m.got != m.want {
			return fmt.Errorf("broker.cluster.%s in %s is %q but this `cluster add` needs %q — refusing a stale/incomplete/wrong cluster seam "+
				"(serve keys cluster mode + raft state on these; a mismatch boots the joiner SINGLE or against the wrong raft path); "+
				"fix broker.cluster to match this node, then re-run", m.name, configPath, m.got, m.want)
		}
	}
	return nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
