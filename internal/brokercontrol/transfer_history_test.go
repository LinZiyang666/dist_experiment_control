// Real N3 Raft/mTLS and R3 embedded JetStream exercise the production publisher.
// Fault adapters suppress a JS reply, reject a checkpoint, or hide committed Aux.
// This does not cover broker routing, origin-ledger durability, or same-PID exec.
package brokercontrol_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferaudit"
	"github.com/LinZiyang666/tether/test/clusterharness"
	"github.com/hashicorp/raft"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const transferHistorySID = "history-continuation"

var errTransferCheckpoint = errors.New("test: publisher checkpoint reply lost")

// Initialize the runtime's permanent network poller before measuring owners.
// Closing this listener leaves no fixture FD; subsequent assertions keep zero
// tolerance, including when a targeted test is the process's first network use.
func warmTransferHistoryNetwork(t *testing.T) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

type transferHistoryFixture struct {
	servers    []*natsserver.Server
	conns      []*nats.Conn
	transports []*raft.NetworkTransport
	nodes      []*cluster.Node
	probes     []clusterharness.LeaderProbe
	js         jetstream.JetStream
	rows       []transferHistoryRow
	closeOnce  sync.Once
}

type transferHistoryRow struct {
	cmd      *cluster.Command
	sequence uint64
}

func newTransferHistoryFixture(t *testing.T) *transferHistoryFixture {
	t.Helper()
	if jsstream.AuditDedupWindow != 2*time.Minute {
		t.Fatal("history tests must retain the frozen production 120s dedupe window")
	}
	f := &transferHistoryFixture{}
	t.Cleanup(f.close)
	var routes []string
	ports := make([]int, 3)
	for i := range ports {
		ports[i] = clusterharness.FreePort(t)
		routes = append(routes, fmt.Sprintf("nats://127.0.0.1:%d", ports[i]))
	}
	for i := range ports {
		s, err := natsserver.NewServer(&natsserver.Options{
			Host: "127.0.0.1", Port: -1, ServerName: fmt.Sprintf("transfer-history-%d", i),
			JetStream: true, StoreDir: t.TempDir(), JetStreamMaxStore: 8 << 30,
			Cluster: natsserver.ClusterOpts{Name: "transfer-history", Host: "127.0.0.1", Port: ports[i]},
			Routes:  natsserver.RoutesFromStr(strings.Join(routes, ",")), NoLog: true, NoSigs: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		f.servers = append(f.servers, s)
		go s.Start()
	}
	for _, s := range f.servers {
		if !s.ReadyForConnections(30 * time.Second) {
			t.Fatal("embedded JetStream server did not become ready")
		}
	}
	if !testharness.WaitFor(t, 30*time.Second, 20*time.Millisecond, func() bool {
		for _, s := range f.servers {
			if s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) == 3 {
				return true
			}
		}
		return false
	}) {
		t.Fatal("R3 JetStream meta group did not become ready")
	}
	for _, s := range f.servers {
		nc, err := nats.Connect(s.ClientURL())
		if err != nil {
			t.Fatal(err)
		}
		f.conns = append(f.conns, nc)
	}
	var err error
	f.js, err = jetstream.New(f.conns[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // ctx-root: bounded embedded history setup.
	defer cancel()
	if err := jsstream.EnsureHistoryStream(ctx, f.js, transferHistorySID, 3); err != nil {
		t.Fatal(err)
	}
	stream, err := f.js.Stream(ctx, jsstream.HistoryStreamName(transferHistorySID))
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.Duplicates != jsstream.AuditDedupWindow || info.Config.Replicas != 3 {
		t.Fatalf("fixture changed production history window or replica factor: %+v", info.Config)
	}
	ca := clusterharness.NewRouteCA(t, "transfer-history")
	for range 3 {
		tr, err := cluster.NewMTLSTransport(cluster.MTLSTransportConfig{
			BindAddr: "127.0.0.1:0", CACert: ca.Pool, Leaf: ca.Leaf(t, "transfer-history"),
		})
		if err != nil {
			t.Fatal(err)
		}
		f.transports = append(f.transports, tr)
	}
	var peers []raft.Server
	for i, tr := range f.transports {
		peers = append(peers, raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(fmt.Sprintf("history-%d", i)), Address: tr.LocalAddr()})
	}
	for i, tr := range f.transports {
		dir := t.TempDir()
		node, err := cluster.New(cluster.Config{
			LocalID: peers[i].ID, Transport: tr, DataDir: dir, DBPath: filepath.Join(dir, "state.db"), BootstrapPeers: peers,
			HeartbeatTimeout: cluster.MultinodeHeartbeatTimeout, ElectionTimeout: cluster.MultinodeElectionTimeout,
			LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout, ApplyTimeout: brokercontrol.PublishTimeout,
		})
		if err != nil {
			t.Fatal(err)
		}
		f.nodes = append(f.nodes, node)
		f.probes = append(f.probes, node)
	}
	// Leadership can be visible before its initial no-op prefix is committed.
	// Commit a real fixture command before the first drain, so a pending audit
	// cannot legitimately advance that unrelated prefix after the baseline.
	// origin: hot-update S7 plan §5.2 / T23 / T24
	f.withLeader(t, func(i int) error {
		return f.nodes[i].ApplyMetaSet("t:transfer_history_fixture_ready", "true")
	})
	return f
}

func (f *transferHistoryFixture) close() {
	f.closeOnce.Do(func() {
		for _, n := range f.nodes {
			_ = n.Shutdown()
		}
		for _, tr := range f.transports {
			_ = tr.Close() // also owns transports from partial constructor failures
		}
		for _, nc := range f.conns {
			nc.Close()
		}
		for _, s := range f.servers {
			s.Shutdown()
			s.WaitForShutdown()
		}
	})
}

func (f *transferHistoryFixture) withLeader(t *testing.T, fn func(int) error) int {
	t.Helper()
	leader := -1
	_, err := clusterharness.WithLeader(t, f.probes, 20*time.Second, func(i int) error {
		leader = i
		return fn(i)
	})
	if err != nil {
		t.Fatal(err)
	}
	return leader
}

func (f *transferHistoryFixture) moveLeader(t *testing.T) {
	t.Helper()
	old := f.withLeader(t, func(int) error { return nil })
	if err := f.nodes[old].TransferLeadership(); err != nil {
		t.Fatal(err)
	}
	if !testharness.WaitFor(t, 20*time.Second, 20*time.Millisecond, func() bool {
		for i, n := range f.nodes {
			if i != old && n.IsLeader() {
				return true
			}
		}
		return false
	}) {
		t.Fatal("a different real Raft leader was not elected")
	}
	if f.withLeader(t, func(int) error { return nil }) == old {
		t.Fatal("leadership move did not change the publisher owner")
	}
}

// Faults are injected after real acceptance. The adapter cannot fabricate a
// successful JS receipt or a committed checkpoint; physical history is read below.
type transferAckLossJS struct {
	jetstream.JetStream
	lost int
}

func (j *transferAckLossJS) Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ack, err := j.JetStream.Publish(ctx, subject, payload, opts...)
	return j.reply(ack, err)
}

func (j *transferAckLossJS) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ack, err := j.JetStream.PublishMsg(ctx, msg, opts...)
	return j.reply(ack, err)
}

func (j *transferAckLossJS) reply(ack *jetstream.PubAck, err error) (*jetstream.PubAck, error) {
	if err == nil && j.lost == 0 {
		j.lost++
		return nil, context.DeadlineExceeded
	}
	return ack, err
}

type transferCheckpointFailure struct {
	*cluster.Node
	checks int
}

func (r *transferCheckpointFailure) AdvanceAuditPublished(uint64) error {
	r.checks++
	return errTransferCheckpoint
}

// Aux unavailability is a committed-log read fault, independent of the origin's
// retained bytes. This tests compaction handling without lowering TrailingLogs.
type transferTruncatedReader struct {
	*cluster.Node
	ids map[string]bool
}

func (r transferTruncatedReader) CommittedCommandAt(idx uint64) (*cluster.Command, error) {
	cmd, err := r.Node.CommittedCommandAt(idx)
	if err == nil && r.ids[cmd.ReqID] {
		return nil, cluster.ErrLogTruncated
	}
	return cmd, err
}

func transferRecord(round int, kind string) schema.AuditTransfer {
	rec := schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: kind, Verb: "push", Tier: "b",
		Ts:      time.Date(2026, 10, 12, 12, 0, round, 0, time.UTC),
		Session: transferHistorySID, Node: "receiver-original", ActorFp: "actor-original",
		TransferID: fmt.Sprintf("transfer-%d", round), AgentInstance: "agent-original",
		EntryNonce: fmt.Sprintf("entry-%d", round), IntentDigest: "intent-original", ObjectVersion: "object-original",
	}
	if round%2 != 0 {
		rec.Verb = "pull"
	}
	if kind != "start" {
		rec.TerminalID = schema.TransferTerminalID(rec.Session, rec.TransferID, rec.EntryNonce)
		rec.Bytes = 1024
	}
	return rec
}

// Read every physical matching row; first-match and SDK dedupe counters cannot
// hide a second identical terminal or an opposite terminal later in the stream.
func assertTransferPhysicalHistory(t *testing.T, f *transferHistoryFixture, cmd *cluster.Command) uint64 {
	t.Helper()
	rec, ok, err := xferaudit.ReplayTransferAudit(cmd)
	if err != nil || !ok {
		t.Fatalf("original canonical record: %v", err)
	}
	canonical, err := xferaudit.CanonicalTransferAudit(rec)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), brokercontrol.PublishTimeout) // ctx-root: bounded raw history oracle.
	defer cancel()
	stream, err := f.js.Stream(ctx, jsstream.HistoryStreamName(rec.Session))
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	count, sequence := 0, uint64(0)
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && seq != 0; seq++ {
		msg, err := stream.GetMsg(ctx, seq)
		if err != nil {
			t.Fatal(err)
		}
		if msg.Subject != proto.SubjAuditTransfer(rec.Session) {
			continue
		}
		var physical schema.AuditTransfer
		if err := json.Unmarshal(msg.Data, &physical); err != nil {
			t.Fatal(err)
		}
		if physical.TransferID != rec.TransferID {
			continue
		}
		count++
		if !bytes.Equal(msg.Data, canonical.Bytes) {
			t.Fatalf("physical history changed original actor, Ts, kind or bytes: seq=%d", seq)
		}
		sequence = seq
	}
	if count != 1 {
		t.Fatalf("physical %s count=%d, want exactly one (ReqID=%s)", rec.Kind, count, cmd.ReqID)
	}
	return sequence
}

func exerciseTransferStartUnacknowledged(t *testing.T, f *transferHistoryFixture, lost *transferAckLossJS, round int) {
	t.Helper()
	cmd, err := xferaudit.PlanTransferAudit(transferRecord(round, "start"))
	if err != nil {
		t.Fatal(err)
	}
	f.withLeader(t, func(i int) error {
		if err := f.nodes[i].Apply(cmd); err != nil {
			return err
		}
		reader := &transferCheckpointFailure{Node: f.nodes[i]}
		p := brokercontrol.NewAuditPublisher(brokercontrol.AuditPublisherConfig{Node: reader, JS: lost})
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second) // ctx-root: injected publish/checkpoint loss.
		defer cancel()
		// The first JS ACK can be lost before the checkpoint is attempted.
		for attempt := 0; attempt < 2; attempt++ {
			if _, err := p.PublishOnce(ctx); err == nil {
				return errors.New("START returned success despite checkpoint refusal")
			}
		}
		if reader.checks == 0 {
			return errors.New("START never reached the real history ACK/checkpoint boundary")
		}
		return nil
	})
	f.rows = append(f.rows, transferHistoryRow{cmd: cmd, sequence: assertTransferPhysicalHistory(t, f, cmd)})
}

// origin: hot-update S7 plan §5.2 / T23
func TestTransferStartAuditSurvivesLeadershipMove(t *testing.T) {
	warmTransferHistoryNetwork(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	f := newTransferHistoryFixture(t)
	lost := &transferAckLossJS{JetStream: f.js}
	for round := 0; round < 5; round++ {
		exerciseTransferStartUnacknowledged(t, f, lost, round)
	}
	if lost.lost != 1 {
		t.Fatal("fixture did not suppress an accepted START's JS ACK")
	}
	f.moveLeader(t)
	ids := map[string]bool{}
	for _, row := range f.rows {
		ids[row.cmd.ReqID] = true
	}
	f.withLeader(t, func(i int) error {
		p := brokercontrol.NewAuditPublisher(brokercontrol.AuditPublisherConfig{
			Node: transferTruncatedReader{Node: f.nodes[i], ids: ids}, JS: f.js,
		})
		ctx, cancel := context.WithTimeout(context.Background(), brokercontrol.PublishTimeout) // ctx-root: truncated log sweep.
		defer cancel()
		if _, err := p.PublishOnce(ctx); err != nil {
			return err
		}
		if p.TruncationLossCount() != 5 {
			return fmt.Errorf("truncation fault did not hide all five original Aux records: %d", p.TruncationLossCount())
		}
		missing := *f.rows[0].cmd
		missing.Aux = nil
		receipt, err := brokercontrol.ConfirmTransferAuditHistory(p, ctx, &missing)
		if err == nil || receipt.Sequence != 0 {
			return errors.New("a truncated Aux/checkpoint was treated as exact START history proof")
		}
		return nil
	})
	for round := 0; round < 5; round++ {
		exerciseTransferRetainedReplay(t, f, round)
	}
	f.close()
	testharness.AssertNoGoroutineLeak(t, "transfer START leader replay", beforeG)
	testharness.AssertNoFDLeak(t, "transfer START leader replay", beforeFD, 0)
}

func exerciseTransferTerminalCheckpointLoss(t *testing.T, f *transferHistoryFixture, round int) {
	t.Helper()
	kind := "complete"
	if round%2 != 0 {
		kind = "failed"
	}
	cmd, err := xferaudit.PlanTransferAudit(transferRecord(round, kind))
	if err != nil {
		t.Fatal(err)
	}
	f.withLeader(t, func(i int) error {
		if err := f.nodes[i].Apply(cmd); err != nil {
			return err
		}
		reader := &transferCheckpointFailure{Node: f.nodes[i]}
		p := brokercontrol.NewAuditPublisher(brokercontrol.AuditPublisherConfig{Node: reader, JS: f.js})
		ctx, cancel := context.WithTimeout(context.Background(), brokercontrol.PublishTimeout) // ctx-root: rejected publisher checkpoint.
		defer cancel()
		if _, err := p.PublishOnce(ctx); !errors.Is(err, errTransferCheckpoint) || reader.checks != 1 {
			return fmt.Errorf("did not fail after a real history ACK: checks=%d err=%v", reader.checks, err)
		}
		cursor, err := f.nodes[i].AuditPublishedIndex()
		if err != nil || cursor != 0 {
			return fmt.Errorf("checkpoint failure advanced cursor: %d, %v", cursor, err)
		}
		return nil
	})
	f.rows = append(f.rows, transferHistoryRow{cmd: cmd, sequence: assertTransferPhysicalHistory(t, f, cmd)})
}

func exerciseTransferRetainedReplay(t *testing.T, f *transferHistoryFixture, round int) {
	t.Helper()
	row := f.rows[round]
	aux := append([]byte(nil), row.cmd.Aux...)
	f.withLeader(t, func(i int) error {
		if err := f.nodes[i].Apply(row.cmd); err != nil {
			return err
		}
		p := brokercontrol.NewAuditPublisher(brokercontrol.AuditPublisherConfig{Node: f.nodes[i], JS: f.js})
		ctx, cancel := context.WithTimeout(context.Background(), brokercontrol.PublishTimeout) // ctx-root: original canonical retry.
		defer cancel()
		if _, err := p.PublishOnce(ctx); err != nil {
			return err
		}
		receipt, err := brokercontrol.ConfirmTransferAuditHistory(p, ctx, row.cmd)
		if err != nil {
			return err
		}
		if receipt.Sequence != row.sequence || !bytes.Equal(aux, row.cmd.Aux) {
			return errors.New("retry changed original physical sequence or canonical Aux")
		}
		if f.nodes[i].DedupCount() == 0 {
			return errors.New("same ReqID retry did not exercise the real Raft dedup ledger")
		}
		return nil
	})
	if got := assertTransferPhysicalHistory(t, f, row.cmd); got != row.sequence {
		t.Fatal("leader replay replaced the original physical history row")
	}
}

// origin: hot-update S7 plan §5.2 / T24 / M12
func TestTransferAuditPublishAckLossAcrossLeaderReplay(t *testing.T) {
	warmTransferHistoryNetwork(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	f := newTransferHistoryFixture(t)
	for round := 0; round < 5; round++ {
		exerciseTransferTerminalCheckpointLoss(t, f, round)
	}
	// All five records cross the real default window together. Serial five-window
	// waits would exceed the 600s matrix budget without exercising more behavior.
	start := time.Now()
	stop, joined := make(chan struct{}), make(chan error, 1)
	controlWrites := 0
	go func() {
		ticker := time.NewTicker(time.Second / 10)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				joined <- nil
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), brokercontrol.PublishTimeout) // ctx-root: independent control-audit load.
				_, err := f.js.Publish(ctx, proto.SubjAuditCall(transferHistorySID), []byte(`{"kind":"call"}`))
				cancel()
				if err != nil {
					joined <- err
					return
				}
				controlWrites++
			}
		}
	}()
	// sleep-fixture: cross the unmodified 120s server dedupe window, not a readiness barrier.
	timer := time.NewTimer(jsstream.AuditDedupWindow + 5*time.Second)
	select {
	case <-timer.C:
	case err := <-joined:
		timer.Stop()
		t.Fatalf("control-audit load stopped during the history window: %v", err)
	}
	close(stop)
	if err := <-joined; err != nil {
		t.Fatal(err)
	}
	if time.Since(start) <= jsstream.AuditDedupWindow {
		t.Fatal("fixture did not cross the real production dedupe window")
	}
	if controlWrites == 0 {
		t.Fatal("independent control-audit load was empty")
	}
	f.moveLeader(t)
	for round := 0; round < 5; round++ {
		exerciseTransferRetainedReplay(t, f, round)
	}
	f.close()
	testharness.AssertNoGoroutineLeak(t, "transfer terminal leader replay", beforeG)
	testharness.AssertNoFDLeak(t, "transfer terminal leader replay", beforeFD, 0)
}

// origin: hot-update S7 plan §5.2 / T24
func TestTransferContinuationMissingHistoryNeverAcknowledges(t *testing.T) {
	warmTransferHistoryNetwork(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	f := newTransferHistoryFixture(t)
	f.withLeader(t, func(i int) error {
		p := brokercontrol.NewAuditPublisher(brokercontrol.AuditPublisherConfig{Node: f.nodes[i], JS: f.js})
		ctx, cancel := context.WithTimeout(context.Background(), brokercontrol.PublishTimeout) // ctx-root: drain initial non-transfer entries.
		defer cancel()
		_, err := p.PublishOnce(ctx)
		return err
	})
	ctx, cancel := context.WithTimeout(context.Background(), brokercontrol.PublishTimeout) // ctx-root: remove fixture history before pending publication.
	defer cancel()
	if err := f.js.DeleteStream(ctx, jsstream.HistoryStreamName(transferHistorySID)); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 5; round++ {
		exerciseTransferMissingHistory(t, f, round)
	}
	f.close()
	testharness.AssertNoGoroutineLeak(t, "transfer missing history", beforeG)
	testharness.AssertNoFDLeak(t, "transfer missing history", beforeFD, 0)
}

func exerciseTransferMissingHistory(t *testing.T, f *transferHistoryFixture, round int) {
	t.Helper()
	cmd, err := xferaudit.PlanTransferAudit(transferRecord(round, "failed"))
	if err != nil {
		t.Fatal(err)
	}
	f.withLeader(t, func(i int) error {
		before, err := f.nodes[i].AuditPublishedIndex()
		if err != nil {
			return err
		}
		if err := f.nodes[i].Apply(cmd); err != nil {
			return err
		}
		p := brokercontrol.NewAuditPublisher(brokercontrol.AuditPublisherConfig{
			Node: f.nodes[i], JS: f.js, SessionExists: func(string) (bool, error) { return false, nil },
		})
		ctx, cancel := context.WithTimeout(context.Background(), brokercontrol.PublishTimeout) // ctx-root: missing history must stay pending.
		defer cancel()
		if _, err := p.PublishOnce(ctx); err == nil || p.DeletedStreamLossCount() != 0 {
			return errors.New("v1 history absence was acknowledged as deleted-session bounded loss")
		}
		cursor, err := f.nodes[i].AuditPublishedIndex()
		if err != nil || cursor != before {
			return fmt.Errorf("missing history advanced publisher cursor: %d, %v", cursor, err)
		}
		return nil
	})
}

// origin: hot-update S7 plan §11.3 / T23 / T24
func TestTransferLifecycleNoLeaks(t *testing.T) {
	warmTransferHistoryNetwork(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer publisher lifecycle", beforeG)
	testharness.AssertNoFDLeak(t, "transfer publisher lifecycle", beforeFD, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	f := newTransferHistoryFixture(t)
	cmd, err := xferaudit.PlanTransferAudit(transferRecord(0, "complete"))
	if err != nil {
		t.Fatal(err)
	}
	f.withLeader(t, func(i int) error {
		if err := f.nodes[i].Apply(cmd); err != nil {
			return err
		}
		beat, done := make(chan struct{}, 1), make(chan struct{})
		p := brokercontrol.NewAuditPublisher(brokercontrol.AuditPublisherConfig{
			Node: f.nodes[i], JS: f.js,
			ListSIDs: func(context.Context) ([]string, error) { return []string{transferHistorySID}, nil },
			Beat: func() {
				select {
				case beat <- struct{}{}:
				default:
				}
			},
		})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second) // ctx-root: joined publisher Run/cancel exercise.
		defer cancel()
		go func() {
			defer close(done)
			p.Run(ctx)
		}()
		select {
		case <-beat:
		case <-ctx.Done():
			return errors.New("publisher did not complete a real publish/reconcile tick")
		}
		cancel()
		joined := time.NewTimer(brokercontrol.PublishTimeout)
		defer joined.Stop()
		select {
		case <-done:
		case <-joined.C:
			return errors.New("publisher did not join after cancellation")
		}
		receipt, err := brokercontrol.ConfirmTransferAuditHistory(p, ctx, cmd)
		if !errors.Is(err, context.Canceled) || receipt.Sequence != 0 {
			return errors.New("canceled history attempt returned success evidence")
		}
		return nil
	})
	assertTransferPhysicalHistory(t, f, cmd)
	f.close()
}
