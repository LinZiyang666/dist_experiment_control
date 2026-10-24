package broker

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontrol"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// These strict port witnesses test broker assembly, not leaf durability or
// receiver effects. Unused methods remain nil and panic if wiring reaches them.
// Real leaf/JS continuity belongs to the plan's T20/T21/T29/T36/T39 integration.
type wiringProbe struct {
	mu        sync.Mutex
	calls     []string
	deadlines []controlbudget.Deadline
	census    x.LedgerCensus
	status    x.Status
	fail      map[string]error
	bound     x.Bindings
	stop      func()
	scanned   bool
}

func (p *wiringProbe) call(name string, d controlbudget.Deadline) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, name)
	p.deadlines = append(p.deadlines, d)
	return p.fail[name]
}
func (p *wiringProbe) takeCalls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := append([]string(nil), p.calls...)
	p.calls = nil
	return result
}

type wiringTracker struct {
	x.TrackerManager
	p *wiringProbe
}

func (w wiringTracker) Bind(b x.Bindings, d controlbudget.Deadline) error {
	w.p.bound = b
	return w.p.call("bind", d)
}
func (w wiringTracker) Status(r x.StatusRequest, d controlbudget.Deadline) (x.Status, error) {
	w.p.mu.Lock()
	scanned := w.p.scanned
	w.p.mu.Unlock()
	// Status cannot certify restoration before a successful, complete census.
	// origin: S7 internal review rv-recovery F05
	if !scanned {
		return x.Status{}, errors.New("status before restored census scan")
	}
	if r.Identity != w.p.census.Rows[0].Identity || r.Origin != w.p.census.Rows[0].Origin {
		return x.Status{}, errors.New("wrong restored identity")
	}
	return w.p.status, w.p.call("status", d)
}
func (w wiringTracker) Stop(d controlbudget.Deadline) error { return w.p.call("stop-tracker", d) }

type wiringLedger struct {
	x.LedgerPort
	p *wiringProbe
}

func (w wiringLedger) Scan(d controlbudget.Deadline) (x.LedgerCensus, error) {
	err := w.p.call("scan", d)
	w.p.mu.Lock()
	w.p.scanned = err == nil && w.p.census.Ready && w.p.census.Complete
	w.p.mu.Unlock()
	return w.p.census, err
}

type wiringController struct {
	x.Controller
	p *wiringProbe
}

func (w wiringController) Stop(d controlbudget.Deadline) error {
	if w.p.stop != nil {
		w.p.stop()
	}
	return w.p.call("stop-controller", d)
}

type wiringFinalizer struct {
	x.FinalizerPort
	p *wiringProbe
}

func (w wiringFinalizer) Advance(id x.Identity, d controlbudget.Deadline) (x.Status, error) {
	if id != w.p.status.Identity {
		return x.Status{}, errors.New("wrong finalizer identity")
	}
	return w.p.status, w.p.call("advance", d)
}
func (w wiringFinalizer) Stop(d controlbudget.Deadline) error { return w.p.call("stop-finalizer", d) }

type wiringCollector struct {
	x.Collector
	p *wiringProbe
}

func (w wiringCollector) Pass(d controlbudget.Deadline) (x.LedgerCensus, error) {
	return w.p.census, w.p.call("collect", d)
}
func (w wiringCollector) Stop(d controlbudget.Deadline) error { return w.p.call("stop-collector", d) }

type wiringBucket struct {
	x.BucketPort
	p *wiringProbe
}

func (w wiringBucket) Stop(d controlbudget.Deadline) error { return w.p.call("stop-bucket", d) }

type wiringJS struct {
	jetstream.JetStream
	publishErr error
}

func (j wiringJS) PublishMsg(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return &jetstream.PubAck{Stream: "history-sid", Sequence: 1}, j.publishErr
}

type wiringAuthority struct{ x.Authority }
type wiringTransport struct{ x.Transport }

func newWiringGraph(t *testing.T) (*Broker, *BrokerTransfers, *wiringProbe) {
	t.Helper()
	identity := x.Identity{SID: "sid", NID: "nid", ActorFP: "actor", AgentInstance: "agent", TransferID: "tid", EntryNonce: "nonce", IntentDigest: "intent", ObjectVersion: "object"}
	origin := x.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 1}
	budget := x.BudgetSeal{StartedAt: time.Now().Add(-time.Hour), EffectiveBudgetNS: int64(30 * time.Second), Size: 23}
	row := x.LedgerRow{Identity: identity, Origin: origin, BudgetSeal: budget, Phase: x.PhaseCleanupPending}
	p := &wiringProbe{census: x.LedgerCensus{Ready: true, Complete: true, Rows: []x.LedgerRow{row}},
		status: x.Status{Identity: identity, Origin: origin, Budget: budget, State: x.StatusLive, Phase: x.PhaseCleanupPending}, fail: map[string]error{}}
	g := &BrokerTransfers{Origin: origin, Tracker: wiringTracker{p: p}, Ledger: wiringLedger{p: p}, Controller: wiringController{p: p},
		Finalizer: wiringFinalizer{p: p}, Collector: wiringCollector{p: p}, Buckets: wiringBucket{p: p}}
	g.Bindings = x.Bindings{JS: wiringJS{}, Ledger: g.Ledger, Authority: wiringAuthority{}, Transport: wiringTransport{}, OwnerGeneration: 1}
	for _, r := range xfercontrol.Registrations() {
		if r.LegacyCarrier || r.ClusterVerb != "" {
			continue
		}
		g.Subscriptions = append(g.Subscriptions, BrokerTransferSubscription{Subject: r.Subject, Handler: func(*nats.Msg) {}})
	}
	b := &Broker{cfg: Config{Now: time.Now, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, transfers: newTransferTracker()}
	return b, g, p
}
func wiringDeadline(t *testing.T) controlbudget.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	requireWiring(t, err)
	return d
}

// origin: hot-update S7 plan §5.3 / §8.2 / M11
// origin: S7 internal review rv-recovery F05
func TestTransferWiringRequiresRestoredCensus(t *testing.T) {
	t.Run("status-before-scan", func(t *testing.T) {
		_, g, p := newWiringGraph(t)
		d := wiringDeadline(t)
		requireWiring(t, g.Tracker.Bind(g.Bindings, d))
		req := x.StatusRequest{Identity: p.census.Rows[0].Identity, Origin: p.census.Rows[0].Origin}
		if _, err := g.Tracker.Status(req, d); err == nil {
			t.Fatal("status accepted before restored census scan")
		}
		if _, err := g.Ledger.Scan(d); err != nil {
			t.Fatal(err)
		}
		_, err := g.Tracker.Status(req, d)
		requireWiring(t, err)
	})
	for _, defect := range []string{"not-ready", "incomplete", "scan-error", "bind-error", "lost-tracker", "reset-start", "reset-budget", "wrong-identity", "wrong-origin", "missing-port", "missing-binding", "missing-registration", "duplicate-registration", "foreign-registration"} {
		t.Run(defect, func(t *testing.T) {
			b, g, p := newWiringGraph(t)
			switch defect {
			case "not-ready":
				p.census.Ready = false
			case "incomplete":
				p.census.Complete = false
			case "scan-error":
				p.fail["scan"] = errors.New("unreadable outbox")
			case "bind-error":
				p.fail["bind"] = errors.New("old backend is not retired")
			case "lost-tracker":
				p.status.State = x.StatusUnknown
			case "reset-start":
				p.status.Budget.StartedAt = time.Now()
			case "reset-budget":
				p.status.Budget.EffectiveBudgetNS++
			case "wrong-identity":
				p.status.Identity.EntryNonce = "replacement"
			case "wrong-origin":
				p.status.Origin.Generation++
			case "missing-port":
				g.Finalizer = nil
			case "missing-binding":
				g.Bindings.Authority = nil
			case "missing-registration":
				g.Subscriptions = g.Subscriptions[1:]
			case "duplicate-registration":
				g.Subscriptions[0] = g.Subscriptions[1]
			case "foreign-registration":
				g.Subscriptions[0].Subject = "unexpected.subject"
			}
			if err := InstallBrokerTransfers(b, g, wiringDeadline(t)); err == nil {
				t.Fatal("installed unproved transfer graph")
			}
			if b.transferRuntime.Load() != nil {
				t.Fatal("failed installation exposed an owner to GC")
			}
		})
	}
	b, g, p := newWiringGraph(t)
	requireWiring(t, InstallBrokerTransfers(b, g, wiringDeadline(t)))
	// Bind reconciles retained dirty writes; the ready census still precedes status.
	// origin: S7 internal review rv-recovery F05
	if got := p.takeCalls(); !reflect.DeepEqual(got, []string{"bind", "scan", "status"}) {
		t.Fatalf("installation order: %v", got)
	}
	if p.bound.Ledger != g.Ledger || p.bound.OwnerGeneration != g.Bindings.OwnerGeneration {
		t.Fatal("tracker retained obsolete backend")
	}
	if err := InstallBrokerTransfers(b, g, wiringDeadline(t)); err == nil {
		t.Fatal("installed a second current owner")
	}
}

// origin: hot-update S7 plan §5.3 / M13
func TestTransferWiringDrivesCleanupAndCollector(t *testing.T) {
	b, g, p := newWiringGraph(t)
	requireWiring(t, InstallBrokerTransfers(b, g, wiringDeadline(t)))
	p.takeCalls()
	// ctx-root: the wiring test owns this bounded exercise lifetime.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := b.finalizeStrandedXfers(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := b.reconcileXferObjects(ctx); err != nil {
		t.Fatal(err)
	}
	if got := p.takeCalls(); !reflect.DeepEqual(got, []string{"scan", "advance", "collect"}) {
		t.Fatalf("live cleanup or collector suppressed: %v", got)
	}
	p.fail["collect"] = errors.New("collector list deadline")
	if _, err := b.reconcileXferObjects(ctx); !errors.Is(err, p.fail["collect"]) {
		t.Fatalf("collector error lost: %v", err)
	}
	delete(p.fail, "collect")
	p.census.Complete = false
	if _, err := b.reconcileXferObjects(ctx); err == nil {
		t.Fatal("incomplete collector was accepted")
	}
	if _, err := b.finalizeStrandedXfers(ctx); err == nil {
		t.Fatal("incomplete ledger reached finalizer")
	}
	p.census.Complete = true
	p.fail["advance"] = errors.New("cleanup pending")
	if _, err := b.finalizeStrandedXfers(ctx); !errors.Is(err, p.fail["advance"]) {
		t.Fatalf("cleanup failure lost: %v", err)
	}
	b.transferRuntime.Store(nil)
	b.cfg.TransferFactory = func(*Broker, jetstream.JetStream, controlbudget.Deadline) (*BrokerTransfers, error) {
		t.Fatal("reaper invoked constructor")
		return nil, nil
	}
	if _, err := b.reconcileXferObjects(ctx); err == nil {
		t.Fatal("missing imported owner fell back to legacy GC")
	}
	if _, err := b.finalizeStrandedXfers(ctx); err == nil {
		t.Fatal("missing imported owner fell back to legacy synthesis")
	}
}

// origin: hot-update S7 plan §1.2 / §5.3 / LQ3
func TestTransferWiringKeepsOnePassDeadline(t *testing.T) {
	b, g, p := newWiringGraph(t)
	requireWiring(t, InstallBrokerTransfers(b, g, wiringDeadline(t)))
	p.calls = nil
	p.deadlines = nil
	p.census.Rows = append(p.census.Rows, p.census.Rows[0])
	// ctx-root: the wiring test owns this bounded exercise lifetime.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := b.finalizeStrandedXfers(ctx); err != nil {
		t.Fatal(err)
	}
	if len(p.deadlines) != 3 || p.deadlines[0] != p.deadlines[1] || p.deadlines[1] != p.deadlines[2] {
		t.Fatalf("per-entry deadline renewal: %v", p.deadlines)
	}
	remaining, err := controlbudget.Remaining(p.deadlines[0])
	if err != nil || remaining > 100*time.Millisecond {
		t.Fatalf("caller deadline extended: %s %v", remaining, err)
	}
	cancel()
	p.takeCalls()
	if _, err := b.reconcileXferObjects(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled pass: %v", err)
	}
	if got := p.takeCalls(); len(got) != 0 {
		t.Fatalf("canceled pass reached owner: %v", got)
	}
}

// origin: hot-update S7 plan §8.2 / P22
func TestTransferWiringStopsEveryPartialOwner(t *testing.T) {
	b, g, p := newWiringGraph(t)
	p.fail["stop-controller"] = errors.New("controller join failure")
	p.fail["stop-tracker"] = errors.New("tracker join failure")
	b.transferRuntime.Store(g)
	err := StopBrokerTransfers(b, wiringDeadline(t))
	if !errors.Is(err, p.fail["stop-controller"]) || !errors.Is(err, p.fail["stop-tracker"]) {
		t.Fatalf("partial close lost errors: %v", err)
	}
	if got := p.takeCalls(); !reflect.DeepEqual(got, []string{"stop-controller", "stop-collector", "stop-tracker", "stop-finalizer", "stop-bucket"}) {
		t.Fatalf("unjoined owner: %v", got)
	}
	if b.transferRuntime.Load() != g {
		t.Fatal("failed stop discarded retained owner")
	}
	b2, g2, p2 := newWiringGraph(t)
	boom := errors.New("constructor failed after partial owner")
	b2.cfg.TransferFactory = func(*Broker, jetstream.JetStream, controlbudget.Deadline) (*BrokerTransfers, error) { return g2, boom }
	// ctx-root: the wiring test owns this bounded exercise lifetime.
	if err := startBrokerTransfers(context.Background(), b2, nil); !errors.Is(err, boom) {
		t.Fatalf("partial factory: %v", err)
	}
	if got := p2.takeCalls(); len(got) != 5 {
		t.Fatalf("partial factory leaked owners: %v", got)
	}
}

// origin: hot-update S7 plan §6.3 / T39 / P22
func TestTransferWiringColdSubscriptionsAreBroadcast(t *testing.T) {
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferSubscriptions(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer broadcast subscriptions", beforeG)
	testharness.AssertNoFDLeak(t, "transfer broadcast subscriptions", beforeFD, 0)
}

func exerciseTransferSubscriptions(t *testing.T) {
	t.Helper()
	// A single broker installs local descriptors without a cluster responder.
	// origin: hot-update S7 plan §6.3 / P22
	requireWiring(t, startBrokerTransferForward(&Broker{}, nil, &BrokerTransfers{}))
	if err := startBrokerTransferForward(&Broker{clusterMode: true}, nil, &BrokerTransfers{}); err == nil {
		t.Fatal("cluster responder claimed readiness without a node")
	}
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	requireWiring(t, err)
	s.Start()
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded NATS not ready")
	}
	sent := make(chan string, 128)
	for i := 0; i < 2; i++ {
		nc, err := nats.Connect(s.ClientURL())
		requireWiring(t, err)
		defer nc.Close()
		b, g, _ := newWiringGraph(t)
		b.clusterMode = true
		for j := range g.Subscriptions {
			subject := g.Subscriptions[j].Subject
			g.Subscriptions[j].Handler = func(*nats.Msg) { sent <- subject }
		}
		b.cfg.TransferFactory = func(*Broker, jetstream.JetStream, controlbudget.Deadline) (*BrokerTransfers, error) { return g, nil }
		// ctx-root: the wiring test owns this bounded exercise lifetime.
		requireWiring(t, startBrokerTransfers(context.Background(), b, nc))
		// origin: hot-update S10 lint L3a / errcheck
		defer func() { requireWiring(t, stopBrokerTransfers(b)) }()
		copy := BrokerTransferSubscriptions(b)
		copy[0].Subject = "mutated"
		if BrokerTransferSubscriptions(b)[0].Subject == "mutated" {
			t.Fatal("hot table aliases a cold caller's slice")
		}
	}
	publisher, err := nats.Connect(s.ClientURL())
	requireWiring(t, err)
	defer publisher.Close()
	for _, r := range xfercontrol.Registrations() {
		if r.LegacyCarrier || r.ClusterVerb != "" {
			continue
		}
		subject := strings.ReplaceAll(r.Subject, "*", "identity")
		requireWiring(t, publisher.Publish(subject, nil))
		for i := 0; i < 2; i++ {
			select {
			case got := <-sent:
				if got != r.Subject {
					t.Fatalf("wrong registration: %s", got)
				}
			case <-time.After(time.Second):
				t.Fatalf("continuation was not broadcast: %s", subject)
			}
		}
	}
}

// origin: hot-update S7 plan §5.2 / T21 / P22
func TestTransferWiringKeepsFailedAuditResponsibility(t *testing.T) {
	b, _, _ := newWiringGraph(t)
	failure := errors.New("history publication was not acknowledged")
	setBrokerJS(b, wiringJS{publishErr: failure})
	record := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: "failed", Session: "sid", Node: "nid", TransferID: "tid", Ts: time.Now(), Verb: "push", Tier: "b"}
	committed := false
	b.emitTransferAuditWithCommit(record, func() { committed = true })
	if committed {
		t.Fatal("failed history publication retired responsibility")
	}
	setBrokerJS(b, wiringJS{})
	b.emitTransferAuditWithCommit(record, func() { committed = true })
	if !committed {
		t.Fatal("confirmed publication never advances legacy callback")
	}
}

// gate-control: TestTransferWiringSourceOwnershipControls
// origin: hot-update S7 plan §5.1 / §12.2 / L13 / P22
func TestTransferWiringSourceOwnership(t *testing.T) {
	for _, name := range []string{"transfer.go", "broker.go", "xfer_inflight.go", "xfer_provision.go"} {
		data, err := os.ReadFile(name)
		requireWiring(t, err)
		if err := transferWiringSource(string(data)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	data, err := os.ReadFile("broker.go")
	requireWiring(t, err)
	file, err := parser.ParseFile(token.NewFileSet(), "broker.go", data, parser.ParseComments)
	requireWiring(t, err)
	foundProbe := false
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "enableJetStream" {
			foundProbe = true
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					// The boot pass now has one adapter; moving it cannot make an
					// early cleanup call invisible to the installation-order guard.
					// origin: hot-update S7 plan §5.3 / P22
					name := ""
					switch fn := call.Fun.(type) {
					case *ast.SelectorExpr:
						name = fn.Sel.Name
					case *ast.Ident:
						name = fn.Name
					}
					if name == "reconcileXferObjects" || name == "finalizeStrandedXfers" || name == "reconcileBrokerTransfersOnBoot" {
						t.Error("cold JS probe ran transfer cleanup before owner installation")
					}
				}
				return true
			})
		}
	}
	if !foundProbe {
		t.Fatal("JetStream probe scanner root disappeared")
	}
}
func transferWiringSource(source string) error {
	f, err := parser.ParseFile(token.NewFileSet(), "source.go", source, parser.ParseComments)
	if err != nil {
		return err
	}
	for _, d := range f.Decls {
		g, ok := d.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range g.Specs {
			typ, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			if typ.Name.Name == "transferTracker" || typ.Name.Name == "transferEntry" {
				if typ.Name.Name == "transferEntry" && (g.Doc == nil || !strings.Contains(g.Doc.Text(), "origin: hot-update S7 plan §5.1")) {
					return errors.New("tracker move lost its origin anchor")
				}
				if !typ.Assign.IsValid() {
					return errors.New("broker still owns transfer state")
				}
			}
		}
	}
	return nil
}

// origin: hot-update S7 plan §12.2 / L13 / P22
func TestTransferWiringSourceOwnershipControls(t *testing.T) {
	requireWiring(t, transferWiringSource("package broker; type transferTracker = leaf.Tracker"))
	if err := transferWiringSource("package broker; type transferTracker struct { entries map[string]int }"); err == nil {
		t.Fatal("local-state control was accepted")
	}
	if err := transferWiringSource("package broker; type transferEntry struct { finalized bool }"); err == nil {
		t.Fatal("entry-state control was accepted")
	}
}

// origin: hot-update S7 plan §11.3 / P22
func TestTransferLifecycleNoLeaks(t *testing.T) {
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	identities := wiringFDIdentities(t)
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer broker wiring", beforeG)
	testharness.AssertNoFDLeak(t, "transfer broker wiring", beforeFD, 0)
	if !reflect.DeepEqual(identities, wiringFDIdentities(t)) {
		t.Fatal("transfer teardown changed the original FD identity multiset")
	}
}
func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	b, g, p := newWiringGraph(t)
	// This is the real moved legacy watchdog, not a port witness. Parent
	// shutdown must stop it even when the explicit cancellation is delayed.
	// ctx-root: the wiring test owns this bounded exercise lifetime.
	parent, cancelParent := context.WithCancel(context.Background())
	legacy := xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{TransferID: "legacy", SID: "sid", Tier: "a", StartedAt: time.Now()})
	cancelWatchdog := b.startTransferWatchdog(parent, legacy)
	t.Cleanup(cancelWatchdog)
	cancelParent()
	reader, writer, err := os.Pipe()
	requireWiring(t, err)
	done := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, reader); close(done) }()
	var once sync.Once
	p.stop = func() { once.Do(func() { _ = writer.Close(); <-done; _ = reader.Close() }) }
	t.Cleanup(p.stop)
	requireWiring(t, InstallBrokerTransfers(b, g, wiringDeadline(t)))
	if _, err := writer.Write([]byte("owned transfer callback")); err != nil {
		t.Fatal(err)
	}
	// ctx-root: the wiring test owns this bounded exercise lifetime.
	if _, err := b.finalizeStrandedXfers(context.Background()); err != nil {
		t.Fatal(err)
	}
	// ctx-root: the wiring test owns this bounded exercise lifetime.
	if _, err := b.reconcileXferObjects(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireWiring(t, StopBrokerTransfers(b, wiringDeadline(t)))
	select {
	case <-done:
	default:
		t.Fatal("normal stop left transfer callback running")
	}
}

func wiringFDIdentities(t *testing.T) map[string]int {
	t.Helper()
	if runtime.GOOS != "linux" {
		return nil
	}
	entries, err := os.ReadDir("/proc/self/fd")
	requireWiring(t, err)
	result := make(map[string]int)
	for _, entry := range entries {
		if target, err := os.Readlink("/proc/self/fd/" + entry.Name()); err == nil {
			result[target]++
		}
	}
	return result
}

// These witnesses record the full config handed to JetStream. Rebuilding an
// ObjectStoreConfig is forbidden here: it cannot represent Duplicates. A write
// becomes the stream's config, as on a server, so the configuration owner's
// confirming INFO reads back exactly what it sent.
