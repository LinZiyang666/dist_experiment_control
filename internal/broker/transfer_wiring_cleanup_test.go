package broker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type wiringConfigJS struct {
	jetstream.JetStream
	config jetstream.StreamConfig
	writes []jetstream.StreamConfig
}
type wiringConfigStream struct {
	jetstream.Stream
	js *wiringConfigJS
}
type wiringConfigStore struct{ jetstream.ObjectStore }
type wiringConfigStatus struct{ jetstream.ObjectStoreStatus }

func (s wiringConfigStream) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return &jetstream.StreamInfo{Config: s.js.config}, nil
}
func (s wiringConfigStore) Status(context.Context) (jetstream.ObjectStoreStatus, error) {
	return wiringConfigStatus{}, nil
}
func (s wiringConfigStatus) Replicas() int { return 1 }
func (j *wiringConfigJS) Stream(context.Context, string) (jetstream.Stream, error) {
	return wiringConfigStream{js: j}, nil
}
func (j *wiringConfigJS) ObjectStore(context.Context, string) (jetstream.ObjectStore, error) {
	return wiringConfigStore{}, nil
}
func (j *wiringConfigJS) AccountInfo(context.Context) (*jetstream.AccountInfo, error) {
	return &jetstream.AccountInfo{Tier: jetstream.Tier{ReservedStore: 2, Limits: jetstream.AccountLimits{MaxStore: 1}}}, nil
}
func (j *wiringConfigJS) UpdateStream(_ context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	j.writes = append(j.writes, cfg)
	j.config = cfg
	return wiringConfigStream{js: j}, nil
}

// Conn has no server behind it; the owner then seals MaxPayload 0 (fail closed for v1).
func (j *wiringConfigJS) Conn() *nats.Conn { return nil }

// installWiringConfigOwner binds the real configuration owner (with this broker's own census) as the
// graph's Buckets port and installs the graph, so legacy raise/converge reach it through the shared
// router exactly as in production. The sample ledger row is cleanup-pending in bucket xfer-sid; an
// honest census would (correctly) refuse a shrink there, so the ledger is idle after installation.
// origin: hot-update S7 plan §2.1 / lead decision on authoritative shrink proof
func installWiringConfigOwner(t *testing.T, js jetstream.JetStream) *Broker {
	t.Helper()
	b, p := installWiringConfigGraph(t, js)
	p.census.Rows = nil
	return b
}

// installWiringConfigGraph keeps the sample cleanup-pending row in xfer-sid.
func installWiringConfigGraph(t *testing.T, js jetstream.JetStream) (*Broker, *wiringProbe) {
	t.Helper()
	b, g, p := newWiringGraph(t)
	setBrokerJS(b, js)
	owner, err := newProvisionOwner(b, js)
	requireWiring(t, err)
	g.Buckets = owner
	requireWiring(t, InstallBrokerTransfers(b, g, wiringDeadline(t)))
	return b, p
}

// origin: hot-update S7 plan §2.1 / M14 / P22
func TestTransferWiringLegacyConfigKeepsRecoveryHorizon(t *testing.T) {
	// ctx-root: the adapter check owns its bounded JetStream call lifetime.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	original := jetstream.StreamConfig{Name: "OBJ_xfer-sid", Subjects: []string{"$O.xfer-sid.C.>", "$O.xfer-sid.M.>"},
		Description: "operator description", MaxBytes: 8 << 30, MaxAge: 48 * time.Hour, Duplicates: 36 * time.Hour,
		Storage: jetstream.FileStorage, Replicas: 1, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardNew,
		Compression: jetstream.S2Compression, AllowDirect: true, AllowRollup: true,
		Metadata: map[string]string{"operator": "keep"}, Placement: &jetstream.Placement{Cluster: "west", Tags: []string{"ssd"}}}
	js := &wiringConfigJS{config: original}
	b := installWiringConfigOwner(t, js)
	requireWiring(t, raiseXferReplicas(ctx, brokerTransferConfigJS(b), "xfer-sid", 3))
	want := original
	want.Replicas = 3
	if len(js.writes) != 1 || !reflect.DeepEqual(js.writes[0], want) {
		t.Fatalf("replica repair erased operator or recovery config: %+v", js.writes)
	}
	info := &jetstream.StreamInfo{Config: want}
	b.convergeOversizedXferBucket(ctx, "sid", "xfer-sid", info, 4<<30, 1)
	want.MaxBytes = 4 << 30
	if len(js.writes) != 2 || !reflect.DeepEqual(js.writes[1], want) || info.Config.MaxBytes != want.MaxBytes {
		t.Fatalf("shrink erased operator/recovery config or lowered replicas: %+v", js.writes)
	}
}

// The legacy small-disk repair keeps its own triggers, but the write itself is the owner's
// proof-gated shrink: responsibility in either tracker, or a census that cannot prove its absence,
// leaves the operator's reservation untouched.
// origin: hot-update S7 plan §2.1 / lead decision on authoritative shrink proof
func TestTransferWiringLegacyShrinkHonoursLiveResponsibility(t *testing.T) {
	original := jetstream.StreamConfig{Name: "OBJ_xfer-sid", MaxBytes: 8 << 30, MaxAge: 48 * time.Hour,
		Duplicates: 36 * time.Hour, Storage: jetstream.FileStorage, Replicas: 1, Discard: jetstream.DiscardNew}
	for _, tc := range []struct {
		name string
		busy func(*testing.T, *Broker, *wiringProbe)
	}{
		{name: "continuation_cleanup_pending", busy: func(*testing.T, *Broker, *wiringProbe) {}},
		{name: "legacy_transfer_admitted", busy: func(t *testing.T, b *Broker, p *wiringProbe) {
			p.census.Rows = nil
			entry := xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{TransferID: "legacy", SID: "sid", Verb: "push", Tier: "b", Bucket: "xfer-sid"})
			if code := putLegacyTransfer(b.transfers, entry); code != "" {
				t.Fatalf("fixture transfer refused: %s", code)
			}
		}},
		{name: "ledger_census_incomplete", busy: func(_ *testing.T, _ *Broker, p *wiringProbe) {
			p.census.Rows, p.census.Complete = nil, false
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// ctx-root: the adapter check owns its bounded JetStream call lifetime.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			js := &wiringConfigJS{config: original}
			b, p := installWiringConfigGraph(t, js)
			tc.busy(t, b, p)
			reduced := original
			reduced.MaxBytes = 4 << 30
			_, err := brokerTransferConfigJS(b).UpdateStream(ctx, reduced)
			if err == nil || !strings.Contains(err.Error(), x.CodeObjectConflict) {
				t.Fatalf("legacy reduction bypassed the owner's census: %v", err)
			}
			info := &jetstream.StreamInfo{Config: original}
			b.convergeOversizedXferBucket(ctx, "sid", "xfer-sid", info, 4<<30, 1)
			if len(js.writes) != 0 || js.config.MaxBytes != 8<<30 || info.Config.MaxBytes != 8<<30 {
				t.Fatalf("shrink ignored live responsibility: writes=%+v", js.writes)
			}
		})
	}
}

// With an owner installed, legacy provisioning takes the owner path, which must still carry the
// small-disk repair: an over-committed store with an oversized idle bucket is shrunk through the
// owner (and admission uses the new ceiling); live responsibility leaves it untouched.
// origin: hot-update S7 plan §2.1 / lead decision on authoritative shrink proof
func TestTransferWiringOwnerPathKeepsSmallDiskRepair(t *testing.T) {
	original := jetstream.StreamConfig{Name: "OBJ_xfer-sid", MaxBytes: 8 << 30, MaxAge: 48 * time.Hour,
		Duplicates: 36 * time.Hour, Storage: jetstream.FileStorage, Replicas: 1, Discard: jetstream.DiscardNew}
	// Below the 2 GiB per-transfer cap, so admission visibly follows the repaired ceiling.
	const target = int64(1 << 30)
	for _, tc := range []struct {
		name       string
		idle       bool
		maxBytes   int64
		admissible int64
	}{
		{name: "idle_bucket_is_repaired", idle: true, maxBytes: target, admissible: xferPayloadLimit(target, 0)},
		{name: "busy_bucket_is_kept", maxBytes: 8 << 30, admissible: xferPayloadLimit(8<<30, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// ctx-root: the provisioning check owns its bounded JetStream call lifetime.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			js := &wiringConfigJS{config: original}
			b, p := installWiringConfigGraph(t, js)
			if tc.idle {
				p.census.Rows = nil
			}
			bucket, admissible, err := ensureXferBucketSizedWithLimit(ctx, b, "sid", 1, target)
			if err != nil || bucket != "xfer-sid" {
				t.Fatalf("owner-path ensure failed: bucket=%q err=%v", bucket, err)
			}
			if js.config.MaxBytes != tc.maxBytes || admissible != tc.admissible {
				t.Fatalf("small-disk repair on the owner path: MaxBytes=%d admissible=%d writes=%+v", js.config.MaxBytes, admissible, js.writes)
			}
		})
	}
}

// origin: hot-update S7 plan §5.2 / T21 / P22
func TestTransferWiringConfirmedTerminalStillRetiresObject(t *testing.T) {
	beforeG := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferConfirmedCleanup(t)
	}
	testharness.AssertNoGoroutineLeak(t, "confirmed transfer cleanup", beforeG)
	testharness.AssertNoFDLeak(t, "confirmed transfer cleanup", beforeFD, 0)
}

type wiringCleanupFailure struct{ jetstream.JetStream }

func (j wiringCleanupFailure) ObjectStore(context.Context, string) (jetstream.ObjectStore, error) {
	return nil, errors.New("object retirement response unavailable")
}

func exerciseTransferConfirmedCleanup(t *testing.T) {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, JetStream: true, StoreDir: t.TempDir()})
	requireWiring(t, err)
	s.Start()
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded JetStream not ready")
	}
	nc, err := nats.Connect(s.ClientURL())
	requireWiring(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	requireWiring(t, err)
	// ctx-root: this exercise joins every embedded server and connection.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	requireWiring(t, jsstream.EnsureHistoryStream(ctx, js, "sid", 1))
	store, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "xfer-sid", Storage: jetstream.MemoryStorage})
	requireWiring(t, err)
	b, _, _ := newWiringGraph(t)
	b.cfg.ClusterDataDir = t.TempDir()
	setBrokerJS(b, js)
	for _, tid := range []string{"confirmed", "unconfirmed"} {
		terminal := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push", Ts: time.Now().UTC(), Session: "sid", Node: "nid", TransferID: tid, Tier: "b"}
		row := xferInflightRecord{TransferID: tid, Session: "sid", Node: "nid", Verb: "push", Tier: "b", StartedAt: terminal.Ts, Terminal: &terminal}
		for _, dir := range []string{b.xferInflightDir(), b.xferTerminalOutboxDir()} {
			requireWiring(t, b.writeLedgerRecord(dir, row))
		}
		if _, err := store.PutBytes(ctx, tid, []byte("retained bytes")); err != nil {
			t.Fatal(err)
		}
		if tid == "confirmed" {
			payload, err := json.Marshal(terminal)
			requireWiring(t, err)
			if _, err := js.Publish(ctx, proto.SubjAuditTransfer("sid"), payload); err != nil {
				t.Fatal(err)
			}
			setBrokerJS(b, wiringCleanupFailure{JetStream: js})
			if b.replayStagedTerminal(ctx, row) {
				t.Fatal("unknown object retirement was acknowledged")
			}
			for _, dir := range []string{b.xferInflightDir(), b.xferTerminalOutboxDir()} {
				if _, err := os.Stat(filepath.Join(dir, xferInflightFilename(tid))); err != nil {
					t.Fatalf("failed retirement discarded its durable responsibility: %v", err)
				}
			}
			setBrokerJS(b, js)
		}
		if !b.replayStagedTerminal(ctx, row) {
			t.Fatal("exact terminal could not retire its cleanup")
		}
		if _, err := store.GetInfo(ctx, tid); !errors.Is(err, jetstream.ErrObjectNotFound) {
			t.Fatalf("confirmed terminal left object behind: %v", err)
		}
		for _, dir := range []string{b.xferInflightDir(), b.xferTerminalOutboxDir()} {
			if _, err := os.Stat(filepath.Join(dir, xferInflightFilename(tid))); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("terminal left a durable ledger row: %v", err)
			}
		}
	}
	stream, err := js.Stream(ctx, jsstream.HistoryStreamName("sid"))
	requireWiring(t, err)
	info, err := stream.Info(ctx)
	if err != nil || info.State.Msgs != 2 || info.State.Consumers != 0 {
		t.Fatalf("cleanup duplicated terminal history or left consumers: info=%+v err=%v", info, err)
	}
}

// An ordinary Run return joins the transfer graph through stopBrokerTransferRun.
// A clean stop must hand back the very sentinel Run produced — embedded callers
// (test/p13's proxy stack, the broker Run tests, every harness) compare
// err == context.Canceled. errors.Join(context.Canceled, nil) printed
// "context canceled" yet compared unequal, so every clean shutdown of an
// embedded broker read as "did not exit cleanly". A failing stop must still
// surface beside the sentinel, and a managed owner is not stopped here at all.
// Identity is compared with ==/!= on purpose: errors.Is would accept the wrap.
// origin: hot-update S6 plan §4.2 / T52; S6-proxyfix make-test regression
func TestTransferRunStopKeepsRunSentinel(t *testing.T) {
	stopErr := errors.New("bucket stop failed")
	sentinels := []struct {
		name string
		err  error
	}{{"nil", nil}, {"canceled", context.Canceled}, {"deadline", context.DeadlineExceeded}}
	for _, tc := range []struct {
		name    string
		graph   bool
		managed bool
		fail    error
	}{
		{name: "no graph installed"},
		{name: "graph stops cleanly", graph: true},
		{name: "graph stop fails", graph: true, fail: stopErr},
		{name: "managed owner joins through the cut", graph: true, managed: true, fail: stopErr},
	} {
		for _, sentinel := range sentinels {
			t.Run(tc.name+"/"+sentinel.name, func(t *testing.T) {
				b := &Broker{}
				p := &wiringProbe{fail: map[string]error{"stop-bucket": tc.fail}}
				if tc.graph {
					b.transferRuntime.Store(&BrokerTransfers{
						Tracker: wiringTracker{p: p}, Controller: wiringController{p: p},
						Finalizer: wiringFinalizer{p: p}, Collector: wiringCollector{p: p},
						Buckets: wiringBucket{p: p},
					})
				}
				got := sentinel.err
				managed := tc.managed
				stopBrokerTransferRun(b, &managed, &got)
				stopped := false
				for _, call := range p.takeCalls() {
					stopped = stopped || call == "stop-bucket"
				}
				if stopped != (tc.graph && !tc.managed) {
					t.Fatalf("graph stopped=%v, want %v", stopped, tc.graph && !tc.managed)
				}
				if tc.fail == nil || tc.managed {
					if got != sentinel.err {
						t.Fatalf("clean stop replaced Run's sentinel %v with %T(%v)", sentinel.err, got, got)
					}
					return
				}
				if !errors.Is(got, tc.fail) {
					t.Fatalf("failed stop was dropped: %v", got)
				}
				if sentinel.err != nil && !errors.Is(got, sentinel.err) {
					t.Fatalf("failed stop dropped Run's sentinel %v: %v", sentinel.err, got)
				}
			})
		}
	}
}

func requireWiring(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
