package brokerruntime

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokerupgradeport"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats.go"
)

// stampedStore is the store owner as the census reads it: only its Stamp.
type stampedStore struct {
	brokerimage.StoreOwner
	stamp storage.StoreStamp
	err   error
}

func (s stampedStore) Stamp() (storage.StoreStamp, error) { return s.stamp, s.err }

// censusTransfers is a transfer owner with (or, embedded alone, without) the
// tracker census the S8 cut reads.
type censusTransfers struct {
	brokerimage.TransferOwner
	entries []xfertrack.CensusEntry
	err     error
}

func (c censusTransfers) Census(controlbudget.Deadline) ([]xfertrack.CensusEntry, error) {
	return c.entries, c.err
}

type opaqueTransfers struct{ brokerimage.TransferOwner }

// The broker's census is read from each current owner through its own API:
// the store's database identity, the capable tracker's entries (settled or
// not), the residue the S6 cut refuses and the image phase. An owner that
// cannot enumerate itself is an error, never an empty broker.
// origin: hot-update S8 plan §4.2 / M06 / M14 / §11.2 P15 P17; S8 BROKER-EXPORTER R-11
func TestTransactionFactsNameEveryOwnerOrRefuse(t *testing.T) {
	store := stampedStore{stamp: storage.StoreStamp{Identity: storage.DBIdentity{Path: "/var/lib/tether/state.db", Device: 7, Inode: 9}}}
	none := func() (int, error) { return 0, nil }
	serving := &proto.BrokerImageStatus{Phase: proto.BrokerImagePhaseServing}
	transfers := censusTransfers{entries: []xfertrack.CensusEntry{{SID: "lab", TransferID: "t1", EntryNonce: "n1"}, {SID: "lab", TransferID: "t0", EntryNonce: "n0", Settled: true}}}
	facts, err := TransactionFacts(store, nil, nil, transfers, func() (int, error) { return 2, nil }, serving)
	if err != nil {
		t.Fatal(err)
	}
	if facts.Store != (brokerupgradeport.StoreIdentity{Path: "/var/lib/tether/state.db", Device: 7, Inode: 9}) || facts.Tunnel || !facts.Transfers ||
		len(facts.Trackers) != 2 || !facts.Trackers[1].Settled || facts.Trackers[0].EntryNonce != "n1" || facts.Unsettled != 2 || !facts.Serving {
		t.Fatalf("facts lost an owner: %+v", facts)
	}
	// A running tunnel server is named even when it holds no allocation, and
	// its allocations are sorted by public port.
	allocations := func() []brokerupgradeport.TunnelSession {
		return []brokerupgradeport.TunnelSession{{Port: 4200, SID: "lab", NID: "b", Epoch: 2}, {Port: 4100, SID: "lab", NID: "a", Epoch: 1}}
	}
	if facts, err := TransactionFacts(store, nil, allocations, nil, none, serving); err != nil || !facts.Tunnel || len(facts.Sessions) != 2 || facts.Sessions[0].Port != 4100 {
		t.Fatalf("tunnel allocations lost: %+v %v", facts, err)
	}
	if facts, err := TransactionFacts(store, nil, func() []brokerupgradeport.TunnelSession { return nil }, nil, none, serving); err != nil || !facts.Tunnel {
		t.Fatalf("an idle tunnel server was not named: %+v %v", facts, err)
	}
	for phase, want := range map[string]bool{proto.BrokerImagePhaseServing: true, proto.BrokerImagePhaseControlReady: true,
		proto.BrokerImagePhaseImporting: false, proto.BrokerImagePhaseFrozen: false, proto.BrokerImagePhaseFailed: false} {
		facts, err := TransactionFacts(store, nil, nil, nil, none, &proto.BrokerImageStatus{Phase: phase})
		if err != nil || facts.Serving != want || facts.Transfers {
			t.Errorf("phase %s: serving=%t transfers=%t err=%v", phase, facts.Serving, facts.Transfers, err)
		}
	}
	if facts, err := TransactionFacts(store, nil, nil, nil, none, nil); err != nil || facts.Serving {
		t.Fatalf("no image status read as serving: %+v %v", facts, err)
	}
	for name, call := range map[string]func() (brokerupgradeport.BrokerFacts, error){
		"no store owner": func() (brokerupgradeport.BrokerFacts, error) {
			return TransactionFacts(nil, nil, nil, nil, none, serving)
		},
		"unreadable store": func() (brokerupgradeport.BrokerFacts, error) {
			return TransactionFacts(stampedStore{err: errors.New("closed")}, nil, nil, nil, none, serving)
		},
		"no residue count": func() (brokerupgradeport.BrokerFacts, error) {
			return TransactionFacts(store, nil, nil, nil, nil, serving)
		},
		"residue unreadable": func() (brokerupgradeport.BrokerFacts, error) {
			return TransactionFacts(store, nil, nil, nil, func() (int, error) { return 0, errors.New("x") }, serving)
		},
		"tracker without census": func() (brokerupgradeport.BrokerFacts, error) {
			return TransactionFacts(store, nil, nil, opaqueTransfers{}, none, serving)
		},
		"tracker census failed": func() (brokerupgradeport.BrokerFacts, error) {
			return TransactionFacts(store, nil, nil, censusTransfers{err: errors.New("busy")}, none, serving)
		},
	} {
		if facts, err := call(); err == nil {
			t.Errorf("%s: described %+v", name, facts)
		}
	}
}

// Control is proven by one real request through the broker's business
// subscriptions on its current connection: a single broker's session list for
// a fresh unowned actor (answered empty by the store), a clustered broker's
// cursor probe answered by itself. No responder, an error answer, a non-empty
// list or another broker's answer is no proof.
// origin: hot-update S8 plan §4.2 / M05 / §11.2 P15; S8 BROKER-EXPORTER R-11
func TestControlProbeNeedsTheBrokersOwnAnswer(t *testing.T) {
	nc, err := nats.Connect(testharness.StartNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	nonce := strings.Repeat("a", 32)
	if ControlProbe(nil, nonce, "") == nil || ControlProbe(nc, "short", "") == nil {
		t.Fatal("a probe without a connection or a nonce succeeded")
	}
	if ControlProbe(nc, nonce, "") == nil {
		t.Fatal("a probe nobody answered succeeded")
	}
	list := strings.Replace(proto.SubjCtrlSessionList("ACTOR"), "ACTOR", "*", 1)
	var answer proto.SessionListResp
	sub, err := nc.Subscribe(list, func(m *nats.Msg) { body, _ := json.Marshal(answer); _ = m.Respond(body) })
	if err != nil {
		t.Fatal(err)
	}
	if err := ControlProbe(nc, nonce, ""); err != nil {
		t.Fatalf("store-answered probe refused: %v", err)
	}
	answer.Error = "store unavailable"
	if ControlProbe(nc, nonce, "") == nil {
		t.Fatal("an error answer proved control")
	}
	answer = proto.SessionListResp{Sessions: []proto.SessionEntry{{SID: "lab"}}}
	if ControlProbe(nc, nonce, "") == nil {
		t.Fatal("a fresh actor owned a session: the probe did not reach this store")
	}
	_ = sub.Unsubscribe()
	responder := "broker-2"
	if _, err := nc.Subscribe(proto.SubjClusterCursor, func(m *nats.Msg) {
		body, _ := json.Marshal(proto.ClusterHealthResp{NodeID: responder})
		_ = m.Respond(body)
	}); err != nil {
		t.Fatal(err)
	}
	if ControlProbe(nc, nonce, "broker-1") == nil {
		t.Fatal("another broker's cursor answer proved this broker's control")
	}
	responder = "broker-1"
	if err := ControlProbe(nc, nonce, "broker-1"); err != nil {
		t.Fatalf("self-answered cursor probe refused: %v", err)
	}
}

// A request naming another broker must be silent before it samples this
// broker's clock, role or store. The uninitialized node is intentional: those
// authorities must remain untouched for a foreign target.
// origin: S8 internal review cluster-roll F9
func TestForeignControlProbeDoesNotSampleThisBroker(t *testing.T) {
	answers := 0
	handler := ClusterHealthResponder(nil, &cluster.Node{}, nil, func() time.Time { t.Fatal("foreign probe sampled local runtime authority"); return time.Time{} }, nil, nil, "", nil, func(*proto.ClusterHealthResp) { t.Fatal("foreign probe projected local health") }, func(*nats.Msg, []byte) { answers++ })
	handler(&nats.Msg{Reply: "private.reply", Data: []byte(`{"query_nonce":"0123456789abcdef0123456789abcdef","target_node":"another-broker"}`)})
	if answers != 0 {
		t.Fatal("foreign target received this broker's control answer")
	}
}
