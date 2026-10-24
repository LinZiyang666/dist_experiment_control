package xferserve

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
)

// A Tier A pull's prepare is answered with the original agent's retained
// copy AND the seal the tracker durably recorded from that sender. A copy that
// is not yet retained is asked for again with the same prepare; a copy that
// differs from the durable seal is source_changed; a status-only answer never
// leaves as success (the ctl would refuse it as an unsealed source); and only
// a Tier A pull may carry inline evidence. A path query probe forwards the
// read-only inspection and returns only the inspected size.
// origin: hot-update S7 plan §6.1 / §7 / §8.1 / FIX-INLINE (FIX-TIERA report items 1 and 3)
func TestTransferInlinePrepareCarriesRetainedCopyAndSeal(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseInlinePrepareLink(t) })
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseInlinePrepareLink(t *testing.T) {
	t.Helper()
	nc, err := nats.Connect(testharness.StartJSNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	data := bytes.Repeat([]byte("retained inline copy\n"), 3000)
	agent := &inlineAgent{data: data}
	sub, err := nc.Subscribe(proto.SubjCmdForwarded(inlineID.SID, inlineID.NID, proto.TransferLinkVerb), agent.answer)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	seal := x.SourceSeal{Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:]), SourceFingerprint: "pinned-source"}
	for _, mode := range []string{"ready", "late", "row-lag", "digest", "foreign-seal", "never", "tier-b"} {
		agent.reset(mode, seal)
		link := agent.link(nc, seal)
		req := inlinePrepareRequest("a")
		budget := 5 * time.Second
		if mode == "never" {
			budget = 3 * time.Second
		}
		if mode == "tier-b" {
			req = inlinePrepareRequest("b")
		}
		d, err := controlbudget.After(budget)
		if err != nil {
			t.Fatal(err)
		}
		resp, attempt, err := link.Request(req, d)
		asks := agent.prepares()
		switch mode {
		case "ready", "late", "row-lag":
			if err != nil || resp.Code != "" || resp.Status == nil || !resp.Status.Budget.SourceSealed || resp.Source == nil || *resp.Source != seal || !bytes.Equal(resp.Inline, data) {
				t.Fatalf("%s: prepare did not carry the retained copy with its durable seal: code=%q bytes=%d err=%v", mode, resp.Code, len(resp.Inline), err)
			}
			want := 1
			if mode == "late" {
				want = 2
			}
			if len(asks) != want {
				t.Fatalf("%s: original agent asked %d times, want %d", mode, len(asks), want)
			}
			for _, ask := range asks {
				if ask.CallID != req.CallID || ask.Identity != proto.Identity(req.Identity) || *ask.Intent != proto.Intent(*req.Intent) || len(ask.Inline) != 0 {
					t.Fatalf("%s: re-ask changed the original prepare: %+v", mode, ask)
				}
			}
		case "digest", "foreign-seal":
			if err != nil || resp.Code != x.CodeSourceChanged || resp.Inline != nil || resp.Source != nil {
				t.Fatalf("%s: unverifiable inline copy was not source_changed: code=%q err=%v", mode, resp.Code, err)
			}
		case "never":
			if err == nil || !strings.Contains(err.Error(), x.CodeResultUnknown) || !attempt.Sent || resp.Inline != nil || resp.Status != nil || len(asks) < 2 {
				t.Fatalf("absent inline copy left as an answer: asks=%d sent=%v err=%v", len(asks), attempt.Sent, err)
			}
		case "tier-b":
			if err != nil || resp.Code != x.CodeResultUnknown || resp.Inline != nil {
				t.Fatalf("object pull accepted inline evidence: code=%q err=%v", resp.Code, err)
			}
		}
	}
	for _, mode := range []string{"size", "no-size"} {
		agent.reset(mode, seal)
		link := agent.link(nc, seal)
		d, err := controlbudget.After(5 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		query := x.Intent{Verb: "pull", Path: "/source"}
		resp, _, err := link.Request(x.Request{Op: x.OpProbe, CallID: "select", Identity: x.Identity{SID: inlineID.SID, NID: inlineID.NID, TransferID: inlineID.TransferID}, Intent: &query}, d)
		probes := agent.probes()
		if len(probes) != 1 || probes[0].Intent == nil || *probes[0].Intent != proto.Intent(query) {
			t.Fatalf("%s: path query was not forwarded to the original agent: %+v", mode, probes)
		}
		if mode == "size" && (err != nil || resp.Probe == nil || resp.Source == nil || *resp.Source != (x.SourceSeal{Size: seal.Size})) {
			t.Fatalf("inspected size was not returned as size-only evidence: %+v %v", resp.Source, err)
		}
		if mode == "no-size" && (err == nil || !strings.Contains(err.Error(), x.CodePayloadUnsupported) || resp.Source != nil) {
			t.Fatalf("path query without inspected size was accepted: %+v %v", resp.Source, err)
		}
	}
}

var inlineID = x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent-instance", TransferID: "inline-transfer",
	EntryNonce: "entry", IntentDigest: strings.Repeat("ab", 32)}
var inlineOrigin = x.Origin{BrokerID: "broker", InstanceID: "broker-instance", BootID: "broker-boot", Generation: 1}

func inlinePrepareRequest(tier string) x.Request {
	id := inlineID
	if tier == "b" {
		id.ObjectVersion = "object"
	}
	now := time.Now().UTC()
	intent := x.Intent{Verb: "pull", Tier: tier, Path: "/source"}
	permit := x.CreationPermit{Identity: id, Origin: inlineOrigin, IssuedAt: now, CreateUntil: now.Add(30 * time.Second), PermitNonce: "permit", Signature: "signed"}
	return x.Request{Op: x.OpPrepare, CallID: "prepare-call", Identity: id, Origin: inlineOrigin, Intent: &intent, Permit: &permit}
}

// inlineAgent is the original agent on its directed intake; the tracker
// view (Status/Rows) is the broker's durable record of the same seal.
type inlineAgent struct {
	mu     sync.Mutex
	mode   string
	data   []byte
	seal   x.SourceSeal
	asks   []proto.TransferLink
	status int
}

func (a *inlineAgent) reset(mode string, seal x.SourceSeal) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.mode, a.seal, a.asks, a.status = mode, seal, nil, 0
}

func (a *inlineAgent) link(nc *nats.Conn, seal x.SourceSeal) *Link {
	return &Link{Conn: func() *nats.Conn { return nc },
		Status: func(r x.StatusRequest, _ x.Deadline) (x.Status, error) {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.status++
			budget := x.BudgetSeal{StartedAt: time.Unix(1700000000, 0).UTC(), EffectiveBudgetNS: int64(proto.XferTimeoutTierA), RemainingNS: int64(20 * time.Second)}
			// row-lag: the seal is durable but the first status reads precede it.
			if a.mode != "row-lag" || a.status > 3 {
				budget.Size, budget.SourceSealed = seal.Size, true
			}
			return x.Status{State: x.StatusLive, Phase: x.PhaseOpen, Identity: r.Identity, Origin: r.Origin, Budget: budget}, nil
		},
		Rows: func(id x.Identity, _ x.Deadline) (x.LedgerRow, error) {
			durable := seal
			return x.LedgerRow{Identity: id, SourceSeal: &durable}, nil
		}}
}

func (a *inlineAgent) answer(msg *nats.Msg) {
	var carrier proto.TransferCommitReq
	if json.Unmarshal(msg.Data, &carrier) != nil || carrier.Link == nil {
		return
	}
	link := *carrier.Link
	a.mu.Lock()
	a.asks = append(a.asks, link)
	asks, mode, seal := len(a.prepares0()), a.mode, proto.SourceSeal(a.seal)
	data := bytes.Clone(a.data)
	a.mu.Unlock()
	result := proto.TransferLinkResult{Version: x.Version, InstanceID: inlineID.AgentInstance}
	if link.Op == proto.TransferLinkProbe {
		if mode != "no-size" {
			size := int64(len(data))
			result.SourceSize = &size
		}
	} else {
		result.Status = &proto.TransferStatusResp{State: proto.StatusState(x.StatusLive), Phase: proto.Phase(x.PhasePrepared), Identity: link.Identity, Origin: link.Origin}
		switch mode {
		case "digest":
			data[0] ^= 1
		case "foreign-seal":
			seal.SourceFingerprint = "replacement-source"
		}
		if mode != "never" && (mode != "late" || asks != 1) {
			result.Inline, result.Source = data, &seal
		}
	}
	body, _ := json.Marshal(proto.TransferCommitResp{OK: true, Link: &result})
	_ = msg.Respond(body)
}

func (a *inlineAgent) prepares0() []proto.TransferLink {
	var out []proto.TransferLink
	for _, ask := range a.asks {
		if ask.Op == proto.TransferLinkPrepare {
			out = append(out, ask)
		}
	}
	return out
}
func (a *inlineAgent) prepares() []proto.TransferLink {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.prepares0()
}
func (a *inlineAgent) probes() []proto.TransferLink {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []proto.TransferLink
	for _, ask := range a.asks {
		if ask.Op == proto.TransferLinkProbe {
			out = append(out, ask)
		}
	}
	return out
}
