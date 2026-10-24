package xferserve

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// origin: S7 internal review s7-p9m-open F1 / hot-update S7 plan §6.1 / T26
func TestTransferDirectoryClaimsBindIssuerAndSurviveReopen(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseDurableDirectoryClaim(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "durable directory claims", g)
	testharness.AssertNoFDLeak(t, "durable directory claims", fd, 0)
}

func exerciseDurableDirectoryClaim(t *testing.T) {
	_, js := assemblyHost(t, t.TempDir())
	d := assemblyDeadline(t)
	a, err := openDirectory(js, 1, d)
	if err != nil {
		t.Fatal(err)
	}
	var permits [2]x.CreationPermit
	var origins [2]x.Origin
	for i, broker := range []string{"original", "new-home"} {
		issuer, err := nkeys.CreateAccount()
		if err != nil {
			t.Fatal(err)
		}
		defer issuer.Wipe()
		pub, _ := issuer.PublicKey()
		o := x.Origin{BrokerID: broker, InstanceID: "instance", BootID: "boot", Generation: 1}
		origins[i] = o
		if err := a.register(o, pub, d); err != nil {
			t.Fatal(err)
		}
		p, err := xferauth.NewPermitAuthority(t.TempDir(), o, issuer, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		permits[i], err = p.IssueFor(x.Identity{SID: "lab", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "issued-only"}, x.Intent{Verb: "push", Tier: "a", Path: "destination"}, d)
		if err != nil {
			t.Fatal(err)
		}
	}
	type claimed struct {
		r   *permitRecord
		err error
	}
	answers := make(chan claimed, 2)
	for _, p := range permits {
		go func() { r, err := a.claim(p, d); answers <- claimed{r, err} }()
	}
	first, second := <-answers, <-answers
	if first.err != nil || second.err != nil || first.r == nil || second.r == nil || *first.r != *second.r {
		t.Fatalf("racing homes did not converge on original signed bytes: %v %v", first.err, second.err)
	}
	if first.r.Permit != permits[0] && first.r.Permit != permits[1] {
		t.Fatal("directory invented a third creation window")
	}
	root := t.TempDir()
	if err := pinDirectory(root, a); err != nil {
		t.Fatal(err)
	}
	again, err := openDirectory(js, 1, d, root)
	if err != nil {
		t.Fatal(err)
	}
	r, err := again.lookup("lab", "issued-only", d)
	if err != nil || r == nil || *r != *first.r {
		t.Fatal("reopen lost the original issuer or permit window")
	}
	peers, err := again.Peers(func(x.Deadline) ([]x.Origin, error) { return []x.Origin{origins[1]}, nil }, d)
	if err != nil || len(peers) != 2 {
		t.Fatal("current roster pruned the removed original origin")
	}
	if _, err := matchingPermit(*r, x.Identity{SID: "lab", NID: "other", ActorFP: "actor", AgentInstance: "agent", TransferID: "issued-only"}, x.Intent{Verb: "push", Tier: "a", Path: "destination"}); err == nil {
		t.Fatal("different node reused a durable claim")
	}
	if _, err := matchingPermit(*r, x.Identity{SID: "lab", NID: "node", ActorFP: "other", AgentInstance: "agent", TransferID: "issued-only"}, x.Intent{Verb: "push", Tier: "a", Path: "destination"}); err == nil {
		t.Fatal("different actor reused a durable claim")
	}
}

type missingDirectoryJS struct {
	jetstream.JetStream
	creates int
}

func (j *missingDirectoryJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if name == authorityStream {
		return nil, jetstream.ErrStreamNotFound
	}
	return j.JetStream.Stream(ctx, name)
}
func (j *missingDirectoryJS) CreateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	j.creates++
	return j.JetStream.CreateStream(ctx, cfg)
}

// origin: S7 internal review s7-p9m-open F1 / hot-update S7 plan §6.1 / T28
func TestTransferDirectoryRejectsRetainedStreamLossAndUnsafeRetention(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseDirectoryLoss(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "directory loss", g)
	testharness.AssertNoFDLeak(t, "directory loss", fd, 0)
}

func exerciseDirectoryLoss(t *testing.T) {
	_, js := assemblyHost(t, t.TempDir())
	d := assemblyDeadline(t)
	a, err := openDirectory(js, 1, d)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := pinDirectory(root, a); err != nil {
		t.Fatal(err)
	}
	missing := &missingDirectoryJS{JetStream: js}
	if _, err := openDirectory(missing, 1, d, root); err == nil {
		t.Fatal("retained authority was replaced after a metadata miss")
	}
	if missing.creates != 0 {
		t.Fatal("retained directory miss attempted CREATE")
	}
	stream, err := js.Stream(t.Context(), authorityStream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cfg := info.Config
	cfg.MaxAge = 3 * time.Minute
	if _, err := js.UpdateStream(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := a.lookup("lab", "missing", d); err == nil {
		t.Fatal("TTL returned an authoritative directory absence")
	}
	if _, err := a.Peers(func(x.Deadline) ([]x.Origin, error) { return []x.Origin{{BrokerID: "broker"}}, nil }, d); err == nil {
		t.Fatal("unsafe retention certified a complete origin set")
	}
}

// A refused CAS can name an accepted proposal that is not visible to the
// first readback yet. Absence cannot become a fresh permit or a definitive no.
// origin: S7 internal review TRANSFER-OVERLAY-REPAIR F4 / hot-update S7 plan §6.1 / T39
func TestTransferDirectoryClaimWaitsForOriginalCASVisibility(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseDirectoryClaimVisibility(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "directory claim visibility", g)
	testharness.AssertNoFDLeak(t, "directory claim visibility", fd, 0)
}

func exerciseDirectoryClaimVisibility(t *testing.T) {
	t.Helper()
	_, js := assemblyHost(t, t.TempDir())
	d := assemblyDeadline(t)
	a, err := openDirectory(js, 1, d)
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer issuer.Wipe()
	pub, err := issuer.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	origin := x.Origin{BrokerID: "original", InstanceID: "instance", BootID: "boot", Generation: 1}
	if err := a.register(origin, pub, d); err != nil {
		t.Fatal(err)
	}
	permits, err := xferauth.NewPermitAuthority(t.TempDir(), origin, issuer, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	permit, err := permits.IssueFor(x.Identity{SID: "lab", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "original"}, x.Intent{Verb: "push", Tier: "a", Path: "destination"}, d)
	if err != nil {
		t.Fatal(err)
	}
	// The positive is real: first persist the winner, then refuse another CAS.
	// Only one negative read's visibility is controlled; no success is fabricated.
	if _, err := a.claim(permit, d); err != nil {
		t.Fatal(err)
	}
	subject := authorityPrefix + "permit." + authorityKey(permit.Identity.SID, permit.Identity.TransferID)
	lag := &directoryVisibility{JetStream: js, subject: subject}
	a.js = lag
	retained, err := a.claim(permit, d)
	if err != nil || retained == nil || retained.Permit != permit {
		t.Fatalf("accepted original CAS lost after a temporary absent read: retained=%v err=%v", retained != nil, err)
	}
	if lag.reads < 2 {
		t.Fatal("claim did not reconcile the original business key after absent readback")
	}
}

type directoryVisibility struct {
	jetstream.JetStream
	subject   string
	reads     int
	invisible bool
	publishes int
}

func (j *directoryVisibility) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	stream, err := j.JetStream.Stream(ctx, name)
	if err != nil {
		return nil, err
	}
	return &directoryVisibleStream{Stream: stream, owner: j}, nil
}

type directoryVisibleStream struct {
	jetstream.Stream
	owner *directoryVisibility
}

func (s *directoryVisibleStream) GetLastMsgForSubject(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	if subject == s.owner.subject {
		s.owner.reads++
		if s.owner.invisible || s.owner.reads == 1 {
			return nil, jetstream.ErrMsgNotFound
		}
	}
	return s.Stream.GetLastMsgForSubject(ctx, subject)
}

func (j *directoryVisibility) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ack, err := j.JetStream.PublishMsg(ctx, msg, opts...)
	if msg.Subject == j.subject {
		j.publishes++
		if err == nil && j.invisible {
			return nil, jetstream.ErrInvalidJSAck
		}
	}
	return ack, err
}

// Publication may have created authority even when its ACK/readback is absent.
// This is sent-unknown, so the original invocation retains its permit request.
// origin: S7 internal review TRANSFER-OVERLAY-REPAIR F4 / hot-update S7 plan §6.1 / §7
func TestTransferPermitClaimUncertaintyKeepsOriginalInvocation(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exercisePermitClaimUncertainty(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "permit claim uncertainty", g)
	testharness.AssertNoFDLeak(t, "permit claim uncertainty", fd, 0)
}

func exercisePermitClaimUncertainty(t *testing.T) {
	t.Helper()
	_, js := assemblyHost(t, t.TempDir())
	a, err := openDirectory(js, 1, assemblyDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer issuer.Wipe()
	pub, err := issuer.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	origin := x.Origin{BrokerID: "original", InstanceID: "instance", BootID: "boot", Generation: 1}
	if err := a.register(origin, pub, assemblyDeadline(t)); err != nil {
		t.Fatal(err)
	}
	permits, err := xferauth.NewPermitAuthority(t.TempDir(), origin, issuer, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	base := x.Base{Clock: func() (string, int64, error) { now, err := controlbudget.Now(); return origin.BootID, now, err }}
	root := t.TempDir()
	ledger, err := xferledger.New(xferledger.Config{Base: base, Root: filepath.Join(root, "ledger"), FallbackRoot: filepath.Join(root, "outbox")})
	if err != nil {
		t.Fatal(err)
	}
	id := x.Identity{SID: "lab", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "original"}
	intent := x.Intent{Verb: "push", Tier: "a", Path: "destination"}
	lag := &directoryVisibility{JetStream: js, subject: authorityPrefix + "permit." + authorityKey(id.SID, id.TransferID), invisible: true}
	a.js = lag
	guard := GuardPermits{PermitAuthority: permits, Directory: a, Owners: Owners{Origin: origin, Ledger: func() x.LedgerPort { return ledger }}}
	d, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := guard.IssueFor(id, intent, d); !errors.Is(err, xferauth.ControlRefusal(x.CodeResultUnknown)) {
		t.Fatalf("sent permit claim was reported as a definitive admission refusal: %v", err)
	}
	if lag.publishes != 1 || lag.reads < 2 {
		t.Fatalf("uncertainty recreated authority instead of reading the original key: publishes=%d reads=%d", lag.publishes, lag.reads)
	}
	original, err := permits.IssueFor(id, intent, assemblyDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	lag.invisible = false
	replay, err := guard.IssueFor(id, intent, assemblyDeadline(t))
	if err != nil || replay != original || lag.publishes != 1 {
		t.Fatalf("reconciliation replaced the original signed permit/window: publishes=%d err=%v", lag.publishes, err)
	}
}
