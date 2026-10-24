package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: simcluster-speed 0a (drill 67): a tier-B push whose upload failed before commit must tell
// the broker so, or the session's bucket stays held for the whole watchdog budget.
//
// S7: the pre-S7 direct flow (pushTierB with its Put retry ladder and watchdog) was removed — its
// rewind-and-Put retry is the "Put retry pretending to be resume" that hot-update S7 plan L1 forbids,
// and no command reached it any more. These tests now drive the real `tether push` command against a
// broker that answers caps as explicit cap0, so the one-shot legacy owner (cli legacyPush) runs.
// Repeated invocation replays evidence instead of re-uploading. The denied-purge
// completion and error regressions now exercise that owner in transfer_legacy_acl_test.go.

func startJetStreamNATS(t *testing.T) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	// origin: hot-update S7 plan §11.4 / P31; Cobra pulls authenticate with a real NKey.
	opts.AlwaysEnableNonce = true
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	return ns.ClientURL()
}

// legacyPushFixture plays a cap0 broker for the real push command: caps answer completely
// WITHOUT a transfer version (the explicit cap0 answer), prepare is answered OK for tier B,
// every finalize is recorded, and push-commit is answered by `commit` — nil for "nobody answers".
type legacyPushFixture struct {
	t         *testing.T
	url, home string
	fins      chan proto.TransferFinalize
	ids       chan string
}

const legacyFixtureSID, legacyFixtureNID = "lab", "lab-1"

func startLegacyPushFixture(t *testing.T, bucket bool, commit nats.MsgHandler) *legacyPushFixture {
	t.Helper()
	f := &legacyPushFixture{t: t, url: startJetStreamNATS(t), home: t.TempDir(), fins: make(chan proto.TransferFinalize, 4), ids: make(chan string, 4)}
	writeReviewPsIdentity(t, f.home, legacyFixtureSID)
	id, err := cli.EnsureIdentity(f.home)
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(f.url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	if bucket {
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: proto.XferBucketName(legacyFixtureSID)}); err != nil {
			t.Fatal(err)
		}
	}
	subscribe := func(subj string, h nats.MsgHandler) {
		sub, err := nc.Subscribe(subj, h)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = sub.Unsubscribe() })
	}
	respond := func(m *nats.Msg, v any) {
		body, _ := json.Marshal(v)
		_ = m.Respond(body)
	}
	subscribe(proto.SubjCtrlCaps(id.PublicKey, legacyFixtureSID), func(m *nats.Msg) {
		respond(m, proto.CapsResp{OK: true, JetStreamReady: true, MaxPayload: nc.MaxPayload()})
	})
	subscribe(proto.SubjCmdBy(legacyFixtureSID, id.PublicKey, legacyFixtureNID, "push"), func(m *nats.Msg) {
		var req proto.PushPrepareReq
		_ = json.Unmarshal(m.Data, &req)
		f.ids <- req.TransferID
		respond(m, proto.PushPrepareResp{OK: true, Tier: "b"})
	})
	subscribe(proto.SubjCtrlTransferFinalize(id.PublicKey, legacyFixtureSID, "*"), func(m *nats.Msg) {
		var fin proto.TransferFinalize
		_ = json.Unmarshal(m.Data, &fin)
		f.fins <- fin
		respond(m, proto.TransferFinalizeResp{OK: true})
	})
	if commit != nil {
		subscribe(proto.SubjCmdBy(legacyFixtureSID, id.PublicKey, legacyFixtureNID, "push-commit"), commit)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return f
}

// push runs the real command with a source above the legacy inline ceiling, so tier B is chosen.
func (f *legacyPushFixture) push() error {
	f.t.Helper()
	local := filepath.Join(f.t.TempDir(), "payload.bin")
	if err := os.WriteFile(local, []byte(strings.Repeat("y", 600<<10)), 0o600); err != nil {
		f.t.Fatal(err)
	}
	cmd := newPushCmd()
	cmd.SetArgs([]string{local, legacyFixtureNID + ":/tmp/payload.bin", "--home", f.home, "--nats-url", f.url, "--timeout", "10s"})
	cmd.SetOut(&strings.Builder{})
	cmd.SetErr(&strings.Builder{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // ctx-root: one bounded CLI invocation.
	defer cancel()
	return cmd.ExecuteContext(ctx)
}

func (f *legacyPushFixture) expectFailedFinalize(code string) {
	f.t.Helper()
	var tid string
	select {
	case tid = <-f.ids:
	default:
		f.t.Fatal("the legacy push never reached its prepare")
	}
	select {
	case fin := <-f.fins:
		if fin.Kind != "failed" || fin.TransferID != tid || fin.Tier != "b" || fin.Code != code {
			f.t.Fatalf("finalize = %+v, want kind=failed transfer_id=%s tier=b code=%s", fin, tid, code)
		}
	case <-time.After(5 * time.Second):
		f.t.Fatalf("the broker was left untold (no finalize{failed,%s} within 5s)", code)
	}
}

// TestPushTierBAbandonSendsFailedFinalize plays the broker from inside the test: prepare is answered
// OK, but no bucket is ever created, so the ctl's ObjectStore bind fails after prepare — exactly the
// "slot held, nobody to release it" shape. The ctl must then publish finalize{failed} for that
// transfer id before returning its error. Deleting the abandon() calls in legacyPush leaves the
// finalize subscription empty and this test red.
// origin: hot-update S7 plan §7 / §9 / P31; this fixture exercises the preselected cap0 compatibility path; it cannot certify a v1 terminal receipt.
func TestPushTierBAbandonSendsFailedFinalize(t *testing.T) {
	f := startLegacyPushFixture(t, false, nil)
	if err := f.push(); err == nil {
		t.Fatal("expected the bucket bind to fail (no bucket was created)")
	}
	f.expectFailedFinalize("bucket_bind_failed")
}

// TestPhaseTimeoutForDerivesFromSize pins the per-phase bound: the flag's own words ("derived from
// the file size") now hold — a small transfer gets the floor-plus-margin, the maximum size gets the
// old flat default, and an explicit --timeout wins whatever the size.
// origin: hot-update S7 plan §9 / P31 — the bound moved with the cap0 push into cli.LegacyPhaseTimeout.
func TestPhaseTimeoutForDerivesFromSize(t *testing.T) {
	small := cli.LegacyPhaseTimeout(false, 12_000_000, cliTransferTimeoutDefault)
	if want := proto.XferTimeoutTierBFloor + 2*time.Minute; small != want {
		t.Fatalf("12 MB phase timeout = %s, want floor+margin %s", small, want)
	}
	if small >= cliTransferTimeoutDefault {
		t.Fatalf("a 12 MB push must not inherit the 2 GiB worst case (%s)", cliTransferTimeoutDefault)
	}
	maxSize := cli.LegacyPhaseTimeout(false, proto.XferMaxBytes, cliTransferTimeoutDefault)
	if maxSize != cliTransferTimeoutDefault {
		t.Fatalf("max-size phase timeout = %s, want the flat default %s (never tighter than the broker)", maxSize, cliTransferTimeoutDefault)
	}
	explicit := cli.LegacyPhaseTimeout(true, 12_000_000, 90*time.Second)
	if explicit != 90*time.Second {
		t.Fatalf("an explicit --timeout must win, got %s", explicit)
	}
}

// TestPushTierBCommitLostStillSendsFailedFinalize: the upload succeeded but the push-commit
// request's reply was lost (nobody answers push-commit here): the ctl must still tell the broker it gave
// up — the broker arbitrates by its committed flag (verb_mismatch if it had forwarded, slot freed if it
// never saw the commit), so sending is always safe and not sending held the slot to the full budget.
// origin: hot-update S7 plan §7 / §9 / P31; this fixture exercises the preselected cap0 compatibility path; it cannot certify a v1 terminal receipt.
func TestPushTierBCommitLostStillSendsFailedFinalize(t *testing.T) {
	// push-commit has no subscriber at all — the request is lost, which is the lost-reply shape.
	f := startLegacyPushFixture(t, true, nil)
	if err := f.push(); err == nil {
		t.Fatal("expected the commit request to fail (no responder)")
	}
	f.expectFailedFinalize("commit_lost")
}

// TestPushTierBCommitRefusedSendsFailedFinalize is the third commit-path abandon: the broker answered
// push-commit with a refusal (tier_invalid, not_owner_or_creator …) INSTEAD of forwarding it. The entry
// is not committed, so the ctl must free the slot with finalize{failed, <the refusal code>} — round-2
// review R4-2-F9 deleted that abandon with every test green (only the lost-reply sibling was pinned).
// origin: simcluster-speed review round 2 R4-2-F9
// origin: hot-update S7 plan §7 / §9 / P31; this fixture exercises the preselected cap0 compatibility path; it cannot certify a v1 terminal receipt.
func TestPushTierBCommitRefusedSendsFailedFinalize(t *testing.T) {
	f := startLegacyPushFixture(t, true, func(m *nats.Msg) {
		body, _ := json.Marshal(proto.TransferCommitResp{OK: false, Code: "tier_invalid", Error: "entry downgraded to tier a"})
		_ = m.Respond(body)
	})
	if err := f.push(); err == nil || !strings.Contains(err.Error(), "tier_invalid") {
		t.Fatalf("expected the refusal to surface, got %v", err)
	}
	f.expectFailedFinalize("tier_invalid")
}

// TestLegacyPullFailureSendsFailedFinalize: a cap0 pull that fails after the
// broker tracked it tells the broker so (finalize{failed}), as the pre-S7 pull
// did, or the broker holds the entry and its object to the watchdog budget. A
// duplicate transfer ID was refused before insertion: finalizing it would end
// the ORIGINAL in-flight pull, so that refusal sends nothing.
// origin: hot-update S7 plan §7 / §9 / P31 (pre-S7 runPull / finishPullTierA/B abandon)
func TestLegacyPullFailureSendsFailedFinalize(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseLegacyPullFailure(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "legacy pull failure", g)
	testharness.AssertNoFDLeak(t, "legacy pull failure", fd, 0)
}

func exerciseLegacyPullFailure(t *testing.T) {
	t.Helper()
	for _, tc := range []struct {
		name, code string
		resp       proto.PullPrepareResp
		bucket     bool
	}{
		{name: "prepare-refused", code: "path_outside_roots", resp: proto.PullPrepareResp{OK: false, Code: "path_outside_roots"}},
		{name: "prepare-duplicate", code: "", resp: proto.PullPrepareResp{OK: false, Code: "transfer_id_in_flight"}},
		{name: "object-missing", code: "object_get_failed", bucket: true, resp: proto.PullPrepareResp{OK: true, Tier: "b", Size: 600 << 10, SHA256: strings.Repeat("a", 64)}},
		{name: "inline-digest-mismatch", code: "transfer_source_changed", resp: proto.PullPrepareResp{OK: true, Tier: "a", Size: 4, SHA256: strings.Repeat("a", 64), InlineData: []byte("data")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := startLegacyPullFixture(t, tc.resp, tc.bucket)
			if err := f.pull(filepath.Join(t.TempDir(), "file")); err == nil {
				t.Fatal("a failed legacy pull reported success")
			}
			if tc.code == "" {
				select {
				case fin := <-f.fins:
					t.Fatalf("a duplicate-ID refusal finalized the original transfer: %+v", fin)
				case <-time.After(500 * time.Millisecond):
				}
				return
			}
			f.expectFailedFinalizeTier(tc.code, tc.resp.Tier)
		})
	}
}

// startLegacyPullFixture plays a cap0 broker for the real pull command: caps
// answer completely without a transfer version, the pull prepare is answered
// with resp (bound to the session bucket for Tier B), and finalizes are recorded.
func startLegacyPullFixture(t *testing.T, resp proto.PullPrepareResp, bucket bool) *legacyPushFixture {
	t.Helper()
	f := &legacyPushFixture{t: t, url: startJetStreamNATS(t), home: t.TempDir(), fins: make(chan proto.TransferFinalize, 4), ids: make(chan string, 4)}
	writeReviewPsIdentity(t, f.home, legacyFixtureSID)
	id, err := cli.EnsureIdentity(f.home)
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(f.url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	if bucket {
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: proto.XferBucketName(legacyFixtureSID)}); err != nil {
			t.Fatal(err)
		}
	}
	respond := func(m *nats.Msg, v any) {
		body, _ := json.Marshal(v)
		_ = m.Respond(body)
	}
	for subject, handler := range map[string]nats.MsgHandler{
		proto.SubjCtrlCaps(id.PublicKey, legacyFixtureSID): func(m *nats.Msg) {
			respond(m, proto.CapsResp{OK: true, JetStreamReady: true, MaxPayload: nc.MaxPayload()})
		},
		proto.SubjCmdBy(legacyFixtureSID, id.PublicKey, legacyFixtureNID, "pull"): func(m *nats.Msg) {
			var req proto.PullPrepareReq
			_ = json.Unmarshal(m.Data, &req)
			f.ids <- req.TransferID
			reply := resp
			if reply.Tier == "b" {
				reply.Bucket, reply.ObjectKey = proto.XferBucketName(legacyFixtureSID), req.TransferID
			}
			respond(m, reply)
		},
		proto.SubjCtrlTransferFinalize(id.PublicKey, legacyFixtureSID, "*"): func(m *nats.Msg) {
			var fin proto.TransferFinalize
			_ = json.Unmarshal(m.Data, &fin)
			f.fins <- fin
			respond(m, proto.TransferFinalizeResp{OK: true})
		},
	} {
		sub, err := nc.Subscribe(subject, handler)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = sub.Unsubscribe() })
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *legacyPushFixture) pull(dst string) error {
	f.t.Helper()
	cmd := newPullCmd()
	cmd.SetArgs([]string{legacyFixtureNID + ":/remote/file", dst, "--home", f.home, "--nats-url", f.url, "--timeout", "10s"})
	cmd.SetOut(&strings.Builder{})
	cmd.SetErr(&strings.Builder{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // ctx-root: one bounded CLI invocation.
	defer cancel()
	return cmd.ExecuteContext(ctx)
}

// TestLegacyPullClosesItsPrivateTmpDescriptor lands five cap0 pulls with the
// garbage collector disabled, so a descriptor left to an *os.File finalizer
// stays visibly open: the private tmp the legacy owner writes through must be
// closed by the owner itself.
// origin: INTEG-D cmd/tether race suite (TestBothPullPathsCarryTheMode fd gate; GC-independent form)
func TestLegacyPullClosesItsPrivateTmpDescriptor(t *testing.T) {
	payload := []byte("landed through the legacy owner\n")
	sum := sha256.Sum256(payload)
	f := startLegacyPullFixture(t, proto.PullPrepareResp{OK: true, Tier: "a", Size: int64(len(payload)), SHA256: hex.EncodeToString(sum[:]), InlineData: payload}, false)
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	beforeG := runtime.NumGoroutine()
	before := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLegacyPullDescriptor(t, f, payload)
	}
	testharness.AssertNoGoroutineLeak(t, "legacy pull tmp descriptor x5", beforeG)
	testharness.AssertNoFDLeak(t, "legacy pull tmp descriptor x5", before, 0)
}

func exerciseLegacyPullDescriptor(t *testing.T, f *legacyPushFixture, payload []byte) {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "file")
	if err := f.pull(dst); err != nil {
		t.Fatal(err)
	}
	// Consume each round's evidence; otherwise the fifth request blocks the
	// fixture's four-slot channel and looks like a client timeout.
	var id string
	select {
	case id = <-f.ids:
	default:
		t.Fatal("pull never prepared")
	}
	select {
	case terminal := <-f.fins:
		if terminal.TransferID != id || terminal.Kind != "complete" {
			t.Fatalf("wrong terminal: %+v", terminal)
		}
	default:
		t.Fatal("pull completed without its legacy finalize")
	}
	if got, err := os.ReadFile(dst); err != nil || string(got) != string(payload) {
		t.Fatalf("legacy pull did not land the bytes: %v", err)
	}
}

func (f *legacyPushFixture) expectFailedFinalizeTier(want, tier string) {
	f.t.Helper()
	var tid string
	select {
	case tid = <-f.ids:
	default:
		f.t.Fatal("the legacy transfer never reached its prepare")
	}
	select {
	case fin := <-f.fins:
		if fin.Kind != "failed" || fin.TransferID != tid || fin.Tier != tier || fin.Code != want {
			f.t.Fatalf("finalize = %+v, want kind=failed transfer_id=%s tier=%q code=%s", fin, tid, tier, want)
		}
	case <-time.After(5 * time.Second):
		f.t.Fatalf("the broker was left untold (no finalize{failed,%s} within 5s)", want)
	}
}
