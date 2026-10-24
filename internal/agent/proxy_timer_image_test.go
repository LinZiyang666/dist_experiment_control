//go:build linux

package agent

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net/netip"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S5 plan §6.5 / §7.2 / §8
func TestProxyFiredCleanupSurvivesCut(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseProxyFiredCleanup(t)
	}
	testharness.AssertNoGoroutineLeak(t, "proxy fired cleanup cut", beforeG)
	testharness.AssertNoFDLeak(t, "proxy fired cleanup cut", beforeFD, 0)
	// This dependency test must remain red while P8/P10 are compile stubs.
	// It exercises actual frozen SS ownership rather than substituting a fake
	// Freeze, listener image or successful stub for the pending integration.
	t.Run("sealed_ss_cut", exerciseProxyTimerSealedCut)
}

func proxyTimerDirective() *proto.ProxyDirective {
	return &proto.ProxyDirective{Enabled: true, PublicPort: 14015, Token: "allocation", Generation: 5, Epoch: 7,
		Keys: []proto.ProxyKey{{SubID: "alice", Secret: "timer-material"}}}
}

func exerciseProxyFiredCleanup(t *testing.T) {
	t.Helper()
	a := newProxyTestAgent(t)
	defer stopProxyOnRunExit(a)
	a.cfg.ProxyFailClosedGrace = time.Hour
	a.applyProxyDirective(nil, proxyTimerDirective())
	a.armFailClosed()
	armed, err := snapshotProxyCut(a)
	if err != nil {
		t.Fatal(err)
	}
	if armed.timer.DeadlineBootNS <= 0 || armed.timer.Fired {
		t.Fatal("precondition: absolute BOOTTIME grace was not armed")
	}
	a.flcMu.Lock()
	serial := a.proxy.flcState.serial
	a.flcMu.Unlock()
	if !latchProxyFailClosed(a, serial) {
		t.Fatal("precondition: timer callback did not latch")
	}
	if a.flcTimer != nil {
		t.Fatal("precondition: fired callback still has a timer handle")
	}
	a.cancelFailClosed() // reconnect cannot erase already-fired work
	fired, err := snapshotProxyCut(a)
	if err != nil {
		t.Fatal(err)
	}
	if !fired.timer.Fired || !fired.timer.CleanupPending || fired.timer.DeadlineBootNS != armed.timer.DeadlineBootNS {
		t.Fatal("timer=nil lost Fired/CleanupPending or renewed the original deadline")
	}
	// The metadata-only replay uses a real listener for the cleanup oracle.
	// Actual descriptor adoption is exercised separately in sealed_ss_cut.
	b := newProxyTestAgent(t)
	defer stopProxyOnRunExit(b)
	if err := restoreProxyCut(b, fired); err != nil {
		t.Fatal(err)
	}
	if b.flcTimer != nil || b.proxy.srv != nil {
		t.Fatal("restore started callbacks or a listener before activation")
	}
	srv := ssproxy.New(b.cfg.Logger)
	lp, err := srv.Start(0, nil)
	if err != nil {
		t.Fatal(err)
	}
	b.proxy.mu.Lock()
	b.proxy.srv, b.proxy.localPort = srv, lp
	b.proxy.mu.Unlock()
	if err := activateProxyTimer(b); err != nil {
		t.Fatal(err)
	}
	if srv.Serving() || b.proxy.srv != nil || !b.proxy.needsReestablish {
		t.Fatal("restored fired cleanup did not retire the live SS owner")
	}
	completed, err := snapshotProxyCut(b)
	if err != nil {
		t.Fatal(err)
	}
	if !completed.timer.Fired || completed.timer.CleanupPending {
		t.Fatal("completed cleanup lost fired receipt or stayed pending")
	}
	finishProxyFailClosed(a, serial)
	if a.proxy.srv != nil {
		t.Fatal("source fired cleanup did not join original SS listener")
	}
	// A scheduled but unfired timer must retain its absolute deadline on import.
	scheduled := fired
	scheduled.timer.Fired, scheduled.timer.CleanupPending = false, false
	c := newProxyTestAgent(t)
	defer stopProxyOnRunExit(c)
	if err := restoreProxyCut(c, scheduled); err != nil {
		t.Fatal(err)
	}
	if err := activateProxyTimer(c); err != nil {
		t.Fatal(err)
	}
	active, err := snapshotProxyCut(c)
	if err != nil {
		t.Fatal(err)
	}
	if active.timer.DeadlineBootNS != armed.timer.DeadlineBootNS || c.flcTimer == nil {
		t.Fatal("import renewed fail-close grace")
	}
	c.cancelFailClosed()
	// Expiry during exec is replayed immediately, not granted another grace.
	scheduled.timer.DeadlineBootNS = 1
	d := newProxyTestAgent(t)
	defer stopProxyOnRunExit(d)
	if err := restoreProxyCut(d, scheduled); err != nil {
		t.Fatal(err)
	}
	if err := activateProxyTimer(d); err != nil {
		t.Fatal(err)
	}
	expired, err := snapshotProxyCut(d)
	if err != nil {
		t.Fatal(err)
	}
	if !expired.timer.Fired || expired.timer.CleanupPending || d.flcTimer != nil {
		t.Fatal("expired absolute deadline was renewed or left unprocessed")
	}
}

func exerciseProxyTimerSealedCut(t *testing.T) {
	t.Helper()
	a := newProxyTestAgent(t)
	defer stopProxyOnRunExit(a)
	a.applyProxyDirective(nil, proxyTimerDirective())
	a.cfg.ProxyFailClosedGrace = time.Hour
	a.armFailClosed()
	defer a.cancelFailClosed()
	frozen, err := a.proxy.srv.Freeze(time.Now().Add(3 * time.Second))
	if err != nil {
		t.Fatalf("SS freeze dependency: %v", err)
	}
	transferred := false
	defer func() {
		if transferred {
			if err := frozen.Discard(); err != nil {
				t.Error(err)
			}
			a.proxy.mu.Lock()
			a.proxy.srv = nil // only the imported owner may close the original OFD
			a.proxy.mu.Unlock()
		} else if err := frozen.Thaw(); err != nil {
			t.Error(err)
		}
	}()
	a.flcMu.Lock()
	serial := a.proxy.flcState.serial
	a.flcMu.Unlock()
	if !latchProxyFailClosed(a, serial) {
		t.Fatal("timer did not fire during frozen cut")
	}
	cut, err := snapshotProxyCut(a)
	if err != nil {
		t.Fatal(err)
	}
	im, sealed, err := frozen.Export()
	if err != nil {
		t.Fatalf("SS sealed export dependency: %v", err)
	}
	defer func() {
		if err := sealed.Close(); err != nil {
			t.Error(err)
		}
	}()
	if len(sealed.Resources) == 0 || im.ListenerRef == "" {
		t.Fatal("nonempty listener/sealed ownership was not exported")
	}
	source := handofftest.DupResources(t, sealed.Resources)
	b := newProxyTestAgent(t)
	defer stopProxyOnRunExit(b)
	if err := restoreProxyCut(b, cut); err != nil {
		t.Fatal(err)
	}
	restored, salvage, err := ssproxy.Restore(im, source)
	if err != nil {
		closeRefusedFlowResources(salvage)
		t.Fatalf("SS restore dependency: %v", err)
	}
	transferred = true
	b.proxy.mu.Lock()
	b.proxy.srv = restored
	b.proxy.mu.Unlock()
	if err := restored.ActivateRaw(); err != nil {
		t.Fatal(err)
	}
	if err := activateProxyTimer(b); err != nil {
		t.Fatal(err)
	}
	if restored.Serving() || b.proxy.srv != nil {
		t.Fatal("fired cleanup after sealed cut resurrected an SS listener")
	}
	// BOOTTIME remains available to prove this is the portable clock's Linux
	// implementation, not a wall-clock timestamp or a fake deadline supplier.
	if now, err := procrequest.BootNow(); err != nil || now <= 0 {
		t.Fatal("BOOTTIME identity is unavailable")
	}
}

// origin: hot-update S5 plan §6.5 / T38
func TestProxyFlowReservationKeepsOrdinaryExposeIndependent(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseProxyFlowReservation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "proxy flow reservation", beforeG)
	testharness.AssertNoFDLeak(t, "proxy flow reservation", beforeFD, 0)
}

func exerciseProxyFlowReservation(t *testing.T) {
	t.Helper()
	a := newProxyTestAgent(t)
	defer stopProxyOnRunExit(a)
	tuple := rawsocket.Tuple{Local: netip.MustParseAddrPort("127.0.0.1:23001"), Remote: netip.MustParseAddrPort("127.0.0.1:23002")}
	if err := expectProxyFlow(a, flow.ID{1}, tuple); err != nil {
		t.Fatal("disabled proxy blocked ordinary expose", err)
	}
	srv := ssproxy.New(a.cfg.Logger)
	defer srv.Stop()
	port, err := srv.Start(0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.SetFlowRevoke(func([16]byte, string) {}); err != nil {
		t.Fatal(err)
	}
	a.proxy.srv = srv
	tuple.Remote = netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port))
	if err := expectProxyFlow(a, flow.ID{2}, tuple); !errors.Is(err, ssproxy.ErrAuthorityPending) {
		t.Fatal("unbound proxy did not retain admission", err)
	}
	a.proxy.localPort = port
	ordinary := tuple
	ordinary.Remote = netip.AddrPortFrom(ordinary.Remote.Addr(), uint16(port%65535+1))
	if err := expectProxyFlow(a, flow.ID{3}, ordinary); err != nil {
		t.Fatal("ordinary expose entered SS binding", err)
	}
	if err := expectProxyFlow(a, flow.ID{3}, ordinary); err != nil {
		t.Fatal("ordinary expose consumed an SS reservation", err)
	}
	if err := expectProxyFlow(a, flow.ID{4}, tuple); err != nil {
		t.Fatal("matching proxy flow was not reserved", err)
	}
	if err := expectProxyFlow(a, flow.ID{4}, tuple); !errors.Is(err, ssproxy.ErrStateUnrepresentable) {
		t.Fatal("matching flow bypassed the one-shot ExpectFlow reservation", err)
	}
	// origin: hot-update S5 plan §4.1 / §9.1 / DRILL-RUN
	legacy := tuple
	legacy.Local = netip.MustParseAddrPort("127.0.0.1:23003")
	if err := expectProxyFlow(a, flow.ID{}, legacy); err != nil {
		t.Fatal("legacy tuple was not reserved", err)
	}
	if err := expectProxyFlow(a, flow.ID{}, legacy); !errors.Is(err, ssproxy.ErrStateUnrepresentable) {
		t.Fatal("legacy tuple reservation was reusable", err)
	}
	if err := srv.ExpectFlow([16]byte{}, legacy); !errors.Is(err, ssproxy.ErrStateUnrepresentable) {
		t.Fatal("v1 accepted a zero FlowID", err)
	}
}

// origin: hot-update S5 plan §6.6 / §7.2 / T33
func TestProxyCutMetadataRestoresBeforeCallbacks(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("pristine", func(t *testing.T) { exerciseProxyCutMetadata(t, false) })
		t.Run("activation", func(t *testing.T) { exerciseProxyCutMetadata(t, true) })
	}
	testharness.AssertNoGoroutineLeak(t, "proxy sealed metadata", beforeG)
	testharness.AssertNoFDLeak(t, "proxy sealed metadata", beforeFD, 0)
}

func exerciseProxyCutMetadata(t *testing.T, activate bool) {
	t.Helper()
	a := newBootAgent(t, "nats://127.0.0.1:1", "node")
	cut := freezeBootAgent(t, a)
	a.proxy.gen, a.proxy.epoch, a.proxy.publicPort, a.proxy.localPort = 5, 7, 14015, 24015
	a.proxy.token, a.proxy.homeAddr, a.proxy.homeEpoch = "private-token-fixture", "private-home-fixture", 9
	a.proxy.digestApplied, a.proxy.keysetDigest = true, [32]byte{1, 2, 3}
	a.proxyDispatchN, a.lastAppliedPushSeq = 17, 13
	a.proxy.flcState = proxyFailClosedState{DeadlineBootNS: 1, Fired: true, CleanupPending: true}
	state, err := snapshotProxyCutUntil(a, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	data, err := encodeProxyCut(state)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(data)
	if bytes.HasPrefix(data, []byte("TPRX")) {
		t.Fatal("new proxy cut bypassed the declared private codec")
	}
	for end := 0; end < len(data); end++ {
		if _, err := decodeProxyCut(data[:end]); err == nil {
			t.Fatal("truncated current metadata accepted")
		}
	}
	if _, err := decodeProxyCut(append(bytes.Clone(data), 0)); err == nil {
		t.Fatal("trailing current metadata accepted")
	}
	retained := state
	retained.publicPort, retained.localPort, retained.token = 0, 0, ""
	retained.needsReestablish = true
	retainedData, err := encodeProxyCut(retained)
	if err != nil {
		t.Fatal(err)
	}
	retainedRoundTrip, err := decodeProxyCut(retainedData)
	clear(retainedData)
	if err != nil || retainedRoundTrip.homeEpoch != retained.homeEpoch || retainedRoundTrip.homeAddr != retained.homeAddr || !retainedRoundTrip.needsReestablish {
		t.Fatal("metadata-only cut lost retained home authority")
	}
	// A parser that rejects every old envelope would satisfy the corruption
	// checks below. Preserve the full v2 metadata and its nonzero timer latch.
	// origin: hot-update S5 plan §7.2 / T33 — integration budget extraction.
	legacyState := state
	legacyState.plannedDeadlineBootNS, legacyState.timer.serial = 23, 0
	// The prior layout predates the allocation identity fields.
	legacyState.allocationSID, legacyState.allocationNID = "", ""
	legacyData, err := encodeLegacyProxyCut(legacyState)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := decodeProxyCut(legacyData); err != nil || got != legacyState {
		t.Fatal("legacy v2 metadata changed during domain decoding")
	}
	for end := 0; end < len(legacyData); end++ {
		if _, err := decodeProxyCut(legacyData[:end]); err == nil {
			t.Fatal("truncated metadata accepted")
		}
	}
	if _, err := decodeProxyCut(append(bytes.Clone(legacyData), 0)); err == nil {
		t.Fatal("trailing metadata accepted")
	}
	wrongEnvelope := bytes.Clone(legacyData)
	wrongEnvelope[0] ^= 1
	if _, err := decodeProxyCut(wrongEnvelope); err == nil {
		t.Fatal("foreign private metadata envelope accepted")
	}
	for _, tc := range []struct {
		name   string
		offset int
		value  uint64
	}{
		{"negative_dispatch", 6, ^uint64(0)}, {"negative_applied", 14, ^uint64(0)}, {"applied_ahead", 14, 18},
		{"negative_generation", 22, ^uint64(0)}, {"negative_epoch", 30, ^uint64(0)},
		{"public_port_overflow", 38, 65536}, {"local_port_overflow", 46, 65536},
		{"negative_home_epoch", 54, ^uint64(0)}, {"negative_deadline", 62, ^uint64(0)},
	} {
		bad := bytes.Clone(legacyData)
		binary.BigEndian.PutUint64(bad[tc.offset:tc.offset+8], tc.value)
		if _, err := decodeProxyCut(bad); err == nil {
			t.Fatalf("invalid %s metadata accepted", tc.name)
		}
	}
	badFlag := bytes.Clone(legacyData)
	badFlag[102] = 2
	if _, err := decodeProxyCut(badFlag); err == nil {
		t.Fatal("nonboolean metadata accepted")
	}
	unfired := bytes.Clone(legacyData)
	unfired[106] = 0
	if _, err := decodeProxyCut(unfired); err == nil {
		t.Fatal("unfired pending cleanup accepted")
	}
	badSize := binary.BigEndian.AppendUint16(bytes.Clone(legacyData[:108]), 4097)
	badSize = append(badSize, bytes.Repeat([]byte{'x'}, 4097)...)
	badSize = append(badSize, legacyData[110+len(state.token):]...)
	if _, err := decodeProxyCut(badSize); err == nil {
		t.Fatal("oversized secret metadata accepted")
	}
	negative := state
	negative.dispatchSequence = -1
	if _, err := encodeProxyCut(negative); err == nil {
		t.Fatal("invalid ordering exported")
	}
	oversized := state
	oversized.token = string(bytes.Repeat([]byte{'x'}, 4097))
	if _, err := encodeProxyCut(oversized); err == nil {
		t.Fatal("oversized private token exported")
	}

	// A late timer latch and apply sequence must reach the real Prepared
	// through the production wrapper, rather than a separately packed fixture.
	wrapper := &imageCut{frozen: cut, readers: []agentimage.Capabilities{{ABI: 1, Domains: []string{"exec-v1", "pty-v1"}}}}
	if prepared, err := prepareImageCut(a, wrapper, nil); !errors.Is(err, errImageABIUnsupported) || prepared != nil {
		if prepared != nil {
			_ = prepared.Close()
		}
		t.Fatal("retained proxy metadata reached an old final reader")
	}
	wrapper.readers = []agentimage.Capabilities{agentimage.CurrentCapabilities("fixture")}
	prepared, err := prepareImageCut(a, wrapper, nil)
	defer func() {
		if wrapper.private != nil {
			_ = wrapper.private.Close()
		}
	}()
	if err != nil {
		t.Fatal(err)
	}
	publicState, dispatch := bootPreparedState(t)
	if bytes.Contains(publicState.Data, []byte(state.token)) || bytes.Contains(publicState.Data, []byte(state.homeAddr)) {
		t.Fatal("proxy metadata secret entered public Bundle JSON")
	}
	t.Setenv("_TETHER_HANDOFF", dispatch)
	agentboot.Reset()
	image := agentboot.Begin(testharness.SilentLog())
	if image == nil || image.Phase() != agentboot.PhaseHeld {
		t.Fatal("metadata dispatch was not held")
	}
	defer agentboot.Reset()
	// Prepare's duplicates belong to the receiver after this modeled exec.
	// Close only the wrapper's original auxiliary memfd at the end below.
	_ = prepared
	b, err := New(Config{NATSURL: "nats://127.0.0.1:1", SID: "lab", NID: "node", Home: t.TempDir(), BootImage: image,
		RemoteFSMode: "off", Logger: testharness.SilentLog()})
	if err != nil {
		t.Fatal(err)
	}
	cleanupBootAgent(t, b)
	got, err := snapshotProxyCut(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.dispatchSequence != 17 || got.lastAppliedSequence != 13 || got.generation != 5 || got.epoch != 7 ||
		got.keysetDigest != state.keysetDigest || got.token != state.token || got.homeAddr != state.homeAddr ||
		got.publicPort != 14015 || got.localPort != 24015 || got.homeEpoch != 9 || !got.digestApplied ||
		!got.timer.Fired || !got.timer.CleanupPending || got.timer.DeadlineBootNS != 1 || b.flcTimer != nil {
		t.Fatal("constructor lost private ordering/allocation/fired cleanup metadata")
	}
	if activate {
		if err := cut.Discard(); err != nil {
			t.Fatal(err)
		}
		if err := activateImage(b); err != nil {
			t.Fatal(err)
		}
		got, err := snapshotProxyCut(b)
		if err != nil || !got.timer.Fired || got.timer.CleanupPending || b.flcTimer != nil {
			t.Fatal("activation failed to finish the carried fired cleanup")
		}
		if _, err := image.ReadSealed(proxyStateRef, proxyStateLimit); err == nil {
			t.Fatal("active ownership accepted pristine private metadata")
		}
		return
	}
	// Pristine repack must retain the taken auxiliary file, while activation
	// closes it. This catches both early close and boot-time descriptor leaks.
	prepared, err = image.Repack()
	if err != nil {
		t.Fatal(err)
	}
	_, dispatch = bootPreparedState(t)
	t.Setenv("_TETHER_HANDOFF", dispatch)
	agentboot.Reset()
	successor := agentboot.Begin(testharness.SilentLog())
	if successor == nil || successor.Phase() != agentboot.PhaseHeld {
		t.Fatal("pristine metadata re-dispatch was not held")
	}
	carried, err := successor.ReadSealed(proxyStateRef, proxyStateLimit)
	if err != nil || !bytes.Equal(carried, data) {
		clear(carried)
		t.Fatal("pristine re-dispatch lost taken private metadata")
	}
	clear(carried)
	// The second modeled exec takes Prepared's descriptors again. Its held
	// image must close them, while the prior image retains only its originals.
	_ = prepared
	successor.Abandon(nil, agentboot.CrashWindowNoRollbackTarget, "test")
	image.Abandon(nil, agentboot.CrashWindowNoRollbackTarget, "test")
	if err := cut.Discard(); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S5 internal review PRIVATE-STATE-2
func TestProxyCutMatchesSealedSSAuthorityBeforeCallbacks(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseProxySealedAuthority(t)
	}
	testharness.AssertNoGoroutineLeak(t, "proxy descriptor agreement", g0)
	testharness.AssertNoFDLeak(t, "proxy descriptor agreement", fd0, 0)
}

func exerciseProxySealedAuthority(t *testing.T) {
	t.Helper()
	a := newProxyTestAgent(t)
	defer stopProxyOnRunExit(a)
	a.applyProxyDirective(nil, proxyTimerDirective())
	a.proxyDrainMu.Lock()
	a.proxyDispatchN, a.lastAppliedPushSeq = 17, 13
	a.proxyDrainMu.Unlock()
	a.cfg.ProxyFailClosedGrace = time.Hour
	a.armFailClosed()
	a.flcMu.Lock()
	serial := a.proxy.flcState.serial
	a.flcMu.Unlock()
	if !latchProxyFailClosed(a, serial) {
		t.Fatal("timer latch precondition")
	}
	state, err := snapshotProxyCut(a)
	if err != nil {
		t.Fatal(err)
	}
	f, err := a.proxy.srv.Freeze(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Discard(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	cut, err := f.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	var sequence, applied, deadline uint64
	var allocationSID, allocationNID string
	var port uint64
	var fired, pending bool
	for _, rec := range cut.Records {
		for _, field := range rec.Fields {
			if rec.Kind == "authority" {
				switch field.Name {
				case "dispatch_sequence":
					sequence = field.U64
				case "last_applied_sequence":
					applied = field.U64
				case "allocation_sid":
					allocationSID = string(field.Bytes)
				case "allocation_nid":
					allocationNID = string(field.Bytes)
				case "allocation_port":
					port = field.U64
				}
			}
			if rec.Kind == "timer" {
				switch field.Name {
				case "fail_close_deadline_boot_ns":
					deadline = field.U64
				case "fired":
					fired = field.U64 != 0
				case "cleanup_pending":
					pending = field.U64 != 0
				}
			}
		}
	}
	if sequence != 17 || applied != 13 || allocationSID != a.cfg.SID || allocationNID != nidOf(a) || port != uint64(state.publicPort) || deadline != uint64(state.timer.DeadlineBootNS) || !fired || !pending {
		t.Fatal("SS descriptor lost control ordering, allocation or fired cleanup")
	}
	im, snapshot, err := f.Export()
	if err != nil {
		t.Fatal(err)
	}
	next, salvage, err := ssproxy.Restore(im, handofftest.DupResources(t, snapshot.Resources))
	if err != nil {
		for _, resource := range salvage {
			for _, file := range resource.Files {
				_ = file.Close()
			}
		}
		t.Fatal(err)
	}
	defer next.Stop()
	if err := ssproxy.ValidateProxyState(next, proxySSState(a, state)); err != nil {
		t.Fatal(err)
	}
	b := newProxyTestAgent(t)
	defer stopProxyOnRunExit(b)
	if err := restoreProxyCut(b, state); err != nil {
		t.Fatal(err)
	}
	if err := wireProxyFlowRevoke(b, next); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*proxyCutState){
		func(p *proxyCutState) { p.dispatchSequence++ },
		func(p *proxyCutState) { p.lastAppliedSequence++ },
		func(p *proxyCutState) { p.generation++ },
		func(p *proxyCutState) { p.epoch++ },
		func(p *proxyCutState) { p.keysetDigest[0] ^= 1 },
		func(p *proxyCutState) { p.publicPort++ },
		func(p *proxyCutState) { p.token += "changed" },
		func(p *proxyCutState) { p.homeEpoch++ },
		func(p *proxyCutState) { p.authoritativelyOff = true },
		func(p *proxyCutState) { p.timer.DeadlineBootNS++ },
		func(p *proxyCutState) { p.plannedDeadlineBootNS++ },
		func(p *proxyCutState) { p.timer.Fired, p.timer.CleanupPending = false, false },
		func(p *proxyCutState) { p.timer.CleanupPending = false },
	} {
		bad := state
		mutate(&bad)
		if ssproxy.ValidateProxyState(next, proxySSState(a, bad)) == nil {
			t.Fatal("inconsistent private envelopes accepted before callbacks")
		}
	}
	b.proxyDrainMu.Lock()
	b.proxyDispatchN++
	b.proxyDrainMu.Unlock()
	if wireProxyFlowRevoke(b, next) == nil {
		t.Fatal("callback installed before private authority agreement")
	}
}

// Read-only compatibility fixtures preserve the exact historic writer layout.
// origin: hot-update S5 internal review PRIVATE-STATE-2
func encodeLegacyProxyCut(st proxyCutState) ([]byte, error) {
	if !validProxyCut(st) || len(st.token) > 4096 || len(st.homeAddr) > 4096 {
		return nil, ssproxy.ErrStateUnrepresentable
	}
	data := []byte("TPRX\x00\x02")
	for _, value := range []int64{st.dispatchSequence, st.lastAppliedSequence, st.generation, st.epoch,
		int64(st.publicPort), int64(st.localPort), st.homeEpoch, st.timer.DeadlineBootNS} {
		data = binary.BigEndian.AppendUint64(data, uint64(value))
	}
	data = append(data, st.keysetDigest[:]...)
	for _, flag := range []bool{st.digestApplied, st.tunnelUp, st.needsReestablish, st.authoritativelyOff,
		st.timer.Fired, st.timer.CleanupPending} {
		if flag {
			data = append(data, 1)
		} else {
			data = append(data, 0)
		}
	}
	for _, value := range []string{st.token, st.homeAddr} {
		data = binary.BigEndian.AppendUint16(data, uint16(len(value)))
		data = append(data, value...)
	}
	data = binary.BigEndian.AppendUint64(data, uint64(st.plannedDeadlineBootNS))
	return data, nil
}
