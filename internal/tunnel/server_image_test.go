package tunnel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

func serverImageTestDeadline(t *testing.T) controlbudget.Deadline {
	t.Helper()
	now, err := rawsocket.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	return controlbudget.Deadline(now + int64(5*time.Second))
}
func serverImageTestConfig() FlowConfig {
	return FlowConfig{Budget: &workload.ReplayBudget{}, BootID: "original-boot", InstanceID: "original-instance", OwnerGen: 7, TokenLookup: func(AuthRequest) error { return nil }, Authorize: func(req AuthRequest) (Authority, error) {
		return Authority{Enabled: true, Allocation: req.Allocation}, nil
	}}
}
func serverImageTestServer(t *testing.T) *Server {
	t.Helper()
	s := newServer("", "127.0.0.1", nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := s.SetFlowConfig(serverImageTestConfig()); err != nil {
		t.Fatal(err)
	}
	return s
}
func serverImageTestEndpoint(s *Server, port int, id byte) *flowEndpoint {
	e := &flowEndpoint{id: flow.EndpointID{id}, allocation: Allocation{Port: port, SID: "session", NID: "agent", TokenHash: "original-token", Epoch: 4}, phase: EndpointPhaseClosing, ownerGen: 8, carrierGen: 13, targetPort: 22, ticket: []byte("secret-original-ticket"), nextSeq: 19, stop: make(chan struct{}), done: make(chan struct{}), createdAt: time.Now(), lineage: [32]byte{9}}
	e.permit = &flowPermit{owner: 7, next: 8, requestID: [16]byte{3}, cutID: [16]byte{4}, consumed: true, deadline: time.Now().Add(PermitTTL), digest: [32]byte{6}}
	req := resumeRequest{Scope: flow.ResumeScopeEndpoint, Mode: flow.ResumeModeActivate, RequestID: [16]byte{3}, Endpoint: e.id, Owner: 7, Next: 8, Cut: [16]byte{4}, Target: 22, Ticket: e.ticket}
	resp := resumeResponse{RequestID: req.RequestID, Endpoint: e.id, Owner: 8, Carrier: 13, Verdict: flow.ResumeVerdict(1), Ticket: e.ticket, Pages: 1, Retirement: flow.RetirementImage{EndpointID: e.id, NextOpenSeq: 19, RetiredThrough: 2, Holes: []uint64{5}}}
	resp.GrantDigest = responseDigest(resp)
	e.receipt = &resumeReceipt{request: req, response: resp}
	close(e.done)
	s.flowState.endpoints[port] = e
	return e
}

// All fence maps and retained owner references survive runtime -> private bytes
// -> import. Closing owners count even with no active carrier or flow socket.
// origin: hot-update S6 plan §2.2 / T24
func TestBrokerEndpointFenceRoundTrip(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux image owner")
	}
	s := serverImageTestServer(t)
	defer closeServerTestResource(t, s.flowState.manager)
	a := serverImageTestEndpoint(s, 30101, 1)
	b := serverImageTestEndpoint(s, 30102, 2)
	s.killGen[a.allocation.Port] = 4
	s.killGenSession[a.allocation.SID] = 5
	s.killGenAllocation[allocationKey(a.allocation)] = 6
	s.forgotten[a.allocation.SID] = true
	s.closedAllocation[allocationKey(a.allocation)] = true
	s.inflightBySID[a.allocation.SID] = 3
	s.inflightByAllocation[allocationKey(a.allocation)] = 2
	a.fence = fenceSnap{port: 4, sess: 5, alloc: 6}
	a.fired, a.cleanupPending = true, true
	now, err := rawsocket.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	a.detachBootNS = uint64(now + int64(3*time.Second))
	a.detachDeadline = time.Now().Add(3 * time.Second)
	s.flowState.waiters = []*flowEndpoint{b, a}
	s.flowState.pending <- struct{}{}
	s.flowState.pending <- struct{}{}
	snap, err := snapshot(s, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := encodeServerCut(sealedstate.CutState{Records: snap.Records})
	if err != nil {
		t.Fatal(err)
	}
	cut, err := decodeServerCut(data)
	if err != nil {
		t.Fatal(err)
	}
	restored := serverImageTestServer(t)
	defer closeServerTestResource(t, restored.flowState.manager)
	if err = restoreServerRecords(restored, cut); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"killGen", "killGenSession", "killGenAllocation", "forgotten", "closedAllocation", "inflightBySID", "inflightByAllocation"} {
		old, new := reflect.ValueOf(s).Elem().FieldByName(name), reflect.ValueOf(restored).Elem().FieldByName(name)
		if !reflect.DeepEqual(serverImageReflectMap(old), serverImageReflectMap(new)) {
			t.Fatalf("fence map %s changed", name)
		}
	}
	restored.mu.Lock()
	restored.inflightBySID[a.allocation.SID] = 0
	restored.inflightByAllocation[allocationKey(a.allocation)] = 0
	restored.maybePruneSessionLocked(a.allocation.SID)
	restored.maybePruneAllocationLocked(allocationKey(a.allocation))
	retained := restored.forgotten[a.allocation.SID] && restored.closedAllocation[allocationKey(a.allocation)]
	restored.mu.Unlock()
	if !retained {
		t.Fatal("closing owner reference lost its fences")
	}
	for _, old := range []*flowEndpoint{a, b} {
		new := restored.flowState.endpoints[old.allocation.Port]
		if new == nil || old.allocation != new.allocation || old.id != new.id || old.fence != new.fence || old.phase != new.phase || old.ownerGen != new.ownerGen || old.carrierGen != new.carrierGen || old.nextSeq != new.nextSeq || old.targetPort != new.targetPort || old.lineage != new.lineage || !bytes.Equal(old.ticket, new.ticket) || new.permit == nil || !new.permit.consumed || new.permit.abort || old.permit.digest != new.permit.digest || new.receipt == nil || !bytes.Equal(encodeResumeRequest(old.receipt.request), encodeResumeRequest(new.receipt.request)) || !bytes.Equal(encodeResumeResponse(old.receipt.response), encodeResumeResponse(new.receipt.response)) {
			t.Fatal("retained endpoint/permit/receipt lost")
		}
	}
	if current := restored.flowState.endpoints[a.allocation.Port]; !current.fired || !current.cleanupPending || current.detachBootNS != a.detachBootNS {
		t.Fatal("import lost fired cleanup or renewed the original BOOTTIME deadline")
	}
	if !reflect.DeepEqual(s.flowState.ticketKey, restored.flowState.ticketKey) || len(restored.flowState.pending) != 2 || len(restored.flowState.waiters) != 2 || restored.flowState.waiters[0].id != b.id || restored.flowState.waiters[1].id != a.id {
		t.Fatal("issuer or pending FIFO/slots changed")
	}
	for _, r := range cut.Records {
		for i := range r.Fields {
			bad := sealedstate.CutState{Records: append([]sealedstate.Record(nil), cut.Records...)}
			for n := range bad.Records {
				if bad.Records[n].Kind == r.Kind && bytes.Equal(clientFields(bad.Records[n])["id"].Bytes, clientFields(r)["id"].Bytes) {
					bad.Records[n].Fields = append(append([]sealedstate.Field(nil), r.Fields[:i]...), r.Fields[i+1:]...)
				}
			}
			if _, err := encodeServerCut(bad); err == nil {
				t.Fatalf("omitted %s.%s accepted", r.Kind, r.Fields[i].Name)
			}
		}
	}
	for n := 0; n < 139; n++ {
		if _, err := serverReceiptResponse(make([]byte, n)); err == nil {
			t.Fatalf("malformed receipt length %d accepted", n)
		}
	}
	if _, err := decodeServerCut(append(data, 1)); err == nil {
		t.Fatal("trailing bytes accepted")
	}
	for n := 0; n < len(data); n++ {
		if _, err := decodeServerCut(data[:n]); err == nil {
			t.Fatalf("truncation %d accepted", n)
		}
	}
}
func serverImageReflectMap(v reflect.Value) []string {
	var result []string
	iter := v.MapRange()
	for iter.Next() {
		result = append(result, fmt.Sprint(iter.Key())+"="+fmt.Sprint(iter.Value()))
	}
	sort.Strings(result)
	return result
}

// An old allocation's close must retain a successor using the same port.
// The endpoint reference is checked after import, not only before export.
// origin: hot-update S6 plan §6.2 / T26
func TestBrokerLateCloseDoesNotKillReplacement(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux image owner")
	}
	for _, dimension := range []string{"token", "epoch"} {
		t.Run(dimension, func(t *testing.T) {
			s := serverImageTestServer(t)
			defer closeServerTestResource(t, s.flowState.manager)
			e := serverImageTestEndpoint(s, 30103, 2)
			e.allocation.TokenHash = "replacement-token"
			e.phase = EndpointPhaseDetached
			old := SessionInfo{PublicPort: e.allocation.Port, SID: e.allocation.SID, NID: e.allocation.NID, TokenHash: "retired-token", Epoch: 3}
			if dimension == "epoch" {
				old.TokenHash = e.allocation.TokenHash
			}
			snap, err := snapshot(s, nil)
			if err != nil {
				t.Fatal(err)
			}
			data, err := encodeServerCut(sealedstate.CutState{Records: snap.Records})
			if err != nil {
				t.Fatal(err)
			}
			cut, err := decodeServerCut(data)
			if err != nil {
				t.Fatal(err)
			}
			s2 := serverImageTestServer(t)
			defer closeServerTestResource(t, s2.flowState.manager)
			if err = restoreServerRecords(s2, cut); err != nil {
				t.Fatal(err)
			}
			if s2.CloseProxyIf(old) || s2.flowState.endpoints[e.allocation.Port] == nil || !endpointLiveLocked(s2, s2.flowState.endpoints[e.allocation.Port]) {
				t.Fatal("late allocation A close killed replacement B")
			}
		})
	}
	t.Run("legacy-epoch", func(t *testing.T) {
		s := serverImageTestServer(t)
		defer closeServerTestResource(t, s.flowState.manager)
		info := SessionInfo{PublicPort: 30104, SID: "session", NID: "agent", TokenHash: "same-token", Epoch: 3}
		ctx, cancel := context.WithCancel(context.Background()) // ctx-root: legacy identity fixture owns its cancellation.
		defer cancel()
		sess := &serverSession{publicPort: info.PublicPort, sid: info.SID, nid: info.NID, tokenHash: info.TokenHash, epoch: 4, cancel: cancel}
		s.sessions[info.PublicPort] = sess
		if s.CloseProxyIf(info) || s.sessions[info.PublicPort] != sess || ctx.Err() != nil || s.closedAllocation[sessionFenceKeyFromInfo(info)] {
			t.Fatal("old epoch close poisoned the current legacy session")
		}
	})

}

// Private-field inventory includes the actual maps and Endpoint cursor rather
// than accepting an opaque ServerImage state reference as schema coverage.
// origin: hot-update S6 plan §2.2 / T24
func TestBrokerTunnelPrivateDescriptorClosure(t *testing.T) {
	want := map[string][]string{
		"Server":          {"flowState", "importedImage", "addr", "publicHost", "tokenLookup", "logger", "tlsCert", "mu", "sessions", "killGen", "killGenAllocation", "killGenSession", "inflightBySID", "forgotten", "inflightByAllocation", "closedAllocation", "closed", "ln", "wg", "imagePaused", "retiringImage", "imageRevoke", "legacyWorkers", "controlKeeper", "controlRaw", "runCtx", "acceptDone", "lifetimeDone", "lifetimeOnce", "controlConns", "monitors", "callbacks", "regLogMu", "regLog", "regLogNow", "hsMu", "hsPerIP"},
		"serverFlowState": {"resumeBytes", "controlBytes", "cfg", "endpoints", "manager", "ticketKey", "pending", "waiters"},
		"flowEndpoint":    {"id", "allocation", "fence", "phase", "listener", "ownerGen", "carrierGen", "targetPort", "ticket", "attachment", "detachDeadline", "timer", "permit", "receipt", "stop", "done", "closeOnce", "pendingRaw", "lineage", "createdAt", "nextSeq", "paused", "pause", "keeper", "timerWork", "fired", "cleanupPending", "detachBootNS"},
		"flowPermit":      {"requestID", "cutID", "owner", "next", "deadline", "digest", "consumed", "abort"}, "resumeReceipt": {"request", "response"},
		"ResumeRequest":  {"Scope", "Mode", "RequestID", "Endpoint", "Owner", "Next", "Cut", "Digest", "Target", "Ticket", "Entries"},
		"ResumeResponse": {"RequestID", "Endpoint", "Owner", "Carrier", "Digest", "GrantDigest", "Verdict", "Reason", "Ticket", "Page", "Pages", "Retirement", "Entries", "Authority"},
		"Allocation":     {"SID", "NID", "Port", "TokenHash", "Epoch"},
		"FlowConfig":     {"Budget", "InstanceID", "OwnerGen", "BootID", "MaxEndpoints", "MaxPendingFDs", "TokenLookup", "Authorize", "BeforeLocalConnect"},
		"fenceSnap":      {"port", "sess", "alloc"}, "sessionFenceKey": {"publicPort", "sid", "nid", "tokenHash"},
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[Server](), reflect.TypeFor[serverFlowState](), reflect.TypeFor[flowEndpoint](), reflect.TypeFor[flowPermit](), reflect.TypeFor[resumeReceipt](), reflect.TypeFor[resumeRequest](), reflect.TypeFor[resumeResponse](), reflect.TypeFor[Allocation](), reflect.TypeFor[FlowConfig](), reflect.TypeFor[fenceSnap](), reflect.TypeFor[sessionFenceKey]()} {
		var got []string
		for i := 0; i < typ.NumField(); i++ {
			got = append(got, typ.Field(i).Name)
		}
		if !reflect.DeepEqual(got, want[typ.Name()]) {
			t.Fatalf("runtime %s field inventory drift: %v", typ.Name(), got)
		}
	}
	for _, d := range serverPrivateDescriptors {
		if d.Version != 1 || d.Owner != "P07" || d.StateRecord != "broker-tunnel-state-v1" || d.CompatibleReader != "broker-image-v1" || len(d.Fields) == 0 {
			t.Fatal("descriptor partition drift")
		}
	}
}

type serverImageTestSource struct {
	files         map[string]*os.File
	failAt, takes int
}

func (s *serverImageTestSource) TakeFile(key string) (*os.File, error) {
	s.takes++
	f := s.files[key]
	delete(s.files, key)
	if s.takes == s.failAt {
		return f, errors.New("injected take failure")
	}
	return f, nil
}
func serverImageCloseResources(resources []handoff.Resource) {
	for _, r := range resources {
		for _, f := range r.Files {
			if f != nil {
				_ = f.Close()
			}
		}
	}
}

func exerciseServerPartialImport(t *testing.T) {
	t.Helper()
	exerciseServerReexport(t)
	for failAt := 1; failAt <= 5; failAt++ {
		s := serverImageTestServer(t)
		ln, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		e := serverImageTestEndpoint(s, port, 1)
		e.phase = EndpointPhaseDetached
		k, err := listenerimage.Capture(ln, endpointListenerRef(e.id))
		if err != nil {
			t.Fatal(err)
		}
		e.listener = k.View()
		if err = k.Pause(serverImageTestDeadline(t)); err != nil {
			t.Fatal(err)
		}
		_, res, err := k.Export()
		if err != nil {
			t.Fatal(err)
		}
		target, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		peer, err := net.Dial("tcp4", target.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		accepted, err := target.Accept()
		if err != nil {
			t.Fatal(err)
		}
		_ = target.Close()
		raw, err := rawsocket.TakeTCP(accepted.(*net.TCPConn))
		if err != nil {
			t.Fatal(err)
		}
		if err = rawsocket.Hold(raw); err != nil {
			t.Fatal(err)
		}
		e.pendingRaw = raw
		rawFile, err := raw.ExportFile()
		if err != nil {
			t.Fatal(err)
		}
		res.Files["raw"] = rawFile
		s.flowState.waiters = []*flowEndpoint{e}
		s.flowState.pending <- struct{}{}
		state, err := snapshot(s, []handoff.Resource{res})
		if err != nil {
			t.Fatal(err)
		}
		frozen, err := s.flowState.manager.Freeze(time.Now().Add(5 * time.Second))
		if err != nil {
			t.Fatal(err)
		}
		flows, fs, err := frozen.Export()
		if err != nil {
			t.Fatal(err)
		}
		cut := sealedstate.CutState{Records: state.Records, Resources: state.Resources}
		if err = bindServerFlowImage(&cut, flows); err != nil {
			t.Fatal(err)
		}
		im, err := serverImageFromCut(cut, flows, "broker-tunnel-state.00000000000000000000000000000001")
		if err != nil {
			t.Fatal(err)
		}
		for i := range im.Endpoints {
			im.Endpoints[i].StateRef = im.StateResource
		}
		data, err := encodeServerCut(cut)
		if err != nil {
			t.Fatal(err)
		}
		ss, err := sealedstate.New(im.StateResource, data)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = k.Detach(); err != nil {
			t.Fatal(err)
		}
		src := &serverImageTestSource{files: map[string]*os.File{}, failAt: failAt}
		resources, err := handoff.MergeResources(state.Resources, fs.Resources, ss.Resources)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range resources {
			for name, f := range r.Files {
				src.files[r.ID+"/"+name] = f
			}
		}
		cfg := ServerImportConfig{PublicHost: "127.0.0.1", Flow: serverImageTestConfig()}
		if failAt == 5 {
			bad := im
			bad.Flows.StateRef = "flow-state.00000000000000000000000000000099"
			probe := &serverImageTestSource{files: map[string]*os.File{}}
			for key, f := range src.files {
				probe.files[key] = f
			}
			owner, held, err := ImportServer(cfg, bad, probe)
			if err == nil || owner != nil || probe.takes != 1 || len(held.Resources) != 1 {
				t.Fatal("outer flow projection mismatch took business files")
			}
		}
		imported, salvage, err := ImportServer(cfg, im, src)
		if failAt <= 4 {
			if err == nil || imported != nil {
				t.Fatal("partial Take accepted")
			}
			var taken int
			for _, r := range salvage.Resources {
				taken += len(r.Files)
			}
			if taken != failAt || taken+len(src.files) != 4 {
				t.Fatalf("salvage conservation: taken=%d remaining=%d", taken, len(src.files))
			}
			for _, r := range salvage.Resources {
				for _, f := range r.Files {
					if _, e := f.Stat(); e != nil {
						t.Fatal("pristine salvage file closed")
					}
				}
			}
			for _, r := range salvage.Resources {
				for _, f := range r.Files {
					if _, err := f.Stat(); err != nil {
						t.Fatal("pristine detach closed original")
					}
				}
			}
			serverImageCloseResources(salvage.Resources)
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if imported.Server().flowState.cfg.Budget != cfg.Flow.Budget || imported.Server().flowState.ticketKey != s.flowState.ticketKey {
				t.Fatal("cold issuer/budget recreated")
			}
			salvage, err = imported.DetachPristine()
			if err != nil {
				t.Fatal(err)
			}
			if _, err = imported.DetachPristine(); err == nil {
				t.Fatal("double detach accepted")
			}
			if len(salvage.Resources) != 3 {
				t.Fatal("private descriptor lost on detach")
			}
			for _, r := range salvage.Resources {
				for _, f := range r.Files {
					if _, err := f.Stat(); err != nil {
						t.Fatal("pristine detach closed original")
					}
				}
			}
			serverImageCloseResources(salvage.Resources)
		}
		for _, f := range src.files {
			_ = f.Close()
		}
		_ = peer.Close()
		_ = s.flowState.manager.Close()
	}
}

// Five real partial-take/adopt/detach cycles retain no private FD or poll owner.
// Identity multisets make close-old/open-new substitutions visible.
// origin: hot-update S6 plan §9.3 / T56
func TestBrokerServerImageLeakCycles(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux image owner")
	}
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = ln.Close()
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	identities := serverImageFDIdentities(t)
	for round := 0; round < 5; round++ {
		exerciseServerPartialImport(t)
	}
	testharness.AssertNoGoroutineLeak(t, "server image", beforeG)
	testharness.AssertNoFDLeak(t, "server image", beforeFD, 0)
	if got := serverImageFDIdentities(t); !reflect.DeepEqual(got, identities) {
		t.Fatalf("FD identity multiset changed: before=%v after=%v", identities, got)
	}
}
func serverImageFDIdentities(t *testing.T) map[string]int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int{}
	for _, e := range entries {
		info, err := os.Stat("/proc/self/fd/" + e.Name())
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		v := reflect.ValueOf(info.Sys()).Elem()
		key := fmt.Sprintf("%d/%d", v.FieldByName("Dev").Uint(), v.FieldByName("Ino").Uint())
		out[key]++
	}
	return out
}

// Fuzz only the pure decoder; invalid private bytes must never panic or take a
// business file. Valid decoding must canonically round-trip every required field.
// origin: hot-update S6 plan §12.1 P07 / T24
func FuzzBrokerTunnelPrivateState(f *testing.F) {
	if runtime.GOOS != "linux" {
		f.Skip("Linux image owner")
	}
	f.Add([]byte("tether-broker-tunnel-cut\x01"))
	f.Add([]byte{0, 1, 255})
	s := newServer("", "127.0.0.1", nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := s.SetFlowConfig(serverImageTestConfig()); err != nil {
		f.Fatal(err)
	}
	snap, err := snapshot(s, nil)
	if err != nil {
		f.Fatal(err)
	}
	data, err := encodeServerCut(sealedstate.CutState{Records: snap.Records})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(data)
	f.Add(append(append([]byte(nil), data...), 1))
	_ = s.flowState.manager.Close()
	f.Fuzz(func(t *testing.T, data []byte) {
		cut, err := decodeServerCut(data)
		if err != nil {
			return
		}
		encoded, err := encodeServerCut(cut)
		if err != nil {
			t.Fatal(err)
		}
		round, err := decodeServerCut(encoded)
		if err != nil || !reflect.DeepEqual(cut, round) || !bytes.Equal(data, encoded) {
			t.Fatal("valid private bytes lost state")
		}
	})
}

// This is the real P06 seam test. It intentionally remains runnable and red
// against the P00-era dependency refusal instead of substituting a fake freeze.
// origin: hot-update S6 plan §6.2 / T24
func TestBrokerServerRuntimeImageRoundTrip(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux image owner")
	}
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = ln.Close()
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerServerRuntimeImage(t)
	}
	testharness.AssertNoGoroutineLeak(t, "server runtime image", beforeG)
	testharness.AssertNoFDLeak(t, "server runtime image", beforeFD, 0)
}
func exerciseBrokerServerRuntimeImage(t *testing.T) {
	t.Helper()
	s := serverImageTestServer(t)
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: unit fixture owns the live server loops.
	defer cancel()
	defer s.Close()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// Exercise the production freeze/codec/import seam with an original accepted
	// raw, its FIFO reservation and both original LISTEN OFDs.
	// origin: hot-update S6 plan §6.2 / T24; internal review rv-fd-1
	e, err := reserveEndpoint(s, Allocation{Port: 0, SID: "session", NID: "agent", TokenHash: "token", Epoch: 1}, fenceSnap{})
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	delete(s.flowState.endpoints, 0)
	e.allocation.Port = e.listener.Addr().(*net.TCPAddr).Port
	s.flowState.endpoints[e.allocation.Port] = e
	s.mu.Unlock()
	peer, err := net.DialTimeout("tcp4", e.listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer closeServerTestResource(t, peer)
	until := time.Now().Add(3 * time.Second)
	for {
		s.mu.Lock()
		pending := e.pendingRaw != nil
		s.mu.Unlock()
		if pending {
			break
		}
		if time.Now().After(until) {
			t.Fatal("original public socket was never accepted")
		}
		time.Sleep(time.Millisecond)
	}
	frozen, err := s.FreezeServer(serverImageTestDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	im, resources, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	public, err := json.Marshal(im)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(public, s.flowState.ticketKey[:]) {
		t.Fatal("ticket key leaked into public image")
	}
	if len(resources) < 3 {
		t.Fatal("private/flow/control-listener state missing")
	}
	s.mu.Lock()
	fp, endpointID, ticketKey := e.pendingRaw.Fingerprint(), e.id, s.flowState.ticketKey
	s.mu.Unlock()
	copies := handofftest.DupResources(t, resources)
	defer copies.Close()
	if err = frozen.Discard(); err != nil {
		t.Fatal(err)
	}
	src := &serverImageTestSource{files: map[string]*os.File{}}
	for _, res := range copies.Borrow() {
		for name := range res.Files {
			file, err := copies.TakeFile(res.ID, name)
			if err != nil {
				t.Fatal(err)
			}
			src.files[res.ID+"/"+name] = file
		}
	}
	owner, salvage, err := ImportServer(ServerImportConfig{PublicHost: "127.0.0.1", Flow: serverImageTestConfig()}, im, src)
	if err != nil {
		serverImageCloseResources(salvage.Resources)
		t.Fatal(err)
	}
	defer owner.Server().Close()
	imported := owner.(*serverImportedImage)
	restored := imported.server.flowState.endpoints[e.allocation.Port]
	if restored == nil || restored.id != endpointID || restored.pendingRaw == nil || restored.pendingRaw.Fingerprint() != fp || imported.server.flowState.ticketKey != ticketKey || len(imported.server.flowState.pending) != 1 || len(imported.server.flowState.waiters) != 1 || imported.server.flowState.waiters[0] != restored {
		t.Fatal("runtime export/import changed raw identity, issuer or FIFO reservation")
	}
	state := imported.stateFile
	flowState := imported.resources[0].Files["state"]
	for _, res := range imported.resources {
		if res.ID == im.Flows.StateRef {
			flowState = res.Files["state"]
		}
	}
	if err = owner.Activate(serverImageTestDeadline(t)); err != nil {
		t.Fatal(err)
	}
	for _, file := range []*os.File{state, flowState} {
		if _, err = file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("ACTIVE retained decode-only inherited memfd")
		}
	}
	if _, err = owner.DetachPristine(); err == nil {
		t.Fatal("activated server returned to pristine ownership")
	}
	if _, err = peer.Write([]byte("original")); err != nil {
		t.Fatal(err)
	}
	if err = restored.pendingRaw.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("original"))
	if _, err = io.ReadFull(restored.pendingRaw, got); err != nil || string(got) != "original" {
		t.Fatalf("imported original socket: %q %v", got, err)
	}
	current, err := owner.Server().FreezeServer(serverImageTestDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = current.Export(); err != nil {
		t.Fatal(err)
	}
	if err = current.Thaw(serverImageTestDeadline(t)); err != nil {
		t.Fatal(err)
	}

}

type serverImageTestRuntimeCut struct {
	server *Server
	flow   *flow.Frozen
	fail   bool
}

func (c *serverImageTestRuntimeCut) snapshot() (serverStateSnapshot, error) {
	if c.fail {
		return serverStateSnapshot{}, sealedstate.ErrInvalid
	}
	return snapshot(c.server, nil)
}
func (c *serverImageTestRuntimeCut) flowExport() (flow.Image, *sealedstate.Snapshot, error) {
	return c.flow.Export()
}
func (c *serverImageTestRuntimeCut) thaw(controlbudget.Deadline) error { return c.flow.Thaw() }
func (c *serverImageTestRuntimeCut) discard() error                    { return c.flow.Discard() }
func exerciseServerReexport(t *testing.T) {
	t.Helper()
	s := serverImageTestServer(t)
	ff, err := s.flowState.manager.Freeze(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	cut := &serverImageTestRuntimeCut{server: s, flow: ff}
	f := &serverFrozenImage{cut: cut}
	for round := 0; round < 5; round++ {
		var previous *os.File
		if f.state != nil {
			previous = f.state.Owned[0]
		}
		im, resources, err := f.Export()
		if err != nil || !serverStateRef(im.StateResource) || len(resources) != 2 {
			t.Fatalf("sealed reexport refused: %v", err)
		}
		if previous != nil {
			if _, err := previous.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatal("replacement leaked old private FD")
			}
		}
	}
	last := f.state
	cut.fail = true
	if _, _, err := f.Export(); err == nil || f.state != last {
		t.Fatal("failed replacement lost last good cut")
	}
	if _, err := last.Owned[0].Stat(); err != nil {
		t.Fatal("failed replacement closed last good private FD")
	}
	discardPrivate := f.state.Owned[0]
	if err = f.Discard(); err != nil {
		t.Fatal(err)
	}
	if _, err := discardPrivate.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("discard leaked private FD")
	}
	if _, _, err = f.Export(); err == nil {
		t.Fatal("export after discard accepted")
	}
}

// origin: hot-update S6 plan §6.2 / T25
func TestBrokerImportedRevokeSurvivesRejectedRegistration(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerImportedRevoke(t)
	}
	testharness.AssertNoGoroutineLeak(t, "imported allocation revoke", g)
	testharness.AssertNoFDLeak(t, "imported allocation revoke", fd, 0)
}
func exerciseBrokerImportedRevoke(t *testing.T) {
	t.Helper()
	s := serverImageTestServer(t)
	defer closeServerTestResource(t, s.flowState.manager)
	allocation := Allocation{Port: 30201, SID: "session", NID: "agent", TokenHash: "revoked", Epoch: 4}
	key := allocationKey(allocation)
	s.closedAllocation[key], s.killGenAllocation[key] = true, 1
	record, err := snapshotServerFences(s)
	if err != nil {
		t.Fatal(err)
	}
	s.importedImage = &serverImportedImage{server: s, state: serverStateSnapshot{Records: []sealedstate.Record{record}}}
	for retry := 0; retry < 5; retry++ {
		s.mu.Lock()
		s.maybePruneAllocationLocked(key)
		retained := s.closedAllocation[key]
		snapshot := s.fenceSnapLocked(allocation.Port, allocation.SID, key)
		s.mu.Unlock()
		if !retained {
			t.Fatal("rejected reconnect pruned sealed revoke")
		}
		if _, err := reserveEndpoint(s, allocation, snapshot); err == nil {
			t.Fatal("same allocation resurrected after imported revoke")
		}
	}
	// A new token is a different allocation, and ordinary close pruning keeps
	// its existing bounded-map contract even on an imported server.
	current := allocation
	current.TokenHash = "replacement"
	currentKey := allocationKey(current)
	s.closedAllocation[currentKey], s.killGenAllocation[currentKey] = true, 1
	s.mu.Lock()
	s.maybePruneAllocationLocked(currentKey)
	kept := s.closedAllocation[currentKey]
	s.mu.Unlock()
	if kept {
		t.Fatal("retained an ordinary close outside the imported fence set")
	}
}

// A cut latch may wrap a terminal close only after every callback/monitor has
// joined. Otherwise CloseProxy could join a worker that itself needs that latch.
// origin: hot-update S6 plan §6.2 / T25
func TestBrokerImageRevokeRequiresJoinedSafePoint(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerImageRevokeSafePoint(t)
	}
	testharness.AssertNoGoroutineLeak(t, "image revoke safe point", g)
	testharness.AssertNoFDLeak(t, "image revoke safe point", fds, 0)
}
func exerciseBrokerImageRevokeSafePoint(t *testing.T) {
	t.Helper()
	s := serverImageTestServer(t)
	defer func() {
		// The unjoined entries below are test-owned receipts, with no workers.
		// Clear them even when a guard mutant makes the assertion fatal.
		s.mu.Lock()
		s.monitors = make(map[*flowCarrier]chan struct{})
		s.callbacks = make(map[*endpointTimer]struct{})
		s.mu.Unlock()
		s.Close()
	}()
	called := 0
	hook := func(ids []string, revoke func() error) error {
		called++
		if len(ids) == 0 {
			t.Fatal("hard fence has no resource identity")
		}
		return revoke()
	}
	if err := SetServerImageRevoke(s, hook); err == nil {
		t.Fatal("active server accepted a cut latch")
	}
	s.mu.Lock()
	s.imagePaused = true
	s.monitors = make(map[*flowCarrier]chan struct{})
	s.monitors[&flowCarrier{}] = make(chan struct{})
	s.mu.Unlock()
	if err := SetServerImageRevoke(s, hook); err == nil {
		t.Fatal("unjoined monitor accepted a cut latch")
	}
	s.mu.Lock()
	s.monitors = make(map[*flowCarrier]chan struct{})
	work := &endpointTimer{done: make(chan struct{})}
	s.callbacks = make(map[*endpointTimer]struct{})
	s.callbacks[work] = struct{}{}
	s.mu.Unlock()
	if err := SetServerImageRevoke(s, hook); err == nil {
		t.Fatal("unjoined timer callback accepted a cut latch")
	}
	finishEndpointTimer(s, work)
	if err := SetServerImageRevoke(s, hook); err != nil {
		t.Fatal(err)
	}
	s.CloseProxy(30101)
	if called != 1 {
		t.Fatal("hard revoke bypassed the cut latch")
	}
	if err := SetServerImageRevoke(s, nil); err != nil {
		t.Fatal(err)
	}
	s.CloseProxy(30102)
	if called != 1 {
		t.Fatal("retired image latch retained terminal ownership")
	}
}

// A peer may already be closed by the owner under test. Other cleanup errors
// must remain visible instead of letting the leak oracle mask a failed Close.
// origin: hot-update S6 plan §11 / T56
func closeServerTestResource(t *testing.T, resource io.Closer) {
	t.Helper()
	if err := resource.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Errorf("close test resource: %v", err)
	}
}
