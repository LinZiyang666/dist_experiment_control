//go:build linux && hotupdate_integration

// Real connection owners are supplied by the fullstack fixture through the
// frozen Fixture/Driver ports. The local carrier control uses real TLS/yamux;
// it does not certify NATS/Caddy upgrades or the missing fullstack assembly.
package hotupdate_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/test/fullstackharness"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
	"golang.org/x/sys/unix"
)

// P22 must bind P19's landed Fixture adapter to P25's live assembly here. A nil
// binding is SETUP-RED, never a skip, successful stub, or permission to run a
// deployment script. All process ownership stays with P05's shared supervisor.
// The callback additionally exposes memory-only private observations; no new
// product DTO or frozen port is introduced by these test-local types.
// origin: hot-update S10 plan §5.1 / §10.2 P22 / lead L15-L17
var fullstackConnectionSupply func(*testing.T, string, string) (*fullstackConnectionRun, error)

type fullstackConnectionRun struct {
	Profile contract.Profile
	Fixture contract.Fixture
	Driver  contract.Driver
	Read    func(context.Context, string) (fullstackConnectionCapture, error)
	// Denominators are sealed before Start, never rebuilt from successful
	// probes or a post-cut surviving owner census.
	InfraExpected map[string]fullstackInfraExpected
}

type fullstackInfraExpected struct {
	Bindings, Owners, Slots json.RawMessage
}

type fullstackConnectionCapture struct {
	Nonce    string
	Flows    []fullstackOriginalFlow
	FlowCuts []fullstackFlowCut
	Infra    []fullstackInfraCut
	Crypto   []fullstackSSCut
	Raw      []contract.RawRef
}

// Actual final export and pristine import receipts, not a sample taken before
// Stage. Private pending payloads remain sealed; only their digests are joined.
type fullstackFlowCheckpoint struct {
	Base, End, Applied, Received, FINEnd uint64
	FIN, CloseWrite                      bool
	ReplaySHA, PendingSHA, RawReadSHA    [32]byte
}

type fullstackFlowCut struct {
	StepID, FlowID string
	Side           int
	Cut, Imported  fullstackFlowCheckpoint
}

type fullstackOriginalFlow struct {
	ID, Kind            string
	Nonce               [16]byte
	Application         contract.Process
	Accepts, Handshakes uint64
	Endpoints           [2]contract.Resource
	Directions          [2]fullstackFlowDirection
}

// Offsets are bytes, including partial records. Digest is computed by the
// independent application receiver, never copied from the sender's buffer.
type fullstackFlowDirection struct {
	Sent, Received, Applied, ReplayBase, ReplayEnd uint64
	Digest                                         [32]byte
	FIN                                            bool
	FINEnd, CloseWrites                            uint64
	MaxGapNS, TailGapNS                            int64
}

// P15's original raw record is retained. These fields are only the join keys
// needed to prove that this original's physical NC was affected by this cut.
type fullstackInfraCut struct {
	StepID, FlowID, ServerID, ALPN string
	CID, SocketCookie              uint64
	Affected, OldOwnersExited      bool
	CanonicalTLS, WSS, RawTCP, SSH uint64
	Failed, Missing, Retried       uint64
	Raw                            contract.RawRef
	Before                         contract.RawRef
}

// origin: hot-update S10 plan §5.4 / §10.2 P22 / §11.2
func TestFullstackOriginalFlowsUseEveryInfraCut(t *testing.T) {
	started := time.Now()
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, topology := range []string{"single", "quorum"} {
			t.Run(fmt.Sprintf("%s/round-%d", topology, round), func(t *testing.T) { exerciseOriginalFlowCut(t, topology) })
		}
	}
	testharness.AssertNoGoroutineLeak(t, "fullstack original socket and carrier collectors", g)
	testharness.AssertNoFDLeak(t, "fullstack original socket and carrier collectors", fd, 0)
	t.Logf("five-cycle total=%s; fullstack acceptance requires every subtest", time.Since(started))
}

func exerciseOriginalFlowCut(t *testing.T, topology string) {
	t.Helper()
	exerciseFullstackConnections(t, "tcp-ssh", topology)
}

func exerciseFullstackConnections(t *testing.T, mode, topology string) {
	t.Helper()
	startup := time.Now()
	if fullstackConnectionSupply == nil {
		t.Fatal("SETUP-RED: NOT_RUN pending S10 P22 fullstackConnectionSupply binding to P25 live assembly, P18 native CLI adapters, and P15 physical ingress receipts")
	}
	run, err := fullstackConnectionSupply(t, mode, topology)
	if err != nil {
		t.Fatalf("SETUP-RED: fullstack connection supply: %v", err)
	}
	if run == nil || run.Fixture == nil || run.Driver == nil || run.Read == nil {
		t.Fatal("SETUP-RED: incomplete fullstack connection supply")
	}
	// Retain independent denominators even if the fixture reuses its buffers.
	run.Profile.Required = slices.Clone(run.Profile.Required)
	run.Profile.Steps = slices.Clone(run.Profile.Steps)
	run.Profile.Artifacts = slices.Clone(run.Profile.Artifacts)
	expected := make(map[string]fullstackInfraExpected, len(run.InfraExpected))
	for id, row := range run.InfraExpected {
		expected[id] = fullstackInfraExpected{slices.Clone(row.Bindings), slices.Clone(row.Owners), slices.Clone(row.Slots)}
	}
	run.InfraExpected = expected
	// Install cleanup before Start: partial provisioning and observer failure
	// still owe both Stop and Join. Cleanup may not erase the original failure.
	defer func() {
		begin := time.Now()
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 15*time.Second)
		defer cancel()
		stopErr := run.Fixture.Stop(ctx)
		joinErr := run.Fixture.Join(ctx)
		if err := errors.Join(stopErr, joinErr); err != nil {
			t.Errorf("fixture Stop/Join failed: %v", err)
		}
		t.Logf("supervisor join=%s", time.Since(begin))
	}()
	if err := fullstackConnectionPlan(run.Profile, topology); err != nil {
		t.Fatalf("SETUP-RED: %v", err)
	}
	raw, err := run.Fixture.Start(t.Context(), run.Profile)
	if err != nil {
		t.Fatalf("SETUP-RED: fixture Start: %v", err)
	}
	if err := fullstackConnectionRaw(raw); err != nil {
		t.Fatal(err)
	}
	t.Logf("startup=%s", time.Since(startup))
	read := func(label string) fullstackConnectionCapture {
		t.Helper()
		nonce := t.Name() + "/" + label
		capture, err := run.Read(t.Context(), nonce)
		if err != nil || capture.Nonce != nonce || len(capture.Raw) == 0 {
			t.Fatalf("SETUP-RED: fresh connection capture %s: %v", label, err)
		}
		for _, ref := range capture.Raw {
			if err := fullstackConnectionRaw(ref); err != nil {
				t.Fatal(err)
			}
		}
		capture.Flows = slices.Clone(capture.Flows)
		capture.FlowCuts = slices.Clone(capture.FlowCuts)
		capture.Infra = slices.Clone(capture.Infra)
		capture.Raw = slices.Clone(capture.Raw)
		capture.Crypto = fullstackCloneSS(capture.Crypto)
		return capture
	}
	before := read("originals")
	if err := fullstackOriginalPopulation(before.Flows, mode); err != nil {
		t.Fatal(err)
	}
	// Independent KCMP_FILE witnesses pin each original product endpoint. Equal
	// inode, FD count, or a collector's claimed OFD string cannot substitute.
	witnesses := make(map[string][2]*fullstackharness.OFDWitness)
	for _, flow := range before.Flows {
		var pair [2]*fullstackharness.OFDWitness
		for side, endpoint := range flow.Endpoints {
			pair[side], err = fullstackharness.NewOFDWitness(endpoint.Owner, endpoint.FD)
			if err != nil {
				t.Fatalf("SETUP-RED: original OFD witness: %v", err)
			}
			defer func(w *fullstackharness.OFDWitness) {
				if err := w.Close(); err != nil {
					t.Error(err)
				}
			}(pair[side])
		}
		witnesses[flow.ID] = pair
	}
	previous := before
	for _, step := range run.Profile.Steps {
		begin := time.Now()
		// This sample proves pre-request load. The private cut/import records
		// separately prove nonempty state at the actual owner boundary.
		cutBefore := read(step.ID + "/before")
		if err := fullstackFlowTransition(previous.Flows, cutBefore.Flows, false); err != nil {
			t.Fatal(err)
		}
		for _, flow := range cutBefore.Flows {
			for _, direction := range flow.Directions {
				if direction.ReplayEnd <= direction.ReplayBase || direction.Applied == 0 {
					t.Fatal("SETUP-RED: cut lacks nonempty replay and applied prefix")
				}
			}
		}
		// Reverse consumes the product's actual pinned previous image. Staging
		// another candidate here could overwrite the very state under test.
		if step.Direction == "forward" {
			if _, err := run.Driver.Stage(t.Context(), step); err != nil {
				t.Fatalf("SETUP-RED: product Stage: %v", err)
			}
		}
		view, err := run.Driver.Apply(t.Context(), step)
		if err != nil {
			t.Fatalf("product cut: %v", err)
		}
		if err := fullstackConnectionView(step, view); err != nil {
			t.Fatal(err)
		}
		fullstackExecutingImage(t, run, step, view)
		after := read(step.ID + "/after")
		if err := fullstackFlowTransition(cutBefore.Flows, after.Flows, true); err != nil {
			t.Fatal(err)
		}
		if step.Target.Role == "agent" || step.Target.Role == "broker" {
			if err := fullstackFlowCheckpoints(step, cutBefore.Flows, after.FlowCuts); err != nil {
				t.Fatal(err)
			}
		}
		for _, flow := range after.Flows {
			for side, endpoint := range flow.Endpoints {
				proof, err := witnesses[flow.ID][side].Check(t.Context(), endpoint.Owner, endpoint.FD)
				if err != nil || proof.SocketCookie == 0 || proof.Flags&unix.O_CLOEXEC == 0 {
					t.Fatal("original product socket lost its kernel OFD")
				}
			}
		}
		if step.Target.Role == "nats" || step.Target.Role == "caddy" {
			if err := fullstackInfraCoverage(step, before.Flows, after.Infra); err != nil {
				t.Fatal(err)
			}
			fullstackPhysicalInfra(t, run, step, after.Infra)
		}
		if mode == "ss" {
			if err := fullstackSSContinuity(step, cutBefore.Crypto, after.Crypto); err != nil {
				t.Fatal(err)
			}
		}
		previous = after
		if time.Since(begin) > upgradebudget.ComponentHandover {
			t.Fatal("component cut exceeded its original budget")
		}
		t.Logf("cut role=%s direction=%s elapsed=%s", step.Target.Role, step.Direction, time.Since(begin))
	}
	settle := time.Now()
	// Natural source completion and exact EOF precede fixture Stop. Killing
	// the fixture to obtain EOF would make M02F/M04A/M04B invisible.
	raw, err = run.Fixture.Drain(t.Context(), t.Name()+"/drain")
	if err != nil {
		t.Fatal(err)
	}
	if err := fullstackConnectionRaw(raw); err != nil {
		t.Fatal(err)
	}
	final := read("settled")
	if err := fullstackFlowTransition(previous.Flows, final.Flows, true); err != nil {
		t.Fatal(err)
	}
	for _, flow := range final.Flows {
		for _, d := range flow.Directions {
			if !d.FIN || d.FINEnd != d.Sent || d.Received != d.Sent || d.Applied != d.Sent || d.CloseWrites != 1 || d.ReplayBase != d.ReplayEnd {
				t.Fatal("original flow lacks exact final bytes, FIN, once CloseWrite, or replay retirement")
			}
		}
	}
	t.Logf("settle=%s; I/O spans every recorded component cut", time.Since(settle))
}

// This is the focused R1/R2 connection composition, not the six-image resource
// test. Each required runtime must be cut in both directions, in layer order.
func fullstackConnectionPlan(p contract.Profile, topology string) error {
	if p.Tier != "hermetic" || p.Topology != topology || p.EvidenceRoot == "" || len(p.Required) == 0 {
		return errors.New("connection test requires the hermetic sealed profile")
	}
	want := []string{"agent/forward", "broker/forward", "nats/forward", "caddy/forward", "caddy/reverse", "nats/reverse", "broker/reverse", "agent/reverse"}
	counts := map[string]int{}
	for _, key := range p.Required {
		counts[key.Role]++
	}
	if counts["agent"] != 4 || counts["caddy"] == 0 || counts["nats"] == 0 || (topology == "single" && counts["broker"] != 1) || (topology == "quorum" && counts["broker"] != 3) {
		return errors.New("connection profile omits a runtime")
	}
	index := 0
	seen := map[string]bool{}
	for _, group := range want {
		// Consume every required key in this group, rejecting duplicates and
		// extra steps rather than deducing the denominator from observed cuts.
		for _, key := range p.Required {
			if group != key.Role+"/forward" && group != key.Role+"/reverse" {
				continue
			}
			if index >= len(p.Steps) {
				return errors.New("connection cut schedule is incomplete")
			}
			s := p.Steps[index]
			if s.Target != key || s.Target.Role+"/"+s.Direction != group || s.ID == "" || seen[s.ID] || s.From == s.To || s.From == "" || s.To == "" {
				return errors.New("connection cut schedule changed target, direction, identity or artifact")
			}
			seen[s.ID] = true
			index++
		}
	}
	if index != len(p.Steps) {
		return errors.New("unexpected connection cut")
	}
	return nil
}

func fullstackConnectionView(step contract.Step, view contract.View) error {
	if view.Stamp.Process.Key != step.Target || view.Stamp.Direction != step.Direction || view.Stamp.TxID == "" || view.Stamp.CutID == "" || view.Stamp.QueryNonce == "" || view.Phase != "completed" || view.Outcome != "success" || len(view.Deferred) != 0 || len(view.Historical) == 0 || len(view.Current) == 0 {
		return errors.New("cut did not return fresh complete current product state")
	}
	for _, receipt := range []contract.Receipt{view.Control, view.Resources, view.Released} {
		if !receipt.Known || !receipt.Complete || receipt.Stamp != view.Stamp || receipt.RunningSHA == (contract.Digest{}) {
			return errors.New("cut lacks all three same-stamp completion proofs")
		}
		if err := fullstackConnectionRaw(receipt.Evidence); err != nil {
			return err
		}
	}
	return fullstackConnectionRaw(view.Raw)
}

// P04 hashes an independently opened executable and revalidates the current
// product stamp. Self-reported SHA and a changed disk pathname cannot sign exec.
// origin: hot-update S10 plan §2.1 / §5.4 / §8 M01B
func fullstackExecutingImage(t *testing.T, run *fullstackConnectionRun, step contract.Step, view contract.View) {
	t.Helper()
	i := slices.IndexFunc(run.Profile.Artifacts, func(a contract.Artifact) bool { return a.Role == step.Target.Role && a.ID == step.To })
	if i < 0 || run.Profile.Artifacts[i].Binary == (contract.Digest{}) {
		t.Fatal("SETUP-RED: missing pinned target artifact")
	}
	read, err := fullstackharness.ReadExecutable(t.Context(), view.Stamp, func(ctx context.Context) (contract.Stamp, error) {
		fresh, err := run.Driver.Query(ctx, step.Target, view.Stamp.TxID, view.Stamp.QueryNonce)
		return fresh.Stamp, err
	})
	if err != nil {
		t.Fatalf("independent executing image: %v", err)
	}
	defer func() {
		if err := read.Close(); err != nil {
			t.Error(err)
		}
	}()
	if read.Current.SHA != run.Profile.Artifacts[i].Binary {
		t.Fatal("target executable bytes were not activated")
	}
}

// Use P15's full native decoder for socket -> accepted Caddy -> NATS CID,
// returned attachments, scheduled canonical probes, and held-pidfd old exits.
// The small Go join cannot replace these raw checks. This subprocess only
// reads evidence; it cannot run a drill or perform a host operation.
// origin: hot-update S10 plan §5.4 / §10.2 P22 / lead L15
func fullstackPhysicalInfra(t *testing.T, run *fullstackConnectionRun, step contract.Step, rows []fullstackInfraCut) {
	t.Helper()
	expected, ok := run.InfraExpected[step.ID]
	if !ok || len(expected.Bindings) == 0 || len(expected.Owners) == 0 || len(expected.Slots) == 0 {
		t.Fatal("SETUP-RED: missing presealed P15 denominators")
	}
	index := slices.IndexFunc(rows, func(c fullstackInfraCut) bool { return c.StepID == step.ID })
	if index < 0 {
		t.Fatal("SETUP-RED: missing P15 cut records")
	}
	row := rows[index]
	if err := fullstackConnectionRaw(row.Before); err != nil {
		t.Fatal(err)
	}
	if err := fullstackConnectionRaw(row.Raw); err != nil {
		t.Fatal(err)
	}
	for _, other := range rows {
		if other.StepID == step.ID && (other.Before != row.Before || other.Raw != row.Raw) {
			t.Fatal("physical cohort was assembled from different cut receipts")
		}
	}
	input, err := json.Marshal(struct {
		Before, After, Topology, Direction string
		Bindings, Owners, Slots            json.RawMessage
	}{row.Before.Path, row.Raw.Path, run.Profile.Topology, step.Direction, expected.Bindings, expected.Owners, expected.Slots})
	if err != nil {
		t.Fatal(err)
	}
	module, err := filepath.Abs("../simcluster/drills/lib/fullstack_infra.py")
	if err != nil {
		t.Fatal(err)
	}
	const check = `import importlib.util,json,pathlib,sys
spec=importlib.util.spec_from_file_location("infra",sys.argv[1])
infra=importlib.util.module_from_spec(spec); spec.loader.exec_module(infra)
p=json.load(sys.stdin)
a=pathlib.Path(p["Before"]).read_bytes(); b=pathlib.Path(p["After"]).read_bytes()
before=infra.record(a)
infra.require(before["tier"]=="hermetic" and before["stamp"]["direction"]==p["Direction"],"wrong integration tier or direction")
infra.validate_cut(a,b,p["Bindings"],p["Owners"],p["Slots"],p["Topology"])
`
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", "-I", "-B", "-c", check, module)
	cmd.Stdin = bytes.NewReader(input)
	if _, err := fullstackharness.CombinedOutput(cmd); err != nil {
		t.Fatalf("P15 physical cut validation failed: %v", err)
	}
}

func fullstackConnectionRaw(ref contract.RawRef) error {
	if ref.Path == "" || ref.Schema == "" || ref.Bytes <= 0 || ref.Bytes > 4<<20 || ref.SHA == (contract.Digest{}) {
		return errors.New("SETUP-RED: incomplete connection raw reference")
	}
	f, err := os.Open(ref.Path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, ref.Bytes+1))
	if err != nil || n != ref.Bytes || !bytes.Equal(h.Sum(nil), ref.SHA[:]) {
		return errors.New("SETUP-RED: connection raw bytes do not match their immutable reference")
	}
	return nil
}

func fullstackOriginalPopulation(flows []fullstackOriginalFlow, mode string) error {
	if len(flows) != 4 {
		return errors.New("original connection population is incomplete")
	}
	seen := map[string]bool{}
	for _, flow := range flows {
		if flow.ID == "" || seen[flow.ID] || flow.Nonce == ([16]byte{}) || flow.Application.PID <= 0 || flow.Application.StartTicks == 0 || flow.Application.BootID == "" || flow.Application.PIDNS == "" || flow.Accepts != 1 {
			return errors.New("original connection identity or accept census is incomplete")
		}
		seen[flow.ID] = true
		kind := "tcp"
		if mode == "ss" {
			kind = "ss"
		} else if flow.Application.Key.NID == "a2" || flow.Application.Key.NID == "a3" {
			kind = "ssh"
		}
		if !slices.Contains([]string{"a0", "a1", "a2", "a3"}, flow.Application.Key.NID) || seen[flow.Application.Key.NID] || flow.Kind != kind || (kind == "ssh" && flow.Handshakes != 1) {
			return errors.New("original connection runtime or real SSH handshake missing")
		}
		seen[flow.Application.Key.NID] = true
		for side, r := range flow.Endpoints {
			role := []string{"agent", "broker"}[side]
			if r.ID == "" || r.FD < 3 || r.Owner.Process.Key.Role != role || r.Owner.Generation == 0 {
				return errors.New("original product endpoint identity missing")
			}
		}
	}
	return nil
}

// origin: hot-update S10 plan §8 M01B/M02F/M03F/M04A/M04B/M11F
func fullstackFlowTransition(before, after []fullstackOriginalFlow, progress bool) error {
	if len(before) == 0 || len(before) != len(after) {
		return errors.New("original flow census changed")
	}
	seen := map[string]bool{}
	for _, a := range after {
		i := slices.IndexFunc(before, func(b fullstackOriginalFlow) bool { return b.ID == a.ID })
		if i < 0 || seen[a.ID] {
			return errors.New("original flow replaced or duplicated")
		}
		seen[a.ID] = true
		b := before[i]
		if a.Application != b.Application || a.Nonce != b.Nonce || a.Kind != b.Kind || a.Accepts != 1 || a.Handshakes != b.Handshakes {
			return errors.New("original application, handshake or accept changed")
		}
		for side := range a.Endpoints {
			if a.Endpoints[side].ID != b.Endpoints[side].ID || a.Endpoints[side].Owner.Process != b.Endpoints[side].Owner.Process {
				return errors.New("original product socket owner replaced")
			}
		}
		for direction, d := range a.Directions {
			old := b.Directions[direction]
			if d.Received == 0 || d.Received > flowRecordBytes*flowRecordCount || d.Received > d.Sent || d.Applied > d.Sent || d.ReplayBase > d.ReplayEnd || d.ReplayEnd > d.Sent || d.Sent < old.Sent || d.ReplayBase < old.ReplayBase || d.ReplayEnd < old.ReplayEnd || d.Received < old.Received || d.Applied < old.Applied || (progress && !old.FIN && d.Received <= old.Received) {
				return errors.New("original duplex offset or replay checkpoint lost")
			}
			if d.MaxGapNS <= 0 || d.MaxGapNS > int64(flowBusinessDeadline) || d.TailGapNS < 0 || d.TailGapNS > int64(flowBusinessDeadline) {
				return errors.New("original flow exceeded the all-sample stall budget")
			}
			if d.Digest != fullstackPrefixDigest(a.Nonce, byte(direction), d.Received) {
				return errors.New("independent receiver byte sequence differs from declared source")
			}
			if old.FIN && (!d.FIN || d.FINEnd != old.FINEnd) || d.CloseWrites > 1 {
				return errors.New("flow FIN lost or CloseWrite repeated")
			}
		}
	}
	return nil
}

func fullstackPrefixDigest(nonce [16]byte, direction byte, size uint64) [32]byte {
	// A bounded hermetic source prevents corrupt offsets allocating/looping
	// forever. Supply may span cuts, but cannot mutate its declared record count.
	if size > flowRecordBytes*flowRecordCount {
		return [32]byte{}
	}
	h := sha256.New()
	for seq := uint64(0); size != 0; seq++ {
		b := flowOracleRecord(nonce, direction, seq)
		n := min(size, uint64(len(b)))
		_, _ = h.Write(b[:n])
		size -= n
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

// origin: hot-update S10 plan §8 M02F/M03F
func fullstackFlowCheckpoints(step contract.Step, originals []fullstackOriginalFlow, rows []fullstackFlowCut) error {
	want := map[string]int{}
	for _, f := range originals {
		for side, endpoint := range f.Endpoints {
			if endpoint.Owner.Process.Key == step.Target {
				want[f.ID] = side
			}
		}
	}
	if len(want) == 0 {
		return errors.New("SETUP-RED: image cut does not own an original flow")
	}
	seen := map[string]bool{}
	for _, r := range rows {
		if r.StepID != step.ID {
			continue
		}
		side, ok := want[r.FlowID]
		if !ok || seen[r.FlowID] || r.Side != side {
			return errors.New("flow image checkpoint has a substituted owner")
		}
		seen[r.FlowID] = true
		c := r.Cut
		if c.End <= c.Base || c.Applied == 0 || c.Received < c.Applied || c.ReplaySHA == ([32]byte{}) || c.PendingSHA == ([32]byte{}) || c.RawReadSHA == ([32]byte{}) {
			return errors.New("SETUP-RED: final flow cut has no measured replay or partial-state digests")
		}
		if c != r.Imported {
			return errors.New("flow replay, offset, pending suffix or FIN changed at import")
		}
	}
	if len(seen) != len(want) {
		return errors.New("flow image checkpoint omitted an original endpoint")
	}
	return nil
}

// origin: hot-update S10 plan §5.4 / §10.2 P22
func fullstackInfraCoverage(step contract.Step, flows []fullstackOriginalFlow, cuts []fullstackInfraCut) error {
	seen := map[string]bool{}
	for _, c := range cuts {
		if c.StepID != step.ID {
			continue
		}
		if seen[c.FlowID] || !slices.ContainsFunc(flows, func(f fullstackOriginalFlow) bool { return f.ID == c.FlowID }) {
			return errors.New("infra cohort duplicated or substituted an original")
		}
		seen[c.FlowID] = true
		if !c.Affected || c.ServerID == "" || c.CID == 0 || c.SocketCookie == 0 || c.ALPN != "tether-flow/1" || !c.OldOwnersExited {
			return errors.New("infra cut lacks physical affected cohort, v1 ALPN or old-owner exit")
		}
		if c.CanonicalTLS == 0 || c.WSS == 0 || c.RawTCP == 0 || c.SSH == 0 || c.Failed != 0 || c.Missing != 0 || c.Retried != 0 {
			return errors.New("canonical first-attempt ingress population failed or disappeared")
		}
		if err := fullstackConnectionRaw(c.Raw); err != nil {
			return err
		}
	}
	if len(seen) == 0 || len(flows) == 0 {
		return errors.New("infra cut did not affect any original connection")
	}
	return nil
}

// This runnable component control exercises live original sockets, pending
// duplex bytes and half-close through real carrier replacement. Infra/product
// exec acceptance remains in the two assigned tests, which fail closed above.
// origin: hot-update S10 plan §8 M02F/M03F/M04A/B/M11F / §11.2
func TestFullstackFlowCarrierKeepsOriginalDuplex(t *testing.T) {
	// Warm runtime netpoll descriptors before the exact lifecycle floor.
	t.Run("warm", func(t *testing.T) { exerciseFlowCarrierLifecycle(t, "active") })
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	started := time.Now()
	for round := 0; round < 5; round++ {
		for _, arm := range []string{"active", "target-eof"} {
			t.Run(fmt.Sprintf("round-%d/%s", round, arm), func(t *testing.T) { exerciseFlowCarrierLifecycle(t, arm) })
		}
	}
	testharness.AssertNoGoroutineLeak(t, "fullstack live carrier control", g)
	testharness.AssertNoFDLeak(t, "fullstack live carrier control", fd, 0)
	t.Logf("five-cycle carrier I/O/cut/settle=%s; no infrastructure acceptance claim", time.Since(started))
}
