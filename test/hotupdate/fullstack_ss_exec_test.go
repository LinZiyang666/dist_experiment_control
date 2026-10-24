//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/fullstackharness"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// These values stay in collector memory after sealed-memfd decoding. Never
// marshal or format this type: nonce/cipher/parser state is private custody.
// Cut and Imported are sampled at actual owner boundaries, before activation;
// post-resume application progress cannot stand in for a preserved checkpoint.
type fullstackSSCheckpoint struct {
	Actor, Flow                        [16]byte
	RXNonce, TXNonce                   [12]byte
	RXCursor, TXCursor                 uint64
	RXPending, TXPending               []byte
	AppliedRX, AppliedTX, SaltRevision uint64
	Revoked, CleanupPending            bool
}

type fullstackSSCut struct {
	StepID                  string
	Cut, Imported           fullstackSSCheckpoint
	Seals                   []drillobserve.Event
	Writes                  []drillobserve.Event
	ActorOwners             uint64
	FreshAuthority          bool
	SaltReplayRejected      bool
	RotationSurvivor        bool
	RevokedVictimClosed     bool
	VictimWritesAfterRevoke uint64
}

// origin: hot-update S10 plan §3.2 / §5.4 / §8 M11S / §10.2 P22 / §11.2
func TestFullstackSSStateAcrossInfraRollback(t *testing.T) {
	started := time.Now()
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, topology := range []string{"single", "quorum"} {
			t.Run(fmt.Sprintf("%s/round-%d", topology, round), func(t *testing.T) { exerciseSSCut(t, topology) })
		}
	}
	testharness.AssertNoGoroutineLeak(t, "fullstack SS actor and private reader", g)
	testharness.AssertNoFDLeak(t, "fullstack SS actor and private reader", fd, 0)
	t.Logf("five-cycle total=%s; fullstack acceptance requires every subtest", time.Since(started))
}

func exerciseSSCut(t *testing.T, topology string) {
	t.Helper()
	exerciseFullstackConnections(t, "ss", topology)
}

// A primitive ledger spans actors and every image, including reverse. Looking
// only at successful writes misses a duplicated Seal followed by a short write.
// Errors deliberately omit crypto identities, private bytes and nonce values.
// origin: hot-update S10 plan §8 M11S / architecture §22.8.6
func fullstackSSPrimitiveLedger(events []drillobserve.Event) error {
	if len(events) < 2 {
		return errors.New("actual SS Seal ledger is empty")
	}
	type identity struct {
		digest [32]byte
		nonce  [12]byte
	}
	seen := make(map[identity]bool)
	for _, e := range events {
		if e.Kind != "ss-seal" || e.Domain != "ss" || e.Actor == ([16]byte{}) || e.Digest == ([32]byte{}) || e.Values[0] == 0 {
			return errors.New("actual SS Seal identity is incomplete")
		}
		id := identity{e.Digest, e.Nonce}
		if seen[id] {
			return errors.New("actual SS primitive reused a subkey/nonce")
		}
		seen[id] = true
	}
	return nil
}

func fullstackSSContinuity(step contract.Step, before, after []fullstackSSCut) error {
	if len(after) == 0 || len(before) != 4 {
		return errors.New("SS cut has no private custody")
	}
	seen := map[[16]byte]bool{}
	var allSeals []drillobserve.Event
	for _, row := range after {
		if row.StepID != step.ID {
			continue
		}
		cut, imported := row.Cut, row.Imported
		if cut.Actor == ([16]byte{}) || cut.Flow == ([16]byte{}) || seen[cut.Actor] || row.ActorOwners != 1 {
			return errors.New("SS actor ownership is empty or duplicated")
		}
		seen[cut.Actor] = true
		prior := slices.IndexFunc(before, func(old fullstackSSCut) bool { return old.Cut.Actor == cut.Actor && old.Cut.Flow == cut.Flow })
		if prior < 0 {
			return errors.New("original SS actor or bound flow was replaced")
		}
		// Both windows must be nonempty simultaneously. A buffered plaintext
		// write alone does not prove a real partial ciphertext syscall.
		if cut.RXCursor == 0 || cut.RXCursor >= uint64(len(cut.RXPending)) || cut.TXCursor == 0 || cut.TXCursor >= uint64(len(cut.TXPending)) || cut.AppliedRX == 0 || cut.AppliedTX == 0 {
			return errors.New("SS cut lacks partial parser and already-Sealed suffix")
		}
		if cut.Actor != imported.Actor || cut.Flow != imported.Flow || cut.RXNonce != imported.RXNonce || cut.TXNonce != imported.TXNonce || cut.RXCursor != imported.RXCursor || cut.TXCursor != imported.TXCursor || !bytes.Equal(cut.RXPending, imported.RXPending) || !bytes.Equal(cut.TXPending, imported.TXPending) || cut.AppliedRX != imported.AppliedRX || cut.AppliedTX != imported.AppliedTX || cut.SaltRevision != imported.SaltRevision || cut.Revoked != imported.Revoked || cut.CleanupPending != imported.CleanupPending {
			return errors.New("SS current checkpoint changed at import or rollback")
		}
		if !row.FreshAuthority || !row.SaltReplayRejected || !row.RotationSurvivor || !row.RevokedVictimClosed || row.VictimWritesAfterRevoke != 0 {
			return errors.New("SS fresh authority, salt history, rotation or irreversible revoke lost")
		}
		// Each row holds this actor's cumulative actual primitive ledger. It
		// must extend the previous image's prefix, not reset on reverse.
		for _, old := range before {
			if old.Cut.Actor != cut.Actor {
				continue
			}
			if len(row.Seals) <= len(old.Seals) {
				return errors.New("SS primitive trace stopped or reset across cut")
			}
			for i, event := range old.Seals {
				next := row.Seals[i]
				if event.Actor != next.Actor || event.Digest != next.Digest || event.Nonce != next.Nonce || event.Values != next.Values {
					return errors.New("SS primitive history was rewritten")
				}
			}
		}
		allSeals = append(allSeals, row.Seals...)
		partial := false
		for _, w := range row.Writes {
			if w.Kind != "ss-write" || w.Domain != "ss" || w.Actor != cut.Actor || w.Values[1] > w.Values[0] || w.Values[2] != w.Values[0]-w.Values[1] || uint64(len(w.Data)) != w.Values[2] {
				return errors.New("SS actual ciphertext Write receipt invalid")
			}
			if w.Values[1] > 0 && w.Values[1] < w.Values[0] {
				partial = true
				if !bytes.Equal(w.Data, cut.TXPending[cut.TXCursor:]) {
					return errors.New("SS partial syscall suffix differs from the cut")
				}
			}
		}
		if !partial {
			return errors.New("SS cut lacks an actual partial ciphertext Write")
		}
	}
	if len(seen) != 4 {
		return errors.New("SS cut omitted an original runtime actor")
	}
	return fullstackSSPrimitiveLedger(allSeals)
}

func fullstackCloneSS(rows []fullstackSSCut) []fullstackSSCut {
	rows = slices.Clone(rows)
	for i := range rows {
		r := &rows[i]
		r.Cut.RXPending, r.Cut.TXPending = slices.Clone(r.Cut.RXPending), slices.Clone(r.Cut.TXPending)
		r.Imported.RXPending, r.Imported.TXPending = slices.Clone(r.Imported.RXPending), slices.Clone(r.Imported.TXPending)
		r.Seals, r.Writes = slices.Clone(r.Seals), slices.Clone(r.Writes)
		for j := range r.Seals {
			r.Seals[j].Data = slices.Clone(r.Seals[j].Data)
		}
		for j := range r.Writes {
			r.Writes[j].Data = slices.Clone(r.Writes[j].Data)
		}
	}
	return rows
}

// A runnable component receipt uses the product's actual Seal observation, an
// independent AEAD client, and original loopback sockets. It proves the reader
// consumes real primitives; only the assigned test signs cross-layer rollback.
// origin: hot-update S10 plan §8 M11S / §11.2
func TestFullstackSSPrimitiveLedgerUsesActualProduct(t *testing.T) {
	child := os.Getenv("TETHER_TEST_FULLSTACK_SS_PRIMITIVE_CHILD") == "1"
	trace := new(fullstackSSPrimitiveTrace)
	if child {
		if drillobserve.Hook != nil {
			t.Fatal("SS primitive control requires exclusive private observer ownership")
		}
		// Install once before any product worker starts. Keeping the hook for
		// this child's lifetime also avoids races with asynchronous teardown.
		drillobserve.Hook = func(e drillobserve.Event) {
			if e.Kind == "ss-seal" {
				trace.mu.Lock()
				trace.seals = append(trace.seals, e)
				trace.mu.Unlock()
			}
		}
	}
	exerciseFullstackSSPrimitive(t, child, trace) // Warm netpoll/supervision before the floor.
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFullstackSSPrimitive(t, child, trace)
	}
	testharness.AssertNoGoroutineLeak(t, "SS primitive observer and original sockets", g)
	testharness.AssertNoFDLeak(t, "SS primitive observer and original sockets", fd, 0)
}

type fullstackSSPrimitiveTrace struct {
	mu    sync.Mutex
	seals []drillobserve.Event
}

func exerciseFullstackSSPrimitive(t *testing.T, child bool, trace *fullstackSSPrimitiveTrace) {
	t.Helper()
	if !child {
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, "-test.run=^TestFullstackSSPrimitiveLedgerUsesActualProduct$", "-test.count=1", "-test.v")
		cmd.Env = append(flowSandboxEnvironment(t.TempDir()), "TETHER_TEST_FULLSTACK_SS_PRIMITIVE_CHILD=1")
		if output, err := fullstackharness.CombinedOutput(cmd); err != nil {
			t.Fatalf("supervised SS primitive child: %v\n%s", err, output)
		}
		return
	}
	trace.mu.Lock()
	before := len(trace.seals)
	trace.mu.Unlock()
	exerciseSSProductInterop(t)
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if len(trace.seals)-before < 2 {
		t.Fatal("SS cycle lacks fresh actual primitives")
	}
	if err := fullstackSSPrimitiveLedger(trace.seals); err != nil {
		t.Fatal(err)
	}
}

// These synthetic records test rejection logic only. They are never installed
// as the missing fullstack supplier and cannot produce a continuity receipt.
// origin: hot-update S10 plan §8 M01B/M02F/M03F/M04A/B/M11F/S
func TestFullstackConnectionOraclesRejectLoss(t *testing.T) {
	before := fullstackFlowControl()
	after := before
	for direction := range after.Directions {
		d := &after.Directions[direction]
		d.Sent, d.Received, d.Applied, d.ReplayEnd = 3*flowRecordBytes, 2*flowRecordBytes, 2*flowRecordBytes, 3*flowRecordBytes
		d.Digest = fullstackPrefixDigest(after.Nonce, byte(direction), d.Received)
	}
	if err := fullstackFlowTransition([]fullstackOriginalFlow{before}, []fullstackOriginalFlow{after}, true); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*fullstackOriginalFlow)
	}{
		{"M01B-recreated-owner", func(f *fullstackOriginalFlow) { f.Endpoints[1].Owner.Process.PID++ }},
		{"M02F-replay-lost", func(f *fullstackOriginalFlow) { f.Directions[0].ReplayEnd = 0 }},
		{"M03F-applied-prefix-byte-lost", func(f *fullstackOriginalFlow) { f.Directions[0].Received++ }},
		{"M04A-original-agent-socket-lost", func(f *fullstackOriginalFlow) { f.Endpoints[0].ID = "replacement" }},
		{"M04B-original-broker-socket-lost", func(f *fullstackOriginalFlow) { f.Endpoints[1].ID = "replacement" }},
		{"M11F-double-raw-owner", func(f *fullstackOriginalFlow) { f.Accepts++ }},
		{"trailing-silence", func(f *fullstackOriginalFlow) { f.Directions[1].TailGapNS = int64(flowBusinessDeadline) + 1 }},
		{"lost-reverse-progress", func(f *fullstackOriginalFlow) { f.Directions[1] = before.Directions[1] }},
		{"duplicate-half-close", func(f *fullstackOriginalFlow) { f.Directions[1].CloseWrites = 2 }},
		{"overflow-offset", func(f *fullstackOriginalFlow) { f.Directions[0].Received = ^uint64(0) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := after
			tc.mutate(&bad)
			if fullstackFlowTransition([]fullstackOriginalFlow{before}, []fullstackOriginalFlow{bad}, true) == nil {
				t.Fatal("connection oracle accepted injected loss")
			}
		})
	}
	fullstackInfraOracleControls(t)
	fullstackFlowCheckpointControls(t)
	fullstackSSOracleControls(t)
}

func fullstackFlowCheckpointControls(t *testing.T) {
	t.Helper()
	flow := fullstackFlowControl()
	flow.Endpoints[0].Owner.Process.Key = contract.Key{Role: "agent", NID: "a0"}
	step := contract.Step{ID: "agent-cut", Target: flow.Endpoints[0].Owner.Process.Key}
	checkpoint := fullstackFlowCheckpoint{Base: 3, End: 9, Applied: 4, Received: 6, FIN: true, FINEnd: 9,
		ReplaySHA: [32]byte{1}, PendingSHA: [32]byte{2}, RawReadSHA: [32]byte{3}}
	row := fullstackFlowCut{StepID: step.ID, FlowID: flow.ID, Cut: checkpoint, Imported: checkpoint}
	if err := fullstackFlowCheckpoints(step, []fullstackOriginalFlow{flow}, []fullstackFlowCut{row}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"replay", "applied-minus-one", "applied-plus-one", "pending", "raw-read", "FIN", "empty", "missing"} {
		t.Run("checkpoint-"+name, func(t *testing.T) {
			bad := row
			switch name {
			case "replay":
				bad.Imported.ReplaySHA = [32]byte{}
			case "applied-minus-one":
				bad.Imported.Applied--
			case "applied-plus-one":
				bad.Imported.Applied++
			case "pending":
				bad.Imported.PendingSHA = [32]byte{}
			case "raw-read":
				bad.Imported.RawReadSHA = [32]byte{}
			case "FIN":
				bad.Imported.FIN = false
			case "empty":
				bad.Cut.Base = bad.Cut.End
			case "missing":
				bad.StepID = "different-cut"
			}
			if fullstackFlowCheckpoints(step, []fullstackOriginalFlow{flow}, []fullstackFlowCut{bad}) == nil {
				t.Fatal("flow checkpoint oracle accepted state loss")
			}
		})
	}
}

func fullstackFlowControl() fullstackOriginalFlow {
	f := fullstackOriginalFlow{ID: "original", Kind: "tcp", Nonce: [16]byte{1}, Accepts: 1, Application: contract.Process{PID: 11, StartTicks: 1}}
	for side := range f.Endpoints {
		f.Endpoints[side] = contract.Resource{ID: fmt.Sprintf("original-%d", side), Owner: contract.Stamp{Process: contract.Process{PID: 20 + side}}}
	}
	for direction := range f.Directions {
		f.Directions[direction] = fullstackFlowDirection{Sent: 2 * flowRecordBytes, Received: flowRecordBytes, Applied: flowRecordBytes, ReplayEnd: 2 * flowRecordBytes, MaxGapNS: 1,
			Digest: fullstackPrefixDigest(f.Nonce, byte(direction), flowRecordBytes)}
	}
	return f
}

func fullstackInfraOracleControls(t *testing.T) {
	t.Helper()
	body := []byte("synthetic oracle input; never product evidence\n")
	path := filepath.Join(t.TempDir(), "oracle-raw")
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	step := contract.Step{ID: "infra-cut", Target: contract.Key{Role: "caddy"}}
	row := fullstackInfraCut{StepID: step.ID, FlowID: "original", ServerID: "server", ALPN: "tether-flow/1", CID: 1, SocketCookie: 1, Affected: true, OldOwnersExited: true, CanonicalTLS: 1, WSS: 1, RawTCP: 1, SSH: 1,
		Raw: contract.RawRef{Path: path, Bytes: int64(len(body)), SHA: sha256.Sum256(body), Schema: "oracle-control"}}
	flows := []fullstackOriginalFlow{fullstackFlowControl()}
	if err := fullstackInfraCoverage(step, flows, []fullstackInfraCut{row}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"bypass", "URL-only", "old-owner", "ALPN", "canonical", "missing", "retry", "wrong-cut", "corrupt-raw"} {
		t.Run("infra-"+name, func(t *testing.T) {
			bad := row
			switch name {
			case "bypass":
				bad.Affected = false
			case "URL-only":
				bad.SocketCookie = 0
			case "old-owner":
				bad.OldOwnersExited = false
			case "ALPN":
				bad.ALPN = ""
			case "canonical":
				bad.CanonicalTLS = 0
			case "missing":
				bad.Missing = 1
			case "retry":
				bad.Retried = 1
			case "wrong-cut":
				bad.StepID = "old-cut"
			case "corrupt-raw":
				bad.Raw.SHA[0] ^= 1
			}
			if fullstackInfraCoverage(step, flows, []fullstackInfraCut{bad}) == nil {
				t.Fatal("infra oracle accepted incomplete evidence")
			}
		})
	}
}

func fullstackSSControlRows() []fullstackSSCut {
	var rows []fullstackSSCut
	for i := byte(1); i <= 4; i++ {
		state := fullstackSSCheckpoint{Actor: [16]byte{i}, Flow: [16]byte{i}, RXNonce: [12]byte{4}, TXNonce: [12]byte{6}, RXCursor: 2, TXCursor: 3,
			RXPending: []byte("private-rx"), TXPending: []byte("private-cipher"), AppliedRX: 7, AppliedTX: 9, SaltRevision: 3}
		seals := []drillobserve.Event{{Kind: "ss-seal", Domain: "ss", Actor: state.Actor, Digest: [32]byte{i}, Values: [16]uint64{2}},
			{Kind: "ss-seal", Domain: "ss", Actor: state.Actor, Digest: [32]byte{i}, Nonce: [12]byte{1}, Values: [16]uint64{9}}}
		writes := []drillobserve.Event{{Kind: "ss-write", Domain: "ss", Actor: state.Actor, Values: [16]uint64{uint64(len(state.TXPending)), state.TXCursor, uint64(len(state.TXPending)) - state.TXCursor}, Data: slices.Clone(state.TXPending[state.TXCursor:])}}
		rows = append(rows, fullstackSSCut{StepID: "reverse", Cut: state, Imported: state, Seals: seals, Writes: writes, ActorOwners: 1,
			FreshAuthority: true, SaltReplayRejected: true, RotationSurvivor: true, RevokedVictimClosed: true})
	}
	return rows
}

func fullstackSSOracleControls(t *testing.T) {
	t.Helper()
	step := contract.Step{ID: "reverse", Direction: "reverse"}
	before := fullstackSSControlRows()
	for i := range before {
		before[i].Seals = before[i].Seals[:1]
	}
	if err := fullstackSSContinuity(step, before, fullstackSSControlRows()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"nonce-reset", "parser-lost", "cipher-resealed", "cursor-reset", "M11S-duplicate-Seal", "double-actor", "empty-window", "no-short-write", "salt-history", "rotation", "revoke", "stale-authority", "missing-runtime"} {
		t.Run("SS-"+name, func(t *testing.T) {
			rows := fullstackSSControlRows()
			r := &rows[0]
			switch name {
			case "nonce-reset":
				r.Imported.TXNonce = [12]byte{}
			case "parser-lost":
				r.Imported.RXPending = nil
			case "cipher-resealed":
				r.Imported.TXPending = []byte("replacement")
			case "cursor-reset":
				r.Imported.TXCursor = 0
			case "M11S-duplicate-Seal":
				r.Seals = append(r.Seals, r.Seals[0])
			case "double-actor":
				r.ActorOwners = 2
			case "empty-window":
				r.Cut.RXCursor = 0
			case "no-short-write":
				r.Writes = nil
			case "salt-history":
				r.SaltReplayRejected = false
			case "rotation":
				r.RotationSurvivor = false
			case "revoke":
				r.VictimWritesAfterRevoke = 1
			case "stale-authority":
				r.FreshAuthority = false
			case "missing-runtime":
				rows = rows[:3]
			}
			if fullstackSSContinuity(step, before, rows) == nil {
				t.Fatal("SS oracle accepted private state loss")
			}
		})
	}
}
