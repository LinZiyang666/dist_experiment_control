//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/test/fullstackharness"
	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// These are private test evidence envelopes, not additions to the frozen wire
// DTOs. The S8 supplier keeps each round's runtime alive until the fresh query
// and /proc/exe checks finish. A saved completed record alone cannot pass.
// origin: hot-update S10 plan §5.2 / §6 / P24
type fullstackTaskProof struct {
	Kind, State, Handle, Nonce, TerminalID string
	HandleAfter, NonceAfter, TerminalAfter string
	Before, After                          upgradetx.Resource
	ChildBefore, ChildAfter                c.Process
	CtlBefore, CtlAfter                    c.Process
	Sent, Received                         []byte
	Effects, TerminalCount, WaitCount      int
	ExitCode, ExpectedExitCode             int
	AcceptedNS, ActiveNS, ReverseNS        int64
	LeaseBefore, LeaseAfter                int64
}

type fullstackRollbackProof struct {
	Forward, Reverse transactionRollbackEvidence
	Tasks            []fullstackTaskProof
	Attempts         []c.Attempt
	Views            []c.View
	RequiredEmitters []c.Key
	ObservedEmitters []c.Key
	Deferred         []string
}

// P18 owns argv dispatch and refuses pending native adapters before effects.
// Native stamps are joined to P16's independently archived BOOTTIME stamps;
// Unix timestamps must never be relabeled as monotonic timestamps by this test.
type fullstackQuerySupply struct {
	Profile      c.Profile
	BindingID    string
	Values       map[string]string
	Home         string
	BinarySHA    c.Digest
	StampRoot    string
	CurrentStamp string
	Key          c.Key
	TxID         string
	Target       c.Digest
}

type fullstackRollbackSupply struct {
	Round int
	RunID string
	Proof fullstackRollbackProof
	Query fullstackQuerySupply
	Cuts  []fullstackCutSupply
}

type fullstackCutSupply struct {
	Step         c.Step
	Stage, Apply fullstackQuerySupply
}

// origin: hot-update S10 plan §5.2 / §6 / §10.2 P24
func TestFullstackRollbackRequiresFreshCurrentProof(t *testing.T) {
	t.Run("oracle", fullstackRollbackControls)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseCurrentRollback(t, round) })
	}
	testharness.AssertNoGoroutineLeak(t, "fullstack current rollback both directions x5", beforeG)
	testharness.AssertNoFDLeak(t, "fullstack current rollback both directions x5", beforeFD, 0)
}

func exerciseCurrentRollback(t *testing.T, round int) {
	t.Helper()
	t.Run("kernel-reader", func(t *testing.T) { fullstackExecutableChallenge(t) })
	for _, role := range []string{"agent", "single", "sole-voter", "N3"} {
		t.Run(role, func(t *testing.T) {
			started := time.Now()
			name := fmt.Sprintf("s10/p24/round-%d/rollback/%s", round, role)
			var supplied fullstackRollbackSupply
			if err := readFullstackEvidence(name, &supplied); err != nil {
				t.Fatalf("SETUP-RED: NOT_RUN pending S10 P24 live rollback proof supplier and P18 fresh query-nonce adapter: %s: %v", name, err)
			}
			if supplied.Round != round || supplied.RunID != name {
				t.Fatal("rollback supply reused another round or runtime")
			}
			// Input paths name target-issued grants; the supplier fills reverse
			// inputs only after the candidate accepts all four retained classes.
			// Stage/Apply are never retried after an uncertain native outcome.
			views := fullstackDriveRollback(t, supplied)
			var finished fullstackRollbackSupply
			if err := readFullstackEvidence(name, &finished); err != nil {
				t.Fatal(err)
			}
			if finished.Round != round || finished.RunID != name || !reflect.DeepEqual(finished.Cuts, supplied.Cuts) ||
				!reflect.DeepEqual(finished.Query, supplied.Query) || finished.Proof.Reverse.Role != role ||
				!reflect.DeepEqual(finished.Proof.RequiredEmitters, supplied.Query.Profile.Required) {
				t.Fatal("supplier changed the sealed runtime/actions after execution")
			}
			supplied = finished
			if err := fullstackJudgeRollback(supplied.Proof); err != nil {
				t.Fatal(err)
			}
			for i, proof := range []transactionRollbackEvidence{supplied.Proof.Forward, supplied.Proof.Reverse} {
				if views[i].Stamp.CutID != proof.After.Observation.Current.Handle.CutID ||
					views[i].Stamp.Generation != proof.After.Observation.Current.Handle.Generation {
					t.Fatal("fresh current proof belongs to another forward/reverse cut")
				}
			}
			t.Logf("cost: supplier startup/io/cut/settle in raw; fresh query+kernel+join=%s", time.Since(started))
		})
	}
}

// Exercise the independent kernel reader even when product CLI wiring is
// unavailable. This signs only the reader's real FD lifecycle, never a service
// cut. Synthetic generation fields belong to this test process alone.
// origin: hot-update S10 plan §2.1 / M09A/M09B
func fullstackExecutableChallenge(t *testing.T) {
	t.Helper()
	key := c.Key{Role: "fixture", Host: "local", Unit: "p24-reader", Domain: "test", UID: uint32(os.Getuid())}
	process, err := fullstackharness.ReadKernelProcess(key, os.Getpid(), "p24-reader")
	if err != nil {
		t.Fatal(err)
	}
	stamp := c.Stamp{Process: process, TxID: "reader", CutID: "reader-cut", Direction: "forward", QueryNonce: "reader-challenge", ScopeDigest: "reader-scope", Generation: 1, Revision: 1, Sequence: 1, AtBootNS: 1}
	read, err := fullstackharness.ReadExecutable(t.Context(), stamp, func(context.Context) (c.Stamp, error) {
		stamp.Sequence++
		stamp.AtBootNS++
		return stamp, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := read.Close(); err != nil {
			t.Error(err)
		}
	}()
	file, err := os.Open(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("independent executable hash: %v %v", copyErr, closeErr)
	}
	var target c.Digest
	copy(target[:], hash.Sum(nil))
	if err := read.VerifyTarget(target); err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"agent", "broker"} {
		t.Run(role, func(t *testing.T) {
			wrong := target
			wrong[0] ^= 1
			if err := read.VerifyTarget(wrong); err == nil {
				t.Fatalf("M09 %s: reported SHA overrode real executing bytes", role)
			}
		})
	}
}

func fullstackDriveRollback(t *testing.T, supply fullstackRollbackSupply) []c.View {
	t.Helper()
	if len(supply.Cuts) != 2 || supply.Cuts[0].Step.Direction != "forward" || supply.Cuts[1].Step.Direction != "reverse" {
		t.Fatal("SETUP-RED: both real product cut directions are required")
	}
	driver, err := fullstackharness.NewDriver(supply.Query.Profile)
	if err != nil {
		t.Fatal(err)
	}
	var views []c.View
	for _, cut := range supply.Cuts {
		if cut.Step.Target != supply.Query.Key || cut.Step.From == cut.Step.To {
			t.Fatal("cut target or artifact pair changed")
		}
		action := "apply"
		if cut.Step.Direction == "reverse" {
			action = "rollback"
		}
		for _, binding := range []struct {
			action  string
			command fullstackQuerySupply
		}{{"stage", cut.Stage}, {action, cut.Apply}} {
			if err := fullstackharness.BindProductCLICommand(driver, cut.Step.Target, cut.Step.ID, binding.action, fullstackProductCommand(binding.command)); err != nil {
				t.Fatal(err)
			}
		}
		started := time.Now()
		if _, err := driver.Stage(t.Context(), cut.Step); err != nil {
			if errors.Is(err, fullstackharness.ErrProductCLIPending) || errors.Is(err, c.ErrContractStub) {
				t.Fatalf("SETUP-RED: NOT_RUN pending S10 P18 artifact-stage adapter and S8 P19: %v", err)
			}
			t.Fatalf("product stage failed: %v", err)
		}
		accepted, err := driver.Apply(t.Context(), cut.Step)
		if err != nil {
			t.Fatalf("product %s failed; original transaction must be queried, never resent: %v", action, err)
		}
		query := supply.Query
		query.TxID = accepted.Stamp.TxID
		query.Target = c.Digest{}
		for _, artifact := range query.Profile.Artifacts {
			if artifact.ID == cut.Step.To && artifact.Role == cut.Step.Target.Role {
				query.Target = artifact.Binary
			}
		}
		if query.Target == (c.Digest{}) {
			t.Fatal("target artifact missing from sealed profile")
		}
		current := fullstackFreshQuery(t, query)
		if current.Phase != "completed" || current.Outcome != "success" || current.Stamp.Direction != cut.Step.Direction {
			t.Fatal("serving or historical completion cannot sign the current cut")
		}
		views = append(views, current)
		t.Logf("cost: %s stage+cut+fresh-query+kernel+join=%s", cut.Step.Direction, time.Since(started))
	}
	return views
}

// This checks the union over all emitters and attempts, not just the last
// success or the ordered DeferredReasons list. Unknown is not an empty census.
// origin: hot-update S10 plan §5.2 / P24
func fullstackNoRefusals(e fullstackRollbackProof) error {
	if len(e.RequiredEmitters) == 0 || len(e.Attempts) == 0 || len(e.Views) == 0 || len(e.Deferred) != 0 {
		return fmt.Errorf("normal profile lacks emitter/attempt evidence or contains a deferred reason")
	}
	seen := make(map[c.Key]bool)
	declared := make(map[c.Key]bool)
	for _, key := range e.RequiredEmitters {
		if declared[key] {
			return fmt.Errorf("duplicate required emitter")
		}
		declared[key] = true
	}
	for _, key := range e.ObservedEmitters {
		if seen[key] || !slices.Contains(e.RequiredEmitters, key) {
			return fmt.Errorf("duplicate or unknown deferred emitter")
		}
		seen[key] = true
	}
	for _, key := range e.RequiredEmitters {
		if !seen[key] {
			return fmt.Errorf("missing runtime from the deferred emitter union")
		}
	}
	for _, attempt := range e.Attempts {
		if !attempt.Sent || !attempt.Completed || attempt.RC != 0 || attempt.Code != "" || attempt.Index != 1 {
			return fmt.Errorf("normal profile contains refusal, unknown result or silent retry")
		}
	}
	views := make(map[c.Key]bool)
	for _, view := range e.Views {
		var native upgradetx.Observation
		if err := json.Unmarshal(view.Current, &native); err != nil || view.Code != "" || len(view.Deferred) != 0 ||
			!declared[view.Stamp.Process.Key] ||
			!native.OwnerKnown || !native.Census.Known || !native.Census.Complete ||
			(native.Current.Outcome != upgradetx.OutcomePending && native.Current.Outcome != upgradetx.OutcomeSuccess) {
			return fmt.Errorf("normal profile contains rejected, missing or unknown current evidence")
		}
		views[view.Stamp.Process.Key] = true
	}
	if len(views) != len(declared) {
		return fmt.Errorf("emitter claimed without its raw current View")
	}
	return nil
}

// Both complete directions must have separate attempts, query nonces and
// sequence barriers. Historical receipt nonces deliberately differ from query
// nonces; equating them would reject the valid native S8 completion contract.
func fullstackJudgeRollback(e fullstackRollbackProof) error {
	if err := fullstackNoRefusals(e); err != nil {
		return err
	}
	f, r := e.Forward.After.Observation, e.Reverse.After.Observation
	for _, observation := range []upgradetx.Observation{f, r} {
		if err := upgradetx.Completed(observation.Current, observation); err != nil {
			return fmt.Errorf("fresh current completion: %w", err)
		}
	}
	if err := judgeTransactionRollback(e.Reverse); err != nil {
		return err
	}
	if f.Current.Direction != upgradetx.DirectionForward || r.Current.Direction != upgradetx.DirectionReverse ||
		f.Nonce == r.Nonce || f.Current.Handle.Key != r.Current.Handle.Key ||
		f.Current.Handle.CutID == r.Current.Handle.CutID || r.Current.SourceGeneration != f.Current.Handle.Generation ||
		r.Current.Handle.Generation != f.Current.Handle.Generation+1 || r.Current.SourceSHA != f.Current.TargetSHA ||
		r.Current.TargetSHA != e.Reverse.ForwardHistoricalBefore.RecordedPreviousSHA ||
		e.Forward.ProcRunningSHA != f.Current.TargetSHA || e.Reverse.ProcRunningSHA != r.Current.TargetSHA {
		return fmt.Errorf("M09: forward/reverse identity or independent executing SHA mismatch")
	}
	if len(e.Tasks) != 4 {
		return fmt.Errorf("M05R: missing post-ACTIVE retained task classes")
	}
	want := map[string]bool{"exec/live": false, "pty/live": false, "exec/terminal-unacked": false, "pty/settled-unexpired": false}
	for _, task := range e.Tasks {
		class := task.Kind + "/" + task.State
		seen, ok := want[class]
		if !ok || seen || task.Handle == "" || task.Nonce == "" || task.Before.ID != task.Handle ||
			task.HandleAfter != task.Handle || task.NonceAfter != task.Nonce || task.TerminalAfter != task.TerminalID ||
			task.After.ID != task.Handle || transactionIdentity(task.Before) != transactionIdentity(task.After) ||
			!transactionHasResource(r.Current.Scope.Expected, task.Before) ||
			!transactionHasResource(r.Census.Resources, task.After) || task.After.Generation != r.Current.Handle.Generation ||
			task.Before.Generation != f.Current.Handle.Generation || task.Effects != 1 ||
			task.ActiveNS <= 0 || task.AcceptedNS <= task.ActiveNS || task.ReverseNS <= task.AcceptedNS ||
			task.LeaseBefore <= task.ReverseNS || task.LeaseAfter != task.LeaseBefore ||
			task.ChildBefore.PID <= 0 || task.ChildBefore.StartTicks == 0 || task.ChildBefore != task.ChildAfter ||
			task.CtlBefore.PID <= 0 || task.CtlBefore.StartTicks == 0 || task.CtlBefore != task.CtlAfter ||
			len(task.Sent) == 0 || !bytes.Equal(task.Sent, task.Received) || task.TerminalID == "" ||
			task.TerminalCount != 1 || task.WaitCount != 1 || task.ExitCode != task.ExpectedExitCode {
			return fmt.Errorf("M05R: retained task lost its original identity, bytes, lease or unique terminal")
		}
		want[class] = true
	}
	return nil
}

func fullstackProductCommand(supply fullstackQuerySupply) fullstackharness.ProductCLICommand {
	command := fullstackharness.ProductCLICommand{BindingID: supply.BindingID, Values: supply.Values, Home: supply.Home, BinarySHA: supply.BinarySHA}
	command.Stamp = func(ctx context.Context, native upgradetx.Stamp) (c.Stamp, error) {
		if err := ctx.Err(); err != nil {
			return c.Stamp{}, err
		}
		body, err := json.Marshal(native)
		if err != nil {
			return c.Stamp{}, err
		}
		path := filepath.Join(supply.StampRoot, fmt.Sprintf("%x.json", sha256.Sum256(body)))
		body, err = os.ReadFile(path)
		if err != nil {
			return c.Stamp{}, fmt.Errorf("missing P16 native stamp custody: %w", err)
		}
		var stamp c.Stamp
		err = c.DecodeJSON("stamp", body, &stamp)
		return stamp, err
	}
	return command
}

func fullstackFreshQuery(t *testing.T, supply fullstackQuerySupply) c.View {
	t.Helper()
	if supply.Profile.Tier != "hermetic" || supply.TxID == "" || !filepath.IsAbs(supply.StampRoot) || !filepath.IsAbs(supply.CurrentStamp) ||
		!slices.Contains(supply.Profile.Required, supply.Key) {
		t.Fatal("SETUP-RED: incomplete local runtime query supply")
	}
	driver, err := fullstackharness.NewDriver(supply.Profile)
	if err != nil {
		t.Fatal(err)
	}
	command := fullstackProductCommand(supply)
	if err := fullstackharness.BindProductCLICommand(driver, supply.Key, "", "query", command); err != nil {
		t.Fatal(err)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	view, err := driver.Query(t.Context(), supply.Key, supply.TxID, hex.EncodeToString(nonce[:]))
	if err != nil {
		if errors.Is(err, fullstackharness.ErrProductCLIPending) || errors.Is(err, c.ErrContractStub) {
			t.Fatalf("SETUP-RED: NOT_RUN pending S8 P13/P19 and S10 P18 fresh nonce binding: %v", err)
		}
		t.Fatalf("product current query failed: %v", err)
	}
	// P16's live cursor must advance at every image barrier. Reopening the
	// immutable query archive here would miss a same-PID ABA between queries.
	previous := view.Stamp
	reader := func(ctx context.Context) (c.Stamp, error) {
		if err := ctx.Err(); err != nil {
			return c.Stamp{}, err
		}
		body, err := os.ReadFile(supply.CurrentStamp)
		if err != nil {
			return c.Stamp{}, err
		}
		var current c.Stamp
		if err := c.DecodeJSON("stamp", body, &current); err != nil {
			return c.Stamp{}, err
		}
		if current.Sequence <= previous.Sequence || current.AtBootNS <= previous.AtBootNS {
			return c.Stamp{}, fmt.Errorf("P16 live cursor did not advance during executable sampling")
		}
		previous = current
		return current, nil
	}
	read, err := fullstackharness.ReadExecutable(t.Context(), view.Stamp, reader)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := read.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := read.VerifyTarget(supply.Target); err != nil {
		t.Fatalf("M09: reported target is not the executing image: %v", err)
	}
	return view
}

// Pure oracle controls are separately selectable and never fill a missing
// runtime receipt. Their source overlays prove the guards are load-bearing;
// product mutations still require the real accepted cut, not setup failure.
func fullstackRollbackControl() fullstackRollbackProof {
	r := transactionRollbackOracleControl("R2")
	_, native := transactionOracleControl()
	f := r
	f.After.Observation, f.ProcRunningSHA = native.Observation, native.Attempt.TargetSHA
	f.After.Observation.Nonce = strings.Repeat("6", 32)
	f.After.Observation.Census.Stamp.Nonce = f.After.Observation.Nonce
	f.After.Observation.Census.Stamp.At = f.After.Observation.At
	e := fullstackRollbackProof{Forward: f, Reverse: r}
	key := c.Key{Role: "agent", Host: "local", Unit: "agent", SID: "lab", NID: "node", Domain: "test"}
	e.RequiredEmitters, e.ObservedEmitters = []c.Key{key}, []c.Key{key}
	e.Attempts = []c.Attempt{{Sent: true, Completed: true, Index: 1}}
	body, _ := json.Marshal(r.After.Observation)
	e.Views = []c.View{{Current: body, Stamp: c.Stamp{Process: c.Process{Key: key}}}}
	owner := r.After.Observation.Census.Stamp.Producer
	for i, class := range []string{"exec/live", "pty/live", "exec/terminal-unacked", "pty/settled-unexpired"} {
		kind, state, _ := strings.Cut(class, "/")
		resource := upgradetx.Resource{Domain: upgradetx.Domain(kind), ID: fmt.Sprintf("post-active-%d", i), Origin: "node", Owner: owner, Generation: 2, State: state, Custody: "owned"}
		after := resource
		after.Generation = 3
		process := c.Process{Key: key, PID: 100 + i, StartTicks: 200, BootID: "boot", PIDNS: "pidns", InstanceID: "child"}
		e.Tasks = append(e.Tasks, fullstackTaskProof{Kind: kind, State: state, Handle: resource.ID, Nonce: "original", TerminalID: "terminal",
			HandleAfter: resource.ID, NonceAfter: "original", TerminalAfter: "terminal",
			Before: resource, After: after, ChildBefore: process, ChildAfter: process, CtlBefore: process, CtlAfter: process,
			Sent: []byte("original\x00bytes"), Received: []byte("original\x00bytes"), Effects: 1, TerminalCount: 1, WaitCount: 1,
			ActiveNS: 10, AcceptedNS: 20, ReverseNS: 30, LeaseBefore: 100, LeaseAfter: 100, ExitCode: 23, ExpectedExitCode: 23})
		a := &e.Reverse.After.Observation.Current
		a.Scope.Expected = append(a.Scope.Expected, resource)
		for _, receipt := range []*upgradetx.Receipt{&a.Control, &a.Resources, &a.Released} {
			receipt.Census.Resources = append(slices.Clone(receipt.Census.Resources), after)
		}
		e.Reverse.After.Observation.Census.Resources = append(slices.Clone(e.Reverse.After.Observation.Census.Resources), after)
	}
	slices.SortFunc(e.Reverse.After.Observation.Current.Scope.Expected, func(a, b upgradetx.Resource) int {
		return strings.Compare(transactionIdentity(a), transactionIdentity(b))
	})
	fullstackSealNativeControl(&e.Forward.After.Observation)
	fullstackSealNativeControl(&e.Reverse.After.Observation)
	e.Reverse.CLI.View = e.Reverse.After
	return e
}

func fullstackDropCurrentRetainedTask(e *fullstackRollbackProof) {
	// Keep the independent original task oracle intact while dropping the
	// settled-but-unexpired PTY from every current export/census. A validator
	// comparing only mutually consistent current rows would accept this loss.
	missing := e.Tasks[3].Handle
	a := &e.Reverse.After.Observation.Current
	for _, rows := range []*[]upgradetx.Resource{&a.Scope.Expected, &a.Control.Census.Resources,
		&a.Resources.Census.Resources, &a.Released.Census.Resources, &e.Reverse.After.Observation.Census.Resources} {
		*rows = slices.DeleteFunc(slices.Clone(*rows), func(r upgradetx.Resource) bool { return r.ID == missing })
	}
	e.Reverse.CLI.View = e.Reverse.After
}

// origin: hot-update S10 plan §5.2 / §6 / M05R/M09A/M09B
func fullstackRollbackControls(t *testing.T) {
	t.Run("receipt-codec", func(t *testing.T) {
		var value struct{ Target c.Digest }
		valid := []byte(`{"Target":"` + strings.Repeat("ab", 32) + `"}`)
		if err := fullstackDecode(valid, reflect.ValueOf(&value).Elem()); err != nil || value.Target[0] != 0xab {
			t.Fatalf("canonical digest decode: %v", err)
		}
		for _, bad := range []string{`{"Target":[1,2]}`, `{"Unknown":1}`, `{"Target":"bad"}`, `{}`,
			`{"Target":"` + strings.Repeat("ab", 32) + `","Target":"` + strings.Repeat("ab", 32) + `"}`} {
			if err := fullstackDecode([]byte(bad), reflect.ValueOf(&value).Elem()); err == nil {
				t.Fatal("noncanonical supplier input accepted")
			}
		}
	})
	if err := fullstackJudgeRollback(fullstackRollbackControl()); err != nil {
		t.Fatalf("positive native control: %v", err)
	}
	mutations := map[string]func(*fullstackRollbackProof){
		"M05R":                 fullstackDropCurrentRetainedTask,
		"M09A":                 func(e *fullstackRollbackProof) { e.Forward.ProcRunningSHA = e.Reverse.ProcRunningSHA },
		"M09B":                 func(e *fullstackRollbackProof) { e.Reverse.ProcRunningSHA = e.Forward.ProcRunningSHA },
		"historical-completed": func(e *fullstackRollbackProof) { e.Reverse.After.Observation.OwnerKnown = false },
		"forward-receipt": func(e *fullstackRollbackProof) {
			e.Reverse.After.Observation.Current.Control = e.Forward.After.Observation.Current.Control
		},
		"replayed-nonce": func(e *fullstackRollbackProof) { e.Reverse.After.Observation.Nonce = e.Forward.After.Observation.Nonce },
		"wrong-scope": func(e *fullstackRollbackProof) {
			e.Reverse.After.Observation.Census.Stamp.ScopeDigest = strings.Repeat("0", 64)
		},
		"wrong-revision":  func(e *fullstackRollbackProof) { e.Reverse.After.Observation.Census.Stamp.Attempt.Revision++ },
		"missing-emitter": func(e *fullstackRollbackProof) { e.ObservedEmitters = nil },
		"defer-then-success": func(e *fullstackRollbackProof) {
			e.Attempts = append([]c.Attempt{{Sent: true, Completed: true, RC: 75, Code: "upgrade_deferred"}}, e.Attempts...)
		},
		"hidden-reason":     func(e *fullstackRollbackProof) { e.Deferred = []string{"legacy_busy"} },
		"unknown-census":    func(e *fullstackRollbackProof) { e.Views[0].Current = []byte(`{}`) },
		"duplicate-effect":  func(e *fullstackRollbackProof) { e.Tasks[0].Effects++ },
		"replacement-ctl":   func(e *fullstackRollbackProof) { e.Tasks[0].CtlAfter.StartTicks++ },
		"lease-renewed":     func(e *fullstackRollbackProof) { e.Tasks[3].LeaseAfter++ },
		"terminal-replaced": func(e *fullstackRollbackProof) { e.Tasks[2].TerminalCount++ },
		"bytes-replaced":    func(e *fullstackRollbackProof) { e.Tasks[1].Received = []byte("replacement") },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			e := fullstackRollbackControl()
			mutate(&e)
			if err := fullstackJudgeRollback(e); err == nil {
				t.Fatalf("%s: invalid fullstack rollback proof accepted", name)
			}
		})
	}
	// Keep the native forward history immutable even after the reverse settles.
	e := fullstackRollbackControl()
	if !reflect.DeepEqual(e.Reverse.ForwardHistoricalBefore, e.Reverse.ForwardHistoricalAfter) {
		t.Fatal("control corrupted immutable history")
	}
	t.Run("state-union", func(t *testing.T) {
		// Every phase/stage/outcome combination is exercised in both directions.
		// Only current completed/released/success can sign whole-step success;
		// a historical rolled_back forward record is not the reverse receipt.
		phases := []upgradetx.Phase{upgradetx.PhasePrepared, upgradetx.PhaseStaged, upgradetx.PhaseReady,
			upgradetx.PhaseFreezing, upgradetx.PhaseActivating, upgradetx.PhaseServingNew, upgradetx.PhaseMigrating,
			upgradetx.PhaseCompleted, upgradetx.PhaseDeferred, upgradetx.PhaseRejected, upgradetx.PhaseRollbackPending,
			upgradetx.PhaseRollbackBlocked, upgradetx.PhaseRolledBack, upgradetx.PhaseRecoveryRequired, upgradetx.PhaseCancelled, upgradetx.PhaseAbandoned}
		stages := []upgradetx.CutStage{upgradetx.CutNone, upgradetx.CutPristine, upgradetx.CutPartialActive, upgradetx.CutActive, upgradetx.CutRetiring, upgradetx.CutReleased}
		outcomes := []upgradetx.Outcome{upgradetx.OutcomePending, upgradetx.OutcomeSuccess, upgradetx.OutcomeDeferred,
			upgradetx.OutcomeRejected, upgradetx.OutcomeRolledBack, upgradetx.OutcomeLegacyInterrupting, upgradetx.OutcomeUnknown, upgradetx.OutcomeCancelled}
		for _, observation := range []upgradetx.Observation{e.Forward.After.Observation, e.Reverse.After.Observation} {
			for _, phase := range phases {
				for _, stage := range stages {
					for _, outcome := range outcomes {
						current := observation
						current.Current.Phase, current.Current.CutStage, current.Current.Outcome = phase, stage, outcome
						want := phase == upgradetx.PhaseCompleted && stage == upgradetx.CutReleased && outcome == upgradetx.OutcomeSuccess
						if got := upgradetx.Completed(current.Current, current) == nil; got != want {
							t.Fatalf("state union %s/%s/%s/%s: completion=%t", current.Current.Direction, phase, stage, outcome, got)
						}
					}
				}
			}
		}
	})
}

// Private supplier envelopes retain native S8 JSON tags and canonical P00
// codecs. Default Go reflection would accept a numeric digest array or silently
// misread snake_case keys, turning an integration seam into a different ABI.
// origin: hot-update S10 plan §5.1 / P24
func readFullstackEvidence(name string, target any) error {
	if *transactionEvidenceDir == "" {
		return fmt.Errorf("supervised runtime supply required via -upgrade-transaction-evidence")
	}
	file, err := os.Open(filepath.Join(*transactionEvidenceDir, name+".json"))
	if err != nil {
		return err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, (16<<20)+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return errors.Join(readErr, closeErr)
	}
	if len(data) > 16<<20 {
		return fmt.Errorf("supplier receipt exceeds the bounded archive")
	}
	return fullstackDecode(data, reflect.ValueOf(target).Elem())
}

func fullstackDecode(data []byte, target reflect.Value) error {
	schemas := map[reflect.Type]string{
		reflect.TypeOf(c.Digest{}): "digest", reflect.TypeOf(c.Key{}): "key", reflect.TypeOf(c.Process{}): "process",
		reflect.TypeOf(c.Profile{}): "profile", reflect.TypeOf(c.Step{}): "step", reflect.TypeOf(c.View{}): "view", reflect.TypeOf(c.Attempt{}): "attempt",
	}
	if schema := schemas[target.Type()]; schema != "" {
		return c.DecodeJSON(schema, data, target.Addr().Interface())
	}
	if target.Kind() == reflect.Slice && target.Type().Elem().Kind() != reflect.Uint8 {
		var rows []json.RawMessage
		if err := json.Unmarshal(data, &rows); err != nil {
			return err
		}
		target.Set(reflect.MakeSlice(target.Type(), len(rows), len(rows)))
		for i, row := range rows {
			if err := fullstackDecode(row, target.Index(i)); err != nil {
				return err
			}
		}
		return nil
	}
	if target.Kind() == reflect.Struct {
		// Decode one key at a time so duplicate keys are rejected before a map
		// could overwrite them. Nested native structs take the same path.
		decoder := json.NewDecoder(bytes.NewReader(data))
		start, err := decoder.Token()
		if err != nil || start != json.Delim('{') {
			return fmt.Errorf("supplier object required")
		}
		seen := make(map[string]bool)
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return fmt.Errorf("duplicate supplier field")
			}
			seen[name] = true
			index := -1
			for i := 0; i < target.NumField(); i++ {
				field := target.Type().Field(i)
				wire := strings.Split(field.Tag.Get("json"), ",")[0]
				if wire == "" {
					wire = field.Name
				}
				if wire == name {
					index = i
					break
				}
			}
			if index < 0 {
				return fmt.Errorf("unknown supplier field %q", name)
			}
			var raw json.RawMessage
			if err := decoder.Decode(&raw); err != nil {
				return err
			}
			if err := fullstackDecode(raw, target.Field(index)); err != nil {
				return err
			}
		}
		if _, err := decoder.Token(); err != nil {
			return err
		}
		if _, err := decoder.Token(); err != io.EOF {
			return fmt.Errorf("trailing supplier data")
		}
		for i := 0; i < target.NumField(); i++ {
			field := target.Type().Field(i)
			tag := strings.Split(field.Tag.Get("json"), ",")
			name := tag[0]
			if name == "" {
				name = field.Name
			}
			if !seen[name] && !slices.Contains(tag[1:], "omitempty") {
				return fmt.Errorf("missing supplier field %q", name)
			}
		}
		return nil
	}
	return json.Unmarshal(data, target.Addr().Interface())
}
