// driver_test.go starts supervised protocol children in isolated temporary homes.
// Synthetic S8 DTOs exercise conversion only; no daemon, upgrade or deployment
// success is simulated. Real S8/S9 CLI bindings remain explicitly pending.
package fullstackharness

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// Exact init dispatch keeps this a protocol child, never a nested go test.
func init() {
	if len(os.Args) != 4 || os.Args[1] != "--fullstack-driver-fixture" {
		return
	}
	mode, root := os.Args[2], os.Args[3]
	if os.Getenv("HOME") != root {
		os.Exit(98)
	}
	f, err := os.OpenFile(filepath.Join(root, "effect-"+mode), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		os.Exit(97)
	}
	if _, err = f.WriteString("effect\n"); err != nil {
		os.Exit(96)
	}
	if err = f.Close(); err != nil {
		os.Exit(95)
	}
	switch mode {
	case "normal":
		_, _ = fmt.Fprint(os.Stdout, "native stdout\n")
		_, _ = fmt.Fprint(os.Stderr, "native stderr\n")
		os.Exit(0)
	case "lost":
		os.Exit(74)
	case "refused":
		_, _ = fmt.Fprint(os.Stderr, "legacy_session_busy\n")
		os.Exit(75)
	case "hang":
		// sleep-fixture: cancellation must join this genuinely live child.
		<-time.NewTimer(time.Hour).C
		os.Exit(94)
	default:
		os.Exit(93)
	}
}

// origin: hot-update S10 plan §10.2 P18 / §11.2
// Cost: no product builds/cuts; four supervised children per exercise, one
// canceled after its effect handshake. Five rounds include every sole waiter.
func TestFullstackDriverNeverRepairsProductState(t *testing.T) {
	t.Run("bindings", exerciseProductBindings)
	t.Run("native_view", exerciseProductView)
	t.Run("python_bridge", exerciseProductBridge)
	g, f := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseProductDriver(t)
	}
	testharness.AssertNoGoroutineLeak(t, "product CLI driver", g)
	testharness.AssertNoFDLeak(t, "product CLI driver", f, 0)
}

// The native S8 struct always serializes an unused Reverse as its zero value.
// P00's exact sentinel alternatives must survive the adapter too: an absent
// reverse is never replaced by a fabricated successful transaction.
// origin: hot-update S10 plan §5.1 / §10.2 P18 / upstream native field closure
func TestFullstackDriverPreservesNativeZeroValues(t *testing.T) {
	for _, phase := range []upgradetx.Phase{upgradetx.PhaseCompleted, upgradetx.PhasePrepared} {
		t.Run(string(phase), func(t *testing.T) {
			native, _ := productViewFixture()
			native.Historical.Reverse = upgradetx.Attempt{}
			a := native.Historical.Forward
			if phase == upgradetx.PhasePrepared {
				a.Phase, a.CutStage, a.Outcome = phase, upgradetx.CutNone, upgradetx.OutcomePending
				a.Control, a.Resources, a.Released = upgradetx.Receipt{}, upgradetx.Receipt{}, upgradetx.Receipt{}
			}
			native.Historical.Forward, native.Observation.Current = a, a
			native.Historical.EffectiveSHA = a.TargetSHA
			native.Observation.RunningSHA, native.Observation.EffectiveSHA = a.TargetSHA, a.TargetSHA
			native.Observation.Census.Stamp.Attempt = a.Handle
			data, err := json.Marshal(native)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeProductView(data)
			if err != nil {
				t.Fatalf("native unused Reverse/Receipt sentinels rejected: %v", err)
			}
			if !reflect.DeepEqual(decoded, native) {
				t.Fatal("native zero sentinels were replaced or omitted")
			}
		})
	}
}

func productTestDriver(t *testing.T) (*productDriver, string) {
	t.Helper()
	root := t.TempDir()
	profile := contract.Profile{ID: "driver-hermetic", Topology: "single", Tier: "hermetic", EvidenceRoot: root,
		Required: []contract.Key{{Role: "agent", Host: "host", Unit: "agent.service", SID: "sid", NID: "nid", Domain: "domain", UID: 1000}}}
	profile.CommandBindings = productTestRaw(t, root, "bindings.json", productCommandCatalog, "command_bindings")
	deadline, err := controlbudget.After(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	profile.Steps = []contract.Step{{ID: "R1-agent", Target: profile.Required[0], Action: "upgrade", Direction: "forward", From: "T0", To: "T1", DeadlineNS: int64(deadline)}}
	driver, err := NewDriver(profile)
	if err != nil {
		t.Fatal(err)
	}
	d := driver.(*productDriver)
	home := filepath.Join(root, "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	return d, home
}

func productTestRaw(t *testing.T, root, name string, data []byte, schema string) contract.RawRef {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return contract.RawRef{Path: path, SHA: sha256.Sum256(data), Bytes: int64(len(data)), Schema: schema}
}

func exerciseProductDriver(t *testing.T) {
	t.Helper()
	d, home := productTestDriver(t)
	state := filepath.Join(home, "product")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".prev", "selector", "marker", "journal", "store"} {
		// Writable sentinels make a fixture repair mutation observable rather
		// than letting filesystem permission denial hide the bad write.
		if err := os.WriteFile(filepath.Join(state, name), []byte("original "+name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	before := productState(t, state)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"lost", "normal", "refused"} {
		out, ref, runErr := d.invoke(t.Context(), mode, true, []string{executable, "--fullstack-driver-fixture", mode, home}, home, 5*time.Second)
		wantRC := map[string]int{"normal": 0, "lost": 74, "refused": 75}[mode]
		if ProcessExitCode(runErr) != wantRC {
			t.Fatalf("%s lost native rc: %v", mode, runErr)
		}
		receipt, readErr := readProductRef(ref)
		if readErr != nil {
			t.Fatal(readErr)
		}
		var record struct {
			RC             int      `json:"rc"`
			Argv           []string `json:"argv"`
			Stdout, Stderr []byte
		}
		if err := json.Unmarshal(receipt, &record); err != nil {
			t.Fatal(err)
		}
		if record.RC != wantRC || !bytes.Equal(out, record.Stdout) || len(record.Argv) != 4 {
			t.Fatalf("%s raw command/rc/stdout changed: %+v", mode, record)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(receipt, &fields); err != nil || len(fields) != 7 || fields["stdout"] == nil || fields["stderr"] == nil {
			t.Fatal("raw command field tags differ from fullstack-cli-command-v1")
		}
		if mode == "normal" && (string(out) != "native stdout\n" || string(record.Stderr) != "native stderr\n") {
			t.Fatal("raw streams were filtered")
		}
		if mode == "refused" && string(record.Stderr) != "legacy_session_busy\n" {
			t.Fatal("legacy namespace was aliased")
		}
		// Reconstruct the driver: an in-memory sent bit cannot protect a lost
		// reply across fixture restart. The native child leaves an independent
		// effect count, so refusing twice without the first send also fails.
		reopened, err := NewDriver(d.profile)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = reopened.(*productDriver).invoke(t.Context(), mode, true, []string{executable, "--fullstack-driver-fixture", mode, home}, home, 5*time.Second)
		if !errors.Is(err, ErrProductCLISent) {
			t.Fatalf("unsafe resend after %s was not rejected: %v", mode, err)
		}
		effect, err := os.ReadFile(filepath.Join(home, "effect-"+mode))
		if err != nil || string(effect) != "effect\n" {
			t.Fatalf("%s effect count differs: %q %v", mode, effect, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	type finished struct {
		raw contract.RawRef
		err error
	}
	done := make(chan finished, 1)
	go func() {
		_, raw, err := d.invoke(ctx, "cancel", true, []string{executable, "--fullstack-driver-fixture", "hang", home}, home, 10*time.Second)
		done <- finished{raw, err}
	}()
	ready := testharness.WaitFor(t, 5*time.Second, 5*time.Millisecond, func() bool {
		_, err := os.Stat(filepath.Join(home, "effect-hang"))
		return err == nil
	})
	cancel()
	result := <-done
	if !ready || result.err == nil || result.raw.Path == "" {
		t.Fatalf("cancellation lost started child or raw: ready=%t err=%v", ready, result.err)
	}
	if _, _, err := d.invoke(t.Context(), "partial", true, []string{filepath.Join(home, "missing-cli")}, home, time.Second); err == nil {
		t.Fatal("missing executable returned success")
	}
	cancelled, stop := context.WithCancel(t.Context())
	stop()
	if _, _, err := d.invoke(cancelled, "not-started", true, []string{executable}, home, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-start cancellation changed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(d.profile.EvidenceRoot, "cli-intent-not-started.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("pre-start cancellation reserved an unsafe invocation")
	}
	if after := productState(t, state); !reflect.DeepEqual(before, after) {
		t.Fatalf("driver repaired product state: before=%v after=%v", before, after)
	}
}

func productState(t *testing.T, root string) map[string]string {
	t.Helper()
	state := map[string]string{}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		state[entry.Name()] = fmt.Sprintf("%o:%x", info.Mode(), sha256.Sum256(data))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return state
}

func exerciseProductBindings(t *testing.T) {
	d, home := productTestDriver(t)
	if err := contract.ValidateJSON("command_bindings", productCommandCatalog); err != nil {
		t.Fatal(err)
	}
	step := d.profile.Steps[0]
	for _, action := range []string{"stage", "apply", "query", "rollback"} {
		id := step.ID
		if action == "query" {
			id = ""
		}
		if err := BindProductCLICommand(d, step.Target, id, action, ProductCLICommand{BindingID: "agent." + action, Home: home}); err != nil {
			t.Fatal(err)
		}
	}
	for _, call := range []func() (contract.View, error){
		func() (contract.View, error) { return d.Stage(t.Context(), step) },
		func() (contract.View, error) { return d.Apply(t.Context(), step) },
		func() (contract.View, error) { return d.Query(t.Context(), step.Target, "original-tx", "fresh-nonce") },
	} {
		view, err := call()
		if !errors.Is(err, ErrProductCLIPending) || view.Raw.Path != "" {
			t.Fatalf("pending binding dispatched or succeeded: %+v %v", view, err)
		}
	}
	entries, err := os.ReadDir(d.profile.EvidenceRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "cli-") {
			t.Fatal("pending CLI had side effects")
		}
	}
	expired := step
	expired.DeadlineNS = 1
	d.profile.Steps[0] = expired
	if _, err := d.Apply(t.Context(), expired); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("expired original step deadline was renewed: %v", err)
	}
	d.profile.Steps[0] = step
	var bounded productOutput
	if _, err := bounded.Write(make([]byte, (4<<20)+1)); err == nil || bounded.Len() != 0 {
		t.Fatal("unbounded native CLI output accepted")
	}
	// Reject an arbitrary repair command even when its manifest hash is valid.
	var malicious productBindings
	if err := json.Unmarshal(productCommandCatalog, &malicious); err != nil {
		t.Fatal(err)
	}
	malicious.Bindings[0].Argv = []string{"/bin/cp", "candidate", ".prev"}
	data, _ := json.Marshal(malicious)
	profile := d.profile
	profile.CommandBindings = productTestRaw(t, profile.EvidenceRoot, "malicious.json", data, "command_bindings")
	if _, err := NewDriver(profile); err == nil {
		t.Fatal("fixture repair argv accepted")
	}
	if err := BindProductCLICommand(d, step.Target, step.ID, "apply", ProductCLICommand{BindingID: "agent.apply"}); !errors.Is(err, ErrProductCLISent) {
		t.Fatal("command rebinding accepted")
	}
	values := map[string]string{}
	for _, binding := range d.bindings {
		for _, arg := range binding.Argv {
			if strings.HasPrefix(arg, "{") {
				values[arg] = "/explicit/input"
			}
		}
	}
	backup := productTestRaw(t, d.profile.EvidenceRoot, "backup-receipt", []byte("product backup receipt\n"), "tether-cluster-backup-v1")
	n3 := d.bindings["broker.quorum.apply"]
	argv, err := productArgv(n3, values, backup)
	if err != nil || !slices.Contains(argv, "--broker-only") || !slices.Contains(argv, "--account-seed") {
		t.Fatalf("N3 contract: %v %v", argv, err)
	}
	if _, err := productArgv(n3, values, contract.RawRef{}); err == nil {
		t.Fatal("missing backup receipt accepted")
	}
	broken := backup
	broken.SHA[0] ^= 1
	if _, err := productArgv(n3, values, broken); err == nil {
		t.Fatal("changed backup receipt accepted")
	}
	for _, role := range []string{"nats", "caddy"} {
		for _, topology := range []string{"single", "quorum"} {
			binding := d.bindings[role+"."+topology+".rollback"]
			argv, err := productArgv(binding, values, contract.RawRef{})
			if err != nil || !slices.Contains(argv, "--rollback") || slices.Contains(argv, "--to-version") || slices.Contains(argv, "--expect-sha256") || slices.Contains(argv, "--backup-taken") {
				t.Fatalf("infra rollback contract: %v %v", argv, err)
			}
		}
	}
}

func exerciseProductView(t *testing.T) {
	native, key := productViewFixture()
	raw, err := json.MarshalIndent(native, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeProductView(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, native) {
		t.Fatal("native field/tag mapping lost values")
	}
	ref := productTestRaw(t, t.TempDir(), "native.json", raw, "s8_view")
	stamp := func(_ context.Context, source upgradetx.Stamp) (contract.Stamp, error) {
		return contract.Stamp{Process: contract.Process{Key: key, BootID: source.Producer.BootID, PIDNS: "fixture-pidns", InstanceID: source.Producer.InstanceID, PID: source.Producer.PID, StartTicks: source.Producer.StartTicks},
			TxID: source.Attempt.TxID, CutID: source.Attempt.CutID, Direction: "reverse", QueryNonce: source.Nonce, ScopeDigest: source.ScopeDigest,
			Generation: source.Attempt.Generation, Revision: source.Attempt.Revision, Sequence: source.Sequence, AtBootNS: source.At + 9000}, nil
	}
	project := func(v upgradetx.View, nonce string) (contract.View, error) {
		data, _ := json.Marshal(v)
		return projectProductView(t.Context(), v, data, ref, key, v.Observation.Current.Handle.TxID, nonce, stamp)
	}
	view, err := project(native, native.Observation.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	for _, corrupt := range []func(*contract.Stamp){
		func(s *contract.Stamp) { s.Process.PIDNS = "" },
		func(s *contract.Stamp) { s.Process.Key.Unit = "other-unit" },
		func(s *contract.Stamp) { s.Direction = "forward" },
		func(s *contract.Stamp) { s.Revision++ },
		func(s *contract.Stamp) { s.Sequence++ },
		func(s *contract.Stamp) { s.QueryNonce = "old-query" },
		func(s *contract.Stamp) { s.ScopeDigest = "other-scope" },
	} {
		reader := func(ctx context.Context, source upgradetx.Stamp) (contract.Stamp, error) {
			s, err := stamp(ctx, source)
			corrupt(&s)
			return s, err
		}
		if _, err := productStamp(t.Context(), reader, native.Observation.Census.Stamp, key, upgradetx.DirectionReverse); err == nil {
			t.Fatal("forged independent stamp accepted")
		}
	}
	if view.Stamp.Direction != "reverse" || view.Stamp.AtBootNS == native.Observation.At || view.Control.Stamp.QueryNonce == view.Stamp.QueryNonce {
		t.Fatal("native stamps were conflated")
	}
	var history struct {
		Record   upgradetx.Record   `json:"record"`
		Metadata upgradetx.Metadata `json:"metadata"`
	}
	var current upgradetx.Observation
	if json.Unmarshal(view.Historical, &history) != nil || json.Unmarshal(view.Current, &current) != nil || !reflect.DeepEqual(history.Record, native.Historical) || !reflect.DeepEqual(history.Metadata, native.Metadata) || !reflect.DeepEqual(current, native.Observation) {
		t.Fatal("historical/current raw was reduced")
	}
	if _, err := project(native, "wrong nonce"); err == nil {
		t.Fatal("wrong query nonce accepted")
	}
	for _, change := range []func(*upgradetx.View){
		func(v *upgradetx.View) { v.Observation.OwnerKnown = false },
		func(v *upgradetx.View) { v.Observation.Census.Complete = false },
		func(v *upgradetx.View) { v.Observation.Current.CutStage = upgradetx.CutActive },
		func(v *upgradetx.View) { v.Observation.Current.Released.Stamp.Attempt.Revision++ },
		func(v *upgradetx.View) { v.Observation.Current.Direction = upgradetx.DirectionForward },
		func(v *upgradetx.View) { v.Observation.Current.Resources.Census.Resources = nil },
	} {
		bad, err := decodeProductView(raw)
		if err != nil {
			t.Fatal(err)
		}
		change(&bad)
		if _, err := project(bad, bad.Observation.Nonce); err == nil {
			t.Fatal("invalid current proof accepted")
		}
	}
	for _, bad := range [][]byte{append(bytes.TrimSpace(raw), []byte(` {}`)...), bytes.Replace(raw, []byte(`"metadata":`), []byte(`"unexpected":0,"metadata":`), 1), bytes.Replace(raw, []byte(`"owner_known": true`), []byte(`"owner_known":false,"owner_known":true`), 1)} {
		if _, err := decodeProductView(bad); err == nil {
			t.Fatal("malformed native raw accepted")
		}
	}
}

func productViewFixture() (upgradetx.View, contract.Key) {
	key := contract.Key{Role: "agent", Host: "host", Unit: "agent.service", Domain: "domain", SID: "sid", NID: "nid", UID: 1000}
	txKey := upgradetx.Key{DomainID: key.Domain, Component: upgradetx.ComponentAgent, SID: key.SID, ConfiguredNID: key.NID, UID: key.UID}
	owner := upgradetx.Owner{BootID: "boot", PID: 42, StartTicks: 123, InstanceID: "instance"}
	makeAttempt := func(direction upgradetx.Direction, gen uint64) upgradetx.Attempt {
		a := upgradetx.Attempt{Handle: upgradetx.Handle{Key: txKey, TxID: fmt.Sprintf("%032x", gen), CutID: fmt.Sprintf("%032x", gen+10), Generation: gen, Revision: 20},
			Direction: direction, SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64), SourceGeneration: gen - 1,
			Phase: upgradetx.PhaseCompleted, CutStage: upgradetx.CutReleased, Outcome: upgradetx.OutcomeSuccess,
			Scope: upgradetx.Scope{ID: strings.Repeat("d", 32), Participants: []upgradetx.Key{txKey},
				Domains:  []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade},
				Expected: []upgradetx.Resource{{Domain: upgradetx.DomainExec, ID: "original-child", Origin: "nid", Owner: owner, Generation: gen - 1, State: "running", Custody: "owned"}}}}
		if direction == upgradetx.DirectionReverse {
			a.SourceSHA, a.TargetSHA = a.TargetSHA, a.SourceSHA
		}
		for i, receipt := range []*upgradetx.Receipt{&a.Control, &a.Resources, &a.Released} {
			s := upgradetx.Stamp{Attempt: a.Handle, Producer: owner, Nonce: fmt.Sprintf("%032x", i+100), Sequence: uint64(i + 1), At: int64(i + 100), ScopeDigest: mustProductScopeDigest(a.Scope)}
			resources := slices.Clone(a.Scope.Expected)
			resources[0].Generation = gen
			*receipt = upgradetx.Receipt{Kind: []upgradetx.ReceiptKind{upgradetx.ReceiptControlReady, upgradetx.ReceiptResourcesMigrated, upgradetx.ReceiptOldGenerationReleased}[i], Stamp: s,
				Known: true, Complete: true, RunningSHA: a.TargetSHA, EvidenceDigest: strings.Repeat("e", 64), Census: upgradetx.Census{Stamp: s, Known: true, Complete: true, Resources: resources, ExpectedDigest: mustProductExpectedDigest(a.Scope)}}
		}
		return a
	}
	forward, reverse := makeAttempt(upgradetx.DirectionForward, 2), makeAttempt(upgradetx.DirectionReverse, 3)
	census := reverse.Released.Census
	census.Stamp.Nonce, census.Stamp.At, census.Stamp.Sequence = strings.Repeat("9", 32), 200, 4
	return upgradetx.View{Historical: upgradetx.Record{Version: 2, Key: txKey, Revision: 20, Forward: forward, Reverse: reverse,
		Grant: upgradetx.Grant{Handle: forward.Handle, Mode: upgradetx.ModeHot}, EffectiveSHA: reverse.TargetSHA, RecordedPreviousSHA: reverse.TargetSHA},
		Metadata:    upgradetx.Metadata{TxID: forward.Handle.TxID, Window: upgradetx.WindowOpen, Pins: []string{"source-pin", "target-pin"}},
		Observation: upgradetx.Observation{Nonce: census.Stamp.Nonce, At: census.Stamp.At, Current: reverse, Census: census, OwnerKnown: true, RunningSHA: reverse.TargetSHA, EffectiveSHA: reverse.TargetSHA}}, key
}

func exerciseProductBridge(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("SETUP: Python3 is required for the hermetic CLI bridge selftest")
	}
	script, err := filepath.Abs("../simcluster/drills/lib/fullstack_cli.py")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	out, err := CombinedOutput(exec.CommandContext(ctx, python, "-I", "-B", script, "--selftest"))
	if err != nil {
		t.Fatalf("Python bridge selftest: %v\n%s", err, out)
	}
	if !bytes.Contains(out, []byte("Ran 3 tests")) {
		t.Fatalf("bridge selftest did not run exact cases: %s", out)
	}
}

func mustProductScopeDigest(scope upgradetx.Scope) string {
	digest, err := upgradetx.ScopeDigest(scope)
	if err != nil {
		panic(err)
	}
	return digest
}

func mustProductExpectedDigest(scope upgradetx.Scope) string {
	digest, err := upgradetx.ExpectedDigest(scope)
	if err != nil {
		panic(err)
	}
	return digest
}

// origin: S10 internal review rv3-assembly F3
func TestStepCompletionRequiresNativeReleasedAttempt(t *testing.T) {
	native, key := productViewFixture()
	current := native.Observation
	a := current.Current
	makeView := func(observation upgradetx.Observation) contract.View {
		data, err := json.Marshal(observation)
		if err != nil {
			t.Fatal(err)
		}
		history, err := json.Marshal(struct {
			Record   upgradetx.Record   `json:"record"`
			Metadata upgradetx.Metadata `json:"metadata"`
		}{native.Historical, native.Metadata})
		if err != nil {
			t.Fatal(err)
		}
		return contract.View{Phase: string(observation.Current.Phase), Outcome: string(observation.Current.Outcome), Current: data, Historical: history,
			Stamp: contract.Stamp{Process: contract.Process{Key: key}, QueryNonce: observation.Nonce, TxID: a.Handle.TxID, CutID: a.Handle.CutID,
				Generation: a.Handle.Generation, Revision: a.Handle.Revision, Direction: string(a.Direction)}}
	}
	step := contract.Step{Target: key, Direction: string(a.Direction)}
	if err := RequireStepCompletion(step, makeView(current), current.Nonce); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*upgradetx.Observation){
		func(o *upgradetx.Observation) {
			o.Current.Phase, o.Current.Outcome, o.Current.CutStage = upgradetx.PhaseStaged, upgradetx.OutcomePending, upgradetx.CutNone
		},
		func(o *upgradetx.Observation) {
			o.Current.Released.Stamp.Attempt.CutID = strings.Repeat("a", 32)
			o.Current.Released.Census.Stamp = o.Current.Released.Stamp
		},
		func(o *upgradetx.Observation) {
			o.Census.OldOwners = []upgradetx.Owner{o.Current.Released.Stamp.Producer}
		},
		func(o *upgradetx.Observation) { o.Current.Resources.Census.Resources[0].Generation-- },
		func(o *upgradetx.Observation) { o.Current.Released = upgradetx.Receipt{} },
	} {
		data, _ := json.Marshal(current)
		var bad upgradetx.Observation
		if err := json.Unmarshal(data, &bad); err != nil {
			t.Fatal(err)
		}
		change(&bad)
		if err := RequireStepCompletion(step, makeView(bad), current.Nonce); err == nil {
			t.Fatal("noncompleted or foreign-cut evidence released a step")
		}
	}
	if err := RequireStepCompletion(step, makeView(current), strings.Repeat("a", 32)); err == nil {
		t.Fatal("old query challenge released a step")
	}
}

// origin: S10 internal review rv3-assembly F1
func TestAssemblyExecutableRequiresCurrentSource(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAssemblyExecutable(t)
	}
	testharness.AssertNoGoroutineLeak(t, "candidate source verification", g)
	testharness.AssertNoFDLeak(t, "candidate source verification", fd, 0)
}

func exerciseAssemblyExecutable(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "judge")
	build := exec.CommandContext(t.Context(), "go", "build", "-trimpath", "-buildvcs=false", "-o", output, "./test/hotupdate/evidencecmd")
	build.Dir, build.Env = root, append(os.Environ(), "GOFLAGS=", "GOWORK=off", "GOENV=off", "CGO_ENABLED=0")
	if err := RunProcess(build); err != nil {
		t.Fatal(err)
	}
	if err := VerifyAssemblyExecutable(t.Context(), output, "judge"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := VerifyAssemblyExecutable(t.Context(), output, "judge"); err == nil {
		t.Fatal("unbound judge signed candidate evidence")
	}
	if err := VerifyAssemblyExecutable(t.Context(), output, "host"); !errors.Is(err, ErrFixturePending) {
		t.Fatalf("missing source host: %v", err)
	}
}
