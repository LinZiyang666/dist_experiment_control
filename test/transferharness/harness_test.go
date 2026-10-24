package transferharness

// Real: embedded pinned NATS, files, physical messages and lifecycle channels.
// Simulated: unit-level supervisor completion only; no process is spawned there.
// Each leak round also launches a real binary through the FIX-ORPHAN adapter.
// Fatal runner cleanup is exercised by the shared testsupervisor fault suite.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: hot-update S7 plan §11.3 / P29.
// origin: S7 internal review rv-lifecycle F7
func TestTransferLifecycleNoLeaks(t *testing.T) {
	g := runtime.NumGoroutine()
	fds := testharness.FDCount()
	for round := 0; round < 5; round++ {
		receipt := exerciseTransferLifecycle(t)
		// A green receipt-only exercise is not a process lifecycle receipt.
		if receipt.PID <= 0 || receipt.NativeRC != 75 {
			t.Fatalf("round %d started no real supervised process: %+v", round, receipt)
		}
	}
	testharness.AssertNoGoroutineLeak(t, "transfer harness", g)
	testharness.AssertNoFDLeak(t, "transfer harness", fds, 0)
}

func exerciseTransferLifecycle(t *testing.T) Receipt {
	t.Helper()
	root := t.TempDir()
	// Each native result traverses the same output/collection/Stop lifecycle;
	// modelling the helper's channel does not claim a real-binary exit receipt.
	for _, rc := range []int{0, 1, 2, 3, 4, 75} {
		out, err := os.CreateTemp(root, "stdout")
		if err != nil {
			t.Fatal(err)
		}
		errout, err := os.CreateTemp(root, "stderr")
		if err != nil {
			_ = out.Close()
			t.Fatal(err)
		}
		done := make(chan Exit, 1)
		done <- Exit{NativeRC: rc}
		calls := 0
		p := &Process{child: Child{Done: done, Stop: func() error { calls++; return nil }},
			stdout: out, stderr: errout, started: time.Now(), done: make(chan struct{}), receipt: Receipt{NativeRC: -1}}
		pending, err := os.CreateTemp(root, "pending-receipt")
		if err != nil {
			t.Fatal(err)
		}
		err = SaveReceipt(p, pending)
		_ = pending.Close()
		if err == nil {
			t.Fatal("unsettled process published a native receipt")
		}
		go p.collect()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second) // ctx-root: test command lifetime.
		receipt, err := p.Wait(ctx)
		cancel()
		if err != nil || receipt.NativeRC != rc {
			t.Fatalf("native outcome lost: got %d want %d: %v", receipt.NativeRC, rc, err)
		}
		file, err := os.CreateTemp(root, "native-receipt")
		if err != nil {
			t.Fatal(err)
		}
		if err := SaveReceipt(p, file); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		if _, err := file.Seek(0, 0); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		var saved Receipt
		err = json.NewDecoder(file).Decode(&saved)
		_ = file.Close()
		if err != nil || saved.NativeRC != rc {
			t.Fatalf("saved native outcome lost: got %d want %d: %v", saved.NativeRC, rc, err)
		}
		if err := p.Stop(); err != nil {
			t.Fatal(err)
		}
		if err := p.Stop(); err != nil || calls != 1 {
			t.Fatalf("supervisor Stop called %d times: %v", calls, err)
		}
	}
	// The same five-round leak body measures real launches as well as the
	// explicitly modelled receipt/file lifecycle above. An absent adapter must
	// fail the required gate instead of leaving process cleanup NOT_RUN.
	return exerciseSupervisedTransferLifecycle(t)
}

// origin: hot-update S7 plan lead L16 / P29.
func TestTransferHarnessRefusesUnsupervisedLaunch(t *testing.T) {
	h := New(t, nil)
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: test command lifetime.
	defer cancel()
	if _, err := h.Start(ctx, Spec{Binary: "/invalid"}); !errors.Is(err, ErrSupervisionUnavailable) {
		t.Fatalf("unprotected launch accepted: %v", err)
	}
}

// origin: hot-update S7 plan §10.4 / P29.
func TestTransferHarnessArtifactIdentityIsPrivate(t *testing.T) {
	root := t.TempDir()
	artifact := filepath.Join(root, "candidate")
	if err := os.WriteFile(artifact, []byte("candidate A"), 0o700); err != nil {
		t.Fatal(err)
	}
	sha, err := FileSHA(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateSpec(root, Spec{Binary: artifact, ArtifactSHA: sha}); err != nil {
		t.Fatal(err)
	}
	for _, spec := range []Spec{
		{Binary: "tether", ArtifactSHA: sha},
		{Binary: artifact, ArtifactSHA: "wrong"},
		{Binary: artifact, ArtifactSHA: sha, Env: []string{"TETHER_HOME=/home/example/.tether"}},
		{Binary: artifact, ArtifactSHA: sha, Env: []string{"TETHER_HOME=/outside/private"}},
		{Binary: artifact, ArtifactSHA: sha, Args: []string{"--home=/home/example/.tether"}},
	} {
		if err := validateSpec(root, spec); err == nil {
			t.Fatalf("unsafe artifact accepted: %+v", spec)
		}
	}
	if err := validateSpec(filepath.Join(root, "another-fixture"), Spec{Binary: artifact, ArtifactSHA: sha}); err == nil {
		t.Fatal("artifact from another fixture accepted")
	}
	t.Setenv("TETHER_HOME", "/unrelated/home")
	t.Setenv("TETHER_TEST_DRILL_OBSERVER", "v1")
	env := privateEnvironment(root, nil)
	homes := 0
	for _, entry := range env {
		if strings.HasPrefix(entry, "TETHER_TEST_DRILL_") {
			t.Fatal("inherited drill authority escaped fixture")
		}
		if strings.HasPrefix(entry, "TETHER_HOME=") {
			homes++
			if entry != "TETHER_HOME="+filepath.Join(root, "home") {
				t.Fatal("inherited home escaped fixture")
			}
		}
	}
	if homes != 1 {
		t.Fatalf("private home count=%d", homes)
	}
}

// origin: hot-update S7 plan §10.4 / T42. A third physical failed row must
// remain visible after an earlier canonical complete; do not ask a boolean API.
func TestTransferHarnessReadsEveryPhysicalHistoryRow(t *testing.T) {
	s, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded NATS not ready")
	}
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // ctx-root: bounded raw oracle exercise.
	defer cancel()
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "history", Subjects: []string{"history.>"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{`{"kind":"start"}`, `{"kind":"complete"}`} {
		if _, err := js.Publish(ctx, "history.original", []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	rows, first, err := ScanStream(ctx, stream, 0)
	if err != nil || len(rows) != 2 || first.State.LastSeq != 2 {
		t.Fatalf("initial raw history: %+v %v", rows, err)
	}
	if _, err := js.Publish(ctx, "history.original", []byte(`{"kind":"failed"}`)); err != nil {
		t.Fatal(err)
	}
	late, last, err := ScanStream(ctx, stream, first.State.LastSeq)
	if err != nil || len(late) != 1 || string(late[0].Data) != `{"kind":"failed"}` || last.Created != first.Created {
		t.Fatalf("late physical terminal lost: %+v %v", late, err)
	}
	if _, _, err := ScanStream(ctx, stream, 10); err == nil {
		t.Fatal("reversed physical cursor accepted")
	}
	store, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "objectoracle"})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("independent SDK Get format oracle")
	if _, err := store.Put(ctx, jetstream.ObjectMeta{Name: "original"}, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	count, digest, err := ReadObject(ctx, store, "original")
	want := sha256.Sum256(data)
	if err != nil || count != int64(len(data)) || digest != hex.EncodeToString(want[:]) {
		t.Fatalf("SDK Get bytes/hash mismatch: bytes=%d sha=%s err=%v", count, digest, err)
	}
}

// origin: hot-update S7 plan lead L16 / P29. This full real-process exercise
// requires the shared helper, rather than substituting a direct cmd.Start.
// origin: S7 internal review rv-lifecycle F7
func TestTransferHarnessSupervisedBinaryLifecycle(t *testing.T) {
	exerciseSupervisedTransferLifecycle(t)
}

func exerciseSupervisedTransferLifecycle(t *testing.T) Receipt {
	t.Helper()
	if SharedSupervisor == nil {
		t.Fatal("required transfer harness has no shared process supervisor")
	}
	h := New(t, SharedSupervisor(t))
	// Each round closes its harness before the outer leak assertions. Cleanup
	// remains an emergency fallback if a live identity or receipt check fails.
	defer func() {
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	}()
	spec := privateProbeBinary(t, h)
	release, trigger, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = release.Close(); _ = trigger.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // ctx-root: supervised test process.
	defer cancel()
	spec.Args = []string{"-test.run=^TestTransferHarnessBinaryNativeProbe$", "-test.count=1"}
	spec.Env, spec.ExtraFiles = []string{"TRANSFER_HARNESS_PROBE=1"}, []*os.File{release}
	p, err := h.Start(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	_ = release.Close()
	if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool {
		body, err := os.ReadFile(p.Receipt().StdoutPath)
		var pid int
		return err == nil && json.Unmarshal(body, &pid) == nil && pid == p.Receipt().PID && pid != os.Getpid()
	}) {
		t.Fatal("supervised process did not publish its real PID")
	}
	census := captureSupervisedCensus(t, p, spec.ArtifactSHA)
	if _, err := trigger.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	receipt, err := p.Wait(ctx)
	var exit interface{ ExitCode() int }
	if receipt.NativeRC != 75 || !errors.As(err, &exit) || exit.ExitCode() != 75 {
		t.Fatalf("native business refusal lost: rc=%d err=%v", receipt.NativeRC, err)
	}
	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
	assertSupervisedCensusRetired(t, census)
	return receipt
}

// origin: hot-update S7 plan §10.4 / P29.
func TestTransferHarnessBinaryNativeProbe(t *testing.T) {
	if os.Getenv("TRANSFER_HARNESS_PROBE") == "1" {
		if err := json.NewEncoder(os.Stdout).Encode(os.Getpid()); err != nil {
			t.Fatal(err)
		}
		release := os.NewFile(3, "release")
		defer func() { _ = release.Close() }()
		var token [1]byte
		if _, err := io.ReadFull(release, token[:]); err != nil || token[0] != 1 {
			t.Fatalf("native probe release: %v", err)
		}
		os.Exit(75)
	}
}
