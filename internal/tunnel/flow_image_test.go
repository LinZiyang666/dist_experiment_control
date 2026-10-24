//go:build linux

package tunnel

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// Real: descriptor-driven P7 codec, Prepare, FD duplicates and transfer ledger.
// Constructed: typed cut records for pure projection/rejection assertions.
// The real_carrier branch separately uses real P1/P5/P6 owners and same-PID exec;
// missing owners are a failure, never a skip or a fabricated success.
// origin: hot-update S5 plan §5.1 / §5.3 / §8 T32
func TestClientImagePreparePermitAndFDClosure(t *testing.T) {
	if mode := os.Getenv(clientImageMode); mode != "" {
		runClientImageHelper(t, mode)
		return
	}
	t.Run("projection_and_permit", exerciseClientImageProjection)
	t.Run("strict_codec", exerciseClientImageCodec)
	t.Run("diagnostics", func(t *testing.T) {
		beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseClientImageDiagnostics(t)
		}
		testharness.AssertNoGoroutineLeak(t, "client image diagnostics", beforeG)
		testharness.AssertNoFDLeak(t, "client image diagnostics", beforeFD, 0)
	})
	t.Run("resource_closure", exerciseClientImageClosure)
	t.Run("export_fence", func(t *testing.T) {
		beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseClientImageExportFence(t)
		}
		testharness.AssertNoGoroutineLeak(t, "client image export fence", beforeG)
		testharness.AssertNoFDLeak(t, "client image export fence", beforeFD, 0)
	})
	t.Run("fd_transfer", func(t *testing.T) {
		beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseClientImageFiles(t)
		}
		testharness.AssertNoGoroutineLeak(t, "client image transfers", beforeG)
		testharness.AssertNoFDLeak(t, "client image transfers", beforeFD, 0)
	})
	t.Run("real_carrier", func(t *testing.T) {
		beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseClientImageCarrier(t)
		}
		testharness.AssertNoGoroutineLeak(t, "client image carrier", beforeG)
		testharness.AssertNoFDLeak(t, "client image carrier", beforeFD, 0)
	})
}

// Export racing the lifecycle exit must either fail before borrowing resources
// or leave its successful sealed file in the cut owner's cleanup transaction.
// origin: hot-update S5 plan §5.3 / T32
func exerciseClientImageExportFence(t *testing.T) {
	t.Helper()
	c := NewClient("", "export-fence", "export-node", nil, silentLog())
	if err := c.SetFlowConfig(FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "export-fence", BootID: "boot", OwnerGen: 1}); err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(c.flowState.manager.Close)
	frozen, err := c.FreezeFlows(time.Now().Add(ResumeAttempt))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(frozen.Discard)
	type result struct {
		file *os.File
		err  error
	}
	results := make(chan result, 8)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := 0; worker < cap(results); worker++ {
		workers.Go(func() {
			<-start
			im, snapshot, err := frozen.Export()
			var file *os.File
			if err == nil {
				for _, res := range snapshot.Resources {
					if res.ID == im.StateRef {
						file = res.Files["state"]
					}
				}
			}
			results <- result{file, err}
		})
	}
	close(start)
	err = frozen.Discard()
	workers.Wait()
	if err != nil {
		t.Fatal(err)
	}
	for worker := 0; worker < cap(results); worker++ {
		r := <-results
		if r.err != nil {
			if !errors.Is(r.err, errFlowState) {
				t.Fatalf("export crossed lifecycle exit: %v", r.err)
			}
			continue
		}
		if r.file == nil {
			t.Fatal("successful export lost its sealed descriptor")
		}
		if _, err := r.file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("concurrent client export escaped lifecycle cleanup")
		}
	}
}

func clientImageRecord(kind string, id byte) sealedstate.Record {
	rec := sealedstate.Record{Kind: kind}
	for _, d := range sealedstate.RecordDescriptors {
		if d.Kind != kind {
			continue
		}
		for _, fd := range d.Fields {
			f := sealedstate.Field{Name: fd.Name, Type: fd.Type}
			if fd.Type >= sealedstate.FieldBytes && fd.Width != 0 {
				f.Bytes = make([]byte, fd.Width)
			}
			if f.Name == "id" {
				f.Bytes[0] = id
			}
			rec.Fields = append(rec.Fields, f)
		}
	}
	return rec
}

func clientImageSet(rec *sealedstate.Record, name string, n uint64, p []byte) {
	for i := range rec.Fields {
		if rec.Fields[i].Name == name {
			rec.Fields[i].U64, rec.Fields[i].Bytes = n, p
			return
		}
	}
	panic("missing fixture field")
}

func clientImageFixture() sealedstate.CutState {
	epID := [16]byte{1}
	flowIDs := [][16]byte{{2}, {3}}
	ep := clientImageRecord(sealedstate.KindEndpoint, epID[0])
	clientImageSet(&ep, "phase", uint64(EndpointPhasePrepared), nil)
	clientImageSet(&ep, "owner_gen", 2, nil)
	clientImageSet(&ep, "carrier_gen", 9, nil)
	clientImageSet(&ep, "flow_ids", 0, append(append([]byte(nil), flowIDs[0][:]...), flowIDs[1][:]...))
	cut := sealedstate.CutState{Records: []sealedstate.Record{ep}}
	for _, id := range flowIDs {
		rec := clientImageRecord(sealedstate.KindFlow, id[0])
		ref := "flow." + hex.EncodeToString(id[:])
		clientImageSet(&rec, "endpoint_id", 0, epID[:])
		clientImageSet(&rec, "owner_gen", 2, nil)
		clientImageSet(&rec, "open_seq", uint64(id[0]-1), nil)
		clientImageSet(&rec, "phase", uint64(flow.FlowPhaseActive), nil)
		clientImageSet(&rec, "tx_end", 7, nil)
		clientImageSet(&rec, "raw_ref", 0, []byte(ref))
		clientImageSet(&rec, "replay_ref", 0, []byte(ref))
		cut.Records = append(cut.Records, rec)
		cut.Resources = append(cut.Resources, handoff.Resource{ID: ref, Kind: "flow-v1", Files: map[string]*os.File{"raw": nil, "replay": nil}})
	}
	permit := clientImageRecord(sealedstate.KindPermit, 4)
	clientImageSet(&permit, "endpoint_id", 0, epID[:])
	clientImageSet(&permit, "owner_gen", 1, nil)
	clientImageSet(&permit, "next_owner_gen", 2, nil)
	clientImageSet(&permit, "purpose", uint64(TicketPurposeHandoff), nil)
	clientImageSet(&permit, "cut_id", 0, bytes.Repeat([]byte{5}, 16))
	clientImageSet(&permit, "consumed", 1, nil)
	clientImageSet(&permit, "request_id", 0, bytes.Repeat([]byte{6}, 16))
	clientImageSet(&permit, "ticket", 0, []byte("private-handoff-ticket-7c2f934e"))
	receipt := clientImageRecord(sealedstate.KindReceipt, 7)
	clientImageSet(&receipt, "endpoint_id", 0, epID[:])
	clientImageSet(&receipt, "owner_gen", 2, nil)
	clientImageSet(&receipt, "carrier_gen", 9, nil)
	clientImageSet(&receipt, "request_id", 0, bytes.Repeat([]byte{6}, 16))
	clientImageSet(&receipt, "cut_id", 0, bytes.Repeat([]byte{5}, 16))
	cut.Records = append(cut.Records, permit, receipt)
	return cut
}

func exerciseClientImageProjection(t *testing.T) {
	cut := clientImageFixture()
	im, err := clientImageFromCut(cut, "flow-state."+strings.Repeat("05", 16))
	if err != nil {
		t.Fatal(err)
	}
	if im.Version != 1 || im.Flows.Version != 1 || len(im.Endpoints) != 1 || len(im.Endpoints[0].FlowIDs) != 2 || len(im.Flows.Records) != 2 || im.Flows.ReservedBytes != 2*workload.DefaultReplayCapacity {
		t.Fatal("public projection lost an endpoint, flow or capacity reservation")
	}
	for _, rec := range im.Flows.Records {
		want := "flow." + hex.EncodeToString(rec.ID[:])
		if rec.RawRef != want || rec.ReplayRef != want {
			t.Fatal("resource IDs must use the frozen dot-separated typed identity")
		}
	}
	data, err := encodeClientCut(cut)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := decodeClientCut(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cut.Records, restored.Records) {
		t.Fatal("permit or cached receipt changed across the private cut")
	}
	f := clientFields(restored.Records[3])
	if f["consumed"].U64 != 1 || f["next_owner_gen"].U64 != 2 || !bytes.Equal(f["request_id"].Bytes, bytes.Repeat([]byte{6}, 16)) {
		t.Fatal("lost-reply restore would consume the permit twice")
	}
	if _, err := clientImageFromCut(cut, "flow-state."+strings.Repeat("06", 16)); !errors.Is(err, sealedstate.ErrInvalid) {
		t.Fatal("public state reference was not tied to the permit cut")
	}
	public, err := json.Marshal(im)
	if err != nil || bytes.Contains(public, []byte("private-handoff-ticket")) || bytes.Contains(public, []byte("request_id")) || bytes.Contains(public, []byte("consumed")) {
		t.Fatal("permit secrets escaped into the public image")
	}
}

func exerciseClientImageCodec(t *testing.T) {
	cut := clientImageFixture()
	data, err := encodeClientCut(cut)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{nil, data[:len(data)-1], append(append([]byte(nil), data...), 0), bytes.Repeat([]byte{0xff}, 32)} {
		if _, err := decodeClientCut(bad); err == nil {
			t.Fatal("decoder accepted malformed or trailing private data")
		}
	}
	cases := []struct {
		name string
		edit func(*sealedstate.CutState)
	}{
		{"missing_consumed", func(c *sealedstate.CutState) { c.Records[3].Fields = c.Records[3].Fields[1:] }},
		{"duplicate_field", func(c *sealedstate.CutState) { c.Records[3].Fields[1] = c.Records[3].Fields[0] }},
		{"case_alias", func(c *sealedstate.CutState) { c.Records[3].Fields[0].Name = "Consumed" }},
		{"bad_boolean", func(c *sealedstate.CutState) { clientImageSet(&c.Records[3], "consumed", 2, nil) }},
		{"overflow", func(c *sealedstate.CutState) { clientImageSet(&c.Records[3], "deadline_boot_ns", ^uint64(0), nil) }},
		{"unknown_phase", func(c *sealedstate.CutState) { clientImageSet(&c.Records[0], "phase", 99, nil) }},
		{"duplicate_permit", func(c *sealedstate.CutState) { c.Records = append(c.Records, c.Records[3]) }},
		{"unconsumed_new_owner", func(c *sealedstate.CutState) { clientImageSet(&c.Records[3], "consumed", 0, nil) }},
		{"owner_jump", func(c *sealedstate.CutState) { clientImageSet(&c.Records[3], "next_owner_gen", 3, nil) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := clientImageFixture()
			tc.edit(&bad)
			switch tc.name {
			case "missing_consumed", "duplicate_field", "case_alias", "bad_boolean", "overflow", "duplicate_permit":
				if err := validateClientRecords(bad.Records); !errors.Is(err, sealedstate.ErrInvalid) {
					t.Fatal("private field schema accepted an invalid decision")
				}
			default:
			}
			if _, err := encodeClientCut(bad); !errors.Is(err, sealedstate.ErrInvalid) {
				t.Fatalf("invalid private decision accepted: %v", err)
			}
		})
	}
}

func exerciseClientImageClosure(t *testing.T) {
	if clientImageName("flow:0100", 256) || clientStateRef("flow-state:"+strings.Repeat("05", 16)) {
		t.Fatal("resource names accepted a colon-separated identity")
	}
	cases := []struct {
		name string
		edit func(*sealedstate.CutState)
	}{
		{"colon_id", func(c *sealedstate.CutState) { c.Resources[0].ID = strings.Replace(c.Resources[0].ID, ".", ":", 1) }},
		{"missing_raw", func(c *sealedstate.CutState) { delete(c.Resources[1].Files, "raw") }},
		{"active_without_raw", func(c *sealedstate.CutState) {
			clientImageSet(&c.Records[1], "raw_ref", 0, nil)
			delete(c.Resources[0].Files, "raw")
		}},
		{"active_without_replay", func(c *sealedstate.CutState) {
			clientImageSet(&c.Records[1], "replay_ref", 0, nil)
			delete(c.Resources[0].Files, "replay")
		}},
		{"missing_flow_resource", func(c *sealedstate.CutState) { c.Resources = c.Resources[:1] }},
		{"extra_fd", func(c *sealedstate.CutState) { c.Resources[0].Files["listener"] = nil }},
		{"swapped_endpoint", func(c *sealedstate.CutState) {
			clientImageSet(&c.Records[1], "endpoint_id", 0, bytes.Repeat([]byte{8}, 16))
		}},
		{"duplicate_flow", func(c *sealedstate.CutState) { c.Records = append(c.Records, c.Records[1]) }},
		{"undeclared_flow", func(c *sealedstate.CutState) { clientImageSet(&c.Records[0], "flow_ids", 0, make([]byte, 16)) }},
		{"secret_data", func(c *sealedstate.CutState) { c.Resources[0].Data = []byte(`{"ticket":"private-handoff-ticket"}`) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cut := clientImageFixture()
			tc.edit(&cut)
			if _, err := clientImageFromCut(cut, ""); !errors.Is(err, sealedstate.ErrInvalid) {
				t.Fatalf("resource closure accepted %s: %v", tc.name, err)
			}
		})
	}
}

func exerciseClientImageFiles(t *testing.T) {
	// These are real OS files; this branch tests ownership, not rawsocket identity.
	// Actual TCP identity and permit CAS are exercised by real_carrier below.
	cut := clientImageFixture()
	var originals []*os.File
	defer func() {
		for _, f := range originals {
			_ = f.Close()
		}
	}()
	for i := range cut.Resources {
		for name := range cut.Resources[i].Files {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			originals = append(originals, r, w)
			cut.Resources[i].Files[name] = r
		}
	}
	prepared, err := handoff.Prepare(handoff.State{Role: "agent", Instance: "client-image-test", Generation: 2}, cut.Resources)
	if err != nil {
		t.Fatalf("real Prepare rejected frozen resource names: %v", err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	for _, f := range originals {
		if _, err := f.Stat(); err != nil {
			t.Fatal("Prepared.Close closed a borrowed original")
		}
	}
	data, err := encodeClientCut(cut)
	if err != nil {
		t.Fatal(err)
	}
	directory, err := decodeClientCut(data)
	if err != nil {
		t.Fatal(err)
	}
	src := handofftest.DupResources(t, cut.Resources)
	salvage, err := takeClientFiles(&directory, src)
	if err != nil || src.Remaining() != 0 || len(salvage) != 2 {
		t.Fatalf("not all four business descriptors were transferred: remaining=%d err=%v", src.Remaining(), err)
	}
	for _, res := range salvage {
		if len(res.Files) != 2 || res.Files["raw"] == nil || res.Files["replay"] == nil {
			t.Fatal("salvage omitted a raw or replay descriptor")
		}
	}
	if again, err := takeClientFiles(&directory, src); err == nil || len(again) != 0 {
		t.Fatal("resource source allowed a second consumption")
	}
	closeClientImageResources(salvage)
	src.Close()
	// A late failure must preserve files from earlier resources, while Borrow
	// retains unattempted files. Together they must be a valid redispatch cut.
	directory, err = decodeClientCut(data)
	if err != nil {
		t.Fatal(err)
	}
	failed := handofftest.DupResources(t, cut.Resources)
	failed.Drop(cut.Resources[1].ID, "replay")
	salvage, err = takeClientFiles(&directory, failed)
	if err == nil || len(salvage) != 2 || len(salvage[0].Files) != 2 || len(salvage[1].Files) != 1 {
		t.Fatal("late Take refusal lost previously transferred files")
	}
	merged, err := handoff.MergeResources(salvage, failed.Borrow())
	if err != nil || len(merged) != 2 {
		t.Fatal("salvage and borrowed remainder cannot be redispatched")
	}
	closeClientImageResources(salvage)
	failed.Close()
}

func closeClientImageResources(resources []handoff.Resource) {
	for _, res := range resources {
		for _, file := range res.Files {
			if file != nil {
				_ = file.Close()
			}
		}
	}
}

const clientImageMode = "TETHER_TEST_CLIENT_IMAGE_MODE"

const clientImageToken = "image-token-82e3112e96c041fa"
const clientImagePreviousPin = "sha256:previous-pin-21323482cdef"

var clientImagePinUntil = time.Unix(4102444800, 123456789)

// exec.Cmd copies stderr concurrently with the protocol reader's assertions.
// Diagnostic reads must not race the copy goroutine, including failure paths.
// origin: hot-update S5 plan §8 / T32
type clientImageDiagnostics struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (d *clientImageDiagnostics) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.buffer.Write(p)
}

func (d *clientImageDiagnostics) String() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.buffer.String()
}

func exerciseClientImageDiagnostics(t *testing.T) {
	t.Helper()
	var d clientImageDiagnostics
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Go(func() {
		<-start
		for range 1000 {
			_, _ = d.Write([]byte("child diagnostic\n"))
		}
	})
	workers.Go(func() {
		<-start
		for range 1000 {
			_ = d.String()
		}
	})
	close(start)
	workers.Wait()
	if strings.Count(d.String(), "child diagnostic\n") != 1000 {
		t.Fatal("concurrent diagnostic capture lost bytes")
	}
}

func exerciseClientImageCarrier(t *testing.T) {

	// ctx-root: this test owns the server and every local connection.
	ctx, cancel := context.WithTimeout(context.Background(), 10*ResumeAttempt)
	defer cancel()
	budget := &workload.ReplayBudget{}
	var blockReply, droppedReply, armReplyFault atomic.Bool
	cfg := FlowConfig{Budget: budget, InstanceID: "client-image-test", BootID: "client-image-boot", OwnerGen: 1, MaxEndpoints: MaxEndpoints, MaxPendingFDs: flow.MaxPendingFDs,
		TokenLookup: func(AuthRequest) error { return nil },
		Authorize: func(r AuthRequest) (Authority, error) {
			if armReplyFault.Load() {
				blockReply.Store(true)
			}
			return Authority{Enabled: true, Allocation: r.Allocation, AppliedRevision: 1}, nil
		},
	}
	srv := NewServer("127.0.0.1:0", "127.0.0.1", func(string, string, int, string, int64) error { return nil }, silentLog())
	if err := srv.SetFlowConfig(cfg); err != nil {
		t.Fatalf("NOT_RUN pending P1: real flow server configuration: %v", err)
	}
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(ln.Close)
	var accepts atomic.Int32
	var workers sync.WaitGroup
	var targetsMu sync.Mutex
	var targets []net.Conn
	var targetsClosing bool
	workers.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			targetsMu.Lock()
			if targetsClosing {
				targetsMu.Unlock()
				_ = conn.Close()
				return
			}
			targets = append(targets, conn)
			targetsMu.Unlock()
			workers.Go(func() {
				defer func(closeFunc func() error) { _ = closeFunc() }(conn.Close)
				_, _ = io.Copy(struct{ io.Writer }{conn}, struct{ io.Reader }{conn})
			})
		}
	})
	defer func() {
		_ = ln.Close()
		targetsMu.Lock()
		targetsClosing = true
		for _, conn := range targets {
			_ = conn.Close()
		}
		targetsMu.Unlock()
		workers.Wait()
	}()
	port := findFreePort(t)
	secondPort := findFreePort(t)
	for secondPort == port {
		secondPort = findFreePort(t)
	}
	srv.mu.Lock()
	control := srv.ln.Addr().String()
	srv.mu.Unlock()
	control, stopRelay := clientImageReplyRelay(t, control, &blockReply, &droppedReply)
	defer stopRelay()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestClientImagePreparePermitAndFDClosure$", "-test.count=1")
	cmd.Env = append(handoff.CleanEnvironment(os.Environ()), clientImageMode+"=before", "TETHER_TEST_CLIENT_CONTROL="+control, "TETHER_TEST_CLIENT_PUBLIC="+strconv.Itoa(port), "TETHER_TEST_CLIENT_PUBLIC_EXTRA="+strconv.Itoa(secondPort), "TETHER_TEST_CLIENT_LOCAL="+strconv.Itoa(ln.Addr().(*net.TCPAddr).Port))
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr clientImageDiagnostics
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	lines := bufio.NewReader(stdout)
	if line, err := lines.ReadString('\n'); err != nil || line != "CLIENT_READY\n" {
		t.Fatalf("client did not create the real endpoint: %q %v", line, err)
	}
	var public []net.Conn
	defer func() {
		for _, c := range public {
			_ = c.Close()
		}
	}()
	for i, publicPort := range []int{port, secondPort} {
		c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort)), ResumeAttempt)
		if err != nil {
			t.Fatal(err)
		}
		public = append(public, c)
		clientImageEcho(t, c, fmt.Sprintf("before-%d", i))
	}
	if accepts.Load() != 2 {
		t.Fatal("two original target connections were not established")
	}
	_, _ = io.WriteString(stdin, "CUT\n")
	line, err := lines.ReadString('\n')
	if err != nil || line != "CLIENT_IMPORTED\n" {
		t.Fatalf("same-PID frozen import failed: %q %v; %s", line, err, stderr.String())
	}
	armReplyFault.Store(true)
	_, _ = io.WriteString(stdin, "RESUME\n")
	line, err = lines.ReadString('\n')
	if err != nil || line != "CLIENT_REPLY_LOST\n" || !droppedReply.Load() {
		t.Fatalf("lost-reply fault was not observed: %q %v", line, err)
	}
	armReplyFault.Store(false)
	blockReply.Store(false)
	_, _ = io.WriteString(stdin, "RETRY\n")
	line, err = lines.ReadString('\n')
	if err != nil || line != "CLIENT_RESUMED\n" {
		t.Fatalf("same-PID import/resume failed: %q %v; %s", line, err, stderr.String())
	}
	for i, conn := range public {
		clientImageEcho(t, conn, fmt.Sprintf("after-%d", i))
	}
	if accepts.Load() != 2 {
		t.Fatal("restore or repeated RESUME redialed an original target")
	}
	_, _ = io.WriteString(stdin, "DONE\n")
	// Drain testing's failure output before Wait closes StdoutPipe. The helper
	// runs the current binary; a later cleanup failure must remain reviewable.
	// origin: hot-update S5 plan §8 / T32
	tail, readErr := io.ReadAll(lines)
	if err := cmd.Wait(); err != nil {
		t.Fatalf("client exec helper failed: %v; %s; %s", err, stderr.String(), tail)
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
}

// The authorization callback arms the fault before the broker can emit its
// receipt. The relay observes actual response bytes, withholds all of them,
// then closes that carrier; it never generates or edits a protocol frame.
// origin: hot-update S5 plan §4.4 / §8 T32
func clientImageReplyRelay(t *testing.T, upstream string, block, dropped *atomic.Bool) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	var mu sync.Mutex
	var conns []net.Conn
	var closing bool
	workers.Go(func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			server, err := net.DialTimeout("tcp", upstream, ResumeAttempt)
			if err != nil {
				_ = client.Close()
				continue
			}
			mu.Lock()
			if closing {
				mu.Unlock()
				_ = client.Close()
				_ = server.Close()
				return
			}
			conns = append(conns, client, server)
			mu.Unlock()
			workers.Go(func() {
				defer func(closeFunc func() error) { _ = closeFunc() }(client.Close)
				defer func(closeFunc func() error) { _ = closeFunc() }(server.Close)
				_, _ = io.Copy(struct{ io.Writer }{server}, struct{ io.Reader }{client})
			})
			workers.Go(func() {
				defer func(closeFunc func() error) { _ = closeFunc() }(client.Close)
				defer func(closeFunc func() error) { _ = closeFunc() }(server.Close)
				p := make([]byte, 32<<10)
				for {
					n, err := server.Read(p)
					if n != 0 {
						if block.Load() {
							dropped.Store(true)
							return
						}
						if _, writeErr := io.Copy(client, bytes.NewReader(p[:n])); writeErr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			})
		}
	})
	return ln.Addr().String(), func() {
		_ = ln.Close()
		mu.Lock()
		closing = true
		for _, conn := range conns {
			_ = conn.Close()
		}
		mu.Unlock()
		workers.Wait()
	}
}

func clientImageEcho(t *testing.T, conn net.Conn, text string) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, text); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(text))
	if _, err := io.ReadFull(conn, got); err != nil || string(got) != text {
		t.Fatal("original business socket lost or duplicated bytes")
	}
}

func runClientImageHelper(t *testing.T, mode string) {
	cfg := FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "client-image-test", BootID: "client-image-boot", OwnerGen: 1, MaxEndpoints: MaxEndpoints, MaxPendingFDs: flow.MaxPendingFDs}
	input := bufio.NewReader(os.Stdin)
	if mode == "before" {
		local, err := strconv.Atoi(os.Getenv("TETHER_TEST_CLIENT_LOCAL"))
		if err != nil {
			t.Fatal(err)
		}
		port, err := strconv.Atoi(os.Getenv("TETHER_TEST_CLIENT_PUBLIC"))
		if err != nil {
			t.Fatal(err)
		}
		secondPort, err := strconv.Atoi(os.Getenv("TETHER_TEST_CLIENT_PUBLIC_EXTRA"))
		if err != nil {
			t.Fatal(err)
		}
		c := NewClient(os.Getenv("TETHER_TEST_CLIENT_CONTROL"), "image-test", "image-node", func(int) (int, error) { return local, nil }, silentLog())
		if err := c.SetFlowConfig(cfg); err != nil {
			t.Fatal(err)
		}
		// ctx-root: the helper owns the pre-exec client lifetime.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c.Start(ctx)
		for _, publicPort := range []int{port, secondPort} {
			if err := c.Open(publicPort, local, clientImageToken); err != nil {
				t.Fatal(err)
			}
			c.mu.Lock()
			secure := c.sessions[publicPort].conn.(*tls.Conn)
			current := certFingerprintDER(secure.ConnectionState().PeerCertificates[0].Raw)
			c.mu.Unlock()
			if err := c.ApplyHome(publicPort, c.brokerAddr, 0, proto.CertPins{Current: current, Previous: clientImagePreviousPin, ValidUntil: &clientImagePinUntil}); err != nil {
				t.Fatal(err)
			}
		}
		fmt.Println("CLIENT_READY")
		if line, err := input.ReadString('\n'); err != nil || line != "CUT\n" {
			t.Fatal("missing cut handshake")
		}
		cutID := [16]byte{42}
		if err := c.PrepareHandoff(2, cutID, time.Now().Add(ResumeAttempt)); err != nil {
			t.Fatal(err)
		}
		frozen, err := c.FreezeFlows(time.Now().Add(ResumeAttempt))
		if err != nil {
			t.Fatal(err)
		}
		// The retry request is part of the private cut even when the permit
		// can reconstruct activation; losing it would erase an in-flight RPC.
		// origin: hot-update S5 plan §4.4 / §5.1 / T32
		cut, err := frozen.SnapshotState()
		if err != nil {
			t.Fatal(err)
		}
		for _, rec := range cut.Records {
			if rec.Kind != sealedstate.KindEndpoint {
				continue
			}
			request, err := decodeResumeRequest(clientFields(rec)["pending_resume"].Bytes)
			if err != nil || request.RequestID == ([16]byte{}) || request.Cut != cutID || request.Mode != flow.ResumeModeActivate || request.Scope != flow.ResumeScopeEndpoint {
				t.Fatal("cut lost the pending authorization request")
			}
		}
		im, snapshot, err := frozen.Export()
		if err != nil {
			t.Fatal(err)
		}
		if len(im.Flows.Records) != 2 || len(im.Endpoints) != 2 {
			t.Fatal("real cut omitted one original flow")
		}
		data, err := json.Marshal(struct {
			PID   int
			Image ClientImage
		}{os.Getpid(), im})
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte(clientImageToken)) || bytes.Contains(data, []byte(clientImagePreviousPin)) {
			t.Fatal("private route state escaped into the public image")
		}
		p, err := handoff.Prepare(handoff.State{Role: "agent", Instance: cfg.InstanceID, Generation: 2, Data: data}, snapshot.Resources)
		if err != nil {
			t.Fatal(err)
		}
		env := handoff.CleanEnvironment(os.Environ())
		for i, entry := range env {
			if strings.HasPrefix(entry, clientImageMode+"=") {
				env[i] = clientImageMode + "=after"
			}
		}
		if err := p.Exec(os.Args[0], []string{os.Args[0], "-test.run=^TestClientImagePreparePermitAndFDClosure$", "-test.count=1"}, env); err != nil {
			_ = p.Close()
			_ = frozen.Thaw()
			t.Fatal(err)
		}
		return
	}
	if mode != "after" {
		t.Fatal("unknown helper mode")
	}
	r, err := handoff.Restore()
	if err != nil || r == nil {
		t.Fatal("real handoff restore failed")
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(r.Close)
	var fixture struct {
		PID   int
		Image ClientImage
	}
	if err := json.Unmarshal(r.State.Data, &fixture); err != nil || fixture.PID != os.Getpid() {
		t.Fatal("handoff changed PID")
	}
	cfg.OwnerGen = 2
	c, salvage, err := RestoreClient(cfg, fixture.Image, r)
	if err != nil {
		closeClientImageResources(salvage)
		t.Fatal(err)
	}
	for _, e := range c.flowState.endpoints {
		if e.token != clientImageToken || e.pins.Current == "" || e.pins.Previous != clientImagePreviousPin || e.pins.ValidUntil == nil || !e.pins.ValidUntil.Equal(clientImagePinUntil) || c.brokerAddr != os.Getenv("TETHER_TEST_CLIENT_CONTROL") || e.brokerAddr != c.brokerAddr {
			t.Fatal("restore lost the sealed route or pin rotation deadline")
		}
	}
	if err := c.ActivateRaw(); err != nil {
		t.Fatal(err)
	}
	fmt.Println("CLIENT_IMPORTED")
	if line, err := input.ReadString('\n'); err != nil || line != "RESUME\n" {
		t.Fatal("missing lost-reply handshake")
	}
	if err := c.ResumeCarriers(time.Now().Add(ResumeAttempt)); err == nil {
		t.Fatal("dropped authorization reply unexpectedly succeeded")
	}
	fmt.Println("CLIENT_REPLY_LOST")
	if line, err := input.ReadString('\n'); err != nil || line != "RETRY\n" {
		t.Fatal("missing retry handshake")
	}
	if err := c.ResumeCarriers(time.Now().Add(ResumeAttempt)); err != nil {
		t.Fatal(err)
	}
	c.flowState.mu.Lock()
	attachments := make(map[int]*flowCarrier)
	for port, e := range c.flowState.endpoints {
		attachments[port] = e.attachment
	}
	c.flowState.mu.Unlock()
	if err := c.ResumeCarriers(time.Now().Add(ResumeAttempt)); err != nil {
		t.Fatal("cached RESUME failed")
	}
	c.flowState.mu.Lock()
	for port, e := range c.flowState.endpoints {
		if e.attachment != attachments[port] {
			c.flowState.mu.Unlock()
			t.Fatal("completed recovery replaced a live attachment")
		}
	}
	c.flowState.mu.Unlock()
	if again, _, err := RestoreClient(cfg, fixture.Image, r); err == nil || again != nil {
		t.Fatal("restore consumed resource twice")
	}
	if again, err := handoff.Restore(); err != nil || again != nil {
		t.Fatal("dispatch was not consumed exactly once")
	}
	fmt.Println("CLIENT_RESUMED")
	if line, err := input.ReadString('\n'); err != nil || line != "DONE\n" {
		t.Fatal("missing completion handshake")
	}
	frozen, err := c.FreezeFlows(time.Now().Add(ResumeAttempt))
	if err != nil {
		t.Fatal(err)
	}
	// A current cut must encode the consumed permit decision, and replacing
	// the final export must close its predecessor while originals remain held.
	// origin: hot-update S5 plan §5.3 / T32
	_, oldSnapshot, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	oldState := oldSnapshot.Owned[0]
	_, snapshot, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oldState.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("client re-export retained a sealed descriptor")
	}
	stateFile := snapshot.Owned[0]
	cut, err := frozen.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range cut.Records {
		if rec.Kind == sealedstate.KindEndpoint && clientFields(rec)["owner_gen"].U64 != 2 {
			t.Fatal("repeated RESUME consumed permit twice")
		}
		if rec.Kind == sealedstate.KindPermit && clientFields(rec)["consumed"].U64 != 1 {
			t.Fatal("current cut lost the consumed permit decision")
		}
	}
	if err := frozen.Discard(); err != nil {
		t.Fatal(err)
	}
	if _, err := stateFile.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("client discard retained a sealed descriptor")
	}
}
