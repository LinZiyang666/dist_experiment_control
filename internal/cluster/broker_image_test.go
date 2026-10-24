package cluster

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/clusterio"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	"golang.org/x/sys/unix"
)

func imageDeadline(t *testing.T, d time.Duration) controlbudget.Deadline {
	t.Helper()
	now, err := procrequest.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	return controlbudget.Deadline(now + int64(d))
}

// origin: hot-update S6 plan §3.2 / T16
func TestImageTransportRejectsCorruptRegistry(t *testing.T) {
	// No listener or worker is needed: corruption must be caught before the
	// wrapper can use a registry entry to bypass transport cancellation.
	for _, entry := range []any{struct{}{}, (*tlsStreamLayer)(nil)} {
		func() {
			network := &raft.NetworkTransport{}
			transportStreams.Store(network, entry)
			defer transportStreams.Delete(network)
			defer func() {
				if got := recover(); got != "cluster: invalid registered transport stream" {
					t.Errorf("corrupt registry did not fail at its ownership boundary: %v", got)
				}
			}()
			wrapNodeImageTransport(network, nil)
		}()
	}
}

// origin: hot-update S6 plan §3.2 / T10
func TestBrokerPartialCloseBlocksReopen(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("broker image requires Linux")
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseNodeCloseUncertainty(t)
	}
	testharness.AssertNoGoroutineLeak(t, "partial close receipts", beforeG)
	testharness.AssertNoFDLeak(t, "partial close receipts", beforeFD, 0)
}

func exerciseNodeCloseUncertainty(t *testing.T) {
	t.Helper()
	for _, object := range []string{"raft", "transport", "bolt", "read_only", "writer"} {
		n, err := openNodeRaw(t.TempDir(), raft.ServerID("close-"+object))
		if err != nil {
			t.Fatal(err)
		}
		failure := errors.New("injected close receipt uncertainty")
		setImageCloseHook(n, func(name string) error {
			if name == object {
				return failure
			}
			return nil
		})
		if err := n.Shutdown(); !errors.Is(err, failure) {
			t.Fatalf("%s close error discarded: %v", object, err)
		}
		phases := map[string]storage.ClosePhase{"raft": n.image.seal.Raft, "transport": n.image.seal.Transport, "bolt": n.image.seal.Bolt, "read_only": n.image.seal.ReadOnly, "writer": n.image.seal.Writer}
		if phases[object] != storage.CloseUnknown {
			t.Fatalf("%s error promoted to closed: %v", object, phases[object])
		}
		for name, phase := range phases {
			if name != object && phase != storage.CloseClosed {
				t.Fatalf("%s prevented cleanup of %s: %v", object, name, phase)
			}
		}
		// Receipt collection is the prerequisite for ReopenCurrent. Isolate each
		// unknown-phase guard as well as the sticky error guard, so an error cannot
		// hide the mutation which converts an unknown writer to a second opener.
		if _, err := ClosedImageStore(n); err == nil {
			t.Fatal("partial close permits reopen receipt")
		}
		if err := n.Shutdown(); !errors.Is(err, failure) {
			t.Fatal("second shutdown forgot the unknown phase")
		}
		n.image.prepared = true
		n.image.shutdownErr = nil
		if _, err := ClosedImageStore(n); err == nil {
			t.Fatalf("unknown %s phase permits reopen receipt", object)
		}
	}
	// File+error and owner+error are different from a nil failed constructor.
	// A real injected transport is closed synchronously, yet an uncertain close
	// returns its partial owner so the caller can retain it without redispatch.
	_, raw := raft.NewInmemTransport("partial")
	transport := &uncertainTransport{InmemTransport: raw}
	n, err := New(Config{Transport: transport})
	if n == nil || err == nil || !transport.closed {
		t.Fatalf("partial constructor owner lost: node=%v error=%v closed=%v", n, err, transport.closed)
	}
	if n.image.seal.Transport != storage.CloseUnknown {
		t.Fatal("partial constructor transport uncertainty erased")
	}
	if _, err := ClosedImageStore(n); err == nil {
		t.Fatal("partial constructor permits current-store reopen")
	}
}

type uncertainTransport struct {
	*raft.InmemTransport
	closed bool
}

func (t *uncertainTransport) Close() error {
	t.closed = true
	return errors.Join(t.InmemTransport.Close(), errors.New("injected transport close error"))
}

// origin: hot-update S6 plan §5 / T38
func TestBrokerRoleObserverDetectsABA(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("broker image requires Linux")
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRoleObserverABA(t)
	}
	testharness.AssertNoGoroutineLeak(t, "role edge observer", beforeG)
	testharness.AssertNoFDLeak(t, "role edge observer", beforeFD, 0)
}

func exerciseRoleObserverABA(t *testing.T) {
	t.Helper()
	n, err := openNodeRaw(t.TempDir(), "role-aba")
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "n.Shutdown", n.Shutdown)
	if err := n.WaitForLeader(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := n.Barrier(time.Second); err != nil {
		t.Fatal(err)
	}
	before, err := ReadImageRole(n)
	if err != nil {
		t.Fatal(err)
	}
	if before.EdgeSeq == 0 || !before.LastRunningIsLeader || before.Term == 0 || before.ConfigIndex == 0 || before.VoterDigest == "" || before.ObserverClosed {
		t.Fatalf("real election has no fresh observation: %+v", before)
	}
	if err := ValidateImageRole(n, before); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*storage.RoleReceipt){
		func(r *storage.RoleReceipt) { r.Term++ },
		func(r *storage.RoleReceipt) { r.ConfigIndex++ },
		func(r *storage.RoleReceipt) { r.VoterDigest = "changed" },
		func(r *storage.RoleReceipt) { r.LastRunningIsLeader = !r.LastRunningIsLeader },
		func(r *storage.RoleReceipt) { r.ObserverClosed = !r.ObserverClosed },
	} {
		stale := before
		change(&stale)
		if err := ValidateImageRole(n, stale); err == nil {
			t.Fatalf("stale role/configuration accepted: %+v", stale)
		}
	}
	// Drive the constructor's actual NotifyCh input across ABA. Current role and
	// term stay identical: polling IsLeader twice cannot detect this interval.
	n.image.notify <- false
	n.image.notify <- true
	after, err := ReadImageRole(n)
	if err != nil {
		t.Fatal(err)
	}
	if after.EdgeSeq != before.EdgeSeq+2 {
		t.Fatalf("ABA not observed: before=%d after=%d", before.EdgeSeq, after.EdgeSeq)
	}
	if err := ValidateImageRole(n, before); err == nil {
		t.Fatal("stale role observation accepted after ABA")
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	receipt, err := ReadImageRole(n)
	if err != nil || !receipt.ObserverClosed {
		t.Fatalf("observer not joined: %+v %v", receipt, err)
	}
	if err := validateStoppedImageRole(after, receipt); err != nil {
		t.Fatalf("ordinary terminal leader edge rejected: %v", err)
	}
	if err := validateStoppedImageRole(before, receipt); err == nil {
		t.Fatal("stopped observer accepted an ABA during shutdown")
	}
	for _, change := range []func(*storage.RoleReceipt){
		func(r *storage.RoleReceipt) { r.Term++ },
		func(r *storage.RoleReceipt) { r.ConfigIndex++ },
		func(r *storage.RoleReceipt) { r.VoterDigest = "changed" },
	} {
		stale := after
		change(&stale)
		if err := validateStoppedImageRole(stale, receipt); err == nil {
			t.Fatalf("stopped role metadata accepted stale observation: %+v", stale)
		}
	}
}

// origin: hot-update S6 plan §3.2 / T55
func TestBrokerNodeLeakCycles(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("broker image requires Linux")
	}
	exerciseNodePartialClose(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseNodePartialClose(t)
	}
	testharness.AssertNoGoroutineLeak(t, "node constructor and exec-return cleanup", beforeG)
	testharness.AssertNoFDLeak(t, "node constructor and exec-return cleanup", beforeFD, 0)
}

func exerciseNodePartialClose(t *testing.T) {
	t.Helper()
	exerciseImagePartialDatabaseOpen(t)
	ca := newTestCA(t)
	native, err := NewMTLSTransport(MTLSTransportConfig{BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, "node")})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", string(native.LocalAddr()))
	if err != nil {
		_ = native.Close()
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "conn.Close", conn.Close)
	v, _ := transportStreams.Load(native)
	stream := v.(*tlsStreamLayer)
	// Accepted TCP without any TLS bytes is a real stalled handshake. Wait for
	// its admission receipt before exercising the constructor failure path.
	waitImageCondition(t, func() bool { c, _ := stream.tracker.Counts(); return c == 1 })
	n, err := New(Config{Transport: native})
	if n != nil || err == nil {
		t.Fatalf("bad config constructor: %v %v", n, err)
	}
	if c, r := stream.tracker.Counts(); c != 0 || r != 0 {
		t.Fatalf("constructor returned before accepted handler joined: %d/%d", c, r)
	}
	native, err = NewMTLSTransport(MTLSTransportConfig{BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, "live")})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	lock, err := AcquireImageDataDirLock(dir)
	if err != nil {
		_ = native.Close()
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "lock.Release", lock.Release)
	rebuild := MTLSTransportConfig{BindAddr: string(native.LocalAddr()), CACert: ca.pool, Leaf: ca.leaf(t, "rebuilt")}
	n, err = New(Config{LocalID: "live", DataDir: dir, DBPath: filepath.Join(dir, "state.db"), Transport: native, Logger: quietLogger(), TransportFactory: func() (raft.Transport, error) { return NewMTLSTransport(rebuild) }})
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "n.Shutdown", n.Shutdown)
	if err := n.WaitForLeader(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := n.RODB().Ping(); err != nil {
		t.Fatal(err)
	}
	if err := n.ApplyMetaSet("t:ack-prefix", "survives-close"); err != nil {
		t.Fatal(err)
	}
	if err := QuiesceImageStore(n, imageDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := n.ApplyMetaSet("t:fenced", "must-not-write"); err == nil {
		t.Fatal("quiesced node admitted a new proposal")
	}
	if err := n.RecoverToSelfOnline(string(native.LocalAddr())); err == nil {
		t.Fatal("quiesced node admitted a transport rebuild")
	}
	if err := ResumeImageStore(n); err != nil {
		t.Fatal(err)
	}
	if err := n.ApplyMetaSet("t:resumed", "after-cut-failure"); err != nil {
		t.Fatal(err)
	}
	resource, err := lock.Export()
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := handoff.Prepare(handoff.State{Role: "broker", Instance: "returned-exec", Generation: 1}, []handoff.Resource{resource})
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Exec(filepath.Join(dir, "missing-candidate"), []string{"missing-candidate"}, handoff.CleanEnvironment(os.Environ())); err == nil {
		t.Fatal("missing candidate exec returned success")
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if err := requireImageDataDirLock(dir); err != nil {
		t.Fatal("exec return lost original lock owner", err)
	}
	if err := n.ApplyMetaSet("t:exec-returned", "still-live"); err != nil {
		t.Fatal("exec failure abandoned current Node", err)
	}
	beforeRecover, err := ReadImageRole(n)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.RecoverToSelfOnline(string(native.LocalAddr())); err != nil {
		t.Fatal(err)
	}
	if _, ok := n.transport.(*imageNetworkTransport); !ok {
		t.Fatal("online recovery installed an untracked transport")
	}
	if err := n.WaitForLeader(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := n.Barrier(time.Second); err != nil {
		t.Fatal(err)
	}
	afterRecover, err := ReadImageRole(n)
	if err != nil {
		t.Fatal(err)
	}
	if afterRecover.EdgeSeq < beforeRecover.EdgeSeq+2 {
		t.Fatal("online recovery lost constructor role notifications")
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal("terminal stop is not idempotent", err)
	}
	if _, err := ClosedImageStore(n); err == nil {
		t.Fatal("cold embedding without registry manufactured image seal")
	}
	// Real current store remains intact, with no rollback to an old snapshot.
	ro, err := storage.OpenReadOnly("file:" + filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "ro.Close", ro.Close)
	var value string
	if err := ro.QueryRow("SELECT value FROM cluster_meta WHERE key='t:ack-prefix'").Scan(&value); err != nil || value != "survives-close" {
		t.Fatalf("acknowledged prefix changed: %q %v", value, err)
	}
}

// Partial database callbacks return real current-store pools with an injected
// error. They must be owned and closed before construction reports failure.
// origin: hot-update S6 plan §3.3 / T10 / T55
func exerciseImagePartialDatabaseOpen(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		return
	}
	for _, failingPool := range []string{"writer", "read-only"} {
		func() {
			dir := t.TempDir()
			path := filepath.Join(dir, "state.db")
			lock, err := AcquireImageDataDirLock(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer checkImageCleanup(t, "partial open lock", lock.Release)
			seed, err := storage.OpenWAL("file:" + path)
			if err != nil {
				t.Fatal(err)
			}
			defer checkImageCleanup(t, "partial open seed", seed.Close)
			identity, err := storage.CanonicalStoreIdentity(path, dir)
			if err != nil {
				t.Fatal(err)
			}
			stamp, err := storage.ReadStoreStamp(seed, identity)
			if err != nil {
				t.Fatal(err)
			}
			if err := seed.Close(); err != nil {
				t.Fatal(err)
			}
			registry := storage.DefaultDriverRegistry()
			failure := errors.New("injected partial database open failure")
			var taken *sql.DB
			defer func() {
				// Keep the adversarial fixture hermetic even when ownership is
				// deliberately removed by a constructor mutation.
				if taken != nil {
					checkImageCleanup(t, "partial returned pool", taken.Close)
				}
			}()
			_, transport := raft.NewInmemTransport("partial-open")
			cfg := Config{LocalID: "partial-open", DataDir: dir, DBPath: path, Transport: transport, ImageRegistry: registry,
				ImageOpenCurrent: storage.OpenCurrent, ImageReadOnly: storage.OpenReadOnly}
			if failingPool == "writer" {
				cfg.ImageOpenCurrent = func(c storage.OpenImageConfig) (*sql.DB, error) {
					db, err := storage.OpenCurrent(c)
					taken = db
					return db, errors.Join(err, failure)
				}
			} else {
				cfg.ImageReadOnly = func(dsn string) (*sql.DB, error) {
					db, err := storage.OpenReadOnly(dsn)
					taken = db
					return db, errors.Join(err, failure)
				}
			}
			n, err := NewForImage(cfg, stamp)
			if !errors.Is(err, failure) || n == nil || taken == nil {
				t.Fatalf("%s partial constructor receipt: owner=%v pool=%v err=%v", failingPool, n != nil, taken != nil, err)
			}
			defer checkImageCleanup(t, "partial node", n.Shutdown)
			if writers, readers := registry.Connections(identity); writers != 0 || readers != 0 {
				t.Fatalf("%s partial constructor abandoned actual pools: %d/%d", failingPool, writers, readers)
			}
			if n.image.seal.Writer != storage.CloseClosed || failingPool == "read-only" && n.image.seal.ReadOnly != storage.CloseClosed {
				t.Fatalf("%s partial constructor lost close phases: writer=%v reader=%v", failingPool, n.image.seal.Writer, n.image.seal.ReadOnly)
			}
		}()
	}
}

func waitImageCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("missing transport admission receipt")
		}
		runtime.Gosched()
	}
}

// origin: hot-update S6 plan §3.4 / T16
func TestRaftSnapshotRPCJoinWithinCut(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("broker image requires Linux")
	}
	exerciseSnapshotRPCJoin(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSnapshotRPCJoin(t)
	}
	testharness.AssertNoGoroutineLeak(t, "mTLS snapshot payload join", beforeG)
	testharness.AssertNoFDLeak(t, "mTLS snapshot payload join", beforeFD, 0)
}

func exerciseSnapshotRPCJoin(t *testing.T) {
	t.Helper()
	ca := newTestCA(t)
	leaf := ca.leaf(t, "rpc")
	cfg := MTLSTransportConfig{BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: leaf}
	native, err := NewMTLSTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := wrapImageTransport(native).(*imageNetworkTransport)
	defer checkImageCleanup(t, "transport.Close", transport.Close)
	_, clientTLS, err := clusterTLSConfigs(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c, err := tls.Dial("tcp", string(native.LocalAddr()), clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "c.Close", c.Close)
	configuration := raft.EncodeConfiguration(raft.Configuration{Servers: []raft.Server{{Suffrage: raft.Voter, ID: "rpc", Address: native.LocalAddr()}}})
	req := raft.InstallSnapshotRequest{SnapshotVersion: 1, Term: 5, LastLogIndex: 200, LastLogTerm: 4, Configuration: configuration, ConfigurationIndex: 100, Size: 1 << 20}
	// Real raft wire tag and msgpack header followed by only a prefix of the
	// declared payload. The TLS stream remains open; no fixture deadline frees it.
	if _, err := c.Write([]byte{2}); err != nil {
		t.Fatal(err)
	}
	if err := codec.NewEncoder(c, &codec.MsgpackHandle{}).Encode(&req); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("payload-prefix")); err != nil {
		t.Fatal(err)
	}
	var rpc raft.RPC
	select {
	case rpc = <-transport.Consumer():
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot never crossed real consumer boundary")
	}
	reader, ok := rpc.Reader.(interface {
		Ticket() clusterio.SnapshotTicket
	})
	if !ok {
		rpc.Respond(nil, errors.New("missing ticket"))
		t.Fatal("snapshot RPC reader has no bound metadata")
	}
	ticket := reader.Ticket()
	digest := sha256.Sum256(configuration)
	if ticket.RPCID == 0 || ticket.Index != req.LastLogIndex || ticket.Term != req.LastLogTerm || ticket.ConfigIndex != req.ConfigurationIndex || ticket.ConfigDigest != hex.EncodeToString(digest[:]) || ticket.PayloadBytes != req.Size {
		rpc.Respond(nil, errors.New("wrong ticket"))
		t.Fatalf("snapshot ticket not bound to RPC: %+v", ticket)
	}
	prefix := make([]byte, len("payload-prefix"))
	if _, err := io.ReadFull(rpc.Reader, prefix); err != nil || string(prefix) != "payload-prefix" {
		rpc.Respond(nil, errors.New("prefix missing"))
		t.Fatalf("snapshot body was not exercised: %q %v", prefix, err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, rpc.Reader)
		rpc.Respond(&raft.InstallSnapshotResponse{Term: 5}, err)
		done <- err
	}()
	if c, r := transport.tracker.Counts(); c != 1 || r != 1 {
		t.Fatalf("stalled RPC census: %d/%d", c, r)
	}
	if err := transport.fence(imageDeadline(t, 2*time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("truncated payload produced successful snapshot")
		}
	case <-time.After(time.Second):
		t.Fatal("cut returned without joining actual io.Copy")
	}
	if c, r := transport.tracker.Counts(); c != 0 || r != 0 {
		t.Fatalf("cut has unjoined accepted RPC owners: %d/%d", c, r)
	}
	exerciseOutboundSnapshotJoin(t)
	exerciseSnapshotStageRequired(t)
}

// origin: hot-update S6 plan §3.1 / T12
func TestBrokerLockHeldAcrossExec(t *testing.T) {
	if mode := os.Getenv("TETHER_P04_LOCK_MODE"); mode != "" {
		runImageLockHelper(t, mode)
		return
	}
	if runtime.GOOS != "linux" {
		t.Skip("broker image requires Linux")
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLockExec(t)
	}
	testharness.AssertNoGoroutineLeak(t, "same-PID lock ownership", beforeG)
	testharness.AssertNoFDLeak(t, "same-PID lock ownership", beforeFD, 0)
}

func imageLockEnvironment(mode, dir string) []string {
	env := handoff.CleanEnvironment(os.Environ())
	clean := env[:0]
	for _, entry := range env {
		if !strings.HasPrefix(entry, "TETHER_P04_LOCK_") {
			clean = append(clean, entry)
		}
	}
	return append(clean, "TETHER_P04_LOCK_MODE="+mode, "TETHER_P04_LOCK_DIR="+dir, "GORACE="+os.Getenv("GORACE")+" atexit_sleep_ms=0")
}

func runImageLockHelper(t *testing.T, mode string) {
	dir := os.Getenv("TETHER_P04_LOCK_DIR")
	if mode == "contender" {
		release, err := AcquireDataDirLock(dir)
		if errors.Is(err, ErrDataDirLockHeld) {
			fmt.Println("HELD")
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		release()
		fmt.Println("ACQUIRED")
		return
	}
	if mode == "watch" {
		stopped := make(chan struct{})
		go func() { _, _ = io.Copy(io.Discard, os.Stdin); close(stopped) }()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		count := 0
		for {
			release, err := AcquireDataDirLock(dir)
			if err == nil {
				release()
				t.Fatal("continuous contender acquired during cut")
			}
			if !errors.Is(err, ErrDataDirLockHeld) {
				t.Fatal(err)
			}
			count++
			if count == 1 {
				fmt.Println("WATCHING")
			}
			select {
			case <-stopped:
				fmt.Printf("WATCHED %d\n", count)
				return
			case <-ticker.C:
			}
		}
	}
	if mode == "before" {
		owner, err := AcquireImageDataDirLock(dir)
		if err != nil {
			t.Fatal(err)
		}
		resource, err := owner.Export()
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := handoff.Prepare(handoff.State{Role: "broker", Instance: "lock-exec", Generation: 1}, []handoff.Resource{resource})
		if err != nil {
			t.Fatal(err)
		}
		f, err := owner.Detach()
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if err := owner.Release(); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("LOCK_CLOSED %d\n", os.Getpid())
		if _, err := io.ReadFull(os.Stdin, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		args := []string{os.Args[0], "-test.run=^TestBrokerLockHeldAcrossExec$", "-test.count=1"}
		if err := prepared.Exec(os.Args[0], args, imageLockEnvironment("after", dir)); err != nil {
			_ = prepared.Close()
			t.Fatal(err)
		}
		t.Fatal("successful exec returned")
	}
	restored, err := handoff.Restore()
	if err != nil || restored == nil {
		t.Fatal("missing inherited lock", err)
	}
	defer checkImageCleanup(t, "restored.Close", restored.Close)
	f, err := restored.TakeFile("broker-store-lock", "lock")
	if err != nil {
		t.Fatal(err)
	}
	owner, err := AdoptDataDirLock(dir, f)
	if err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	fmt.Printf("LOCK_ADOPTED %d\n", os.Getpid())
	if _, err := io.ReadFull(os.Stdin, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
	fmt.Println("LOCK_RELEASED")
	if second, err := handoff.Restore(); second != nil || err != nil {
		t.Fatal("handoff restored twice", err)
	}
}

func exerciseLockExec(t *testing.T) {
	t.Helper()
	dir := testharness.GuardProcessDirectory(t, t.TempDir())
	contender := func(want string) {
		cmd := exec.Command(os.Args[0], "-test.run=^TestBrokerLockHeldAcrossExec$", "-test.count=1")
		cmd.Env = imageLockEnvironment("contender", dir)
		cmd.Dir = dir
		output, err := cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(output), want+"\n") {
			t.Fatalf("real contender wants %s: %s %v", want, output, err)
		}
	}
	contender("ACQUIRED")
	cmd := exec.Command(os.Args[0], "-test.run=^TestBrokerLockHeldAcrossExec$", "-test.count=1")
	cmd.Env = imageLockEnvironment("before", dir)
	cmd.Dir = dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		_ = input.Close()
		_ = output.Close()
		if !waited {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			_ = cmd.Wait()
		}
	}()
	lines := bufio.NewReader(output)
	readLine := func() string {
		result := make(chan string, 1)
		go func() {
			line, err := lines.ReadString('\n')
			if err != nil {
				line += " READ_ERROR"
			}
			result <- line
		}()
		select {
		case line := <-result:
			return line
		case <-time.After(5 * time.Second):
			t.Fatalf("helper barrier missing: %s", stderr.String())
			return ""
		}
	}
	line := readLine()
	if line != fmt.Sprintf("LOCK_CLOSED %d\n", cmd.Process.Pid) {
		t.Fatalf("closed barrier: %q", line)
	}
	contender("HELD") // deterministic gap after original FD closes, before exec
	watcher := exec.Command(os.Args[0], "-test.run=^TestBrokerLockHeldAcrossExec$", "-test.count=1")
	watcher.Env = imageLockEnvironment("watch", dir)
	watcher.Dir = dir
	watcherInput, err := watcher.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	watcherOutput, err := watcher.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var watcherError bytes.Buffer
	watcher.Stderr = &watcherError
	if err := watcher.Start(); err != nil {
		t.Fatal(err)
	}
	watcherWaited := false
	defer func() {
		_ = watcherInput.Close()
		_ = watcherOutput.Close()
		if !watcherWaited {
			_ = watcher.Process.Kill()
			_ = watcher.Wait()
		}
	}()
	watcherLines := bufio.NewReader(watcherOutput)
	if line, err := watcherLines.ReadString('\n'); err != nil || line != "WATCHING\n" {
		t.Fatalf("continuous contender failed to start: %q %v", line, err)
	}
	if _, err := input.Write([]byte("G")); err != nil {
		t.Fatal(err)
	}
	line = readLine()
	if line != fmt.Sprintf("LOCK_ADOPTED %d\n", cmd.Process.Pid) {
		t.Fatalf("same-PID adoption: %q", line)
	}
	contender("HELD")
	_ = watcherInput.Close()
	watched, err := watcherLines.ReadString('\n')
	if err != nil {
		t.Fatal("continuous contender lost receipt", err)
	}
	parts := strings.Fields(watched)
	if len(parts) != 2 || parts[0] != "WATCHED" {
		t.Fatalf("continuous contender receipt: %q", watched)
	}
	count, err := strconv.Atoi(parts[1])
	if err != nil || count < 2 {
		t.Fatalf("continuous contender never repeated: %q", watched)
	}
	_, _ = io.Copy(io.Discard, watcherLines)
	err = watcher.Wait()
	watcherWaited = true
	if err != nil {
		t.Fatalf("continuous contender found lock gap: %v %s", err, watcherError.String())
	}
	if _, err := input.Write([]byte("R")); err != nil {
		t.Fatal(err)
	}
	if line := readLine(); line != "LOCK_RELEASED\n" {
		t.Fatalf("normal release receipt: %q", line)
	}
	contender("ACQUIRED")
	_ = input.Close()
	_, _ = io.Copy(io.Discard, lines)
	err = cmd.Wait()
	waited = true
	if err != nil {
		t.Fatalf("helper exit: %v %s", err, stderr.String())
	}
	// Invalid inherited identities are refused without closing caller salvage.
	plain, err := os.OpenFile(DataDirLockPath(dir), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "plain.Close", plain.Close)
	if owner, err := AdoptDataDirLock(dir, plain); owner != nil || err == nil {
		t.Fatal("unlocked same-inode descriptor adopted")
	}
	if _, err := plain.Stat(); err != nil {
		t.Fatal("refusal consumed caller salvage", err)
	}
	owner, err := AcquireImageDataDirLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "owner.Release", owner.Release)
	if err := requireImageDataDirLock(dir); err != nil {
		t.Fatal(err)
	}
	otherDir := t.TempDir()
	if err := requireImageDataDirLock(otherDir); err == nil {
		t.Fatal("unlocked data directory accepted for hot open")
	}
	resource, err := owner.Export()
	if err != nil {
		t.Fatal(err)
	}
	fd, err := unix.FcntlInt(resource.Files["lock"].Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := os.NewFile(uintptr(fd), "duplicate-lock")
	defer checkImageCleanup(t, "duplicate.Close", duplicate.Close)
	if adopted, err := AdoptDataDirLock(dir, duplicate); adopted != nil || err == nil {
		t.Fatal("two wrappers adopted the same lock OFD")
	}
	detached, err := owner.Detach()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(detached.Fd(), unix.F_SETFD, 0); err != nil {
		t.Fatal(err)
	}
	adopted, adoptErr := AdoptDataDirLock(dir, detached)
	if _, err := unix.FcntlInt(detached.Fd(), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	if adopted != nil || adoptErr == nil {
		if adopted != nil {
			_ = adopted.Release()
		}
		t.Fatal("inheritable original lock adopted without CLOEXEC")
	}
	adopted, err = AdoptDataDirLock(dir, detached)
	if err != nil {
		_ = detached.Close()
		t.Fatal("valid original lock cannot be adopted after flag refusal", err)
	}
	defer checkImageCleanup(t, "adopted.Release", adopted.Release)
	// The pathname can be replaced while the original inode remains held. Neither
	// a hot opener nor adoption may move to the replacement's independent domain.
	oldName := DataDirLockPath(dir) + ".old"
	if err := os.Rename(DataDirLockPath(dir), oldName); err != nil {
		t.Fatal(err)
	}
	replacement, err := os.OpenFile(DataDirLockPath(dir), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_ = replacement.Close()
	if err := requireImageDataDirLock(dir); err == nil {
		t.Fatal("replaced lock pathname accepted for hot open")
	}
	original, err := adopted.Detach()
	if err != nil {
		t.Fatal(err)
	}
	refused, err := AdoptDataDirLock(dir, original)
	if refused != nil || err == nil {
		if refused != nil {
			_ = refused.Release()
		}
		t.Fatal("mismatched lock pathname adopted")
	}
	if _, err := original.Stat(); err != nil {
		t.Fatal("identity refusal destroyed inherited salvage", err)
	}
	_ = original.Close()

}

// origin: hot-update S6 plan §3.2 / T10 / T55
func TestBrokerNodeCurrentStoreCut(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("broker current-store image requires Linux")
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseNodeCurrentStoreCut(t)
	}
	testharness.AssertNoGoroutineLeak(t, "current store cut and partial opener", beforeG)
	testharness.AssertNoFDLeak(t, "current store cut and partial opener", beforeFD, 0)
}

func exerciseNodeCurrentStoreCut(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	lock, err := AcquireImageDataDirLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "lock.Release", lock.Release)
	seed, err := openNodeRaw(dir, "current-cut")
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.WaitForLeader(3 * time.Second); err != nil {
		_ = seed.Shutdown()
		t.Fatal(err)
	}
	if err := seed.ApplyMetaSet("t:before-cut", "acknowledged"); err != nil {
		_ = seed.Shutdown()
		t.Fatal(err)
	}
	cfg, stamp := closeCurrentStoreSeed(t, seed)
	_, cfg.Transport = raft.NewInmemTransport("current-cut")
	n, err := NewForImage(cfg, stamp)
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "n.Shutdown", n.Shutdown)
	if err := n.WaitForLeader(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := n.Barrier(time.Second); err != nil {
		t.Fatal(err)
	}
	if err := n.ApplyMetaSet("t:after-import", "acknowledged-current"); err != nil {
		t.Fatal(err)
	}
	deadline := imageDeadline(t, 5*time.Second)
	if err := PrepareImageShutdown(n, deadline); err != nil {
		t.Fatal(err)
	}
	if _, err := ClosedImageStore(n); err == nil {
		t.Fatal("provisional close receipt allowed reopen")
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	seal, err := ClosedImageStore(n)
	if err != nil {
		t.Fatal(err)
	}
	if seal.Stamp.AppliedIndex < stamp.AppliedIndex || seal.DriverWriters != 0 || seal.DriverReaders != 0 || !seal.Role.ObserverClosed {
		t.Fatalf("invalid final close receipt: %+v", seal)
	}
	if w, r := cfg.ImageRegistry.Connections(seal.Stamp.Identity); w != 0 || r != 0 {
		t.Fatalf("actual driver connections survive: %d/%d", w, r)
	}
	_, cfg.Transport = raft.NewInmemTransport("current-cut")
	reopened, err := NewForImage(cfg, seal.Stamp)
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "reopened.Shutdown", reopened.Shutdown)
	var value string
	if err := reopened.db.QueryRow("SELECT value FROM cluster_meta WHERE key='t:after-import'").Scan(&value); err != nil || value != "acknowledged-current" {
		t.Fatalf("current reopen lost ACK: %q %v", value, err)
	}
	if err := reopened.Shutdown(); err != nil {
		t.Fatal(err)
	}
	// The real opener can return an owned pool alongside an error. Constructor
	// cleanup must close that pool before returning nil; no second open can race.
	failure := errors.New("injected error after actual current pool open")
	openCurrent := cfg.ImageOpenCurrent
	cfg.ImageOpenCurrent = func(config storage.OpenImageConfig) (*sql.DB, error) {
		db, err := openCurrent(config)
		if err != nil {
			return db, err
		}
		return db, failure
	}
	_, cfg.Transport = raft.NewInmemTransport("current-cut")
	partial, err := NewForImage(cfg, seal.Stamp)
	if !errors.Is(err, failure) {
		t.Fatalf("partial opener error lost: %v", err)
	}
	if partial != nil {
		if err := partial.Shutdown(); err != nil {
			t.Fatalf("partial pool not reaped: %v", err)
		}
	}
	if w, r := cfg.ImageRegistry.Connections(seal.Stamp.Identity); w != 0 || r != 0 {
		t.Fatalf("file plus error abandoned driver handles: %d/%d", w, r)
	}
}

func exerciseOutboundSnapshotJoin(t *testing.T) {
	t.Helper()
	ca := newTestCA(t)
	config := MTLSTransportConfig{BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, "outbound")}
	serverNative, err := NewMTLSTransport(config)
	if err != nil {
		t.Fatal(err)
	}
	server := wrapImageTransport(serverNative).(*imageNetworkTransport)
	defer checkImageCleanup(t, "server.Close", server.Close)
	clientNative, err := NewMTLSTransport(config)
	if err != nil {
		t.Fatal(err)
	}
	client := wrapImageTransport(clientNative).(*imageNetworkTransport)
	defer checkImageCleanup(t, "client.Close", client.Close)
	source, writer := io.Pipe()
	defer checkImageCleanup(t, "writer.Close", writer.Close)
	req := &raft.InstallSnapshotRequest{SnapshotVersion: 1, Term: 5, LastLogIndex: 200, LastLogTerm: 4, Configuration: raft.EncodeConfiguration(raft.Configuration{Servers: []raft.Server{{Suffrage: raft.Voter, ID: "server", Address: server.LocalAddr()}}}), ConfigurationIndex: 100, Size: 1 << 20}
	sent := make(chan error, 1)
	go func() {
		var response raft.InstallSnapshotResponse
		sent <- client.InstallSnapshot("server", server.LocalAddr(), req, &response, source)
	}()
	wrote := make(chan error, 1)
	go func() { _, err := writer.Write(bytes.Repeat([]byte("x"), 300000)); wrote <- err }()
	var rpc raft.RPC
	select {
	case rpc = <-server.Consumer():
	case <-time.After(2 * time.Second):
		_ = writer.Close()
		t.Fatal("outbound snapshot never crossed mTLS consumer")
	}
	copied := make(chan error, 1)
	go func() {
		size, err := io.Copy(io.Discard, rpc.Reader)
		if size != req.Size {
			err = errors.Join(err, io.ErrUnexpectedEOF)
		}
		rpc.Respond(&raft.InstallSnapshotResponse{Term: 5}, err)
		copied <- err
	}()
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("outbound snapshot has no actual payload prefix")
	}
	if c, r := client.tracker.Counts(); c != 1 || r != 1 {
		t.Fatalf("outgoing snapshot source not owned: %d/%d", c, r)
	}
	if err := client.fence(imageDeadline(t, 2*time.Second)); err != nil {
		_ = writer.Close()
		t.Fatal("outgoing snapshot source was not joined", err)
	}
	select {
	case err := <-sent:
		if err == nil {
			t.Fatal("truncated outgoing snapshot succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("outgoing io.Copy survived cut")
	}
	select {
	case err := <-copied:
		if err == nil {
			t.Fatal("truncated incoming snapshot succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("incoming snapshot peer survived cut")
	}
	if err := server.fence(imageDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
}

// A decoded mTLS snapshot must be refused before Consumer when its real P05
// stage is unavailable. This tests absence, never a substitute successful codec.
func exerciseSnapshotStageRequired(t *testing.T) {
	t.Helper()
	owner, err := openNodeRaw(t.TempDir(), "unbound-stage")
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "owner.Shutdown", owner.Shutdown)
	ca := newTestCA(t)
	config := MTLSTransportConfig{BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, "required-stage")}
	native, err := NewMTLSTransport(config)
	if err != nil {
		t.Fatal(err)
	}
	transport := wrapNodeImageTransport(native, owner).(*imageNetworkTransport)
	transport.stage = nil // this private fixture's adapter is absent before activation
	transport.start()
	defer checkImageCleanup(t, "transport.Close", transport.Close)
	_, clientTLS, err := clusterTLSConfigs(config)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := tls.Dial("tcp", string(native.LocalAddr()), clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "conn.Close", conn.Close)
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	req := &raft.InstallSnapshotRequest{SnapshotVersion: 1, Term: 999, LastLogIndex: 20, LastLogTerm: 10, ConfigurationIndex: 1, Configuration: raft.EncodeConfiguration(raft.Configuration{}), Size: 64}
	if _, err := conn.Write([]byte{2}); err != nil {
		t.Fatal(err)
	}
	if err := codec.NewEncoder(conn, &codec.MsgpackHandle{}).Encode(req); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(make([]byte, 64)); err != nil {
		t.Fatal(err)
	}
	forwarded := make(chan bool, 1)
	go func() {
		select {
		case rpc := <-transport.Consumer():
			rpc.Respond(nil, errors.New("unvalidated snapshot forwarded"))
			forwarded <- true
		case <-transport.stop:
			forwarded <- false
		}
	}()
	var responseError string
	if err := codec.NewDecoder(conn, &codec.MsgpackHandle{}).Decode(&responseError); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(responseError, "snapshot validation unavailable") {
		t.Fatalf("unvalidated snapshot reached Raft Consumer: %q", responseError)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if <-forwarded {
		t.Fatal("unvalidated snapshot reached Raft Consumer")
	}
}

// Admission is only a prior observation. A sole voter can gain another voter
// before preparation, so the real shutdown boundary must sample membership.
// origin: hot-update S6 internal review rv-cluster-7
func TestImageShutdownResamplesMultiVoterLeadership(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseChangedShutdownMembership(t)
	}
	testharness.AssertNoGoroutineLeak(t, "changed shutdown membership x5", beforeG)
	testharness.AssertNoFDLeak(t, "changed shutdown membership x5", beforeFD, 0)
}
func exerciseChangedShutdownMembership(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	lock, err := AcquireImageDataDirLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "lock.Release", lock.Release)
	n, err := openNodeRaw(dir, "shutdown-role")
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "n.Shutdown", n.Shutdown)
	if err := n.WaitForLeader(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	// Bind actual registered handles and the real schema/identity reader. This
	// is an ordinary cold node, with no simulated close or image factory.
	n.cfg.ImageRegistry = storage.DefaultDriverRegistry()
	n.cfg.ImageStamp = func(db *sql.DB) (storage.StoreStamp, error) {
		id, err := storage.CanonicalStoreIdentity(n.cfg.DBPath, n.cfg.DataDir)
		if err != nil {
			return storage.StoreStamp{}, err
		}
		return storage.ReadStoreStamp(db, id)
	}
	if err := n.ApplyMetaSet("t:admitted", "sole-voter"); err != nil {
		t.Fatal(err)
	}
	if err := n.AddVoter("joined-voter", "joined-voter"); err != nil {
		t.Fatal(err)
	}
	if err := PrepareImageShutdown(n, imageDeadline(t, 5*time.Second)); err == nil || !strings.Contains(err.Error(), "multi-voter leader") {
		t.Fatalf("changed membership reached shutdown preparation: %v", err)
	}
	if n.image.prepared {
		t.Fatal("refused role produced a shutdown receipt")
	}
	if err := ResumeImageStore(n); err != nil {
		t.Fatal(err)
	}
	if err := n.db.Ping(); err != nil {
		t.Fatal("role refusal closed the current writer", err)
	}
}

// A hot singleton resumes its normal Raft election without spending a random
// heartbeat interval; constructor speed cannot be bought with faster timers.
// origin: hot-update S6 plan §5 / T37 / T48 / EXITS-2-3
func TestImageSoleVoterCampaignKeepsProductionTimers(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("broker image requires Linux")
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseImageSoleVoterCampaign(t)
	}
	testharness.AssertNoGoroutineLeak(t, "image singleton campaign x5", beforeG)
	testharness.AssertNoFDLeak(t, "image singleton campaign x5", beforeFD, 0)
}
func exerciseImageSoleVoterCampaign(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	lock, err := AcquireImageDataDirLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "lock.Release", lock.Release)
	ca := newTestCA(t)
	trans, err := NewMTLSTransport(MTLSTransportConfig{BindAddr: "127.0.0.1:0", CACert: ca.pool, Leaf: ca.leaf(t, "singleton")})
	if err != nil {
		t.Fatal(err)
	}
	addr := string(trans.LocalAddr())
	cfg := fastMultinodeCfg("singleton", dir, trans, nil)
	cfg.ImageOpenCurrent, cfg.ImageReadOnly, cfg.ImageRegistry = storage.OpenCurrent, storage.OpenReadOnly, storage.DefaultDriverRegistry()
	cfg.ImageStamp = func(db *sql.DB) (storage.StoreStamp, error) {
		id, err := storage.CanonicalStoreIdentity(cfg.DBPath, dir)
		if err != nil {
			return storage.StoreStamp{}, err
		}
		return storage.ReadStoreStamp(db, id)
	}
	cold, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "cold.Shutdown", cold.Shutdown)
	if err = cold.WaitForLeader(MultinodeHeartbeatTimeout / 2); err == nil {
		t.Fatal("cold constructor bypassed its production election timer")
	}
	if err = cold.WaitForLeader(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err = cold.ApplyMetaSet("t:campaign-ack", "original"); err != nil {
		t.Fatal(err)
	}
	if err = PrepareImageShutdown(cold, imageDeadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = cold.Shutdown(); err != nil {
		t.Fatal(err)
	}
	seal, err := ClosedImageStore(cold)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Transport, err = NewMTLSTransport(MTLSTransportConfig{BindAddr: addr, CACert: ca.pool, Leaf: ca.leaf(t, "singleton")})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	current, err := NewForImage(cfg, seal.Stamp)
	if current != nil {
		defer checkImageCleanup(t, "current.Shutdown", current.Shutdown)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = current.WaitForLeader(MultinodeHeartbeatTimeout / 2); err != nil {
		t.Fatalf("hot singleton waited for the production heartbeat: %v", err)
	}
	if time.Since(started) >= MultinodeHeartbeatTimeout/2 {
		t.Fatal("hot singleton campaign consumed a heartbeat interval")
	}
	rc := raftConfig(current.cfg)
	if rc.HeartbeatTimeout != MultinodeHeartbeatTimeout || rc.ElectionTimeout != MultinodeElectionTimeout || rc.LeaderLeaseTimeout != MultinodeLeaderLeaseTimeout {
		t.Fatal("hot campaign shortened production timers")
	}
	if err = current.VerifyLeaderRead(func(db *sql.DB) error {
		var value string
		if err := db.QueryRow("SELECT value FROM cluster_meta WHERE key='t:campaign-ack'").Scan(&value); err != nil {
			return err
		}
		if value != "original" {
			return errors.New("hot campaign lost current ACK")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Cleanup still runs after a failed assertion. Report unexpected close errors
// without aborting the remaining defers; fenced sockets may already be closed.
// origin: hot-update S6 plan §3.2 / T55
func checkImageCleanup(t *testing.T, name string, closeOwner func() error) {
	t.Helper()
	if err := closeOwner(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Errorf("%s: %v", name, err)
	}
}
