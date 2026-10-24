package adminsock

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// origin: hot-update S6 plan §2.1 / §12.1 P10
var (
	_ StartInheritedFunc = (*Server).StartInherited
	_ FenceImageFunc     = (*Server).FenceImage
	_ JoinImageFunc      = (*Server).JoinImage
	_ DetachImageFunc    = (*Server).DetachImage
)

// The original pathname and socket identity survive cold capture, view pause,
// pristine adoption and inherited start. Every generation answers real requests;
// a completed Unix connect alone would miss a discarded Accept backlog.
// origin: hot-update S6 plan §6.1 / T19
func TestUnixListenerHotNoUnlink(t *testing.T) {
	requireAdminImage(t)
	exerciseAdminInheritedClose(t)
	t.Run("replacement_path", exerciseAdminReplacementPath)
	t.Run("refusal_preserves_owner", exerciseAdminImageRefusals)
}

// Accepted-but-idle connections cannot cross admission. Reads cancel and join;
// admitted writes finish their real SQL effect and reply before the cut receipt.
// origin: hot-update S6 plan §6.1 / T19
func TestAdminImageJoinsAcceptedHandlers(t *testing.T) {
	requireAdminImage(t)
	t.Run("idle_and_sql_read", exerciseAdminCanceledRead)
	t.Run("admitted_write", exerciseAdminCommittedWrite)
	t.Run("armed_reload_reply", exerciseAdminArmedReply)
	t.Run("failed_reload_reply", exerciseAdminFailedReply)
	for _, op := range []string{OpAudit, OpEvents} {
		t.Run(op, func(t *testing.T) { exerciseAdminCanceledCallback(t, op) })
	}
	t.Run("control_root_cancel", exerciseAdminRootCancel)
}

// These cycles cover cold capture, partial start refusal, handler replies,
// pristine transfer and terminal cleanup. The FD identity multiset rules out
// closing an unrelated FD to conceal a leaked keeper alias with equal counts.
// origin: hot-update S6 plan §9.3 / §12.1 P10
func TestBrokerAdminLeakCycles(t *testing.T) {
	requireAdminImage(t)
	// Warm the runtime's permanent netpoll descriptors before the baseline.
	ln, err := net.Listen("unix", filepath.Join(t.TempDir(), "warm.sock"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	beforeIDs := adminFDIdentities(t)
	for round := 0; round < 5; round++ {
		exerciseAdminInheritedClose(t)
	}
	testharness.AssertNoGoroutineLeak(t, "admin inherited cycles", beforeG)
	testharness.AssertNoFDLeak(t, "admin inherited cycles", beforeFD, 0)
	if got := adminFDIdentities(t); !reflect.DeepEqual(beforeIDs, got) {
		t.Errorf("FD identity multiset changed: before=%v after=%v", beforeIDs, got)
	}
}

func requireAdminImage(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("listener image requires Linux")
	}
}

func adminDeadline(t *testing.T, duration time.Duration) controlbudget.Deadline {
	t.Helper()
	now, err := rawsocket.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	return controlbudget.Deadline(now + int64(duration))
}

func adminWait(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(upgradebudget.RequestLatency)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatal("admin barrier did not arrive")
		}
		time.Sleep(time.Millisecond)
	}
}

func adminReadBackend() Backend {
	return Backend{RuntimeSnapshot: func() *RuntimeReport { return &RuntimeReport{Schema: "admin_runtime", SchemaVersion: 2, OpenFDs: 17} }}
}

func adminStart(t *testing.T, backend Backend) *Server {
	t.Helper()
	s := New(filepath.Join(t.TempDir(), "admin.sock"), backend)
	// ctx-root: hermetic admin server lifetime, joined before this exercise ends.
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func adminCall(t *testing.T, path string) {
	t.Helper()
	resp, err := (&Client{Path: path, Timeout: upgradebudget.RequestLatency}).Call(Request{Op: OpRuntime})
	if err != nil || !resp.OK || resp.Runtime == nil || resp.Runtime.OpenFDs != 17 {
		t.Fatalf("original Unix route did not answer: resp=%+v err=%v", resp, err)
	}
}

func exerciseAdminInheritedClose(t *testing.T) {
	t.Helper()
	s := adminStart(t, adminReadBackend())
	info, err := os.Lstat(s.path)
	if err != nil {
		t.Fatalf("cold capture unlinked Unix socket: %v", err)
	}
	s.mu.Lock()
	original := s.keeper.Descriptor()
	s.mu.Unlock()
	adminCall(t, s.path)
	deadline := adminDeadline(t, upgradebudget.RequestLatency)
	if _, err := s.DetachImage(); err == nil {
		t.Fatal("active owner detached")
	}
	if err := s.FenceImage(deadline); err != nil {
		t.Fatal(err)
	}
	if err := s.JoinImage(deadline); err != nil {
		t.Fatal(err)
	}
	keeper, err := s.DetachImage()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = keeper.Close() }()
	desc, _, err := keeper.Export()
	if err != nil || desc != original {
		t.Fatalf("export changed original OFD identity: %+v %v", desc, err)
	}
	file, err := keeper.Detach()
	if err != nil {
		t.Fatal(err)
	}
	adopted, err := listenerimage.Adopt(desc, file)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	defer func() { _ = adopted.Close() }()
	// Connect while paused: the keeper must keep LISTEN and preserve backlog.
	queued, err := net.DialTimeout("unix", s.path, upgradebudget.RequestLatency)
	if err != nil {
		t.Fatalf("paused keeper refused original Unix route: %v", err)
	}
	defer func() { _ = queued.Close() }()
	if err := queued.SetDeadline(time.Now().Add(upgradebudget.RequestLatency)); err != nil {
		t.Fatal(err)
	}
	if _, err := queued.Write([]byte("{\"op\":\"runtime\"}\n")); err != nil {
		t.Fatal(err)
	}
	// Partial construction must leave the supplied keeper and route pristine.
	rejected := New(s.path+".wrong", adminReadBackend())
	if err := rejected.StartInherited(adopted, deadline); err == nil {
		t.Fatal("partial inherited construction accepted a different route")
	}
	if err := rejected.Close(); err != nil {
		t.Fatal(err)
	}
	next := New(s.path, adminReadBackend())
	if err := next.StartInherited(adopted, deadline); err != nil {
		t.Fatalf("inherited start took cold path: %v", err)
	}
	defer func() { _ = next.Close() }()
	if adopted.Descriptor() != original {
		t.Fatal("inherited start replaced original OFD")
	}
	gotInfo, err := os.Lstat(s.path)
	if err != nil || !os.SameFile(info, gotInfo) {
		t.Fatalf("inherited start changed pathname inode: %v", err)
	}
	// Retired normal cleanup must not unlink or close the replacement's route.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	var response Response
	if err := json.NewDecoder(queued).Decode(&response); err != nil || !response.OK {
		t.Fatalf("queued request lost on inherited start: %+v %v", response, err)
	}
	adminCall(t, s.path)
	if err := next.FenceImage(deadline); err != nil {
		t.Fatal(err)
	}
	if err := JoinImageReplies(next, deadline); err != nil {
		t.Fatal(err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(s.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("normal current owner did not unlink original pathname: %v", err)
	}
}

func exerciseAdminReplacementPath(t *testing.T) {
	t.Helper()
	s := adminStart(t, adminReadBackend())
	deadline := adminDeadline(t, upgradebudget.RequestLatency)
	if err := s.FenceImage(deadline); err != nil {
		t.Fatal(err)
	}
	if err := s.JoinImage(deadline); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(s.path, s.path+".old"); err != nil {
		t.Fatal(err)
	}
	replacement, err := net.Listen("unix", s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replacement.Close() }()
	info, err := os.Lstat(s.path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.Lstat(s.path)
	if err != nil || !os.SameFile(info, got) {
		t.Fatalf("stale normal close unlinked replacement: %v", err)
	}
}

func exerciseAdminImageRefusals(t *testing.T) {
	t.Helper()
	s := adminStart(t, adminReadBackend())
	for _, deadline := range []controlbudget.Deadline{0, 1} {
		if err := s.FenceImage(deadline); err == nil {
			t.Fatal("invalid deadline changed admission")
		}
		adminCall(t, s.path)
	}
	s.mu.Lock()
	keeper := s.keeper
	s.mu.Unlock()
	deadline := adminDeadline(t, upgradebudget.RequestLatency)
	wrong := New(s.path+".wrong", adminReadBackend())
	if err := wrong.StartInherited(keeper, deadline); err == nil {
		t.Fatal("wrong Unix identity accepted")
	}
	if err := wrong.StartInherited(nil, deadline); err == nil {
		t.Fatal("nil inherited keeper accepted")
	}
	if err := wrong.StartInherited(keeper, 0); err == nil {
		t.Fatal("expired inherited start accepted")
	}
	adminCall(t, s.path)
	if err := s.JoinImage(adminDeadline(t, 25*time.Millisecond)); err == nil || !strings.Contains(err.Error(), "not fenced") {
		t.Fatalf("unfenced Join did not reject admission immediately: %v", err)
	}
	if err := s.StartInherited(keeper, deadline); err == nil {
		t.Fatal("active owner started a second accept loop")
	}
	if err := s.FenceImage(deadline); err != nil {
		t.Fatal(err)
	}
	if err := s.JoinImage(deadline); err != nil {
		t.Fatal(err)
	}
	if err := s.StartInherited(keeper, deadline); err != nil {
		t.Fatalf("failed cut could not resume original owner: %v", err)
	}
	adminCall(t, s.path)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.JoinImage(deadline); err != nil {
		t.Fatal(err)
	}
}

func exerciseAdminCanceledRead(t *testing.T) {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	// ctx-root: SQL test reservation; released before the exercise completes.
	held, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	s := adminStart(t, Backend{DB: db})
	idle, err := net.DialTimeout("unix", s.path, upgradebudget.RequestLatency)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = idle.Close() }()
	adminWait(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.handlers) == 1 })
	result := make(chan error, 1)
	go func() {
		resp, callErr := (&Client{Path: s.path}).Call(Request{Op: OpSessions})
		if callErr == nil && resp.Error == "" {
			callErr = errors.New("blocked SQL read reported success")
		}
		result <- callErr
	}()
	adminWait(t, func() bool { return db.Stats().WaitCount > 0 })
	deadline := adminDeadline(t, upgradebudget.RequestLatency)
	if err := s.FenceImage(deadline); err != nil {
		t.Fatal(err)
	}
	if err := s.JoinImage(adminDeadline(t, upgradebudget.RequestLatency/2)); err != nil {
		t.Fatalf("accepted SQL/idle handlers failed to cancel and join: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("canceled SQL read lost its reply: %v", err)
	}
	s.mu.Lock()
	remaining := len(s.handlers)
	s.mu.Unlock()
	if remaining != 0 || db.Stats().InUse != 1 {
		t.Fatalf("join left accepted SQL handlers: handlers=%d DB=%+v", remaining, db.Stats())
	}
	if err := idle.SetReadDeadline(time.Now().Add(upgradebudget.RequestLatency)); err != nil {
		t.Fatal(err)
	}
	if _, err := idle.Read(make([]byte, 1)); err == nil {
		t.Fatal("idle pre-fence connection survived")
	}
}

func exerciseAdminCommittedWrite(t *testing.T) {
	t.Helper()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	fp := "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	s := adminStart(t, Backend{DB: db, SessionCreatorWrite: func(fp, addedBy, note string, allow bool) error {
		close(entered)
		<-release
		_, err := db.Exec(`INSERT INTO session_creators(fp, added_at, added_by, note) VALUES (?, CURRENT_TIMESTAMP, ?, ?)`, fp, addedBy, note)
		return err
	}})
	result := make(chan *Response, 1)
	go func() {
		resp, err := (&Client{Path: s.path}).Call(Request{Op: OpSessionAllow, FP: fp, Note: "committed"})
		if err != nil {
			resp = &Response{Error: err.Error()}
		}
		result <- resp
	}()
	adminWait(t, func() bool {
		select {
		case <-entered:
			return true
		default:
			return false
		}
	})
	deadline := adminDeadline(t, upgradebudget.RequestLatency)
	if err := s.FenceImage(deadline); err != nil {
		t.Fatal(err)
	}
	if err := s.JoinImage(adminDeadline(t, 25*time.Millisecond)); err == nil {
		t.Fatal("join declared accepted SQL writer drained before commit/reply")
	}
	if _, err := s.DetachImage(); err == nil {
		t.Fatal("unjoined writer permitted image detach")
	}
	once.Do(func() { close(release) })
	resp := <-result
	if !resp.OK {
		t.Fatalf("admitted write lost commit reply: %+v", resp)
	}
	if err := s.JoinImage(deadline); err != nil {
		t.Fatal(err)
	}
	var note string
	if err := db.QueryRow(`SELECT note FROM session_creators WHERE fp=?`, fp).Scan(&note); err != nil || note != "committed" {
		t.Fatalf("admitted SQL effect lost: %q %v", note, err)
	}
}

type adminClusterFunc func(Request) Response

func (fn adminClusterFunc) HandleCluster(req Request) Response { return fn(req) }

func exerciseAdminArmedReply(t *testing.T) {
	t.Helper()
	armed, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var s *Server
	var keeper listenerimage.Keeper
	var cutErr error
	s = adminStart(t, Backend{Cluster: adminClusterFunc(func(req Request) Response {
		deadline := adminDeadline(t, upgradebudget.RequestLatency)
		cutErr = s.FenceImage(deadline)
		if cutErr == nil {
			cutErr = s.JoinImage(deadline)
		}
		if cutErr == nil {
			keeper, cutErr = s.DetachImage()
		}
		close(armed)
		<-release
		return Response{Op: req.Op, OK: cutErr == nil, Reloading: cutErr == nil, NextGeneration: 2, BrokerImage: &proto.BrokerImageStatus{Generation: 1}}
	})})
	result := make(chan *Response, 1)
	go func() {
		resp, err := (&Client{Path: s.path}).Call(Request{Op: OpBrokerUpgradeReload})
		if err != nil {
			resp = &Response{Error: err.Error()}
		}
		result <- resp
	}()
	adminWait(t, func() bool {
		select {
		case <-armed:
			return true
		default:
			return false
		}
	})
	if cutErr != nil {
		once.Do(func() { close(release) })
		t.Fatalf("DB-free reload self-joined: %v", cutErr)
	}
	defer func() {
		if keeper != nil {
			_ = keeper.Close()
		}
	}()
	if err := JoinImageReplies(s, adminDeadline(t, 25*time.Millisecond)); err == nil {
		t.Fatal("reply barrier reported armed reply written before backend returned")
	}
	once.Do(func() { close(release) })
	resp := <-result
	if !resp.OK || !resp.Reloading || resp.NextGeneration != 2 || resp.BrokerImage == nil {
		t.Fatalf("DB-free armed reply lost protocol fields: %+v", resp)
	}
	if err := JoinImageReplies(s, adminDeadline(t, upgradebudget.RequestLatency)); err != nil {
		t.Fatal(err)
	}
}

func adminFDIdentities(t *testing.T) map[string]int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	identities := make(map[string]int)
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		} // ReadDir's own descriptor has closed.
		if err != nil {
			t.Fatal(err)
		}
		identities[target]++
	}
	return identities
}

func exerciseAdminCanceledCallback(t *testing.T, op string) {
	t.Helper()
	entered, canceled := make(chan struct{}), make(chan struct{})
	callback := func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		close(canceled)
		return ctx.Err()
	}
	s := adminStart(t, Backend{
		AuditTail: func(ctx context.Context, _ string, _ int) ([]AuditEntry, error) { return nil, callback(ctx) },
		EventsTail: func(ctx context.Context, _ int, _ time.Duration, _ string) ([]AuditEntry, bool, error) {
			return nil, false, callback(ctx)
		},
	})
	result := make(chan error, 1)
	go func() {
		resp, err := (&Client{Path: s.path}).Call(Request{Op: op, SID: "test"})
		if err == nil && resp.Error == "" {
			err = errors.New("canceled callback reported success")
		}
		result <- err
	}()
	adminWait(t, func() bool {
		select {
		case <-entered:
			return true
		default:
			return false
		}
	})
	deadline := adminDeadline(t, upgradebudget.RequestLatency)
	if err := s.FenceImage(deadline); err != nil {
		t.Fatal(err)
	}
	if err := s.JoinImage(adminDeadline(t, upgradebudget.RequestLatency/2)); err != nil {
		t.Fatalf("read callback failed to cancel and join: %v", err)
	}
	select {
	case <-canceled:
	default:
		t.Fatal("join returned before callback cancellation")
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func exerciseAdminRootCancel(t *testing.T) {
	t.Helper()
	// ctx-root: current control-owner cancellation exercise.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := New(filepath.Join(t.TempDir(), "admin.sock"), adminReadBackend())
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	idle, err := net.DialTimeout("unix", s.path, upgradebudget.RequestLatency)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = idle.Close() }()
	adminWait(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return len(s.handlers) == 1 })
	cancel()
	adminWait(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.closed })
	if err := s.JoinImage(adminDeadline(t, upgradebudget.RequestLatency)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(s.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("normal canceled owner retained pathname: %v", err)
	}
}

func exerciseAdminFailedReply(t *testing.T) {
	t.Helper()
	armed, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var s *Server
	s = adminStart(t, Backend{Cluster: adminClusterFunc(func(req Request) Response {
		err := s.FenceImage(adminDeadline(t, upgradebudget.RequestLatency))
		close(armed)
		<-release
		return Response{Op: req.Op, OK: err == nil, Reloading: err == nil}
	})})
	peer, err := net.DialTimeout("unix", s.path, upgradebudget.RequestLatency)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peer.Close() }()
	if _, err := peer.Write([]byte("{\"op\":\"broker_upgrade_reload\"}\n")); err != nil {
		t.Fatal(err)
	}
	adminWait(t, func() bool {
		select {
		case <-armed:
			return true
		default:
			return false
		}
	})
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	if err := JoinImageReplies(s, adminDeadline(t, upgradebudget.RequestLatency)); err == nil {
		t.Fatal("failed armed reply reported successful drain")
	}
	if err := s.JoinImage(adminDeadline(t, upgradebudget.RequestLatency)); err != nil {
		t.Fatalf("reply failure lost the SQL drain receipt: %v", err)
	}
}
