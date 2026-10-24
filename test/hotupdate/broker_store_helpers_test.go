//go:build linux

package hotupdate_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/clusterharness"
)

func startBrokerStoreFixture(t *testing.T, topology string) *clusterharness.BrokerImageFixture {
	t.Helper()
	f, err := clusterharness.StartBrokerImageFixture(t, clusterharness.BrokerImageFixtureConfig{Topology: topology})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func brokerStoreCheckpoint(t *testing.T, f *clusterharness.BrokerImageFixture, label string) clusterharness.BrokerStoreEvidence {
	t.Helper()
	row, err := f.Checkpoint(label)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(row.JSON)
	if row.Label != label || row.Digest != hex.EncodeToString(sum[:]) {
		t.Fatal("checkpoint digest or label mismatch")
	}
	var value clusterharness.BrokerStoreEvidence
	if err := json.Unmarshal(row.JSON, &value); err != nil {
		t.Fatal(err)
	}
	if value.PID <= 0 || value.StartTicks == 0 || value.RunningSHA == "" || value.SourceSHA == "" || value.Generation == 0 || value.Command == "" || value.LogMapping == nil {
		t.Fatal("checkpoint lacks independent process/source identity and evidence mapping")
	}
	if strings.Contains(string(row.JSON), "secret") || strings.Contains(string(row.JSON), "PRIVATE KEY") {
		t.Fatal("private material appeared in fixture output")
	}
	if value.Topology != "single" {
		for name, expected := range map[string]time.Duration{"raft_heartbeat_ns": cluster.MultinodeHeartbeatTimeout, "raft_election_ns": cluster.MultinodeElectionTimeout, "raft_lease_ns": cluster.MultinodeLeaderLeaseTimeout} {
			if value.OracleInputs[name] != strconv.FormatInt(int64(expected), 10) {
				t.Fatalf("fixture shrank or failed to observe actual production Raft timer %s", name)
			}
		}
	}
	return value
}

func brokerStoreACK(t *testing.T, f *clusterharness.BrokerImageFixture, id string) clusterharness.BrokerStoreEvidence {
	t.Helper()
	row := brokerStoreCheckpoint(t, f, "write/"+id)
	sum := sha256.Sum256([]byte("ack:" + id))
	if row.ACKs[id] != hex.EncodeToString(sum[:]) || row.Effects[id] != 1 {
		t.Fatalf("ACK does not prove one committed nonce effect for %s", id)
	}
	return row
}

func brokerStoreAssertClosed(t *testing.T, row clusterharness.BrokerStoreEvidence) {
	t.Helper()
	seal := row.Seal
	if seal.Writer != storage.CloseClosed || seal.ReadOnly != storage.CloseClosed || seal.Bolt != storage.CloseClosed || seal.Raft != storage.CloseClosed || seal.Transport != storage.CloseClosed ||
		seal.DriverWriters != 0 || seal.DriverReaders != 0 || seal.ActiveRPCs != 0 || seal.ActiveBackups != 0 || row.Drivers != [2]uint32{} || row.Counters["sqlite_fds"] != 0 {
		t.Fatalf("seal does not describe actual closed owners: phases=%v seal_drivers=%v drivers=%v rpc=%d backups=%d counters=%v", []storage.ClosePhase{seal.Writer, seal.ReadOnly, seal.Bolt, seal.Raft, seal.Transport}, [2]uint32{seal.DriverWriters, seal.DriverReaders}, row.Drivers, seal.ActiveRPCs, seal.ActiveBackups, row.Counters)
	}
	switch row.Store.Journal {
	case "wal":
		if seal.CheckpointBusy != 0 || seal.CheckpointLog != 0 || seal.CheckpointDone != 0 {
			t.Fatal("WAL was not really truncated before writer close")
		}
	case "delete":
		if seal.CheckpointBusy != -1 || seal.CheckpointLog != -1 || seal.CheckpointDone != -1 {
			t.Fatal("DELETE store lacks its explicit non-applicable checkpoint receipt")
		}
	default:
		t.Fatalf("unsupported journal %q", row.Store.Journal)
	}
	if row.Store.Synchronous != 2 {
		t.Fatal("effective synchronous is not FULL")
	}
}

func brokerStoreAssertCurrent(t *testing.T, before, after clusterharness.BrokerStoreEvidence) {
	t.Helper()
	if after.PID != before.PID || after.StartTicks != before.StartTicks {
		t.Fatal("component exec replaced the process identity")
	}
	if after.RunningSHA == before.RunningSHA || after.Generation != before.Generation+1 {
		t.Fatal("same-PID exec did not replace the mapped artifact and advance generation")
	}
	if before.Store.Identity != after.Store.Identity || !reflect.DeepEqual(before.Store.Schema, after.Store.Schema) || before.Store.Journal != after.Store.Journal || after.Store.Synchronous != 2 {
		t.Fatal("hot opener changed the current database, schema or durability")
	}
	if after.ReplayGateCount != 1 || !reflect.DeepEqual(after.BeforeReplay, before.ACKs) || after.BeforeReplayCursor < before.Store.AppliedIndex {
		t.Fatalf("durable ACK prefix was missing before the first Raft replay: gate=%d before=%v observed=%v cursor=%d floor=%d", after.ReplayGateCount, before.ACKs, after.BeforeReplay, after.BeforeReplayCursor, before.Store.AppliedIndex)
	}
	if !after.SecondRestoreEmpty || after.Counters["handoff_fds"] != 0 || after.Counters["sealed_custody_fds"] != 0 || after.Drivers[0] != 1 {
		t.Fatal("receiver retained a dispatch or failed to prove one current writer")
	}
}

type brokerLockProbe struct {
	stop               chan struct{}
	done               chan struct{}
	mu                 sync.Mutex
	attempts, acquired int
	err                error
}

// The parent is a different process/open-file-description from the exec owner.
// Each attempt uses the production acquisition path and observes real contention.
func startBrokerLockProbe(t *testing.T, dir string) *brokerLockProbe {
	t.Helper()
	p := &brokerLockProbe{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(p.done)
		for {
			lock, err := cluster.AcquireImageDataDirLock(dir)
			p.mu.Lock()
			p.attempts++
			if lock != nil {
				p.acquired++
				p.err = lock.Release()
			} else if err == nil || !strings.Contains(err.Error(), cluster.ErrDataDirLockHeld.Error()) {
				p.err = err
				if err == nil {
					p.err = fmt.Errorf("missing contention error")
				}
			}
			p.mu.Unlock()
			select {
			case <-p.stop:
				return
			case <-time.After(2 * time.Millisecond):
			}
		}
	}()
	t.Cleanup(func() { p.close() })
	return p
}
func (p *brokerLockProbe) close() {
	select {
	case <-p.done:
		return
	default:
	}
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
	<-p.done
}
func (p *brokerLockProbe) result() (int, int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.attempts, p.acquired, p.err
}

// A blocked second Bolt open is observed AFTER its entry marker. Kill always
// pairs with Wait; successful open after owner close is the independent control.
// origin: hot-update S6 plan §3.1 / T06
func brokerProbeBolt(t *testing.T, dir string, blocked bool) {
	t.Helper()
	root := filepath.Dir(dir)
	path := filepath.Join(root, "store-owner")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, "-test.run=^TestBrokerStoreBoltProbe$", "-test.count=1", filepath.Join(dir, "raft", "raft.db"))
	cmd.Dir = root
	for _, row := range handoff.CleanEnvironment(os.Environ()) {
		name, _, _ := strings.Cut(row, "=")
		if name != "HOME" && name != "GORACE" && !strings.HasPrefix(name, "TETHER_") && !strings.HasPrefix(name, "_TETHER_") {
			cmd.Env = append(cmd.Env, row)
		}
	}
	cmd.Env = append(cmd.Env, "HOME="+root, "TETHER_HOME="+root, "GORACE="+strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0"), "_TETHER_S6_STORE_FIXTURE=bolt")
	cleanup := hotUpdateOwnProcessGroup(cmd)
	defer cleanup()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	cmd.Stdout = writer
	waitProcess, err := testharness.StartProcess(cmd)
	if err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	_ = writer.Close()
	done := make(chan struct{})
	lines := make(chan string, 2)
	readDone := make(chan struct{})
	var waitErr error
	go func() {
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(readDone)
	}()
	go func() { waitErr = waitProcess(); close(done) }()
	defer func() { _ = cmd.Process.Kill(); <-done; <-readDone }()
	select {
	case line := <-lines:
		if line != "BOLT_SECOND_OPEN_ENTER" {
			t.Fatal("second-open entry was not observed")
		}
	case <-ctx.Done():
		t.Fatal("Bolt helper never entered the opener")
	}
	if blocked {
		select {
		case <-done:
			t.Fatalf("second open did not block under real Bolt owner: %v", waitErr)
		case <-time.After(200 * time.Millisecond):
		}
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		<-done
		if waitErr == nil || cmd.ProcessState == nil {
			t.Fatal("watchdog did not kill and Wait for the blocked Bolt helper")
		}
	} else {
		<-done
		if waitErr != nil {
			t.Fatal("Bolt could not reopen after actual owner close", waitErr)
		}
		<-readDone
		select {
		case line := <-lines:
			if line != "BOLT_SECOND_OPEN_DONE" {
				t.Fatal("missing released-Bolt positive control")
			}
		default:
			t.Fatal("helper exit did not prove a successful Bolt open")
		}
	}
	<-readDone
}
