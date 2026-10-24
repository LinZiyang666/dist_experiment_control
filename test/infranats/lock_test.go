//go:build linux && amd64 && infranats_integration

package infranats_test

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/infralaunch"
	"github.com/LinZiyang666/tether/test/infraharness"
	"github.com/nats-io/nats.go"
	"golang.org/x/sys/unix"
)

const stockLockMode = "_TETHER_INFRA_NATS_LOCK_MODE"

// The helper is the same race-instrumented test binary, supervised before Start.
// It execs the official supplied server via the product Prepared.Exec operation.
// Only the stock server's /proc descriptors can prove its lock inheritance.
// origin: hot-update S9 plan §4.1 / §9.2 / T63 / M08
func TestNATSProcessLockFailureWindows(t *testing.T) {
	if os.Getenv(stockLockMode) == "exec" {
		stockLockHelper(t)
		return
	}
	for _, version := range []string{"2.14.5", "2.14.6"} {
		t.Run(version, func(t *testing.T) {
			for _, window := range []string{"worker_closes_last_sender_reference", "legacy_owner_alive", "replaced_inode", "alias_store"} {
				t.Run(window, func(t *testing.T) { exerciseStockLock(t, version, window) })
			}
		})
	}
}

func openFixtureFile(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	})
	return f
}

func lockReferences(t *testing.T, pid int, store c.Store) int {
	t.Helper()
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		t.Fatal(err)
	}
	refs := 0
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil || fd < 3 {
			continue
		}
		var stat unix.Stat_t
		if err := unix.Stat(fmt.Sprintf("/proc/%d/fd/%s", pid, entry.Name()), &stat); err != nil {
			if errors.Is(err, unix.ENOENT) {
				continue
			}
			t.Fatal(err)
		}
		if uint64(stat.Dev) == store.LockDevice && stat.Ino == store.LockInode {
			refs++
		}
	}
	return refs
}

func mustBusy(t *testing.T, dir, locks *os.File) {
	t.Helper()
	second, err := infralaunch.AcquireStore(dir, locks)
	if second != nil {
		_ = second.Close()
	}
	if !errors.Is(err, infralaunch.ErrStoreBusy) {
		t.Fatalf("second store launch admitted while stock owns the OFD: %v", err)
	}
}

func exerciseStockLock(t *testing.T, version, window string) {
	n := newStockNode(t, "lock"+strings.ReplaceAll(window, "_", ""))
	n.writeConfig(t, nil, false)
	binary, artifact := stockArtifact(t, version)
	locksPath := filepath.Join(n.root, "locks")
	if err := os.Mkdir(locksPath, 0700); err != nil {
		t.Fatal(err)
	}
	dir, locks := openFixtureFile(t, n.store), openFixtureFile(t, locksPath)
	guard, err := infralaunch.AcquireStore(dir, locks)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := guard.Close(); err != nil {
			t.Error(err)
		}
	}()
	store := guard.Identity()
	if window == "legacy_owner_alive" {
		// A direct legacy server has no lock. Flock succeeds, so a real census
		// of that exact live owner must independently refuse LaunchClaim.
		n.start(t, version)
		seedCurrentStore(t, n, 1) // A legacy owner needs real open JS assets before its census.
		if lockReferences(t, int(n.current.PID), store) != 0 {
			t.Fatal("legacy fixture unexpectedly inherited the guard")
		}
		exe, conf := openFixtureFile(t, binary), openFixtureFile(t, n.config)
		self, err := infralaunch.CurrentProcess(invocation("legacy-claim", version))
		if err != nil {
			t.Fatal(err)
		}
		request := launchRequest(t, self, store, artifact, hashOpenFile(t, conf))
		backend := launchBackend(t, request, infralaunch.Selection{Executable: exe, Config: conf, Directory: dir, Locks: locks, Guard: guard,
			Revision: 1, Journal: sha256.Sum256([]byte("launch journal"))}, func() c.Census {
			current, err := infrahost.ReadProcess(n.current.PID, budget(t, time.Second))
			if err != nil {
				t.Fatal(err)
			}
			// Store responsibility is proven by a real open JS file, not just
			// by the server argv or its ability to answer INFO.
			if !processHasStore(t, current.PID, n.store) {
				t.Fatal("legacy premise lacks actual open-store responsibility")
			}
			return c.Census{Complete: true, Generation: 1, Processes: []c.Process{current}, OldOwnerPending: 1, Digest: sha256.Sum256([]byte("legacy store census"))}
		})
		for _, entry := range []string{"service", "recover", "bootstrap", "reinstall"} {
			request.Tx[0]++
			receipt, err := backend.Do(request, budget(t, time.Second), nil)
			if !errors.Is(err, infralaunch.ErrStoreBusy) || receipt.Applied {
				t.Fatalf("legacy live owner bypassed %s launch: %v", entry, err)
			}
		}
		return
	}
	// The helper takes its own guard, so the parent relinquishes this OFD before
	// launch. Its immutable dev/inode is still the expected permanent lock.
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), path, "-test.run=^TestNATSProcessLockFailureWindows$", "-test.count=1")
	cmd.Dir = n.root
	cmd.Env = append(privateEnvironment(), stockLockMode+"=exec", "_TETHER_INFRA_NATS_LOCK_ROOT="+n.root,
		"_TETHER_INFRA_NATS_LOCK_BINARY="+binary, "_TETHER_INFRA_NATS_LOCK_VERSION="+version,
		"INVOCATION_ID="+invocation(n.name, version))
	if window == "replaced_inode" {
		cmd.Env = append(cmd.Env, "_TETHER_INFRA_NATS_LOCK_REPLACED=1")
	}
	stdinRead, stdinWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stdinRead.Close(); _ = stdinWrite.Close() }()
	outRead, outWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = outRead.Close(); _ = outWrite.Close() }()
	log, err := os.CreateTemp(n.root, "launcher-stderr-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := log.Close(); err != nil {
			t.Error(err)
		}
	}()
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdinRead, outWrite, log
	proc, err := infraharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if exists, err := infraharness.ProcessExists(proc.Identity()); err != nil {
			t.Error(err)
		} else if exists {
			_ = proc.Stop()
		}
	})
	_ = stdinRead.Close()
	_ = outWrite.Close()
	if err := outRead.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var before c.Process
	if err := json.NewDecoder(outRead).Decode(&before); err != nil {
		t.Fatal("launcher did not publish before-exec identity")
	}
	if before.PID != int32(proc.Identity().PID) || before.StartTicks != proc.Identity().StartTicks {
		t.Fatal("launcher handshake changed PID/start ticks")
	}
	if window == "replaced_inode" {
		name := fmt.Sprintf("%x-%x.lock", store.Device, store.Inode)
		if err := os.Rename(filepath.Join(locksPath, name), filepath.Join(locksPath, "displaced.lock")); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(locksPath, name), nil, 0640); err != nil {
			t.Fatal(err)
		}
		if err := json.NewEncoder(stdinWrite).Encode(artifact); err != nil {
			t.Fatal(err)
		}
		var rejection struct{ Code string }
		if err := json.NewDecoder(outRead).Decode(&rejection); err != nil || rejection.Code != "LOCK_INODE_REJECT" {
			t.Fatal("displaced permanent inode did not reject before stock exec")
		}
		if err := proc.Wait(); err != nil {
			t.Fatal(err)
		}
		if exists, err := infraharness.ProcessExists(proc.Identity()); err != nil || exists {
			t.Fatal("rejected launcher left an incarnation")
		}
		if err := os.Remove(filepath.Join(locksPath, name)); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(locksPath, "displaced.lock"), filepath.Join(locksPath, name)); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err := json.NewEncoder(stdinWrite).Encode(artifact); err != nil {
		t.Fatal(err)
	}
	poll(t, 5*time.Second, "stock same-PID exec", func() bool {
		current, err := infrahost.ReadProcess(before.PID, budget(t, time.Second))
		if err != nil {
			return false
		}
		if current.ExeSHA256 != artifact.BinarySHA256 {
			return false
		}
		if current.StartTicks != before.StartTicks || current.InvocationID != before.InvocationID || current.ExeInode == before.ExeInode {
			t.Fatal("stock exec changed incarnation or retained launcher image")
		}
		n.current = current
		return true
	})
	// Helper closed all sender/backend references before Exec. This must be
	// the server's own last lock reference, never a lingering parent guardian.
	if lockReferences(t, int(before.PID), store) != 1 {
		t.Fatal("official stock server did not inherit exactly one lifetime-lock reference")
	}
	mustBusy(t, dir, locks)
	if window == "alias_store" {
		alias := filepath.Join(n.root, "alias")
		if err := os.Symlink(n.store, alias); err != nil {
			t.Fatal(err)
		}
		mustBusy(t, openFixtureFile(t, alias), locks)
	}
	pin, err := infrahost.PinProcess(n.current, budget(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := pin.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := pin.Signal(c.SignalStop, budget(t, time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if err := proc.Wait(); err != nil {
		var exit *exec.ExitError
		if !errorsAsKilled(err, &exit) {
			t.Fatal(err)
		}
	}
	if exists, err := infraharness.ProcessExists(proc.Identity()); err != nil || exists {
		t.Fatal("stock server or supervisor left a zombie")
	}
	last, err := infralaunch.AcquireStore(dir, locks)
	if err != nil {
		t.Fatal("server exit did not release the last OFD reference")
	}
	if last.Identity() != store {
		t.Fatal("retirement replaced the permanent lock inode")
	}
	if err := last.Close(); err != nil {
		t.Fatal(err)
	}
}

func processHasStore(t *testing.T, pid int32, store string) bool {
	t.Helper()
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		target, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, entry.Name()))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			t.Fatal(err)
		}
		if strings.HasPrefix(target, store+"/") {
			return true
		}
	}
	return false
}

func launchRequest(t *testing.T, self c.Process, store c.Store, artifact c.Artifact, config c.Digest) c.HostRequest {
	t.Helper()
	return c.HostRequest{Version: 1, Op: c.LaunchClaim, Component: c.NATS, Tx: c.ID{1}, Step: 1, Domain: c.ID{2}, Sequence: 1,
		PolicyRevision: 1, KeyID: "local-launch", HostBootID: self.BootID, Nonce: c.ID{3}, Expected: self, Candidate: artifact,
		Store: store, ExpectedJournal: sha256.Sum256([]byte("expected journal")), Config: config, Payload: sha256.Sum256([]byte("selected immutable slot")), RemainingNS: uint64(10 * time.Second)}
}

func launchBackend(t *testing.T, r c.HostRequest, s infralaunch.Selection, census func() c.Census) *infralaunch.Backend {
	t.Helper()
	backend, err := infralaunch.NewBackend(infralaunch.Bindings{
		// This L2 binding supplies immutable test selections only. It cannot
		// certify root authorization, systemd metadata or a product journal.
		Authorize:   func(c.HostRequest, c.Deadline, c.Stop) (infralaunch.Selection, error) { return s, nil },
		StoreCensus: func(c.Store, c.Deadline, c.Stop) (c.Census, error) { return census(), nil },
		Process: func(expected c.Process, d c.Deadline) (c.Process, error) {
			if expected.PID == int32(os.Getpid()) {
				return infralaunch.CurrentProcess(r.Expected.InvocationID)
			}
			return infrahost.ReadProcess(expected.PID, d)
		},
		Inspect: func(c.ID, c.Deadline) (c.Census, error) { return census(), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Error(err)
		}
	})
	return backend
}

func stockLockHelper(t *testing.T) {
	root := os.Getenv("_TETHER_INFRA_NATS_LOCK_ROOT")
	binary := os.Getenv("_TETHER_INFRA_NATS_LOCK_BINARY")
	version := os.Getenv("_TETHER_INFRA_NATS_LOCK_VERSION")
	dir, locks := openFixtureFile(t, filepath.Join(root, "store")), openFixtureFile(t, filepath.Join(root, "locks"))
	guard, err := infralaunch.AcquireStore(dir, locks)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := guard.Close(); err != nil {
			t.Error(err)
		}
	})
	exe, conf := openFixtureFile(t, binary), openFixtureFile(t, filepath.Join(root, "nats.conf"))
	self, err := infralaunch.CurrentProcess(os.Getenv("INVOCATION_ID"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(self); err != nil {
		t.Fatal(err)
	}
	var artifact c.Artifact
	if err := json.NewDecoder(os.Stdin).Decode(&artifact); err != nil || artifact.Version != version || artifact.BinarySHA256 != hashOpenFile(t, exe) {
		t.Fatal("exec authorization handshake missing")
	}
	r := launchRequest(t, self, guard.Identity(), artifact, hashOpenFile(t, conf))
	selection := infralaunch.Selection{Executable: exe, Config: conf, Directory: dir, Locks: locks, Guard: guard, Revision: 1, Journal: sha256.Sum256([]byte("launch journal"))}
	backend := launchBackend(t, r, selection, func() c.Census {
		entries, err := os.ReadDir(filepath.Join(root, "store"))
		if err != nil || len(entries) != 0 {
			return c.Census{}
		}
		return c.Census{Complete: true, Generation: 1, Digest: sha256.Sum256([]byte("private empty store census"))}
	})
	receipt, err := backend.Do(r, budget(t, 5*time.Second), nil)
	if os.Getenv("_TETHER_INFRA_NATS_LOCK_REPLACED") == "1" {
		if !errors.Is(err, infralaunch.ErrLaunchRejected) || receipt.Applied {
			t.Fatal("displaced permanent inode was admitted")
		}
		if err := json.NewEncoder(os.Stdout).Encode(struct{ Code string }{"LOCK_INODE_REJECT"}); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err != nil || !receipt.Applied {
		t.Fatalf("stock launch rejected: %v", err)
	}
	prepared, err := backend.Transfer(r)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := prepared.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Exec(budget(t, 5*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	t.Fatal("stock exec unexpectedly returned")
}

// A witness delegates to the real product gate and records only its successful
// original-checkpoint verification. Calling a second Seal after Completed
// would prove the wrong instant and could hide repair-before-read mutations.
// origin: hot-update S9 plan §4.3 / T60/T61 / M20
type recoveryWitness struct {
	c.Recovery
	t        *testing.T
	node     *stockNode
	baseline *storeBaseline
	verified int
}

func (w *recoveryWitness) VerifyBeforeReplay(cp c.Checkpoint, d c.Deadline, stop c.Stop) error {
	if cp.Proof == (c.Digest{}) || !cp.Assets.Complete || !cp.Transfers.Complete {
		return c.ErrIncomplete
	}
	verifyRaw(w.t, dataConnection(w.t, w.node.url), w.baseline.object)
	if err := w.Recovery.VerifyBeforeReplay(cp, d, stop); err != nil {
		return err
	}
	w.verified++
	return nil
}

func stockPlan(t *testing.T, n *stockNode, tx byte, version string, action c.Action) c.Plan {
	t.Helper()
	_, candidate := stockArtifact(t, version)
	dir, err := os.Open(n.store)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := dir.Close(); err != nil {
			t.Error(err)
		}
	}()
	store, err := infralaunch.StoreIdentity(dir)
	if err != nil {
		t.Fatal(err)
	}
	config, err := os.ReadFile(n.config)
	if err != nil {
		t.Fatal(err)
	}
	return c.Plan{Tx: c.ID{tx}, Step: 1, Domain: c.ID{7}, Component: c.NATS, Action: action, Candidate: candidate, Previous: n.artifact,
		Before: n.current, ConfigSHA256: sha256.Sum256(config), Store: store, PolicyRevision: 1}
}

// Native refusal survives recovery: repair may enable a new transaction, but
// it cannot retroactively make the failed original observation window green.
// The fixtures below mutate real server placement/availability, not Status.
// origin: hot-update S9 plan §3.2 / §4.2 / §9.2 / T62 / M13/M15
func TestNATSQuorumAndReplicaRefusals(t *testing.T) {
	for _, fault := range []string{"N2", "lost_quorum", "stream_R1", "stream_R2", "consumer_lag"} {
		t.Run(fault, func(t *testing.T) {
			service, _ := productService(t)
			count := 3
			if fault == "N2" {
				count = 2
			}
			var nodes []*stockNode
			for idx := 0; idx < count; idx++ {
				nodes = append(nodes, newStockNode(t, fmt.Sprint("refusal", idx)))
			}
			for _, n := range nodes {
				n.writeConfig(t, nodes, true)
				n.start(t, "2.14.5")
			}
			baseline := seedCurrentStore(t, nodes[0], count)
			if count == 3 {
				waitCurrent(t, nodes)
			}
			n := nodes[0]
			var restore func()
			switch fault {
			case "N2":
				restore = func() {
					nodes = append(nodes, newStockNode(t, "refusal2"))
					for _, peer := range nodes {
						peer.writeConfig(t, nodes, true)
						if peer.proc == nil {
							peer.start(t, "2.14.5")
						} else {
							peer.reload(t)
						}
					}
					raiseRecoveredReplicas(t, n)
					waitCurrent(t, nodes)
				}
			case "lost_quorum":
				nodes[1].stop(t, c.SignalStop)
				nodes[2].stop(t, c.SignalStop)
				restore = func() { nodes[1].start(t, "2.14.5"); nodes[2].start(t, "2.14.5"); waitCurrent(t, nodes) }
			case "stream_R1", "stream_R2":
				legacy, err := dataConnection(t, n.url).Conn().JetStream()
				if err != nil {
					t.Fatal(err)
				}
				info, err := legacy.StreamInfo("OBJ_payload")
				if err != nil {
					t.Fatal(err)
				}
				cfg := info.Config
				cfg.Replicas = 1
				if fault == "stream_R2" {
					cfg.Replicas = 2
				}
				cfg.Placement = &nats.Placement{Cluster: "infra", Tags: []string{}}
				if _, err := legacy.UpdateStream(&cfg); err != nil {
					t.Fatal(err)
				}
				// Verify that the target actually participates; a target-off R1 is
				// allowed and cannot serve as a negative-control premise.
				info, err = legacy.StreamInfo("OBJ_payload")
				if err != nil {
					t.Fatal(err)
				}
				if info.Cluster == nil {
					t.Fatal("SETUP-RED: constrained-group placement missing")
				}
				for _, candidate := range nodes {
					if candidate.name == info.Cluster.Leader {
						n = candidate
					}
				}
				if n.name != info.Cluster.Leader {
					t.Fatal("SETUP-RED: constrained-group owner unknown")
				}
				restore = func() {
					cfg.Replicas = 3
					if _, err := legacy.UpdateStream(&cfg); err != nil {
						t.Fatal(err)
					}
					waitCurrent(t, nodes)
				}
			case "consumer_lag":
				legacy, err := dataConnection(t, n.url).Conn().JetStream()
				if err != nil {
					t.Fatal(err)
				}
				info, err := legacy.ConsumerInfo("OBJ_payload", "original")
				if err != nil || info.Cluster == nil {
					t.Fatal("consumer group premise missing")
				}
				var peer *stockNode
				for _, replica := range info.Cluster.Replicas {
					for _, candidate := range nodes {
						if candidate != n && candidate.name == replica.Name {
							peer = candidate
						}
					}
				}
				if peer == nil {
					t.Fatal("consumer lag requires an actual off-target follower")
				}
				fd, err := unix.PidfdOpen(int(peer.current.PID), 0)
				if err != nil {
					t.Fatal(err)
				}
				if err := unix.PidfdSendSignal(fd, unix.SIGSTOP, nil, 0); err != nil {
					_ = unix.Close(fd)
					t.Fatal(err)
				}
				restore = func() {
					if err := unix.PidfdSendSignal(fd, unix.SIGCONT, nil, 0); err != nil {
						t.Fatal(err)
					}
					_ = unix.Close(fd)
					waitCurrent(t, nodes)
				}
				// Advance the actual consumer's delivered state while its follower
				// is stopped; unrelated stream writes cannot establish consumer lag.
				sub, err := legacy.PullSubscribe("", "original", nats.Bind("OBJ_payload", "original"))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := sub.Fetch(1, nats.MaxWait(time.Second)); err != nil {
					_ = sub.Unsubscribe()
					t.Fatal(err)
				}
				if err := sub.Unsubscribe(); err != nil {
					t.Fatal(err)
				}
				poll(t, 2*time.Second, "real consumer currentness deficit", func() bool {
					info, err := legacy.ConsumerInfo("OBJ_payload", "original")
					if err != nil || info.Cluster == nil {
						return false
					}
					for _, replica := range info.Cluster.Replicas {
						if replica.Name == peer.name && replica.Lag > 0 {
							return true
						}
					}
					return false
				})
			}
			if restore != nil {
				originalRestore, restored := restore, false
				restore = func() {
					if restored {
						return
					}
					restored = true
					originalRestore()
				}
				t.Cleanup(restore)
			}
			plan := stockPlan(t, n, byte(40+count), "2.14.6", c.Upgrade)
			before := n.current
			status, err := service.Start(plan, budget(t, 10*time.Second), nil)
			if err == nil || status.Phase != c.Deferred || status.Code == "" || status.Phase == c.Completed {
				t.Fatal("actual topology/quorum/replica fault was accepted")
			}
			current, readErr := infrahost.ReadProcess(before.PID, budget(t, time.Second))
			if readErr != nil || current != before {
				t.Fatal("refusal signalled or replaced the original NATS")
			}
			refusal := status
			restore()
			original, err := service.Status(plan.Tx, budget(t, time.Second))
			if err != nil || original.Phase != refusal.Phase || original.Code != refusal.Code {
				t.Fatal("recovery erased the original red window")
			}
			verifyRaw(t, dataConnection(t, n.url), baseline.object)
			plan.Tx = c.ID{99}
			status, err = service.Start(plan, budget(t, 120*time.Second), nil)
			if err != nil || status.Phase != c.Completed {
				t.Fatalf("repaired topology did not enable a new real transaction: %v", err)
			}
		})
	}
}

func raiseRecoveredReplicas(t *testing.T, n *stockNode) {
	t.Helper()
	legacy, err := dataConnection(t, n.url).Conn().JetStream()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"OBJ_payload", "AUDIT"} {
		info, err := legacy.StreamInfo(name)
		if err != nil {
			t.Fatal(err)
		}
		cfg := info.Config
		cfg.Replicas = 3
		if _, err := legacy.UpdateStream(&cfg); err != nil {
			t.Fatal(err)
		}
	}
	consumer, err := legacy.ConsumerInfo("OBJ_payload", "original")
	if err != nil {
		t.Fatal(err)
	}
	cfg := consumer.Config
	cfg.Replicas = 3
	if _, err := legacy.UpdateConsumer("OBJ_payload", &cfg); err != nil {
		t.Fatal(err)
	}
}

func privateEnvironment() []string {
	// No production HOME/config or credential variables reach a stock process.
	return []string{"PATH=/usr/bin:/bin", "LANG=C", "GORACE=atexit_sleep_ms=0"}
}
