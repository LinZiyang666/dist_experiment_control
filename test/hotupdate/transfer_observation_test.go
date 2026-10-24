//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/transferharness"
	server "github.com/nats-io/nats-server/v2/server"
)

// A completed command cannot stand in for an observation: every live owner
// must supply its own sealed records before any image-cut test can use them.
// origin: S7 internal review INTEG-M F1
func TestTransferProductionOwnersEmitCustody(t *testing.T) {
	a := buildTransferArtifacts(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				exerciseTransferProductionCustody(t, a)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "production transfer observation adapters", g)
	testharness.AssertNoFDLeak(t, "production transfer observation adapters", fd, 0)
}

func exerciseTransferProductionCustody(t *testing.T, a transferArtifacts) {
	for _, direction := range []string{"push", "pull"} {
		t.Run(direction, func(t *testing.T) {
			t.Parallel()
			s := newTransferStack(t, a)
			s.requireCapable()
			r := startTransferRun(t, s, transferCell{"b", direction, "agent", "synced-prefix"})
			defer r.watch.close()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			result, err := s.ctl.Wait(ctx)
			if err != nil || result.NativeRC != 0 {
				t.Fatalf("original transfer failed: rc=%d err=%v %s", result.NativeRC, err, s.output(s.ctl)+"\nagent: "+s.output(s.agent)+"\nbroker: "+s.output(s.broker))
			}
			body, err := os.ReadFile(r.target)
			transferMust(t, err)
			if !bytes.Equal(body, r.body) {
				t.Fatal("original transfer bytes changed")
			}
			for _, role := range []string{"agent", "broker", "ctl"} {
				if role != "ctl" {
					if err := s.collectors[role].drain(); err != nil {
						t.Fatalf("%s drain: %v\nagent: %s\nbroker: %s", role, err, s.output(s.agent), s.output(s.broker))
					}
				}
				rows, err := s.collectors[role].snapshot()
				transferMust(t, err)
				if len(rows) == 0 {
					t.Fatalf("%s supplied no production transfer observations despite native rc=0", role)
				}
				// Ledger retirement cannot stand in for the worker's joined actors.
				// origin: hot-update S7 plan §10.4 / S7 INTEG-O retained receipt seam
				if role == "agent" && !testharness.WaitFor(t, 5*time.Second, 10*time.Millisecond, func() bool {
					transferMust(t, s.collectors[role].drain())
					current, err := s.collectors[role].snapshot()
					transferMust(t, err)
					for _, row := range current {
						if row.Kind == x.ObserveRetirement && row.Phase == x.PhaseSettled && row.AdmissionClosed && row.HistorySeq > 0 && row.CanonicalDigest != "" && row.OriginalDeadline.Deadline > 0 {
							return true
						}
					}
					return false
				}) {
					t.Fatal("worker supplied no exact receipt after original actor Stop/Join")
				}
				if role == "broker" {
					found := false
					for _, row := range rows {
						if row.Kind == x.ObserveReserve && row.GrantNonce != "" {
							status := fullstackReadTransferStatus(t, s, row.TransferID, x.Status{})
							if status.Identity.EntryNonce != row.EntryNonce || status.State != x.StatusTerminal {
								t.Fatal("private status read lost original terminal authority")
							}
							found = true
							break
						}
					}
					if !found {
						t.Fatal("broker grant observation missing")
					}
				}
			}
			receiver := "agent"
			if direction == "pull" {
				receiver = "ctl"
			}
			rows, err := s.collectors[receiver].snapshot()
			transferMust(t, err)
			found := false
			primitive := false
			for _, row := range rows {
				if transferWindowMatches("synced-prefix", row) && row.DurableOffset > 0 && row.DurableOffset < int64(len(r.body)) {
					transferMust(t, validateTransferBoundary(r.cell, row, int64(len(r.body))))
					found = true
				}
				// The entry record is the boundary before the irreversible syscall.
				// A record stamped after inflight begins cannot select that window.
				// origin: S7 internal review INTEG-M F7 / hot-update S7 plan §10.4
				if row.Kind == x.ObservePrimitiveEnter {
					if row.Outcome != x.OutcomeNotEntered {
						t.Fatal("primitive entry observation lost the unentered boundary")
					}
					primitive = true
				}
			}
			if !found {
				t.Fatal("receiver supplied no nonempty synced prefix")
			}
			if !primitive {
				t.Fatal("receiver supplied no primitive entry observation")
			}
		})
	}
}

// A restarted NATS server's independent oracle can reconnect before the
// production broker and agent restore their subscriptions. Capability must
// come from that real topology before the late-terminal relay admits business.
// origin: S7 internal review INTEG-M F6
func TestTransferRelayWaitsForProductionSubscriptions(t *testing.T) {
	a := buildTransferArtifacts(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				exerciseTransferRelayReadiness(t, a)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "production transfer relay readiness", g)
	testharness.AssertNoFDLeak(t, "production transfer relay readiness", fd, 0)
}

func exerciseTransferRelayReadiness(t *testing.T, a transferArtifacts) {
	s := newTransferStack(t, a)
	s.requireCapable()
	fullstackLateTransferRelay(t, s)
}

func transferProductionSubscriptionsReady(s *transferStack) bool {
	probe, err := proto.SubjCtrlTransferRequestWildcard(x.OpProbe)
	if err != nil {
		return false
	}
	info, err := s.ns.Connz(&server.ConnzOptions{Subscriptions: true})
	if err != nil {
		return false
	}
	broker, agent := false, false
	for _, conn := range info.Conns {
		for _, sub := range conn.Subs {
			broker = broker || sub == probe
			agent = agent || sub == proto.SubjCmdForwarded(s.sid, s.nid, "*")
		}
	}
	return broker && agent
}

// The original capable prepare carries the full signed owner binding. A
// settled ledger row may already be retired when an independent status reader
// starts; rediscovering from that census would discard retained authority.
// origin: S7 internal review INTEG-M F5
func fullstackOriginalTransferAuthority(t *testing.T, s *transferStack, scope x.Identity) (x.Identity, x.Origin) {
	t.Helper()
	s.wireMu.Lock()
	defer s.wireMu.Unlock()
	for _, message := range s.wire {
		var request struct {
			Continuation *proto.TransferContinuation `json:"continuation"`
		}
		if json.Unmarshal(message.Data, &request) != nil || request.Continuation == nil {
			continue
		}
		id, origin := x.Identity(request.Continuation.Identity), x.Origin(request.Continuation.Origin)
		if id.SID != scope.SID || id.NID != scope.NID || id.ActorFP != scope.ActorFP || id.TransferID != scope.TransferID {
			continue
		}
		transferMust(t, proto.ValidateTransferWire(proto.Identity(id)))
		transferMust(t, proto.ValidateTransferWire(proto.Origin(origin)))
		return id, origin
	}
	t.Fatal("original transfer request supplied no retained status authority")
	return x.Identity{}, x.Origin{}
}

// The first fetched chunk has no durable prefix. Only a later byte boundary
// can satisfy the frozen cut profile; the validator still checks all evidence.
// origin: S7 internal review INTEG-M F1
func TestTransferByteBoundaryRequiresNonemptySyncedPrefix(t *testing.T) {
	for _, window := range []string{"receiver-write", "Sync-pending", "synced-prefix"} {
		row := x.Observation{FetchedOffset: 65536, Kind: x.ObserveSynced}
		if window == "receiver-write" {
			row.Kind = x.ObserveFetched
		}
		row.FileSynced = window == "synced-prefix"
		if transferWindowMatches(window, row) {
			t.Fatalf("%s accepted an empty durable prefix", window)
		}
		row.DurableOffset = 32768
		if !transferWindowMatches(window, row) {
			t.Fatalf("%s lost its nonempty byte boundary", window)
		}
	}
}

// The ACTIVE image slot must identify the imported tracker before constructing
// business roots. A later Bind event cannot prove this installation ordering.
// origin: S7 internal review INTEG-M F1
func TestTransferBrokerImageInstallationEmitsCustody(t *testing.T) {
	a := buildTransferArtifacts(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				exerciseTransferBrokerImageCustody(t, a)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "transfer broker installation custody", g)
	testharness.AssertNoFDLeak(t, "transfer broker installation custody", fd, 0)
}

func exerciseTransferBrokerImageCustody(t *testing.T, a transferArtifacts) {
	s := newTransferStack(t, a)
	s.requireCapable()
	r := startTransferRun(t, s, transferCell{"b", "push", "broker", "synced-prefix"})
	defer r.watch.close()
	r.boundary = awaitTransferBoundary(t, s, r)
	r.id = r.boundary.TransferID
	transferMust(t, validateTransferBoundary(r.cell, r.boundary, int64(len(r.body))))
	s.brokerCut(false)
	// The installed maintenance cut now carries its authority's 64-byte token.
	// Compare the actual admin identity rather than guessing a token width.
	// origin: S7 INTEG-O current maintenance cut / hot-update S7 plan §10.4
	status, err := (&adminsock.Client{Path: s.admin, Timeout: time.Second}).Call(adminsock.Request{Op: adminsock.OpRuntime})
	transferMust(t, err)
	if !status.OK || status.Runtime == nil || status.Runtime.BrokerImage == nil || status.Runtime.BrokerImage.CutID == "" || status.Runtime.BrokerImage.RunningSHA256 != a.shaB {
		t.Fatal("current admin owner supplied no actual cut identity")
	}
	current := status.Runtime.BrokerImage
	var installed x.Observation
	if !testharness.WaitFor(t, 5*time.Second, 10*time.Millisecond, func() bool {
		rows, err := s.collectors["broker"].snapshot()
		transferMust(t, err)
		for _, row := range rows {
			if row.TransferID == r.id && row.Kind == x.ObserveInstall && row.CutID == current.CutID && row.Generation == current.Generation && row.ReaderSHA == current.RunningSHA256 {
				installed = row
				return true
			}
		}
		return false
	}) {
		t.Fatal("actual ACTIVE slot supplied no imported transfer observation")
	}
	if !installed.Active || !installed.InstalledBeforeReaper || installed.EntryNonce != r.boundary.EntryNonce || installed.ObjectVersion != r.boundary.ObjectVersion || installed.OriginalDeadline.Deadline == 0 {
		t.Fatal("installation lost original tracker authority or deadline")
	}
	identity, err := s.broker.SampleIdentity()
	transferMust(t, err)
	if installed.PID != identity.PID || installed.ProcessStart != identity.ProcessStart || installed.BootID != identity.BootID || installed.Generation <= 1 {
		t.Fatal("installation does not belong to the original broker's current image")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	result, err := s.ctl.Wait(ctx)
	if err != nil || result.NativeRC != 0 || result.PID != r.before.PID || result.ProcessStart != r.before.ProcessStart {
		t.Fatalf("original ctl completion changed: native_rc=%d err=%v", result.NativeRC, err)
	}
	body, err := os.ReadFile(r.target)
	transferMust(t, err)
	if !bytes.Equal(body, r.body) {
		t.Fatal("broker image cut changed original transfer bytes")
	}
	transferMust(t, s.collectors["broker"].drain())
}

// The test runner owns this optional evidence directory. Preserve native
// process outputs and sealed observations before temporary fixture cleanup.
// No keys, capsules, file contents or private image bytes enter this receipt.
// origin: S7 internal review INTEG-M F1 / hot-update S7 plan §0
func saveTransferObservations(s *transferStack) {
	s.t.Helper()
	root := os.Getenv("S7_TRANSFER_RECEIPTS")
	if root == "" {
		if s.t.Failed() {
			for _, process := range []*transferharness.Process{s.broker, s.agent, s.ctl} {
				if process != nil {
					s.t.Logf("native_rc=%d process stderr:\n%s", process.Receipt().NativeRC, s.output(process))
				}
			}
		}
		return
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		s.t.Error(err)
		return
	}
	// Every receipt is immutable, including repeated positive/mutant selectors.
	dir, err := os.MkdirTemp(root, transferDigest([]byte(s.t.Name()))+"-")
	if err != nil {
		s.t.Error(err)
		return
	}
	for role, c := range s.collectors {
		rows, observeErr := c.snapshot()
		body, err := json.Marshal(struct {
			Test  string
			Rows  []x.Observation
			Error string
		}{s.t.Name(), rows, fmt.Sprint(observeErr)})
		if err == nil {
			err = os.WriteFile(filepath.Join(dir, role+"-observations.json"), body, 0600)
		}
		if err != nil {
			s.t.Error(err)
		}
		if c.process == nil {
			continue
		}
		receipt := c.process.Receipt()
		body, err = json.Marshal(receipt)
		if err == nil {
			err = os.WriteFile(filepath.Join(dir, role+"-receipt.json"), body, 0600)
		}
		if err != nil {
			s.t.Error(err)
		}
		for name, path := range map[string]string{"stdout": receipt.StdoutPath, "stderr": receipt.StderrPath} {
			body, err = os.ReadFile(path)
			if err == nil {
				err = os.WriteFile(filepath.Join(dir, role+"-"+name+".log"), body, 0600)
			}
			if err != nil {
				s.t.Error(err)
			}
		}
	}
	s.t.Logf("raw_path=%s", dir)
}

func transferCustodyOwner(pids map[string]int, pid int) (string, error) {
	owner := ""
	for role, current := range pids {
		if pid <= 0 || current != pid {
			continue
		}
		if owner != "" {
			return "", fmt.Errorf("ambiguous transfer custody producer")
		}
		owner = role
	}
	if owner == "" {
		return "", fmt.Errorf("transfer observation has no original producer")
	}
	return owner, nil
}

// Both endpoint directions, tracker records and unknown/duplicate producers
// use the actual PID binding. Direction alone cannot choose an OFD keeper.
// origin: S7 internal review INTEG-M F3
func TestTransferObservationSelectsItsProducerCustody(t *testing.T) {
	pids := map[string]int{"agent": 11, "ctl": 22, "broker": 33}
	for role, pid := range pids {
		owner, err := transferCustodyOwner(pids, pid)
		if err != nil || owner != role {
			t.Fatalf("%s observation used another keeper: %s %v", role, owner, err)
		}
	}
	for _, pid := range []int{0, 44} {
		if _, err := transferCustodyOwner(pids, pid); err == nil {
			t.Fatal("absent producer accepted")
		}
	}
	pids["duplicate"] = 22
	if _, err := transferCustodyOwner(pids, 22); err == nil {
		t.Fatal("ambiguous producer accepted")
	}
}
