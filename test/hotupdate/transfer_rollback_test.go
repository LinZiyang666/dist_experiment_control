//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// Faults are inserted into the actual importer, after the real constructors
// have returned their owners. No fake Store/Control or pristine snapshot is
// handed to rollback. The same overlay reaches A and B and is normally inert.
// origin: hot-update S7 plan §8.2 / T37
func transferFaultOverlay(t *testing.T, root, dir string) string {
	t.Helper()
	replacements := make(map[string]string)
	flags := strings.Fields(os.Getenv("GOFLAGS"))
	var kept []string
	for i := 0; i < len(flags); i++ {
		flag := flags[i]
		if strings.HasPrefix(flag, "-overlay=") || flag == "-overlay" {
			name := strings.TrimPrefix(flag, "-overlay=")
			if flag == "-overlay" {
				i++
				if i == len(flags) {
					t.Fatal("GOFLAGS overlay has no value")
				}
				name = flags[i]
			}
			data, err := os.ReadFile(name)
			transferMust(t, err)
			var prior struct{ Replace map[string]string }
			transferMust(t, json.Unmarshal(data, &prior))
			for from, to := range prior.Replace {
				replacements[from] = to
			}
		} else {
			kept = append(kept, flag)
		}
	}
	path := filepath.Join(root, "internal/brokerimage/import.go")
	read := path
	if replacement, ok := replacements[path]; ok {
		read = replacement
	}
	data, err := os.ReadFile(read)
	transferMust(t, err)
	source := string(data)
	for _, edit := range []struct{ old, next string }{
		{"\"sync\"", "\"sync\"\n\"time\""},
		{"i.store, err = i.cfg.NewStore(i.bundle.Store, lock, deadline)", "i.store, err = i.cfg.NewStore(i.bundle.Store, lock, deadline)\n\tif transferBinaryFault(\"partial-store\") { err = ErrImage }"},
		{"i.control, err = i.cfg.NewControl(i.store, deadline)", "i.control, err = i.cfg.NewControl(i.store, deadline)\n\tif transferBinaryFault(\"partial-control\") { err = ErrImage }"},
		{"return image, tunnel.Salvage{}, nil", "if transferBinaryFault(\"pristine\") { salvage, err := image.detachLocked(); return nil, salvage, errors.Join(ErrImage, err) }; return image, tunnel.Salvage{}, nil"},
		{"if err = i.control.StartBusiness(deadline); err != nil {\n\t\treturn errors.Join(ErrActive, err)\n\t}", "if err = i.control.StartBusiness(deadline); err != nil {\n\t\treturn errors.Join(ErrActive, err)\n\t}\nif transferBinaryFault(\"active-progress\") { return ErrActive }"},
	} {
		if strings.Count(source, edit.old) != 1 {
			t.Fatalf("NOT_RUN: real importer fault anchor is missing or ambiguous: %s", edit.old)
		}
		source = strings.Replace(source, edit.old, edit.next, 1)
	}
	source += `
// Test overlay: one on-disk arm, atomically consumed by the actual importer.
func transferBinaryFault(point string) bool {
 root := os.Getenv("S7_TRANSFER_FAULT_ROOT")
 if root == "" { return false }
 path := root + "/fault-" + point
 if os.Rename(path, path + ".entered") != nil { return false }
 if point == "active-progress" {
  until := time.Now().Add(10*time.Second)
  for time.Now().Before(until) {
   if _, err := os.Stat(path + ".release"); err == nil { break }
   time.Sleep(5*time.Millisecond)
  }
 }
 return true
}
`
	patched := filepath.Join(dir, "broker-image-import.go")
	transferMust(t, os.WriteFile(patched, []byte(source), 0600))
	replacements[path] = patched
	path = filepath.Join(root, "internal/agentboot/image.go")
	read = path
	if replacement, ok := replacements[path]; ok {
		read = replacement
	}
	data, err = os.ReadFile(read)
	transferMust(t, err)
	source = string(data)
	for _, edit := range []struct{ old, next string }{
		{"if err := imp.PreActivate(); err != nil {\n\t\treturn 0, 0, 0, fmt.Errorf(\"agentboot: prepare activation: %w\", err)\n\t}", "if err := imp.PreActivate(); err != nil {\n\t\treturn 0, 0, 0, fmt.Errorf(\"agentboot: prepare activation: %w\", err)\n\t}\nif transferAgentBinaryFault(\"agent-pristine\") { return 0, 0, 0, errors.New(\"injected pristine import failure\") }"},
		{"if terr := imp.Thaw(); terr != nil {\n\t\treturn 0, 0, 0, fmt.Errorf(\"agentboot: thaw: %w\", terr)\n\t}", "if terr := imp.Thaw(); terr != nil {\n\t\treturn 0, 0, 0, fmt.Errorf(\"agentboot: thaw: %w\", terr)\n\t}\nif transferAgentBinaryFault(\"agent-active-progress\") { return 0, 0, 0, errors.New(\"injected active import failure\") }"},
	} {
		if strings.Count(source, edit.old) != 1 {
			t.Fatalf("actual agent importer fault anchor is missing or ambiguous: %s", edit.old)
		}
		source = strings.Replace(source, edit.old, edit.next, 1)
	}
	source += `
// Test overlay: responsibility is already real; only the later failure is injected.
func transferAgentBinaryFault(point string) bool {
 root := os.Getenv("S7_TRANSFER_FAULT_ROOT")
 if root == "" { return false }
 path := root + "/fault-" + point
 if os.Rename(path, path + ".entered") != nil { return false }
 if point == "agent-active-progress" {
  until := time.Now().Add(10*time.Second)
  for time.Now().Before(until) {
   if _, err := os.Stat(path + ".release"); err == nil { break }
   time.Sleep(5*time.Millisecond)
  }
 }
 return true
}
`
	patched = filepath.Join(dir, "agent-boot-image.go")
	transferMust(t, os.WriteFile(patched, []byte(source), 0600))
	replacements[path] = patched
	// Prevent the S8 target's actual durable completion, keeping its inherited
	// watchdog responsible. Publication and activation have already succeeded;
	// chmod affects the journal's directory only for this real write attempt.
	// origin: S8 internal review activation-and-route-migration F5
	transferPatchSource(t, root, dir, replacements, "internal/upgradestore/store.go", [][2]string{{
		"func (s *Store) Apply(expected upgradetx.Handle, event upgradetx.Event) (out upgradetx.Record, err error) {",
		`func (s *Store) Apply(expected upgradetx.Handle, event upgradetx.Event) (out upgradetx.Record, err error) {
 if root:=os.Getenv("S7_TRANSFER_FAULT_ROOT");root!="" && event.Phase==upgradetx.PhaseCompleted && expected.Key.Component==upgradetx.ComponentAgent {
  if view,loadErr:=s.Load(expected.Key,expected.TxID);loadErr==nil && view.Historical.Reverse.Handle.TxID=="" {
   if _,armed:=os.Stat(root+"/fault-watchdog");armed==nil {
    _ = os.WriteFile(root+"/fault-watchdog.entered",[]byte("entered"),0600)
    parent:=filepath.Dir(s.options.Path)
    if permissionErr:=os.Chmod(parent,0500);permissionErr!=nil {return out,permissionErr}
    defer os.Chmod(parent,0700)
   }
  }
 }
`}}, "")
	transferObservationOverlay(t, root, dir, replacements)
	transferCutBarrierOverlay(t, root, dir, replacements)
	data, err = json.Marshal(struct{ Replace map[string]string }{replacements})
	transferMust(t, err)
	manifest := filepath.Join(dir, "overlay.json")
	transferMust(t, os.WriteFile(manifest, data, 0600))
	return strings.Join(append(kept, "-overlay="+manifest), " ")
}

// Fault arms dispatch through the same target-issued transaction as normal
// cuts. Waiting for success would conceal the importer failure under test.
// origin: S8 internal review activation-and-route-migration F3
func (s *transferStack) armBrokerReload(sha string) {
	candidate := s.a.b
	if sha == s.a.shaA {
		candidate = s.a.a
	}
	pending := s.startBrokerTransaction(candidate, sha)
	s.awaitTransactionStart(pending)
}
func transferArmWatchdog(t *testing.T, s *transferStack) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Fatal("SETUP-RED2: real commit permission fault requires a non-root runner")
	}
	transferMust(t, os.WriteFile(filepath.Join(s.h.Root, "fault-watchdog"), []byte("armed\n"), 0600))
}

// origin: hot-update S7 plan §10.1 / T37
func TestTransferPartialActiveRollbackKeepsCurrentOwners(t *testing.T) {
	a := buildTransferArtifacts(t)
	requireTransferIntegration(t, a)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	// The blocking parent joins all five rounds before the direct leak checks.
	// In particular, five real 120s watchdog rounds cannot run serially inside
	// the lead's 600s per-test partition. Fixture concurrency is bounded below.
	// origin: hot-update S7 plan lead L17 / §11.3
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				exerciseTransferPartialRollback(t, a)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "transfer partial ACTIVE rollback", g)
	testharness.AssertNoFDLeak(t, "transfer partial ACTIVE rollback", fd, 0)
}
func exerciseTransferPartialRollback(t *testing.T, a transferArtifacts) {
	for _, fault := range []string{"pristine", "partial-store", "partial-control", "active-progress", "agent-pristine", "agent-active-progress"} {
		for _, tier := range []string{"a", "b"} {
			for _, direction := range []string{"push", "pull"} {
				t.Run(fault+"/tier-"+tier+"/"+direction, func(t *testing.T) {
					t.Parallel()
					s := newTransferStack(t, a)
					s.requireCapable()
					layer := "broker"
					process := s.broker
					if strings.HasPrefix(fault, "agent-") {
						layer, process = "agent", s.agent
					}
					cell := transferCell{tier, direction, layer, "synced-prefix"}
					runTransferCell(t, s, cell, func(release func()) {
						arm := filepath.Join(s.h.Root, "fault-"+fault)
						transferMust(t, os.WriteFile(arm, []byte("armed\n"), 0600))
						before, err := process.SampleIdentity()
						transferMust(t, err)
						if layer == "agent" {
							if _, err = s.startAgentTransaction(a.b, s.artifact.URL+"/candidate.tar.gz", a.archiveB); err != nil {
								t.Fatal(err)
							}
						} else {
							s.armBrokerReload(a.shaB)
						}
						if !testharness.WaitFor(t, 20*time.Second, 10*time.Millisecond, func() bool {
							_, err := os.Stat(arm + ".entered")
							return err == nil
						}) {
							t.Fatal("real importer did not reach its armed fault")
						}
						var advanced x.Observation
						if strings.HasSuffix(fault, "active-progress") {
							// The cut was taken at the parked boundary; ACTIVE progress
							// needs the original receiver to run again.
							// origin: S7 FIX-CUT live-cut barrier / hot-update S7 plan §8.2 / T37
							release()
							// The overlay delays only the injected failure. Business owners
							// are already thawed and continue at the real product rate.
							current, err := process.SampleIdentity()
							transferMust(t, err)
							transferMust(t, validateTransferProcess(before, current, a.shaB))
							advanced = awaitTransferActiveAdvance(t, s.collectors[layer], a.shaB)
							transferMust(t, os.WriteFile(arm+".release", []byte("release\n"), 0600))
						}
						if !testharness.WaitFor(t, 20*time.Second, 20*time.Millisecond, func() bool {
							after, err := process.SampleIdentity()
							return err == nil && after.RunningSHA == before.RunningSHA
						}) {
							t.Fatal("actual importer failure did not return to the original artifact")
						}
						after, err := process.SampleIdentity()
						transferMust(t, err)
						transferMust(t, validateTransferProcess(before, after, a.shaA))
						rows, err := s.collectors[layer].snapshot()
						transferMust(t, err)
						transferMust(t, validateTransferRollback(rows, !strings.HasSuffix(fault, "pristine")))
						if advanced.TransferID != "" {
							transferMust(t, validateTransferRollbackAdvance(advanced, rows))
						}
					})
				})
			}
		}
	}
}

// A nonempty old checkpoint cannot certify R2. First observe a real imported
// owner, then new durable progress in that same incarnation before releasing
// the importer error. Returning to the old bundle must lose this evidence.
// origin: hot-update S7 plan §8.2 / T37
func awaitTransferActiveAdvance(t *testing.T, c *transferCollector, readerSHA string) x.Observation {
	t.Helper()
	var advanced x.Observation
	if !testharness.WaitFor(t, 8*time.Second, 5*time.Millisecond, func() bool {
		rows, err := c.snapshot()
		transferMust(t, err)
		for _, installed := range rows {
			if installed.Kind != x.ObserveInstall || !installed.Active || installed.ReaderSHA != readerSHA {
				continue
			}
			for _, row := range rows {
				if row.TransferID == installed.TransferID && row.Generation == installed.Generation && row.EventSeq > installed.EventSeq &&
					(row.DurableOffset > installed.DurableOffset || row.PrimaryRevision > installed.PrimaryRevision || row.OutboxRevision > installed.OutboxRevision) {
					advanced = row
					return true
				}
			}
		}
		return false
	}) {
		t.Fatal("INCOMPLETE: ACTIVE candidate made no new durable progress before rollback")
	}
	return advanced
}
func validateTransferRollbackAdvance(advanced x.Observation, rows []x.Observation) error {
	for _, restored := range rows {
		if restored.Kind != x.ObserveRollback || restored.TransferID != advanced.TransferID || restored.BoottimeNS <= advanced.BoottimeNS {
			continue
		}
		if restored.Generation != advanced.Generation+1 || restored.EntryNonce != advanced.EntryNonce || restored.ObjectVersion != advanced.ObjectVersion ||
			restored.DurableOffset < advanced.DurableOffset || restored.LastConsumedStreamSeq < advanced.LastConsumedStreamSeq || restored.PrimaryRevision < advanced.PrimaryRevision ||
			restored.OutboxRevision < advanced.OutboxRevision || restored.OriginalDeadline != advanced.OriginalDeadline ||
			(advanced.CanonicalDigest != "" && restored.CanonicalDigest != advanced.CanonicalDigest) {
			return errors.New("rollback discarded progress made by the ACTIVE candidate")
		}
		return nil
	}
	return errors.New("no fresh rollback observation after ACTIVE candidate progress")
}

// The two receivers have separate launch cells. The product 120-second timer
// is not accelerated, and its original transfer budget is checked again after
// generation+1 rollback, including pending courier and outbox responsibility.
// origin: hot-update S7 plan §10.1 / T38
func TestTransferWatchdogRollbackKeepsOriginalDeadline(t *testing.T) {
	a := buildTransferArtifacts(t)
	requireTransferIntegration(t, a)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	// The blocking parent joins all five rounds before the direct leak checks.
	// In particular, five real 120s watchdog rounds cannot run serially inside
	// the lead's 600s per-test partition. Fixture concurrency is bounded below.
	// origin: hot-update S7 plan lead L17 / §11.3
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				exerciseTransferWatchdog(t, a)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "transfer watchdog original budget", g)
	testharness.AssertNoFDLeak(t, "transfer watchdog original budget", fd, 0)
}
func exerciseTransferWatchdog(t *testing.T, a transferArtifacts) {
	for _, direction := range []string{"push", "pull"} {
		t.Run(direction, func(t *testing.T) {
			t.Parallel()
			s := newTransferStack(t, a)
			s.requireCapable()
			cell := transferCell{"b", direction, "agent", "terminal-pre-history-ACK"}
			runTransferCell(t, s, cell, func(release func()) {
				before, err := s.agent.SampleIdentity()
				transferMust(t, err)
				start := time.Now()
				// This injection needs the actual boot/register barrier so chmod
				// cannot prevent download/PREPARE instead of the commit write.
				transferArmWatchdog(t, s)
				tx := s.armAgentCut()
				if !testharness.WaitFor(t, 20*time.Second, 10*time.Millisecond, func() bool { _, err := os.Stat(filepath.Join(s.h.Root, "fault-watchdog.entered")); return err == nil }) {
					t.Fatal("S8 target never attempted its armed durable completion")
				}
				// The candidate exec happened at the parked boundary. The real
				// watchdog window that follows must not freeze the broker owner.
				// origin: S7 FIX-CUT live-cut barrier / hot-update S7 plan §10.1 / T38
				release()
				if !testharness.WaitFor(t, upgradebudget.ComponentHandover+30*time.Second, 20*time.Millisecond, func() bool {
					after, err := s.agent.SampleIdentity()
					return err == nil && after.RunningSHA == before.RunningSHA
				}) {
					t.Fatal("original agent watchdog did not return to A")
				}
				if time.Since(start) < upgradebudget.ComponentHandover {
					t.Fatal("watchdog was accelerated below the real 120-second window")
				}
				s.waitAgentRollback(tx, before.RunningSHA)
				rows, err := s.collectors["agent"].snapshot()
				transferMust(t, err)
				transferMust(t, validateTransferRollback(rows, true))
			})
		})
	}
}
func validateTransferRollback(rows []x.Observation, active bool) error {
	var entered, restored *x.Observation
	for i := range rows {
		r := &rows[i]
		if r.Kind == x.ObserveRollback {
			restored = r
		}
	}
	if restored == nil || restored.TransferID == "" || restored.CutID == "" {
		return errors.New("rollback has no original transfer responsibility")
	}
	if !active {
		if restored.Active {
			return errors.New("pristine rollback changed owner activation")
		}
		return nil
	}
	for i := range rows {
		r := &rows[i]
		if r.Kind == x.ObserveInstall && r.Active && r.TransferID == restored.TransferID && r.PID == restored.PID && r.Generation+1 == restored.Generation {
			entered = r
		}
	}
	if entered == nil || restored.Generation != entered.Generation+1 || restored.EntryNonce != entered.EntryNonce || restored.ObjectVersion != entered.ObjectVersion ||
		restored.DurableOffset < entered.DurableOffset || restored.LastConsumedStreamSeq < entered.LastConsumedStreamSeq || restored.OriginalDeadline != entered.OriginalDeadline ||
		restored.PrimaryRevision < entered.PrimaryRevision || restored.OutboxRevision < entered.OutboxRevision || restored.CanonicalDigest != entered.CanonicalDigest {
		return errors.New("ACTIVE rollback restored stale checkpoint, deadline, canonical outcome or outbox")
	}
	if restored.PID != entered.PID || restored.ProcessStart != entered.ProcessStart || restored.BootID != entered.BootID {
		return errors.New("rollback replaced original process")
	}
	return nil
}
