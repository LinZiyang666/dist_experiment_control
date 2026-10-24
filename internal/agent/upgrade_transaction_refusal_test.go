//go:build linux

// upgrade_transaction_refusal_test.go — granted Starts on the production S8
// RuntimePort that must not reach exec: precise refusals before acceptance
// (retained domains, epochs, a foreign table freeze) and failures of the
// final re-export after the activation intent, which keep Current.
// origin: hot-update S8 plan §4.1 / §6.1 / §7.1 / §8.1; S8 FIX-AGENTWIRE
package agent

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/nats-io/nats.go"
)

// lateRevocationRuntime replaces the runtime with one whose clock can fail on
// demand; the returned injection records a PTY-class (clock-unknown)
// revocation, retained state only a pty-v1 reader can carry.
func lateRevocationRuntime(t *testing.T, a *Agent) func() {
	t.Helper()
	stopProcessRuntime(t.Context(), a)
	cfg := processRuntimeConfig(a)
	cfg.Instance = "late-" + a.instanceID
	var broken atomic.Bool
	cfg.Clock = func() (int64, error) {
		if broken.Load() {
			return 0, syscall.EIO
		}
		return procrequest.BootNow()
	}
	rt, err := execsession.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	a.processRuntime = rt
	return func() {
		broken.Store(true)
		_, err := a.processRuntime.Revoke(a.cfg.SID, "late authority loss")
		broken.Store(false)
		if err == nil {
			t.Error("late clock failure was not observed")
		}
	}
}

// exerciseGrantedPostSelectFailure fails the final re-export after the select
// and activation intents are durable: a late PTY-class revocation the candidate
// cannot read, or target bytes replaced after the readers were bound. Nothing
// reaches exec; the source keeps running with its selector, pins and Current.
// origin: hot-update S8 plan §4.1 / §5.2 / §6.1 / §8.1; S8 FIX-AGENTWIRE
func exerciseGrantedPostSelectFailure(t *testing.T, kind string) {
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	a := cutTestAgent(t)
	inject := lateRevocationRuntime(t, a)
	nc := cutNATS(t)
	r := openGrantedRoute(t, a, false)
	dst := a.cfg.UpgradeExecutablePath
	before := mustSHA(t, dst)
	// The candidate reads every current domain but PTY.
	candidate := s8Script("post-select "+kind+" "+a.instanceID, "exec-v1 flow-v1 ss-v1 broker-image-v1 transfer-v1")
	grant, receipt := r.publish(t, a, nc, candidate)
	execs, observed := 0, 0
	a.cfg.UpgradeExecFn = func(string) error { execs++; return syscall.ENOEXEC }
	a.cfg.UpgradeCutObserver = func(agentimage.Bundle) {
		observed++
		switch kind {
		case "abi_late":
			inject()
		case "target_replaced":
			if err := os.Chmod(receipt.TargetPath, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(receipt.TargetPath, s8Script("replaced "+a.instanceID, s8Domains), 0o755); err != nil {
				t.Fatal(err)
			}
		}
	}
	resp := r.start(t, a, nc, grant, receipt)
	if observed != 1 || execs != 0 || resp.Code != upgradetx.CodeUnsettled {
		t.Fatalf("post-select %s failure: observed=%d execs=%d code=%s error=%s", kind, observed, execs, resp.Code, resp.Error)
	}
	r.requireRecovery(t, grant, receipt)
	cutRequireOpen(t, a)
	if mustSHA(t, dst) != before {
		t.Error("a failed S8 cut changed the canonical executable")
	}
	if kind == "abi_late" {
		if err := checkCandidateABI(a, agentimage.Capabilities{ABI: 1, Domains: []string{"exec-v1"}}); !errors.Is(err, errImageABIUnsupported) {
			t.Error("the late retained revocation was erased to make the cut readable")
		}
	}
}

// origin: hot-update S8 plan §3.2 / §8.2 F3 F4; S8 FIX-AGENTWIRE. A journal
// that becomes unwritable right after Freeze (EROFS/ENOSPC/EACCES) leaves the
// engine unable to record the cut or its own failure, so it never calls Thaw.
// The adapter must not hold business frozen behind that unknown record: the
// unactivated cut thaws itself within its budget, releases the reservation,
// and nothing reaches exec.
func TestAbandonedTransactionCutThawsWithinBudget(t *testing.T) {
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	old := transactionCutBudget
	transactionCutBudget = 300 * time.Millisecond
	t.Cleanup(func() { transactionCutBudget = old })
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exerciseAbandonedTransactionCut(t) })
	}
	testharness.AssertNoFDLeak(t, "abandoned transaction cut x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "abandoned transaction cut x5", beforeG)
}

func exerciseAbandonedTransactionCut(t *testing.T) {
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	a := cutTestAgent(t)
	task := cutStartTask(t, a)
	nc := cutNATS(t)
	r := openGrantedRoute(t, a, false)
	grant, receipt := r.publish(t, a, nc, s8Script("abandoned cut "+a.instanceID, s8Domains))
	execs := 0
	a.cfg.UpgradeExecFn = func(string) error { execs++; return syscall.ENOEXEC }
	t.Cleanup(func() { _ = os.Chmod(r.control, 0o700) })
	late := r.interpose(a, nil)
	late.afterFreeze = func() {
		if err := os.Chmod(r.control, 0o500); err != nil {
			t.Fatal(err)
		}
	}
	resp := r.start(t, a, nc, grant, receipt)
	if resp.Code != upgradetx.CodeUnsettled || execs != 0 {
		t.Fatalf("unjournaled cut: code=%s error=%q execs=%d", resp.Code, resp.Error, execs)
	}
	if !a.processRuntime.Frozen() {
		t.Fatal("the fixture did not leave the engine's cut frozen")
	}
	// The abandon owns the cut latch until thaw, resume and gate-open finish.
	cutWait(t, "abandoned cut thaws within its budget", func() bool {
		if !a.cutMu.TryLock() {
			return false
		}
		a.cutMu.Unlock()
		return !a.processRuntime.Frozen() && !a.workloads.Frozen()
	})
	cutRequireOpen(t, a)
	r.requireDomainHeld(t, false)
	if err := os.Chmod(r.control, 0o700); err != nil {
		t.Fatal(err)
	}
	cutFinishTask(t, a, task)
	if execs != 0 {
		t.Fatal("an abandoned cut reached exec")
	}
}

// grantedCandidate offers the S3 table's candidate as an S8 artifact. An ABI 0
// bare-exec reader is never an S8 target, even for an empty image: the cut
// always carries the required adjunct. "future" declares a second epoch
// without reading this one; pty_without_domain reads every domain but PTY; a
// foreign table freeze must defer an otherwise compatible S8 candidate.
func grantedCandidate(kind string, abi int, tag string) []byte {
	switch {
	case abi == 0 && kind != "foreign_freeze" && kind != "ss_metadata" && kind != "no_incarnation":
		return []byte(cutCandidateScript(0) + "# " + tag + "\n")
	case kind == "future":
		return []byte("#!/bin/sh\n# " + tag + "\necho '" + proto.VersionLine() + "'\necho 'image-domains " + s8Domains +
			" upgrade-cut-v1'\necho 'image-features upgrade-tx-v1 authority-incarnation-v1 upgrade-cut-v1'\necho 'image-abi v" +
			strconv.Itoa(abi) + "'\n")
	case kind == "pty_without_domain":
		return s8Script(tag, "exec-v1 flow-v1 ss-v1 broker-image-v1 transfer-v1")
	}
	return s8Script(tag, s8Domains)
}

// exerciseGrantedCandidateDomains runs one retained-domain row through a
// granted Start. Every refusal is precise and precedes acceptance; a
// compatible candidate reaches exec with its retained task in the adjunct.
// origin: hot-update S8 plan §6.1 / §7.1; S8 FIX-FIXTURES R2; S8 FIX-AGENTWIRE
func exerciseGrantedCandidateDomains(t *testing.T, nc *nats.Conn, kind string) {
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	a := cutTestAgent(t)
	abi, task := retainCandidateDomain(t, a, kind)
	if kind == "ss_metadata" {
		// Representable SS ordering metadata passes the busy checks, but the
		// census must retain its original identity across the accepted cut.
		a.proxy.mu.Lock()
		a.proxy.gen, a.proxy.epoch = 3, 2
		a.proxy.mu.Unlock()
	}
	r := openGrantedRoute(t, a, false)
	if kind == "no_incarnation" {
		// An agent that never registered has no creation identity to carry.
		unknown := ""
		a.incarnation.Store(&unknown)
	}
	dst := a.cfg.UpgradeExecutablePath
	old := mustSHA(t, dst)
	grant, receipt := r.publish(t, a, nc, grantedCandidate(kind, abi, kind+" "+a.instanceID))
	execs, carried, proxyCarried := 0, false, false
	a.cfg.UpgradeExecFn = func(string) error {
		execs++
		state, _ := preparedAdjunct(t)
		for _, row := range state.Scope.Expected {
			carried = carried || task.pid != "" && row.ID == "task:"+task.pid
			// origin: S8 internal review TAGGED-SUITE-SWEEP-A F2; hot-update S10 plan §12.2 / TIDY-1.
			// Retained public metadata needs its real source owner, rather than
			// acceptance with an unnamed or replacement SS responsibility.
			id := "proxy:" + r.key.SID + "/" + r.key.ConfiguredNID
			proxyCarried = proxyCarried || row.Domain == upgradetx.DomainSS && row.ID == id && row.Origin == id &&
				row.Owner == grant.Issuer && row.Generation == grant.Handle.Generation-1 && row.State == "retained" && row.Custody == "source"
		}
		return syscall.ENOEXEC
	}
	resp := r.start(t, a, nc, grant, receipt)
	want := map[string][2]string{
		"task": {upgradetx.CodeRejected, agentupgradeport.ReasonImageABIUnsupported}, "revocation": {upgradetx.CodeRejected, agentupgradeport.ReasonImageABIUnsupported},
		"courier": {upgradetx.CodeRejected, agentupgradeport.ReasonImageABIUnsupported}, "tombstone": {upgradetx.CodeRejected, agentupgradeport.ReasonImageABIUnsupported},
		"empty": {upgradetx.CodeRejected, agentupgradeport.ReasonImageABIUnsupported}, "future": {upgradetx.CodeRejected, "image_epoch_unreadable"},
		"pty_without_domain": {upgradetx.CodeRejected, agentupgradeport.ReasonImageABIUnsupported},
		"foreign_freeze":     {upgradetx.CodeDeferred, agentupgradeport.ReasonFrozenTable},
		"no_incarnation":     {upgradetx.CodeDeferred, agentupgradeport.ReasonIncarnationUnknown},
	}
	if expected, refused := want[kind]; refused {
		if execs != 0 || resp.Code != expected[0] || resp.Error != expected[1] {
			t.Fatalf("%s: code=%s error=%q execs=%d, want %s %q", kind, resp.Code, resp.Error, execs, expected[0], expected[1])
		}
		r.requireUnaccepted(t, grant)
	} else {
		if execs != 1 || resp.Code != upgradetx.CodeUnsettled {
			t.Fatalf("compatible candidate refused: code=%s error=%q execs=%d", resp.Code, resp.Error, execs)
		}
		r.requireRecovery(t, grant, receipt)
		if kind == "ss_metadata" && !proxyCarried {
			t.Error("the final cut omitted the retained SS metadata source identity")
		}
		if task.pid != "" {
			if !carried {
				t.Error("the final cut omitted the retained task from the adjunct scope")
			}
			cutFinishTask(t, a, task)
		}
	}
	if a.processRuntime.Frozen() {
		t.Fatal("granted validation stranded the runtime frozen")
	}
	if mustSHA(t, dst) != old {
		t.Fatal("granted validation modified the canonical executable")
	}
	for _, path := range []string{upgradePrevPath(dst), upgradeMarkerPath(dst)} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("granted validation wrote %s", filepath.Base(path))
		}
	}
}
