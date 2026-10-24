//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S5 plan §3.3 / §8 / T41
// origin: hot-update S5 internal review HOTUPDATE-T41
func TestHalfCloseTailAcrossRealExec(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	a := flowBuildArtifacts(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseHalfCloseRealExec(t, a)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "original half-close tail across product exec", beforeG)
	testharness.AssertNoFDLeak(t, "original half-close tail across product exec", beforeFD, 0)
}

func exerciseHalfCloseRealExec(t *testing.T, a flowArtifacts) {
	s := flowStartProduct(t, a)
	target := flowListenTarget(t)
	port := flowProductExpose(t, s, target)
	beforeReplay := flowCountLinks(flowProductFDs(t, s.agent.Process.Pid), "memfd:tether-replay")
	p := flowConnectPair(t, target, port)
	agentIdentity := flowProductSocketIdentity(t, s.agent.Process.Pid, p.target.RemoteAddr(), p.target.LocalAddr())
	brokerIdentity := flowProductSocketIdentity(t, os.Getpid(), p.public.RemoteAddr(), p.public.LocalAddr())
	flowExchangeRecord(t, p, 0, 0)
	if err := p.public.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	_ = p.target.SetReadDeadline(time.Now().Add(flowBusinessDeadline))
	n, err := p.target.Read(make([]byte, 1))
	if n != 0 || err != io.EOF {
		t.Fatalf("original request FIN did not reach target before cut: n=%d err=%v", n, err)
	}
	// A small prefix proves that read EOF has not already closed the response
	// direction. The declared final tail exceeds replay capacity after exec.
	flowExchangeRecord(t, p, 1, 0)
	flowUpgradeProduct(t, s, a)
	flowCheckOriginalSocket(t, s.agent.Process.Pid, p.target.RemoteAddr(), p.target.LocalAddr(), agentIdentity)
	flowCheckOriginalSocket(t, os.Getpid(), p.public.RemoteAddr(), p.public.LocalAddr(), brokerIdentity)
	const tailRecords = 640 // 10MiB, strictly larger than the 8MiB send replay.
	if int64(tailRecords*flowRecordBytes) <= workload.DefaultReplayCapacity {
		t.Fatal("half-close tail does not exceed send replay capacity")
	}
	done := make(chan error, 1)
	go func() {
		var err error
		for seq := uint64(1); seq <= tailRecords; seq++ {
			if err = flowWriteBytes(p.target, flowOracleRecord(p.nonce, 1, seq)); err != nil {
				break
			}
		}
		if err == nil {
			err = p.target.CloseWrite()
		}
		done <- err
	}()
	var progress atomic.Uint64
	receipt, readErr := flowReadLedgerFrom(p.public, p.nonce, 1, 1, tailRecords, &progress)
	writeErr := <-done
	if readErr != nil || writeErr != nil {
		t.Fatalf("original half-close response tail: read=%v write=%v", readErr, writeErr)
	}
	if receipt.Records != tailRecords || receipt.Bytes != tailRecords*flowRecordBytes || !receipt.EOF || progress.Load() != tailRecords {
		t.Fatalf("half-close tail final receipt incomplete: %+v", receipt)
	}
	t.Logf("half-close original tail records=%d bytes=%d sha256=%x max_gap=%s exact_eof=%t", receipt.Records, receipt.Bytes, receipt.Digest, receipt.MaxGap, receipt.EOF)
	if target.count.Load() != 1 {
		t.Fatalf("half-close target redialed: accepts=%d want=1", target.count.Load())
	}
	flowRequireProductRetired(t, s, []flowSocketIdentity{agentIdentity}, []flowSocketIdentity{brokerIdentity}, beforeReplay)
}

// These controls run without unfinished S5 owners. They test the external
// identity/byte judge, not a substitute flow implementation. The real tests
// above still fail at the missing production dependency, never skip to legacy.
// origin: hot-update S5 plan §8 / T39–T41 oracle non-vacuity
func TestFlowContinuityOracleRejectsSocketAndTailFaults(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseFlowContinuityOracle(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "independent TCP continuity oracle controls", beforeG)
	testharness.AssertNoFDLeak(t, "independent TCP continuity oracle controls", beforeFD, 0)
}

func exerciseFlowContinuityOracle(t *testing.T) {
	if err := flowValidateRunningSHA("old", "new", "new"); err != nil {
		t.Fatal(err)
	}
	if err := flowValidateRunningSHA("old", "old", "new"); err == nil {
		t.Fatal("disk-only replacement passed the running SHA oracle")
	}
	if err := flowValidateRunningSHA("old", "third", "new"); err == nil {
		t.Fatal("unrelated running image passed the candidate SHA oracle")
	}
	identity := flowSocketIdentity{Dev: 1, Ino: 101, Type: 0140000}
	if err := flowValidateSocketIdentity(identity, identity); err != nil {
		t.Fatal(err)
	}
	redial := identity
	redial.Ino++
	if err := flowValidateSocketIdentity(identity, redial); err == nil {
		t.Fatal("application redial passed the original OFD oracle")
	}
	for _, fault := range []string{"none", "early-close", "drop-tail", "duplicate", "reorder", "wrong-nonce", "trailing-byte", "trailing-silence"} {
		t.Run(fault, func(t *testing.T) { exerciseFlowLedgerFault(t, fault) })
	}
}

func exerciseFlowLedgerFault(t *testing.T, fault string) {
	target := flowListenTarget(t)
	p := flowConnectPair(t, target, flowTCPPort(target.listener.Addr()))
	const records = 3
	written := make(chan error, 1)
	closeAfterRead := make(chan struct{})
	go func() {
		var err error
		for seq := uint64(0); seq < records; seq++ {
			if (fault == "early-close" && seq == 0) || (fault == "drop-tail" && seq == records-1) {
				break
			}
			nonce, actualSeq := p.nonce, seq
			if fault == "wrong-nonce" {
				nonce[0] ^= 1
			}
			if fault == "duplicate" && seq == 1 {
				actualSeq = 0
			}
			if fault == "reorder" {
				actualSeq = records - 1 - seq
			}
			if err = flowWriteBytes(p.target, flowOracleRecord(nonce, 1, actualSeq)); err != nil {
				break
			}
		}
		if err == nil && fault == "trailing-byte" {
			err = flowWriteBytes(p.target, []byte{1})
		}
		if fault == "trailing-silence" {
			<-closeAfterRead
		}
		if err == nil {
			err = p.target.CloseWrite()
		}
		written <- err
	}()
	receipt, err := flowReadLedger(p.public, p.nonce, 1, records, nil)
	close(closeAfterRead)
	writeErr := <-written
	if writeErr != nil {
		t.Fatalf("external fault fixture failed to write its bytes: %v", writeErr)
	}
	if fault == "none" {
		if err != nil || receipt.Records != records || !receipt.EOF {
			t.Fatalf("healthy independent ledger rejected: %+v %v", receipt, err)
		}
	} else if err == nil {
		t.Fatalf("external continuity oracle accepted %s: %+v", fault, receipt)
	}
}
