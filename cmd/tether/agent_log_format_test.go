package main

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S3 plan §3.2 and §8.2; DR-1. Python consumes these same
// golden records. Neither side can silently substitute an unwrapped message.
func TestAgentLogFormatMatchesActivationRecords(t *testing.T) {
	t.Run("pty-records", testPTYLogFormats)
	const format = "agent: image activated generation=%d tasks=%d freeze_window_ms=%d shim_ms=%d import_ms=%d"
	// Copied from activateImage in internal/agent/bootstrap.go. Pin the emitter
	// too: a layout change must update the cross-language contract deliberately.
	source, err := os.ReadFile("../../internal/agent/bootstrap.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "a.cfg.Logger.Info(fmt.Sprintf("+strconv.Quote(format)+",") {
		t.Fatal("activation emitter no longer matches the log contract")
	}
	golden, err := os.ReadFile("../../test/simcluster/tests/testdata/agent-activation-records.txt")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(golden), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want text and JSON records, got %d lines", len(lines))
	}
	for i, jsonOut := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", jsonOut), func(t *testing.T) {
			var output bytes.Buffer
			logger, err := newLoggerTo("info", jsonOut, &output)
			if err != nil {
				t.Fatal(err)
			}
			// Only the clock is fixed; the daemon's real handler renders it.
			record := slog.NewRecord(time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC), slog.LevelInfo,
				fmt.Sprintf(format, 1, 1, 12, 2, 4), 0)
			if err := logger.Handler().Handle(t.Context(), record); err != nil {
				t.Fatal(err)
			}
			if output.String() != lines[i]+"\n" {
				t.Fatalf("activation record changed:\n got %s\nwant %s", output.String(), lines[i])
			}
		})
	}
}

// origin: hot-update S4 plan §4.2 / §9.1; P18. Pin both complete layouts and
// the final Prepare/activation call sites that select their authoritative state.
// hot-update S5 (plan §5.1 / T33) routes both final Prepares through
// prepareImageCut, which adds the SS cut and then calls PrepareChecked; the
// "cut" record must still follow that final re-export in each caller.
func testPTYLogFormats(t *testing.T) {
	t.Helper()
	for _, file := range []string{"upgrade.go", "upgrade_image.go"} {
		source, err := os.ReadFile("../../internal/agent/" + file)
		if err != nil {
			t.Fatal(err)
		}
		cut := strings.LastIndex(string(source), `execsession.LogPTYImage(a.cfg.Logger, "cut", cut.generation, cut.frozen.Bundle().Runtime)`)
		prepare := strings.LastIndex(string(source), "prepareImageCut(a, cut, ")
		if prepare < 0 || cut <= prepare {
			t.Fatalf("%s must log the final Prepare re-export", file)
		}
	}
	source, err := os.ReadFile("../../internal/agent/upgrade_image.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(source)
	start := strings.Index(body, "\nfunc prepareImageCut(")
	if start < 0 {
		t.Fatal("prepareImageCut no longer exists; re-pin the final Prepare re-export")
	}
	body = body[start+1:]
	if end := strings.Index(body, "\nfunc "); end >= 0 {
		body = body[:end]
	}
	if !strings.Contains(body, "\treturn cut.frozen.PrepareChecked(") {
		t.Fatal("prepareImageCut must end in the final PrepareChecked re-export")
	}
	source, err = os.ReadFile("../../internal/agent/bootstrap.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "a.processRuntime.LogPTYActivation(a.imageGeneration)") {
		t.Fatal("activation must observe the imported runtime owner")
	}
	source, err = os.ReadFile("../../internal/agent/exec.go")
	if err != nil {
		t.Fatal(err)
	}
	// gofmt realigns a composite literal whenever a field is added (S5 added
	// SharedReplayBudget), so the field pins ignore the alignment padding.
	if !regexp.MustCompile(`\bPTYStarted:\s+func\(string, proto\.RunReq, workload\.Identity\) \{\},`).Match(source) {
		t.Fatal("PTY start audit must come from the payload-free runtime record")
	}
	if !regexp.MustCompile(`\bLogger:\s+a\.cfg\.Logger,`).Match(source) {
		t.Fatal("runtime receipts must use the agent logger")
	}
	source, err = os.ReadFile("../../internal/agent/agent.go")
	if err != nil {
		t.Fatal(err)
	}
	session := string(source)
	lost := `a.processRuntime.LogPTYCarrier("carrier-lost", a.imageGeneration)`
	restored := `a.processRuntime.LogPTYCarrier("carrier-restored", a.imageGeneration)`
	lossAt, restoredAt := strings.Index(session, lost), strings.Index(session, restored)
	if strings.Count(session, lost) != 1 || lossAt <= strings.Index(session, "fin.Do(intent)") || lossAt >= strings.Index(session, "a.setSessionFinalizer(nil)") {
		t.Fatal("carrier loss must be observed once after session teardown")
	}
	if strings.Count(session, restored) != 1 || restoredAt <= strings.Index(session, "if applyLeaseVerdict(a, resp.Lease)") || restoredAt >= strings.Index(session, `a.cfg.Logger.Info("agent: registered",`) {
		t.Fatal("carrier restoration must follow successful session registration/lease acceptance")
	}

	st := ptysession.Image{Input: workload.ReplayState{Base: 3, End: 7}, InputFIN: true, InputEnded: true,
		Output: workload.ReplayState{Base: 11, End: 19, FIN: true}, ControlSeq: 5, LastKASeq: 4,
		KADeadlineBootNS: 181000000000, Revoked: "private-revocation-reason", MasterClosed: true,
		Pending: []byte("private-payload")}
	records := []ptysession.Record{
		ptysession.ImageRecord("cut", 7, "task-1", st),
		ptysession.ImageRecord("activated", 8, "task-1", st),
		ptysession.ImageRecord("carrier-lost", 8, "task-1", st),
		ptysession.ImageRecord("carrier-restored", 8, "task-1", st),
		ptysession.ControlRecord("task-1", "resize", ptysession.ControlReceipt{Seq: 5}),
		ptysession.ControlRecord("task-1", "signal", ptysession.ControlReceipt{Seq: 6, Error: "pty control failed: signal_error"}),
		ptysession.ControlRecord("task-1", "hangup", ptysession.ControlReceipt{Seq: 7, Error: "private-error-text"}),
		ptysession.StartedRecord("task-1", "private-request-id", 1234, 5678),
		ptysession.ExitedRecord("task-1", 129, 1, st),
		ptysession.SettledRecord("task-1", 129, 1, 3, 7, 19, 19),
	}
	for _, jsonOut := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", jsonOut), func(t *testing.T) {
			var output bytes.Buffer
			logger, err := newLoggerTo("info", jsonOut, &output)
			if err != nil {
				t.Fatal(err)
			}
			for _, receipt := range records {
				r := slog.NewRecord(time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC), slog.LevelInfo, receipt.Message, 0)
				r.Add(receipt.Fields...)
				if err := logger.Handler().Handle(t.Context(), r); err != nil {
					t.Fatal(err)
				}
			}
			format := "text"
			if jsonOut {
				format = "json"
			}
			want, err := os.ReadFile("testdata/agent-pty-records." + format)
			if err != nil {
				t.Fatal(err)
			}
			if output.String() != string(want) {
				t.Fatalf("PTY %s records changed:\n got %s\nwant %s", format, output.String(), want)
			}
		})
	}
}
