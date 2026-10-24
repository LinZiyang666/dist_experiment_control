//go:build linux

// Real: canonical intent, signed leases, attachment fences and registry images.
// Stubbed: broker ACL checks, terminal allocation and stream ownership, which
// belong to the caller. No terminal side effect is granted by a rejected call.
// origin: hot-update S4 plan §8.1 / T17
package procrequest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

func ptyRequestSpec() proto.RunReq {
	return proto.RunReq{Argv: []string{"/bin/sh", "-c", "read -r line; exit 23"},
		Env: map[string]string{"ONE": "1", "TWO": "2"}, Cwd: "/tmp",
		Cols: 80, Rows: 24, Safe: true, KAIntervalMS: 1000}
}

// origin: hot-update S4 plan §8.1 / T17
func TestPTYIntentNormalization(t *testing.T) {
	// Literal canonical bytes are an independent oracle for the domain prefix,
	// field set/order and map order. Neither expected value uses ptyIntentHash.
	const canonical = `{"argv":["/bin/sh","-c","read -r line; exit 23"],"env":{"ONE":"1","TWO":"2"},"cwd":"/tmp","cols":80,"rows":24,"safe":true,"ka_interval_ms":1000}`
	want := sha256.Sum256([]byte("tether-pty-intent-v1\n" + canonical))
	got, err := ptyIntentHash(ptyRequestSpec())
	if err != nil || got != hex.EncodeToString(want[:]) {
		t.Fatalf("PTY canonical digest changed: got=%s want=%x err=%v", got, want, err)
	}
	for _, tc := range []struct {
		name       string
		cols, rows int
		ka         int
		wantCols   int
		wantRows   int
		wantKA     int
	}{
		{"defaults", 0, 0, 0, 80, 24, 0},
		{"missing-cols", 0, 90, 1, 80, 24, 1000},
		{"missing-rows", 120, 0, 999, 80, 24, 1000},
		{"negative-cols", -1, 90, 1000, 80, 24, 1000},
		{"negative-rows", 120, -1, 1001, 80, 24, 1001},
		{"negative-both", -1, -2, 0, 80, 24, 0},
		{"minimum-size", 1, 1, 59999, 1, 1, 59999},
		{"maximum-size", 65535, 65535, 60000, 65535, 65535, 60000},
		{"clamped-ka", 120, 90, 60001, 120, 90, 60000},
		{"overflow-safe-ka", 80, 24, int(^uint(0) >> 1), 80, 24, 60000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := ptyRequestSpec()
			spec.Cols, spec.Rows, spec.KAIntervalMS = tc.cols, tc.rows, tc.ka
			before, _ := json.Marshal(spec)
			got, err := ptyIntentHash(spec)
			if err != nil {
				t.Fatal(err)
			}
			expected := ptyRequestSpec()
			expected.Cols, expected.Rows, expected.KAIntervalMS = tc.wantCols, tc.wantRows, tc.wantKA
			body, err := json.Marshal(expected)
			if err != nil {
				t.Fatal(err)
			}
			want := sha256.Sum256(append([]byte("tether-pty-intent-v1\n"), body...))
			if got != hex.EncodeToString(want[:]) {
				t.Fatalf("normalization: got=%s want=%x", got, want)
			}
			after, _ := json.Marshal(spec)
			if !bytes.Equal(before, after) {
				t.Fatal("intent normalization mutated caller memory")
			}
		})
	}
	t.Run("nil-empty-env", func(t *testing.T) {
		spec := ptyRequestSpec()
		spec.Env = nil
		nilHash, err := ptyIntentHash(spec)
		if err != nil {
			t.Fatal(err)
		}
		spec.Env = map[string]string{}
		emptyHash, err := ptyIntentHash(spec)
		if err != nil || nilHash != emptyHash {
			t.Fatalf("empty env changed intent: nil=%s empty=%s err=%v", nilHash, emptyHash, err)
		}
	})
	t.Run("sorted-env-keys", func(t *testing.T) {
		// More than one tiny map is necessary: a single iteration can happen
		// to be sorted even when an implementation concatenates random keys.
		const canonical = `{"argv":["/bin/sh"],"env":{"A":"0","B":"1","C":"2","D":"3","E":"4","F":"5","G":"6","H":"7"},"cols":80,"rows":24}`
		want := sha256.Sum256([]byte("tether-pty-intent-v1\n" + canonical))
		for round := 0; round < 32; round++ {
			spec := proto.RunReq{Argv: []string{"/bin/sh"}, Env: make(map[string]string)}
			for i := 0; i < 8; i++ {
				key := (i + round) % 8
				spec.Env[string(rune('A'+key))] = fmt.Sprint(key)
			}
			got, err := ptyIntentHash(spec)
			if err != nil || got != hex.EncodeToString(want[:]) {
				t.Fatalf("map insertion/iteration changed intent: round=%d got=%s want=%x err=%v", round, got, want, err)
			}
		}
	})
	for _, tc := range []struct {
		name string
		edit func(*proto.RunReq)
	}{
		{"nil-argv", func(s *proto.RunReq) { s.Argv = nil }},
		{"empty-argv", func(s *proto.RunReq) { s.Argv = []string{} }},
		{"empty-command", func(s *proto.RunReq) { s.Argv[0] = "" }},
		{"nul-argv", func(s *proto.RunReq) { s.Argv[1] = "bad\x00arg" }},
		{"utf8-argv", func(s *proto.RunReq) { s.Argv[1] = "\xff" }},
		{"nul-cwd", func(s *proto.RunReq) { s.Cwd = "/bad\x00cwd" }},
		{"utf8-cwd", func(s *proto.RunReq) { s.Cwd = "\xff" }},
		{"empty-env-key", func(s *proto.RunReq) { s.Env[""] = "value" }},
		{"equals-env-key", func(s *proto.RunReq) { s.Env["A=B"] = "value" }},
		{"nul-env-key", func(s *proto.RunReq) { s.Env["A\x00B"] = "value" }},
		{"utf8-env-key", func(s *proto.RunReq) { s.Env["\xff"] = "value" }},
		{"nul-env-value", func(s *proto.RunReq) { s.Env["ONE"] = "a\x00b" }},
		{"utf8-env-value", func(s *proto.RunReq) { s.Env["ONE"] = "\xff" }},
		{"overflow-cols", func(s *proto.RunReq) { s.Cols = 65536 }},
		{"overflow-rows", func(s *proto.RunReq) { s.Rows = 65536 }},
		{"overflow-with-default-row", func(s *proto.RunReq) { s.Cols, s.Rows = 65536, 0 }},
		{"overflow-with-default-col", func(s *proto.RunReq) { s.Cols, s.Rows = 0, 65536 }},
		{"negative-ka", func(s *proto.RunReq) { s.KAIntervalMS = -1 }},
		{"oversize-intent", func(s *proto.RunReq) { s.Argv[1] = strings.Repeat("x", 1<<20) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := ptyRequestSpec()
			tc.edit(&spec)
			if hash, err := ptyIntentHash(spec); !errors.Is(err, ErrIdentity) || hash != "" {
				t.Fatalf("invalid intent accepted: hash=%s err=%v", hash, err)
			}
			f := newRequestFixture(t, 1)
			h := f.issue(t, "invalid")
			if _, fresh, err := f.r.ReservePTY(f.scope, h, spec, "invalid", "_TINBOX.pty", 1); !errors.Is(err, ErrIdentity) || fresh || len(f.r.records) != 0 {
				t.Fatalf("invalid intent acquired creation rights: fresh=%t records=%d err=%v", fresh, len(f.r.records), err)
			}
		})
	}
}

// origin: hot-update S4 plan §2.2 / T17
func TestPTYReservationAuthenticatesScopeAndAttachment(t *testing.T) {
	f := newRequestFixture(t, 2)
	h := f.issue(t, "pty")
	spec := ptyRequestSpec()
	spec.ActorFP = "untrusted nested actor"
	original, fresh, err := f.r.ReservePTY(f.scope, h, spec, "terminalTask", "_TINBOX.pty", 3)
	if err != nil || !fresh || original.Kind != proto.ProcessKindPTY {
		t.Fatalf("PTY reservation: fresh=%t kind=%q err=%v", fresh, original.Kind, err)
	}
	assertPTYIntentRetries(t, f.r, original)
	for _, field := range []string{"session", "node", "actor", "lineage"} {
		t.Run(field, func(t *testing.T) {
			wrong := f.scope
			switch field {
			case "session":
				wrong.SID += "changed"
			case "node":
				wrong.NID += "changed"
			case "actor":
				wrong.ActorFP = requestOtherScope().ActorFP
			case "lineage":
				wrong.ClientLineage += "changed"
			}
			// Claiming the lease actor inside Run must not repair a mismatch
			// in the authenticated scope; even a never-seen request is denied.
			spec := ptyRequestSpec()
			spec.ActorFP = f.scope.ActorFP
			for _, handle := range []proto.ProcessHandle{h, {Lease: h.Lease, RequestID: "unseen"}} {
				if _, fresh, err := f.r.ReservePTY(wrong, handle, spec, "attackerTask", "_TINBOX.attacker", 4); !errors.Is(err, ErrIdentity) || fresh {
					t.Fatalf("authenticated scope replaced by payload: fresh=%t err=%v", fresh, err)
				}
			}
			if _, err := f.r.Lookup(wrong, h); !errors.Is(err, ErrIdentity) {
				t.Fatalf("wrong scope read retained PTY: %v", err)
			}
			if _, err := f.r.Attach(wrong, h, "_TINBOX.attacker", 4); !errors.Is(err, ErrIdentity) {
				t.Fatalf("wrong scope attached to retained PTY: %v", err)
			}
		})
	}
	for _, tc := range []struct {
		name  string
		inbox string
		epoch uint64
		want  error
	}{
		{"zero-epoch", "_TINBOX.pty", 0, ErrIdentity},
		{"older-epoch", "_TINBOX.pty", 2, ErrStale},
		{"same-epoch-different-inbox", "_TINBOX.attacker", 3, ErrStale},
		{"empty-inbox", "", 4, ErrIdentity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, fresh, err := f.r.ReservePTY(f.scope, h, ptyRequestSpec(), "unused", tc.inbox, tc.epoch); !errors.Is(err, tc.want) || fresh {
				t.Fatalf("bad attachment accepted: fresh=%t err=%v want=%v", fresh, err, tc.want)
			}
			got, err := f.r.Lookup(f.scope, h)
			if err != nil || !reflect.DeepEqual(got, original) {
				t.Fatalf("refusal changed original attachment: epoch=%d inbox=%q err=%v", got.AttachEpoch, got.Inbox, err)
			}
		})
	}
	got, fresh, err := f.r.ReservePTY(f.scope, h, ptyRequestSpec(), "unused", "_TINBOX.replacement", 4)
	if err != nil || fresh || got.PID != original.PID || got.AttachEpoch != 4 || got.Inbox != "_TINBOX.replacement" || got.Kind != proto.ProcessKindPTY {
		t.Fatalf("new attachment lost creation identity: fresh=%t record=%+v err=%v", fresh, got, err)
	}
}

func assertPTYIntentRetries(t *testing.T, r *Registry, original Record) {
	t.Helper()
	scope := scopeOf(original.Handle.Lease)
	for _, tc := range []struct {
		name string
		edit func(*proto.RunReq)
	}{
		{"argv", func(s *proto.RunReq) { s.Argv[1] = "changed" }},
		{"cwd", func(s *proto.RunReq) { s.Cwd = "/" }},
		{"safe", func(s *proto.RunReq) { s.Safe = false }},
		{"env", func(s *proto.RunReq) { s.Env["ONE"] = "changed" }},
		{"cols", func(s *proto.RunReq) { s.Cols++ }},
		{"rows", func(s *proto.RunReq) { s.Rows++ }},
		{"ka", func(s *proto.RunReq) { s.KAIntervalMS++ }},
		{"ka-off", func(s *proto.RunReq) { s.KAIntervalMS = 0 }},
	} {
		spec := ptyRequestSpec()
		tc.edit(&spec)
		if _, fresh, err := r.ReservePTY(scope, original.Handle, spec, "unused", original.Inbox, original.AttachEpoch); !errors.Is(err, ErrConflict) || fresh {
			t.Fatalf("changed %s accepted in phase %s: fresh=%t err=%v", tc.name, original.Phase, fresh, err)
		}
	}
	for _, actor := range []string{"", scope.ActorFP, requestOtherScope().ActorFP, "untrusted"} {
		spec := ptyRequestSpec()
		spec.ActorFP = actor
		spec.Env = map[string]string{"TWO": "2", "ONE": "1"}
		spec.Cols, spec.Rows, spec.KAIntervalMS = 0, 90, 1
		got, fresh, err := r.ReservePTY(scope, original.Handle, spec, "unused", original.Inbox, original.AttachEpoch)
		if err != nil || fresh || !reflect.DeepEqual(got, original) {
			t.Fatalf("equivalent PTY intent lost record: phase=%s fresh=%t got=%+v err=%v", original.Phase, fresh, got, err)
		}
	}
}

// origin: hot-update S4 plan §5.1 / T17
func TestPTYRegistrySharesAdmissionAndRejectsUnknownKinds(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	f := newRequestFixture(t, 2)
	if got := f.r.RetainedKinds(); len(got) != 0 || f.r.Frozen() {
		t.Fatalf("empty summary changed registry: kinds=%v", got)
	}
	execHandle, ptyHandle := f.issue(t, "exec"), f.issue(t, "pty")
	execRecord := f.reserve(t, execHandle, "execTask")
	if _, fresh, err := f.r.ReservePTY(f.scope, ptyHandle, ptyRequestSpec(), execRecord.PID, "_TINBOX.pty", 1); !errors.Is(err, ErrConflict) || fresh {
		t.Fatalf("PTY reused exec PID: fresh=%t err=%v", fresh, err)
	}
	ptyRecord, fresh, err := f.r.ReservePTY(f.scope, ptyHandle, ptyRequestSpec(), "ptyTask", "_TINBOX.pty", 1)
	if err != nil || !fresh {
		t.Fatalf("PTY reservation: fresh=%t err=%v", fresh, err)
	}
	kinds := f.r.RetainedKinds()
	if !reflect.DeepEqual(kinds, []string{"", "pty"}) || f.r.Frozen() {
		t.Fatalf("starting records missing from live summary: %v", kinds)
	}
	kinds[1] = "caller-mutation"
	if !reflect.DeepEqual(f.r.RetainedKinds(), []string{"", "pty"}) {
		t.Fatal("summary aliases registry kinds")
	}
	assertPTYIntentRetries(t, f.r, ptyRecord) // A duplicate needs no new capacity.
	if _, fresh, err := f.r.ReservePTY(f.scope, f.issue(t, "full"), ptyRequestSpec(), "extra", "_TINBOX.extra", 1); !errors.Is(err, ErrCapacity) || fresh {
		t.Fatalf("PTY bypassed shared capacity: fresh=%t err=%v", fresh, err)
	}
	if _, fresh, err := f.r.ReservePTY(f.scope, execHandle, ptyRequestSpec(), "unused", execRecord.Inbox, 1); !errors.Is(err, ErrConflict) || fresh {
		t.Fatalf("exec became PTY on retry: fresh=%t err=%v", fresh, err)
	}
	if _, fresh, err := f.r.Reserve(f.scope, ptyHandle, requestSpec(), "unused", ptyRecord.Inbox, 1); !errors.Is(err, ErrConflict) || fresh {
		t.Fatalf("PTY became exec on retry: fresh=%t err=%v", fresh, err)
	}
	if _, err := f.r.Freeze(); !errors.Is(err, ErrBusy) {
		t.Fatalf("starting PTY exported: %v", err)
	}
	for _, record := range []Record{execRecord, ptyRecord} {
		if err := f.r.Running(record.Handle, record.PID); err != nil {
			t.Fatal(err)
		}
	}
	state, err := f.r.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"exec", "PTY", "future"} {
		bad := requestCloneState(t, state)
		bad.Records[len(bad.Records)-1].Kind = kind
		if restored, err := RestoreWithClock(bad, f.r.now); err == nil || restored != nil {
			t.Fatalf("unknown restored kind %q exposed owner: owner=%p err=%v", kind, restored, err)
		}
	}
	// Kind is an independent dispatch fence, even if a damaged image holds
	// the requested digest under the opposite valid kind.
	for _, record := range []Record{execRecord, ptyRecord} {
		bad := requestCloneState(t, state)
		for i := range bad.Records {
			if bad.Records[i].PID == record.PID {
				if record.Kind == "" {
					bad.Records[i].Kind = proto.ProcessKindPTY
				} else {
					bad.Records[i].Kind = ""
				}
			}
		}
		r, err := RestoreWithClock(bad, f.r.now)
		if err != nil {
			t.Fatal(err)
		}
		r.Thaw()
		var fresh bool
		if record.Kind == "" {
			_, fresh, err = r.Reserve(f.scope, record.Handle, requestSpec(), "unused", record.Inbox, 1)
		} else {
			_, fresh, err = r.ReservePTY(f.scope, record.Handle, ptyRequestSpec(), "unused", record.Inbox, 1)
		}
		if !errors.Is(err, ErrConflict) || fresh {
			t.Fatalf("matching digest bypassed kind fence: kind=%q fresh=%t err=%v", record.Kind, fresh, err)
		}
	}
	// The live preflight summary runs concurrently with admission and GC.
	// Exercise its read lock while the same table is repeatedly changed;
	// a serial summary test would never detect a missing reader fence.
	for round := 0; round < 5; round++ {
		f := newRequestFixture(t, 64)
		h := f.issue(t, "summary")
		start, done := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(done)
			<-start
			for i := 0; i < 1000; i++ {
				_ = f.r.RetainedKinds()
				runtime.Gosched()
			}
		}()
		close(start)
		for i := 0; i < 32; i++ {
			h.RequestID = fmt.Sprintf("summary%d", i)
			record, fresh, err := f.r.ReservePTY(f.scope, h, ptyRequestSpec(), h.RequestID, "_TINBOX.summary", 1)
			if err != nil || !fresh {
				t.Fatalf("concurrent summary admission: fresh=%t err=%v", fresh, err)
			}
			terminal := Terminal{ID: "terminal" + record.PID, ExitCode: 23}
			if err := f.r.Complete(h, record.PID, terminal); err != nil {
				t.Fatal(err)
			}
			if err := f.r.Settle(f.scope, h, terminal.ID); err != nil {
				t.Fatal(err)
			}
		}
		f.now.Store(h.Lease.ExpiresBootNS)
		if n := f.r.Collect(); n != 32 {
			t.Fatalf("concurrent summary collection: %d", n)
		}
		<-done
		if kinds := f.r.RetainedKinds(); len(kinds) != 0 {
			t.Fatalf("collected table retained a kind: %v", kinds)
		}
	}
	testharness.AssertNoGoroutineLeak(t, "pty-registry-summary", beforeG)
	testharness.AssertNoFDLeak(t, "pty-registry-summary", beforeFD, 0)
}

// origin: hot-update S4 plan §8.1 / T17
func TestPTYAdditionPreservesExecDigestAndRecordJSON(t *testing.T) {
	const canonical = `{"argv":["/bin/sh","-c","read -r line; exit 23"],"env":{"ONE":"1","TWO":"2"},"cwd":"/tmp","stdin":"b3JpZ2luYWwtaW5wdXQ=","safe":true}`
	want := sha256.Sum256([]byte("tether-process-intent-v1\n" + canonical))
	spec := requestSpec()
	spec.ActorFP = requestOtherScope().ActorFP
	got, err := intentHash(spec)
	if err != nil || got != hex.EncodeToString(want[:]) {
		t.Fatalf("exec digest changed: got=%s want=%x err=%v", got, want, err)
	}
	// Exec's existing omitempty treatment of both slices and maps stays exact.
	spec.Env, spec.Stdin = nil, nil
	nilHash, err := intentHash(spec)
	if err != nil {
		t.Fatal(err)
	}
	spec.Env, spec.Stdin = map[string]string{}, []byte{}
	emptyHash, err := intentHash(spec)
	if err != nil || nilHash != emptyHash {
		t.Fatalf("empty exec fields changed digest: nil=%s empty=%s err=%v", nilHash, emptyHash, err)
	}
	f := newRequestFixture(t, 1)
	record := f.reserve(t, f.issue(t, "exec"), "execTask")
	if record.Kind != "" {
		t.Fatalf("exec kind acquired a nonzero alias: %q", record.Kind)
	}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(`"kind"`)) {
		t.Fatal("exec record now emits an S4-only field")
	}
}
