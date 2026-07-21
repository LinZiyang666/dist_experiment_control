package main

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// admin_events_test.go (#30) — the R11 2>&1-cleanliness discipline for `admin events --json`, plus
// that the filter flags are actually sent on the wire.

// TestAdminEventsJSONStreamStaysClean: with --json the SUCCESS path puts ONLY machine JSON on stdout,
// so `admin events --json 2>&1 | jq` never sees prose. We MERGE stdout+stderr (test at the sink) and
// assert the merged output parses as adminEventsJSON.
func TestAdminEventsJSONStreamStaysClean(t *testing.T) {
	stubCallAdmin(t, func(_ string, req adminsock.Request) (*adminsock.Response, error) {
		if req.Op != adminsock.OpEvents {
			t.Fatalf("unexpected op %q", req.Op)
		}
		return &adminsock.Response{Op: adminsock.OpEvents, OK: true, Events: []adminsock.AuditEntry{
			{Subject: "tether.v2.sys.events", Seq: 1, Ts: time.Unix(1700, 0).UTC(), Body: map[string]any{"type": "tetherd_restarted"}},
			{Subject: "tether.v2.sys.events", Seq: 2, Ts: time.Unix(1701, 0).UTC(), Body: map[string]any{"type": "proxy_keyset_changed", "sid": "lab"}},
		}}, nil
	})

	var merged bytes.Buffer
	root := newRootCmd()
	root.SetOut(&merged)
	root.SetErr(&merged)
	root.SetArgs([]string{"admin", "events", "--json"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}

	var got adminEventsJSON
	if err := json.Unmarshal(merged.Bytes(), &got); err != nil {
		t.Fatalf("merged stdout+stderr is not clean JSON (%v):\n%s", err, merged.String())
	}
	if got.Schema != "admin_events" || got.SchemaVersion != 1 || len(got.Events) != 2 {
		t.Errorf("decoded events wrong: %+v", got)
	}
}

// TestAdminEventsJSONEmptyIsCleanArray: no events must serialize as [] (never null) so `jq '.events |
// length'` is 0, not a null-deref.
func TestAdminEventsJSONEmptyIsCleanArray(t *testing.T) {
	stubCallAdmin(t, func(string, adminsock.Request) (*adminsock.Response, error) {
		return &adminsock.Response{Op: adminsock.OpEvents, OK: true, Events: nil}, nil
	})
	var out bytes.Buffer
	root := newRootCmd()
	root.SetOut(&out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"admin", "events", "--json"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}
	var got adminEventsJSON
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("not JSON: %v (%s)", err, out.String())
	}
	if got.Events == nil {
		t.Errorf("events must be [] not null: %s", out.String())
	}
}

// TestAdminEventsSendsFilters: --n/--since/--kind must reach the wire request (a monitor relies on the
// server, not the CLI, doing the filtering; a flag that is parsed but not sent is a silent no-op).
func TestAdminEventsSendsFilters(t *testing.T) {
	var got adminsock.Request
	stubCallAdmin(t, func(_ string, req adminsock.Request) (*adminsock.Response, error) {
		got = req
		return &adminsock.Response{Op: adminsock.OpEvents, OK: true}, nil
	})
	root := newRootCmd()
	root.SetOut(&bytes.Buffer{})
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"admin", "events", "--json", "-n", "7", "--since", "2h", "--kind", "disk_pressure"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got.N != 7 {
		t.Errorf("N not sent: %d", got.N)
	}
	if got.Since != "2h0m0s" {
		t.Errorf("since not sent: %q", got.Since)
	}
	if got.EventKind != "disk_pressure" {
		t.Errorf("kind not sent: %q", got.EventKind)
	}
}

// TestAdminEventsJSONFailureWritesNothingToStdout: on a transport failure the --json path emits NO
// partial object to stdout (the failure travels via non-zero exit + stderr).
func TestAdminEventsJSONFailureWritesNothingToStdout(t *testing.T) {
	stubCallAdmin(t, func(string, adminsock.Request) (*adminsock.Response, error) {
		return nil, unavailErr("admin socket /x: dial: no such file")
	})
	var out bytes.Buffer
	root := newRootCmd()
	root.SetOut(&out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"admin", "events", "--json"})
	if err := root.Execute(); err == nil {
		t.Fatal("expected a transport error")
	}
	if out.Len() != 0 {
		t.Errorf("stdout must be empty on failure, got %q", out.String())
	}
}
