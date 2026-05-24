// Package serveconf — ps-retention-plan §Config tests.
//
// These tests pin the two duration fields the broker.yaml decoder now
// exposes (`proc_retention`, `proc_gc_interval`) so a future refactor
// can't silently regress the wire-through to cmd/tether/serve.go's
// broker.Config (audit shard ps-retention-#3 found exactly that
// regression in the first commit of the plan).
package serveconf

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeConfig drops `body` into a tempdir-scoped broker.yaml and
// returns the path, ready to feed Load().
func writeConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "broker.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestProcRetentionDuration_Parses(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
broker:
  storage:
    db: /tmp/x.db
    proc_retention: 90m
    proc_gc_interval: 5m
`))
	if err != nil {
		t.Fatal(err)
	}
	d, err := cfg.ProcRetentionDuration()
	if err != nil {
		t.Fatal(err)
	}
	if d != 90*time.Minute {
		t.Errorf("proc_retention: want 90m got %v", d)
	}
	gc, err := cfg.ProcGCIntervalDuration()
	if err != nil {
		t.Fatal(err)
	}
	if gc != 5*time.Minute {
		t.Errorf("proc_gc_interval: want 5m got %v", gc)
	}
}

func TestProcRetentionDuration_EmptyMeansZero(t *testing.T) {
	cfg, err := Load(writeConfig(t, `broker: {}`))
	if err != nil {
		t.Fatal(err)
	}
	d, err := cfg.ProcRetentionDuration()
	if err != nil {
		t.Fatal(err)
	}
	if d != 0 {
		t.Errorf("empty proc_retention should be zero (broker.New picks default), got %v", d)
	}
	gc, err := cfg.ProcGCIntervalDuration()
	if err != nil {
		t.Fatal(err)
	}
	if gc != 0 {
		t.Errorf("empty proc_gc_interval should be zero, got %v", gc)
	}
}

// The reject-on-invalid contract is enforced in the accessors and
// also re-checked from Load() so a bad value aborts startup before
// any side effect. Tests below pin both layers: accessor (direct
// Config) + Load (full yaml round-trip).

func TestProcRetentionDuration_InvalidIsLoud(t *testing.T) {
	cfg := &Config{Broker: BrokerSection{Storage: StorageSection{
		ProcRetention: "not-a-duration",
	}}}
	if _, err := cfg.ProcRetentionDuration(); err == nil {
		t.Errorf("accessor: invalid proc_retention silently accepted")
	}
	if _, err := Load(writeConfig(t, `
broker:
  storage:
    proc_retention: not-a-duration
`)); err == nil {
		t.Errorf("Load: invalid proc_retention silently accepted")
	}
}

func TestProcRetentionDuration_RejectsNegative(t *testing.T) {
	cfg := &Config{Broker: BrokerSection{Storage: StorageSection{
		ProcRetention: "-1h",
	}}}
	if _, err := cfg.ProcRetentionDuration(); err == nil {
		t.Errorf("accessor: negative proc_retention silently accepted")
	}
	if _, err := Load(writeConfig(t, `
broker:
  storage:
    proc_retention: -1h
`)); err == nil {
		t.Errorf("Load: negative proc_retention silently accepted")
	}
}

func TestProcGCInterval_RejectsSubMinute(t *testing.T) {
	cfg := &Config{Broker: BrokerSection{Storage: StorageSection{
		ProcGCInterval: "30s",
	}}}
	if _, err := cfg.ProcGCIntervalDuration(); err == nil {
		t.Errorf("accessor: sub-minute proc_gc_interval silently accepted")
	}
	if _, err := Load(writeConfig(t, `
broker:
  storage:
    proc_gc_interval: 30s
`)); err == nil {
		t.Errorf("Load: sub-minute proc_gc_interval silently accepted")
	}
}
