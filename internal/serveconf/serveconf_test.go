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

// TestXferReapIntervalDuration (#58/P10) pins the new home-authoritative orphan-reap cadence knob:
// a valid sub-minute value parses (the whole point — a drill / operator can shorten it below the
// proc-GC 1m floor to OBSERVE the reap), empty means "broker default", and a sub-second value is
// rejected at Load so a bad config fails the launch instead of churning the JS API.
func TestXferReapIntervalDuration(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
broker:
  cluster:
    xfer_reap_interval: 8s
`))
	if err != nil {
		t.Fatal(err)
	}
	d, err := cfg.XferReapIntervalDuration()
	if err != nil {
		t.Fatal(err)
	}
	if d != 8*time.Second {
		t.Errorf("xfer_reap_interval: want 8s got %v", d)
	}

	empty, err := Load(writeConfig(t, `broker: {}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := empty.XferReapIntervalDuration(); got != 0 {
		t.Errorf("empty xfer_reap_interval should be zero (broker default), got %v", got)
	}

	if _, err := Load(writeConfig(t, "broker:\n  cluster:\n    xfer_reap_interval: 200ms\n")); err == nil {
		t.Error("Load: sub-second xfer_reap_interval silently accepted (should reject at launch)")
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

// --- #39 disk_check_interval knob ---

func TestDiskCheckIntervalDuration_Parses(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
broker:
  observability:
    disk_check_interval: 2m
`))
	if err != nil {
		t.Fatal(err)
	}
	d, err := cfg.DiskCheckIntervalDuration()
	if err != nil {
		t.Fatal(err)
	}
	if d != 2*time.Minute {
		t.Errorf("disk_check_interval: want 2m got %v", d)
	}
}

func TestDiskCheckIntervalDuration_EmptyMeansZero(t *testing.T) {
	// Empty ⇒ 0 so serve.go/broker fall back to the built-in 5m default (default preserved).
	cfg := &Config{}
	d, err := cfg.DiskCheckIntervalDuration()
	if err != nil {
		t.Fatal(err)
	}
	if d != 0 {
		t.Errorf("empty disk_check_interval should be zero (broker picks 5m default), got %v", d)
	}
}

func TestDiskCheckIntervalDuration_RejectsSubSecondAndInvalid(t *testing.T) {
	// Sub-second: a negative/zero would panic time.NewTicker; a sub-second poll is statfs churn.
	if _, err := (&Config{Broker: BrokerSection{Obs: ObsSection{DiskCheckInterval: "500ms"}}}).DiskCheckIntervalDuration(); err == nil {
		t.Errorf("accessor: sub-second disk_check_interval silently accepted")
	}
	if _, err := (&Config{Broker: BrokerSection{Obs: ObsSection{DiskCheckInterval: "-5m"}}}).DiskCheckIntervalDuration(); err == nil {
		t.Errorf("accessor: negative disk_check_interval silently accepted")
	}
	if _, err := (&Config{Broker: BrokerSection{Obs: ObsSection{DiskCheckInterval: "nonsense"}}}).DiskCheckIntervalDuration(); err == nil {
		t.Errorf("accessor: unparseable disk_check_interval silently accepted")
	}
	// Load() must run the validator so a bad value aborts startup BEFORE storage.Open.
	if _, err := Load(writeConfig(t, `
broker:
  observability:
    disk_check_interval: 500ms
`)); err == nil {
		t.Errorf("Load: sub-second disk_check_interval silently accepted")
	}
}
