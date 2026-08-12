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
	"strings"
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

// TestXferCrossHomeReapAgeDuration (R16 #58 Lane C) pins the leader cross-home GC age-floor knob.
//
// EXTERNAL REVIEW F2: this test used to assert that 8s PARSES — i.e. it pinned the unsafe behaviour as
// correct. The production schema must only let an operator RAISE the floor: in a split-home session the
// leader cannot see a transfer still live on another home, so any floor below the tier-B watchdog lets
// it delete an in-use object. Empty still means the broker's derived default.
func TestXferCrossHomeReapAgeDuration(t *testing.T) {
	if _, err := Load(writeConfig(t, "broker:\n  cluster:\n    xfer_cross_home_reap_age: 8s\n")); err == nil {
		t.Fatal("8s must be REJECTED: it is far below the tier-B watchdog, so the leader could reap an " +
			"object still live on another home (external review F2)")
	}
	cfg, err := Load(writeConfig(t, "broker:\n  cluster:\n    xfer_cross_home_reap_age: 30m\n"))
	if err != nil {
		t.Fatal(err)
	}
	if d, err := cfg.XferCrossHomeReapAgeDuration(); err != nil || d != 30*time.Minute {
		t.Fatalf("raising the floor must be allowed: want 30m got %v (err %v)", d, err)
	}
	if _, err := Load(writeConfig(t, "broker:\n  cluster:\n    xfer_cross_home_reap_age: 14m59s\n")); err == nil {
		t.Fatalf("anything below the %s safe floor must be rejected", MinXferCrossHomeReapAge)
	}
	empty, err := Load(writeConfig(t, `broker: {}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := empty.XferCrossHomeReapAgeDuration(); got != 0 {
		t.Errorf("empty xfer_cross_home_reap_age should be zero (broker default), got %v", got)
	}
	if _, err := Load(writeConfig(t, "broker:\n  cluster:\n    xfer_cross_home_reap_age: 200ms\n")); err == nil {
		t.Error("Load: sub-second xfer_cross_home_reap_age accepted (a near-zero floor could tear out a live peer transfer)")
	}
	if _, err := Load(writeConfig(t, "broker:\n  cluster:\n    xfer_cross_home_reap_age: 25h\n")); err == nil {
		t.Error("Load: >24h xfer_cross_home_reap_age accepted (an effectively-disabled floor immortalizes split-home garbage)")
	}
}

// TestReapIntervalUpperBound (external review N-5) pins the upper clamp on the three interval knobs: a
// value like 10000h passes the sub-second/sub-minute floor yet SILENTLY DISABLES the reaper/monitor
// (immortal garbage / no disk-pressure). Each knob must reject > 24h, accept exactly 24h (the boundary),
// and reject just past it — and the rejection must fire at Load(), not only at the accessor.
func TestReapIntervalUpperBound(t *testing.T) {
	xfer := func(v string) *Config {
		return &Config{Broker: BrokerSection{Cluster: ClusterSection{XferReapInterval: v}}}
	}
	disk := func(v string) *Config { return &Config{Broker: BrokerSection{Obs: ObsSection{DiskCheckInterval: v}}} }
	gc := func(v string) *Config {
		return &Config{Broker: BrokerSection{Storage: StorageSection{ProcGCInterval: v}}}
	}

	cases := []struct {
		name  string
		over  func() (time.Duration, error)
		atMax func() (time.Duration, error)
		just  func() (time.Duration, error)
	}{
		{"xfer_reap_interval", xfer("10000h").XferReapIntervalDuration, xfer("24h").XferReapIntervalDuration, xfer("24h1s").XferReapIntervalDuration},
		{"disk_check_interval", disk("10000h").DiskCheckIntervalDuration, disk("24h").DiskCheckIntervalDuration, disk("24h1s").DiskCheckIntervalDuration},
		{"proc_gc_interval", gc("10000h").ProcGCIntervalDuration, gc("24h").ProcGCIntervalDuration, gc("24h1s").ProcGCIntervalDuration},
	}
	for _, c := range cases {
		if _, err := c.over(); err == nil {
			t.Errorf("%s: 10000h must be rejected (silently-disabled reaper), got nil error", c.name)
		}
		if d, err := c.atMax(); err != nil || d != 24*time.Hour {
			t.Errorf("%s: exactly 24h must be accepted (boundary), got d=%v err=%v", c.name, d, err)
		}
		if _, err := c.just(); err == nil {
			t.Errorf("%s: 24h1s must be rejected (just over the bound), got nil error", c.name)
		}
	}

	// Load()-level: an over-bound value must fail the launch, not just the accessor.
	if _, err := Load(writeConfig(t, "broker:\n  cluster:\n    xfer_reap_interval: 10000h\n")); err == nil {
		t.Error("Load: over-24h xfer_reap_interval silently accepted (should reject at launch)")
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

// ---------------------------------------------------------------------------
// #75 strict decoding — the non-strict yaml.Unmarshal this pins against used
// to swallow mis-nested / typo'd keys with zero warning. The live incident
// shape was an `observability:` block that never took effect while the broker
// wrote unbounded logs elsewhere. Every "must error" case below passed
// SILENTLY before the strict decoder landed.
// origin: docs/deploy-tier-gotchas.md #75
// ---------------------------------------------------------------------------

func TestLoadStrictRejectsUnknownAndMisnestedKeys(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantKey string // substring the error must carry so the operator sees WHICH key
	}{
		{
			// The live racknerd shape: observability hoisted to the document
			// root instead of under broker: — silently ignored before.
			name: "observability mis-nested at document root",
			body: `
broker:
  domain: example.com
observability:
  log_file: /var/log/tether/broker.log
  log_max_size_mb: 50
`,
			wantKey: "observability",
		},
		{
			// One nesting level short: log_file directly under broker:.
			name: "log_file directly under broker",
			body: `
broker:
  domain: example.com
  log_file: /var/log/tether/broker.log
`,
			wantKey: "log_file",
		},
		{
			name: "typo'd key inside observability",
			body: `
broker:
  observability:
    log_flie: /var/log/tether/broker.log
`,
			wantKey: "log_flie",
		},
		{
			name: "unknown top-level key",
			body: `
broker:
  domain: example.com
brokre:
  domain: oops.example.com
`,
			wantKey: "brokre",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tc.body))
			if err == nil {
				t.Fatalf("mis-configured yaml silently accepted (the #75 failure mode)")
			}
			if !strings.Contains(err.Error(), tc.wantKey) {
				t.Fatalf("error must name the offending key %q so the operator can find it; got: %v", tc.wantKey, err)
			}
		})
	}
}

func TestLoadStrictAcceptsLegalShapes(t *testing.T) {
	// Fully-populated legal config: every field lands where it should.
	cfg, err := Load(writeConfig(t, `
broker:
  domain: example.com
  public_host: example.com
  tls:
    acme:
      email: ops@example.com
  nats:
    url: nats://127.0.0.1:4222
    wss_listen: ":443"
    ws_internal: "127.0.0.1:8222"
  observability:
    log_file: /var/log/tether/broker.log
    log_max_size_mb: 50
    log_max_backups: 2
`))
	if err != nil {
		t.Fatalf("legal config refused: %v", err)
	}
	if cfg.Broker.Obs.LogFile != "/var/log/tether/broker.log" || cfg.Broker.Obs.LogMaxSizeMB != 50 {
		t.Fatalf("observability did not land: %+v", cfg.Broker.Obs)
	}
	if cfg.Broker.TLS.ACME.Email != "ops@example.com" {
		t.Fatalf("inert tls stub did not parse: %+v", cfg.Broker.TLS)
	}

	// Empty file and comment-only file = zero config, no error (the agent-side
	// loader's EOF tolerance, mirrored).
	for _, body := range []string{"", "# just a comment\n", "\n\n"} {
		if _, err := Load(writeConfig(t, body)); err != nil {
			t.Fatalf("empty/comment-only config must be tolerated, got: %v", err)
		}
	}

	// Empty path = zero config (the no --config path).
	if _, err := Load(""); err != nil {
		t.Fatalf("empty path must yield zero config: %v", err)
	}
}

func TestLoadStrictRejectsSecondDocument(t *testing.T) {
	// A second non-empty document could hide a section the first doesn't show.
	if _, err := Load(writeConfig(t, `
broker:
  domain: example.com
---
broker:
  domain: hidden.example.com
`)); err == nil {
		t.Fatal("second non-empty YAML document silently accepted")
	}
	// A benign trailing `---` (empty second document) stays tolerated.
	if _, err := Load(writeConfig(t, "broker:\n  domain: example.com\n---\n")); err != nil {
		t.Fatalf("empty trailing document must be tolerated: %v", err)
	}
	// An empty MIDDLE document must not shield a later non-empty one.
	if _, err := Load(writeConfig(t, "broker:\n  domain: example.com\n---\n---\nbroker:\n  domain: x\n")); err == nil {
		t.Fatal("empty middle document shielded a hidden third document")
	}
}
