// Package spawnsafe makes the agent's exec/run spawn path survive a hung
// network filesystem.
//
// The problem (architecture / docs/reviews/remote-fs-resilience-plan.md):
// os/exec's Command runs LookPath at CONSTRUCTION time, stat()ing every $PATH
// dir in order against the agent's $PATH. When the first dir is on a wedged
// NFS/CIFS/sshfs mount (uninterruptible D-state, kill -9 cannot reap), the
// stat hangs forever BEFORE any fork — so a trivial `whoami` hangs even though
// the agent daemon itself is fine.
//
// The fix has three load-bearing pieces, all behind a func-injection seam so
// they are hermetically testable with no real NFS:
//
//   - Classify mounts from /proc/self/mountinfo (procfs, never hangs) by
//     fstype; longest-prefix map a path to its backing mount. (Policy.snapshot)
//   - Bounded, single-flight, sticky, self-healing health probe of a hangable
//     mount (one statfs behind a goroutine+timer; a known-dead mount is never
//     re-probed but auto-heals if its outstanding probe later succeeds).
//   - Sanitize $PATH (drop unhealthy-hangable dirs), resolve argv[0] against
//     the sanitized PATH ourselves, and hand the caller an absolute Path so it
//     can build &exec.Cmd{Path: abs, Args: argv} directly — bypassing the
//     hanging LookPath entirely. Order is load-bearing: sanitize, THEN resolve.
//
// On a machine with no hangable mounts, or with the feature off, Prepare
// returns Decision{Active:false} and the caller uses the legacy exec.Command
// path unchanged — byte-identical to before. On non-Linux the mountinfo read
// fails, yielding zero hangable mounts, so the package is fully inert.
package spawnsafe

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Mode is the agent's remote-fs posture (from agent.yaml remote_fs.mode).
type Mode int

const (
	// ModeAuto sanitizes + warns only when a hangable mount is actually
	// unhealthy; on healthy/local machines it is inert. Default.
	ModeAuto Mode = iota
	// ModeOff disables the feature entirely (today's exact spawn path),
	// unless a per-call Safe=true escalates it back to auto-behavior.
	ModeOff
)

// ParseMode maps an agent.yaml string to a Mode. "" defaults to auto. An
// unknown value is an error so New() can fail loud (matching KnownFields).
func ParseMode(s string) (Mode, error) {
	switch strings.TrimSpace(s) {
	case "", "auto":
		return ModeAuto, nil
	case "off":
		return ModeOff, nil
	default:
		return ModeAuto, fmt.Errorf("remote_fs.mode: unknown value %q (want auto|off)", s)
	}
}

// Defaults.
const (
	DefaultProbeTimeout = 2 * time.Second
	DefaultWedgeCeiling = 64

	// DefaultHealthTTL bounds how long a HEALTHY verdict is trusted without
	// re-validation. It is the backstop for the two ZERO-EVIDENCE workloads
	// that the evidence-driven invalidation (see invalidateHealthy) structurally
	// cannot see:
	//
	//   (a) explicit argv[0] + local cwd (`exec -- /bin/bash -c ...`): Prepare's
	//       explicit branch never enters boundedResolveInDirs, and /bin/bash
	//       execve's instantly — so the agent produces no stall at all, while
	//       sanitizePATH still consulted the stale-healthy verdict and handed the
	//       CHILD a poisoned PATH. That child is the one that D-hangs. This is the
	//       exact shape of the D-state bash processes found on timan107.
	//   (b) wedge ceiling saturated: boundedResolveInDirs / RunStartWithCleanup
	//       early-return BEFORE their select, so no timeout branch ever runs.
	//
	// Five minutes, not thirty seconds: evidence covers every path that actually
	// stalls and heals it within one command, so the TTL only serves workloads
	// that are NOT currently broken — buying them faster healing at the price of
	// an order of magnitude more false-demotion dice rolls for everyone else is
	// the wrong trade (plan §-1 OQ-2).
	//
	// A DEAD verdict is never re-probed and the TTL does not apply to it: see the
	// D-state thread budget argument on mountHealth.
	DefaultHealthTTL = 5 * time.Minute
)

// MountSource returns the raw bytes of the mount table (real impl reads
// /proc/self/mountinfo). Returning an error/empty yields zero mounts ⇒ the
// package is inert (the non-Linux path).
type MountSource func() ([]byte, error)

// ProbeFn does ONE blocking liveness check of a mountpoint; the real impl is a
// statfs that returns true on success and may block forever on a wedged mount
// (that is the point — the Policy wraps it in a bounded goroutine). Tests
// inject a fake that blocks on a channel they control.
type ProbeFn func(mountpoint string) bool

// RealMountSource reads /proc/self/mountinfo. Empty on non-Linux / read error.
func RealMountSource() ([]byte, error) { return os.ReadFile("/proc/self/mountinfo") }

// fstype categories.
type fsKind int

const (
	kindLocal       fsKind = iota // not a known network fs ⇒ assume local, never probe
	kindAutomount                 // autofs: protect resolution/start, but never probe or drop
	kindRemoteProbe               // hangable network fs ⇒ probe before trusting
)

// remoteFstypes is the conservative denylist of fstypes whose server can wedge
// a syscall into D-state. Anything NOT matched is treated as local: a "classify
// everything" engine risks self-inflicting an outage by stripping a novel local
// fstype, so we only ever ADD safety.
var remoteFstypes = map[string]bool{
	"nfs": true, "nfs4": true, "cifs": true, "smb3": true, "smbfs": true,
	"fuse.sshfs": true, "sshfs": true, "fuse.s3fs": true,
	"fuse.glusterfs": true, "glusterfs": true, "lustre": true,
	"ceph": true, "fuse.ceph": true, "9p": true, "afs": true,
	"ncpfs": true, "coda": true, "beegfs": true, "gpfs": true,
	"objectivefs": true,
}

// classifyFstype maps an fstype string to a category. Prefix rules catch
// fuse.* network backends and nfs* variants we did not enumerate.
//
// autofs is a third category (external review F10): it cannot be probed because a
// stat can synchronously trigger the automount, and it must not be treated as
// confirmed-dead because that would break the first access on a healthy machine.
// It still makes the policy active so resolution/start run behind deadlines.
func classifyFstype(fstype string) fsKind {
	if fstype == "autofs" {
		return kindAutomount
	}
	if remoteFstypes[fstype] {
		return kindRemoteProbe
	}
	if strings.HasPrefix(fstype, "nfs") || strings.HasPrefix(fstype, "fuse.") {
		return kindRemoteProbe
	}
	return kindLocal
}

type mountEntry struct {
	mountpoint string
	fstype     string
	kind       fsKind
	signature  string // full mountinfo line; changes on remount/instance replacement
}

const (
	stUnprobed int8 = iota
	stHealthy
	stUnhealthy
)

// mountHealth is the cached verdict for one hangable mountpoint. result is the
// buffered(1) channel the single outstanding probe goroutine sends into; it is
// retained after a timeout so a late success can self-heal a sticky-dead mount.
// done is closed by the launcher once the first verdict is decided, so concurrent
// joiners wake immediately instead of each blocking a full probeTimeout (review m9).
//
// A struct is ONE GENERATION of one mountpoint's verdict. Re-validating a healthy
// mount REPLACES the pointer (p.health[mp] = &mountHealth{}) — it never resets
// this struct in place. Two failure modes make in-place reset wrong. Both were
// found by the round-1 adversarial critics rather than by reasoning, and both are
// stated in the SUBJUNCTIVE on purpose: they describe what in-place reset would do,
// not defects that exist here. (The first of them was disarmed in the same commit
// that introduced this design — the launcher now closes its captured local, not the
// field — so reading it as a present-tense fact would send the next reader looking
// for a bug that is not there.)
//
//   - a launcher that closed the FIELD h.done rather than the local it captured
//     would hit close-of-closed the moment a reset swapped in a fresh channel;
//   - the launcher's timeout branch is guarded by `state == stUnprobed`, so a
//     late-waking launcher from the OLD generation would refuse to demote a
//     re-armed entry — the fix would silently not fix anything.
//
// Both are pinned: TestMountHealthy_reArmReplacesGenerationPointer deterministically,
// TestMountHealthy_reArmSurvivesConcurrentLauncherWakeup under churn.
//
// Two invariants carry the whole correctness argument. They are load-bearing:
//
//	INV-1  h.result == nil  ⟺  this struct's probe goroutine has provably exited.
//	       result is set to nil only after a value was received from it; the
//	       timeout branch deliberately KEEPS result because the goroutine may
//	       still be in D-state.
//	INV-2  state == stHealthy  ⇒  result == nil.
//	       The only two writes of stHealthy both nil out result in the same
//	       critical section; the timeout branch never writes stHealthy.
//
// INV-2 is why the re-validation guard is exactly `state == stHealthy` and
// nothing more: that one term ALREADY implies "no probe is in flight", so an
// additional `result == nil` conjunct would be a clause that can never be false.
// Do not add it back — three independent round-1 drafts hung their whole
// D-state-thread argument on that tautology, which would have shipped two
// identity tests. `state == stHealthy` is the load-bearing half; it is also what
// keeps a DEAD verdict sticky, so widening it re-probes dead mounts and leaks an
// unbounded number of D-state threads (probes do NOT take a wedge slot).
type mountHealth struct {
	state    int8
	launched bool
	result   chan bool
	done     chan struct{}
	// decidedAt is when a PROBE last decided state. It drives the TTL only; it is
	// deliberately NOT refreshed by applyMounts' carry-over (see the note there).
	decidedAt time.Time
}

// Policy is the per-agent spawn-safety engine. Construct with New; safe for
// concurrent use by all forwarded exec/run handlers.
type Policy struct {
	mode         Mode
	probeTimeout time.Duration
	healthTTL    time.Duration
	wedgeCeiling int
	safeDir      string
	safeDirCfg   string
	pathsOnce    sync.Once

	mountSrc MountSource
	probe    ProbeFn
	now      func() time.Time                                // nil ⇒ time.Now (test seam)
	resolver func(name string, dirs []string) (string, bool) // nil ⇒ resolveInDirs (test seam)

	bootHangable bool // hasHangable at New time; immutable (review m7 fast path)

	mu          sync.Mutex
	gen         string // content hash of the last mountinfo snapshot
	mounts      []mountEntry
	hasHangable bool
	fallback    []string // existence-checked local fallback PATH dirs
	health      map[string]*mountHealth
	wedged      int
}

// Config carries the operator-tunable knobs from agent.yaml/Config.
type Config struct {
	Mode         Mode
	SafeDir      string        // "" ⇒ os.TempDir()
	ProbeTimeout time.Duration // 0 ⇒ DefaultProbeTimeout
	WedgeCeiling int           // 0 ⇒ DefaultWedgeCeiling
	HealthTTL    time.Duration // 0 ⇒ DefaultHealthTTL

	// Seams; nil ⇒ real implementations.
	MountSource MountSource
	Probe       ProbeFn
	Now         func() time.Time                                // clock seam for the health TTL
	Resolver    func(name string, dirs []string) (string, bool) // argv[0] resolution (test seam)
}

// New builds a Policy and takes the initial mount snapshot. It validates config
// (returning an error for a negative wedge_ceiling or a relative safe_dir —
// external review F12) and NEVER touches the filesystem before the mount
// snapshot exists, so a dead fallback/safe dir cannot D-hang startup before the
// protection logic is built (external review F3). The only unconditional syscall
// is the procfs mountinfo read.
func New(cfg Config) (*Policy, error) {
	if cfg.WedgeCeiling < 0 {
		return nil, fmt.Errorf("remote_fs.wedge_ceiling: must not be negative (got %d; 0 = default)", cfg.WedgeCeiling)
	}
	if cfg.ProbeTimeout < 0 {
		return nil, fmt.Errorf("remote_fs.probe_timeout: must not be negative")
	}
	if cfg.HealthTTL < 0 {
		// NOT spelled as an agent.yaml key: unlike probe_timeout / wedge_ceiling /
		// safe_dir, HealthTTL is a test seam with no yaml surface, and naming a key
		// that does not exist sends an operator hunting for a knob they cannot set
		// (plan §7 R12 keeps this batch key-free so rollback stays a pure binary swap).
		return nil, fmt.Errorf("spawnsafe.Config.HealthTTL: must not be negative")
	}
	if sd := strings.TrimSpace(cfg.SafeDir); sd != "" && !filepath.IsAbs(sd) {
		return nil, fmt.Errorf("remote_fs.safe_dir: must be an absolute path (got %q)", sd)
	}
	p := &Policy{
		mode:         cfg.Mode,
		probeTimeout: cfg.ProbeTimeout,
		healthTTL:    cfg.HealthTTL,
		wedgeCeiling: cfg.WedgeCeiling,
		safeDirCfg:   cfg.SafeDir,
		mountSrc:     cfg.MountSource,
		probe:        cfg.Probe,
		now:          cfg.Now,
		resolver:     cfg.Resolver,
		health:       map[string]*mountHealth{},
	}
	if p.probeTimeout <= 0 {
		p.probeTimeout = DefaultProbeTimeout
	}
	if p.healthTTL <= 0 {
		p.healthTTL = DefaultHealthTTL
	}
	if p.wedgeCeiling <= 0 {
		p.wedgeCeiling = DefaultWedgeCeiling
	}
	if p.mountSrc == nil {
		p.mountSrc = RealMountSource
	}
	if p.probe == nil {
		p.probe = realProbe
	}
	if p.now == nil {
		p.now = time.Now
	}
	// Snapshot FIRST (procfs, never hangs) — before any path is touched (F3).
	p.snapshot()
	// Remember whether the BOOT snapshot had any hangable mount. A machine that
	// booted with none is treated as local for the process lifetime: an auto-mode
	// Prepare short-circuits without even the per-spawn procfs read, so a
	// healthy/local agent is truly zero-syscall-per-spawn (review m7). An explicit
	// --safe still refreshes (review F7). A network mount added AFTER boot is not
	// picked up by auto until restart — documented (usage §7.7 / plan §1).
	p.mu.Lock()
	p.bootHangable = p.hasHangable
	p.mu.Unlock()
	// Only touch fallback/safe-dir candidates when the boot snapshot proves this
	// policy will actually protect spawns. A local agent stays at one procfs read,
	// and mode=off remains a true escape hatch until a call explicitly requests
	// --safe. Late --safe escalation initializes these lazily in Prepare.
	if p.bootHangable && p.mode != ModeOff {
		p.ensurePaths()
	}
	return p, nil
}

func (p *Policy) ensurePaths() {
	p.pathsOnce.Do(func() {
		// Classify every candidate against the snapshot before touching it; each
		// touch is additionally bounded against symlink-hidden remote targets.
		p.fallback = p.localFallbackDirs()
		p.safeDir = p.resolveSafeDir(p.safeDirCfg)
	})
}

// resolveSafeDir picks the local-writable substitute-cwd directory: a valid
// override, else the first of a candidate list that is local + writable, else ""
// (no substitution — review F8: never store an unvalidated os.TempDir()).
func (p *Policy) resolveSafeDir(override string) string {
	candidates := []string{strings.TrimSpace(override), os.TempDir(), "/tmp", "/var/tmp"}
	for _, c := range candidates {
		if c == "" || !filepath.IsAbs(c) {
			continue
		}
		if p.validSafeDir(c) {
			return c
		}
	}
	return "" // nothing safe; Prepare then leaves cwd unset rather than substitute a bad dir
}

// validSafeDir reports whether dir is usable as the local substitute cwd: its
// backing mount must not be hangable, and a bounded writability touch must
// succeed. The touch runs behind the wedge-slotted bounded path so even a
// healthy-classified symlink into a dead mount cannot D-hang it.
func (p *Policy) validSafeDir(dir string) bool {
	if p.IsHangablePath(dir) {
		return false
	}
	return p.boundedTouch(func() bool {
		if tmp, err := os.MkdirTemp(dir, ".tether-safe-"); err == nil {
			_ = os.Remove(tmp)
			return true
		}
		return false
	})
}

// boundedTouch runs a (potentially fs-touching) probe under a deadline + wedge
// slot, so a healthy-classified-but-actually-dead path cannot hang startup
// (external review F3). The abandoned goroutine releases its slot on recovery.
func (p *Policy) boundedTouch(fn func() bool) bool {
	if !p.TryAcquireSpawnSlot() {
		return false
	}
	done := make(chan bool, 1)
	go func() {
		ok := fn()
		// Publish only after releasing the shared slot. Otherwise a caller woken
		// by the result can immediately fail its next healthy operation when
		// wedge_ceiling=1.
		p.ReleaseSpawnSlot()
		done <- ok
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(p.probeTimeout):
		return false
	}
}

// localFallbackPATH is the guaranteed-local candidate set appended to a sanitized
// PATH (after per-candidate classification) so resolution always has a local
// place to look.
var localFallbackPATH = []string{"/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"}

// localFallbackDirs returns the fallback PATH candidates that classify local
// (against the already-taken snapshot) AND exist as a directory. A candidate on
// a hangable mount is skipped WITHOUT a Stat, and the Stat itself is bounded, so
// this cannot D-hang startup (external review F3).
func (p *Policy) localFallbackDirs() []string {
	out := make([]string, 0, len(localFallbackPATH))
	for _, d := range localFallbackPATH {
		if p.IsHangablePath(d) {
			continue
		}
		isDir := p.boundedTouch(func() bool {
			if fi, err := os.Stat(d); err == nil && fi.IsDir() {
				return true
			}
			return false
		})
		if isDir {
			out = append(out, d)
		}
	}
	return out
}

// snapshot reads + parses the mount table. Called at New and whenever a
// generation change is detected.
func (p *Policy) snapshot() {
	raw, err := p.mountSrc()
	if err != nil {
		raw = nil // non-Linux / unreadable ⇒ zero mounts ⇒ inert
	}
	p.applyMounts(parseMountinfo(raw), hashBytes(raw))
}

// refreshIfChanged re-snapshots only when the mount table content changed. The
// read is procfs (never hangs); the comparison is a cheap hash. Cheap enough to
// call before each spawn so a freshly-mounted (or unmounted) fs is picked up.
func (p *Policy) refreshIfChanged() {
	raw, err := p.mountSrc()
	if err != nil {
		raw = nil
	}
	h := hashBytes(raw)
	p.mu.Lock()
	same := h == p.gen
	p.mu.Unlock()
	if same {
		return
	}
	p.applyMounts(parseMountinfo(raw), h)
}

// applyMounts installs a new mount snapshot, CARRYING OVER the health verdict +
// in-flight probe of every hangable mount whose effective topmost mountinfo entry
// is unchanged. External review F4: a bare `health = {}` reset on any mountinfo
// change meant an unrelated container/bind mount churn discarded a dead mount's
// single-flight state, so the next spawn launched a fresh permanent probe →
// O(generations) D-state threads under churn. Only changed/removed mounts lose
// their cached health.
//
// This is NOT where a stale healthy verdict expires — that is mountHealthy's T10/T11.
// Three things follow, and each has been gotten wrong at least once:
//   - Do not loosen the signature carry-over to "fix" stale health. That is the
//     F4 regression's exact shape: unrelated bind/container churn would discard a
//     dead mount's single-flight state and launch a fresh permanent probe each
//     generation.
//   - The carried-over pointer keeps its decidedAt, deliberately. Refreshing it
//     here would let unrelated mount churn renew the TTL forever, which on a busy
//     multi-user box is continuous.
//   - Mount-table change is a bad trigger for re-probing anyway: a wedged mount
//     cannot be unmounted, so its mountinfo line is the MOST stable one on the
//     box. The trigger correlates inversely with the failure.
func (p *Policy) applyMounts(mounts []mountEntry, gen string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	hasHangable := false
	for _, m := range mounts {
		if m.kind != kindLocal {
			hasHangable = true
		}
	}
	oldTop := make(map[string]mountEntry, len(p.mounts))
	for _, m := range p.mounts {
		oldTop[m.mountpoint] = m
	}
	newTop := make(map[string]mountEntry, len(mounts))
	for _, m := range mounts {
		newTop[m.mountpoint] = m
	}
	newHealth := make(map[string]*mountHealth, len(p.health))
	for mp, h := range p.health {
		old, oldOK := oldTop[mp]
		cur, curOK := newTop[mp]
		if oldOK && curOK && old.signature == cur.signature && cur.kind == kindRemoteProbe {
			newHealth[mp] = h // unchanged hangable mount ⇒ keep its verdict/probe
		}
	}
	p.mounts = mounts
	p.gen = gen
	p.hasHangable = hasHangable
	p.health = newHealth
}

// hashBytes is an FNV-1a hash rendered as a string. Not cryptographic — only
// used to detect mount-table content changes.
func hashBytes(b []byte) string {
	var h uint64 = 1469598103934665603
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return strconv.FormatUint(h, 16)
}

// parseMountinfo parses /proc/self/mountinfo. Format per proc(5):
//
//	36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=continue
//	(0) (1) (2)  (3)   (4)  (5)        (6..)  (-) (T)  (src)     (opts)
//
// Fields before " - " are variable in count (optional tags), so we split on the
// " - " separator: mountpoint is field 4 (octal-escaped), fstype is the first
// token after " - ". Garbage/truncated lines are skipped, never panic.
func parseMountinfo(raw []byte) []mountEntry {
	if len(raw) == 0 {
		return nil
	}
	var out []mountEntry
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" {
			continue
		}
		sep := strings.Index(line, " - ")
		if sep < 0 {
			continue
		}
		left := strings.Fields(line[:sep])
		right := strings.Fields(line[sep+3:])
		if len(left) < 5 || len(right) < 1 {
			continue
		}
		mp := unescapeOctal(left[4])
		fstype := right[0]
		out = append(out, mountEntry{
			mountpoint: mp,
			fstype:     fstype,
			kind:       classifyFstype(fstype),
			signature:  line,
		})
	}
	return out
}

// unescapeOctal decodes the \NNN octal escapes the kernel uses in mountinfo
// paths (\040 space, \011 tab, \012 newline, \134 backslash).
func unescapeOctal(s string) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// mountForPath returns the longest-prefix mount backing an absolute path, with
// component-boundary awareness so /nfs does not match /nfslocal. Returns the
// zero mountEntry (kindLocal) if nothing matches. For STACKED mounts at the same
// mountpoint (Linux overmount), the LAST entry in mountinfo wins — that is the
// topmost (real backing) mount, so `>=` not `>` (external review F11): a local
// dir over-mounted by a dead NFS classifies as the NFS, not fail-open as local.
func (p *Policy) mountForPath(path string) mountEntry {
	clean := filepath.Clean(path)
	var best mountEntry
	bestLen := -1
	for _, m := range p.mounts {
		if !pathHasPrefix(clean, m.mountpoint) {
			continue
		}
		if len(m.mountpoint) >= bestLen {
			best = m
			bestLen = len(m.mountpoint)
		}
	}
	return best
}

// pathHasPrefix reports whether path is at or under prefix, on a component
// boundary. Both are assumed cleaned.
func pathHasPrefix(path, prefix string) bool {
	if prefix == "/" {
		return true
	}
	if path == prefix {
		return true
	}
	return strings.HasPrefix(path, prefix+"/")
}

// pathOnDeadMount reports whether path's longest-prefix mount is a hangable
// network mount that probes-as unhealthy. Local (incl. autofs, review F10) paths
// return false without any probe.
func (p *Policy) pathOnDeadMount(path string) bool {
	p.mu.Lock()
	m := p.mountForPath(path)
	p.mu.Unlock()
	if m.kind == kindLocal || m.kind == kindAutomount {
		return false
	}
	return !p.mountHealthy(m.mountpoint)
}

// mountHealthy returns the liveness of a probe-able hangable mountpoint:
// single-flight (at most ONE probe goroutine per mountpoint generation), sticky
// for a DEAD verdict (never re-probed — see the budget argument below), and
// self-healing (a late success from the outstanding goroutine flips a dead
// verdict back to healthy).
//
// A HEALTHY verdict is NOT terminal. Before this fix it was, with no TTL and no
// re-validation, so a mount that was alive at first probe and died later was
// trusted for the process lifetime: the whole feature silently degraded to "two
// deadlines and nothing else" while the dead $PATH dir was never dropped
// (gotcha #81; timan107 ran 18 days that way). Two things expire it now:
//
//	T10  invalidateHealthy() — evidence: something actually stalled, so some
//	     cached healthy verdict is provably wrong. Cheap, targeted, self-correcting.
//	T11  the TTL below — backstop for workloads that produce NO evidence at all
//	     (see DefaultHealthTTL).
//
// Both re-arm by REPLACING the *mountHealth (never resetting in place — see the
// two fuses documented on mountHealth), so the fresh generation starts at
// stUnprobed and the ordinary launcher path re-probes it.
//
// D-STATE THREAD BUDGET (the hard constraint this design has to earn). Both bounds
// are per (mountpoint, GENERATION), not per mountpoint for all time — a mountinfo
// signature change legitimately drops the entry and lets a fresh generation probe
// again, which is the pre-existing F4 behaviour and is not changed here:
//   - Instantaneous: a probe is launched only from the !launched branch of a
//     fresh struct, and by INV-2 a healthy entry has no probe in flight when it
//     is replaced ⇒ at most ONE probe goroutine per generation at any instant,
//     exactly as before this change.
//   - Lifetime: a re-probe either returns (goroutine exits, nothing leaked) or
//     times out ⇒ stUnhealthy ⇒ sticky ⇒ never re-probed. Leaking a SECOND
//     abandoned probe within one generation requires first returning to stHealthy,
//     which requires the first abandoned statfs to have returned. ⇒ steady state is
//     ≤1 abandoned probe per live generation.
//
// What this does NOT bound: a mount that is remounted repeatedly while dead would
// get a new generation each time. That is the same exposure the feature has had
// since v0.3.3, and a wedged mount is precisely the one that cannot be unmounted.
//
// Probes deliberately do NOT take a wedge slot: 64 stuck spawns must never be
// able to starve the one probe that would teach the agent its mount died.
func (p *Policy) mountHealthy(mp string) bool {
	p.mu.Lock()
	h := p.health[mp]
	if h == nil {
		h = &mountHealth{}
		p.health[mp] = h
	}
	// Self-heal: drain a late result from the outstanding probe goroutine.
	if h.result != nil && h.state != stHealthy {
		select {
		case ok := <-h.result:
			h.result = nil
			if ok {
				h.state = stHealthy
			} else {
				h.state = stUnhealthy
			}
			// Stamp the self-healed verdict too: without this a mount that just
			// came back would be born already-expired and re-probed immediately.
			h.decidedAt = p.now()
		default:
		}
	}
	// T11 — a healthy verdict older than the TTL is re-armed (new generation) and
	// falls through to the launcher path below. Guarded on stHealthy alone: by
	// INV-2 that already means no probe is in flight, and it is what keeps a DEAD
	// verdict out of here (widening it re-probes dead mounts ⇒ unbounded D-state
	// threads, strictly worse than the bug being fixed).
	if h.state == stHealthy && p.now().Sub(h.decidedAt) >= p.healthTTL {
		h = &mountHealth{}
		p.health[mp] = h
	}
	if h.state == stHealthy {
		p.mu.Unlock()
		return true
	}
	if h.state == stUnhealthy {
		p.mu.Unlock()
		return false // sticky: never re-probe
	}
	// Unprobed: the first caller is the LAUNCHER (single-flight probe); later
	// concurrent callers are JOINERS that wait on the launcher's `done` so they
	// wake the instant the verdict is decided, not a full probeTimeout later (m9).
	isLauncher := !h.launched
	if isLauncher {
		h.launched = true
		h.result = make(chan bool, 1)
		h.done = make(chan struct{})
		ch := h.result
		probe := p.probe
		go func() { ch <- probe(mp) }()
	}
	ch := h.result
	done := h.done
	timeout := p.probeTimeout
	p.mu.Unlock()

	if !isLauncher {
		// Joiner: wake on the launcher's verdict, with a self-timeout fallback in
		// case the launcher path is somehow not progressing.
		select {
		case <-done:
		case <-time.After(timeout):
		}
		p.mu.Lock()
		s := h.state
		p.mu.Unlock()
		return s == stHealthy
	}

	// Launcher: wait for the probe verdict or the deadline, set state, and close
	// this generation's `done` exactly once to release joiners.
	//
	// `h` and `done` are THIS generation's struct/channel, captured above. A T10/T11
	// re-arm may have swapped p.health[mp] to a fresh struct while we waited; writing
	// through the captured `h` then lands on an orphaned struct that nobody reads,
	// which is exactly right — a stale launcher must not be able to stamp its verdict
	// onto the new generation. Closing the captured `done` (NOT h.done, which used to
	// be read as a field here) is what makes that safe: re-arming can never make this
	// close hit a channel some other generation already closed.
	//
	// Cost, accepted: a joiner already parked on the old generation gets one verdict
	// from the pre-invalidation epoch — at most one spawn decided on stale data. The
	// alternative (re-entering mountHealthy) risks lock re-entrancy, which is worse.
	select {
	case ok := <-ch:
		p.mu.Lock()
		if h.result == ch {
			h.result = nil
		}
		if ok {
			h.state = stHealthy
		} else {
			h.state = stUnhealthy
		}
		h.decidedAt = p.now()
		s := h.state
		close(done)
		p.mu.Unlock()
		return s == stHealthy
	case <-time.After(timeout):
		p.mu.Lock()
		if h.state == stUnprobed {
			h.state = stUnhealthy // mark dead; retain h.result for self-heal
			h.decidedAt = p.now()
		}
		s := h.state
		close(done)
		p.mu.Unlock()
		return s == stHealthy
	}
}

// invalidateHealthy expires every cached HEALTHY verdict so the next consult
// re-probes. It is the evidence-driven half of the #81 fix: a spawn that stalled
// past its deadline IS the proof that some cached "this mount is fine" is stale,
// because a live mount does not wedge a stat or an execve.
//
// GLOBAL, not per-mount, and that is a deliberate trade rather than an oversight: the
// evidence says "something stalled", and the stall carries no way to say WHICH mount
// did it — a wedged resolution walked several $PATH dirs, and a wedged execve names a
// path we already believed was fine. Expiring one healthy verdict is O(1) memory and
// costs at most one extra statfs the next time that mount is consulted (microseconds
// when it is alive), so over-expiring is cheap while under-expiring is the bug.
//
// Deliberately narrow, and each clause is load-bearing:
//   - Only stHealthy entries are touched. stUnhealthy is sticky (re-probing a dead
//     mount leaks another permanent D-state thread) and stUnprobed has nothing to
//     expire.
//   - No probe is launched here. This is O(#mounts) pure memory under p.mu, which
//     is why it is safe to call from a watchdog's timeout branch.
//
// Call sites, and what actually pins each — the architecture gate covers the two that
// mint a spawn-timeout sentinel and nothing else, so do not read it as covering all
// four:
//   - boundedResolveInDirs, timeout branch — argv[0] resolution wedged.
//     Pinned by the gate (mint site) + TestPrepare_staleHealthyMountDroppedAfterSpawnTimeout.
//   - RunStartWithCleanup, timeout branch — execve wedged (the only execve watchdog
//     in the process after exec.go's duplicate collapsed into it).
//     Pinned by the gate (mint site) + TestPrepare_wedgeCeilingSaturatedStillDropsDeadPathDirs.
//   - Prepare, when the caller passed --safe — the documented manual override.
//     Pinned by TestPrepare_safeForcesRevalidation (that it happens) and
//     TestPrepare_safeInvalidatesBeforeCwdCheck (that it happens BEFORE the cwd check).
//     Not visible to the gate: no sentinel is minted here.
//   - Agent.boundedHomeRead's timeout — a wedged state.json read is the same evidence,
//     and the only trigger that fires with nobody running a command.
//     Pinned by TestBoundedHomeRead_stallExpiresCachedMountHealth in internal/agent.
//     Not visible to the gate either: that branch returns (nil,false) and mints nothing.
//
// MUST NOT be called while p.mu is held — notably from mountHealthy, whose timeout
// branch takes p.mu immediately after. p.mu is not reentrant, so such a call deadlocks
// every spawn on the agent. The gate checks lock domination (not just the function
// name), because extracting a helper defeated the name check while still deadlocking.
func (p *Policy) invalidateHealthy() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for mp, h := range p.health {
		if h != nil && h.state == stHealthy {
			p.health[mp] = &mountHealth{}
		}
	}
}

// InvalidateHealthy is invalidateHealthy for callers outside the package (the
// agent's own bounded-read watchdog). See invalidateHealthy for the contract.
func (p *Policy) InvalidateHealthy() { p.invalidateHealthy() }

// Decision is the result of Prepare.
//
//   - Active=false ⇒ the machine booted with no hangable mount (and was not
//     --safe-escalated): the caller uses the legacy exec.Command path UNCHANGED.
//   - Active=true ⇒ the caller ALWAYS builds &exec.Cmd{Path: Decision.Path, Args:
//     argv} (never exec.Command, whose LookPath would D-hang on the agent PATH —
//     review F1/F2) and runs the start under the spawn watchdog. When Outage is
//     false (a hangable mount is present but nothing is dead), the caller keeps
//     the LEGACY env/cwd so the child is byte-identical to before the feature
//     (only the resolution + execve are now bounded). When Outage is true, the
//     caller uses Env/Cwd and emits Warn.
type Decision struct {
	Active bool
	Path   string // resolved absolute argv[0]
	Outage bool   // a dead $PATH dir was dropped ⇒ use Env/Cwd/Warn, else legacy env/cwd
	Env    []string
	Cwd    string
	Warn   string
}

// FSError is a fail-fast spawn rejection; Code is one of the wire reason values
// (remote_fs_unhealthy / remote_fs_unsafe_cwd / remote_fs_not_found).
type FSError struct {
	Code   string
	Detail string
}

func (e *FSError) Error() string {
	if e.Detail == "" {
		return e.Code
	}
	return e.Code + ": " + e.Detail
}

// Reason codes (also the RunChunk.Reason / ExecChunk.Error values).
const (
	ReasonUnhealthy     = "remote_fs_unhealthy"
	ReasonUnsafeCwd     = "remote_fs_unsafe_cwd"
	ReasonNotFound      = "remote_fs_not_found"
	ReasonSpawnTimeout  = "remote_fs_spawn_timeout"
	ReasonTooManyWedged = "too_many_wedged_spawns"
)

// ErrSpawnTimeout / ErrTooManyWedged are returned by RunStart.
var (
	ErrSpawnTimeout  = errors.New(ReasonSpawnTimeout)
	ErrTooManyWedged = errors.New(ReasonTooManyWedged)
)

// Prepare decides how to spawn argv on a node whose network FS may be wedged.
//
//   - lookupPATH is the PATH that exec.Command's LookPath WOULD walk — i.e. the
//     AGENT process PATH — because that, not the child env's PATH, is where the
//     legacy spawn D-hangs (external review F2). Prepare resolves argv[0] against
//     a sanitized copy of it.
//   - childEnv is the env the child would run with (used only on an outage, to
//     hand back a sanitized + PWD-corrected env).
//
// On a machine that booted with no hangable mount (and is not --safe-escalated)
// Prepare returns Decision{Active:false} and the caller uses the legacy
// exec.Command path unchanged. Otherwise it ALWAYS self-resolves argv[0] to an
// absolute Path (so the caller bypasses LookPath) and bounds the resolution I/O,
// so neither a poisoned PATH nor a symlink-hidden dead mount can unbounded-hang
// (review F1). When no dead dir is dropped, Decision.Outage is false and the
// caller keeps the legacy env/cwd (byte-identical child); when a dead dir is
// dropped, Outage is true and the caller uses Env/Cwd/Warn. An *FSError is a
// fail-fast rejection for a lexically-known-dead argv[0]/cwd.
func (p *Policy) Prepare(argv []string, cwd, lookupPATH string, childEnv []string, requestedSafe bool) (Decision, error) {
	if len(argv) == 0 {
		return Decision{}, nil
	}
	if p.mode == ModeOff && !requestedSafe {
		return Decision{}, nil
	}
	if !p.bootHangable && !requestedSafe {
		// Booted local AND not explicitly escalated ⇒ inert with zero per-spawn
		// syscalls (review m7). An explicit --safe bypasses this so a network
		// mount added AFTER boot is still detected (review F7).
		return Decision{}, nil
	}
	p.refreshIfChanged()
	if requestedSafe {
		// --safe is documented (usage.md §7.7) as the manual override for a wedged
		// mount. Refreshing the mount TABLE was never enough: on timan107 the table
		// was already correct and it was the cached HEALTHY verdict that was wrong,
		// so --safe behaved identically to no flag at all. Expire them here, before
		// the cwd/argv[0] checks below consult any of them.
		p.invalidateHealthy()
	}
	p.mu.Lock()
	hangable := p.hasHangable
	p.mu.Unlock()
	if !hangable {
		return Decision{}, nil // no hangable mount (now) ⇒ inert
	}
	p.ensurePaths()

	// Lexical fail-fast for a KNOWN-dead cwd / explicit argv[0] (string-only, no
	// I/O). A symlink-hidden or relative dead target is NOT caught here but is
	// bounded by the execve watchdog / bounded resolution below, never unbounded.
	if cwd != "" && p.pathOnDeadMount(cwd) {
		return Decision{}, &FSError{Code: ReasonUnsafeCwd, Detail: cwd}
	}
	name := argv[0]
	explicit := strings.ContainsRune(name, '/')
	if explicit && p.pathOnDeadMount(name) {
		return Decision{}, &FSError{Code: ReasonUnhealthy, Detail: name}
	}

	// Sanitize the LOOKUP path (the agent PATH LookPath would walk — review F2).
	kept, dropped := p.sanitizePATH(lookupPATH)
	outage := len(dropped) > 0

	// Resolve argv[0] to an absolute path. Always self-resolve (no LookPath); the
	// I/O is bounded (review F1). On an outage, also search the local fallback
	// dirs so a binary whose only home was a dropped dead dir can still be found.
	var abs string
	if explicit {
		abs = name // verbatim; a symlink/relative dead target is bounded by the watchdog
	} else {
		dirs := kept
		if outage {
			dirs = appendFallback(kept, p.fallback)
		}
		r, err := p.boundedResolveInDirs(name, dirs)
		if err != nil {
			return Decision{}, err
		}
		abs = r
	}

	d := Decision{Active: true, Path: abs, Outage: outage}
	if outage {
		cwdOut := cwd
		if cwdOut == "" && p.safeDir != "" {
			cwdOut = p.safeDir
		}
		// Hand the child a sanitized PATH (best-effort for its own sub-lookups)
		// and inject PWD (os/exec only injects it when Env==nil — review M3).
		childKept, _ := p.sanitizePATH(envGet(childEnv, "PATH"))
		env := envWith(childEnv, "PATH", strings.Join(appendFallback(childKept, p.fallback), string(os.PathListSeparator)))
		if cwdOut != "" {
			env = envWith(env, "PWD", cwdOut)
		}
		d.Env = env
		d.Cwd = cwdOut
		d.Warn = fmt.Sprintf("[tether agent] remote-fs: dropped %d unresponsive network $PATH dir(s) [%s]; safe mode active",
			len(dropped), strings.Join(dropped, ", "))
	}
	return d, nil
}

// sanitizePATH drops PATH entries whose backing mount is hangable-and-dead.
// Returns (keptDirs, droppedDirs) with duplicates removed from kept. It does NOT
// append the fallback dirs — that is done only on an outage (so a healthy spawn's
// resolution is over the verbatim PATH, byte-identical to LookPath; review M2).
func (p *Policy) sanitizePATH(raw string) (kept, dropped []string) {
	seen := map[string]bool{}
	for _, dir := range filepath.SplitList(raw) {
		if dir == "" {
			// Match Unix LookPath semantics: an empty PATH element means ".".
			// boundedResolveInDirs retains Go's ErrDot rejection for the default
			// secure behavior.
			dir = "."
		}
		if p.pathOnDeadMount(dir) {
			dropped = append(dropped, dir)
			continue
		}
		if !seen[dir] {
			seen[dir] = true
			kept = append(kept, dir)
		}
	}
	return kept, dropped
}

// appendFallback returns dirs with the local fallback dirs appended (deduped).
func appendFallback(dirs, fallback []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(dirs)+len(fallback))
	for _, d := range dirs {
		if !seen[d] {
			seen[d] = true
			out = append(out, d)
		}
	}
	for _, d := range fallback {
		if !seen[d] {
			seen[d] = true
			out = append(out, d)
		}
	}
	return out
}

// boundedResolveInDirs runs resolveInDirs under a deadline + a wedge slot, so a
// symlink in an otherwise-local dir that points into a dead mount (which
// os.Stat would follow and D-hang on) is abandoned rather than hanging the
// handler unbounded (external review F1). On timeout it returns a fail-fast
// remote_fs_spawn_timeout; the abandoned stat releases its slot on fs recovery.
func (p *Policy) boundedResolveInDirs(name string, dirs []string) (string, error) {
	if !p.TryAcquireSpawnSlot() {
		return "", &FSError{Code: ReasonTooManyWedged}
	}
	type res struct {
		path string
		ok   bool
	}
	resolve := p.resolver
	if resolve == nil {
		resolve = resolveInDirs
	}
	ch := make(chan res, 1)
	go func() {
		r, ok := resolve(name, dirs)
		p.ReleaseSpawnSlot()
		ch <- res{r, ok}
	}()
	select {
	case r := <-ch:
		if !r.ok {
			return "", &FSError{Code: ReasonNotFound, Detail: name}
		}
		if !filepath.IsAbs(r.path) {
			return "", fmt.Errorf("%w: %s", exec.ErrDot, r.path)
		}
		return r.path, nil
	case <-time.After(p.probeTimeout):
		// Evidence: resolution wedged, so some dir we walked is on a mount whose
		// cached healthy verdict is stale. Expire them before returning, so the
		// NEXT command re-probes and drops the dead dir instead of repeating this
		// timeout forever (gotcha #81).
		p.invalidateHealthy()
		return "", &FSError{Code: ReasonSpawnTimeout, Detail: name}
	}
}

// resolveInDirs finds name as an executable regular file in dirs, in order,
// mirroring exec.LookPath's executable check. Runs inside boundedResolveInDirs.
func resolveInDirs(name string, dirs []string) (string, bool) {
	for _, dir := range dirs {
		cand := filepath.Join(dir, name)
		if isExecutable(cand) {
			return cand, true
		}
	}
	return "", false
}

func isExecutable(path string) bool {
	fi, err := os.Stat(path)
	if err != nil || fi.IsDir() {
		return false
	}
	err = syscall.Access(path, 1) // X_OK
	if err == nil {
		return true
	}
	// Match os/exec's permission-aware check closely enough for a non-setuid
	// agent. Otherwise ACL/identity denial must not be mistaken for an executable
	// candidate.
	if !errors.Is(err, syscall.ENOSYS) && !errors.Is(err, syscall.EPERM) {
		return false
	}
	return fi.Mode()&0o111 != 0
}

// SafeDir returns the validated local safe directory, initializing the bounded
// candidate scan on first explicit inspection if the policy was otherwise inert.
func (p *Policy) SafeDir() string {
	p.ensurePaths()
	return p.safeDir
}

// ProbeTimeout returns the configured bounded-probe deadline. The agent reuses
// it as the deadline for bounding its own Home reads (Component I).
func (p *Policy) ProbeTimeout() time.Duration { return p.probeTimeout }

// IsHangablePath reports whether a path is backed by ANY hangable mount
// (regardless of current health). Used at New() to decide whether to guard the
// agent's Home reads at all.
func (p *Policy) IsHangablePath(path string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	m := p.mountForPath(path)
	return m.kind != kindLocal
}

// RunStart runs start (a cmd.Start) under a spawn-window deadline. On timeout it
// ABANDONS the goroutine (a D-state child ignores SIGKILL, so killing is
// pointless) and returns ErrSpawnTimeout; the abandoned goroutine self-reaps the
// wedged counter if start ever returns after the fs recovers. Past wedgeCeiling
// concurrent abandoned spawns it fails fast with ErrTooManyWedged so a retrying
// ctl loop cannot exhaust the agent's fds/threads.
func (p *Policy) RunStart(start func() error, timeout time.Duration) error {
	return p.RunStartWithCleanup(start, timeout, nil, nil)
}

// RunStartWithCleanup is RunStart plus ownership hooks for resources that live
// outside the generic policy (the PTY Session). onAbandon runs synchronously
// when the deadline wins; reapOnReturn runs after the abandoned start eventually
// returns and before the wedge slot is released.
func (p *Policy) RunStartWithCleanup(
	start func() error,
	timeout time.Duration,
	onAbandon func(),
	reapOnReturn func(error),
) error {
	if !p.TryAcquireSpawnSlot() {
		return ErrTooManyWedged
	}
	done := make(chan error, 1)
	go func() { done <- start() }()

	select {
	case err := <-done:
		p.ReleaseSpawnSlot()
		return err
	case <-time.After(timeout):
		// Evidence: the execve itself wedged (argv[0] or cwd is on a mount we still
		// believe is healthy). Expire cached healthy verdicts BEFORE onAbandon so the
		// next spawn re-probes — see invalidateHealthy.
		p.invalidateHealthy()
		if onAbandon != nil {
			onAbandon()
		}
		// Abandon. Free the slot only when start eventually returns (on fs recovery).
		go func() {
			err := <-done
			if reapOnReturn != nil {
				reapOnReturn(err)
			}
			p.ReleaseSpawnSlot()
		}()
		return ErrSpawnTimeout
	}
}

// TryAcquireSpawnSlot reserves one of the bounded concurrent-spawn slots,
// returning false at the wedge ceiling. Pair every true with exactly one
// ReleaseSpawnSlot — immediately on an in-time start, or from the reaper when an
// abandoned start finally returns (so a recovered mount frees the slot).
//
// Callers that must clean up resources on abandon (close exec pipes, Wait the
// child) use RunStartWithCleanup, NOT these primitives directly. The older advice
// here — "manage your own goroutine, RunStart cannot close a caller's
// StdoutPipe/StderrPipe" — predates RunStartWithCleanup and is what led the agent
// to grow a second, line-for-line copy of the watchdog; that copy is exactly where
// gotcha #81's invalidation hook would have been forgotten. These primitives are
// now only for callers doing something neither watchdog models (boundedTouch).
func (p *Policy) TryAcquireSpawnSlot() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.wedged >= p.wedgeCeiling {
		return false
	}
	p.wedged++
	return true
}

// ReleaseSpawnSlot frees a slot acquired by TryAcquireSpawnSlot.
func (p *Policy) ReleaseSpawnSlot() {
	p.mu.Lock()
	if p.wedged > 0 {
		p.wedged--
	}
	p.mu.Unlock()
}

// WedgedCount returns the current number of abandoned spawns (for tests/metrics).
func (p *Policy) WedgedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.wedged
}

// envGet returns the value of key in an os/exec-style env slice ("K=V"), or "".
// The last occurrence wins, matching exec semantics.
func envGet(env []string, key string) string {
	pfx := key + "="
	val := ""
	for _, e := range env {
		if strings.HasPrefix(e, pfx) {
			val = e[len(pfx):]
		}
	}
	return val
}

// envWith returns env with key's entry replaced (or appended). Drops any
// duplicate occurrences of key. The original slice is not mutated.
func envWith(env []string, key, val string) []string {
	pfx := key + "="
	out := make([]string, 0, len(env)+1)
	replaced := false
	for _, e := range env {
		if strings.HasPrefix(e, pfx) {
			if !replaced {
				out = append(out, pfx+val)
				replaced = true
			}
			continue
		}
		out = append(out, e)
	}
	if !replaced {
		out = append(out, pfx+val)
	}
	return out
}
