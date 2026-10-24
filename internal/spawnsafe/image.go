// image.go — the spawn-safety half of an agent image (hot-update S2).
//
// What crosses a same-PID exec is the VERDICTS: which hangable mounts this
// process already decided are healthy or dead, and when. What must not
// cross is anything with a goroutine behind it — an abandoned spawn helper
// (wedged slot) or a probe still in D-state — because a goroutine does not
// survive exec and a verdict that still depends on one would be a verdict
// about a thread that no longer exists. So Export refuses while any spawn
// or probe is in flight (the deferral reasons the image layer reports as
// spawn_in_flight / remote_fs_probe_in_flight), and Import installs only
// verdicts whose mount is still the same mount instance.
// origin: docs/reviews/hot-update-agent-image-abi-plan.md §3.6.
package spawnsafe

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"time"
)

// StateVersion is the schema epoch of State.
const StateVersion = 1

// maxVerdicts bounds an imported verdict list (a mount table is a few
// hundred lines on the busiest box).
const maxVerdicts = 4096

var (
	// ErrSpawnInFlight: a spawn/lookup/touch is abandoned or still in flight.
	ErrSpawnInFlight = errors.New("spawnsafe: spawn in flight")
	// ErrProbeInFlight: a mount probe goroutine has not returned — including
	// one the timeout already gave up on (the sticky-dead retained result).
	ErrProbeInFlight = errors.New("spawnsafe: mount probe in flight")
	// errImportNotFresh: Import into a policy that already decided something.
	// A package-level value rather than an errors.New under p.mu: the
	// spawn-stall gate's lock-domination check follows callee NAMES, and
	// "New" is in its invalidating closure (spawnsafe.New reaches the
	// invalidation), so an errors.New inside the critical section reads as a
	// deadlock to it.
	errImportNotFresh = errors.New("spawnsafe: import requires a policy that has decided nothing")
)

// State is the exported verdict table.
type State struct {
	Version  int            `json:"version"`
	Gen      string         `json:"gen"`
	Verdicts []MountVerdict `json:"verdicts,omitempty"`
}

// MountVerdict is one decided mount: its mountpoint, the mountinfo line
// signature the verdict was taken against (a remount or instance
// replacement invalidates it), the verdict and its instant.
type MountVerdict struct {
	Mountpoint        string `json:"mountpoint"`
	Signature         string `json:"signature"`
	Healthy           bool   `json:"healthy"`
	DecidedAtUnixNano int64  `json:"decided_at_unix_nano"`
}

// Census is what an image layer asks before it freezes anything. Wedged is
// abandoned reaper ownership; InFlight is still within the bounded window.
// Preflight defers only on Wedged, then the cut quiesces Runtime creations
// before Policy.Freeze closes slot admission and drains InFlight.
// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT (drill 36.A).
// ProbesRetained counts the in-flight probes a timed-out (sticky-dead)
// verdict still waits on: they are part of ProbesInFlight, and the one
// shape that never clears without the probe returning or a daemon restart
// — the operator's deferral detail says so.
type Census struct {
	Wedged         int
	InFlight       int
	ProbesInFlight int
	ProbesRetained int
}

// Census reports the in-flight counts without waiting.
func (p *Policy) Census() Census {
	p.mu.Lock()
	defer p.mu.Unlock()
	retained := 0
	for _, h := range p.health {
		if h != nil && h.result != nil && h.state == stUnhealthy {
			retained++
		}
	}
	return Census{Wedged: p.wedged, InFlight: p.inFlight, ProbesInFlight: p.probesInFlight, ProbesRetained: retained}
}

// ImportReady reports whether Import would be accepted: a policy that has
// decided nothing and has nothing in flight. It is the pre-check an image
// importer asks before installing any domain, so that Import itself cannot
// refuse after other domains are installed.
func (p *Policy) ImportReady() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.health) != 0 || p.wedged != 0 || p.inFlight != 0 || p.spawnCut != nil || p.probesInFlight != 0 {
		return errImportNotFresh
	}
	return nil
}

// Export returns the decided verdicts, or ErrSpawnInFlight/ErrProbeInFlight
// while anything with a goroutine behind it exists. Unprobed entries carry
// nothing and are skipped; a dead verdict whose probe already returned
// false is exported as unhealthy (sticky); a dead verdict whose probe is
// still out is exactly the ErrProbeInFlight case.
func (p *Policy) Export() (State, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.wedged > 0 || p.inFlight > 0 {
		return State{}, fmt.Errorf("%w: abandoned=%d, in flight=%d", ErrSpawnInFlight, p.wedged, p.inFlight)
	}
	if p.probesInFlight > 0 {
		return State{}, fmt.Errorf("%w: %d probe goroutine(s) not returned", ErrProbeInFlight, p.probesInFlight)
	}
	top := p.topmostLocked()
	st := State{Version: StateVersion, Gen: p.gen}
	for mp, h := range p.health {
		var healthy bool
		switch h.state {
		case stUnprobed:
			continue // nothing decided; the next image probes afresh
		case stHealthy:
			healthy = true
		case stUnhealthy:
			healthy = false
		}
		entry, ok := top[mp]
		if !ok || entry.kind != kindRemoteProbe {
			continue // applyMounts would have dropped it; do not resurrect it
		}
		st.Verdicts = append(st.Verdicts, MountVerdict{Mountpoint: mp, Signature: entry.signature,
			Healthy: healthy, DecidedAtUnixNano: h.decidedAt.UnixNano()})
	}
	sort.Slice(st.Verdicts, func(i, j int) bool { return st.Verdicts[i].Mountpoint < st.Verdicts[j].Mountpoint })
	return st, nil
}

// Validate is the pure half of Import: shape only, no policy state.
func (st State) Validate() error {
	if st.Version != StateVersion {
		return fmt.Errorf("spawnsafe: state version %d, want %d", st.Version, StateVersion)
	}
	if len(st.Verdicts) > maxVerdicts {
		return fmt.Errorf("spawnsafe: %d verdicts exceed %d", len(st.Verdicts), maxVerdicts)
	}
	seen := make(map[string]bool, len(st.Verdicts))
	for _, v := range st.Verdicts {
		if v.Mountpoint == "" || !filepath.IsAbs(v.Mountpoint) || seen[v.Mountpoint] || v.Signature == "" || v.DecidedAtUnixNano <= 0 {
			return fmt.Errorf("spawnsafe: invalid or duplicate verdict for %q", v.Mountpoint)
		}
		seen[v.Mountpoint] = true
	}
	return nil
}

// Import installs exported verdicts into a policy that has decided nothing
// yet (fresh from New) and returns how many it installed. A verdict is
// installed only for a mount that is still the same instance (same
// mountpoint, same mountinfo line signature, still a probed kind) — exactly
// applyMounts' carry-over rule, applied per mount. The exported Gen (the
// hash of the WHOLE mount table) is not a gate: one unrelated bind or
// container mount since the exporter's last spawn changes it, which on a
// busy box is the norm, and a whole-table gate would silently drop every
// verdict on every hot update and re-probe every dead mount (the F4
// regression shape). An installed verdict keeps its original instant so the
// health TTL continues from the real decision, and has no probe behind it
// (INV-2 holds by construction: result is nil).
// origin: S2 internal review I3.
func (p *Policy) Import(st State) (int, error) {
	if err := st.Validate(); err != nil {
		return 0, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.health) != 0 || p.wedged != 0 || p.inFlight != 0 || p.spawnCut != nil || p.probesInFlight != 0 {
		return 0, errImportNotFresh
	}
	top := p.topmostLocked()
	installed := 0
	for _, v := range st.Verdicts {
		entry, ok := top[v.Mountpoint]
		if !ok || entry.signature != v.Signature || entry.kind != kindRemoteProbe {
			continue
		}
		h := &mountHealth{decidedAt: time.Unix(0, v.DecidedAtUnixNano)}
		if v.Healthy {
			h.state = stHealthy
		} else {
			h.state = stUnhealthy
		}
		p.health[v.Mountpoint] = h
		installed++
	}
	return installed, nil
}

// topmostLocked mirrors applyMounts' "last line wins" map of mountpoints.
func (p *Policy) topmostLocked() map[string]mountEntry {
	top := make(map[string]mountEntry, len(p.mounts))
	for _, m := range p.mounts {
		top[m.mountpoint] = m
	}
	return top
}
