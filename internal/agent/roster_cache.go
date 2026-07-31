package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// roster_cache.go (C2) — the agent's signed-roster + seed-bundle discovery cache, persisted in its OWN
// file (<home>/agent/<sid>/roster_cache.json), SEPARATE from the daemon-owned state.json. The split is
// load-bearing: the CLI (`agent config refresh --once`, `agent join`) AND the daemon both write the
// cache, while ONLY the daemon writes state.json's port-tokens/proxy — co-mingling them would risk a
// lost-update of the only durable copy of tunnel tokens. Monotone-gating (AdoptDecision) + atomic
// writes (tmp+fsync+rename, 0600) + continuous NATS refresh make a concurrent cross-process cache
// write self-healing (v1; no flock).

type rosterCacheStore struct {
	mu   sync.Mutex
	path string
}

func newRosterCacheStore(home, sid string) *rosterCacheStore {
	return &rosterCacheStore{path: filepath.Join(home, "agent", sid, "roster_cache.json")}
}

func (s *rosterCacheStore) load() (*RosterCache, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("agent roster cache: read: %w", err)
	}
	var rc RosterCache
	if err := json.Unmarshal(b, &rc); err != nil {
		return nil, fmt.Errorf("agent roster cache: parse: %w", err)
	}
	return &rc, nil
}

func (s *rosterCacheStore) save(rc *RosterCache) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("agent roster cache: mkdir: %w", err)
	}
	body, err := json.MarshalIndent(rc, "", "  ")
	if err != nil {
		return fmt.Errorf("agent roster cache: marshal: %w", err)
	}
	f, err := os.CreateTemp(filepath.Dir(s.path), "."+filepath.Base(s.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("agent roster cache: open tmp: %w", err)
	}
	tmp := f.Name()
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("agent roster cache: chmod: %w", err)
	}
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("agent roster cache: write: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("agent roster cache: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("agent roster cache: close: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("agent roster cache: rename: %w", err)
	}
	return syncStateParentDir(s.path)
}

// ReadRosterCache returns the persisted discovery cache for (home,sid), migrating ONCE from a legacy
// C1 state.json Roster field if roster_cache.json does not exist yet. nil (no error) when neither
// exists. Exported so cmd/tether (a different package) can read/refresh the cache without touching the
// unexported stateStore.
func ReadRosterCache(home, sid string) (*RosterCache, error) {
	rc, err := newRosterCacheStore(home, sid).load()
	if err != nil {
		return nil, err
	}
	if rc != nil {
		return rc, nil
	}
	// Migration: a C1 agent stored the cache inside state.json.Roster.
	sf, serr := newStateStore(home, sid).load()
	//nolint:nilerr // Deliberate, and the doc comment above already promises it: "nil (no error) when
	// neither exists". This is a DISCOVERY CACHE and the migration read is best-effort — an unreadable
	// or corrupt state.json here means the agent re-discovers the roster, which is the same work it
	// does on a first run. Surfacing the error instead would turn a stale scratch file into a hard
	// failure of a call whose entire contract is "give me the cache if there is one".
	if serr != nil || sf == nil || sf.Roster == nil {
		return nil, nil
	}
	return sf.Roster, nil
}

// WriteRosterCache atomically persists the discovery cache (CLI + daemon both call it).
func WriteRosterCache(home, sid string, rc *RosterCache) error {
	return newRosterCacheStore(home, sid).save(rc)
}
