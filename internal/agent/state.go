// Per-session agent state persisted to disk so the tunnel client can re-establish
// proxies on agent restart without re-running `tether expose`.
//
// Architecture I.2 / K.1 / F.6: agent owns the raw tunnel tokens (broker
// only keeps SHA256), so agent must persist them or restart loses
// every proxy. Path: ~/.tether/agent/<sid>/state.json, mode 0600 in
// a 0700 dir, atomic tmp+fsync+rename writes.
//
// Schema is intentionally tiny — for v1 we only need the port_tokens
// table the architecture mentions. Future fields (last_known_node_id,
// reconcile checkpoints, etc.) get added as new optional JSON fields
// so old state files keep loading.
package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// PortToken is one persisted expose row. Mirrors what the broker
// forwarded in proto.ExposeForwardedReq, minus the actor fp (agent
// doesn't need to re-present that on restart).
type PortToken struct {
	Name      string `json:"name"`
	Port      int    `json:"port"`       // public port (14000-14999)
	LocalPort int    `json:"local_port"` // agent-side port being exposed
	Token     string `json:"token"`      // raw tunnel auth token
}

// ProxyState (P13) persists the embedded SS proxy's tunnel footprint so a
// restart re-establishes the SAME public port + tunnel. The SS PSKs are NEVER
// persisted (re-delivered by the broker on every (re)register / push); only
// the tunnel token + last-applied epoch live here. Pointer/omitempty so a
// pre-P13 state.json still loads and a proxy-off agent writes no proxy key.
type ProxyState struct {
	PublicPort int    `json:"public_port"`
	LocalPort  int    `json:"local_port"`
	Token      string `json:"token"`
	Epoch      int64  `json:"epoch"`
}

// StateFile is the on-disk shape. Writers serialize through stateMu
// in the agent so concurrent expose / expose-rm don't trample each
// other's writes.
type StateFile struct {
	PortTokens []PortToken `json:"port_tokens"`
	Proxy      *ProxyState `json:"proxy,omitempty"`
}

type stateStore struct {
	mu   sync.Mutex
	path string
}

// newStateStore returns a store rooted at home/agent/<sid>/state.json.
// The file is not touched until the first AddPort / RemovePort call.
func newStateStore(home, sid string) *stateStore {
	return &stateStore{
		path: filepath.Join(home, "agent", sid, "state.json"),
	}
}

// load reads the current state.json, returning an empty StateFile if
// the file doesn't exist yet.
func (s *stateStore) load() (*StateFile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked()
}

func (s *stateStore) loadLocked() (*StateFile, error) {
	return parseStateBytes(os.ReadFile(s.path))
}

// loadNoLock reads state.json WITHOUT taking s.mu. Writes are atomic
// (tmp+fsync+rename, saveLocked), so a lock-free reader always sees a complete
// file — never a torn one. Used by the agent's hung-fs-bounded read path
// (Component I, agent.loadStateBounded): if os.ReadFile D-hangs on a wedged
// network Home, the abandoned reader holds NO mutex, so AddPort/RemovePort/
// SetProxy/load are not poisoned (review B1). It is read-only and eventually
// consistent — exactly what a register snapshot needs.
func (s *stateStore) loadNoLock() (*StateFile, error) {
	return parseStateBytes(os.ReadFile(s.path))
}

func parseStateBytes(b []byte, err error) (*StateFile, error) {
	if errors.Is(err, os.ErrNotExist) {
		return &StateFile{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("agent state: read: %w", err)
	}
	var sf StateFile
	if err := json.Unmarshal(b, &sf); err != nil {
		return nil, fmt.Errorf("agent state: parse: %w", err)
	}
	return &sf, nil
}

// AddPort inserts (or updates by Name) a PortToken row and writes
// the file atomically. Update-by-name lets a re-exposed name pick up
// the new (port, token) without leaving an orphan entry.
func (s *stateStore) AddPort(p PortToken) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sf, err := s.loadLocked()
	if err != nil {
		return err
	}
	replaced := false
	for i, existing := range sf.PortTokens {
		if existing.Name == p.Name {
			sf.PortTokens[i] = p
			replaced = true
			break
		}
	}
	if !replaced {
		sf.PortTokens = append(sf.PortTokens, p)
	}
	return s.saveLocked(sf)
}

// RemovePort drops the entry matching name (no-op if absent). Used by
// expose-rm and by the reconciler when broker tells us a port was
// REVOKED out from under us.
func (s *stateStore) RemovePort(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sf, err := s.loadLocked()
	if err != nil {
		return err
	}
	out := sf.PortTokens[:0]
	for _, p := range sf.PortTokens {
		if p.Name != name {
			out = append(out, p)
		}
	}
	sf.PortTokens = out
	return s.saveLocked(sf)
}

// SetProxy upserts the proxy footprint (nil clears it). Atomic write.
func (s *stateStore) SetProxy(p *ProxyState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sf, err := s.loadLocked()
	if err != nil {
		return err
	}
	sf.Proxy = p
	return s.saveLocked(sf)
}

// GetProxy returns the persisted proxy footprint, or nil if none.
func (s *stateStore) GetProxy() (*ProxyState, error) {
	sf, err := s.load()
	if err != nil {
		return nil, err
	}
	return sf.Proxy, nil
}

// saveLocked writes the StateFile via tmp+fsync+rename atomic replace
// (architecture I.2 — the fsync matters: state.json holds the only copy
// of the raw tunnel tokens, so a post-rename crash must not surface an
// empty or partial file). 0700 dir, 0600 file per K.1 permission rules.
func (s *stateStore) saveLocked(sf *StateFile) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("agent state: mkdir: %w", err)
	}
	body, err := json.MarshalIndent(sf, "", "  ")
	if err != nil {
		return fmt.Errorf("agent state: marshal: %w", err)
	}
	f, err := os.CreateTemp(filepath.Dir(s.path), "."+filepath.Base(s.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("agent state: open tmp: %w", err)
	}
	tmp := f.Name()
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("agent state: chmod tmp: %w", err)
	}
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("agent state: write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("agent state: fsync tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("agent state: close tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("agent state: rename: %w", err)
	}
	if err := syncStateParentDir(s.path); err != nil {
		return err
	}
	return nil
}

func syncStateParentDir(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("agent state: open parent directory: %w", err)
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("agent state: fsync parent directory: %w", err)
	}
	return nil
}
