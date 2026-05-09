// Per-session agent state persisted to disk so frpc can re-establish
// proxies on agent restart without re-running `tether expose`.
//
// Architecture I.2 / K.1 / F.6: agent owns the raw frp tokens (broker
// only keeps SHA256), so agent must persist them or restart loses
// every proxy. Path: ~/.tether/agent/<sid>/state.json, mode 0600 in
// a 0700 dir, atomic tmp+rename writes.
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
	Token     string `json:"token"`      // raw frp auth token
}

// StateFile is the on-disk shape. Writers serialize through stateMu
// in the agent so concurrent expose / expose-rm don't trample each
// other's writes.
type StateFile struct {
	PortTokens []PortToken `json:"port_tokens"`
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
	b, err := os.ReadFile(s.path)
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

// saveLocked writes the StateFile via tmp+rename atomic replace.
// 0700 dir, 0600 file per architecture K.1 permission rules.
func (s *stateStore) saveLocked(sf *StateFile) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("agent state: mkdir: %w", err)
	}
	body, err := json.MarshalIndent(sf, "", "  ")
	if err != nil {
		return fmt.Errorf("agent state: marshal: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		return fmt.Errorf("agent state: write tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("agent state: rename: %w", err)
	}
	return nil
}
