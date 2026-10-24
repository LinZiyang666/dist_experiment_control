package agent

import (
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/LinZiyang666/tether/internal/agentstate"
)

// The persistence leaf owns schema and disk operations; the agent retains its lease latch.
// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
type PortToken = agentstate.PortToken
type ProxyState = agentstate.ProxyState
type RosterCache = agentstate.RosterCache
type StateFile = agentstate.StateFile
type stateStore struct {
	mu       sync.Mutex
	path     string
	detached atomic.Bool
}

func newStateStore(home, sid string) *stateStore {
	return &stateStore{path: filepath.Join(home, "agent", sid, "state.json")}
}
func (s *stateStore) domain() *agentstate.Store {
	return agentstate.NewView(s.path, &s.mu, &s.detached)
}
func (s *stateStore) detach()                         { s.domain().Detach() }
func (s *stateStore) reattach()                       { s.domain().Reattach() }
func (s *stateStore) load() (*StateFile, error)       { return s.domain().Load() }
func (s *stateStore) loadNoLock() (*StateFile, error) { return s.domain().LoadNoLock() }
func parseStateBytes(b []byte, err error) (*StateFile, error) {
	return agentstate.ParseStateBytes(b, err)
}
func (s *stateStore) AddPort(p PortToken) error { return s.domain().AddPort(p) }
func (s *stateStore) UpdatePortHome(name, homeBrokerAddr string, epoch int64) error {
	return s.domain().UpdatePortHome(name, homeBrokerAddr, epoch)
}
func (s *stateStore) RemovePort(name string) error          { return s.domain().RemovePort(name) }
func (s *stateStore) SetProxy(p *ProxyState) error          { return s.domain().SetProxy(p) }
func (s *stateStore) GetProxy() (*ProxyState, error)        { return s.domain().GetProxy() }
func (s *stateStore) SetRosterCache(rc *RosterCache) error  { return s.domain().SetRosterCache(rc) }
func (s *stateStore) GetRosterCache() (*RosterCache, error) { return s.domain().GetRosterCache() }
