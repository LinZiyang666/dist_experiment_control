// Opaque census identities never expose key material, salts or parser state.
// origin: hot-update S8 plan §4.2 / S8 internal review TAGGED-SUITE-SWEEP-A F2
package ssproxy

import "time"

// Responsibility names an existing private record. Material covers its entire
// FIFO/live-salt history; connection covers both AEAD cursors, pending bytes and
// original sockets. Their mutable contents stay exclusively in the SS codec.
type Responsibility struct {
	Kind string
	ID   [16]byte
}

// Census reads identity membership under the SS owner guard. It is valid while
// running or frozen and performs no IO, authority renewal or secret export.
func Census(s *Server, deadline time.Time) ([]Responsibility, error) {
	if s == nil {
		return nil, ErrStateUnrepresentable
	}
	if err := lockServerUntil(s, deadline); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	st := s.imageState
	if s.closed || st == nil {
		return nil, ErrStateUnrepresentable
	}
	// The existing exporter lazily assigns this identity at its first cut.
	// Describe precedes that cut, so establish the same retained identity here
	// under the same guard; every later export and import preserves it.
	if st.id == ([16]byte{}) {
		id, err := newStateID()
		if err != nil {
			return nil, ErrStateUnrepresentable
		}
		st.id = id
	}
	rows := []Responsibility{{Kind: "server", ID: st.id}, {Kind: "authority", ID: st.id}, {Kind: "timer", ID: st.id}}
	if s.ln != nil || st.listenerFile != nil {
		rows = append(rows, Responsibility{Kind: "listener", ID: st.id})
	}
	for id := range st.actors {
		rows = append(rows, Responsibility{Kind: "connection", ID: id})
	}
	for _, m := range st.materials {
		rows = append(rows, Responsibility{Kind: "material", ID: m.id})
	}
	for _, b := range st.bindings {
		rows = append(rows, Responsibility{Kind: "binding", ID: b.id})
	}
	return rows, nil
}
