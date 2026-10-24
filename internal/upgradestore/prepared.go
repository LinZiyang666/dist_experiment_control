package upgradestore

import (
	"time"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Prepared returns only an already persisted inert authorization. It never
// accepts or reconstructs an execution: the component's live observer must
// separately prove source ownership before a roll may dispatch this grant.
// origin: hot-update S8 plan §3.1; S8 internal review cluster-roll F3
func (s *Store) Prepared(key upgradetx.Key, txID string) (out upgradetx.View, err error) {
	if key != s.options.Key {
		return out, failure(upgradetx.CodeRejected, "component key mismatch", false)
	}
	err = s.locked(func(_ *diskFile, e *diskEntry) error {
		for _, p := range e.Grants {
			g := p.Grant
			if g.Handle.TxID != txID || p.Reverse != nil {
				continue
			}
			a := upgradetx.Attempt{Handle: g.Handle, Direction: upgradetx.DirectionForward, SourceSHA: g.SourceSHA, TargetSHA: g.TargetSHA, SourceGeneration: g.Handle.Generation - 1, Phase: upgradetx.PhasePrepared, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomePending, Scope: p.Scope}
			out.Historical = upgradetx.Record{Version: upgradetx.RecordVersion, Key: key, Revision: g.Handle.Revision, Grant: g, Forward: a}
			return nil
		}
		return failure(upgradetx.CodeGrantUnknown, "grant is not recorded", false)
	})
	return
}

// ReadParentDeadline is called while the component writer already holds the
// shared short guard. Re-locking that inode through another Store would
// self-deadlock. Atomic journal publication plus checksum/record validation
// makes this read safe; it performs no mutation or authorization renewal.
// origin: S8 internal review cluster-roll F10
func ReadParentDeadline(path string, key upgradetx.Key, parent, slotDigest string) (int64, error) {
	s := &Store{options: Options{Path: path, Key: key}, now: time.Now}
	d, err := s.read()
	if err != nil {
		return 0, err
	}
	for _, entry := range d.Entries {
		if entry.Key != key || entry.Current == nil {
			continue
		}
		r := entry.Current
		g := r.Grant
		if g.ClientNonce == parent && g.CapabilityDigest == slotDigest && r.Forward.Phase == upgradetx.PhasePrepared && g.ExpiresAt > time.Now().UnixNano() {
			return g.ExpiresAt, nil
		}
	}
	return 0, failure(upgradetx.CodeDeferred, "parent reservation is unavailable", false)
}
