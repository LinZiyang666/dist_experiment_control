package xferserve

import (
	"encoding/json"
	"errors"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nkeys"
)

// GuardPermits refuses creation while the original-origin census is unknown.
// Rehome must never manufacture a second creation window merely because this
// broker's own ledger is empty. Verify remains bound to the retained issuer.
// origin: hot-update S7 plan §6.1 / §6.3 / T39
type GuardPermits struct {
	*xferauth.PermitAuthority
	Owners    Owners
	Directory *Directory
}

func controlDeadline() (x.Deadline, error) { return controlbudget.After(5 * time.Second) }

func (p GuardPermits) IssueFor(id x.Identity, intent x.Intent, d x.Deadline) (x.CreationPermit, error) {
	if p.Directory != nil {
		retained, err := p.Directory.lookup(id.SID, id.TransferID, d)
		if err != nil {
			return x.CreationPermit{}, err
		}
		if retained != nil {
			return matchingPermit(*retained, id, intent)
		}
	}
	censuses, err := p.Owners.SessionCensus(id.SID, d)
	if err != nil {
		return x.CreationPermit{}, err
	}
	for _, census := range censuses {
		if !census.Ready || !census.Complete {
			return x.CreationPermit{}, errors.New(x.CodeOwnerRecovering)
		}
		if census.Origin == p.Owners.Origin {
			continue
		}
		for _, held := range census.Entries {
			if held.Identity.SID == id.SID && held.Identity.TransferID == id.TransferID {
				return x.CreationPermit{}, errors.New(x.CodeOwnerRecovering + ": original origin retains this transfer")
			}
		}
	}
	permit, err := p.PermitAuthority.IssueFor(id, intent, d)
	if err != nil || p.Directory == nil {
		return permit, err
	}
	retained, err := p.Directory.claim(permit, d)
	if err != nil || retained == nil {
		// A claim was sent: unconfirmed publication can still become authority.
		// Preserve sent-unknown so the original invocation reconciles its ID.
		// origin: S7 internal review TRANSFER-OVERLAY-REPAIR F4 / hot-update S7 plan §6.1 / §7
		return x.CreationPermit{}, xferauth.ControlRefusal(x.CodeResultUnknown)
	}
	return matchingPermit(*retained, id, intent)
}

func matchingPermit(r permitRecord, id x.Identity, intent x.Intent) (x.CreationPermit, error) {
	digest, err := proto.TransferIntentDigest(proto.Intent(intent))
	p := r.Permit
	if err != nil || p.Identity.SID != id.SID || p.Identity.TransferID != id.TransferID || p.Identity.NID != id.NID || p.Identity.ActorFP != id.ActorFP ||
		p.Identity.AgentInstance != id.AgentInstance || p.Identity.IntentDigest != digest {
		return x.CreationPermit{}, xferauth.ControlRefusal(x.CodeIntentConflict)
	}
	return p, nil
}

func (p GuardPermits) Verify(permit x.CreationPermit) error {
	if p.Directory == nil {
		return p.PermitAuthority.Verify(permit)
	}
	d, err := controlDeadline()
	if err != nil {
		return err
	}
	r, err := p.Directory.lookup(permit.Identity.SID, permit.Identity.TransferID, d)
	if err != nil || r == nil || r.Permit != permit {
		return xferauth.ErrDenied
	}
	return nil
}

// CapsuleStore is an immutable, private journal beside the original ledger.
// It is synced before any capsule or grant can leave the origin. Replaying
// prepare returns the original bytes; a new random nonce would strand the
// original courier after revocation. The ledger still proves whether the entry
// is unsettled, so this journal alone never renews a retired entry.
// origin: hot-update S7 plan §6.4 / T31
type CapsuleStore struct {
	Root   string
	issuer nkeys.KeyPair // borrowed from this origin, never serialized
	mu     sync.Mutex
}

func (s *CapsuleStore) path(id x.Identity) string {
	return filepath.Join(s.Root, authorityKey(id.SID, id.TransferID, id.EntryNonce)+".json")
}

func (s *CapsuleStore) retain(c x.SettlementCapsule) (x.SettlementCapsule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, err := s.load(c.Identity)
	if err == nil {
		if old.Origin != c.Origin || old.Identity != c.Identity || old.ReceiverNkey != c.ReceiverNkey || old.Issuer != c.Issuer {
			return x.SettlementCapsule{}, xferauth.ErrDenied
		}
		return old, writeDurable(s.path(c.Identity), old)
	}
	if !os.IsNotExist(err) {
		return x.SettlementCapsule{}, err
	}
	if err = os.MkdirAll(s.Root, 0700); err != nil {
		return x.SettlementCapsule{}, err
	}
	if err = writeDurable(s.path(c.Identity), c); err != nil {
		return x.SettlementCapsule{}, err
	}
	return c, nil
}

func (s *CapsuleStore) load(id x.Identity) (x.SettlementCapsule, error) {
	data, err := os.ReadFile(s.path(id))
	if err != nil {
		return x.SettlementCapsule{}, err
	}
	var c x.SettlementCapsule
	if json.Unmarshal(data, &c) != nil || c.Identity != id {
		return x.SettlementCapsule{}, xferauth.ErrDenied
	}
	// Cached ordinary CONNECT grants also reach Principal after revocation.
	// The disk journal must authenticate its capsule there too; valid JSON is
	// not permission to replace the original receiver with another NKey.
	// origin: hot-update S7 plan §6.4 / T31
	checked, err := xferauth.SignCapsule(s.issuer, c)
	if err != nil || checked != c {
		return x.SettlementCapsule{}, xferauth.ErrDenied
	}
	return c, nil
}

func (s *CapsuleStore) row(row x.LedgerRow) (x.LedgerRow, error) {
	c, err := s.load(row.Identity)
	if os.IsNotExist(err) {
		return row, nil
	}
	if err != nil || row.Origin != c.Origin || (row.Settlement != nil && *row.Settlement != c) {
		return x.LedgerRow{}, xferauth.ErrDenied
	}
	row.Settlement = &c
	return row, nil
}

func (s *CapsuleStore) Census(c x.LedgerCensus) (x.LedgerCensus, error) {
	for i, row := range c.Rows {
		var err error
		c.Rows[i], err = s.row(row)
		if err != nil {
			return x.LedgerCensus{}, err
		}
	}
	return c, nil
}

func SettlementRows(link *Link, rows Rows) Rows {
	return func(id x.Identity, d x.Deadline) (x.LedgerRow, error) {
		r, err := rows(id, d)
		if err != nil || link == nil || link.Capsules == nil {
			return r, err
		}
		return link.Capsules.row(r)
	}
}

// A durable directory marker pins the stream across graph/image replacement.
// It prevents a deleted directory from silently bootstrapping a fresh authority.
func pinDirectory(root string, a *Directory) error {
	path := filepath.Join(root, "directory.json")
	data, err := os.ReadFile(path)
	if err == nil {
		var old time.Time
		if json.Unmarshal(data, &old) != nil || !old.Equal(a.created) {
			return xferauth.ControlRefusal(x.CodeOwnerRecovering)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	return writeDurable(path, a.created)
}
