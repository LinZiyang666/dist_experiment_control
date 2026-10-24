package infrapriv

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"net"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
)

const LaunchQueryPurpose = "TLQ1"
const LaunchOfferPurpose = "TLO1"

// LaunchIdentity is public bootstrap material, not a permit or a journal view.
// The launch seed signs only queries and Claim/Ack under this root-owned epoch.
// origin: S9 internal review rv-infra F03
type LaunchIdentity struct {
	Version        uint16
	Domain         c.ID
	PolicyRevision uint64
	KeyID          string
	HostBootID     string
	ServiceGID     uint32
}

type LaunchQuery struct {
	Identity    LaunchIdentity
	Component   c.Component
	Expected    c.Process
	Nonce       c.ID
	RemainingNS uint64
	Signature   []byte
}

// LaunchOffers is a read-only resolver. A query never claims a permit, starts a
// unit, or lends FDs. The returned request must still traverse Host.Do's signed
// policy/domain/journal CAS before the launch backend obtains custody.
type LaunchOffers interface {
	Offer(LaunchQuery, c.Peer, c.Deadline, c.Stop) (c.HostRequest, error)
}

func launchQueryBytes(q LaunchQuery) ([]byte, error) {
	if q.Identity.Version != 1 || q.Identity.Domain == (c.ID{}) || q.Identity.PolicyRevision == 0 || q.Identity.KeyID == "" || q.Identity.HostBootID == "" || q.Identity.ServiceGID == 0 || (q.Component != c.NATS && q.Component != c.Caddy) || q.Expected.BootID != q.Identity.HostBootID || q.Expected.PID <= 0 || q.Expected.StartTicks == 0 || q.Expected.InvocationID == "" || q.Expected.Cgroup == "" || q.Expected.ExeSHA256 == (c.Digest{}) || q.Nonce == (c.ID{}) || q.RemainingNS == 0 || q.RemainingNS > uint64(WorkerBudget) {
		return nil, ErrPermission
	}
	q.Signature = nil
	data, err := json.Marshal(q)
	if err != nil || len(data) > c.MaxRequestBytes {
		return nil, ErrIPC
	}
	return append([]byte("tether-infra-launch-query-v1\n"), data...), nil
}

func SignLaunchQuery(q LaunchQuery, key *Credential) (LaunchQuery, error) {
	data, err := launchQueryBytes(q)
	if err != nil {
		return q, err
	}
	q.Signature, err = key.Sign(data)
	return q, err
}

func decodeLaunchQuery(data []byte) (LaunchQuery, error) {
	var q LaunchQuery
	if len(data) > c.MaxRequestBytes || json.Unmarshal(data, &q) != nil {
		return q, ErrIPC
	}
	canonical, _ := json.Marshal(q)
	if !bytes.Equal(data, canonical) || len(q.Signature) != ed25519.SignatureSize {
		return LaunchQuery{}, ErrIPC
	}
	if _, err := launchQueryBytes(q); err != nil {
		return LaunchQuery{}, err
	}
	return q, nil
}

func (s *PolicyStore) authorizeLaunchQuery(q LaunchQuery, peer c.Peer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.current
	if s.failed || s.rotating || q.Identity.Domain != p.Domain || q.Identity.HostBootID != p.HostBootID || q.Identity.PolicyRevision != p.Revision || q.Identity.ServiceGID != p.ServiceGID {
		return ErrPolicyStale
	}
	if peer.PID != q.Expected.PID || peer.StartTicks != q.Expected.StartTicks || peer.BootID != q.Expected.BootID {
		return ErrPermission
	}
	data, err := launchQueryBytes(q)
	if err != nil {
		return err
	}
	for _, key := range p.Keys {
		if key.Scope == LaunchAuthority && key.KeyID == q.Identity.KeyID && key.UID == peer.UID && key.GID == peer.GID && ed25519.Verify(key.PublicKey, data, q.Signature) {
			return nil
		}
	}
	return ErrPermission
}

func (s *Server) serveLaunchQuery(conn *net.UnixConn, data []byte, peer c.Peer, entered int64, deadline c.Deadline, stop c.Stop) error {
	q, err := decodeLaunchQuery(data)
	if err != nil {
		return err
	}
	if err := s.policy.authorizeLaunchQuery(q, peer); err != nil {
		return err
	}
	now, err := controlbudget.Now()
	if err != nil {
		return err
	}
	remaining := int64(q.RemainingNS) - (now - entered)
	if remaining <= 0 {
		return ErrBudget
	}
	deadline = min(deadline, c.Deadline(now+min(remaining, int64(ActionBudget))))
	offers, ok := s.files.(LaunchOffers)
	if !ok {
		return c.ErrIncomplete
	}
	request, err := offers.Offer(q, peer, deadline, stop)
	if err != nil {
		return err
	}
	if !offerMatches(request, q) {
		return ErrPermission
	}
	encoded, err := c.EncodeRequest(request)
	if err != nil {
		return err
	}
	if err := conn.SetWriteDeadline(time.Now().Add(time.Duration(max(0, int64(deadline)-now)))); err != nil {
		return err
	}
	if err := writeFrame(conn, LaunchOfferPurpose, encoded); err != nil {
		return err
	}
	return conn.CloseWrite()
}

func offerMatches(r c.HostRequest, q LaunchQuery) bool {
	return r.Op == c.LaunchClaim && r.Component == q.Component && r.Expected == q.Expected && r.Nonce == q.Nonce && r.Domain == q.Identity.Domain && r.PolicyRevision == q.Identity.PolicyRevision && r.KeyID == q.Identity.KeyID && r.HostBootID == q.Identity.HostBootID && r.RemainingNS <= q.RemainingNS && len(r.Signature) == 0
}
