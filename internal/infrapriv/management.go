package infrapriv

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// HostTransport is the authenticated local IPC client. A management adapter
// borrows it and its credential; it never owns either business NC or root keys.
type HostTransport interface {
	Do(c.HostRequest, c.Deadline, c.Stop) (c.HostReceipt, error)
}

// HostAuthority comes from the installed policy and the existing credential
// loader. Sign is typically Credential.Sign, so no seed enters a DTO or error.
// Reloading policy constructs another adapter; an old revision cannot be renewed.
type HostAuthority struct {
	Domain   c.ID
	BootID   string
	Revision uint64
	KeyID    string
	Public   ed25519.PublicKey
	Sign     func([]byte) ([]byte, error)
}

type ManagementHost struct {
	transport HostTransport
	authority HostAuthority
}

var _ c.Host = (*ManagementHost)(nil)

// NewManagementHost binds real IPC and signing authority without doing I/O.
// Physical support is checked by the helper on every operation, including reads.
// origin: hot-update S9 plan §2.1 / §6.3 / WIRE-2 R11
func NewManagementHost(transport HostTransport, authority HostAuthority) (*ManagementHost, error) {
	if transport == nil || authority.Sign == nil || len(authority.Public) != ed25519.PublicKeySize ||
		authority.Domain == (c.ID{}) || authority.BootID == "" || authority.Revision == 0 || authority.KeyID == "" {
		return nil, c.ErrIncomplete
	}
	authority.Public = append(ed25519.PublicKey(nil), authority.Public...)
	return &ManagementHost{transport, authority}, nil
}

func (h *ManagementHost) authorize(r c.HostRequest, d c.Deadline) (c.HostRequest, error) {
	if _, err := controlbudget.Remaining(d); err != nil {
		return c.HostRequest{}, ErrBudget
	}
	a := h.authority
	if r.Domain != a.Domain || r.HostBootID != a.BootID || r.PolicyRevision != a.Revision {
		return c.HostRequest{}, ErrPolicyStale
	}
	if len(r.Signature) != 0 {
		if r.KeyID != a.KeyID || c.VerifyRequest(r, a.Public) != nil {
			return c.HostRequest{}, ErrPermission
		}
		return r, nil
	}
	r.KeyID = a.KeyID
	if r.Nonce == (c.ID{}) {
		if _, err := rand.Read(r.Nonce[:]); err != nil {
			return c.HostRequest{}, ErrCredential
		}
	}
	data, err := c.SigningBytes(r)
	if err != nil {
		return c.HostRequest{}, err
	}
	r.Signature, err = a.Sign(data)
	if err != nil || c.VerifyRequest(r, a.Public) != nil {
		return c.HostRequest{}, ErrPermission
	}
	return r, nil
}

func (h *ManagementHost) AuthorizeCaddy(p c.Plan, r c.HostRequest, d c.Deadline) (c.HostRequest, error) {
	if r.Tx != p.Tx || r.Step != p.Step || r.Component != c.Caddy || r.Domain != p.Domain ||
		r.PolicyRevision != p.PolicyRevision || r.HostBootID != p.Before.BootID {
		return c.HostRequest{}, ErrPermission
	}
	return h.authorize(r, d)
}

func (h *ManagementHost) Do(r c.HostRequest, d c.Deadline, stop c.Stop) (c.HostReceipt, error) {
	r, err := h.authorize(r, d)
	if err != nil {
		return c.HostReceipt{}, err
	}
	receipt, err := h.transport.Do(r, d, stop)
	if err != nil {
		return c.HostReceipt{}, err
	}
	encoded, encodeErr := c.EncodeRequest(r)
	if encodeErr != nil || receipt.Tx != r.Tx || receipt.Sequence != r.Sequence || receipt.Op != r.Op ||
		receipt.RequestDigest != sha256.Sum256(encoded) || receipt.Journal == (c.Digest{}) || receipt.Revision == 0 || receipt.Code != "" {
		return c.HostReceipt{}, ErrResultUnknown
	}
	return receipt, nil
}

func (h *ManagementHost) observation(op c.HostOp, tx c.ID, step uint32, d c.Deadline) (c.HostRequest, error) {
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return c.HostRequest{}, ErrBudget
	}
	var nonce c.ID
	if _, err := rand.Read(nonce[:]); err != nil {
		return c.HostRequest{}, ErrCredential
	}
	return c.HostRequest{Version: c.RequestVersion, Op: op, Component: c.Caddy, Tx: tx, Step: step,
		Domain: h.authority.Domain, PolicyRevision: h.authority.Revision, HostBootID: h.authority.BootID,
		Sequence: binary.BigEndian.Uint64(nonce[:8]) | 1, Nonce: nonce, RemainingNS: uint64(remaining)}, nil
}

func (h *ManagementHost) Inspect(domain c.ID, d c.Deadline) (c.Census, error) {
	if domain != h.authority.Domain {
		return c.Census{}, ErrPermission
	}
	var tx c.ID
	if _, err := rand.Read(tx[:]); err != nil {
		return c.Census{}, ErrCredential
	}
	r, err := h.observation(c.Inspect, tx, 1, d)
	if err != nil {
		return c.Census{}, err
	}
	receipt, err := h.Do(r, d, nil)
	if err != nil {
		return c.Census{}, err
	}
	if !receipt.Census.Complete || receipt.Census.Generation == 0 || receipt.Census.Digest == (c.Digest{}) {
		return c.Census{}, c.ErrIncomplete
	}
	return receipt.Census, nil
}

// VerifyCaddyIngress consumes a fresh root observation. A saved SwitchIngress
// receipt has no authority here: the root resolves current rules and seals the
// displaced owner itself, under the authenticated plan/domain/boot/policy.
// origin: hot-update S9 plan §5.2 / WIRE-2 R11
func (h *ManagementHost) VerifyCaddyIngress(p c.Plan, old, target c.Process, census c.Census, d c.Deadline, stop c.Stop) error {
	if p.Component != c.Caddy || p.Domain != h.authority.Domain || p.PolicyRevision != h.authority.Revision ||
		p.Before.BootID != h.authority.BootID || !census.Complete || census.Generation == 0 || census.Digest == (c.Digest{}) ||
		old == (c.Process{}) || target == (c.Process{}) || old == target {
		return c.ErrIncomplete
	}
	r, err := h.observation(c.VerifyIngress, p.Tx, p.Step, d)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(target)
	if err != nil {
		return c.ErrIncomplete
	}
	r.Expected, r.BeforeCensus, r.Payload = old, census.Digest, sha256.Sum256(encoded)
	receipt, err := h.Do(r, d, stop)
	if err != nil {
		return err
	}
	if receipt.Applied || receipt.Current != target || receipt.Census.Digest != census.Digest ||
		!receipt.Census.Complete || receipt.Census.Generation != census.Generation || receipt.Census.OldOwnerPending != 0 {
		return c.ErrIncomplete
	}
	return nil
}
