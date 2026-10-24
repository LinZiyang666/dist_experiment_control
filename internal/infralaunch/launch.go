// Package infralaunch owns stock exec and the permanent store-lock handoff.
// It has no business bootstrap, NATS client, or systemd orchestration dependency.
// origin: hot-update S9 plan §4.1 / §6.2 / T14 / T15
package infralaunch

import (
	"crypto/sha256"
	"errors"
	"os"
	"sync"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
)

var (
	ErrStoreBusy      = errors.New("infra_store_busy")
	ErrLaunchRejected = errors.New("infra_launch_rejected")
)

// Selection is a memory-only view of root-owned, immutable records. Authorize
// must resolve Payload under the current domain/journal CAS and check policy,
// local launch key, peer/cgroup/MainPID, effective unit, startup fence, catalog,
// and (for Caddy) the finalized current storage selector. It must not hold the
// journal mutex while calling this backend or waiting for launcher exec.
// Files are borrowed; the backend duplicates them before returning custody.
type Selection struct {
	Executable *os.File
	Config     *os.File
	Directory  *os.File
	Locks      *os.File
	Guard      *Lock
	Revision   uint64
	Journal    infracontract.Digest
	// Original and Receipt are resolved only from a committed root journal on
	// worker replacement; Guard must be recovered from the receiver's OFD.
	Original *infracontract.HostRequest
	Receipt  *infracontract.HostReceipt
}

// Bindings keep P02's independent census and P04's durable admission outside
// this leaf. StoreCensus must enumerate legacy owners too, including aliases;
// successful flock alone never proves the store has no pre-retrofit writer.
type Bindings struct {
	Authorize   func(infracontract.HostRequest, infracontract.Deadline, infracontract.Stop) (Selection, error)
	StoreCensus func(infracontract.Store, infracontract.Deadline, infracontract.Stop) (infracontract.Census, error)
	Process     func(infracontract.Process, infracontract.Deadline) (infracontract.Process, error)
	Inspect     func(infracontract.ID, infracontract.Deadline) (infracontract.Census, error)
}

type claimKey struct {
	tx        infracontract.ID
	step      uint32
	component infracontract.Component
}
type pending struct {
	request  infracontract.HostRequest
	digest   infracontract.Digest
	prepared *Prepared
	receipt  infracontract.HostReceipt
	restored bool
}

// Backend retains a helper-side reference until an independently observed
// stock exec is acknowledged. P04 journals both operations; this in-memory
// custody is not a substitute for its persistent startup fence or recovery.
type Backend struct {
	mu       sync.Mutex
	bindings Bindings
	claims   map[claimKey]*pending
	closed   bool
}

var _ infracontract.Host = (*Backend)(nil)

func NewBackend(b Bindings) (*Backend, error) {
	if b.Authorize == nil || b.StoreCensus == nil || b.Process == nil || b.Inspect == nil {
		return nil, infracontract.ErrIncomplete
	}
	return &Backend{bindings: b, claims: make(map[claimKey]*pending)}, nil
}

func admission(d infracontract.Deadline, stop infracontract.Stop) error {
	if _, err := controlbudget.Remaining(d); err != nil {
		return err
	}
	select {
	case <-stop:
		return ErrLaunchRejected
	default:
		return nil
	}
}

func (b *Backend) Inspect(id infracontract.ID, d infracontract.Deadline) (infracontract.Census, error) {
	if err := admission(d, nil); err != nil {
		return infracontract.Census{}, err
	}
	return b.bindings.Inspect(id, d)
}

// Do is only the LaunchClaim/LaunchAck backend. P04 performs signed envelope
// verification and durable intent/result ordering around this exact Host API.
// origin: hot-update S9 plan §6.2 / T15
func (b *Backend) Do(r infracontract.HostRequest, d infracontract.Deadline, stop infracontract.Stop) (infracontract.HostReceipt, error) {
	if err := supported(); err != nil {
		return infracontract.HostReceipt{}, err
	}
	if err := admission(d, stop); err != nil {
		return infracontract.HostReceipt{}, err
	}
	if r.Op != infracontract.LaunchClaim && r.Op != infracontract.LaunchAck {
		return infracontract.HostReceipt{}, ErrLaunchRejected
	}
	encoded, err := infracontract.EncodeRequest(r)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	selection, err := b.bindings.Authorize(r, d, stop)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	if selection.Revision == 0 || selection.Journal == (infracontract.Digest{}) {
		return infracontract.HostReceipt{}, infracontract.ErrIncomplete
	}
	current, err := b.bindings.Process(r.Expected, d)
	if err != nil || current != r.Expected {
		return infracontract.HostReceipt{}, ErrLaunchRejected
	}
	if err := admission(d, stop); err != nil {
		return infracontract.HostReceipt{}, err
	}
	key := claimKey{r.Tx, r.Step, r.Component}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := admission(d, stop); err != nil {
		return infracontract.HostReceipt{}, err
	}
	if b.closed {
		return infracontract.HostReceipt{}, ErrLaunchRejected
	}
	claim := b.claims[key]
	if r.Op == infracontract.LaunchAck {
		if claim == nil {
			claim, err = restoredPending(r, selection, d)
			if err != nil {
				return infracontract.HostReceipt{}, err
			}
			b.claims[key] = claim
		}
		return b.ack(r, selection, current, claim, d)
	}
	digest := infracontract.Digest(sha256.Sum256(encoded))
	if claim != nil {
		if claim.digest != digest {
			return infracontract.HostReceipt{}, ErrLaunchRejected
		}
		return claim.receipt, nil
	}
	// origin: hot-update S9 plan §4.1 / T15
	census, err := b.bindings.StoreCensus(r.Store, d, stop)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	if !census.Complete || census.Generation == 0 || census.Digest == (infracontract.Digest{}) {
		return infracontract.HostReceipt{}, infracontract.ErrIncomplete
	}
	if len(census.Processes) != 0 || len(census.Sockets) != 0 || census.OldOwnerPending != 0 || census.LiveBusiness != 0 {
		return infracontract.HostReceipt{}, ErrStoreBusy
	}
	prepared, err := prepare(r, selection)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	prepared.deadline = d
	if err := admission(d, stop); err != nil {
		_ = prepared.Close()
		return infracontract.HostReceipt{}, err
	}
	receipt := infracontract.HostReceipt{Tx: r.Tx, Sequence: r.Sequence, Revision: selection.Revision, Op: r.Op, Applied: true, RequestDigest: digest, Journal: selection.Journal, Current: current, Census: census}
	b.claims[key] = &pending{r, digest, prepared, receipt, false}
	return receipt, nil
}

func (b *Backend) ack(r infracontract.HostRequest, s Selection, observed infracontract.Process, p *pending, d infracontract.Deadline) (infracontract.HostReceipt, error) {
	// The launcher cannot attest its own future exec. P02 observes the server,
	// and P04 issues Ack on that observation, retaining the original permit.
	// origin: hot-update S9 plan §4.1 / T14
	if p == nil || r.Payload != p.request.Payload || r.Domain != p.request.Domain || r.Store != p.request.Store || r.Candidate != p.request.Candidate || r.Config != p.request.Config || r.PolicyRevision != p.request.PolicyRevision || r.KeyID != p.request.KeyID || r.Sequence <= p.request.Sequence || r.BeforeCensus != p.receipt.Census.Digest || !sameInvocation(observed, p.request.Expected) || observed.ExeSHA256 != r.Candidate.BinarySHA256 || (observed.ExeDevice == p.request.Expected.ExeDevice && observed.ExeInode == p.request.Expected.ExeInode) {
		return infracontract.HostReceipt{}, ErrLaunchRejected
	}
	if err := p.prepared.verify(); err != nil {
		return infracontract.HostReceipt{}, err
	}
	if err := executableIdentity(p.prepared.executable, observed); err != nil {
		return infracontract.HostReceipt{}, err
	}
	// The helper's own lock reference cannot attest receiver custody. Match the
	// server's kernel fdinfo lock record, then recheck the pinned incarnation so
	// PID reuse or a close during observation cannot acknowledge a different task.
	// origin: S9 internal review rv-infra F04
	if err := inheritedLock(p.prepared.lock, observed); err != nil {
		return infracontract.HostReceipt{}, err
	}
	current, err := b.bindings.Process(observed, d)
	if err != nil || current != observed {
		return infracontract.HostReceipt{}, ErrLaunchRejected
	}
	encoded, err := infracontract.EncodeRequest(r)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	// Close, never LOCK_UN: unlocking an OFD would also unlock the server's dup.
	if err := p.prepared.Close(); err != nil {
		return infracontract.HostReceipt{}, err
	}
	delete(b.claims, claimKey{r.Tx, r.Step, r.Component})
	return infracontract.HostReceipt{Tx: r.Tx, Sequence: r.Sequence, Revision: s.Revision, Op: r.Op, Applied: true, RequestDigest: infracontract.Digest(sha256.Sum256(encoded)), Journal: s.Journal, Current: observed, Census: p.receipt.Census}, nil
}

func sameInvocation(a, b infracontract.Process) bool {
	return a.BootID == b.BootID && a.PID == b.PID && a.StartTicks == b.StartTicks && a.InvocationID == b.InvocationID && a.Cgroup == b.Cgroup
}

// Transfer duplicates the SAME flock OFD for SCM_RIGHTS. The backend keeps its
// reference even if send/reply fails, until Ack or explicit fenced recovery.
func (b *Backend) Transfer(r infracontract.HostRequest) (*Prepared, error) {
	data, err := infracontract.EncodeRequest(r)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	p := b.claims[claimKey{r.Tx, r.Step, r.Component}]
	if p == nil || b.closed || p.restored || p.digest != infracontract.Digest(sha256.Sum256(data)) {
		return nil, ErrLaunchRejected
	}
	return p.prepared.duplicate()
}

// HasClaimCustody selects only the recovery mechanism, never admission. A root
// serving both connections still owns the original OFD; a replacement worker
// must recover the receiver's actual description. Do rechecks every operand.
func HasClaimCustody(b *Backend, r infracontract.HostRequest) bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	claim := b.claims[claimKey{r.Tx, r.Step, r.Component}]
	return !b.closed && claim != nil && claim.request.Payload == r.Payload
}

// Close releases this worker's references, preserving receiver custody. P04
// keeps the durable fence closed after worker death and rechecks legacy owners.
func (b *Backend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	var err error
	for key, p := range b.claims {
		err = errors.Join(err, p.prepared.Close())
		delete(b.claims, key)
	}
	return err
}

// Prepared owns the explicit exec allowlist. Descriptor order is frozen for
// Send/Receive: executable, config, store directory, lock directory, lock OFD.
// No path, argv, environment text or private journal crosses this protocol.
type Prepared struct {
	request    infracontract.HostRequest
	executable *os.File
	config     *os.File
	directory  *os.File
	lock       *Lock
	deadline   infracontract.Deadline
}

// LaunchFrame is the fixed little protocol around canonical HostRequest bytes:
// three big-endian integers followed by RequestBytes bytes, with five rights on
// the first byte. Deadline is same-host BOOTTIME, never a cross-host timestamp.
// Unlike RemainingNS, this original root deadline cannot be renewed in transit.
// origin: hot-update S9 plan §2.1 / §2.2 / §6.2 / T15
type LaunchFrame struct {
	Version      uint32
	RequestBytes uint32
	Deadline     infracontract.Deadline
}

func (p *Prepared) Close() error {
	if p == nil {
		return nil
	}
	var err error
	for _, f := range []*os.File{p.executable, p.config, p.directory} {
		if f != nil {
			err = errors.Join(err, f.Close())
		}
	}
	if p.lock != nil {
		err = errors.Join(err, p.lock.Close())
	}
	p.executable, p.config, p.directory, p.lock = nil, nil, nil, nil
	return err
}

// Exec replaces this PID with the verified stock executable. It returns only
// on failure; the caller then closes all received files and exits unsuccessfully.
func (p *Prepared) Exec(d infracontract.Deadline, stop infracontract.Stop) error {
	if p == nil || p.deadline <= 0 {
		return ErrLaunchRejected
	}
	if p.deadline < d {
		d = p.deadline
	}
	if err := admission(d, stop); err != nil {
		return err
	}
	if err := p.verify(); err != nil {
		return err
	}
	return execPrepared(p, d, stop)
}

// RestoreClaim resumes only the acknowledgement of an already consumed permit.
// P04 supplies the immutable original request/receipt from its durable journal;
// Authorize must bind those records to recovery.Payload under the current CAS.
// P02 supplies Selection.Guard by recovering the actual server-held OFD after
// validating its pidfd/incarnation. A fresh open of the lock is insufficient.
// No new launch rights are issued, and a policy rotation requires explicit root
// reauthorization before these original records can pass the epoch checks.
// origin: hot-update S9 plan §4.1 / §6.2 / T14 / T15
func RestoreClaim(b *Backend, recovery, original infracontract.HostRequest, receipt infracontract.HostReceipt, d infracontract.Deadline, stop infracontract.Stop) error {
	if b == nil {
		return ErrLaunchRejected
	}
	if err := supported(); err != nil {
		return err
	}
	if err := admission(d, stop); err != nil {
		return err
	}
	if _, err := infracontract.EncodeRequest(recovery); err != nil {
		return err
	}
	data, err := infracontract.EncodeRequest(original)
	if err != nil {
		return err
	}
	digest := infracontract.Digest(sha256.Sum256(data))
	if recovery.Op != infracontract.RecoverHost || original.Op != infracontract.LaunchClaim || recovery.Tx != original.Tx || recovery.Step != original.Step || recovery.Domain != original.Domain || recovery.Component != original.Component || recovery.Store != original.Store || recovery.PolicyRevision != original.PolicyRevision || recovery.KeyID != original.KeyID || recovery.Sequence <= original.Sequence || !sameInvocation(recovery.Expected, original.Expected) || recovery.Expected.ExeSHA256 != original.Candidate.BinarySHA256 || !receipt.Applied || receipt.Op != infracontract.LaunchClaim || receipt.Tx != original.Tx || receipt.Sequence != original.Sequence || receipt.RequestDigest != digest || receipt.Current != original.Expected || !receipt.Census.Complete || receipt.Census.Generation == 0 || receipt.Census.Digest == (infracontract.Digest{}) || len(receipt.Census.Processes) != 0 || len(receipt.Census.Sockets) != 0 || receipt.Census.OldOwnerPending != 0 || receipt.Census.LiveBusiness != 0 {
		return ErrLaunchRejected
	}
	if _, err := infracontract.EncodeReceipt(receipt); err != nil {
		return err
	}
	selection, err := b.bindings.Authorize(recovery, d, stop)
	if err != nil {
		return err
	}
	if selection.Guard == nil || selection.Revision == 0 || selection.Journal == (infracontract.Digest{}) {
		return infracontract.ErrIncomplete
	}
	current, err := b.bindings.Process(recovery.Expected, d)
	if err != nil || current != recovery.Expected {
		return ErrLaunchRejected
	}
	census, err := b.bindings.StoreCensus(original.Store, d, stop)
	if err != nil {
		return err
	}
	if !census.Complete || census.Generation == 0 || census.Digest == (infracontract.Digest{}) {
		return infracontract.ErrIncomplete
	}
	if len(census.Processes) != 1 || census.Processes[0] != current {
		return ErrStoreBusy
	}
	p, err := prepare(original, selection)
	if err != nil {
		return err
	}
	if err := executableIdentity(p.executable, current); err != nil {
		_ = p.Close()
		return err
	}
	p.deadline = d
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := admission(d, stop); err != nil {
		_ = p.Close()
		return err
	}
	key := claimKey{original.Tx, original.Step, original.Component}
	if b.closed || b.claims[key] != nil {
		_ = p.Close()
		return ErrLaunchRejected
	}
	b.claims[key] = &pending{original, digest, p, receipt, true}
	return nil
}

// A replacement Accept=yes worker can acknowledge a consumed permit but cannot
// issue its launch rights again. All correlation comes from the durable Claim.
// origin: S9 internal review rv-infra F03 / F04
func restoredPending(r infracontract.HostRequest, s Selection, d infracontract.Deadline) (*pending, error) {
	if s.Original == nil || s.Receipt == nil || s.Guard == nil {
		return nil, ErrLaunchRejected
	}
	original, receipt := *s.Original, *s.Receipt
	data, err := infracontract.EncodeRequest(original)
	if err != nil {
		return nil, err
	}
	digest := infracontract.Digest(sha256.Sum256(data))
	if original.Op != infracontract.LaunchClaim || original.Tx != r.Tx || original.Step != r.Step || original.Component != r.Component || receipt.Op != infracontract.LaunchClaim || !receipt.Applied || receipt.Tx != original.Tx || receipt.Sequence != original.Sequence || receipt.RequestDigest != digest || receipt.Current != original.Expected || !receipt.Census.Complete || receipt.Census.Generation == 0 || receipt.Census.Digest == (infracontract.Digest{}) {
		return nil, ErrLaunchRejected
	}
	p, err := prepare(original, s)
	if err != nil {
		return nil, err
	}
	p.deadline = d
	return &pending{original, digest, p, receipt, true}, nil
}
