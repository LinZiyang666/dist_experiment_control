// Package ingressswitch owns the canonical Caddy TCP entrance, not Caddy's
// business connections. A redirect only changes the first packet of a new flow;
// the physical census remains responsible for every earlier connection.
// origin: hot-update S9 plan §5.2 / P07
package ingressswitch

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"reflect"
	"strconv"
	"strings"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
)

const (
	Version       = 1
	CanonicalPort = 443
	ShadowPort    = 8443
	// Only the root probe gets this mark; it never removes the live redirect.
	PrimaryMark = 0x54495448
)

var (
	ErrConflict       = errors.New("infra_busy")
	ErrProbe          = fmt.Errorf("%w: ingress probe", infracontract.ErrIncomplete)
	ErrResultUnknown  = errors.New("infra_result_unknown")
	ErrBudgetExceeded = errors.New("infra_budget_exceeded")
)

// Snapshot is the kernel marker, read independently after an ambiguous reply.
// Digest references a root-owned immutable Change, not a caller configuration.
type Snapshot struct {
	Version    uint16
	Generation uint64
	Digest     infracontract.Digest
	Redirect   bool
}

// Change is an immutable root record. P04 journals its intent/result and P03
// authenticates the whole HostRequest before the worker reaches this backend.
// Returning advances the generation too: a delayed unflip cannot undo a later cut.
type Change struct {
	Version         uint16
	Domain          infracontract.ID
	Tx              infracontract.ID
	Step            uint32
	Before          Snapshot
	After           Snapshot
	Primary         infracontract.Process
	Shadow          infracontract.Process
	ReadyProof      infracontract.Digest
	ExpectedJournal infracontract.Digest
	BeforeCensus    infracontract.Digest
	WarmCensus      infracontract.Digest
	ReturnProof     infracontract.Digest
}

type Records interface {
	Load(infracontract.Digest) (Change, error)
}

// Backend performs one atomic, owned-table transaction under its permanent
// mutation lock. A failed/ambiguous apply is queried, never blindly reversed.
type Backend interface {
	Read(infracontract.Deadline, infracontract.Stop) (Snapshot, error)
	CompareAndSwap(Snapshot, Snapshot, infracontract.Deadline, infracontract.Stop) (Snapshot, error)
}

// ProbeProof is produced by P02 from the actual client, accepted connection,
// listener and process. Matching TLS certificates or upstream NATS IDs cannot
// replace these socket/process observations.
type ProbeProof struct {
	Version      uint16
	TLS          bool
	FirstAttempt bool
	Mark         uint32
	StartedNS    int64
	FinishedNS   int64
	Client       infracontract.Socket
	Accepted     infracontract.Socket
	Listener     infracontract.Socket
	ReadyProof   infracontract.Digest
}

// Seal includes the scopes which a WSS-only counter misses. BarrierClosed means
// the producer observed this redirect generation throughout its full census;
// two instantaneous zero counts do not establish a sealed entrance.
type Seal struct {
	Version           uint16
	Rules             Snapshot
	Owner             infracontract.Process
	BarrierClosed     bool
	AcceptedComplete  bool
	ListenersComplete bool
	SYNComplete       bool
	ConntrackComplete bool
	AcceptedTCP       uint32
	PendingSYN        uint32
	PendingConntrack  uint32
	Census            infracontract.Census
}

// Evidence must be a real physical producer at composition time. This leaf does
// not manufacture a complete empty census when P02 has not been installed.
type Evidence interface {
	Probe(infracontract.Process, bool, infracontract.Deadline, infracontract.Stop) (ProbeProof, error)
	Seal(Snapshot, infracontract.Process, infracontract.Deadline, infracontract.Stop) (Seal, error)
	Census(infracontract.ID, infracontract.Deadline) (infracontract.Census, error)
}

type Switch struct {
	backend  Backend
	records  Records
	evidence Evidence
}

var _ infracontract.Host = (*Switch)(nil)

func New(backend Backend, records Records, evidence Evidence) (*Switch, error) {
	if missing(backend) || missing(records) || missing(evidence) {
		return nil, infracontract.ErrContractStub
	}
	return &Switch{backend: backend, records: records, evidence: evidence}, nil
}

// An interface holding a typed nil pointer is not an installed backend. Keeping
// it would defer missing composition until the first privileged operation.
// origin: hot-update S9 plan §2.1 / T16
func missing(value any) bool {
	return value == nil || reflect.ValueOf(value).Kind() == reflect.Pointer && reflect.ValueOf(value).IsNil()
}

// ChangeDigest binds all state, identities, and proof references. After.Digest
// is cleared to avoid a self-reference; nothing else is omitted from the record.
func ChangeDigest(change Change) infracontract.Digest {
	if !stateShapesValid() {
		return infracontract.Digest{}
	}
	change.After.Digest = infracontract.Digest{}
	data, _ := json.Marshal(change) // This closed value contains no fallible JSON types.
	// Root records remain bounded even though their IPC operand is only a
	// digest. A large internal loader result cannot evade the request budget.
	// origin: hot-update S9 plan §2.2 / T16
	if len(data) > infracontract.MaxRequestBytes {
		return infracontract.Digest{}
	}
	return sha256.Sum256(append([]byte("tether-ingress-change-v1\n"), data...))
}

// origin: hot-update S9 plan §6.2 / T16
func (s *Switch) Do(req infracontract.HostRequest, deadline infracontract.Deadline, stop infracontract.Stop) (infracontract.HostReceipt, error) {
	if err := active(deadline, stop); err != nil {
		return infracontract.HostReceipt{}, err
	}
	if req.Version != Version || req.Op != infracontract.SwitchIngress || req.Component != infracontract.Caddy ||
		req.Tx == (infracontract.ID{}) || req.Domain == (infracontract.ID{}) || req.Sequence == 0 || req.Payload == (infracontract.Digest{}) {
		return infracontract.HostReceipt{}, infracontract.ErrIncomplete
	}
	data, err := infracontract.EncodeRequest(req)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	change, err := s.records.Load(req.Payload)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	if !validChange(change) || change.After.Digest != req.Payload || ChangeDigest(change) != req.Payload ||
		change.Domain != req.Domain || change.Tx != req.Tx || change.Step != req.Step || change.Primary != req.Expected ||
		change.ExpectedJournal != req.ExpectedJournal ||
		change.BeforeCensus != req.BeforeCensus || change.WarmCensus != req.WarmCensus || change.ReturnProof != req.ReturnProof {
		return infracontract.HostReceipt{}, infracontract.ErrIncomplete
	}
	// The returning probe must bypass the still-serving shadow. A successful
	// canonical request to shadow is never authority to send new flows to main.
	// origin: hot-update S9 plan §5.2 / T17
	target, bypass := change.Shadow, false
	if !change.After.Redirect {
		target, bypass = change.Primary, true
	}
	current, err := s.backend.Read(deadline, stop)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	// A previous worker can die after the kernel commit but before journaling
	// its result. The exact marker is an effect receipt, even if the service
	// became unhealthy afterward. Re-probing here would lose that distinction;
	// readiness and retirement are independently checked by the Caddy driver.
	// origin: hot-update S9 plan §6.2 / T16
	if current == change.After {
		return effectReceipt(req, data, current, target), nil
	}
	if current != change.Before {
		return infracontract.HostReceipt{}, ErrConflict
	}
	probeStart, err := controlbudget.Now()
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	proof, err := s.evidence.Probe(target, bypass, deadline, stop)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	if err = VerifyProbe(proof, target, bypass, change.ReadyProof); err != nil {
		return infracontract.HostReceipt{}, err
	}
	probeEnd, err := controlbudget.Now()
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	// A receipt from an earlier hop can have the same executable SHA and even
	// the same still-running process. Only this call's physical observation
	// qualifies the entrance now; future or cached clock values are not evidence.
	// origin: hot-update S9 plan §2.3 / §5.2 / T17
	if proof.StartedNS < probeStart || proof.FinishedNS > probeEnd {
		return infracontract.HostReceipt{}, ErrProbe
	}
	if err = active(deadline, stop); err != nil {
		return infracontract.HostReceipt{}, err
	}
	current, err = s.backend.CompareAndSwap(change.Before, change.After, deadline, stop)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	if current != change.After {
		return infracontract.HostReceipt{}, ErrConflict
	}
	return effectReceipt(req, data, current, target), nil
}

func effectReceipt(req infracontract.HostRequest, encoded []byte, rules Snapshot, target infracontract.Process) infracontract.HostReceipt {
	return infracontract.HostReceipt{Tx: req.Tx, Sequence: req.Sequence, Revision: rules.Generation,
		Op: req.Op, Applied: true, RequestDigest: sha256.Sum256(encoded), Current: target}
}

func (s *Switch) Inspect(domain infracontract.ID, deadline infracontract.Deadline) (infracontract.Census, error) {
	if err := active(deadline, nil); err != nil {
		return infracontract.Census{}, err
	}
	census, err := s.evidence.Census(domain, deadline)
	if err != nil {
		return infracontract.Census{}, err
	}
	if domain == (infracontract.ID{}) || !validCensus(census) {
		return infracontract.Census{}, infracontract.ErrIncomplete
	}
	return census, nil
}

// RetirementSeal only authorizes retiring the displaced owner. Original long
// business may still be live; old-owner responsibility must be zero. The same
// check applies to shadow after return, including its retained conntrack flows.
// origin: hot-update S9 plan §5.2 / T16
func (s *Switch) RetirementSeal(change Change, deadline infracontract.Deadline, stop infracontract.Stop) (Seal, error) {
	if err := active(deadline, stop); err != nil {
		return Seal{}, err
	}
	if !validChange(change) || ChangeDigest(change) != change.After.Digest {
		return Seal{}, infracontract.ErrIncomplete
	}
	rules, err := s.backend.Read(deadline, stop)
	if err != nil {
		return Seal{}, err
	}
	if rules != change.After {
		return Seal{}, ErrConflict
	}
	owner := change.Primary
	if !rules.Redirect {
		owner = change.Shadow
	}
	seal, err := s.evidence.Seal(rules, owner, deadline, stop)
	if err != nil {
		return Seal{}, err
	}
	if err = VerifySeal(seal, rules, owner); err != nil {
		return Seal{}, err
	}
	// A rule change during census invalidates the admission barrier immediately.
	after, err := s.backend.Read(deadline, stop)
	if err != nil {
		return Seal{}, err
	}
	if after != rules {
		return Seal{}, ErrConflict
	}
	return seal, nil
}

// origin: hot-update S9 plan §5.2 / T17
func VerifyProbe(p ProbeProof, target infracontract.Process, bypass bool, ready infracontract.Digest) error {
	mark, port := uint32(0), ShadowPort
	if bypass {
		mark, port = PrimaryMark, CanonicalPort
	}
	if !stateShapesValid() || p.Version != Version || !p.TLS || !p.FirstAttempt || p.Mark != mark ||
		p.StartedNS <= 0 || p.FinishedNS < p.StartedNS || ready == (infracontract.Digest{}) || p.ReadyProof != ready ||
		!validProcess(target) || !validSocket(p.Client) || !validSocket(p.Accepted) || !validSocket(p.Listener) ||
		p.Accepted.Owner != target || p.Listener.Owner != target || p.Accepted.NetNS != p.Listener.NetNS ||
		p.Client.Inode == p.Accepted.Inode && p.Client.NetNS == p.Accepted.NetNS ||
		p.Client.Cookie == p.Accepted.Cookie && p.Client.NetNS == p.Accepted.NetNS ||
		p.Client.Remote != p.Accepted.Local || p.Client.Local != p.Accepted.Remote ||
		!hasPort(p.Accepted.Local, port) || !hasPort(p.Listener.Local, port) || p.Listener.Remote != "" {
		return ErrProbe
	}
	return nil
}

// origin: hot-update S9 plan §5.2 / T16
func VerifySeal(seal Seal, rules Snapshot, owner infracontract.Process) error {
	if !stateShapesValid() || seal.Version != Version || seal.Rules != rules || !validProcess(owner) || seal.Owner != owner ||
		!seal.BarrierClosed || !seal.AcceptedComplete || !seal.ListenersComplete || !seal.SYNComplete || !seal.ConntrackComplete ||
		seal.AcceptedTCP != 0 || seal.PendingSYN != 0 || seal.PendingConntrack != 0 || !validCensus(seal.Census) || seal.Census.OldOwnerPending != 0 {
		return infracontract.ErrIncomplete
	}
	for _, socket := range seal.Census.Sockets {
		if !validSocket(socket) {
			return infracontract.ErrIncomplete
		}
		if socket.Owner == owner && socket.Remote != "" {
			// Kernel LISTEN rows have an explicit wildcard peer with port zero.
			// This exception only applies after every independent admission and
			// pending-work guard above; a concrete peer still owns accepted work.
			// origin: S9 internal review s9-p9eng F07
			peer, err := netip.ParseAddrPort(socket.Remote)
			if err != nil || !peer.Addr().IsUnspecified() || peer.Port() != 0 {
				return infracontract.ErrIncomplete
			}
		}
	}
	return nil
}

func validChange(c Change) bool {
	return c.Version == Version && c.Domain != (infracontract.ID{}) && c.Tx != (infracontract.ID{}) &&
		c.Before.Version == Version && c.After.Version == Version && c.Before.Generation < math.MaxUint64 &&
		c.After.Generation == c.Before.Generation+1 && c.Before.Redirect != c.After.Redirect &&
		(c.Before.Generation != 0 && c.Before.Digest != (infracontract.Digest{}) || c.Before.Generation == 0 && c.Before.Digest == (infracontract.Digest{}) && !c.Before.Redirect) &&
		c.After.Digest != (infracontract.Digest{}) && validProcess(c.Primary) && validProcess(c.Shadow) &&
		c.Primary != c.Shadow && c.Primary.BootID == c.Shadow.BootID && c.ReadyProof != (infracontract.Digest{}) &&
		c.ExpectedJournal != (infracontract.Digest{}) && c.BeforeCensus != (infracontract.Digest{}) && c.WarmCensus != (infracontract.Digest{}) &&
		(c.After.Redirect || c.ReturnProof != (infracontract.Digest{}))
}

func validProcess(p infracontract.Process) bool {
	return p.BootID != "" && p.PID > 0 && p.StartTicks > 0 && p.InvocationID != "" &&
		p.ExeDevice != 0 && p.ExeInode != 0 && p.ExeSHA256 != (infracontract.Digest{}) && p.Cgroup != ""
}

func validSocket(s infracontract.Socket) bool {
	return s.NetNS != 0 && s.Device != 0 && s.Inode != 0 && s.Cookie != 0 && s.Local != "" && validProcess(s.Owner)
}

func validCensus(c infracontract.Census) bool {
	if !c.Complete || c.Generation == 0 || c.Digest == (infracontract.Digest{}) || len(c.Processes) == 0 {
		return false
	}
	seen := make(map[infracontract.Process]bool)
	for _, process := range c.Processes {
		if !validProcess(process) || seen[process] {
			return false
		}
		seen[process] = true
	}
	for _, socket := range c.Sockets {
		if !validSocket(socket) || !seen[socket.Owner] {
			return false
		}
	}
	return true
}

func hasPort(address string, port int) bool {
	_, value, err := net.SplitHostPort(address)
	return err == nil && value == strconv.Itoa(port)
}

func active(deadline infracontract.Deadline, stop infracontract.Stop) error {
	select {
	case <-stop:
		return ErrBudgetExceeded
	default:
	}
	_, err := controlbudget.Remaining(deadline)
	if err != nil {
		return errors.Join(ErrBudgetExceeded, err)
	}
	return nil
}

// Independent declarations freeze the private records consumed by the kernel
// marker, root immutable loader and physical producer. Reflecting the actual
// struct alone would let both a new field and its inventory appear together,
// while an old reader silently ignored that field. Every guard checks this
// declaration before treating a digest as a v1 proof.
// origin: hot-update S9 plan §2.2 / §10.2 / T16
var stateShapes = []struct {
	typ    reflect.Type
	fields string
}{
	{reflect.TypeFor[Snapshot](), "Version:uint16;Generation:uint64;Digest:infracontract.Digest;Redirect:bool"},
	{reflect.TypeFor[Change](), "Version:uint16;Domain:infracontract.ID;Tx:infracontract.ID;Step:uint32;Before:ingressswitch.Snapshot;After:ingressswitch.Snapshot;Primary:infracontract.Process;Shadow:infracontract.Process;ReadyProof:infracontract.Digest;ExpectedJournal:infracontract.Digest;BeforeCensus:infracontract.Digest;WarmCensus:infracontract.Digest;ReturnProof:infracontract.Digest"},
	{reflect.TypeFor[ProbeProof](), "Version:uint16;TLS:bool;FirstAttempt:bool;Mark:uint32;StartedNS:int64;FinishedNS:int64;Client:infracontract.Socket;Accepted:infracontract.Socket;Listener:infracontract.Socket;ReadyProof:infracontract.Digest"},
	{reflect.TypeFor[Seal](), "Version:uint16;Rules:ingressswitch.Snapshot;Owner:infracontract.Process;BarrierClosed:bool;AcceptedComplete:bool;ListenersComplete:bool;SYNComplete:bool;ConntrackComplete:bool;AcceptedTCP:uint32;PendingSYN:uint32;PendingConntrack:uint32;Census:infracontract.Census"},
	{reflect.TypeFor[TLSAttempt](), "StartedNS:int64;FinishedNS:int64;Success:bool;Mark:uint32"},
}

func StateDescriptors() []infracontract.RecordDescriptor {
	rows := make([]infracontract.RecordDescriptor, 0, len(stateShapes))
	for _, shape := range stateShapes {
		row := infracontract.RecordDescriptor{Name: shape.typ.Name(), Version: 1, MaxBytes: infracontract.MaxRequestBytes}
		for _, entry := range strings.Split(shape.fields, ";") {
			name, typ, _ := strings.Cut(entry, ":")
			row.Fields = append(row.Fields, infracontract.FieldDescriptor{Name: name, Type: typ})
		}
		rows = append(rows, row)
	}
	return rows
}

func stateShapesValid() bool {
	for _, shape := range stateShapes {
		fields := strings.Split(shape.fields, ";")
		if len(fields) != shape.typ.NumField() {
			return false
		}
		for i, entry := range fields {
			field := shape.typ.Field(i)
			if entry != field.Name+":"+field.Type.String() || field.Tag != "" {
				return false
			}
		}
	}
	return true
}
