// infra.go collects S9 physical cohorts without exercising upgrade authority.
// origin: hot-update S10 plan §5.4 / §10.2 P15.
package fullstackharness

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/ingressmove"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// InfraBinding joins an original workload to its actual control/data NC. Sender
// and receiver are separate rows, even when they belong to the same transfer.
// P14/P12 supply original identities; this adapter never creates replacements.
// origin: hot-update S10 plan §5.4 / §10.2 P15.
type InfraBinding struct {
	WorkloadID  string
	Leg         string
	Participant c.ID
	Original    contract.Process
}

// InfraTranslation comes from the independently retained root nft/conntrack
// reader. Canonical 443 may DNAT to the warm listener; the complete original
// and reply tuples bind that translation to this accepted socket and rule epoch.
// Missing kernel translation remains pending, never guessed from 8443 or a URL.
type InfraTranslation struct {
	NetNS           uint64          `json:"netns"`
	OriginalLocal   string          `json:"original_local"`
	OriginalRemote  string          `json:"original_remote"`
	ReplyLocal      string          `json:"reply_local"`
	ReplyRemote     string          `json:"reply_remote"`
	RouteGeneration uint64          `json:"route_generation"`
	RulesDigest     string          `json:"rules_digest"`
	Raw             json.RawMessage `json:"raw"`
}

// InfraPhysical preserves the S9 attachment and independent socket census. For
// WSS, Upstream is the Caddy-owned outbound socket; ServerAccepted belongs to
// NATS and ServerID/CID come from that server's connection observation. For raw
// NATS, Upstream is nil and Accepted equals ServerAccepted. No URL is a witness.
type InfraPhysical struct {
	Translation    *InfraTranslation `json:"translation"`
	Attachment     c.Attachment      `json:"attachment"`
	Accepted       c.Socket          `json:"accepted"`
	Upstream       *c.Socket         `json:"upstream"`
	ServerAccepted c.Socket          `json:"server_accepted"`
	ServerID       string            `json:"server_id"`
	CID            uint64            `json:"cid"`
	Census         c.Census          `json:"census"`
}

// InfraProbe is a first-attempt result for one predeclared canonical slot. P19
// retains the slot denominator; collection cannot trim failed or missing probes.
// Raw/Accepted/Census are the physical ingress path, including no-hint clients.
type InfraProbe struct {
	Translation       *InfraTranslation `json:"translation"`
	ID                string            `json:"id"`
	Kind              string            `json:"kind"`
	StartedBootNS     int64             `json:"started_boot_ns"`
	FinishedBootNS    int64             `json:"finished_boot_ns"`
	Attempts          uint64            `json:"attempts"`
	RC                int               `json:"rc"`
	Authenticated     bool              `json:"authenticated"`
	Hinted            bool              `json:"hinted"`
	URL               string            `json:"url"`
	TLSName           string            `json:"tls_name"`
	CertificateSHA256 string            `json:"certificate_sha256"`
	RouteGeneration   uint64            `json:"route_generation"`
	Raw               c.Socket          `json:"raw"`
	Accepted          c.Socket          `json:"accepted"`
	Census            c.Census          `json:"census"`
}

// InfraObservation is supplied by the actual S9 witness/admin adapter. Stamp
// must be the fresh upstream query response, not the caller's requested stamp.
// Status retains every native field, including incomplete census/diagnostics.
// A missing upstream producer returns an error, never an empty observation.
// InfraSignal retains the actual host receipt, joined to signal_applied by its
// RequestDigest. ObservedBootNS belongs to the same target host as S9 events.
type InfraSignal struct {
	Receipt        c.HostReceipt `json:"receipt"`
	ObservedBootNS int64         `json:"observed_boot_ns"`
}

type InfraObservation struct {
	Stamp    contract.Stamp
	Status   json.RawMessage
	Physical []InfraPhysical
	Probes   []InfraProbe
	Raw      []contract.RawRef
	Events   []c.Observation
	Signals  []InfraSignal
	Outbound []InfraPhysical
}

type InfraRead func(context.Context, c.Plan, []c.Attachment, string, c.Deadline) (InfraObservation, error)

type InfraOwner struct {
	Role    string    `json:"role"`
	Process c.Process `json:"process"`
}

type InfraOwnerExit struct {
	Role           string    `json:"role"`
	Process        c.Process `json:"process"`
	Exited         bool      `json:"exited"`
	Source         string    `json:"source"`
	ObservedBootNS int64     `json:"observed_boot_ns"`
}

type infraBindingDisk struct {
	WorkloadID  string          `json:"workload_id"`
	Leg         string          `json:"leg"`
	Participant c.ID            `json:"participant"`
	Original    json.RawMessage `json:"original"`
}

type infraRecord struct {
	Schema      string             `json:"schema"`
	Tier        string             `json:"tier"`
	Stage       string             `json:"stage"`
	Stamp       json.RawMessage    `json:"stamp"`
	Plan        c.Plan             `json:"plan"`
	Checkpoint  c.Checkpoint       `json:"checkpoint"`
	Bindings    []infraBindingDisk `json:"bindings"`
	Attachments []c.Attachment     `json:"attachments"`
	Physical    []InfraPhysical    `json:"physical"`
	Status      json.RawMessage    `json:"status"`
	Executing   c.Process          `json:"executing"`
	Probes      []InfraProbe       `json:"probes"`
	Owners      []InfraOwnerExit   `json:"owners"`
	Raw         []json.RawMessage  `json:"raw"`
	Events      []c.Observation    `json:"events"`
	Signals     []InfraSignal      `json:"signals"`
	Outbound    []InfraPhysical    `json:"outbound"`
}

type infraHeldOwner struct {
	owner InfraOwner
	pin   *infrahost.ProcessPin
}

// InfraCollector owns only observer pidfds and immutable raw files. It never
// signals a service, changes a URL/hosts entry, reconnects an NC, or launches a
// process. Source processes are supervised by P05. Calls are synchronous; source
// adapters must honor both ctx and the original S9 absolute deadline.
// origin: hot-update S10 plan §5.4 / §10.2 P15; lead L15.
type InfraCollector struct {
	mu       sync.Mutex
	root     string
	tier     string
	registry *ingressmove.Registry
	read     InfraRead
	ctx      context.Context
	cancel   context.CancelFunc
	owners   []infraHeldOwner
	total    int64
	closed   bool
	failure  error
}

const (
	infraRecordLimit  = 4 << 20
	infraArchiveLimit = 256 << 20
)

func NewInfraCollector(profile contract.Profile, registry *ingressmove.Registry, read InfraRead) (*InfraCollector, error) {
	if registry == nil || read == nil || !filepath.IsAbs(profile.EvidenceRoot) ||
		(profile.Tier != "hermetic" && profile.Tier != "deploy") {
		return nil, errors.New("fullstack: infra collector requires real sources and an absolute evidence root")
	}
	info, err := os.Stat(profile.EvidenceRoot)
	if err != nil || !info.IsDir() {
		return nil, errors.New("fullstack: infra evidence directory unavailable")
	}
	// ctx-root: collector reads end at Close, independently of one query context.
	ctx, cancel := context.WithCancel(context.Background())
	return &InfraCollector{root: profile.EvidenceRoot, tier: profile.Tier, registry: registry, read: read, ctx: ctx, cancel: cancel}, nil
}

// HoldOwner must precede retirement, including for temporary Caddy and support
// workers discovered later in this same step. Opening a pidfd after disappearance
// cannot prove which incarnation exited. P19 supplies the sealed owner roster;
// capture does not infer roster completeness from whatever pins remain.
// origin: hot-update S10 plan §5.4 / §8 M08C / §10.2 P15.
func (r *InfraCollector) HoldOwner(owner InfraOwner, deadline c.Deadline) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return os.ErrClosed
	}
	if r.failure != nil {
		return r.failure
	}
	if !slices.Contains([]string{"nats", "caddy", "temporary-caddy", "helper", "launcher"}, owner.Role) {
		return r.fail(errors.New("fullstack: unknown retiring infra owner role"))
	}
	for _, held := range r.owners {
		if held.owner.Process == owner.Process {
			return r.fail(errors.New("fullstack: duplicate retiring infra owner"))
		}
	}
	pin, err := infrahost.PinProcess(owner.Process, deadline)
	if err != nil {
		return r.fail(err)
	}
	r.owners = append(r.owners, infraHeldOwner{owner: owner, pin: pin})
	return nil
}

// DecodeInfraStatus requires S9's exact native field closure, including array
// widths, spelling and order. Re-encoding rejects duplicates/aliases/missing
// fields that encoding/json alone accepts. The original bytes are still archived.
// origin: hot-update S10 plan §5.1 / §5.2 / §10.2 P15.
func DecodeInfraStatus(raw []byte) (c.Status, error) {
	var status c.Status
	if len(raw) == 0 || len(raw) > infraRecordLimit || !utf8.Valid(raw) {
		return status, errors.New("fullstack: invalid infra status bytes")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&status); err != nil {
		return c.Status{}, errors.New("fullstack: invalid infra status shape")
	}
	encoded, err := json.Marshal(status)
	if err != nil || !bytes.Equal(raw, encoded) || status.Phase < c.Prepared || status.Phase > c.Halted ||
		status.Tx == (c.ID{}) || status.Step == 0 || status.Revision == 0 {
		return c.Status{}, errors.New("fullstack: noncanonical or unknown infra status")
	}
	return status, nil
}

// S9 releases the cohort before publishing Completed. Snapshot needs an active
// seal, whereas both ACK ledgers deliberately survive Release until the next
// Seal. Post-completion reads therefore join the retained return ledger to the
// fresh physical source and verify the exact one-step release revision twice.
// A later admission/new seal invalidates this read; no observer reacquires a
// mutation seal or fabricates an active checkpoint to make sampling work.
// origin: hot-update S10 plan §5.4 / §10.2 P15; S9 Registry.Release.
func infraAttachmentSnapshot(registry *ingressmove.Registry, cp c.Checkpoint, stage string, deadline c.Deadline) ([]c.Attachment, error) {
	if stage != "completed" {
		return registry.Snapshot(cp, deadline)
	}
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return nil, err
	}
	ledger, err := registry.Ledger(c.RouteReturn)
	if err != nil {
		return nil, err
	}
	return infraReleasedAttachments(cp, ledger)
}

func infraReleasedAttachments(cp c.Checkpoint, ledger ingressmove.Ledger) ([]c.Attachment, error) {
	if cp.Assets.Revision == ^uint64(0) || ledger.Revision != cp.Assets.Revision+1 ||
		len(ledger.Bound) == 0 || len(ledger.Bound) != len(ledger.Attached) ||
		len(ledger.Bound) != len(ledger.Installed) || !slices.Equal(ledger.Bound, ledger.Attached) {
		return nil, ErrIdentityChanged
	}
	for i, bound := range ledger.Bound {
		if bound.Participant == (c.ID{}) || ledger.Installed[i].Participant != bound.Participant {
			return nil, c.ErrIncomplete
		}
	}
	return slices.Clone(ledger.Bound), nil
}

func encodeInfraBindings(bindings []InfraBinding, attachments []c.Attachment) ([]infraBindingDisk, error) {
	if len(bindings) == 0 {
		return nil, c.ErrIncomplete
	}
	rows := make([]infraBindingDisk, 0, len(bindings))
	seen := make(map[[2]string]bool)
	for _, binding := range bindings {
		key := [2]string{binding.WorkloadID, binding.Leg}
		if binding.WorkloadID == "" || seen[key] || !slices.Contains([]string{"flow", "sender", "receiver"}, binding.Leg) ||
			!slices.ContainsFunc(attachments, func(a c.Attachment) bool { return a.Participant == binding.Participant }) {
			return nil, c.ErrIncomplete
		}
		seen[key] = true
		original, err := contract.EncodeJSON("process", binding.Original)
		if err != nil {
			return nil, err
		}
		rows = append(rows, infraBindingDisk{binding.WorkloadID, binding.Leg, binding.Participant, original})
	}
	return rows, nil
}

// Capture samples the real Registry twice around the independent read. A late
// admission or a changed socket invalidates this read; callers may retry only
// read-only sampling, never repeat an unsafe upgrade action. Each returned ref
// retains full S9 DTOs alongside P00's snake_case stamp/raw codec.
// origin: hot-update S10 plan §5.1 / §5.4 / §10.2 P15.
func (r *InfraCollector) Capture(ctx context.Context, plan c.Plan, checkpoint c.Checkpoint, stage, nonce string, bindings []InfraBinding, deadline c.Deadline) (_ contract.RawRef, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return contract.RawRef{}, os.ErrClosed
	}
	if r.failure != nil {
		return contract.RawRef{}, r.failure
	}
	defer func() {
		if err != nil && !errors.Is(err, ingressmove.ErrBusy) && !errors.Is(err, ErrIdentityChanged) {
			err = r.fail(err)
		}
	}()
	if ctx.Err() != nil {
		return contract.RawRef{}, ctx.Err()
	}
	if nonce == "" || !slices.Contains([]string{"before", "outbound", "returned", "completed"}, stage) ||
		(plan.Component != c.NATS && plan.Component != c.Caddy) {
		return contract.RawRef{}, c.ErrIncomplete
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := r.ctx.Err(); err != nil {
		return contract.RawRef{}, err
	}
	attachments, err := infraAttachmentSnapshot(r.registry, checkpoint, stage, deadline)
	if err != nil {
		return contract.RawRef{}, err
	}
	rows, err := encodeInfraBindings(bindings, attachments)
	if err != nil {
		return contract.RawRef{}, err
	}
	observed, err := r.read(call, plan, slices.Clone(attachments), nonce, deadline)
	if err != nil {
		return contract.RawRef{}, err
	}
	status, err := DecodeInfraStatus(observed.Status)
	if err != nil {
		return contract.RawRef{}, err
	}
	stamp, err := contract.EncodeJSON("stamp", observed.Stamp)
	if err != nil {
		return contract.RawRef{}, err
	}
	direction := "forward"
	if plan.Action == c.Rollback {
		direction = "reverse"
	}
	if status.Tx != plan.Tx || status.Step != plan.Step || status.Before != plan.Before ||
		observed.Stamp.QueryNonce != nonce || observed.Stamp.TxID != hex.EncodeToString(plan.Tx[:]) ||
		observed.Stamp.Process.BootID != plan.Before.BootID || observed.Stamp.Direction != direction || observed.Stamp.Revision != status.Revision || len(observed.Raw) == 0 {
		return contract.RawRef{}, c.ErrIncomplete
	}
	// Independently hash the executing /proc inode through S9's pidfd reader.
	// A disk SHA or product status is never substituted for this observation.
	executing, err := infrahost.ReadProcess(status.Current.PID, deadline)
	if err != nil || executing != status.Current {
		return contract.RawRef{}, errors.Join(ErrIdentityChanged, err)
	}
	if len(observed.Physical) != len(attachments) || observed.Probes == nil || observed.Events == nil || observed.Signals == nil || observed.Outbound == nil {
		return contract.RawRef{}, c.ErrIncomplete
	}
	for i, physical := range observed.Physical {
		if physical.Attachment != attachments[i] || !physical.Census.Complete || physical.Census.Digest == (c.Digest{}) {
			return contract.RawRef{}, c.ErrIncomplete
		}
	}
	if stage == "completed" && plan.Action != c.Reload {
		ledger, err := r.registry.Ledger(c.RouteOutbound)
		if err != nil || len(ledger.Bound) != len(observed.Outbound) || len(ledger.Bound) == 0 {
			return contract.RawRef{}, errors.Join(c.ErrIncomplete, err)
		}
		for i, proof := range observed.Outbound {
			if proof.Attachment != ledger.Bound[i] {
				return contract.RawRef{}, c.ErrIncomplete
			}
		}
		returned, err := r.registry.Ledger(c.RouteReturn)
		if err != nil || !slices.Equal(returned.Bound, attachments) {
			return contract.RawRef{}, errors.Join(c.ErrIncomplete, err)
		}
	}
	after, err := infraAttachmentSnapshot(r.registry, checkpoint, stage, deadline)
	if err != nil || !slices.Equal(attachments, after) {
		return contract.RawRef{}, errors.Join(c.ErrIncomplete, err)
	}
	current, err := infrahost.ReadProcess(executing.PID, deadline)
	if err != nil || current != executing || call.Err() != nil {
		return contract.RawRef{}, errors.Join(ErrIdentityChanged, err, call.Err())
	}
	owners := make([]InfraOwnerExit, 0, len(r.owners))
	for _, held := range r.owners {
		exited, err := held.pin.Exited(deadline)
		if err != nil {
			return contract.RawRef{}, err
		}
		at, err := controlbudget.Now()
		if err != nil {
			return contract.RawRef{}, err
		}
		owners = append(owners, InfraOwnerExit{held.owner.Role, held.owner.Process, exited, "held-pidfd", at})
	}
	raw := make([]json.RawMessage, 0, len(observed.Raw))
	for _, ref := range observed.Raw {
		encoded, err := contract.EncodeJSON("raw_ref", ref)
		if err != nil {
			return contract.RawRef{}, err
		}
		raw = append(raw, encoded)
	}
	return r.archive(infraRecord{Schema: "fullstack-infra-v1", Tier: r.tier, Stage: stage,
		Stamp: stamp, Plan: plan, Checkpoint: checkpoint, Bindings: rows, Attachments: attachments,
		Physical: observed.Physical, Status: observed.Status, Executing: executing, Probes: observed.Probes,
		Owners: owners, Raw: raw, Events: observed.Events, Signals: observed.Signals, Outbound: observed.Outbound})
}

func (r *InfraCollector) archive(record infraRecord) (contract.RawRef, error) {
	data, err := json.Marshal(record)
	if err != nil || len(data) > infraRecordLimit || r.total+int64(len(data)) > infraArchiveLimit {
		return contract.RawRef{}, errors.New("fullstack: infra raw archive ceiling")
	}
	file, err := os.CreateTemp(r.root, "infra-*.json")
	if err != nil {
		return contract.RawRef{}, err
	}
	_, writeErr := file.Write(data)
	err = errors.Join(writeErr, file.Sync(), file.Close())
	if err != nil {
		_ = os.Remove(file.Name())
		return contract.RawRef{}, err
	}
	r.total += int64(len(data))
	return contract.RawRef{Path: file.Name(), SHA: sha256.Sum256(data), Bytes: int64(len(data)), Schema: record.Schema}, nil
}

func (r *InfraCollector) fail(err error) error {
	if r.failure == nil {
		r.failure = err
	}
	return r.failure
}

// Drain releases this cut's observer pins after its raw judgment. Keeping one
// collector through the rollout must not retain every predecessor pidfd until
// the final step. It preserves archived receipts and sticky source failures.
// origin: hot-update S10 plan §2.1 / §5.4 / §10.2 P15.
func (r *InfraCollector) Drain(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return os.ErrClosed
	}
	r.releasePins()
	return r.failure
}

func (r *InfraCollector) releasePins() {
	for _, held := range r.owners {
		if err := held.pin.Close(); err != nil {
			r.failure = errors.Join(r.failure, err)
		}
	}
	r.owners = nil
}

// Close releases every observer pin even when one close fails. Sticky failures
// survive cleanup; observing no survivor after cleanup cannot erase a bad cut.
// P22/P25 exercise these calls directly in their five-round leak assertions.
// origin: hot-update S10 plan §11.2 / §10.2 P15.
func (r *InfraCollector) Close() error {
	// Cancel before taking the operation mutex: a source blocked in TLS/WSS must
	// release its read and join workers before Close can release observer pins.
	// Context cancellation is concurrency-safe and owns no business lifetime.
	if r.cancel != nil {
		r.cancel()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		r.releasePins()
	}
	return r.failure
}

func (r *InfraCollector) Join(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		return errors.New("fullstack: infra collector Close must precede Join")
	}
	return r.failure
}
