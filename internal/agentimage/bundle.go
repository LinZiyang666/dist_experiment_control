// Package agentimage composes the agent's runtime-layer state into one
// versioned bundle for a same-PID image switch and takes it apart again on
// the other side. It owns the ORDER of the cut (the order in which the
// execsession runtime, the shared process table, the spawn-safety policy
// and the proc-event courier are frozen, exported, imported and thawed) and
// the fail-closed rules that make an import all-or-nothing; each domain's
// own schema and validation stay in its package.
//
// The bundle is metadata only: every byte-carrying object (replay rings,
// pipes) travels as a descriptor through internal/handoff. The bundle
// contains the request registry's signing secret (inside Runtime.Registry),
// so its JSON may only ever be written into handoff's sealed anonymous
// snapshot — never logged, never put in a marker file. String() prints a
// summary for that reason.
//
// Interfaces here are deadline / channel based, never context based: a
// data-plane object bound to a caller's context is gotcha #80 (a control
// session rebuild killed a live exit for 7h40m), and the S5 data-plane
// domain that will join this bundle is forbidden from accepting a ctx by
// the dataplane_lifetime gate.
// origin: docs/reviews/hot-update-agent-image-abi-plan.md §3.
package agentimage

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/agentimage/transfercontract"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
)

const (
	// Role is the handoff.State.Role every agent image is dispatched under;
	// Decode refuses any other role so a bundle meant for another owner
	// (a broker image, a test harness) is never activated as the agent's.
	Role = "agent"
	// BundleVersion is the schema epoch of Bundle: proto.ImageABIVersion,
	// the number `tether version` prints on its last line so an upgrading
	// agent can refuse a candidate that cannot read its image.
	BundleVersion = proto.ImageABIVersion
	// MaxBundleBytes bounds the encoded bundle (handoff's own 64 MiB is the
	// outer bound; a bundle near it would mean bytes leaked into metadata).
	MaxBundleBytes = 16 << 20
	// maxHandoffFiles mirrors handoff's descriptor bound so an oversized
	// cut is refused before thousands of descriptors are duplicated.
	maxHandoffFiles = 16384
)

// Deferral reason codes: the deferred set the agent's upgrade preflight
// reports to the operator (architecture §22.6). Each names exactly one
// condition. The first six are raised by this package's Freeze; the remaining
// reasons by the agent's ownership checks for domains later stages absorb
// (each of those stages removes its code from the set; S10 asserts the set
// is empty but for legacy_session_busy, the documented permanent member).
const (
	ReasonSpawnInFlight          = "spawn_in_flight"
	ReasonRemoteFSProbeInFlight  = "remote_fs_probe_in_flight"
	ReasonForeignProcessRows     = "foreign_process_rows"
	ReasonImageTooLarge          = "image_too_large"
	ReasonRuntimeClosed          = "runtime_closed"
	ReasonPlatformUnsupported    = "platform_unsupported"
	ReasonPTYBusy                = "pty_busy"
	ReasonFlowBusy               = "flow_busy"
	ReasonSSBusy                 = "ss_busy"
	ReasonTransfersInFlight      = transfercontract.ReasonTransfersInFlight
	ReasonLegacySessionBusy      = "legacy_session_busy"
	ReasonSSStateUnrepresentable = "ss_state_unrepresentable"
)

// DeferredReasons is the documented deferred set, in the order §22.8 lists
// it. A fixed-size array so that adding a member is a visible edit of the
// contract, not a silent append.
// Representable Flow/SS owners participate in the cut; legacy owners still defer.
// origin: hot-update S5 internal review rv-authority-7
var DeferredReasons = [9]string{
	ReasonSpawnInFlight, ReasonRemoteFSProbeInFlight, ReasonForeignProcessRows, ReasonImageTooLarge,
	ReasonRuntimeClosed, ReasonPlatformUnsupported,
	ReasonTransfersInFlight, ReasonLegacySessionBusy, ReasonSSStateUnrepresentable,
}

// DeferredError retains the shared error identity: errors.As must recognize
// an adapter's refusal before the composition wraps other domain failures.
// origin: S7 internal review IMAGE-LEAF F1
type DeferredError = transfercontract.DeferredError

// ErrBundle wraps every bundle-level validation refusal.
var ErrBundle = errors.New("agent image: invalid bundle")

// Lineage identifies which agent generation exported the bundle and on
// which boot; InstanceID/RoutingNID are the agent's identities that also
// travel through the environment, so a bootstrap can cross-check them.
type Lineage struct {
	Generation      uint64 `json:"generation"`
	BootID          string `json:"boot_id"`
	ProtoVersion    int    `json:"proto_version"`
	InstanceID      string `json:"instance_id,omitempty"`
	RoutingNID      string `json:"routing_nid,omitempty"`
	ExporterRelease string `json:"exporter_release,omitempty"`
}

// Matches reports whether the environment-carried identities agree with
// the bundle's (an empty bundle value matches anything: S2 leaves them
// unset, S3 fills them).
func (l Lineage) Matches(instanceID, routingNID string) bool {
	return (l.InstanceID == "" || l.InstanceID == instanceID) && (l.RoutingNID == "" || l.RoutingNID == routingNID)
}

// Bundle is the composed image. Every domain carries its own version.
type Bundle struct {
	// Calibration travels as an optional handoff resource, outside the strict
	// S3 bundle schema. In-process cuts retain the same validated value here.
	// origin: hot-update S5 plan §7.1 / DRILL-RUN S3 compatibility
	admissionAt int64

	Version   int                   `json:"version"`
	Instance  string                `json:"instance"`
	Lineage   Lineage               `json:"lineage"`
	Processes workload.ProcessState `json:"processes"`
	Runtime   execsession.Image     `json:"runtime"`
	Courier   CourierState          `json:"courier"`
	SpawnSafe *spawnsafe.State      `json:"spawn_safe,omitempty"`
	// A nonnil domain retains responsibility even with no active connections.
	// origin: hot-update S5 plan §5.1 / T33
	Flow *tunnel.ClientImage `json:"flow,omitempty"`
	SS   *ssproxy.Image      `json:"ss,omitempty"`
	// The detailed registry, outcomes and settlement credentials travel only
	// in the sealed transfer resource, including prepared and courier-only cuts.
	// origin: hot-update S7 plan §8.2–§8.4 / P21
	Transfers *xfervalue.ImageRef `json:"transfers,omitempty"`
}

// String is a summary: the bundle carries a signing secret and must never
// be printed whole.
func (b Bundle) String() string {
	return fmt.Sprintf("agentimage.Bundle{version=%d instance=%s generation=%d tasks=%d rows=%d courier=%d}",
		b.Version, b.Instance, b.Lineage.Generation, len(b.Runtime.Tasks), len(b.Processes.Records), len(b.Courier.Events))
}

// Validate is the pure, cross-domain half of an import: everything that
// can be refused before a single descriptor is taken. Domain internals
// (the registry table, each stream's state) are validated by their own
// packages when the domain is imported.
func (b Bundle) Validate() error {
	if b.admissionAt < 0 || b.admissionAt > b.Runtime.FrozenAtBootNS {
		return fmt.Errorf("%w: admission is outside the accounting interval", ErrBundle)
	}
	if b.Version != BundleVersion {
		return fmt.Errorf("%w: bundle version %d, this binary reads %d", ErrBundle, b.Version, BundleVersion)
	}
	if b.Instance == "" || b.Instance != b.Runtime.Instance {
		return fmt.Errorf("%w: instance %q does not name the runtime image", ErrBundle, b.Instance)
	}
	if b.Lineage.Generation == 0 {
		return fmt.Errorf("%w: generation 0", ErrBundle)
	}
	if b.Lineage.ProtoVersion != proto.ProtoVersion {
		return fmt.Errorf("%w: exporter wire epoch %d, this binary speaks %d", ErrBundle, b.Lineage.ProtoVersion, proto.ProtoVersion)
	}
	boot, err := procrequest.CurrentBootID()
	if err != nil {
		return fmt.Errorf("%w: boot identity: %v", ErrBundle, err)
	}
	if b.Lineage.BootID != boot || b.Runtime.Registry.BootID != boot || b.Processes.BootID != boot {
		return fmt.Errorf("%w: bundle was exported on another boot", ErrBundle)
	}
	if len(b.Lineage.ExporterRelease) > 64 || len(b.Lineage.InstanceID) > 256 || len(b.Lineage.RoutingNID) > 256 {
		return fmt.Errorf("%w: lineage texts exceed their bounds", ErrBundle)
	}
	if b.Runtime.Version != execsession.ImageVersion {
		return fmt.Errorf("%w: runtime image version %d", ErrBundle, b.Runtime.Version)
	}
	tasks := make(map[string]bool, len(b.Runtime.Tasks))
	for _, t := range b.Runtime.Tasks {
		tasks[t.PID] = true
	}
	for _, row := range b.Processes.Records {
		if !tasks[row.ID] {
			return &DeferredError{Reason: ReasonForeignProcessRows, Detail: "process row " + row.ID + " belongs to no exported task",
				Cause: fmt.Errorf("%w: foreign process row", ErrBundle)}
		}
	}
	if err := b.Courier.Validate(); err != nil {
		return fmt.Errorf("%w: courier: %v", ErrBundle, err)
	}
	if b.SpawnSafe != nil {
		if err := b.SpawnSafe.Validate(); err != nil {
			return fmt.Errorf("%w: spawn safety: %v", ErrBundle, err)
		}
	}
	if err := validateFlowSS(b); err != nil {
		return err
	}
	if ref := b.Transfers; ref != nil {
		if ref.Schema != xfervalue.StateSchema || !cutRef(ref.ResourceID, "xfer-state.") ||
			!validTransferDigest(ref.Digest) || ref.EncodedBytes == 0 || ref.EncodedBytes > sealedstate.MaxStateBytes {
			return fmt.Errorf("%w: inconsistent transfer metadata", ErrBundle)
		}
	}
	// origin: hot-update S4 plan §5.1 — the domain owner validates each
	// kind's exact file set. Count that set, not six hypothetical exec FDs:
	// a closed PTY master and a terminal task retain fewer descriptors.
	files := 0
	for _, task := range b.Runtime.Tasks {
		files += len(task.Files)
		if files > maxHandoffFiles {
			return &DeferredError{Reason: ReasonImageTooLarge, Detail: fmt.Sprintf("%d files exceed the descriptor bound", files)}
		}
	}
	return nil
}

// RequiredDomains describes the retained state of this exact bundle, including
// settled registry entries and revocation responsibility with no live task.
// origin: hot-update S4 plan §5.2
func (b Bundle) RequiredDomains() []string {
	domains := execsession.RequiredDomains(b.Runtime)
	if b.Flow != nil {
		domains = append(domains, "flow-v1")
	}
	if b.SS != nil {
		domains = append(domains, "ss-v1")
	}
	// A zero byte cursor or an empty entry projection does not retire a domain.
	// origin: hot-update S7 plan §8.4 / P21
	if b.Transfers != nil {
		domains = append(domains, xfervalue.Version)
	}
	return domains
}

func validTransferDigest(digest string) bool {
	decoded, err := hex.DecodeString(digest)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == digest
}

// The codec owns field/phase validation. Composition additionally proves that
// its references actually enter this cut and share the node's existing limits.
// origin: hot-update S7 plan §8.2–§8.3 / P21
func validateTransferState(b Bundle, state TransferState, resources []handoff.Resource) error {
	if state.Schema != xfervalue.StateSchema || state.BootID != b.Lineage.BootID ||
		state.Generation == 0 || !state.AdmissionClosed || state.Inflight != 0 || len(state.Encoded) == 0 {
		return fmt.Errorf("%w: inconsistent transfer lineage or admission", ErrBundle)
	}
	instance := b.Lineage.InstanceID
	if instance == "" {
		instance = b.Instance
	}
	if state.InstanceID != instance {
		return fmt.Errorf("%w: transfer belongs to another agent", ErrBundle)
	}
	var used xfervalue.Reservation
	limits := xfervalue.Reservation{ReplayBytes: workload.DefaultReplayBudget, NonReplayBytes: 64 << 20,
		SealedBytes: sealedstate.MaxStateBytes, PublicBytes: MaxBundleBytes, HandoffBytes: 64 << 20}
	used.ReplayBytes = b.Runtime.BudgetUsed
	if b.Flow != nil {
		used.ReplayBytes += b.Flow.Flows.ReservedBytes
	}
	for _, r := range state.Reservations {
		for _, pair := range [][3]int64{
			{used.ReplayBytes, r.ReplayBytes, limits.ReplayBytes}, {used.NonReplayBytes, r.NonReplayBytes, limits.NonReplayBytes},
			{used.SealedBytes, r.SealedBytes, limits.SealedBytes}, {used.PublicBytes, r.PublicBytes, limits.PublicBytes},
			{used.HandoffBytes, r.HandoffBytes, limits.HandoffBytes},
		} {
			if pair[0] < 0 || pair[1] < 0 || pair[0] > pair[2] || pair[1] > pair[2]-pair[0] {
				return &DeferredError{Reason: ReasonImageTooLarge, Detail: "combined transfer reservation exceeds the cut bound"}
			}
		}
		used.ReplayBytes += r.ReplayBytes
		used.NonReplayBytes += r.NonReplayBytes
		used.SealedBytes += r.SealedBytes
		used.PublicBytes += r.PublicBytes
		used.HandoffBytes += r.HandoffBytes
	}
	refs := transferResourceRefs(state)
	for _, ref := range refs {
		// A reference must match one of the three (kind, basename) pairs exactly.
		if ref.ResourceID == "" ||
			((ref.Kind != xfervalue.ResourceSource || ref.Name != xfervalue.SourceFileName) &&
				(ref.Kind != xfervalue.ResourceTmp || ref.Name != xfervalue.TmpFileName) &&
				(ref.Kind != xfervalue.ResourceParent || ref.Name != xfervalue.ParentFileName)) {
			return fmt.Errorf("%w: transfer resource kind or name differs", ErrBundle)
		}
	}
	files := make(map[string]*os.File)
	seen := make(map[*os.File]bool)
	for _, resource := range resources {
		for name, file := range resource.Files {
			key := resource.ID + "/" + name
			ref, ok := refs[key]
			if !ok || ref.Kind != resource.Kind || len(resource.Data) != 0 || file == nil || files[key] != nil || seen[file] {
				return fmt.Errorf("%w: transfer resource closure differs", ErrBundle)
			}
			files[key], seen[file] = file, true
		}
	}
	if len(files) != len(refs) {
		return fmt.Errorf("%w: transfer resource is missing", ErrBundle)
	}
	return nil
}

func transferResourceRefs(state TransferState) map[string]xfervalue.ResourceRef {
	refs := make(map[string]xfervalue.ResourceRef)
	for _, ref := range state.Refs {
		if ref != (xfervalue.ResourceRef{}) {
			refs[ref.ResourceID+"/"+ref.Name] = ref
		}
	}
	return refs
}

// validateFlowSS checks public bounds and typed references without taking a FD.
// Deep private-state validation stays with each domain's sealed reader.
// origin: hot-update S5 plan §3.4 / §5.1
func validateFlowSS(b Bundle) error {
	invalid := func() error { return fmt.Errorf("%w: inconsistent flow/SS metadata", ErrBundle) }
	reserved := b.Runtime.BudgetUsed
	if reserved < 0 {
		return invalid()
	}
	if b.Flow != nil && reserved > workload.DefaultReplayBudget {
		return &DeferredError{Reason: ReasonImageTooLarge, Detail: "runtime replay reservation exceeds the node bound"}
	}
	if im := b.Flow; im != nil {
		if im.Version != flow.Version || im.Flows.Version != flow.Version || !cutRef(im.StateRef, "flow-state.") || !cutRef(im.Flows.StateRef, "flow-state.") || len(im.Endpoints) > tunnel.MaxEndpoints || !flowImageCapacity(im.Flows) || im.Flows.ReservedBytes < 0 {
			return invalid()
		}
		endpoints := make(map[flow.EndpointID]bool, len(im.Endpoints))
		for _, ep := range im.Endpoints {
			// Endpoint records may share the client's aggregate sealed cut. An
			// arbitrary flow-state ref could name another cut, so only this exact
			// aggregate ref or a typed endpoint resource is accepted here. Restore
			// then checks the public projection against the sealed records.
			// origin: hot-update S5 plan §5.1 / §12.1 / T33
			if ep.ID == (flow.EndpointID{}) || endpoints[ep.ID] || ep.OwnerGen == 0 || len(ep.FlowIDs) > len(im.Flows.Records) || (ep.StateRef != im.StateRef && !cutRef(ep.StateRef, "flow-endpoint.")) {
				return invalid()
			}
			endpoints[ep.ID] = true
		}
		// Check before addition so malicious metadata cannot wrap the sum.
		if im.Flows.ReservedBytes > workload.DefaultReplayBudget-reserved {
			return &DeferredError{Reason: ReasonImageTooLarge, Detail: "combined replay reservation exceeds the node bound"}
		}
	}
	if im := b.SS; im != nil {
		if im.Version != ssproxy.Version || !cutRef(im.StateRef, "ss-state.") || im.ListenerRef != "" && !cutRef(im.ListenerRef, "ss-listener.") || im.ConnectionCount < 0 || im.ConnectionCount > ssproxy.MaxActors || im.SecretBytes < 0 || im.SecretBytes > sealedstate.MaxStateBytes {
			return invalid()
		}
	}
	return nil
}

// The active reservation and retained terminal proof have independent quotas.
// origin: hot-update S5 internal review rv-image-2
func flowImageCapacity(im flow.Image) bool {
	active, terminal := 0, 0
	for _, record := range im.Records {
		if record.Phase == flow.FlowPhaseTerminal {
			terminal++
		} else {
			active++
		}
	}
	return active <= flow.MaxFlows && terminal <= flow.MaxTerminalMetadata
}

// Only typed, canonical IDs enter the manifest. A target/key string must never
// be smuggled into an otherwise opaque reference or an import error.
func cutRef(ref, prefix string) bool {
	suffix, ok := strings.CutPrefix(ref, prefix)
	if !ok || len(suffix) != 32 {
		return false
	}
	raw, err := hex.DecodeString(suffix)
	return err == nil && hex.EncodeToString(raw) == suffix
}

// Decode parses a bundle strictly (unknown fields refused, trailing data
// refused) and validates it. It is pure: no descriptor is touched.
func Decode(state handoff.State) (Bundle, error) {
	dec := json.NewDecoder(bytes.NewReader(state.Data))
	dec.DisallowUnknownFields()
	var b Bundle
	if err := dec.Decode(&b); err != nil {
		return Bundle{}, fmt.Errorf("%w: decode: %v", ErrBundle, err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return Bundle{}, fmt.Errorf("%w: trailing data", ErrBundle)
	}
	if state.Role != Role || state.Instance != b.Instance || state.Generation != b.Lineage.Generation {
		return Bundle{}, fmt.Errorf("%w: handoff envelope names role %q instance %q generation %d, bundle says %q %q %d",
			ErrBundle, state.Role, state.Instance, state.Generation, Role, b.Instance, b.Lineage.Generation)
	}
	if err := b.Validate(); err != nil {
		return Bundle{}, err
	}
	return b, nil
}

// Encode serialises a bundle for a handoff.State (the sealed memfd is the
// only place its bytes may go — they carry the registry key). It is the
// one serialisation point and enforces the size bound; agentboot uses it to
// repack an in-process cut.
func Encode(b Bundle) ([]byte, error) { return encode(b) }

// encode is the one place the bundle is serialised; it enforces the size bound.
func encode(b Bundle) ([]byte, error) {
	data, err := json.Marshal(b)
	if err != nil {
		return nil, fmt.Errorf("%w: encode: %v", ErrBundle, err)
	}
	if len(data) > MaxBundleBytes {
		return nil, &DeferredError{Reason: ReasonImageTooLarge, Detail: fmt.Sprintf("bundle is %d bytes", len(data))}
	}
	return data, nil
}

// Domain counts come from the final actual owner cut, not shared budget Used.
// FD fingerprints let the external keeper count remaining OFD duplicates live.
// origin: hot-update S5 plan §3.4 / §9.1 / DRILL-OBSERVER
// DomainObservations projects only public counts and fingerprints. The caller
// retains the sole emission hook, preserving the original event order.
// origin: hot-update S7 plan §11.1 / P21; moved from agent.observeImageDomains
func DomainObservations(b Bundle) []drillobserve.Event {
	var events []drillobserve.Event
	counts := map[string]*drillobserve.Event{}
	for _, name := range []string{"exec", "pty", "flow", "ss"} {
		counts[name] = &drillobserve.Event{Kind: "domain-cut", Domain: name, Values: [16]uint64{b.Lineage.Generation}}
	}
	for _, task := range b.Runtime.Tasks {
		domain := "exec"
		if task.PTY != nil {
			domain = "pty"
		}
		e := counts[domain]
		for _, fp := range task.Files {
			events = append(events, drillobserve.Event{Kind: "domain-file", Domain: domain, Values: [16]uint64{fp.Dev, fp.Ino, 0}})
			e.Values[1]++
		}
		if task.PTY != nil {
			e.Values[2] += uint64(task.PTY.Input.Capacity + task.PTY.Output.Capacity)
			e.Values[3] += task.PTY.Input.End - task.PTY.Input.Base + task.PTY.Output.End - task.PTY.Output.Base
		} else {
			for _, stream := range []*execsession.StreamImage{task.Stdin, task.Stdout, task.Stderr} {
				if stream != nil {
					e.Values[2] += uint64(stream.Replay.Capacity)
					e.Values[3] += stream.Replay.End - stream.Replay.Base
				}
			}
		}
	}
	if b.Flow != nil {
		for _, r := range b.Flow.Flows.Records {
			if r.RawRef != "" {
				counts["flow"].Values[1]++
			}
			if r.ReplayRef != "" {
				counts["flow"].Values[1]++
			}
			counts["flow"].Values[3] += r.TxEnd - r.TxBase
		}
		counts["flow"].Values[2] = uint64(b.Flow.Flows.ReservedBytes)
	}

	for _, name := range []string{"exec", "pty", "flow", "ss"} {
		events = append(events, *counts[name])
	}
	return events
}
