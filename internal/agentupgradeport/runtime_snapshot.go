// Runtime image inspection uses the existing owners and their original cut fence.
// origin: hot-update S8 plan §4.2 / S10 internal review MAKETEST-GATES F4
package agentupgradeport

import (
	"fmt"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/binaryupgrade"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// origin: hot-update S5 plan §5.2 / T35. Both readers are bound to the
// actual selected files, including a sibling's candidate already at dst.
// A changed prev after staging must not inherit a cached declaration.
func CheckCutReaders(exePath string, b agentimage.Bundle, readers []agentimage.Capabilities) error {
	if len(readers) == 0 {
		return agentimage.ErrABIUnsupported
	}
	if err := agentimage.VerifyCapabilitySHA(exePath, readers[0]); err != nil {
		return err
	}
	if err := VerifyPrevReader(exePath, readers); err != nil {
		return err
	}
	return agentimage.CheckBundleCapabilities(b, readers...)
}

func VerifyPrevReader(exePath string, readers []agentimage.Capabilities) error {
	if len(readers) > 1 {
		return agentimage.VerifyCapabilitySHA(binaryupgrade.PrevPath(exePath), readers[1])
	}
	return nil
}

// origin: hot-update S5 plan §5.2 / T35. Every retained responsibility is
// required before staging; the second and final gates read the exact bundle.
func RequiredRuntimeDomains(runtime *execsession.Runtime, flow, ss, transfer bool) []string {
	required := []string{"exec-v1"}
	if runtime != nil {
		required = runtime.RequiredDomains()
	}
	if flow {
		required = append(required, "flow-v1")
	}
	if ss {
		required = append(required, "ss-v1")
	}
	// A prepared entry or courier-only owner is retained responsibility. The
	// fixed Manager port has no live-byte census; presence conservatively
	// requires its reader rather than guessing from transfer admission counts.
	// origin: hot-update S7 plan §8.4 / P21
	if transfer {
		required = append(required, xfercontract.Version)
	}
	return required
}

// The production source reader is this binary's own emitter bound to the
// running SHA; a configured running image (tests, readonly installs) is probed
// like any candidate so its declarations belong to those exact bytes.
func SourceReader(path, sha string, probe func(string) (string, agentimage.Capabilities, error)) (string, agentimage.Capabilities, error) {
	if path != "" {
		release, caps, err := probe(path)
		if err == nil && caps.SHA256 != sha {
			err = agentimage.ErrRunningImageDiffers
		}
		return release, caps, err
	}
	return proto.ReleaseVersion, agentimage.CurrentCapabilities(sha), nil
}

// A cut owned by another path, or a table frozen by a foreign owner, is a
// precise deferral before the frozen busy reasons; neither is waited out.
func RuntimeDeferrals(cut *sync.Mutex, workloads *workload.Manager, preflight func() []agentimage.DeferredError) []agentimage.DeferredError {
	if !cut.TryLock() {
		return []agentimage.DeferredError{{Reason: ReasonUpgradeInProgress, Detail: "another cut owns the runtime"}}
	}
	cut.Unlock()
	if workloads.Frozen() {
		return []agentimage.DeferredError{{Reason: ReasonFrozenTable, Detail: "process table frozen by another owner"}}
	}
	return preflight()
}

// transactionSnapshot reads the runtime under the same bounded fence as
// imageIsEmpty. Unknown flow and SS owners still refuse instead of reading as
// an empty domain; typed owners are named by Host.Domains. Capable transfers
// are named by their worker's census (transactionTransfers).
// origin: hot-update S8 plan §4.2 / §9.0 / M06 / M14; S8 FIX-DESCRIBE (transfer census)
func SnapshotRuntime(runtime *execsession.Runtime, cut *sync.Mutex, workloads *workload.Manager, blocked bool, exportCourier func() agentimage.CourierState) (execsession.Image, agentimage.CourierState, error) {
	var courier agentimage.CourierState
	if blocked {
		return execsession.Image{}, courier, &agentimage.DeferredError{Reason: ReasonDomainCensus, Detail: "flow or SS census unavailable"}
	}
	courier = exportCourier()
	if runtime == nil {
		return execsession.Image{}, courier, nil
	}
	if !cut.TryLock() {
		return execsession.Image{}, courier, &agentimage.DeferredError{Reason: ReasonUpgradeInProgress, Detail: "another cut owns the runtime"}
	}
	defer cut.Unlock()
	if workloads.Frozen() {
		return execsession.Image{}, courier, &agentimage.DeferredError{Reason: ReasonFrozenTable, Detail: "process table frozen by another owner"}
	}
	if err := runtime.Freeze(time.Now().Add(upgradebudget.SafePointWait)); err != nil {
		return execsession.Image{}, courier, err
	}
	img, _, err := runtime.Export()
	if thawErr := runtime.Thaw(); err == nil {
		err = thawErr
	}
	return img, courier, err
}

// Flow and SS census rows name original owners, never volatile cut references.
// origin: hot-update S8 plan §4.2 / §6.2 / S8 internal review TAGGED-SUITE-SWEEP-A F2
// FlowSSState contains only public opaque identities. Proxy retains the agent's
// ordering/cleanup obligation even after the SS listener has been removed.
type FlowSSState struct {
	Flow  *tunnel.ClientImage
	SS    []ssproxy.Responsibility
	Proxy bool
}

// CensusOwners preserves the legacy/unknown refusal while allowing the real
// image owners to supply identities. Metadata without a server remains proxy
// responsibility, which the agent's sealed proxy adjunct carries independently.
func CensusOwners(owners agentimage.Owners, flowState tunnel.Census, proxy bool) (FlowSSState, error) {
	if flowState.Unsupported || flowState.Legacy+flowState.Admission+flowState.Rehome != 0 {
		return FlowSSState{}, deferral(ReasonDomainCensus)
	}
	if owners.Flow == nil && flowState.Owned+flowState.Retained != 0 {
		return FlowSSState{}, deferral(ReasonDomainCensus)
	}
	return CensusFlowSS(owners.Flow, owners.SS, proxy)
}

// CensusFlowSS reads the typed owners without freezing or renewing authority.
// The caller must reject unknown adapters before calling this producer.
func CensusFlowSS(client *tunnel.Client, server *ssproxy.Server, proxy bool) (FlowSSState, error) {
	out := FlowSSState{Proxy: proxy}
	deadline := time.Now().Add(upgradebudget.SafePointWait)
	if client != nil {
		im, err := tunnel.CensusClient(client, deadline)
		if err != nil {
			return FlowSSState{}, deferral(ReasonDomainCensus)
		}
		out.Flow = &im
	}
	if server != nil {
		var err error
		out.SS, err = ssproxy.Census(server, deadline)
		if err != nil {
			return FlowSSState{}, deferral(ReasonDomainCensus)
		}
	}
	return out, nil
}

func (p *runtimePort) domainRows(bundle *agentimage.Bundle, owner upgradetx.Owner, generation uint64, custody string) ([]upgradetx.Resource, error) {
	var state FlowSSState
	// A public image cannot prove private SS membership. Keep the original
	// unknown-owner guard until this host supplies the real domain producer.
	if p.host.Domains == nil && bundle != nil && !idleDomainImages(bundle) {
		return nil, deferral(ReasonDomainCensus)
	}
	if p.host.Domains != nil {
		var err error
		state, err = p.host.Domains()
		if err != nil {
			return nil, deferredError(err)
		}
	}
	if bundle != nil {
		// The final flow export owns its exact stream and retirement set. SS's
		// opaque public image must agree with the private owner's enumeration;
		// a count alone can never manufacture unnamed connection rows.
		state.Flow = bundle.Flow
		connections, listener := 0, ""
		for _, e := range state.SS {
			if e.Kind == "connection" {
				connections++
			}
			if e.Kind == "listener" {
				listener = fmt.Sprintf("ss-listener.%x", e.ID)
			}
		}
		if s := bundle.SS; s != nil {
			if connections != s.ConnectionCount || listener != s.ListenerRef || s.SecretBytes > 0 && len(state.SS) == 0 {
				return nil, deferral(ReasonDomainCensus)
			}
		} else if len(state.SS) != 0 {
			return nil, deferral(ReasonDomainCensus)
		}
	}
	var rows []upgradetx.Resource
	add := func(domain upgradetx.Domain, id, origin, disposition string) {
		row := upgradetx.Resource{Domain: domain, ID: id, Origin: origin, Owner: owner, Generation: generation, State: disposition, Custody: custody}
		if disposition == "terminal" {
			row.Custody = "retained" // receipt/history still belongs to this generation
		}
		rows = append(rows, row)
	}
	if im := state.Flow; im != nil {
		if im.Version != flow.Version || im.Flows.Version != flow.Version {
			return nil, deferral(ReasonDomainCensus)
		}
		streams := make(map[flow.ID]flow.EndpointID)
		for _, e := range im.Endpoints {
			if e.ID == (flow.EndpointID{}) || e.OwnerGen == 0 {
				return nil, deferral(ReasonDomainCensus)
			}
			id := fmt.Sprintf("endpoint:%x", e.ID)
			add(upgradetx.DomainFlow, id, id, "running")
			for _, id := range e.FlowIDs {
				if _, duplicate := streams[id]; duplicate {
					return nil, deferral(ReasonDomainCensus)
				}
				streams[id] = e.ID
			}
		}
		for _, r := range im.Flows.Records {
			ep, ok := streams[r.ID]
			if !ok || r.ID == (flow.ID{}) || r.OpenSeq == 0 || r.Intent == ([32]byte{}) {
				return nil, deferral(ReasonDomainCensus)
			}
			delete(streams, r.ID)
			disposition := "running"
			if r.Phase == flow.FlowPhaseTerminal {
				disposition = "terminal"
			}
			// Offsets, FIN bits and carrier counters are codec obligations of
			// this stream, not new identities every time normal IO progresses.
			add(upgradetx.DomainFlow, fmt.Sprintf("flow:%x", r.ID), fmt.Sprintf("endpoint:%x/open:%d/intent:%x", ep, r.OpenSeq, r.Intent), disposition)
		}
		if len(streams) != 0 || len(im.Flows.Records) == 0 && im.Flows.ReservedBytes != 0 {
			return nil, deferral(ReasonDomainCensus)
		}
		for _, r := range im.Flows.Retirement {
			if r.EndpointID == (flow.EndpointID{}) || r.NextOpenSeq == 0 {
				return nil, deferral(ReasonDomainCensus)
			}
			id := fmt.Sprintf("retirement:%x", r.EndpointID)
			add(upgradetx.DomainFlow, id, fmt.Sprintf("endpoint:%x", r.EndpointID), "retained")
		}
	}
	for _, e := range state.SS {
		switch e.Kind {
		case "server", "authority", "timer", "listener", "connection", "material", "binding":
		default:
			return nil, deferral(ReasonDomainCensus)
		}
		if e.ID == ([16]byte{}) {
			return nil, deferral(ReasonDomainCensus)
		}
		id := fmt.Sprintf("ss-%s:%x", e.Kind, e.ID)
		add(upgradetx.DomainSS, id, id, "running")
	}
	if state.Proxy {
		id := "proxy:" + p.host.Key.SID + "/" + p.host.Key.ConfiguredNID
		add(upgradetx.DomainSS, id, id, "retained")
	}
	return rows, nil
}
