package broker

// runtime_introspect.go (R13) — the broker's process self-introspection for the OpRuntime admin
// verb. It answers the one question the fleet's leak/crash incidents could not: "is this LIVE
// broker leaking goroutines / fds, or has a reconciler stalled?" — without a debugger, a restart,
// or a pprof HTTP surface.
//
// WHY NO pprof (the judgment the roadmap §2 asked for)
// ----------------------------------------------------
// net/http/pprof is deliberately NOT introduced. The operational need is "spot a leak on a live
// broker", which the COUNTS below serve completely: a leak shows as a monotonically climbing
// goroutine or fd count across successive `admin runtime` polls, already gated behind the root-only
// 0600 admin socket. A standing pprof endpoint would instead add (a) attack surface — full heap /
// goroutine-stack dumps and a CPU-profile DoS vector, exposing far more than counts; and (b) binary
// size + a listener to secure. Deep stack-level forensics is a rare, deliberate, offline action, not
// a permanent HTTP surface. So: counts over the existing socket, no pprof.
//
// WHY NumGoroutine AND NOT /proc Threads
// --------------------------------------
// Goroutines is runtime.NumGoroutine() — the Go scheduler's live count. Threads is a genuinely
// different quantity and is NOT a goroutine proxy: 10k parked leaked goroutines add ZERO OS threads,
// so using it as one would report a flat count while the process bleeds goroutines. Note Threads here
// is the runtime's threadcreate profile — a CUMULATIVE count of OS threads (M) EVER created, MONOTONE:
// it never falls as threads exit, so it is a high-water mark, not a live thread count. A climbing value
// signals thread churn. Both are reported, from their own sources, so they can be seen to diverge.

import (
	"database/sql"
	"errors"
	"runtime"
	"runtime/pprof"
	"sort"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/brokerupgrade/brokerruntime"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infraattach"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// runtimeSnapshot builds the OpRuntime report from THIS process's live runtime plus the R7
// reconcile registry. It allocates only the returned slices and closes every fd it opens — the leak
// detector must never itself leak. Safe to call from the admin goroutine concurrently with the Run
// loop: NumGoroutine/pprof are process-global, the /proc reads are stateless, bootAt is written once
// before the admin socket starts (happens-before), and reconcilers.status() is internally locked.
func (b *Broker) runtimeSnapshot() *adminsock.RuntimeReport {
	rep := &adminsock.RuntimeReport{
		Schema: "admin_runtime",
		// v2 (B7): a BREAKING change to reconcilers[] — the `leader_only` bool was REMOVED and
		// `authority` ("any"/"leader") took its semantic place. docs/usage.md's bump policy is explicit
		// that deleting or changing a key bumps, and only additive omitempty fields do not. The fields
		// added alongside it are last_dur_ms / max_dur_ms / overruns on reconcilers[] and cadence_ms /
		// last_iter / iterations on cluster_loops[]. The first four are omitempty and would NOT have
		// justified a bump on their own; last_iter and iterations are NOT omitempty, because zero is the
		// value that means DEAD there and a dropped key would delete the signal exactly when it is bad
		// news. Either way the REMOVAL above is what forces the bump. A monitoring rule keyed on leader_only now sees a
		// schema_version it does not recognise instead of silently matching nothing.
		SchemaVersion: 2,
		Goroutines:    runtime.NumGoroutine(),               // in-process TRUTH, never the Threads proxy
		Threads:       pprof.Lookup("threadcreate").Count(), // OS threads (M) EVER created — monotone, never falls
		OpenFDs:       openFDCount(),                        // -1 if not measurable (non-Linux)
		RSSBytes:      brokerruntime.ResidentSetBytes(),     // -1 if not measurable (non-Linux)
	}
	if !b.bootAt.IsZero() {
		rep.UptimeSeconds = b.cfg.Now().Sub(b.bootAt).Seconds()
	}
	// Per-reconciler last-tick from the R7a registry (R13 only CONSUMES lastTick; it does not touch
	// the scheduler). Nil registry (an admin call landing in the sliver of Run before the registry is
	// wired) yields an empty list rather than a panic.
	if b.reconcilers != nil {
		for _, st := range b.reconcilers.status() {
			rep.Reconcilers = append(rep.Reconcilers, adminsock.ReconcilerTick{
				Name:       st.Name,
				IntervalMS: st.Interval.Milliseconds(),
				Authority:  st.Authority,
				LastTick:   st.LastTick,
				Runs:       st.Runs,
				Skips:      st.Skips,
				LastErr:    st.LastErr,
				LastDurMS:  st.LastDur.Milliseconds(),
				MaxDurMS:   st.MaxDur.Milliseconds(),
				Overruns:   st.Overruns,
			})
		}
	}
	// Batch-A A5: the four cluster loops (audit-publisher / reconciler / observe /
	// topology-reconcile, plus the optional alert-webhook) were entirely
	// unobservable — nothing reported whether the topology reconciler had ticked
	// in the last minute or had died quietly on its first iteration. They ride
	// the same ReconcilerTick shape as the registry entries, tagged so the two
	// sources stay distinguishable.
	// RB2-4: ONE snapshot, taken BEFORE the loop rows are built, feeding both the alert_webhook block and
	// the alert-webhook row's iteration count. Two independent reads is what let a caller observe
	// accepted+rejected=1 next to iterations=0 — a state the wire contract says cannot exist.
	var webhookIters uint64
	var webhookLastIter time.Time
	var haveWebhook bool
	if b.cl != nil {
		if p := b.cl.webhook.Load(); p != nil {
			st, iters, lastIter := p.Stats()
			rep.AlertWebhook, webhookIters, webhookLastIter, haveWebhook = &st, iters, lastIter, true
		}
	}
	if b.cl != nil && b.cl.loops != nil {
		// Sorted: a map range gives a different order every call, so two
		// export-incident bundles taken seconds apart would diff for no reason
		// (external review, gate quality #4).
		snap := b.cl.loops.Snapshot()
		names := make([]string, 0, len(snap))
		for name := range snap {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			st := snap[name]
			// Batch-A review M8/F-03: these rows are NOT ReconcilerTick rows in
			// the sense adminsock/protocol.go documents — there LastTick failing
			// to advance means "stalled", and a loop row's LastTick never
			// advances by construction. Emitting them into the same array
			// produced 4-5 permanent false "stalled" entries in
			// `admin runtime --json` and every export-incident bundle.
			//
			// They are reported under their own key instead, carrying what is actually observable:
			// the start time, plus (B7) the declared cadence and the loop's own per-iteration beat.
			// LastIter is a pointer so "never beat" is nil rather than the zero time, which reads as a
			// timestamp in JSON. Iterations is always emitted: 0 is the value that means DEAD.
			var lastIter *time.Time
			if !st.LastIter.IsZero() {
				li := st.LastIter
				lastIter = &li
			}
			iters := st.Iters
			if name == webhookLoopName && haveWebhook {
				// From the SAME snapshot as rep.AlertWebhook above, so the documented equality
				// `accepted + rejected == iterations` holds at every observable boundary rather than
				// eventually (RB2-4). LastIter comes from that same poster snapshot too: mixing the
				// poster's count with loopSet's separately-beaten timestamp allowed
				// iterations=1,last_iter=null at the boundary between those writes.
				iters = webhookIters
				if webhookLastIter.IsZero() {
					lastIter = nil
				} else {
					li := webhookLastIter
					lastIter = &li
				}
			}
			rep.ClusterLoops = append(rep.ClusterLoops, adminsock.ClusterLoopInfo{
				Name:       name,
				StartedAt:  st.StartedAt,
				CadenceMS:  st.Cadence.Milliseconds(),
				LastIter:   lastIter,
				Iterations: iters,
			})
		}
	}
	rep.BrokerImage = brokerImageSnapshot(b)
	infraattach.SetRuntimeReport(b.infra, rep)
	return rep
}

func openFDCount() int { return infraattach.ProcessFDCount() }

// The resident-set reader (residentSetBytes/splitFields) moved verbatim to
// brokerruntime.ResidentSetBytes: the S8 broker exporter's thin Broker hooks
// are funded by moving pure helpers out, never by growing the Broker.
// origin: S8 BROKER-EXPORTER R-11 (P15 per-package net-zero)

// The status has one producer for local runtime, reload replies and authenticated
// remote health. Running SHA is measured from the executable inode, never the
// staged pathname. Snapshot returns a value copy so consumers cannot mutate it.
// origin: hot-update S6 plan §5 / §9.1
func brokerImageSnapshot(b *Broker) *proto.BrokerImageStatus { return b.imageState.Snapshot() }

func setBrokerImagePhase(b *Broker, phase string) {
	b.imageState.SetPhase(phase, brokerruntime.ImageStatusMode(b.cl != nil && b.cl.node != nil, func() (int, error) { return b.cl.node.NumVoters() }))
}

// InitializeBrokerImageStatus installs validated boot lineage before responders.
// origin: hot-update S6 plan §4.1 / §5 / §9.1
func InitializeBrokerImageStatus(b *Broker, generation uint64, cutID string) error {
	return b.imageState.Initialize(generation, cutID)
}

// sealBrokerCut runs at the S6 cut's pre-shutdown point (business fenced, store
// still open) after the role check prior: an armed S8 cut seals its adjunct.
func sealBrokerCut(b *Broker, prior error) error {
	return brokerupgrade.SealCut(b.cfg.Upgrade, b.read().SQL(), prior)
}

// brokerTransactionPort is the S8 broker cut exporter's view of this Broker:
// the live owners of the current control generation (counted by identity in
// brokerruntime.TransactionFacts), the current store, the S6 cut's arming
// signal and the real control request. It reads the generation installed at
// call time: an image activation replaces imageControl, never this port.
// Every planned cut first captures the role/maintenance admission the S6 cut
// checks (captureBrokerReloadAdmission); without it the S6 cut itself refuses
// ("image role admission missing"). Single mode retains its local owner lease.
// origin: hot-update S8 plan §4.2 / §6.3 / §11.2 P15 P17; S8 BROKER-EXPORTER R-11
func brokerTransactionPort(b *Broker) brokerupgrade.BrokerPort {
	return brokerupgrade.BrokerPort{DB: func() *sql.DB { return b.read().SQL() },
		Arm: func(d controlbudget.Deadline) error {
			return brokerupgrade.ArmPlannedCut(d, func(admission controlbudget.Deadline) error {
				req := adminsock.Request{}
				if grant := brokerupgrade.ArmedTransaction(b.cfg.Upgrade); grant.ParentTxID != "" {
					req.Holder, req.CutID = grant.ParentTxID, grant.Handle.CutID
				}
				// origin: S8 internal review roll-recovery-stall F5; S6 lead D-C
				return captureBrokerReloadAdmission(b, req, admission)
			}, b.reloadSignal.Arm)
		},
		Control: func(nonce string) error { return brokerruntime.ControlProbe(b.nc.Load(), nonce, b.selfID) },
		Released: func(domain, id, origin string) bool {
			return brokerruntime.ReleasedAllocation(b.read().SQL(), domain, id, origin, func(port int, sid, nid, hash string) bool {
				return tunnel.ServerAllocationReleased(b.tunnelSrv.Load(), tunnel.SessionInfo{PublicPort: port, SID: sid, NID: nid, TokenHash: hash})
			})
		},
		Facts: func() (brokerupgrade.BrokerFacts, error) {
			c, srv := b.imageControl, b.tunnelSrv.Load()
			if c == nil {
				return brokerupgrade.BrokerFacts{}, brokerimage.ErrImage
			}
			var sessions func() []brokerupgrade.TunnelSession
			if srv != nil {
				sessions = func() (out []brokerupgrade.TunnelSession) {
					for _, s := range srv.SessionInfos() {
						out = append(out, brokerupgrade.TunnelSession{Port: s.PublicPort, SID: s.SID, NID: s.NID, Epoch: s.Epoch})
					}
					return out
				}
			}
			return brokerruntime.TransactionFacts(c.store, c.keepers, sessions, c.transfers, func() (int, error) {
				ids := map[string]struct{}{}
				err := errors.Join(censusLegacyTransfers(b.transfers, ids), censusBrokerTransferLedgers(b, ids))
				return len(ids), err
			}, brokerImageSnapshot(b))
		}}
}
