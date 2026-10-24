package determinism

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// enum_switch_default_test.go — a switch over one of THIS repo's enums may not carry a `default:`.
//
// origin: line-2 §7 / plan §1 订正 2. `.golangci.yml` sets exhaustive's
// `default-signifies-exhaustive: true`, which is necessary — without it every default-carrying switch
// in the tree reports and the linter becomes noise. It also opens a one-line escape hatch, and the
// escape hatch was measured rather than imagined:
//
//	1. baseline: exhaustive reports cmd/tether/cluster_doctor_online.go missing TopoUnreported/TopoConverged
//	2. delete `case TopoBehind:` and `case TopoUnknownAction:` — i.e. re-inject the batch-C external
//	   review MAJOR, where the doctor reported PASS on a cluster that was not converging — and
//	   exhaustive catches it immediately
//	3. now add a bare `default:` to that switch. exhaustive goes quiet. Re-inject the SAME defect from
//	   step 2 and exhaustive stays quiet: 0 issues, gate green, defect shipped.
//
// So the cheapest way to clear an exhaustive violation is also the way to permanently blind the check
// on that switch. The whole value argument for enabling exhaustive rests on it catching that class of
// defect, which makes this guard the other half of the linter rather than an extra.
//
// WHY IT MATCHES ON CONSTANT NAMES
// --------------------------------
// Resolving a switch tag to its declared type needs full type information (go/packages over every
// build tag), which is slow and, worse, gives a different answer depending on which tags are on. This
// scans for switches whose CASES name a constant from one of the enum families below — the same thing
// a reader does — and it is deliberately conservative: a family has to be registered here to be
// guarded, so the failure mode is a missed switch rather than a false accusation. enumFamilies is the
// list to grow when a new enum appears.
//
// A `default:` that is genuinely required (a wire-facing switch that must tolerate an unknown value
// from a newer peer, say) goes in allowedEnumDefaults with a reason, and that ledger has the same
// reverse assertion as every other one here: an entry that no longer names a real site fails.

// enumFamilies maps a family label to the constant-name prefixes that belong to it. A switch is
// considered "over this enum" when at least one of its case expressions is a selector or identifier
// whose name starts with one of these prefixes.
// Every closed enum in the tree, enumerated from `= iota` declarations. Prefixes only — a family is
// recognised when a case names a constant starting with one of them.
//
// origin: line-2 external review IDG-3 / PI-13. The first version listed THREE families and the tree
// has thirteen, so the `default:` escape hatch stayed open on ten enums while the gate reported green —
// and a review lane, after completing the list, found five production switches still carrying one.
// A registry that covers 3/13 is worse than none, because its green is read as coverage.
//
// TestEnumFamiliesCoverEveryIotaEnum below re-derives this list from source and fails when an enum
// appears without an entry, so the 3/13 state cannot recur silently.
var enumFamilies = map[string][]string{
	// Explicit upstream string and wire values are as closed as iota enums.
	// origin: hot-update S10 plan §12.2 / P29
	"infracontract.Action":          {"Recover", "Reload", "Rollback", "Upgrade"},
	"infracontract.ArtifactKind":    {"CaddyBinary", "HelperBinary", "LauncherBinary", "NATSBinary"},
	"infracontract.Component":       {"Caddy", "HostSupport", "NATS"},
	"infracontract.RouteDirection":  {"RouteOutbound", "RouteReturn"},
	"infracontract.RoutePurpose":    {"RouteBind", "RoutePrepare"},
	"infrahost.BootstrapStage":      {"EffectiveUnitVerified", "FilesInstalled", "HelperVersionVerified", "RuntimeReady", "SocketActive"},
	"infrahost.EffectPhase":         {"EffectApplied", "EffectIntent", "EffectNotApplied", "EffectUnknown"},
	"infrapriv.CredentialKind":      {"LocalInfra", "LocalLaunch", "SYSRead", "SYSStep"},
	"infrapriv.Scope":               {"AccountAuthority", "LaunchAuthority", "LocalAuthority", "RootAuthority"},
	"infrareadiness.Identity":       {"ActivatedMember", "ProvisionedAgent"},
	"infraunit.UnitTarget":          {"UnitCaddy", "UnitCaddyWarm", "UnitHelperService", "UnitHelperSocket", "UnitNATS", "UnitNATSWarm"},
	"upgradetx.ActionKind":          {"ActionActivate", "ActionControl", "ActionDispatch", "ActionFreeze", "ActionMigrate", "ActionPublish", "ActionRelease", "ActionRetire", "ActionReverse", "ActionSelect"},
	"upgradetx.ActionState":         {"ActionDurable", "ActionIntent", "ActionObserved"},
	"upgradetx.Component":           {"ComponentAgent", "ComponentBroker", "ComponentCaddy", "ComponentNATS"},
	"upgradetx.CutStage":            {"CutActive", "CutNone", "CutPartialActive", "CutPristine", "CutReleased", "CutRetiring"},
	"upgradetx.Direction":           {"DirectionForward", "DirectionReverse"},
	"upgradetx.Domain":              {"DomainAuthority", "DomainBrokerRuntime", "DomainBrokerStore", "DomainControl", "DomainExec", "DomainFlow", "DomainPTY", "DomainSS", "DomainTransferTracker", "DomainTransferWorker", "DomainUpgrade"},
	"upgradetx.Mode":                {"ModeHot", "ModeLegacyInterrupting"},
	"upgradetx.Outcome":             {"OutcomeCancelled", "OutcomeDeferred", "OutcomeLegacyInterrupting", "OutcomePending", "OutcomeRejected", "OutcomeRolledBack", "OutcomeSuccess", "OutcomeUnknown"},
	"upgradetx.Phase":               {"PhaseAbandoned", "PhaseActivating", "PhaseCancelled", "PhaseCompleted", "PhaseDeferred", "PhaseFreezing", "PhaseMigrating", "PhasePrepared", "PhaseReady", "PhaseRecoveryRequired", "PhaseRejected", "PhaseRollbackBlocked", "PhaseRollbackPending", "PhaseRolledBack", "PhaseServingNew", "PhaseStaged"},
	"upgradetx.ReceiptKind":         {"ReceiptControlReady", "ReceiptOldGenerationReleased", "ReceiptResourcesMigrated"},
	"upgradetx.StepKind":            {"StepAgentCut", "StepBrokerCut", "StepHomeConverge", "StepRelease"},
	"upgradetx.WindowState":         {"WindowClosed", "WindowClosing", "WindowOpen"},
	"xferclient.jetStreamStallKind": {"stallNoAnswer", "stallNoProgress"},
	"xfertrack.abandonRefusal":      {"abandonClaimed", "abandonCommitted", "abandonFinalized", "abandonNoEntry", "abandonNotPush", "abandonTierA"},
	// origin: hot-update S9 plan §2 / P35; WIRE-2 R11 freezes VerifyIngress=18.
	// Phase.Staged needs its own longest prefix; it must not be attributed to HostOp.Stage.
	"infracontract.Phase":  {"Prepared", "Staged", "Ready", "Moving", "Cutting", "Verifying", "Returning", "Retiring", "Completed", "Deferred", "Halted"},
	"infracontract.HostOp": {"Inspect", "Stage", "FenceStart", "Promote", "SignalLDM", "SignalStop", "SignalGraceful", "StartWarm", "SwitchIngress", "FinalizeStorage", "ReleaseStart", "RetireWarm", "RecoverHost", "RotatePolicy", "UpdateSupport", "LaunchClaim", "LaunchAck", "VerifyIngress"},
	// Full member prefixes keep shared Phase/Role names attributed to their owner.
	// origin: hot-update S7 plan §11.2 / T52 / P00 enum manifest
	"xfercontract.StatusState":         {"StatusUnknown", "StatusLive", "StatusTerminal"},
	"xfercontract.Phase":               {"PhaseOpen", "PhasePrepared", "PhaseSending", "PhaseReceiving", "PhaseCommitReserved", "PhaseDecided", "PhaseTerminalPending", "PhaseHistoryConfirmed", "PhaseCleanupPending", "PhaseSettled"},
	"xfercontract.CommitPrimitive":     {"PrimitiveRename", "PrimitiveLink"},
	"xfercontract.PrimitiveOutcome":    {"OutcomeNotEntered", "OutcomeInflight", "OutcomeLanded"},
	"xfercontract.CancelReason":        {"CancelUser", "CancelDeadline", "CancelRevoked", "CancelStopped"},
	"xfercontract.AuditDelivery":       {"DeliveryPending", "DeliveryRaftCommitted", "DeliveryHistoryConfirmed"},
	"xfercontract.TerminalDisposition": {"DispositionPending", "DispositionComplete", "DispositionFailed", "DispositionConflict"},
	"xfercontract.ObjectWritePhase":    {"WriteOpen", "WritePending", "WriteMetadataPending", "WriteFinalized", "WriteRetiring", "WriteRetired"},
	"xfercontract.ReaderPhase":         {"ReaderOpen", "ReaderFetching", "ReaderDirty", "ReaderDeletePending", "ReaderStopped"},
	"xfercontract.LedgerMutationKind":  {"MutationAdmit", "MutationSourceSeal", "MutationReserve", "MutationDecide", "MutationHistory", "MutationCleanup", "MutationAbandon"},
	"xfercontract.AccessKind":          {"AccessCreate", "AccessContinue", "AccessStatus", "AccessSettle"},
	"xfercontract.Operation":           {"OpProbe", "OpPermit", "OpPrepare", "OpStatus", "OpSource", "OpReserve", "OpCommit", "OpTerminal", "OpCancel", "OpFinalize", "OpCensus", "OpBucketConfig"},
	"xfercontract.ControlKind":         {"ControlStart", "ControlGrant", "ControlCancel", "ControlRevoke", "ControlReceipt"},
	"xfercontract.PrincipalRole":       {"RoleCTL", "RoleAgent", "RoleBroker", "RoleSettlement"},
	"xfercontract.ObservationKind":     {"ObserveAppend", "ObserveResolve", "ObserveFinalize", "ObserveFetched", "ObserveSynced", "ObserveReaderStop", "ObservePrimitiveEnter", "ObservePrimitiveExit", "ObserveDirectorySync", "ObserveFreeze", "ObserveImport", "ObserveControl", "ObserveAcknowledge", "ObserveReserve", "ObserveFinish", "ObserveCallbackEnter", "ObserveCallbackJoin", "ObserveLedgerSync", "ObserveHistory", "ObserveRetirement", "ObserveInstall", "ObserveRollback", "ObserveAttempt", "ObserveDrain"},

	// Logical labels for the frozen untyped string vocabularies. Wire fields
	// keep their additive string types; the scanner still closes their cases.
	// origin: hot-update S6 plan §9.1 / §13
	"proto.BrokerImagePhase": {"BrokerImagePhase"},
	"proto.BrokerImageMode":  {"BrokerImageMode"},
	"brokerimage.Detail":     {"DetailMaintenance", "DetailStore", "DetailControl"},
	// origin: hot-update S6 plan §2 / §13 / P00 enum manifest.
	// Full member prefixes avoid stealing S5 CloseMode or spawnsafe.Mode cases.
	"storage.ClosePhase": {"CloseOpen", "CloseStopped", "CloseClosed", "CloseUnknown"},
	"brokerimage.Mode":   {"ModeSingle", "ModeSoleVoter", "ModeQuorum"},
	"brokermaint.Phase":  {"MaintPrepared", "MaintActive", "MaintEnded"},
	// RetryTransfer is S7 P26's member of the same family, not a new family.
	// origin: hot-update S7 plan §7 / T32 / P26
	"cli.RetryFamily":            {"RetryReadOnly", "RetryStableProcess", "RetryUnsafe", "RetryTransfer"},
	"controlbudget.AttemptPhase": {"PhaseDNS", "PhaseProxy", "PhaseTLS", "PhaseWS", "PhaseConnect", "PhaseInbox", "PhaseDiscovery", "PhaseBanner", "PhaseProbe", "PhaseCreate", "PhaseRequest", "PhaseBackoff", "PhaseCancel", "PhaseResult", "PhaseFreeze", "PhaseExec", "PhaseImport", "PhaseAuth", "PhaseReady", "PhaseMaintenance"},
	"clusterupgrade.PlanScope":   {"ScopeWholeHost", "ScopeBrokerDaemon"},
	// origin: hot-update S5 plan §2 / §10.2 — explicit wire values are closed too.
	"flow.Kind":          {"KindOpen", "KindResume", "KindReject", "KindData", "KindACK", "KindFIN", "KindClose"},
	"flow.Phase":         {"FlowPhaseOpening", "FlowPhaseConnecting", "FlowPhaseActive", "FlowPhaseDraining", "FlowPhaseTerminal"},
	"flow.Reason":        {"ReasonMalformed", "ReasonOffset", "ReasonIntent", "ReasonUnknownFlow", "ReasonRetired", "ReasonRevoked", "ReasonStaleEpoch", "ReasonOwnerConflict", "ReasonCapacity", "ReasonUnauthorized", "ReasonDeadline", "ReasonUnsupported", "ReasonIOFailure", "ReasonNonceExhausted", "ReasonStateInvalid"},
	"flow.ResumeScope":   {"ResumeScope"},
	"flow.ResumeMode":    {"ResumeMode"},
	"flow.ResumeVerdict": {"ResumeVerdict"},
	"flow.CloseMode":     {"CloseMode"},
	// origin: hot-update S6 plan §9.3 / T53; closed values follow the extracted codec owner.
	"flowstate.EndpointPhase": {"EndpointPhase"},
	"flowstate.TicketPurpose": {"TicketPurpose"},
	"ssproxy.SSPhase":         {"SSPhase"},
	"ssproxy.AuthorityPhase":  {"AuthorityPhase"},
	"rawsocket.ConnectState":  {"ConnectConnecting", "ConnectConnected", "ConnectFailed"},
	"sealedstate.Kind":        {"KindAuthority", "KindBinding", "KindEndpoint", "KindFlow", "KindMaterial", "KindPermit", "KindReceipt", "KindRetirement", "KindSaltRing", "KindSSActor", "KindSSServer", "KindTimer"},
	"sealedstate.FieldType":   {"FieldU64", "FieldNonnegativeI64", "FieldBool", "FieldBytes", "FieldUTF8", "FieldID16", "FieldNonce12", "FieldSalt32"},

	// origin: hot-update S4 plan §10 — terminal ownership, source, and client phases.
	"ptysession.actorPhase":  {"phaseActive", "phaseFreezing", "phaseFrozen", "phaseClosed"},
	"ptysession.sourcePhase": {"sourceReading", "sourceDiscarding", "sourceEnded"},
	"execclient.ptyPhase":    {"ptyProbing", "ptyCreating", "ptyAttached", "ptySettling", "ptyDone"},
	"agent.openOutcome":      {"openState", "openPort", "opened"},
	// origin: gotcha #72 — the bounded-teardown ladder's caller intent. Two members today
	// (rebuild / shutdown) with genuinely different terminal moves (self-exec vs exit); a third
	// would need its own, so a `default:` over this must never be allowed to silence exhaustive.
	"agent.teardownIntent": {"teardown"},
	// origin: hot-update S5 plan §10.2 / AGENT-SLIM-S5 — the extracted teardown
	// callback intent stays closed even before its first switch is introduced.
	"connrecovery.Intent": {"Rebuild", "Shutdown"},
	"agent.transferMode":  {"mode"},
	// origin: h1 D (docs/reviews/h1-plan.md workstream D). shouldReapRun's
	// three-way verdict: a future fourth state (say "reap without SIGHUP" for
	// a non-PTY stream) must be forced through every switch rather than
	// silently inheriting a `default:` — the whole point of the ladder is
	// that each state maps to a DIFFERENT irreversible action.
	"agent.reapVerdict": {"reap"},
	"authcallout.role":  {"role"},
	"broker.admitRole":  {"admit"},
	// origin: simcluster-speed external review F2. Why a creator's push{failed} was NOT claimed —
	// committed / tier-A / already finalized / gone / not a push — each maps to a different reply
	// (refuse with the reason, idempotent OK, verb_mismatch). A sixth reason must pick its reply in
	// handleFinalizeReq's switch, not inherit one from a `default:`.
	"broker.abandonRefusal":       {"abandon"},
	"brokerruntime.CutoverAction": {"Cutover"},
	// The ledger disposition/site enums and the caps verdict moved verbatim into
	// their S7 leaves (broker ledgerDisposition -> xferledger.LegacyDisposition,
	// ledgerSite -> xferledger.LegacySite, main capsStatus -> cli.TransferCapsStatus);
	// the families follow the declarations so their switches stay guarded.
	// origin: hot-update S7 plan §12 P13 / P27 / INTEG-A source moves
	"xferledger.LegacyDisposition": {"LegacyLeave", "LegacyReplay", "LegacySynthesize"},
	"xferledger.LegacySite":        {"LegacySiteOutbox", "LegacySitePrimary"},
	"broker.passAuthority":         {"authority"},
	"cli.Source":                   {"Source"},
	"clusterupgrade.AgentPresence": {"Agent"},
	"cli.TransferCapsStatus":       {"TransferCapsUndetermined", "TransferCapsRefused", "TransferCapsOK"},
	// S7/S9 closed iota enums. Full member names: ConnectionRole shares RoleAgent with
	// xfercontract.PrincipalRole, so its block is attributed through RoleUnknown and
	// RoleAgent stays PrincipalRole's prefix; the 27 Caddy cursor values are persisted.
	// origin: hot-update S7 plan §6.4 / T31; S9 plan §12 P05 / P13 (s9-p9b request)
	"xferauth.ConnectionRole":  {"RoleUnknown", "RoleCtlUnactivated", "RoleCtlActivated", "RoleTransferSettlement", "RoleSessionRemoval"},
	"infraauth.Phase":          {"Fenced", "AuthBinding", "AuthUsable", "BusinessBinding", "ControlReady"},
	"infraupgrade.caddyCursor": {"caddyValidate", "caddyInspect", "caddyStage", "caddyFence", "caddyStartWarm", "caddyWarmReady", "caddySealOutbound", "caddyPrepareOutbound", "caddyRedirectWarm", "caddyMoveOutbound", "caddyDrainOld", "caddyPromote", "caddyCut", "caddyOldExit", "caddyFinalizeStorage", "caddyReleaseStart", "caddyMainReady", "caddySealReturn", "caddyPrepareReturn", "caddyRedirectMain", "caddyMoveReturn", "caddyDrainWarm", "caddyRetireWarm", "caddyWarmExit", "caddyReleaseCohort", "caddyComplete", "caddyFinished"},
	// origin: gotcha #83 ⑤ (simcluster-speed §8.5). The two facts the start-joiner boundary reads
	// before deciding its grace: the joiner's local nats-server state and whether a `tether serve`
	// process exists. A fifth nats state or a third process state must be routed through
	// joinerStartGrace deliberately — the conservative "full grace" is what a `default:` would
	// hand it silently, and a WRONG conservative answer here is the 120 s that produced #83.
	"main.natsLocalState":   {"natsLocal"},
	"main.brokerProcState":  {"brokerProc"},
	"natsconf.RenderIntent": {"Intent"},
	"natsconf.TopoState":    {"Topo"},
	"spawnsafe.fsKind":      {"kind"},
	"spawnsafe.probeState":  {"st"},
	"spawnsafe.Mode":        {"Mode"},
	// hot-update S3: the admission sites an image cut closes (internal/agent/upgrade_image.go).
	"agent.gateKind": {"gate"},
}

// allowedEnumDefaults are the sites where a `default:` is correct, keyed by "file:ENCLOSING FUNCTION".
// Empty, and the failure text is written expecting it to stay that way: within one binary an enum is
// closed, so a default is a way of not deciding.
//
// KEYED BY FUNCTION, NOT BY LINE. origin: line-2 closure verification §6 B4. This shipped keyed by
// "file:line" — the identical scheme that review M15 had just finished REMOVING from the TLS ledger in this
// same increment, for the identical reason: inserting one comment line above the site invalidates the key,
// so the gate fires with a message claiming the ledgered site "no longer exists" when it is right there.
// The map being empty is why nobody noticed: a broken key scheme in a ledger with no entries has no
// symptoms until the first entry, at which point the person adding it inherits a trap.
//
// Fixing an empty ledger looks like wasted work and is the opposite: the cost of the wrong scheme is paid
// by whoever adds entry number one, and they will have no reason to suspect the mechanism.
var allowedEnumDefaults = map[string]string{}

// enumConstPrefixes flattens enumFamilies for lookup, remembering which family each prefix came from.
func enumConstPrefixes() map[string]string {
	out := map[string]string{}
	for family, prefixes := range enumFamilies {
		pkg, _, _ := strings.Cut(family, ".")
		for _, p := range prefixes {
			out[p] = family
			out[pkg+"."+p] = family
		}
	}
	return out
}

// Controls know the family being exercised. Give their cases the same package
// qualification real source supplies; shared Phase/Role names cannot be
// attributed by the iteration order of a whole-repository prefix map.
// origin: hot-update S10 plan §12.2 / P29
func enumMemberExpr(family, member string) ast.Expr {
	pkg, _, _ := strings.Cut(family, ".")
	return &ast.SelectorExpr{X: ast.NewIdent(pkg), Sel: ast.NewIdent(member)}
}

// caseEnumFamily reports which enum family a case clause's expressions belong to ("" = none).
func caseEnumFamily(cc *ast.CaseClause, prefixes map[string]string) string {
	for _, e := range cc.List {
		var name string
		switch v := e.(type) {
		case *ast.SelectorExpr: // natsconf.TopoStuck
			pkg, ok := v.X.(*ast.Ident)
			if !ok {
				continue
			}
			name = pkg.Name + "." + v.Sel.Name
		case *ast.Ident: // ledgerLeave
			name = v.Name
		default:
			continue
		}
		if family := enumFamilyForName(name, prefixes); family != "" {
			return family
		}
	}
	return ""
}

// enumMembers scans iota and explicitly typed constant blocks and returns each family's full set of member
// names. A family is matched to a block by prefix, the same way cases are.
//
// origin: line-2 external review IDG-3, second pass. The first rule here was "a switch over a repo enum
// may not carry a `default:` at all", and applying it exposed five production switches whose default was
// CORRECT — each carried a real semantic (the zero value IS the owner role; IntentPreserve IS the
// fallback; deny IS the safe answer for an unknown auth role). Enumerating their missing cases left them
// with both full coverage AND a defensive default, which is the best available shape — and the gate
// still reddened, because the rule was measuring the wrong thing.
//
// The risk was never `default:`. It is a `default:` that HIDES a missing case: with
// default-signifies-exhaustive:true, adding one silences exhaustive on that switch forever, so the next
// enum member joins by inheriting whatever the fallback happens to do. A switch that names every member
// and then keeps a default has nothing hidden — the next member added still reddens exhaustive, because
// exhaustive counts cases, not defaults.
//
// So the rule is now: a default is allowed IFF every member of the enum appears as a case.
func enumMembers(t *testing.T, root string, prefixes map[string]string) map[string]map[string]bool {
	t.Helper()
	out := map[string]map[string]bool{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata", "test":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, p, nil, 0)
		if perr != nil {
			return nil
		}
		// A contract may declare its typed constants in a dependency-free value
		// subpackage and re-export them by alias (xfercontract.Phase is declared in
		// internal/xfercontract/xferport). Every ancestor package directory under
		// internal/ may therefore own a registered family whose type the file declares.
		// origin: hot-update S7 plan §8.1 / §11.2 / T52
		var owners []string
		if rel, err := filepath.Rel(root, filepath.Dir(p)); err == nil {
			parts := strings.Split(filepath.ToSlash(rel), "/")
			for i := 1; i < len(parts)-1; i++ {
				owners = append(owners, parts[i])
			}
		}
		for family, names := range enumFileMembers(f, prefixes, owners...) {
			if out[family] == nil {
				out[family] = map[string]bool{}
			}
			for name := range names {
				out[family][name] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk for enum members: %v", err)
	}
	return out
}

// origin: hot-update S5 plan §10.2 — wire enums and the private codec's aliases
// deliberately use explicit values. Treating only iota as closed left every S5
// switch with an empty member set, so an unknown-state fallback passed vacuously.
// owners are ancestor package names that may own a family declared in this file.
func enumFileMembers(f *ast.File, prefixes map[string]string, owners ...string) map[string]map[string]bool {
	return enumFileMembersInRegistry(f, prefixes, enumFamilies, owners...)
}

// The disk control supplies the same registry the source reader just parsed;
// using the compiled enumFamilies here would conceal an on-disk missing family.
// origin: hot-update S10 plan §12.2 / P29
func enumFileMembersInRegistry(f *ast.File, prefixes map[string]string, registry map[string][]string, owners ...string) map[string]map[string]bool {
	out := map[string]map[string]bool{}
	add := func(family, name string) {
		if out[family] == nil {
			out[family] = map[string]bool{}
		}
		out[family][name] = true
	}
	ast.Inspect(f, func(n ast.Node) bool {
		gd, ok := n.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			return true
		}
		isIota := false
		var names []string
		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for _, v := range vs.Values {
				ast.Inspect(v, func(n ast.Node) bool {
					if id, ok := n.(*ast.Ident); ok && id.Name == "iota" {
						isIota = true
					}
					return true
				})
			}
			for _, nm := range vs.Names {
				if nm.Name != "_" {
					names = append(names, nm.Name)
				}
			}
			// Untyped public phase/mode/detail constants share a const block.
			// Attribute each name, never the entire mixed block to its first family.
			// origin: hot-update S6 plan §9.1 / §13
			for _, name := range vs.Names {
				for prefix, family := range prefixes {
					if vs.Type == nil && strings.HasPrefix(family, f.Name.Name+".") && strings.HasPrefix(name.Name, prefix) {
						add(family, name.Name)
					}
				}
			}
			if typ, ok := vs.Type.(*ast.Ident); ok {
				for _, owner := range append([]string{f.Name.Name}, owners...) {
					family := owner + "." + typ.Name
					if _, registered := registry[family]; registered {
						for _, name := range vs.Names {
							if name.Name != "_" {
								add(family, name.Name)
							}
						}
						break
					}
				}
			}
		}
		if !isIota || len(names) == 0 {
			return true
		}
		// Attribute the block to a family by matching its FIRST member against the prefix table.
		for _, nm := range names {
			if family := enumFamilyForName(nm, prefixes); family != "" {
				for _, member := range names {
					add(family, member)
				}
				return true
			}
		}
		return true
	})
	return out
}

// origin: hot-update S5 plan §10.2 — member discovery and the default guard
// share this predicate so an explicit wire member cannot vanish between them.
func enumSwitchGap(sw *ast.SwitchStmt, prefixes map[string]string, members map[string]map[string]bool) (family string, hasDefault bool, uncovered []string) {
	named := map[string]bool{}
	for _, stmt := range sw.Body.List {
		cc, ok := stmt.(*ast.CaseClause)
		if !ok {
			continue
		}
		if cc.List == nil {
			hasDefault = true
			continue
		}
		if family == "" {
			family = caseEnumFamily(cc, prefixes)
		}
		for _, e := range cc.List {
			switch v := e.(type) {
			case *ast.SelectorExpr:
				named[v.Sel.Name] = true
			case *ast.Ident:
				named[v.Name] = true
			}
		}
	}
	for member := range members[family] {
		if !named[member] {
			uncovered = append(uncovered, member)
		}
	}
	sort.Strings(uncovered)
	return family, hasDefault, uncovered
}

func TestNoDefaultOnRepoEnumSwitch(t *testing.T) {
	root := repoRoot(t)
	prefixes := enumConstPrefixes()
	members := enumMembers(t, root, prefixes)

	type site struct {
		key, family string
		uncovered   []string
	}
	var offenders []site
	seen := map[string]bool{}
	guarded := 0
	hitsByFamily := map[string]int{}

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata", "test":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			return rerr
		}
		rel = filepath.ToSlash(rel)

		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, p, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", rel, perr)
		}
		filePrefixes := enumPrefixesForFile(f)
		ast.Inspect(f, func(n ast.Node) bool {
			sw, ok := n.(*ast.SwitchStmt)
			if !ok || sw.Body == nil {
				return true
			}
			family, hasDefault, uncovered := enumSwitchGap(sw, filePrefixes, members)
			if family == "" {
				return true
			}
			guarded++
			hitsByFamily[family]++
			// Keyed by enclosing function, not by line — see allowedEnumDefaults. A line-keyed ledger is
			// invalidated by a comment inserted above the site, which is review M15's finding applied here
			// before this ledger has its first entry.
			key := rel + ":" + enclosingFuncName(f, sw.Pos())
			seen[key] = true
			// A default is fine when every member is also named: exhaustive still reddens on the NEXT
			// member, because it counts cases. It is only a problem when it is covering for a gap.
			if hasDefault && len(uncovered) > 0 && allowedEnumDefaults[key] == "" {
				offenders = append(offenders, site{key: key, family: family, uncovered: uncovered})
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	sort.Slice(offenders, func(i, j int) bool { return offenders[i].key < offenders[j].key })
	if len(offenders) > 0 {
		var lines []string
		for _, o := range offenders {
			lines = append(lines, o.key+"  (switch over "+o.family+"; unnamed: "+strings.Join(o.uncovered, ", ")+")")
		}
		t.Errorf("%d switch(es) over a repo enum rely on `default:` to cover a member they never name:\n  %s\n\n"+
			"With exhaustive's default-signifies-exhaustive:true, that default makes the linter stop "+
			"checking this switch FOREVER — measured: with a default in place, re-injecting the batch-C\n"+
			"doctor defect (missing TopoBehind/TopoUnknownAction, reporting PASS on a non-converging\n"+
			"cluster) produces zero reports. Enumerate the missing cases instead. If a default really is\n"+
			"required, add the site to allowedEnumDefaults WITH a reason.",
			len(offenders), strings.Join(lines, "\n  "))
	}

	// Reverse assertion: an allow-list entry that no longer names a guarded switch is dead weight, and
	// a dead entry is how an allow-list turns into an allow-everything.
	var stale []string
	for key := range allowedEnumDefaults {
		if !seen[key] {
			stale = append(stale, key)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d allowedEnumDefaults entr(y/ies) no longer name a switch over a repo enum "+
			"(moved, deleted, or the default was removed):\n  %s\n\nDelete them.",
			len(stale), strings.Join(stale, "\n  "))
	}

	// Non-vacuity PER FAMILY, not one global floor.
	//
	// origin: line-2 closure verification. The floor was `guarded == 0` plus `guarded < 5`, over a scan
	// that reaches ~18 production switches across 15 registered families. Both are satisfied by a prefix
	// list that has rotted down to one working family: five switches over natsconf.TopoState alone clear
	// them, and the other fourteen families could stop being recognised entirely with the gate still
	// green. A global count cannot distinguish "the walker works" from "one family's prefixes happen to
	// still match".
	//
	// Per-family is the assertion that means what the global one was pretending to: every family in
	// enumFamilies must be reached by at least one real switch, so a prefix that stops matching is named.
	if guarded == 0 {
		t.Fatal("the scan found NO switch over any registered enum — the prefix list or the walker is " +
			"broken, and every default would pass unnoticed")
	}
	// The families that HAVE switches today, pinned by name. A family with no switch is not a defect —
	// TestEnumFamiliesCoverEveryIotaEnum requires every iota enum to be REGISTERED, and most of this repo's
	// enums are consumed with `if`/`==` rather than `switch`. Registering one before its first switch is
	// correct: the coverage is then already in place when somebody writes it.
	//
	// So the floor cannot be "every family must be hit" — that assertion and the coverage assertion together
	// would demand a switch for every enum in the tree. (It was written that way first and immediately
	// reported 7 of 15 families "unreached", all of which were fine.)
	//
	// What IS assertable is the set that is hit TODAY. If one of these stops being hit, either its prefixes
	// rotted or its switches were deleted, and both need a person. The reverse half matters just as much: a
	// family NOT in this list that starts being hit means somebody wrote the first switch over that enum, and
	// the gate says so — which is the moment its coverage becomes load-bearing.
	familiesWithSwitches := map[string]bool{
		// origin: hot-update S10 plan §12.2 / P29 — actual upstream switch union.
		"infracontract.ArtifactKind": true,
		"infracontract.Component":    true,
		"infrapriv.Scope":            true,
		"infrareadiness.Identity":    true,
		"infraunit.UnitTarget":       true,
		"upgradetx.ActionKind":       true,
		"upgradetx.ActionState":      true,
		"upgradetx.Component":        true,
		"upgradetx.Domain":           true,
		"upgradetx.Phase":            true,
		"upgradetx.ReceiptKind":      true,
		"upgradetx.StepKind":         true,
		"infracontract.Phase":        true,
		"infracontract.HostOp":       true,
		// origin: hot-update S6 plan §9.3 / T54 — first live switches pin family coverage.
		"brokerimage.Mode":           true,
		"brokermaint.Phase":          true,
		"cli.RetryFamily":            true,
		"clusterupgrade.PlanScope":   true,
		"controlbudget.AttemptPhase": true,
		"proto.BrokerImageMode":      false, // Status projection has no dispatch switch.
		// S5 switches now name every wire/private member; keep coverage non-vacuous.
		// origin: hot-update S5 plan §2.2 / §10.2
		"flow.Kind":             true,
		"flow.ResumeMode":       true,
		"sealedstate.FieldType": true,
		"sealedstate.Kind":      true,
		"ssproxy.SSPhase":       true,
		"agent.openOutcome":     true,
		// h1 D: the ctl-liveness reaper switches on shouldReapRun's verdict —
		// from here on that switch's exhaustiveness is load-bearing (each
		// verdict maps to a different irreversible action).
		"agent.reapVerdict":           true,
		"authcallout.role":            true,
		"broker.admitRole":            true,
		"broker.abandonRefusal":       true, // external review F2: handleFinalizeReq's reply selection
		"brokerruntime.CutoverAction": true,
		// The moved ledger disposition and caps verdict switches (S7 source moves),
		// the first live switches over the S7 contract enums and the S9 Caddy cursor.
		// origin: hot-update S7 plan §11.2 / T52; S9 plan §12 P13
		"xferledger.LegacyDisposition":    true,
		"cli.TransferCapsStatus":          true,
		"xfercontract.AuditDelivery":      true,
		"xfercontract.CancelReason":       true,
		"xfercontract.ControlKind":        true,
		"xfercontract.LedgerMutationKind": true,
		"xfercontract.ObjectWritePhase":   true,
		"xfercontract.Operation":          true,
		"xfercontract.Phase":              true,
		"xfercontract.PrimitiveOutcome":   true,
		"xfercontract.PrincipalRole":      true,
		"xfercontract.StatusState":        true,
		"infraupgrade.caddyCursor":        true,
		// gotcha #83 ⑤: the two String() switches over the start-joiner boundary's facts. Every member
		// is enumerated and there is no default; a fifth nats state or a third process state must
		// name its own string and its own row in joinerStartGrace's table.
		"main.natsLocalState":   true,
		"main.brokerProcState":  true,
		"natsconf.RenderIntent": true,
		"natsconf.TopoState":    true,
		// hot-update S2: spawnsafe.Policy.Export switches on a mount verdict's
		// probe state to decide what crosses an image (healthy / sticky dead /
		// nothing). A fourth state must say which of the three it exports as.
		"spawnsafe.probeState": true,
	}
	var lostCoverage, newlySwitched []string
	for family := range enumFamilies {
		hit := hitsByFamily[family] > 0
		switch {
		case familiesWithSwitches[family] && !hit:
			lostCoverage = append(lostCoverage, family)
		case !familiesWithSwitches[family] && hit:
			newlySwitched = append(newlySwitched, family)
		}
	}
	sort.Strings(lostCoverage)
	sort.Strings(newlySwitched)
	if len(lostCoverage) > 0 {
		t.Errorf("%d family(ies) that had switches now match NONE:\n  %s\n\n"+
			"Either the constant-name prefixes in enumFamilies no longer match how the constants are spelled "+
			"— in which case those switches are UNGUARDED while this gate reports green — or the switches "+
			"were deleted. The global floor this replaced (`guarded >= 5`) was satisfied by "+
			"natsconf.TopoState's five switches on its own, so it could not see this at all.",
			len(lostCoverage), strings.Join(lostCoverage, "\n  "))
	}
	if len(newlySwitched) > 0 {
		t.Errorf("%d family(ies) are now matched by a switch but are not in familiesWithSwitches:\n  %s\n\n"+
			"Somebody wrote the first switch over that enum. Add it to the list in the same commit: from now "+
			"on its coverage is load-bearing, and this is the assertion that will notice if it stops.",
			len(newlySwitched), strings.Join(newlySwitched, "\n  "))
	}
}

// TestEnumFamiliesCoverEveryIotaEnum keeps the family table from silently under-covering the tree.
//
// origin: line-2 external review IDG-3 / PI-13. enumFamilies shipped with THREE entries while the tree
// had thirteen closed enums, so the escape hatch stayed open on ten of them while the gate reported
// green. The count is the whole problem: a registry at 3/13 is worse than no registry, because its green
// is read as coverage. Re-deriving the denominator from source is the only way that cannot drift.
func TestEnumFamiliesCoverEveryIotaEnum(t *testing.T) {
	root := repoRoot(t)
	prefixes := enumConstPrefixes()
	members := enumMembers(t, root, prefixes)
	// origin: hot-update S5 plan §2 / §10.2 — these families have no iota
	// declaration to trigger the historical registration check below.
	closedFamilies := flowSSClosedFamilies()
	for family, count := range transferClosedFamilies() {
		closedFamilies[family] = count
	}
	for family, count := range brokerClosedFamilies() {
		closedFamilies[family] = count
	}
	for family, count := range closedFamilies {
		if _, registered := enumFamilies[family]; !registered {
			t.Errorf("closed wire family %s is not registered", family)
		}
		if len(members[family]) != count {
			t.Errorf("closed wire family %s: discovered %d members, want %d", family, len(members[family]), count)
		}
	}

	// Every `= iota` const block in prod code, by the name of its first member.
	var unregistered []string
	total := 0
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata", "test":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, p, nil, 0)
		if perr != nil {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		ast.Inspect(f, func(n ast.Node) bool {
			gd, ok := n.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				return true
			}
			isIota, first := false, ""
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for _, v := range vs.Values {
					ast.Inspect(v, func(n ast.Node) bool {
						if id, ok := n.(*ast.Ident); ok && id.Name == "iota" {
							isIota = true
						}
						return true
					})
				}
				if first == "" && len(vs.Names) > 0 {
					first = vs.Names[0].Name
				}
			}
			if !isIota || first == "" {
				return true
			}
			total++
			for pre := range prefixes {
				if strings.HasPrefix(first, pre) {
					return true
				}
			}
			unregistered = append(unregistered,
				filepath.ToSlash(rel)+":"+strconv.Itoa(fset.Position(gd.Pos()).Line)+
					"  (first member "+first+")")
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	sort.Strings(unregistered)
	if len(unregistered) > 0 {
		t.Errorf("%d of %d iota enum(s) have no entry in enumFamilies:\n  %s\n\n"+
			"An unregistered enum is invisible to TestNoDefaultOnRepoEnumSwitch, so a `default:` on a "+
			"switch over it silences exhaustive permanently and the next member added inherits the "+
			"fallback. Add a prefix to enumFamilies.",
			len(unregistered), total, strings.Join(unregistered, "\n  "))
	}
	if total < 10 {
		t.Errorf("only found %d iota enums in prod code; the scan is broken (measured: 13)", total)
	}
	if len(members) < 10 {
		t.Errorf("enumMembers resolved only %d families; a family with no members makes every switch "+
			"over it pass vacuously", len(members))
	}
}

func flowSSClosedFamilies() map[string]int {
	return map[string]int{
		"flow.Kind": 9, "flow.Phase": 5, "flow.Reason": 16,
		"flow.ResumeScope": 2, "flow.ResumeMode": 6, "flow.ResumeVerdict": 3, "flow.CloseMode": 4,
		"flowstate.EndpointPhase": 6, "flowstate.TicketPurpose": 2,
		"ssproxy.SSPhase": 9, "ssproxy.AuthorityPhase": 3,
		"rawsocket.ConnectState": 3, "sealedstate.Kind": 12, "sealedstate.FieldType": 8,
	}
}

// origin: hot-update S5 plan §10.1 / T49 — exercise the same member discovery
// as the production scan, including aliases and a member with a new prefix.
func TestFlowSSEnumControls(t *testing.T) {
	prefixes := enumConstPrefixes()
	members := enumMembers(t, repoRoot(t), prefixes)
	for family, count := range flowSSClosedFamilies() {
		if len(members[family]) != count {
			t.Errorf("%s has %d discoverable members, want %d", family, len(members[family]), count)
		}
	}
	for _, tc := range []struct {
		pkg, typ, constants, family string
	}{
		{"flow", "Kind uint8", "KindOpen Kind = 1; KindOpenACK Kind = 2; FutureFrame Kind = 10", "flow.Kind"},
		{"sealedstate", "Kind = string", "KindFlow Kind = \"flow\"; KindSSActor Kind = \"ss-actor\"; FutureRecord Kind = \"future\"", "sealedstate.Kind"},
		{"sealedstate", "FieldType = uint8", "FieldU64 FieldType = 1; FieldBytes FieldType = 4; FutureField FieldType = 9", "sealedstate.FieldType"},
	} {
		t.Run(tc.family, func(t *testing.T) {
			f, err := parser.ParseFile(token.NewFileSet(), "contracts.go", "package "+tc.pkg+"\ntype "+tc.typ+"\nconst ("+tc.constants+")", 0)
			if err != nil {
				t.Fatal(err)
			}
			got := enumFileMembers(f, prefixes)[tc.family]
			if len(got) != 3 {
				t.Fatalf("explicit members/aliases discovered = %v, want all three", got)
			}
			for name := range got {
				if strings.HasPrefix(name, "Future") {
					continue
				}
				cc := &ast.CaseClause{List: []ast.Expr{ast.NewIdent(name)}}
				if family := caseEnumFamily(cc, prefixes); family != tc.family {
					t.Errorf("case %s resolves to %s, want %s", name, family, tc.family)
				}
			}
			var all []ast.Expr
			for name := range got {
				all = append(all, ast.NewIdent(name))
			}
			// Test every possible missing member, including a future member
			// outside the registered prefixes. Existing cases identify its family.
			for missing := range got {
				var cases []ast.Expr
				for _, e := range all {
					if e.(*ast.Ident).Name != missing {
						cases = append(cases, e)
					}
				}
				sw := &ast.SwitchStmt{Body: &ast.BlockStmt{List: []ast.Stmt{&ast.CaseClause{List: cases}, &ast.CaseClause{}}}}
				family, hasDefault, gaps := enumSwitchGap(sw, prefixes, map[string]map[string]bool{tc.family: got})
				if family != tc.family || !hasDefault || len(gaps) != 1 || gaps[0] != missing {
					t.Errorf("default concealed %s: family=%s default=%v gaps=%v", missing, family, hasDefault, gaps)
				}
			}
			sw := &ast.SwitchStmt{Body: &ast.BlockStmt{List: []ast.Stmt{&ast.CaseClause{List: all}, &ast.CaseClause{}}}}
			if _, hasDefault, gaps := enumSwitchGap(sw, prefixes, map[string]map[string]bool{tc.family: got}); !hasDefault || len(gaps) != 0 {
				t.Errorf("fully enumerated defensive default rejected: %v", gaps)
			}
		})
	}
}

// enclosingFuncName returns the name of the function containing pos, or "<file-level>" for a switch inside
// a package-level declaration. Methods are spelled "Recv.Method" so two same-named methods on different
// types do not collide.
//
// origin: line-2 closure verification §6 B4 — the key scheme for allowedEnumDefaults, chosen to be immune
// to a comment inserted above the site. Same shape as the sibling helper in
// test/architecture/tls_verify_pairing_test.go; the two ledgers are in different packages, so sharing the
// function would mean a third package existing only to hold it.
func enclosingFuncName(f *ast.File, pos token.Pos) string {
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Pos() > pos || fn.End() < pos {
			continue
		}
		name := fn.Name.Name
		if fn.Recv != nil && len(fn.Recv.List) > 0 {
			t := fn.Recv.List[0].Type
			if star, isStar := t.(*ast.StarExpr); isStar {
				t = star.X
			}
			if id, isIdent := t.(*ast.Ident); isIdent {
				name = id.Name + "." + name
			}
		}
		return name
	}
	return "<file-level>"
}

// Specific prefixes win over historical broad ones regardless of map order.
// origin: hot-update S6 plan §13 / T54
func enumFamilyForName(name string, prefixes map[string]string) string {
	family, longest := "", 0
	for prefix, candidate := range prefixes {
		if len(prefix) > longest && strings.HasPrefix(name, prefix) {
			family, longest = candidate, len(prefix)
		}
	}
	return family
}

// cli.RetryFamily 3 -> 4: S7 P26 appended RetryTransfer (plan §7 / T32).
func brokerClosedFamilies() map[string]int {
	closed := map[string]int{"storage.ClosePhase": 4, "brokerimage.Mode": 3, "brokermaint.Phase": 3,
		"cli.RetryFamily": 4, "controlbudget.AttemptPhase": 21, "clusterupgrade.PlanScope": 2}
	for family, count := range brokerStringFamilies() {
		closed[family] = count
	}
	return closed
}

func brokerStringFamilies() map[string]int {
	return map[string]int{"proto.BrokerImagePhase": 8, "proto.BrokerImageMode": 3, "brokerimage.Detail": 4}
}

// Explicit string families are closed too; a default cannot hide a missing
// member, and a future typed member must be discovered even with a new prefix.
// origin: hot-update S6 plan §13 / T54
func TestBrokerEnumControls(t *testing.T) {
	prefixes := enumConstPrefixes()
	members := enumMembers(t, repoRoot(t), prefixes)
	for family, count := range brokerClosedFamilies() {
		if len(members[family]) != count {
			t.Errorf("%s: got %d members, want %d", family, len(members[family]), count)
		}
	}
	for _, name := range []string{"ModeSingle", "ModeSoleVoter", "ModeQuorum"} {
		if got := enumFamilyForName(name, prefixes); got != "brokerimage.Mode" {
			t.Fatalf("%s attributed to %s", name, got)
		}
	}
	for _, tc := range []struct{ source, family string }{
		{`package owner; import cb "github.com/LinZiyang666/tether/internal/controlbudget"; func f() { switch x { case cb.PhaseReady: } }`, "controlbudget.AttemptPhase"},
		{`package owner; import "github.com/LinZiyang666/tether/internal/agentboot"; func f() { switch x { case agentboot.PhaseReady: } }`, ""},
		{`package agentboot; func f() { switch x { case PhaseReady: } }`, ""},
		{`package owner; import cb "example.org/controlbudget"; func f() { switch x { case cb.PhaseReady: } }`, ""},
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "alias.go", tc.source, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			if cc, ok := n.(*ast.CaseClause); ok && caseEnumFamily(cc, enumPrefixesForFile(file)) != tc.family {
				t.Errorf("wrong imported enum attribution: %s", tc.source)
			}
			return true
		})
	}
	for family := range brokerClosedFamilies() {
		parts := strings.Split(family, ".")
		for missing := range members[family] {
			var cases []ast.Expr
			for name := range members[family] {
				if name != missing {
					cases = append(cases, enumMemberExpr(family, name))
				}
			}
			sw := &ast.SwitchStmt{Body: &ast.BlockStmt{List: []ast.Stmt{&ast.CaseClause{List: cases}, &ast.CaseClause{}}}}
			got, def, gaps := enumSwitchGap(sw, prefixes, members)
			if got != family || !def || len(gaps) != 1 || gaps[0] != missing {
				t.Fatalf("%s missing %s escaped: %s %v %v", family, missing, got, def, gaps)
			}
		}
		var first string
		for name := range members[family] {
			first = name
			break
		}
		file, err := parser.ParseFile(token.NewFileSet(), "contracts.go", "package "+parts[0]+"\ntype "+parts[1]+" string\nconst ("+first+" "+parts[1]+" = \"first\"; FutureMember "+parts[1]+" = \"future\")", 0)
		if err != nil {
			t.Fatal(err)
		}
		if got := enumFileMembers(file, prefixes)[family]; len(got) != 2 || !got["FutureMember"] {
			t.Fatalf("future typed member not discovered: %v", got)
		}
	}
}

// A selector must belong to the imported enum package. PhaseReady in agentboot
// is unrelated to controlbudget.PhaseReady, even though both use string enums.
// Resolve named import aliases without loading a platform-specific type graph.
// origin: hot-update S6 plan §13 / T54
func enumPrefixesForFile(file *ast.File) map[string]string {
	aliases := map[string]string{}
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil || !strings.HasPrefix(path, "github.com/LinZiyang666/tether/internal/") {
			continue
		}
		pkg := filepath.Base(path)
		alias := pkg
		if imp.Name != nil {
			alias = imp.Name.Name
		}
		// The contract's dependency-free value/port halves host xfercontract's
		// families by alias; an importer of xfervalue or xferport switches over
		// the same xfercontract enums and must stay guarded.
		// origin: hot-update S7 plan §11.1 / P00 (s7-p9i value split, R1)
		if strings.HasPrefix(path, "github.com/LinZiyang666/tether/internal/xfercontract/") {
			pkg = "xfercontract"
		}
		if alias != "_" && alias != "." {
			aliases[alias] = pkg
		}
	}
	out := map[string]string{}
	for family, prefixes := range enumFamilies {
		pkg, _, _ := strings.Cut(family, ".")
		for _, prefix := range prefixes {
			if pkg == file.Name.Name {
				out[prefix] = family
			}
			for alias, imported := range aliases {
				if imported == pkg {
					out[alias+"."+prefix] = family
				}
			}
		}
	}
	return out
}

// String-valued contracts are closed even before their first dispatch switch.
// origin: hot-update S7 plan §11.2 / T52
func transferClosedFamilies() map[string]int {
	return map[string]int{
		"xfercontract.StatusState":         3,
		"xfercontract.Phase":               10,
		"xfercontract.CommitPrimitive":     2,
		"xfercontract.PrimitiveOutcome":    3,
		"xfercontract.CancelReason":        4,
		"xfercontract.AuditDelivery":       3,
		"xfercontract.TerminalDisposition": 4,
		"xfercontract.ObjectWritePhase":    6,
		"xfercontract.ReaderPhase":         5,
		"xfercontract.LedgerMutationKind":  7,
		"xfercontract.AccessKind":          4,
		"xfercontract.Operation":           12,
		"xfercontract.ControlKind":         5,
		"xfercontract.PrincipalRole":       4,
		"xfercontract.ObservationKind":     24,
	}
}

// Exercise every missing typed member, including a future constant outside
// the registered prefixes. Registration must precede the first live switch.
// origin: hot-update S7 plan §11.2 / T52
func TestTransferEnumControls(t *testing.T) {
	prefixes := enumConstPrefixes()
	members := enumMembers(t, repoRoot(t), prefixes)
	for family, count := range transferClosedFamilies() {
		if _, registered := enumFamilies[family]; !registered || len(members[family]) != count {
			t.Fatalf("%s: registered=%v, got %d members, want %d", family, registered, len(members[family]), count)
		}
		for missing := range members[family] {
			var cases []ast.Expr
			for name := range members[family] {
				if name != missing {
					cases = append(cases, enumMemberExpr(family, name))
				}
			}
			sw := &ast.SwitchStmt{Body: &ast.BlockStmt{List: []ast.Stmt{&ast.CaseClause{List: cases}, &ast.CaseClause{}}}}
			got, def, gaps := enumSwitchGap(sw, prefixes, members)
			if got != family || !def || len(gaps) != 1 || gaps[0] != missing {
				t.Fatalf("%s missing %s escaped: %s %v %v", family, missing, got, def, gaps)
			}
		}
		parts := strings.Split(family, ".")
		var first string
		for name := range members[family] {
			first = name
			break
		}
		file, err := parser.ParseFile(token.NewFileSet(), "contracts.go", "package "+parts[0]+"\ntype "+parts[1]+" string\nconst ("+first+" "+parts[1]+" = \"first\"; FutureMember "+parts[1]+" = \"future\")", 0)
		if err != nil {
			t.Fatal(err)
		}
		if got := enumFileMembers(file, prefixes)[family]; len(got) != 2 || !got["FutureMember"] {
			t.Fatalf("%s: future typed member invisible: %v", family, got)
		}
	}
	file, err := parser.ParseFile(token.NewFileSet(), "alias.go", `package owner; import xc "github.com/LinZiyang666/tether/internal/xfercontract"; func f() { switch x { case xc.PhaseOpen: } }`, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(file, func(n ast.Node) bool {
		if cc, ok := n.(*ast.CaseClause); ok && caseEnumFamily(cc, enumPrefixesForFile(file)) != "xfercontract.Phase" {
			t.Error("contract import alias lost enum ownership")
		}
		return true
	})
}

// Family and member identities, rather than a count, are reconciled against the
// append-only S10 union. This includes the source-declared S7/S8/S9 additions,
// untyped string vocabularies, explicit wire values and value-subpackage aliases.
// Each registered prefix must still name a member; stale ledger prefixes cannot
// become a reusable allowance for an unrelated future declaration.
// origin: hot-update S10 plan §12.2 / P29
func fullstackEnumInventory(files map[string]*ast.File) ([]string, error) {
	registry := map[string][]string{}
	for name, node := range fullstackInventoryMap(files["test/determinism/enum_switch_default_test.go"], "enumFamilies") {
		values, ok := node.(*ast.CompositeLit)
		if !ok {
			return nil, fmt.Errorf("enum family %s has no prefix list", name)
		}
		for _, value := range values.Elts {
			text, err := strconv.Unquote(fullstackInventoryExpr(value))
			if err != nil || text == "" {
				return nil, fmt.Errorf("enum family %s has an invalid prefix", name)
			}
			registry[name] = append(registry[name], text)
		}
	}
	if len(registry) == 0 {
		return nil, fmt.Errorf("empty enum family root")
	}
	prefixes := map[string]string{}
	// Sort for deterministic tie-breaking; qualified type discovery below is
	// authoritative for shared Role/Phase names, not map iteration order.
	var families []string
	for family := range registry {
		families = append(families, family)
	}
	sort.Strings(families)
	for _, family := range families {
		for _, prefix := range registry[family] {
			prefixes[prefix] = family
		}
	}
	members := map[string]map[string]bool{}
	scalarFamilies := map[string]bool{}
	for path, file := range files {
		if strings.HasPrefix(path, "test/") || strings.HasSuffix(path, "_test.go") || !strings.HasPrefix(file.Name.Name, "infra") && !strings.HasPrefix(file.Name.Name, "upgrade") && !strings.HasPrefix(file.Name.Name, "xfer") {
			continue
		}
		for _, decl := range file.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}
			for _, spec := range gd.Specs {
				typ := spec.(*ast.TypeSpec)
				if _, scalar := typ.Type.(*ast.Ident); scalar {
					scalarFamilies[file.Name.Name+"."+typ.Name.Name] = true
				}
			}
		}
	}
	for path, file := range files {
		if strings.HasPrefix(path, "test/") || strings.HasSuffix(path, "_test.go") {
			continue
		}
		var owners []string
		parts := strings.Split(filepath.Dir(path), "/")
		for i := 1; i < len(parts); i++ {
			owners = append(owners, parts[i])
		}
		for family, names := range enumFileMembersInRegistry(file, prefixes, registry, owners...) {
			if members[family] == nil {
				members[family] = map[string]bool{}
			}
			for name := range names {
				members[family][name] = true
			}
		}
		// The reverse walk is independent of registration: omitting a whole
		// new family cannot erase its declaration from the denominator.
		for _, decl := range file.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				value := spec.(*ast.ValueSpec)
				if typ, ok := value.Type.(*ast.Ident); ok {
					family := file.Name.Name + "." + typ.Name
					registered := len(registry[family]) > 0
					for _, owner := range owners {
						registered = registered || len(registry[owner+"."+typ.Name]) > 0
					}
					if scalarFamilies[family] && !registered {
						return nil, fmt.Errorf("%s: unregistered actual typed family %s", path, family)
					}
				}
			}
			iota, registered := false, false
			ast.Inspect(gd, func(node ast.Node) bool {
				if id, ok := node.(*ast.Ident); ok {
					iota = iota || id.Name == "iota"
					registered = registered || enumFamilyForName(id.Name, prefixes) != ""
				}
				return true
			})
			if iota && !registered {
				return nil, fmt.Errorf("%s: unregistered actual iota family", path)
			}
		}
	}
	var rows []string
	for _, family := range families {
		if len(members[family]) == 0 {
			return nil, fmt.Errorf("dead enum family %s", family)
		}
		for _, prefix := range registry[family] {
			live := false
			for member := range members[family] {
				live = live || strings.HasPrefix(member, prefix)
			}
			if !live {
				return nil, fmt.Errorf("dead enum member prefix %s.%s", family, prefix)
			}
		}
		for member := range members[family] {
			rows = append(rows, family+"\t"+member)
		}
	}
	sort.Strings(rows)
	return rows, nil
}
