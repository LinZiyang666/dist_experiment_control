// Agent upgrade entry adapters and register writers. Authorized cuts use the
// S8 engine; v1 marker projection stays isolated from the transaction journal.
// origin: hot-update S8 plan §3.3 / §4.2 / §11.2 P14.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"runtime"
	"time"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/binaryupgrade"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/xfercontract"

	"github.com/nats-io/nats.go"
)

func (a *Agent) handleUpgradeForwarded(nc *nats.Conn, msg *nats.Msg) {
	if msg.Reply == "" {
		return
	}
	var req proto.UpgradeForwardedReq
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{Code: "json_parse", Error: err.Error()})
		return
	}
	if req.ReExecOnly {
		a.handleReExecOnly(msg, req, nc)
		return
	}
	a.handleUpgradeInstallLocked(msg)
}

func leasedInstanceRefusesUpgrade(a *Agent) bool {
	return nidOf(a) != a.cfg.NID
}

// handleUpgradeInstallLocked preserves the old method surface as an explicit
// refusal. Its envelope cannot authorize the transaction install pipeline.
func (a *Agent) handleUpgradeInstallLocked(msg *nats.Msg) {
	// origin: hot-update S8 plan §3.3 / §7.2. The old envelope carries no
	// target-issued grant. A new agent cannot download, flip or cut from it;
	// the authorized transaction endpoint owns those effects and their lease.
	a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{Code: "upgrade_deferred", Error: "a target-issued upgrade transaction is required"})
}

// handleReExecOnly refuses grantless execution of staged bytes. Root supply
// does not replace the canonical journal, recorded prev or inherited lease.
func (a *Agent) handleReExecOnly(msg *nats.Msg, _ proto.UpgradeForwardedReq, _ ...*nats.Conn) {
	// origin: hot-update S8 plan §3.3 / §5.2. Root staging supplies bytes,
	// not execution authority. The same transaction, prev proof and long OFD
	// lease are mandatory for shared-binary re-exec and writable installs.
	a.replyUpgradeForwarded(msg, proto.UpgradeForwardedResp{Code: "upgrade_deferred", Error: "staged re-exec requires a target-issued upgrade transaction"})
}

// reExecInPlace owns the cut latch until exec or abandon. An exec error
// thaws the original owners BEFORE any disk recovery; even the untracked
// cluster leg stays alive, since systemd stop would kill its live children.
// origin: hot-update S3 plan §2.2 / Q3.
func (a *Agent) reExecInPlace(exePath string, cut *imageCut, tracked bool) {
	if _, err := readUpgradeMarker(upgradeMarkerPath(exePath)); err != nil || a.cfg.Upgrade != nil {
		if cut != nil && cut.abandon(a) == nil && cut.releaseAdmission != nil {
			cut.releaseAdmission()
		}
		a.cfg.Logger.Error("agent: legacy re-exec refused a transaction journal")
		return
	}

	if a.cfg.UpgradeNoExit && a.cfg.UpgradeExecFn == nil {
		if cut != nil {
			_ = cut.abandon(a)
			if cut.releaseAdmission != nil {
				cut.releaseAdmission()
			}
		}
		return
	}
	expected := captureUpgradeReport(a)
	go func() {
		if cut != nil && cut.releaseAdmission != nil {
			defer cut.releaseAdmission()
		}
		var err error
		if cut != nil {
			if a.cfg.UpgradeCutObserver != nil {
				a.cfg.UpgradeCutObserver(cut.frozen.Bundle())
			}
			prepared, prepareErr := prepareImageCut(a, cut, func(b agentimage.Bundle) error {
				// Keep the fresh bundle and candidate pin explicit at the
				// final callback; the prev pin is independently checked too.
				// origin: hot-update S5 plan §5.2 / T35.
				if len(cut.readers) == 0 {
					return errImageABIUnsupported
				}
				if err := agentimage.VerifyCapabilitySHA(exePath, cut.readers[0]); err != nil {
					return err
				}
				if err := agentupgradeport.VerifyPrevReader(exePath, cut.readers); err != nil {
					return err
				}
				return agentimage.CheckBundleCapabilities(b, cut.readers...)
			})
			err = prepareErr
			if prepareErr == nil {
				execsession.LogPTYImage(a.cfg.Logger, "cut", cut.generation, cut.frozen.Bundle().Runtime)
				argv := append([]string(nil), os.Args...)
				argv[0] = exePath
				// A pathname changed after preparation cannot inherit the probe.
				err = checkUpgradeCutReaders(exePath, cut.frozen.Bundle(), cut.readers)
				if err == nil && cut.legacy {
					err = a.upgradeExec(exePath)
				} else if err == nil && a.cfg.UpgradeExecFn != nil {
					err = a.cfg.UpgradeExecFn(exePath)
				} else if err == nil {
					err = prepared.Exec(exePath, argv, execEnv())
				}
				_ = prepared.Close()
			}
		} else {
			err = a.upgradeExec(exePath)
		}
		if cut != nil {
			if thawErr := cut.abandon(a); thawErr != nil {
				// origin: hot-update S5 plan §5.4. A failed fresh grant leaves
				// SS pending and admission closed, but the local thaw succeeded.
				// The tracked disk transaction must still restore the old binary;
				// otherwise a failed exec leaves a pending marker without an owner.
				if tracked && errors.Is(thawErr, errUpgradeImageRecovery) {
					recoverCapturedExec(a, exePath, expected, err)
				}
				return
			}
		}
		if tracked && recoverCapturedExec(a, exePath, expected, err) {
			return
		}
		a.cfg.Logger.Error("agent: re-exec failed with no pending marker; staying alive on the thawed image", "err", err, "exe", exePath)
	}()
}

// recoverFromFailedExec is the F3 closer (upgrade-safety plan §3.2): when
// syscall.Exec of the just-installed binary fails, the still-running OLD
// process image is the only surviving copy of the old code — restore the prev
// slot in place and keep running (reexec=false: we already ARE the restored
// code). Returns true when a pending marker was found and handled; false
// means this exec was not a marker-tracked upgrade (the ReExecOnly leg) and
// the caller stays alive on the thawed image.
func (a *Agent) recoverFromFailedExec(exePath string, execErr error) bool {
	return recoverCapturedExec(a, exePath, captureUpgradeReport(a), execErr)
}
func recoverCapturedExec(a *Agent, exePath string, expected *upgradeMarker, execErr error) bool {
	return agentupgrade.RecoverLegacyExec(legacyAgentPort(a), exePath, expected, execErr)
}

func (a *Agent) replyUpgradeForwarded(msg *nats.Msg, resp proto.UpgradeForwardedResp) {
	if msg.Reply == "" {
		return
	}
	body, _ := json.Marshal(&resp)
	_ = msg.Respond(body)
}

// Historical smoke sentinel alias; binaryupgrade now owns this refusal.
// errUpgradeSmokeFailed marks a smoke-gate refusal (upgrade-safety plan
// §3.1): the sha-verified artifact could not exec or printed no parsable
// release tag. The artifact is bad for every node — maps to the
// `smoke_failed` wire code, which aborts a fleet rollout.

var (
	ErrUpgradeHTTPStatus    = binaryupgrade.ErrUpgradeHTTPStatus
	ErrUpgradeHTTPRetryable = binaryupgrade.ErrUpgradeHTTPRetryable
	ErrUpgradeTooLarge      = binaryupgrade.ErrUpgradeTooLarge
	// errRunningImageDiffers is the prev-slot admissibility refusal (hot-update
	// S3 plan §2.1): the on-disk binary is neither this running image nor the
	// candidate, so no rollback could safely hand it an image. The text is the
	// operator-facing `install_failed` Error verbatim.
	errRunningImageDiffers = agentimage.ErrRunningImageDiffers
)

// installPolicy carries the image-cut checks of the `node upgrade` path. The
// install primitive is also driven directly by the durability and lock-order
// tests, which run from the test binary and pass no policy.
type installPolicy struct {
	candidateABI *int                       // receives the candidate's declared image ABI
	readers      *[]agentimage.Capabilities // target and actual rollback reader, SHA-bound
	runningSHA   string                     // non-empty: enforce prev ∈ {running image, candidate}
}

func sha256OfBytes(b []byte) string {
	return agentupgradeport.SHA256OfBytes(b)
}

func sha256OfFile(path string) (string, error) {
	return agentimage.SHA256OfFile(path)
}

func (a *Agent) installNewBinary(tarball []byte, dst string, policy ...installPolicy) (string, error) {
	var pol installPolicy
	if len(policy) > 0 {
		pol = policy[0]
	}
	if _, err := readUpgradeMarker(upgradeMarkerPath(dst)); err != nil {
		return "", err
	}
	return upgradeStore().Install(tarball, dst, binaryupgrade.InstallInputs{
		CandidateABI: pol.candidateABI, Readers: pol.readers, RunningSHA: pol.runningSHA,
		Now: a.upgradeNow, Generation: func() uint64 { return a.imageGeneration + 1 },
		SID: a.cfg.SID, NID: a.cfg.NID, InstanceID: a.instanceID, Logger: a.cfg.Logger,
		CheckCandidate: func(reader agentimage.Capabilities) error { return checkCandidateABI(a, reader) },
	})
}

func copyFile(src, dst string) error {
	return agentimage.CopyBinary(src, dst, syncFile)
}

const upgradeSmokeTimeout = agentimage.VersionProbeTimeout

// errUpgradeEpochMismatch marks a smoke-gate refusal because the CANDIDATE
// binary belongs to a different ProtoVersion epoch (external review F5): the
// request's ProtoVersion only proves ctl/broker/this-agent agree — it says
// nothing about the downloaded bytes. Installing a cross-epoch binary would
// smuggle an epoch change past `proto_bump_requires_reinstall`, and the new
// process would be exactly rejected by every same-epoch broker.

const smokeOutputCap = agentimage.VersionOutputLimit

func smokeVersion(binPath string, candidateABI ...*int) (string, error) {
	return agentupgradeport.SmokeVersion(binPath, candidateABI...)
}

// smokeVersionImageABI preserves the S3 release/ABI API for older callers.
func smokeVersionImageABI(binPath string) (string, int, error) {
	release, capabilities, err := smokeVersionCapabilities(binPath)
	return release, capabilities.ABI, err
}

func smokeVersionCapabilities(binPath string) (string, agentimage.Capabilities, error) {
	return agentimage.ProbeCapabilities(binPath, agentboot.RunVersionProbe)
}

func checkUpgradeCutReaders(exePath string, b agentimage.Bundle, readers []agentimage.Capabilities) error {
	return agentupgradeport.CheckCutReaders(exePath, b, readers)
}

// initPTYUpgradeAdmission runs before runtime callbacks can admit requests.
// A marker for a sibling is not authority over this agent's PTYs. Once armed,
// only the durable commit path can release the restriction; MarkReady cannot.
// origin: hot-update S4 plan §5.2 / T31.
func initPTYUpgradeAdmission(a *Agent) error {
	exePath, err := a.upgradeExePath()
	if err != nil {
		return err
	}
	m, err := readUpgradeMarker(upgradeMarkerPath(exePath))
	if err != nil {
		return err
	}
	if m == nil || m.State != upgradeStatePending || !a.markerTargetsThisAgent(m) {
		return nil
	}
	a.gate.mu.Lock()
	a.gate.ptyBlocked = true
	a.gate.ptyUpgradeID = m.UpgradeID
	a.gate.flowBlocked, a.gate.ssBlocked = true, true
	// Fail closed before reading the SHA-bound rollback reader, including a
	// probe error. Only transfer creation is gated; retained settlement runs.
	// origin: hot-update S7 plan §8.4 / P21
	a.gate.transferBlocked = true
	a.gate.domainUpgradeID = m.UpgradeID
	a.gate.mu.Unlock()
	previous, err := previousImageCapabilities(exePath, m)
	if err != nil {
		return err
	}
	// origin: hot-update S5 plan §5.2 / T36. The actual prev's SHA-bound
	// capabilities independently release each new-domain restriction.
	a.gate.mu.Lock()
	if a.gate.domainUpgradeID == m.UpgradeID {
		a.gate.flowBlocked = agentimage.CheckDomains(previous, []string{"exec-v1", "flow-v1"}) != nil
		a.gate.ssBlocked = agentimage.CheckDomains(previous, []string{"exec-v1", "ss-v1"}) != nil
		a.gate.transferBlocked = agentimage.CheckDomains(previous, []string{"exec-v1", xfercontract.Version}) != nil
	}
	a.gate.mu.Unlock()
	// A rollback reader without pty-v1 is not an error of this boot: the
	// restriction armed above simply stays until the durable commit releases it.
	if agentimage.CheckDomains(previous, []string{"exec-v1", "pty-v1"}) == nil {
		// A capable rollback reader needs no pending restriction, including an
		// older marker without a transaction ID. This is established before any
		// subscription; later releases still require durable commit ownership.
		a.gate.mu.Lock()
		if a.gate.ptyUpgradeID == m.UpgradeID {
			a.gate.ptyBlocked = false
			a.gate.ptyUpgradeID = ""
		}
		a.gate.mu.Unlock()
	}
	return nil
}

func ptyAdmissionPending(a *Agent, upgradeID string) bool {
	a.gate.mu.Lock()
	defer a.gate.mu.Unlock()
	return upgradeID != "" && a.gate.ptyBlocked && a.gate.ptyUpgradeID == upgradeID
}

// P8 wires this into Config.PTYAdmission. It protects create, never probe:
// hiding PTYVersion would send a new ctl into the forbidden legacy fallback.
func checkPTYUpgradeAdmission(a *Agent) error {
	a.gate.mu.Lock()
	defer a.gate.mu.Unlock()
	if a.gate.ptyBlocked {
		return procrequest.ErrBusy
	}
	return nil
}

func releasePTYUpgradeAdmission(a *Agent, upgradeID string) {
	a.gate.mu.Lock()
	defer a.gate.mu.Unlock()
	if upgradeID != "" && a.gate.ptyUpgradeID == upgradeID {
		a.gate.ptyBlocked = false
		a.gate.ptyUpgradeID = ""
	}
}

// A legacy marker is probed through the same bounded, isolated supervisor.
// A stored declaration with a mismatching SHA is corrupt, not a cache miss.
// executeRollback independently rechecks PrevSHA immediately before rename.
// origin: hot-update S4 plan §5.2 / T30 / T46.
func previousImageCapabilities(exePath string, m *upgradeMarker) (agentimage.Capabilities, error) {
	caps := agentimage.Capabilities{ABI: m.PrevImageABI, Domains: append([]string(nil), m.PrevImageDomains...), SHA256: m.PrevSHA}
	return agentimage.PreviousCapabilities(upgradePrevPath(exePath), caps, m.PrevImageSHA256, smokeVersionCapabilities)
}

const ssStateUnrepresentable = "ss_state_unrepresentable"

// Keep the public composition contract as the upgrade reason source of truth.
// origin: hot-update S5 internal review rv-authority-7

func flowImageOwnerReady(a *Agent) bool {
	adapter, ok := a.cfg.ExposeAdapter.(*TunnelExposeAdapter)
	return ok && adapter.client != nil && !adapter.client.FlowCensus().Unsupported
}

// Census covers metadata as well as sockets. The legacy adapter fallback is
// deliberately limited to the known production implementation: an arbitrary
// adapter's SessionCount cannot prove it has no hidden listener or FD.
// origin: hot-update S5 plan §7.3 / lead Q2 / T35 / T37.
func flowUpgradeCensus(a *Agent) tunnel.Census {
	if a.cfg.ExposeAdapter == nil {
		return includeUpgradeRehomes(a, tunnel.Census{})
	}
	if adapter, ok := a.cfg.ExposeAdapter.(interface{ FlowCensus() tunnel.Census }); ok {
		c := adapter.FlowCensus()
		// A census without the corresponding composition owner is still
		// unknown responsibility, even if the adapter claims zero actors.
		if !flowImageOwnerReady(a) {
			c.Unsupported = true
		}
		return includeUpgradeRehomes(a, c)
	}
	if adapter, ok := a.cfg.ExposeAdapter.(*TunnelExposeAdapter); ok {
		// origin: hot-update S5 plan §7.3 / T37. An incompletely initialized
		// adapter cannot supply an ownership proof.
		if adapter.client == nil {
			return tunnel.Census{Unsupported: true}
		}
		c := adapter.client.FlowCensus()
		if !c.Unsupported {
			return includeUpgradeRehomes(a, c)
		}
		adapter.mu.RLock()
		n := len(adapter.localFor)
		adapter.mu.RUnlock()
		n = max(n, adapter.SessionCount())
		return includeUpgradeRehomes(a, tunnel.Census{Legacy: n})
	}
	// Unknown adapters still expose their installed count for the admission
	// handover: sample it after admission, while retaining the refusal flag.
	// origin: hot-update S5 plan §7.3 / T37
	c := tunnel.Census{Unsupported: true}
	if counter, ok := a.cfg.ExposeAdapter.(interface{ SessionCount() int }); ok {
		c.Legacy = counter.SessionCount()
	}
	return c
}

func hasFlowUpgradeState(a *Agent) bool {
	c := flowUpgradeCensus(a)
	return c.Unsupported || c.Owned+c.Legacy+c.Admission+c.Rehome+c.Retained > 0
}

func hasSSUpgradeState(a *Agent) bool {
	if a.proxy == nil {
		return false
	}
	a.proxy.mu.Lock()
	retained := a.proxy.srv != nil || a.proxy.publicPort != 0 || a.proxy.localPort != 0 ||
		a.proxy.token != "" || a.proxy.gen != 0 || a.proxy.epoch != 0 || a.proxy.needsReestablish || a.proxy.digestApplied
	a.proxy.mu.Unlock()
	// origin: hot-update S5 plan §7.2 / T35. Ordering and fired cleanup are
	// responsibilities even after the last listener/server was removed.
	a.flcMu.Lock()
	retained = retained || a.flcTimer != nil || a.proxy.flcState.DeadlineBootNS != 0 || a.proxy.flcState.Fired || a.proxy.flcState.CleanupPending
	a.flcMu.Unlock()
	a.proxyDrainMu.Lock()
	retained = retained || a.proxyDispatchN != 0
	a.proxyDrainMu.Unlock()
	return retained
}

func checkSSImageState(a *Agent) error {
	return agentupgradeport.CheckSSImageState(hasSSUpgradeState(a) || a.gate.count(gateSS) != 0, func() error {
		_, err := snapshotProxyCutUntil(a, time.Now().Add(upgradebudget.SafePointWait))
		return err
	})
}

func requiredUpgradeDomains(a *Agent) []string {
	return agentupgradeport.RequiredRuntimeDomains(a.processRuntime, hasFlowUpgradeState(a), hasSSUpgradeState(a), transferImageWorker(a) != nil)
}

// These guards must run before journal/accept/listener/keyset side effects;
// probe capability stays visible while a pending old reader blocks creation.
// origin: hot-update S5 plan §5.2 / T36.
func checkFlowUpgradeAdmission(a *Agent) error {
	a.gate.mu.Lock()
	defer a.gate.mu.Unlock()
	if a.gate.closed || a.gate.flowBlocked {
		return procrequest.ErrBusy
	}
	return nil
}

func checkSSUpgradeAdmission(a *Agent) error {
	a.gate.mu.Lock()
	defer a.gate.mu.Unlock()
	if a.gate.closed || a.gate.ssBlocked {
		return procrequest.ErrBusy
	}
	return nil
}

func flowSSAdmissionPending(a *Agent, upgradeID string) bool {
	a.gate.mu.Lock()
	defer a.gate.mu.Unlock()
	return upgradeID != "" && a.gate.domainUpgradeID == upgradeID && (a.gate.flowBlocked || a.gate.ssBlocked || a.gate.transferBlocked)
}

func releaseFlowSSUpgradeAdmission(a *Agent, upgradeID string) {
	a.gate.mu.Lock()
	defer a.gate.mu.Unlock()
	if upgradeID != "" && a.gate.domainUpgradeID == upgradeID {
		a.gate.flowBlocked, a.gate.ssBlocked = false, false
		// Called only after the durable marker write, never by MarkReady or
		// a visible committed rename whose directory sync still failed.
		// origin: hot-update S7 plan §8.4 / P21
		a.gate.transferBlocked = false
		a.gate.domainUpgradeID = ""
	}
}

// The caller keeps the exact report identity across the network wait.
// origin: hot-update S8 plan §3.3 / M11.
func registerAgent(ctx context.Context, a *Agent, nc *nats.Conn, delivery **upgradeMarker) (proto.NodeRegisterResp, error) {
	req := proto.NodeRegisterReq{
		ProtoVersion:              proto.ProtoVersion,
		ReleaseVersion:            proto.ReleaseVersion,
		NID:                       nidOf(a),
		OS:                        runtime.GOOS,
		Arch:                      runtime.GOARCH,
		BootID:                    readBootID(),
		BrokerImageMaintenance:    brokermaint.Capability,
		BrokerImageMaintenanceKey: agentMaintenanceKey(a),
		// P13: this build implements proxy-v1. S8 FIX-DESCRIBE: upgrade-tx-v1
		// only while a transaction endpoint is bound (agentupgradeport.RegisterCapabilities).
		Capabilities: agentupgradeport.RegisterCapabilities(a.upgradeEndpoint),
		// InstanceID identifies THIS process run so the broker can tell an
		// incumbent reconnecting from a second process presenting the same
		// baked credential. A non-empty value IS the capability advertisement —
		// deliberately NOT a second Capabilities token, which would change the
		// register body of every single-agent device for no benefit.
		InstanceID: a.instanceID,
		// LeasedNID tells the broker we are running under an assigned lease
		// name rather than our configured basename; it folds into the existing
		// nodes.proxy_capable expression so an ephemeral instance never holds a
		// public egress port.
		LeasedNID: nidOf(a) != a.cfg.NID,
		// Consumed with Swap: it is only meaningful on the FIRST register after
		// an adoption. Carrying it further would let a much later register claim
		// processes under a name this agent no longer has any relationship to.
		PreviousNID: previousNIDOnce(a),
		// The root of this agent's credential family, stated rather than left to
		// be parsed out of NID. Without it the broker folds a device the
		// operator named `gpu-02` into the `gpu` family and offers its clone
		// `gpu-03` — a name in a different family that this agent's binding does
		// not cover. (external review F2)
		ConfiguredNID: a.cfg.NID,
		// D6 §6.5: self-report the DETERMINISTIC nats server_name we are connected
		// to (NOT the volatile NUID) so the leader can bridge it to a home broker.
		// "" on a single-node bus → inert. ConnectedServerName reflects the actual
		// connected server, updated across reconnects (drives §7.4 rehome).
		ServerID: nc.ConnectedServerName(),
		// C1: report our cached signed-roster generation so the leader can flag a
		// sustained-behind agent (agent_roster_stale). 0 ⇒ pre-C1 / no cache. Advisory.
		RosterGen: a.cachedRosterGen(),
		// #78: the proxy.participate opt-out. Capabilities stays untouched —
		// nodeHasProxyCap falls back to a release-version check when the
		// token is absent, so omitting CapProxyV1 could NOT express opt-out.
		ProxyOptOut: a.cfg.ProxyOptOut,
	}
	// upgrade-safety plan §3.1: the first register after a `node upgrade`
	// carries the outcome (this one IS the health check-in when pending);
	// empty strings on every ordinary register keep the wire byte-identical.
	expectedIncarnation := agentIncarnation(a)
	marker := captureUpgradeReport(a)
	req.UpgradeState, req.UpgradeDetail = upgradeReportForMarker(a, marker)
	if delivery != nil {
		*delivery = marker
	}
	subject := proto.SubjNodeRegister(a.cfg.SID, nidOf(a))

	return agentupgrade.Register(ctx, nc, agentupgrade.RegisterPort{
		Request: req, Subject: subject, Logger: a.cfg.Logger, Timeout: a.cfg.RegisterTimeout,
		RetryInitial: a.cfg.RegisterRetryInitial, RetryMax: a.cfg.RegisterRetryMax, WaitBackoff: waitBackoff,
		Marshal: func(req proto.NodeRegisterReq) ([]byte, error) { return marshalAuthorityRegister(a, req) },
		Snapshot: func(req *proto.NodeRegisterReq) func(proto.NodeRegisterResp) {
			receipt := a.courier.registerReceipt()
			req.LocalProcesses, req.LocalPorts = a.buildLocalSnapshot()
			processes := req.LocalProcesses
			return func(resp proto.NodeRegisterResp) { a.courier.onRegisterSuccess(receipt, processes, resp) }
		},
		Authority: func(body []byte) error { return checkRegisterAuthority(a, expectedIncarnation, body) },
		Connected: func(server, url string, resp proto.NodeRegisterResp) {
			rememberConnectedBroker(a, nc, server, url, resp)
		},
		Revoke: func(code string) { revokeSessionWorkloads(a, code) }, SessionGone: errSessionGone,
	})
}
func upgradeReportForMarker(a *Agent, m *upgradeMarker) (string, string) {
	return agentupgrade.LegacyReport(legacyAgentPort(a), m)
}
func commitCapturedUpgradeReport(a *Agent, reported string, expected *upgradeMarker) {
	agentupgrade.CommitLegacyReport(legacyAgentPort(a), reported, expected)
}

func watchdogCapturedUpgrade(a *Agent, exePath string, expected *upgradeMarker) rollbackResult {
	return rollbackResult(agentupgrade.WatchdogLegacy(legacyAgentPort(a), exePath, expected))
}

// ProbeUpgradeControl is the runtime observer's fresh control-evidence port.
// The runtime still independently proves running SHA, scope and resource census.
func ProbeUpgradeControl(a *Agent, nonce string) error {
	nc, ctx := a.controlReady.Load(), a.loadRunCtx()
	if nc == nil || ctx == nil || a.cfg.Upgrade == nil {
		return &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "agent control subscriptions are not ready"}
	}
	return probeAgentControl(ctx, a, nc, nonce)
}

func marshalAuthorityRegister(a *Agent, req proto.NodeRegisterReq) ([]byte, error) {
	req.Incarnation = agentIncarnation(a)
	return marshalAgentTransferRegister(a, req)
}

func captureUpgradeReport(a *Agent) *upgradeMarker {
	exe, err := a.upgradeExePath()
	if err != nil {
		return nil
	}
	marker, _ := readUpgradeMarker(upgradeMarkerPath(exe))
	return marker
}

// The port borrows existing locks and preserves admission/rollback ownership.
// origin: hot-update S8 plan §3.3 / §12.1 / P14.
func legacyAgentPort(a *Agent) agentupgrade.LegacyPort {
	return agentupgrade.LegacyPort{
		SID: a.cfg.SID, NID: a.cfg.NID, InstanceID: a.instanceID, BootProof: a.upgradeBootProofID,
		ExePath: a.upgradeExePath, RunningSHA: a.runningImageSHA, Mu: &a.upgradeMu,
		Store: upgradeStore(), Logger: a.cfg.Logger, Stop: &a.upgradeWatchdogStop,
		RollbackInFlight: &a.rollbackInFlight, Now: a.upgradeNow, Exec: a.upgradeExec,
		NoExit: a.cfg.UpgradeNoExit, CustomExec: a.cfg.UpgradeExecFn != nil,
		AdmissionPending: func(id string) bool { return ptyAdmissionPending(a, id) || flowSSAdmissionPending(a, id) },
		ReleaseAdmission: func(id string) { releasePTYUpgradeAdmission(a, id); releaseFlowSSUpgradeAdmission(a, id) },
		ImagePhase: func() agentboot.Phase {
			if a.image == nil {
				return agentboot.PhaseNone
			}
			return a.image.Phase()
		},
		RollbackImage: func(path string, m *upgradeMarker, reason string, retry bool) bool {
			return bool(rollbackCurrentImage(a, path, m, reason, retry))
		},
	}
}

// agentRuntimeHost is the agent's half of the S8 RuntimePort. Every callback
// is a package function over the existing owners; Agent gains no method.
// origin: hot-update S8 plan §2.2 / §6.1 / §11.2 P14 P17; S8 FIX-AGENTWIRE
func agentRuntimeHost(a *Agent, ports *UpgradeDependencies) agentupgradeport.Host {
	return withTransferCensus(a, agentupgradeport.Host{
		Key: ports.Key, Journal: ports.Store, Inherited: ports.Cut,
		Owner:      func() (upgradetx.Owner, error) { return agentupgrade.ProcessOwner(a.instanceID) },
		RunningSHA: a.runningImageSHA,
		Source: func(sha string) (string, agentimage.Capabilities, error) {
			return agentupgradeport.SourceReader(a.cfg.UpgradeRunningImagePath, sha, smokeVersionCapabilities)
		},
		Probe:     smokeVersionCapabilities,
		Candidate: func(c agentimage.Capabilities) error { return checkCandidateABI(a, c) },
		Deferrals: func() []agentimage.DeferredError { return transactionDeferrals(a) },
		Required:  func() []string { return requiredUpgradeDomains(a) },
		Snapshot:  func() (execsession.Image, agentimage.CourierState, error) { return transactionSnapshot(a) },
		Freeze: func(readers []agentimage.Capabilities) (agentupgradeport.Image, error) {
			return freezeTransaction(a, readers)
		},
		Exec: func(path string, prepared *handoff.Prepared) error { return execTransaction(a, path, prepared) },
		Lease: func(ctx context.Context, key upgradetx.Key) (upgradelock.Lease, error) {
			return agentupgrade.AcquireCurrentLease(ctx, a.cfg.Upgrade, key)
		},
		Stage:       func() upgradetx.CutStage { return a.image.CutStage() },
		Control:     func(nonce string) error { return ProbeUpgradeControl(a, nonce) },
		Incarnation: func() string { return agentIncarnation(a) },
		StaleAfter:  transactionCutBudget,
	})
}

// transactionCutBudget is how long a frozen, unactivated S8 cut may hold
// business; tests shorten it to observe the stand-alone thaw.
var transactionCutBudget = upgradebudget.ComponentHandover
