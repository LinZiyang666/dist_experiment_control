package adminsock

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// Request is the on-wire admin call. Op selects the verb; the
// remaining fields are op-specific (and unused fields stay empty).
type Request struct {
	// Local receipt time charges framing and admission wait to the original
	// budget. It never travels across hosts or survives this one request.
	infraReceived time.Time
	// Infra is an independent envelope, never an operand of broker reload or
	// an S8 transaction. Legacy messages omit it byte-for-byte.
	// origin: hot-update S9 plan §7.1 / T46
	Infra *proto.InfraCommand `json:"infra,omitempty"`
	// Upgrade uses the frozen transaction envelope on the same socket. Its
	// custom marshaller emits that envelope directly, without legacy fields.
	// origin: hot-update S8 plan §2.2 / P13
	Upgrade *UpgradeRequest `json:"-"`
	Op      string          `json:"op"`

	// Audit args
	SID string `json:"sid,omitempty"` // also used by Evict
	N   int    `json:"n,omitempty"`   // audit / events tail count

	// #30 OpEvents filter: return only sys.events whose "type" equals EventKind ("" = all kinds).
	// The time window reuses the existing Since field (a Go duration string, e.g. "1h").
	EventKind string `json:"event_kind,omitempty"`

	// Evict args
	NID string `json:"nid,omitempty"`

	// Session-creator admission args (prerelease audit round 2).
	FP   string `json:"fp,omitempty"`   // SHA256:… fingerprint to admit / remove
	Note string `json:"note,omitempty"` // free text an operator can attach

	// D7 cluster args (all omitempty; byte-compatible with the original 4 ops).
	NodeID    string `json:"node_id,omitempty"`
	NodePub   string `json:"node_pub,omitempty"`   // node-identity pubkey the operator typed
	Host      string `json:"host,omitempty"`       // new node's host/raft addr
	JoinToken string `json:"join_token,omitempty"` // nonce|sig from `cluster sign-join`
	CertFP    string `json:"cert_fp,omitempty"`    // rotate-tunnel-cert / cluster add (the joiner's tunnel cert fp)
	// D9 round-1 BLOCKER: the joiner's expose-home identity (else an added voter can never
	// serve as an expose home — D6 rehome is structurally impossible). nats_server_id is
	// derived from node_id (the §6.5 SSOT), not carried here.
	TunnelAddr string `json:"tunnel_addr,omitempty"` // cluster add: the joiner's public tunnel addr
	PublicHost string `json:"public_host,omitempty"` // cluster add: the joiner's public host
	NatsRoute  string `json:"nats_route,omitempty"`  // cluster add / set-route: the NATS route URL
	// v0.4.2 set-raft-addr / set-route: allow a loopback/unspecified advertise (single-host dev only).
	AllowLoopback bool `json:"allow_loopback,omitempty"`
	// B6 A3 version-skew gate (additive, omitempty): the joiner declares what it will run, read
	// from `cluster sign-join` (defaulted from the JOINER binary's proto.{ProtoVersion,ReleaseVersion}).
	// Proto mismatch is a HARD reject; release skew is advisory (a rolling upgrade legitimately
	// mixes releases). 0 / "" = not declared (an older `cluster add`) → allow + warn.
	JoinerProto   int    `json:"joiner_proto,omitempty"`
	JoinerRelease string `json:"joiner_release,omitempty"`
	Retire        bool   `json:"retire,omitempty"`
	Now           bool   `json:"now,omitempty"`
	Abort         bool   `json:"abort,omitempty"`
	Confirmed     bool   `json:"confirmed,omitempty"` // the F==0 typed-confirm result
	// B3 item 7: cluster remove --force bypasses ONLY the new expose-ownership probe on a
	// VOTER_ADD_FAILED node (never the raft phase-gate). Additive, omitempty, LOCAL socket only.
	Force bool `json:"force,omitempty"`
	// C-rebalance: preview the proxy-home moves without executing them (`cluster rebalance proxy --dry-run`).
	// Also: online force-single Arm DryRun = a zero-mutation drill (runs the gates + reports, mints no token).
	DryRun bool `json:"dry_run,omitempty"`
	// G5 #13 OpBrokerUpgradeReload: the target release (idempotency skip if already running it) + the
	// hex sha256 the STAGED on-disk binary must match (staging is the privileged precondition; the reload
	// refuses if the on-disk binary is stale so it never re-execs a bad/unstaged image).
	ToVersion    string `json:"to_version,omitempty"`
	ExpectSHA256 string `json:"expect_sha256,omitempty"`
	// Signed remote cut identity survives the shared local admission path.
	// origin: hot-update S6 internal review rv-cluster-3
	CutID  string `json:"cut_id,omitempty"`
	Holder string `json:"holder,omitempty"`

	// Online force-single args (local socket only). ConfirmPeersDead must list EVERY other node_id (the
	// operator's typed assertion that they are TRULY dead); ArmToken is the broker-minted token from Arm,
	// presented on Commit (a stray lone Commit is refused).
	ConfirmPeersDead []string `json:"confirm_peers_dead,omitempty"`
	ArmToken         string   `json:"arm_token,omitempty"`

	// B4 operator-alert args (all omitempty; byte-compatible with the pre-B4 ops). raise
	// uses Kind/Severity/Message (+optional Label → dedup key manual:<label>); clear uses
	// DedupKey. The broker re-validates Kind==manual and Severity ∈ {info,severe}.
	AlertKind     string `json:"alert_kind,omitempty"`
	AlertSeverity string `json:"alert_severity,omitempty"`
	AlertMessage  string `json:"alert_message,omitempty"`
	AlertLabel    string `json:"alert_label,omitempty"`
	AlertDedupKey string `json:"alert_dedup_key,omitempty"`

	// B6 OPS#3: the SERVER-LOCAL bundle directory the broker writes the online backup into.
	BackupPath string `json:"backup_path,omitempty"`
	// External-review F3: opt-in to backing up a NON-leader follower (a possibly-stale local view).
	// Default false ⇒ online backup must run on the leader (the freshest committed state).
	AllowFollower bool `json:"allow_follower,omitempty"`

	// B6 OPS#12 export-incident: optional time window (a Go duration string, e.g. "24h") + sid
	// filter (default = ACTIVE sessions).
	Since string   `json:"since,omitempty"`
	SIDs  []string `json:"sids,omitempty"`

	// B7 DOC#2 `cluster ops show <node>`: when set, scope the ops view to one node (else list all).
	OpsNode string `json:"ops_node,omitempty"`

	// C4 operation-controller fields: a join bundle to approve, and an op_id to confirm/abort/show.
	JoinBundle string `json:"join_bundle,omitempty"`
	OpID       string `json:"op_id,omitempty"`

	// C2 `cluster seeds publish` args (all omitempty; byte-compatible with the pre-C2 ops).
	Bootstrap string   `json:"bootstrap,omitempty"` // the well-known HTTPS manifest URL
	Endpoints []string `json:"endpoints,omitempty"` // client-dialable NATS endpoints to publish
}

// Response is the on-wire reply. Exactly one of Sessions / Nodes /
// Audit / Evict is populated based on the request op. Error is
// non-empty for failures; OK=true means the requested action
// succeeded (used by Evict where there's no payload to inspect).
type Response struct {
	Infra *proto.InfraUpgradeResp `json:"infra,omitempty"`
	// Full transaction views are never copied into the public RuntimeReport.
	// origin: hot-update S8 plan §2.2 / P13
	Upgrade *UpgradeResponse `json:"-"`
	Op      string           `json:"op"`

	OK    bool   `json:"ok,omitempty"`
	Error string `json:"error,omitempty"`
	// B2 item 4: stable machine error code (one of the Code* consts), "" on success / legacy.
	// Additive + omitempty ⇒ an old CLI ignores it and an old broker never sets it.
	Code string `json:"code,omitempty"`

	// G5 #13 OpBrokerUpgradeReload result: Reloading = a re-exec into the staged binary was armed (the ctl
	// channel drops momentarily as the broker restarts); AlreadyAtVersion = idempotent no-op skip.
	Reloading        bool `json:"reloading,omitempty"`
	AlreadyAtVersion bool `json:"already_at_version,omitempty"`

	// Share the remote shape; an armed next generation is distinct from a
	// completed cut witnessed by the actually running image.
	// origin: hot-update S6 plan §9.1 / T46
	BrokerImage    *proto.BrokerImageStatus `json:"broker_image,omitempty"`
	NextGeneration uint64                   `json:"next_generation,omitempty"`

	// Creators is the session-create allow-list (OpSessionCreators). prerelease audit round 2.
	Creators []CreatorEntry `json:"creators,omitempty"`

	Sessions []SessionEntry `json:"sessions,omitempty"`
	Nodes    []NodeEntry    `json:"nodes,omitempty"`
	Audit    []AuditEntry   `json:"audit,omitempty"`

	// Events (#30 OpEvents) is the tail of the H.1 `events` stream, oldest→newest. It reuses
	// AuditEntry (subject/seq/ts/body) — an events message is the same decoded-JS shape; body carries
	// the event "type" + its allow-listed scalar fields.
	Events []AuditEntry `json:"events,omitempty"`
	// Truncated (#30 / external review N-5) is set when an OpEvents tail stopped EARLY — at the
	// eventsMaxScan cap or a request-deadline mid-scan — so older matching messages were NOT examined
	// and the tail is partial. Additive + omitempty: an old CLI ignores it, an old broker never sets
	// it, so both directions degrade to today's behavior. Not a schema-version bump.
	Truncated bool `json:"truncated,omitempty"`

	Evict *EvictResult `json:"evict,omitempty"`

	// Runtime (R13) is the OpRuntime process-introspection snapshot.
	Runtime *RuntimeReport `json:"runtime,omitempty"`

	// Alert (B4) reports what an operator alert raise/clear did: the resolved dedup_key
	// (so the CLI can echo the exact `alert clear <key>` command) and whether the raise
	// was a no-op because a manual alert was already ACTIVE for that key.
	Alert *AlertResult `json:"alert,omitempty"`

	// D7 cluster reply fields.
	Cluster *ClusterStatusReport `json:"cluster,omitempty"`
	Homes   *ClusterHomesReport  `json:"homes,omitempty"` // C6 建议5: `cluster status --homes`

	// C-rebalance: `cluster rebalance proxy` result (which proxy homes moved / would move).
	ProxyRebalance *ProxyRebalanceReport `json:"proxy_rebalance,omitempty"`

	// Online force-single Arm/Commit result: the per-peer probe verdict, whether the gates would pass,
	// the arm token (Arm, non-dry-run), the dwell remaining, and the abandoned peers (Commit).
	ForceSingle *ForceSingleReport `json:"force_single,omitempty"`

	QuorumProj *QuorumProjection `json:"quorum_proj,omitempty"` // set when an F==0 confirm is required
	NotLeader  bool              `json:"not_leader,omitempty"`  // admin change attempted on a follower
	LeaderHost string            `json:"leader_host,omitempty"` // where to re-run (empty mid-election)
	Nonce      string            `json:"nonce,omitempty"`       // cluster add step-1 challenge: sign this on the joiner

	// B6 OPS#3 online backup result.
	Backup *BackupResult `json:"backup,omitempty"`

	// B6 OPS#12 export-incident result.
	Incident *IncidentBundle `json:"incident,omitempty"`

	// B7 DOC#2 ops controller result.
	Ops []ClusterOpEntry `json:"ops,omitempty"`

	// C4: the op_id (= plan-id) a join-approve / retire created, for the CLI `--wait` poller.
	OpID string `json:"op_id,omitempty"`

	// C2 seeds publish/show result. AccountPub is included so the CLI can mint a full invite
	// (account_pub, not a fingerprint); Generation is the new seed_generation.
	SeedAccountPub string   `json:"seed_account_pub,omitempty"`
	SeedGeneration uint64   `json:"seed_generation,omitempty"`
	SeedEndpoints  []string `json:"seed_endpoints,omitempty"`
	SeedBootstrap  string   `json:"seed_bootstrap,omitempty"`
}

// RuntimeReport (R13) is the OpRuntime process-introspection snapshot. Every field is measured
// FROM THIS PROCESS at request time — there are no cached/long-lived objects behind it (the verb
// exists to catch leaks, so it must not itself hold a goroutine, timer, or growing buffer).
//
// Goroutines is runtime.NumGoroutine(): the number of live goroutines the Go scheduler is tracking
// RIGHT NOW. That is the ONLY correct signal for a goroutine leak. Threads is a SEPARATE, distinct
// measurement (the runtime's threadcreate profile: OS threads / M EVER created, MONOTONE — it never
// falls as threads exit, so it is a high-water mark, not a live count) and MUST NOT be read as a
// goroutine proxy: 10k leaked goroutines that are all parked add ZERO OS threads, so it would report
// a flat count while the process bleeds goroutines. The two are reported side by side precisely so
// an operator can see they diverge.
type RuntimeReport struct {
	// Public allowlist only; a private Status is never serialized here.
	// origin: hot-update S9 plan §2.2 / §3.3 / T46
	InfraCapability     *proto.InfraCapability `json:"infra_capability,omitempty"`
	Infra               *proto.InfraReport     `json:"infra,omitempty"`
	InfraUpgradeVersion uint16                 `json:"infra_upgrade_version,omitempty"`
	Schema              string                 `json:"schema"`         // "admin_runtime" — machine-dispatch discriminator
	SchemaVersion       int                    `json:"schema_version"` // B2 (schema, schema_version) contract; v1

	// Goroutines is runtime.NumGoroutine() — the in-process count of live goroutines. A steadily
	// climbing value across polls is the goroutine-leak signal the fleet's incidents needed.
	Goroutines int `json:"goroutines"`
	// Threads is the runtime's threadcreate profile count: OS threads (M) EVER created — MONOTONE, it
	// never decreases as threads exit, so it is a high-water mark, not a live thread count. It is NOT a
	// goroutine proxy (see the type doc) — it is here so leaks in the two domains can be told apart.
	Threads int `json:"threads"`
	// OpenFDs is the number of open file descriptors (Linux /proc/self/fd). -1 = not measurable on
	// this platform (v1 ships Linux-only; architecture I.5). fd growth is the OTHER standard leak.
	OpenFDs int `json:"open_fds"`
	// RSSBytes is the resident-set size in bytes (Linux /proc/self/statm). -1 = not measurable.
	RSSBytes int64 `json:"rss_bytes"`
	// UptimeSeconds is wall time since the broker's Run started (from the injectable clock).
	UptimeSeconds float64 `json:"uptime_seconds"`

	// Reconcilers is one entry per registered R7 periodic reconciliation pass, carrying the
	// last-tick the registry recorded. A pass whose LastTick stops advancing while the process is
	// up is a stalled reconciler — the other class of live-broker fault this verb surfaces.
	Reconcilers []ReconcilerTick `json:"reconcilers"`
	// ClusterLoops (batch-A A5, corrected by review M8) lists the long-running
	// cluster loops with their start time only — see ClusterLoopInfo for why they
	// are NOT folded into Reconcilers. omitempty: absent in single mode, so no
	// schema_version bump (docs/usage.md §9.14 bump policy).
	ClusterLoops []ClusterLoopInfo `json:"cluster_loops,omitempty"`

	// AlertWebhook carries the alert-webhook poster's DELIVERY OUTCOME, which is a different question
	// from the loop liveness in ClusterLoops (external review B2-4).
	//
	// It is a pointer with omitempty so its ABSENCE is unambiguous: no webhook is configured. When it is
	// present the counters inside are NOT omitempty, because zero is a meaningful answer for each of them
	// — the same reasoning as ClusterLoopInfo.Iterations. Additive, so no schema_version bump
	// (docs/usage.md §9.14 bump policy).
	AlertWebhook *AlertWebhookStats `json:"alert_webhook,omitempty"`

	// The pointer distinguishes unmeasured image status from generation zero.
	// Additive to the existing admin_runtime schema; do not bump its version.
	// origin: hot-update S6 plan §9.1 / T46
	BrokerImage *proto.BrokerImageStatus `json:"broker_image,omitempty"`
}

// The local socket supplies administrator authority through its existing
// 0700/0600 admission. These ports use the same Service as remote management;
// missing ports refuse before an effect instead of invoking legacy reload.
// Plan resolves public selectors into the host's private inventory and catalog.
// Capability must perform a real wired-runtime probe, not return a build flag.
// origin: hot-update S9 plan §3.3 / §7.1 / T46
type InfraBindings struct {
	Service    c.Service
	Plan       func(proto.InfraCommand) (c.Plan, error)
	Capability func() *proto.InfraCapability
}

const (
	OpInfraUpgrade = "infra_upgrade"
	OpInfraStatus  = "infra_status"
	OpInfraRecover = "infra_recover"
	OpInfraProbe   = "infra_probe"
)

func infraOperation(op string) string {
	switch op {
	case OpInfraUpgrade:
		return "start"
	case OpInfraStatus:
		return "status"
	case OpInfraRecover:
		return "recover"
	case OpInfraProbe:
		return "probe"
	default:
		return ""
	}
}

// Retain S8's strict framing and transaction decoder. Presence (including
// null) is checked before legacy dispatch: json.Unmarshal would otherwise
// discard an infra operand and let a reload perform the wrong effect.
// origin: hot-update S9 plan §7.1 / T46 / M23
func decodeInfraAdminRequest(body []byte) (Request, error) {
	if err := upgradewire.CheckJSON(body); err != nil {
		return Request{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return Request{}, err
	}
	var op string
	if err := json.Unmarshal(fields["op"], &op); err != nil {
		return Request{}, err
	}
	if infraOperation(op) == "" {
		if fields["infra"] != nil {
			return Request{Op: op}, errors.New(proto.CodeInfraConfigInvalid)
		}
		return decodeAdminRequest(body)
	}
	var wire struct {
		Op    string              `json:"op"`
		Infra *proto.InfraCommand `json:"infra"`
	}
	if len(body) > proto.InfraMaxRequestBytes || upgradewire.StrictDecode(body, &wire) != nil || wire.Infra == nil || wire.Infra.Op != infraOperation(op) {
		return Request{Op: op}, errors.New(proto.CodeInfraConfigInvalid)
	}
	return Request{Op: op, Infra: wire.Infra}, proto.ValidateInfraCommand(*wire.Infra)
}

func infraAdminError(op, code string) Response {
	return Response{Op: op, Code: code, Error: code, Infra: &proto.InfraUpgradeResp{Code: code, Error: code}}
}

// Dispatch is a package function to keep Server's established method surface.
// It owns no detached worker: the Service must return/join under the original
// deadline and stop channel. Shadow lifetime belongs to the durable driver.
// origin: hot-update S9 plan §7.1 / §7.2 / T46
func dispatchInfra(s *Server, r Request) Response {
	if r.Infra == nil || r.Upgrade != nil || r.Infra.Op != infraOperation(r.Op) {
		return infraAdminError(r.Op, proto.CodeInfraConfigInvalid)
	}
	if err := proto.ValidateInfraCommand(*r.Infra); err != nil {
		return infraAdminError(r.Op, proto.InfraCode(err))
	}
	type legacy Request
	extra := r
	extra.Op, extra.Infra = "", nil
	body, err := json.Marshal(legacy(extra))
	if err != nil || string(body) != `{"op":""}` {
		return infraAdminError(r.Op, proto.CodeInfraConfigInvalid)
	}
	b := s.backend.Infra
	if b == nil || b.Service == nil || b.Plan == nil || b.Capability == nil {
		return infraAdminError(r.Op, proto.CodeInfraUnsupported)
	}
	ctx := s.readContext()
	remaining := time.Duration(r.Infra.RemainingNS)
	if !r.infraReceived.IsZero() {
		elapsed := s.now().Sub(r.infraReceived)
		if elapsed < 0 || elapsed >= remaining {
			return infraAdminError(r.Op, proto.CodeInfraBudgetExceeded)
		}
		remaining -= elapsed
	}
	deadline, err := controlbudget.After(remaining)
	if err != nil {
		return infraAdminError(r.Op, proto.CodeInfraBudgetExceeded)
	}
	if ctx.Err() != nil {
		return infraAdminError(r.Op, proto.CodeInfraBudgetExceeded)
	}
	response := ExecuteInfra(b, *r.Infra, deadline, ctx.Done())
	return Response{Op: r.Op, OK: response.OK, Error: response.Error, Code: response.Code, Infra: response}
}

func validateInfraPlan(r proto.InfraCommand, p c.Plan) error {
	if c.ValidatePlan(p) != nil || hex.EncodeToString(p.Tx[:]) != r.TxID || (r.Component == "nats" && p.Component != c.NATS) || (r.Component == "caddy" && p.Component != c.Caddy) {
		return errors.New(proto.CodeInfraConfigInvalid)
	}
	if (r.Action == "upgrade" && (p.Action != c.Upgrade || p.Candidate.Version != r.ToVersion || hex.EncodeToString(p.Candidate.BinarySHA256[:]) != r.ExpectSHA256)) || (r.Action == "rollback" && p.Action != c.Rollback) || (r.Action == "reload" && (p.Action != c.Reload || hex.EncodeToString(p.ConfigSHA256[:]) != r.ConfigSHA256)) {
		return errors.New(proto.CodeInfraConfigInvalid)
	}
	return nil
}

// Completed needs nonempty physical evidence and zero pending old owners.
// Error text and all private Status fields are dropped even on a failed cut.
// Query may successfully return deferred/halted state, but recovery cannot
// claim completion from it. A missing result after mutation is unknown.
// origin: hot-update S9 plan §2.3 / §7.2 / T46
func infraAdminStatus(op string, tx c.ID, s c.Status, err error) Response {
	code := proto.InfraCode(err)
	if s.Tx != (c.ID{}) && s.Tx != tx {
		code = proto.CodeInfraInternal
		s = c.Status{}
	}
	if s.Tx == (c.ID{}) && code == "" {
		code = proto.CodeInfraResultUnknown
	}
	if s.Code != "" && code == "" && op != OpInfraStatus {
		code = proto.InfraCode(errors.New(s.Code))
	}
	if s.Phase == c.Completed && (s.Step == 0 || s.Revision == 0 || s.Census.Generation == 0 || s.Census.Digest == (c.Digest{}) || s.Current.BootID == "" || s.Current.PID <= 0 || s.Current.StartTicks == 0 || s.Current.InvocationID == "" || s.Current.ExeDevice == 0 || s.Current.ExeInode == 0 || s.Current.Cgroup == "" || s.Census.OldOwnerPending != 0 || !s.Census.Complete || s.Proof == (c.Digest{}) || s.Current.ExeSHA256 == (c.Digest{})) {
		code = proto.CodeInfraRecoveryRequired
	}
	if code == "" && op != OpInfraStatus && s.Phase != c.Completed {
		code = proto.CodeInfraRecoveryRequired
		if s.Phase == c.Deferred {
			code = proto.CodeInfraNotReady
		}
	}
	response := &proto.InfraUpgradeResp{OK: code == "", Code: code, Error: code}
	if s.Tx != (c.ID{}) {
		response.Report = proto.InfraStatusReport(s)
	} else {
		// The stable requested identity survives an unknown outcome. It is a
		// query handle, never evidence that the target accepted or completed it.
		response.Report = &proto.InfraReport{TxID: hex.EncodeToString(tx[:]), Phase: "unknown", Code: code}
	}
	return Response{Op: op, OK: response.OK, Code: code, Error: code, Infra: response}
}

// Sanitize at the boundary even when a runtime collector owns the private
// status. The caller controls when a complete, real readiness probe is present.
func SetInfraRuntimeReport(r *RuntimeReport, capability *proto.InfraCapability, status *c.Status) {
	if r == nil {
		return
	}
	r.InfraCapability, r.Infra = nil, nil
	r.InfraUpgradeVersion = 0
	if capability != nil && capability.Version == proto.InfraRequestVersion && (capability.NATS || capability.Caddy) {
		copy := *capability
		r.InfraCapability = &copy
		r.InfraUpgradeVersion = proto.InfraRequestVersion
	}
	if status != nil {
		r.Infra = proto.InfraStatusReport(*status)
	}
}

func invalidInfraAdminResponse(op, detail string) Response {
	if infraOperation(op) != "" {
		return infraAdminError(op, proto.CodeInfraConfigInvalid)
	}
	return invalidAdminResponse(op, detail)
}

func fencedInfraAdminResponse(r Request) Response {
	if r.Infra != nil || infraOperation(r.Op) != "" {
		return infraAdminError(r.Op, proto.CodeInfraBusy)
	}
	return fencedAdminResponse(r)
}

func (s *Server) handleRuntime() Response {
	if s.backend.RuntimeSnapshot == nil {
		return Response{Op: OpRuntime, Error: "runtime introspection unavailable", Code: CodeBadRequest}
	}
	rep := s.backend.RuntimeSnapshot()
	if rep == nil {
		return Response{Op: OpRuntime, Error: "runtime introspection unavailable", Code: CodeBadRequest}
	}
	return Response{Op: OpRuntime, OK: true, Runtime: rep}
}
