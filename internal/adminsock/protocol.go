// Package adminsock implements the broker-local admin Unix socket
// (architecture I.2b / P9). The wire protocol is intentionally
// trivial: one line-delimited JSON Request per connection, then one
// line-delimited JSON Response, then connection closes. No streaming,
// no multiplexing — admin commands are infrequent and read-mostly,
// so per-call connect overhead is fine and the simplicity makes the
// permission model easy to reason about.
//
// All wire types live here so the server (broker side) and client
// (cmd/tether/admin side) decode the same shapes.
package adminsock

import "time"

// Op enumerates the admin verbs. Keep this list small: every
// addition needs a corresponding handler + a CLI subcommand. v1
// covers the four actions architecture P9 calls out.
const (
	OpSessions = "sessions"
	OpNodes    = "nodes"
	OpAudit    = "audit"
	OpEvict    = "evict"

	// OpRuntime (R13) is a broker-LOCAL process-introspection read: the live
	// goroutine count (runtime.NumGoroutine — the in-process TRUTH, never the OS
	// thread count as a proxy), OS thread count, open fd count, resident-set size,
	// process uptime, and each periodic reconciler's last-tick. It reads only this
	// process's own runtime + the R7 reconcile registry — no DB, no raft, no
	// leadership — so it answers in BOTH single and cluster mode and on a follower.
	// Its purpose is diagnosing a goroutine/fd leak or a stalled reconciler on a
	// LIVE broker (the fleet has had leak/crash incidents), which is why it is a
	// normal operator verb over the same root-only admin socket, NOT a build-tag /
	// env / hidden path (that would be a test backdoor, roadmap §2 T2).
	OpRuntime = "runtime"

	// OpEvents (#30) is a broker-LOCAL read of the H.1 `events` JetStream stream — the
	// operator-facing sys.events feed (session_created/destroyed, member_joined, agent_registered/
	// evicted, agent_roster_stale, disk_pressure, tetherd_restarted, plus the operational proxy_* /
	// nats_topology_* / rehome / grow-cutover kinds). Members can SUBSCRIBE to sys.events over NATS,
	// but the OPERATOR (root on the broker host, no NATS member credential) had no reader: this verb
	// is that reader, on the same root-only 0600 admin socket as OpRuntime/OpAudit/the alert verbs.
	// It tails the PERSISTED stream (so `--since`/history works, not just a live sub), filters by
	// kind, and NEVER carries a secret — the events stream payloads are hand-built allow-listed scalar
	// maps (rehome_events.go), so the reader relays only what the producers already keep secret-free.
	OpEvents = "events"

	// D7 §8.1 cluster admin verbs. They are routed to Backend.Cluster (a
	// ClusterAdminBackend); when that is nil (production until the D9 cutover) the
	// server replies "cluster mode not enabled". A non-leader broker replies with
	// NotLeader+LeaderHost so the CLI tells the operator where to re-run (§8.1: no
	// network forwarding of admin changes).
	OpClusterAdd       = "cluster_add"
	OpClusterRemove    = "cluster_remove"
	OpClusterDrain     = "cluster_drain"
	OpClusterTransfer  = "cluster_transfer"
	OpClusterStatus    = "cluster_status"
	OpClusterHomes     = "cluster_homes" // C6 建议5: aggregate all exposes + __proxy__ home/epoch/ready_reason
	OpClusterRotateCrt = "cluster_rotate_cert"
	// v0.4.2 phase-fluidity: online raft/NATS advertise-address rebind — the routine replacement
	// for force-single. SetRaftAddr (Host) rewrites raft_addr + the raft config in place;
	// SetRoute (NatsRoute) rewrites the NATS route-mesh advertise (the twin).
	OpClusterSetRaftAddr = "cluster_set_raft_addr"
	OpClusterSetRoute    = "cluster_set_route"
	// OpBrokerUpgradeReload (G5 #13) reloads THIS broker daemon into the freshly-staged on-disk binary
	// via a PID-preserving syscall.Exec. Reached ONLY via the in-process account-signed NATS upgrade
	// trigger (cluster_upgrade_trigger.go dispatches it into HandleCluster directly) — it is deliberately
	// NOT in the clusterOps routing table, so the raw admin socket answers `unknown op` for it (no CLI
	// sends it). REFUSES if this broker is the raft leader (the orchestrator transfers leadership off
	// first). Idempotent (skip if already at ToVersion) + sha-gated (never re-exec a stale binary).
	OpBrokerUpgradeReload = "broker_upgrade_reload"

	// C4 operation-controller verbs (leader-local; a follower replies NotLeader+LeaderHost).
	OpClusterJoinApprove = "cluster_join_approve" // approve a join bundle → create+drive a join op
	OpClusterRetire      = "cluster_retire"       // create+drive a recoverable retire op
	OpClusterOpConfirm   = "cluster_op_confirm"   // mid-flight re-confirm a BLOCKED op
	OpClusterOpAbort     = "cluster_op_abort"     // abort a stuck op (frees the active slot)

	// B4 operator alert verbs: raise / clear a Raft-replicated alert from the broker
	// host (operator trust tier, same as the cluster_* verbs above). Leader-local: a
	// follower replies NotLeader+LeaderHost. nil Backend.Cluster (single mode) =>
	// cluster_not_enabled (alerts are Raft-replicated; a single broker has no raft).
	OpClusterAlertRaise = "cluster_alert_raise"
	OpClusterAlertClear = "cluster_alert_clear"

	// B6 OPS#3 online backup: write a consistent { state.db, manifest.json } bundle off the
	// node's RO handle (any node, leader or follower — no raft write). nil Backend.Cluster
	// (single mode) => cluster_not_enabled (use `cluster backup --offline` for a single broker).
	OpClusterBackup = "cluster_backup"

	// C2 seed publish/show: record the cluster's public client-dialable endpoints + bootstrap URL
	// (publish, leader-only) and read them back for `agent join`/doctor (show, leader-agnostic).
	OpClusterSeedsPublish = "cluster_seeds_publish"
	OpClusterSeedsShow    = "cluster_seeds_show"

	// B6 OPS#12 incident export: a leader-local READ-ONLY assembler over the three replicated
	// sources (alerts + roster + per-sid audit). Secret-scrubbed (allowlist projection). nil
	// Backend.Cluster (single mode) => cluster_not_enabled.
	OpExportIncident = "export_incident"

	// B7 DOC#2 ops controller: a READ-ONLY view of membership operations, DERIVED from the
	// replicated cluster_nodes columns (phase / phase_changed_at / added_at / voter_add_error) —
	// no separate state, so it can never diverge from the phase SSOT. Leader-agnostic (any node
	// reads its replica). nil Backend.Cluster (single mode) => cluster_not_enabled.
	OpClusterOps = "cluster_ops"

	// C-rebalance: proactively spread the per-node __proxy__ homes evenly across the eligible
	// (deliverable + reachable) voters. The reaper only rehomes on a home DOWN; this leader-local
	// pass evens out a lopsided distribution (e.g. after `cluster add` brings a fresh empty voter,
	// or a drained broker rejoins). DryRun previews the moves. nil Backend.Cluster => single mode.
	OpClusterRebalanceProxy = "cluster_rebalance_proxy"

	// Online force-single (the quorum-loss escape hatch WITHOUT a broker stop). Two-step armed flow over
	// the LOCAL root-only admin socket (never reachable from a remote ctl): Arm runs the gates (sustained
	// quorum-loss dwell + peer-liveness HARD-REFUSE) and, unless DryRun, mints an ArmToken; Commit
	// re-checks the gates and does the in-process raft RecoverToSelfOnline. DryRun makes Arm a zero-mutation
	// drill runnable on a healthy cluster. Dispatched BEFORE the leader gate (a quorum-lost survivor is
	// never leader).
	OpClusterForceSingleArm    = "cluster_force_single_arm"
	OpClusterForceSingleCommit = "cluster_force_single_commit"
)

// Cluster-admin machine error codes (B2 item 4) — stable identifiers set on Response.Code so a
// converge/monitoring script can branch on the failure KIND instead of substring-matching prose.
// "" means success or a legacy/un-coded reply (the CLI then classifies it as exitInternal=70).
const (
	CodeNotLeader             = "not_leader"
	CodeAlreadyVoter          = "already_voter"
	CodeNotAVoter             = "not_a_voter"
	CodeCatchUpStalled        = "catch_up_stalled"
	CodeQuorumConfirmRequired = "quorum_confirm_required"
	CodeNonceUsed             = "nonce_used"
	CodeClusterNotEnabled     = "cluster_not_enabled"
	CodeNodeUnknown           = "node_unknown"
	CodeStoreError            = "store_error"
	CodeBadRequest            = "bad_request"
	CodeRemoveOwnsResources   = "remove_owns_resources" // B3 item 7: a VOTER_ADD_FAILED node still homes exposes
	CodeVersionSkew           = "version_skew"          // B6 A3: joiner proto != cluster proto (hard reject; CLI exit 64)

	// Online force-single refusal codes (the anti-split-brain gates).
	CodePeerAlive          = "peer_alive"           // a confirmed-dead peer answered a TCP probe → HARD-REFUSE
	CodeQuorumNotLost      = "quorum_not_lost"      // not (yet) continuously quorum-lost for T_dwell
	CodeForceSingleRefused = "force_single_refused" // generic force-single gate refusal
	CodeArmExpired         = "arm_expired"          // commit without a fresh arm token (missing/expired/wrong)
	CodeIsLeader           = "is_leader"            // G5 #13: reload refused because this broker is the raft leader
	CodeShaMismatch        = "sha_mismatch"         // G5 #13: on-disk binary sha != the staged target (staging didn't land)
)

// clusterOps is the set the server routes to Backend.Cluster.
//
// v0.4.2: OpClusterAdd is deliberately NOT routed. Its backend (AddNode) does a DIRECT AddVoter,
// which can wedge an N=1 cluster by admitting an unreachable voter — the exact failure this epic
// removes. The `cluster add` CLI was deleted in C8; grows now go through OpClusterJoinApprove ->
// driveJoin (nonvoter-staged, wedge-safe). Dropping OpClusterAdd here closes the last reachable
// direct-AddVoter admission path (a raw socket request is rejected as an unrouted op).
var clusterOps = map[string]bool{
	OpClusterRemove: true, OpClusterDrain: true,
	OpClusterTransfer: true, OpClusterStatus: true, OpClusterHomes: true, OpClusterRotateCrt: true,
	OpClusterSetRaftAddr: true, OpClusterSetRoute: true,
	OpClusterAlertRaise: true, OpClusterAlertClear: true,
	OpClusterBackup: true, OpExportIncident: true, OpClusterOps: true,
	OpClusterSeedsPublish: true, OpClusterSeedsShow: true,
	OpClusterJoinApprove: true, OpClusterRetire: true,
	OpClusterOpConfirm: true, OpClusterOpAbort: true,
	OpClusterRebalanceProxy: true,
	// Online force-single: routed to the backend so HandleCluster can dispatch them BEFORE the leader
	// gate. They are local-socket-only by construction (the admin socket is a root-only Unix socket; no
	// NATS path dispatches admin ops), so a remote ctl can never reach them.
	OpClusterForceSingleArm: true, OpClusterForceSingleCommit: true,
}

// Request is the on-wire admin call. Op selects the verb; the
// remaining fields are op-specific (and unused fields stay empty).
type Request struct {
	Op string `json:"op"`

	// Audit args
	SID string `json:"sid,omitempty"` // also used by Evict
	N   int    `json:"n,omitempty"`   // audit / events tail count

	// #30 OpEvents filter: return only sys.events whose "type" equals EventKind ("" = all kinds).
	// The time window reuses the existing Since field (a Go duration string, e.g. "1h").
	EventKind string `json:"event_kind,omitempty"`

	// Evict args
	NID string `json:"nid,omitempty"`

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
	Op string `json:"op"`

	OK    bool   `json:"ok,omitempty"`
	Error string `json:"error,omitempty"`
	// B2 item 4: stable machine error code (one of the Code* consts), "" on success / legacy.
	// Additive + omitempty ⇒ an old CLI ignores it and an old broker never sets it.
	Code string `json:"code,omitempty"`

	// G5 #13 OpBrokerUpgradeReload result: Reloading = a re-exec into the staged binary was armed (the ctl
	// channel drops momentarily as the broker restarts); AlreadyAtVersion = idempotent no-op skip.
	Reloading        bool `json:"reloading,omitempty"`
	AlreadyAtVersion bool `json:"already_at_version,omitempty"`

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
	Schema        string `json:"schema"`         // "admin_runtime" — machine-dispatch discriminator
	SchemaVersion int    `json:"schema_version"` // B2 (schema, schema_version) contract; v1

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
}

// ReconcilerTick is one R7 reconcile pass's observability row, projected from the registry's
// status snapshot (R13 consumes the lastTick the registry has recorded since R7a; it does not
// change the scheduler).
// ClusterLoopInfo describes one long-running cluster loop (audit publisher,
// reconciler, observe, topology reconcile, alert webhook).
//
// Batch-A review M8: these were briefly reported as ReconcilerTick rows, but the
// contract on that type is that a LastTick which stops advancing means the pass
// is STALLED — and a loop row's LastTick cannot advance, because the set only
// observes the loop's start. Four permanent false "stalled" rows in every
// `admin runtime --json` and every export-incident bundle is a worse outcome
// than not reporting the loops at all.
//
// So they get their own key with only the field that is actually knowable. Per-iteration
// liveness arrives when B7 moves these loops onto the reconciler registry.
type ClusterLoopInfo struct {
	Name      string    `json:"name"`
	StartedAt time.Time `json:"started_at"`
}

type ReconcilerTick struct {
	Name       string    `json:"name"`
	IntervalMS int64     `json:"interval_ms"`
	LeaderOnly bool      `json:"leader_only"`
	LastTick   time.Time `json:"last_tick"` // last INVOCATION of the pass fn; zero == never invoked
	Runs       uint64    `json:"runs"`
	Skips      uint64    `json:"skips"` // came due but gated off by leaderOnly (never invoked)
	LastErr    string    `json:"last_err,omitempty"`
}

// ClusterOpEntry is one membership operation, DERIVED from a cluster_nodes row (B7 DOC#2). Kind is
// inferred from phase; State is the live phase; LastError carries voter_add_error / a stall hint.
type ClusterOpEntry struct {
	NodeID    string `json:"node_id"`
	Kind      string `json:"kind"`  // add | drain | retire | join
	State     string `json:"state"` // v1 vocab: in_progress | done | failed | stalled | draining | retiring
	Phase     string `json:"phase"`
	StartedAt string `json:"started_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
	LastError string `json:"last_error,omitempty"`
	Resume    string `json:"resume,omitempty"` // operator hint for an interrupted/failed op

	// C4 real-operation-log fields (additive omitempty; present only for op-backed rows — a legacy
	// phase-derived row carries only the v1 fields above). OpState is the RICH named cursor; State
	// stays the v1 vocab a monitor negotiated on, so cluster_ops schema_version STAYS 1 (these are
	// purely additive — a v1 monitor ignores the unknown keys).
	OpID      string           `json:"op_id,omitempty"`
	TargetEnd string           `json:"target_node,omitempty"`
	OpState   string           `json:"op_state,omitempty"`
	Terminal  bool             `json:"terminal,omitempty"`
	CreatedAt string           `json:"created_at,omitempty"`
	Timeline  []ClusterOpEvent `json:"timeline,omitempty"`
}

// ClusterOpEvent is one durable transition in an operation's timeline (C4).
type ClusterOpEvent struct {
	State string `json:"state"`
	At    string `json:"at,omitempty"`
	Note  string `json:"note,omitempty"`
}

// BackupResult reports a completed online backup (the server-local bundle dir + its size + the
// committed cursor + the node identity it captured).
type BackupResult struct {
	Path         string `json:"path"`
	Bytes        int64  `json:"bytes"`
	AppliedIndex uint64 `json:"applied_index"`
	SelfID       string `json:"self_id"`
	// External-review F3 (+ re-review): backup-source freshness provenance for the CLI/DR tooling.
	// SourceRole is "leader" | "follower" (a string so the follower warning serializes).
	SourceRole string `json:"source_role"`
	LeaderID   string `json:"leader_id,omitempty"`
}

// IncidentBundle is the B6 OPS#12 read-only forensic export: the alert timeline, the membership
// timeline (allowlist-projected — NEVER the join PoP material), and per-session audit history.
// Every field is secret-scrubbed at assembly (the broker never SELECT *'s cluster_nodes and
// denylist-scrubs the open audit Body).
type IncidentBundle struct {
	Schema        string                  `json:"schema"`         // Audit UX: machine-dispatch discriminator
	SchemaVersion int                     `json:"schema_version"` // B2 (schema, schema_version) contract
	GeneratedAt   string                  `json:"generated_at"`
	LeaderID      string                  `json:"leader_id"`
	Since         string                  `json:"since,omitempty"`
	Alerts        []IncidentAlert         `json:"alerts"`
	Roster        []IncidentNode          `json:"roster"`
	Audit         map[string][]AuditEntry `json:"audit"` // keyed by sid
	// Audit EH-MAJOR-1: a forensic bundle must NEVER read as complete-but-empty when a read failed.
	// Partial=true + Errors lists any source that could not be fully assembled (mirrors B2 item 5).
	Errors  []string `json:"errors"`
	Partial bool     `json:"partial"`
}

// IncidentAlert is one alert history row (+ ack, if any).
type IncidentAlert struct {
	ID        string `json:"id"`
	Kind      string `json:"kind"`
	Severity  string `json:"severity"`
	DedupKey  string `json:"dedup_key"`
	State     string `json:"state"`
	Message   string `json:"message"`
	RaisedAt  string `json:"raised_at"`
	ClearedAt string `json:"cleared_at,omitempty"`
	AckedBy   string `json:"acked_by,omitempty"`
	AckedAt   string `json:"acked_at,omitempty"`
}

// IncidentNode is the allowlist-projected membership-timeline row. join_nonce/join_sig (the 0013
// PoP material) and cert columns are NEVER included.
type IncidentNode struct {
	NodeID         string `json:"node_id"`
	Name           string `json:"name"`
	Phase          string `json:"phase"`
	RaftAddr       string `json:"raft_addr"`
	AddedAt        string `json:"added_at,omitempty"`
	PhaseChangedAt string `json:"phase_changed_at,omitempty"`
	VoterAddError  string `json:"voter_add_error,omitempty"`
}

// ClusterStatusReport is the broker's self-report for `cluster status`/`doctor`
// (one report per NATS-reachable broker; the ctl view aggregates them). reach_source
// distinguishes a self-report from a direct :7400 raft-ping (offline mode B).
type ClusterStatusReport struct {
	SchemaVersion int    `json:"schema_version"` // §17 / review F4: monitors negotiate on this
	View          string `json:"view"`           // "ctl-nats" | "offline"
	Health        string `json:"health"`         // HEALTHY_HA | DEGRADED | QUORUM_LOST | FORCE_SINGLE | "" (offline snapshot)
	ExitCode      int    `json:"exit_code"`      // 0 | 1 | 2 | 3
	LeaderID      string `json:"leader_id"`      // empty mid-election
	Banner        string `json:"banner"`
	NextStep      string `json:"next_step"`
	// B1: plain-language, user-facing additions. All ADDITIVE — schema_version stays 1
	// (additive omitempty / always-present fields don't break a monitor that ignores
	// unknown keys; schema_version discriminates a BREAKING shape change only).
	Verdict      string `json:"verdict,omitempty"`   // voter-count plain-language verdict (authoritative socket view)
	ViewHost     string `json:"view_host,omitempty"` // SelfID of the broker that produced this self-report
	IsLeaderView bool   `json:"is_leader_view"`      // NO omitempty: false (a non-leader view) MUST serialize
	// B2 item 5: broker-side problems folded into the report instead of being dropped when a
	// report is ALSO present. Branch-load-bearing ⇒ NOT omitempty (the CLI normalizes Errors to
	// [] so `errors`/`partial` are always present for a monitor).
	Errors  []string            `json:"errors"`
	Partial bool                `json:"partial"`
	Nodes   []ClusterNodeStatus `json:"nodes"`
	// TopoDesired (C3) is the cluster-wide desired topology generation (replicated). A voter whose
	// per-node TopoObserved trails this is not yet converged → the cluster is not HEALTHY_HA.
	TopoDesired uint64 `json:"topo_desired,omitempty"`
	// HealthLabel (C6 建议6) is the hyphenated 5-state operator vocab DERIVED from Health + the voter
	// count: HEALTHY-HA | DEGRADED-WRITABLE | READ-ONLY | FORCE-SINGLE | NOT-HA | "" (offline snapshot
	// with no voter count). ADDITIVE — the legacy Health string + ExitCode + schema_version stay
	// byte-stable, so an existing monitor is untouched.
	HealthLabel string `json:"health_label"`
	// v0.4.2 phase-fluidity grow-readiness (additive omitempty; schema_version stays 1). The self
	// broker's committed raft advertise address is loopback/unspecified — peers cannot dial it, so a
	// cross-network grow cannot catch up the new voter until it is rebound with `cluster
	// set-raft-addr`. SelfNatsRouteLoopback is the twin for the :6222 route mesh. SelfRaftAdvertise
	// carries the actual committed self address for the banner/doctor detail.
	SelfRaftAdvertiseLoopback bool   `json:"self_raft_advertise_loopback,omitempty"`
	SelfNatsRouteLoopback     bool   `json:"self_nats_route_loopback,omitempty"`
	SelfRaftAdvertise         string `json:"self_raft_advertise,omitempty"`
}

// ClusterHomesReport (C6 建议5) aggregates every ALLOCATED expose + __proxy__ allocation with its home
// broker / epoch / readiness, for `cluster status --homes`. Own schema_version (independent of the
// status report). Secret-free.
type ClusterHomesReport struct {
	SchemaVersion int         `json:"schema_version"` // starts at 1
	IsLeaderView  bool        `json:"is_leader_view"` // reachability is leader-only; false ⇒ best-effort
	Homes         []HomeEntry `json:"homes"`          // [] never null, sorted (proxy-first, then port)
	Errors        []string    `json:"errors"`
	Partial       bool        `json:"partial"`
}

// HomeEntry is one expose/proxy allocation's home view (NEVER carries a token/psk).
type HomeEntry struct {
	Kind         string `json:"kind"` // "proxy" | "expose"
	SID          string `json:"sid"`
	NID          string `json:"nid"`
	Name         string `json:"name"`
	Port         int    `json:"port"`
	HomeBroker   string `json:"home_broker"`
	Epoch        int64  `json:"epoch"`
	Reconnects   int64  `json:"reconnects"`   // == Epoch (only PlanReassignHome bumps it, +1 monotone)
	PublicURL    string `json:"public_url"`   // "<home public_host>:<port>", "" when home unknown/down
	ReadyReason  string `json:"ready_reason"` // ready|no_home|catching_up|tunnel_down|keyset_stale
	Ready        bool   `json:"ready"`        // proxyInSubRender(ReadyReason)
	LastRehomeAt string `json:"last_rehome_at,omitempty"`
}

// ProxyRebalanceReport (C-rebalance) is the result of `cluster rebalance proxy`: how many proxy homes
// were (or, with --dry-run, would be) moved to even out the load across the eligible voters. Secret-free.
type ProxyRebalanceReport struct {
	DryRun  bool                 `json:"dry_run,omitempty"`
	Voters  int                  `json:"voters"`  // eligible (deliverable + reachable) voter homes considered
	Proxies int                  `json:"proxies"` // movable __proxy__ allocations (ready + home is an eligible voter)
	Planned int                  `json:"planned"` // moves the greedy planner produced (≥ len(Moves) if a pass stopped early)
	Moves   []ProxyRebalanceMove `json:"moves"`   // [] never null; one ATTEMPTED move per entry (done or error)
}

// ForceSingleReport is the online force-single Arm/Commit result.
type ForceSingleReport struct {
	WouldProceed bool `json:"would_proceed"` // Arm/dry-run: all gates pass → commit would proceed
	// BrokerSelfID is the node_id of the broker that OWNS this admin socket (F2). The CLI renders
	// the TTY confirm prompt from THIS value (not the operator-supplied --self-id) so a mistyped
	// --self-id can never quietly confirm the wrong node — and the broker rejects a mismatched arm.
	BrokerSelfID   string                 `json:"broker_self_id,omitempty"`
	Reason         string                 `json:"reason,omitempty"`          // why WouldProceed is false (dry-run drill detail / gate refusal)
	Peers          []ForceSinglePeerProbe `json:"peers,omitempty"`           // per-peer liveness verdict
	DwellRemaining string                 `json:"dwell_remaining,omitempty"` // time left before the quorum-loss dwell is satisfied ("" = satisfied)
	ArmToken       string                 `json:"arm_token,omitempty"`       // Arm (non-dry-run): present this on Commit
	Abandoned      []string               `json:"abandoned,omitempty"`       // Commit: node_ids removed from the new {self} config
}

// ForceSinglePeerProbe is one peer's liveness verdict in a ForceSingleReport.
type ForceSinglePeerProbe struct {
	NodeID    string `json:"node_id"`
	Confirmed bool   `json:"confirmed"`         // operator listed it in --confirm-peers-dead
	Alive     bool   `json:"alive"`             // a TCP probe completed on some port → HARD-REFUSE
	OnPort    string `json:"on_port,omitempty"` // which port answered (raft/nats/tunnel addr), if alive
}

// ProxyRebalanceMove is one __proxy__ home reassignment (NEVER carries a token/psk).
type ProxyRebalanceMove struct {
	SID   string `json:"sid"`
	NID   string `json:"nid"`
	Port  int    `json:"port"`
	From  string `json:"from_broker"`
	To    string `json:"to_broker"`
	Done  bool   `json:"done,omitempty"`  // executed (false on dry-run or a failed move)
	Error string `json:"error,omitempty"` // non-empty if the move's raft propose failed
}

// ClusterNodeStatus is one roster row joined against the live raft configuration.
type ClusterNodeStatus struct {
	NodeID     string `json:"node_id"`
	Name       string `json:"name"`
	Phase      string `json:"phase"` // roster phase
	Role       string `json:"role"`  // leader | voter | learner | "" (not in raft config)
	AppliedLag uint64 `json:"applied_lag"`
	// Audit OBS-MAJOR-1: last_contact_secs was a fabricated freshness signal (declared, NEVER
	// written, always 0 = "just contacted" for a dead peer) — removed. Real reachability is
	// Reachable / AppliedLag / ReachSource.
	AccountNkMatch bool   `json:"account_nk_match"`
	StreamActual   int    `json:"stream_actual"`
	StreamTarget   int    `json:"stream_target"`
	Reachable      bool   `json:"reachable"`
	ReachSource    string `json:"reach_source"` // "self" | "unverified" | "nats-health" | "raft-ping" | "disk-snapshot"
	Inconsistent   bool   `json:"inconsistent"` // phase says voter but raft config disagrees (or vice-versa)

	// B5 OPS#7 cert-rotation visibility (additive omitempty; schema_version stays 1). Public
	// fingerprints only — never private cert material. CertValidSecs is now→valid_until in
	// seconds (clock-honest, not an RFC3339 timestamp); 0/absent = no rotation window pinned.
	CertFP        string `json:"cert_fp,omitempty"`
	CertFPPrev    string `json:"cert_fp_prev,omitempty"`
	CertValidSecs int64  `json:"cert_valid_secs,omitempty"`

	// B5 OPS#9 capacity signals (additive omitempty). Populated ONLY on the self row (a leader
	// cannot statfs a peer's disk); absent on peers and on a node with no StoreDir configured.
	DiskFreePct int `json:"disk_free_pct,omitempty"`
	PortsUsed   int `json:"ports_used,omitempty"`
	PortsTotal  int `json:"ports_total,omitempty"`

	// B6 OPS#4 running version (additive omitempty; live self-report from each broker's
	// ClusterHealthResp — no persisted column to go stale on an upgrade). Empty = a node that
	// did not report (older broker / unreachable) → rendered as "?".
	ReleaseVersion string `json:"release_version,omitempty"`
	ProtoVer       int    `json:"proto_ver,omitempty"`

	// C3 topology reconcile self-report (additive omitempty; live from ClusterHealthResp). Applied =
	// generation in the on-disk conf; Observed = generation the live nats-server confirmed loading.
	// TopoReported distinguishes a C3 broker (always reports) from an older one — the HEALTHY-HA gate
	// degrades only a REPORTING voter whose Observed trails TopoDesired.
	TopoApplied         uint64 `json:"topo_applied,omitempty"`
	TopoObserved        uint64 `json:"topo_observed,omitempty"`
	TopoReconcileReason string `json:"topo_reconcile_reason,omitempty"`
	TopoReported        bool   `json:"topo_reported,omitempty"`
}

// QuorumProjection mirrors broker.QuorumProjection on the wire (the F==0 confirm gate).
type QuorumProjection struct {
	Voters         int `json:"voters"`
	Quorum         int `json:"quorum"`
	FaultTolerance int `json:"fault_tolerance"`
}

// ClusterAdminBackend handles the D7 cluster verbs. The broker provides an adapter
// that wraps its ClusterAdmin; adminsock stays a leaf (it imports neither
// internal/cluster nor internal/broker — the adapter translates to these wire
// types). nil Backend.Cluster (production until D9) => "cluster mode not enabled".
type ClusterAdminBackend interface {
	HandleCluster(req Request) Response
}

// SessionEntry mirrors the SQLite sessions row (no pin_hash; that
// column never leaves storage even via admin).
type SessionEntry struct {
	SID       string    `json:"sid"`
	Name      string    `json:"name"`
	OwnerFP   string    `json:"owner_fp"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
}

// NodeEntry mirrors the SQLite nodes row. The client derives the
// heartbeat age for display from LastHeartbeatAt.
type NodeEntry struct {
	SID             string    `json:"sid"`
	NID             string    `json:"nid"`
	Status          string    `json:"status"`
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
	BootID          string    `json:"boot_id,omitempty"`
	ReleaseVersion  string    `json:"release_version,omitempty"`
	ProtoVersion    int       `json:"proto_version,omitempty"`
}

// AuditEntry is one already-decoded audit message from
// `history-<sid>`. Subject is preserved so the operator sees which
// of {audit.call, audit.proc, audit.port} this row is, and Body is
// the raw JSON so the operator can inspect verb-specific fields
// without the admin protocol needing to mirror every audit shape.
type AuditEntry struct {
	Subject string         `json:"subject"`
	Seq     uint64         `json:"seq"`
	Ts      time.Time      `json:"ts"`
	Body    map[string]any `json:"body"`
}

// AlertResult reports the outcome of an operator alert raise/clear (B4). DedupKey is the
// resolved key (manual or manual:<label>) for the CLI to echo the clear command; AlreadyActive
// is true when a raise was a no-op because a manual alert was already ACTIVE for that key.
type AlertResult struct {
	DedupKey      string `json:"dedup_key"`
	AlreadyActive bool   `json:"already_active,omitempty"`
}

// EvictResult reports what the broker actually changed when an
// `evict` succeeded: which rows it deleted, and whether a
// sys.events agent_evicted broadcast was emitted (false when the
// target had no provisioning row, i.e. the evict was a no-op).
type EvictResult struct {
	SID                string `json:"sid"`
	NID                string `json:"nid"`
	NodeRowDeleted     bool   `json:"node_row_deleted"`
	AgentProvDeleted   bool   `json:"agent_prov_deleted"`
	BroadcastedEvicted bool   `json:"broadcasted_evicted"`
}
