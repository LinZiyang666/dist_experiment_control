# File Transfer (push / pull) — Implementation Plan

Date: 2026-05-12
Status: draft
Target release: v0.2.0

## Background

`docs/architecture.md` §941 explicitly defers file transfer to a v2
discussion: "v1 不做文件搬运原语 (push/pull); ... 视需要再评估". This
plan delivers that v2 feature.

Today operators move files by hand: `tether expose --local 8080` on
the agent, run a Python `http.server`, `curl` from ctl, `tether expose
rm`. It works but is ugly: no audit, no integrity check, no symmetric
direction (pull from agent→ctl requires ctl to first start its own
server somewhere reachable), keys/permissions left to the operator.

This plan adds two first-class verbs — `tether push` and `tether pull` —
that move a single file through the existing NATS / JetStream
control plane, reusing nkey auth_callout for authorization and the
audit stream for visibility.

## Goals

1. `tether push <local-path> <node>:<remote-path>` — copy ctl→agent (source first, scp-compatible).
2. `tether pull <node>:<remote-path> <local-path>` — copy agent→ctl (source first, scp-compatible).
3. Single regular file per invocation. Atomic destination replace
   via `<dst>.tether.tmp.<pid>` + `rename(2)` (no partially-written
   destination ever visible on success path; `<dst>.tether.tmp.*`
   may be left on agent crash and is the operator's to clean).
4. End-to-end integrity: SHA-256 verified at the receiver against
   the sender-supplied digest; mismatch → reject + cleanup of tmp.
5. Auth: same activated-member ACL as `exec`. Owner-only is **not** required (every member can push/pull).
6. Audit: each transfer emits one `audit.transfer` event per
   {start, complete, failed} into the per-session history stream.
7. Two-tier size handling, transparent to the operator:
   - **Tier A** (≤ **8 MiB binary**): single chunk over a NATS
     request/reply pair. No JetStream. The 8 MiB raw cap accounts for
     base64 expansion in JSON (`encoding/json` encodes `[]byte` as
     base64; 8 MiB raw → ~10.67 MiB base64 → fits a 16 MiB
     `max_payload` with metadata headroom).
   - **Tier B** (8 MiB – 200 MiB): per-transfer JetStream Object
     Store bucket; chunked by `nats.go`'s default 128 KiB. No
     operator-visible `--resume`.
   - **> 200 MiB** rejected with `error: file too large (XYZ MiB > 200 MiB);
     use `tether expose` + rsync/scp for bulk transfers`.
   - **Tier-A only when NATS server allows it**: ctl reads
     `nc.MaxPayload()` from the live connection at transfer time
     (returns the value the NATS server advertised in INFO). If the
     binary + base64 + JSON metadata can't fit, ctl auto-falls-back
     to Tier B (or errors `payload_too_small` if JS is also
     unavailable). Operators never see `max_payload exceeded` from
     NATS itself.
8. 1-minute total-transfer timeout for tier A; 5 minutes tier B.
   Both configurable via CLI flag.

## Non-Goals

- **Directory** push/pull (v2.1 candidate; would need tar-stream + per-entry SHA + path validation against `..`).
- **Resume** as an operator-visible feature. nats.go object store
  has internal reconnect retry, but if the whole `tether push`
  process dies the file restarts from scratch on the next invocation.
- **Compression**: not in v2. Most use cases (logs, configs, model
  shards) either compress poorly or are already compressed. Adding
  gzip on the path adds CPU + makes audit/integrity work harder. v2.1.
- **Bandwidth throttling**: rely on NATS server `max_payload` / JetStream
  rate limits if needed; not a CLI knob.
- **Symbolic-link transfers**: not in v2. Symlink at the leaf
  rejected with `not_a_regular_file`; symlink components in the
  parent chain are resolved during path validation (see "Refusing
  dangerous paths"). `--dereference` style flag is a v2.1 question.
- **Special files**: device nodes, sockets, named pipes, etc. —
  not supported; rejected with `not_a_regular_file`.
- **GB-scale files**: explicitly out-of-scope. Operator should fall
  back to `tether expose` + rsync/scp, documented in §usage.md.

## Design

### CLI surface

```
tether push <local-path> <node>:<remote-path> [--timeout DUR] [--force]
tether pull <node>:<remote-path> <local-path> [--timeout DUR] [--force]
```

- **Argument order is source → destination**, matching scp. `push`
  has local source first; `pull` has remote source first.
- `--force` is the simple flag: **default fails with `dst_exists` if
  the destination file exists**; `--force` overwrites any existing
  regular file. No attempt to detect "tether-managed" files — a
  marker scheme adds compat baggage out of proportion to the
  feature. (The receiver still writes via `.tether.tmp.<pid>` +
  `rename` for atomicity; that's an implementation detail, not a
  UX contract.)
- `--timeout` overrides tier defaults (1m / 5m). Capped at 30 min.

### Wire protocol

**Naming convention** (fix Round-2 #5):
- *Bucket* name (Object Store user-facing): `xfer-<sid>-<actor-fp-short>-<ulid>`
- *Stream* name (NATS internal, used in JS API subjects + `$O` subjects): `OBJ_<bucket>` = `OBJ_xfer-<sid>-...`
- proto fields carry the **bucket** name; permissions and broker
  reconcile use the **stream** form (prefix `OBJ_`).

**Subject map**:

| Subject | Direction | Carries |
|---|---|---|
| `tether.v1.ctrl.by.<actor>.s.<sid>.caps.req` | ctl → broker | `CapsReq` (no body) — preflight JS / max_payload probe |
| `tether.v1.s.<sid>.cmd.by.<actor>.node.<nid>.push.req` | ctl → broker → agent (held open in tier-B push) | `PushPrepareReq` |
| `tether.v1.s.<sid>.cmd.by.<actor>.node.<nid>.pull.req` | ctl → broker → agent (held open in tier-B pull until agent put done) | `PullPrepareReq` |
| `tether.v1.s.<sid>.cmd.by.<actor>.node.<nid>.push-commit.req` | ctl → broker → agent | `TransferCommitReq` (tier-B push only) |
| `tether.v1.s.<sid>.ev.node.<nid>.transfer.<id>.complete` | agent → broker | `TransferEvent{Kind:complete}` (push receiver-side outcome) |
| `tether.v1.s.<sid>.ev.node.<nid>.transfer.<id>.failed` | agent → broker | `TransferEvent{Kind:failed}` (push receiver-side outcome) |
| `tether.v1.ctrl.by.<actor>.s.<sid>.transfer.<id>.finalize.req` | ctl → broker | `TransferFinalize{OK, Code, Error, Bytes, DurationMs}` (pull receiver-side outcome — ctl IS the receiver for pull) |
| `tether.v1.s.<sid>.audit.transfer` | broker → JS (history-<sid>) | `AuditTransfer` |

**The finalization invariant (covers concerns #1 + #2)**: the broker
ALWAYS hears from the **receiver side** before writing the final audit
entry, regardless of tier. Push: agent is the receiver, agent emits
`ev.transfer.<id>.complete|failed`. Pull: ctl is the receiver, ctl
publishes `ctrl.by.<actor>.s.<sid>.transfer.<id>.finalize.req` after
its local `Get`+verify+rename (or fail). For both tiers (A and B):

| Verb | Receiver | Final-result publisher | Subject |
|---|---|---|---|
| Push tier A | agent | agent | `ev.node.<nid>.transfer.<id>.complete\|failed` |
| Push tier B | agent | agent | same (tier doesn't change who's the receiver) |
| Pull tier A | ctl   | ctl   | `ctrl.by.<actor>.s.<sid>.transfer.<id>.finalize.req` |
| Pull tier B | ctl   | ctl   | same |

`<id>` is the bucket name for tier B (`xfer-<sid>-...`) and a
short-lived ULID for tier A (so the broker can correlate ev /
finalize against the original push.req / pull.req it forwarded).
Generated by broker in step 2/3 and echoed back to ctl + agent in
PrepareResp / pull.req.forwarded.

(All `cmd.by.<actor>.node.*.*.req` and `ev.node.<nid>.>` are already
covered by existing wildcards in `PermissionsForActivatedMember` and
`PermissionsForAgent`; no new command-plane perms.)

**Full proto type list** (`internal/proto/messages.go`):

```go
// PushPrepareReq — ctl → broker on push.req.
// Tier A: InlineData populated, no bucket. Tier B: no inline data,
// ctl asks the broker to allocate a bucket; ctl receives bucket name
// in PushPrepareResp and starts Put.
type PushPrepareReq struct {
    Verb       string `json:"verb"`         // "push"
    Path       string `json:"path"`         // absolute agent-side path
    Size       int64  `json:"size"`         // bytes (informational; agent re-verifies)
    SHA256     string `json:"sha256"`       // lowercase hex
    Force      bool   `json:"force,omitempty"`
    Tier       string `json:"tier"`         // "a" | "b"
    InlineData []byte `json:"inline_data,omitempty"` // tier A only
}

// PushPrepareResp — broker → ctl on push.req reply inbox.
// Tier A: OK=true means broker accepted the request and forwarded
//   to agent. The original push.req inbox is also re-used by the
//   broker to relay the agent's reply (broker proxies, see below).
//   ctl reads OK + agent-supplied TransferID to anticipate the
//   subsequent ev.transfer event from the broker for audit
//   correlation.
//   Final outcome arrives via the broker's audit.transfer write +
//   broker proxying agent's ev.transfer reply back on this same
//   inbox after broker observes it on ev.node.<nid>.transfer.<id>.>.
//   (Avoids "ctl can't tell if push succeeded" gap.)
// Tier B: OK=true means broker created the bucket; ctl proceeds to
//   ObjectStore.Put then sends push-commit.req. Final result:
//   broker proxies agent's ev.transfer.<bucket>.complete|failed
//   onto the push-commit.req reply inbox, then deletes bucket.
type PushPrepareResp struct {
    OK          bool      `json:"ok"`
    Code        string    `json:"code,omitempty"`
    Error       string    `json:"error,omitempty"`
    Tier        string    `json:"tier,omitempty"`
    TransferID  string    `json:"transfer_id"`              // ULID; for tier B it equals Bucket suffix; for tier A it's a short token
    Bucket      string    `json:"bucket,omitempty"`         // tier-B only
    ObjectName  string    `json:"object_name,omitempty"`    // tier-B only ("data")
    ExpiresAt   time.Time `json:"expires_at,omitempty"`     // tier-B only
}

// PullPrepareReq — ctl → broker.
// Tier-not-known-yet at request time: agent will stat the path,
// determine size, compute sha256, then choose tier based on its
// own broker capabilities. ctl does NOT pre-decide.
type PullPrepareReq struct {
    Verb  string `json:"verb"`         // "pull"
    Path  string `json:"path"`
    Force bool   `json:"force,omitempty"`
}

// PullPrepareResp — broker → ctl on pull.req reply inbox.
// Tier A (single round trip): OK + Size + SHA256 + InlineData. ctl
//   writes locally + verifies + rename + sends finalize.req.
// Tier B (multi-step): OK + TransferID + Bucket + agent has already
//   begun (or finished) Put-ing into the bucket; ctl uses bucket
//   info to start ObjectStore.Get, then verifies + rename, then
//   sends finalize.req.
type PullPrepareResp struct {
    OK         bool      `json:"ok"`
    Code       string    `json:"code,omitempty"`
    Error      string    `json:"error,omitempty"`
    Tier       string    `json:"tier,omitempty"`
    TransferID string    `json:"transfer_id"`
    Size       int64     `json:"size,omitempty"`
    SHA256     string    `json:"sha256,omitempty"`
    InlineData []byte    `json:"inline_data,omitempty"`  // tier-A only
    Bucket     string    `json:"bucket,omitempty"`       // tier-B only
    ObjectName string    `json:"object_name,omitempty"`
    ExpiresAt  time.Time `json:"expires_at,omitempty"`
}

// TransferCommitReq — ctl → broker on push-commit.req. Tier-B PUSH only.
// Signals ctl finished ObjectStore.Put; broker forwards
// push-commit.req.forwarded to agent so agent starts its Get.
// Reply on this inbox is broker-proxied final outcome (see PushPrepareResp).
// Note: pull does NOT use a commit message — agent puts before
// PullPrepareResp returns to ctl, so ctl already has everything
// needed to Get directly. Pull's final outcome flows via finalize.req
// instead.
type TransferCommitReq struct {
    TransferID string `json:"transfer_id"`
    Bucket     string `json:"bucket"`
}

// TransferEvent — agent → broker on ev.node.<nid>.transfer.<id>.<kind>
// PUSH only (agent is the push receiver). Broker subscribes,
// validates, writes audit.transfer, deletes bucket (tier B), then
// proxies the result back on the push.req or push-commit.req
// reply inbox to satisfy ctl's outstanding request.
type TransferEvent struct {
    Kind       string `json:"kind"`            // "complete" | "failed"
    Verb       string `json:"verb"`            // "push"
    TransferID string `json:"transfer_id"`
    Bucket     string `json:"bucket,omitempty"`  // tier B only
    Bytes      int64  `json:"bytes,omitempty"`
    SHA256     string `json:"sha256,omitempty"`  // agent-computed cross-check
    Code       string `json:"code,omitempty"`    // failure code if Kind==failed
    Error      string `json:"error,omitempty"`
}

// TransferFinalize — ctl → broker on
// ctrl.by.<actor>.s.<sid>.transfer.<id>.finalize.req. PULL only
// (ctl is the pull receiver). Broker writes audit.transfer +
// deletes bucket (tier B) on receipt, replies OK on this inbox to
// confirm the audit landed (so ctl can exit cleanly).
type TransferFinalize struct {
    Kind       string `json:"kind"`            // "complete" | "failed"
    TransferID string `json:"transfer_id"`
    Bucket     string `json:"bucket,omitempty"`  // tier B only
    Bytes      int64  `json:"bytes,omitempty"`
    DurationMs int64  `json:"duration_ms,omitempty"`
    Code       string `json:"code,omitempty"`
    Error      string `json:"error,omitempty"`
}

// TransferFinalizeResp — broker → ctl ack of finalize.
type TransferFinalizeResp struct {
    OK    bool   `json:"ok"`
    Code  string `json:"code,omitempty"`
    Error string `json:"error,omitempty"`
}

// CapsReq / CapsResp — ctl preflight probe. Used by chooseTier
// before any push/pull to learn (a) whether broker has JetStream,
// (b) what max_payload the NATS server in front of it advertises.
// Cached for the life of one `tether` invocation. Membership-gated
// at the broker (only members of <sid> may probe).
type CapsReq struct{}
type CapsResp struct {
    OK             bool   `json:"ok"`
    Code           string `json:"code,omitempty"`
    Error          string `json:"error,omitempty"`
    JetStreamReady bool   `json:"jetstream_ready"`
    MaxPayload     int    `json:"max_payload"`        // bytes; broker echoes nc.MaxPayload() of its own ctl-facing connection
    BrokerVersion  string `json:"broker_version,omitempty"`
}
```

**Failure codes** (the set valid in `Code` fields):
`dst_exists | not_a_regular_file | sha_mismatch | too_large | tier_mismatch | path_outside_roots | path_not_absolute | transfer_disabled | payload_too_small | jetstream_unavailable | too_many_in_flight | version_skew | io_error | ctl_disconnect | agent_no_responders`.

### Tier A — inline transfer

For files ≤ 8 MiB raw binary (subject to live `nc.MaxPayload()`
allowing the JSON-encoded base64-expanded payload):

- ctl reads the file fully, computes SHA-256, puts the bytes in
  `PushPrepareReq.InlineData`. `encoding/json` encodes `[]byte` as
  base64-string; 8 MiB raw → ~10.67 MiB base64 + tiny JSON struct
  overhead → fits a 16 MiB NATS `max_payload`.
- `max_payload` is a **NATS server config** (`max_payload` in
  nats.conf), set by `scripts/install.sh` broker template to
  16777216 (16 MiB). **There is no client-side option** that can
  raise the server's limit; the client only reads the server's
  advertised value via `nc.MaxPayload()`. Operators on un-bumped
  brokers (default 1 MiB max_payload) will see `chooseTier` fall
  back to Tier B for anything that doesn't fit; if JS is also
  unavailable, the ctl returns `payload_too_small` with a hint to
  upgrade the broker.
- Agent receives, runs path validation, opens
  `<dst>.tether.tmp.<pid>` with `O_NOFOLLOW|O_EXCL`, writes,
  fsyncs, validates SHA-256, `rename(2)`.
- One round-trip; sub-second for small files on LAN.

Tier-A pull is the same shape with ctl as receiver: `pull.req` →
broker forwards → agent lstats + validates + reads + sha → reply
`PullPrepareResp{Tier:"a", Size, SHA256, InlineData}` straight to
ctl's reply inbox; ctl writes locally + verifies SHA + rename. No
commit phase, no JS bucket. (For tier-B pull, see the explicit
state machine below — pull is NOT a simple producer/consumer swap.)

### Tier B — JetStream Object Store

For files 8 – 200 MiB. **Explicit state machine** (no implementation-
time TBDs):

```
                    1. push.req           2. ack with bucket name
              ctl ───────────────► broker ──────────────► ctl
                   {size,sha,Tier=b}     {bucket, expires_at}
                                                    │
              3. ObjectStore.Put(bucket, file)      │
              ctl ──────────────────────────────► JetStream
                                                    │
              4. push.commit (after Put done)       │
              ctl ───────────────► broker           │
                                       │            │
                                       │ 5. push.commit.forwarded
                                       └─────────► agent
                                                    │
              6. ObjectStore.Get(bucket, hash+tmp)  │
                          agent ◄───────────────  JetStream
                                                    │
              7. ev.node.<nid>.transfer.complete    │
                          agent ─────────► broker   │
                                                    │
                          broker:                   │
                          - writes audit.transfer   │
                          - deletes OBJ_xfer bucket │
                                                    │
              8. push reply (with ok/code/error)    │
              ctl ◄─────────────────── broker       │
```

**Phase responsibilities** (no ambiguity about who owns what):

| Phase | Actor | Action | Failure handling |
|---|---|---|---|
| 1. `push.req` | ctl | publish `proto.PushPrepareReq{size, sha256, tier=b}` to `cmd.by.<actor>.node.<nid>.push.req`; reply inbox = ctl's | n/a |
| 2. ACL + bucket | broker | check membership; create JetStream Object Store **bucket name** `xfer-<sid>-<actor-fp-short>-<ulid>` (which nats.go backs by **stream** `OBJ_xfer-<sid>-<actor-fp-short>-<ulid>`) with `MaxBytes=200MiB, Replicas=1, MaxAge=10min for objects`. **Broker also schedules a 10-min deferred delete of the stream itself.** Reply `PushPrepareResp{bucket}` to ctl. | bucket create fails → `jetstream_unavailable`; ACL fails → `not_a_member` |
| 3. `Put` | ctl | streams file into bucket via `nats.go` ObjectStore API | NATS disconnect → fail fast, ctl returns `tier_b_put_failed` |
| 4. `push.commit` | ctl | publishes `proto.PushCommit{bucket}` to `cmd.by.<actor>.node.<nid>.push-commit.req` | timeout (60s default after Put) |
| 5. forward | broker | forwards `push.commit` to agent | unchanged from existing forward path |
| 6. `Get` + verify | agent | `ObjectStore.Get(bucket)` while computing SHA-256 + writing tmp file; on SHA match → `rename(2)`; on mismatch → delete tmp | any error → emit failure event in step 7 |
| 7. `ev.transfer.*` | agent | publishes `ev.node.<nid>.transfer.<id>.complete` or `.failed{code, msg}` (the SAME pattern as existing `ev.node.<nid>.proc.<pid>.exit`) | n/a — agent is just an emitter |
| 8. audit + cleanup + reply proxy | broker | subscribes to `s.<sid>.ev.node.*.transfer.>`; on receipt: writes `audit.transfer` (single-writer invariant preserved), deletes bucket (tier B), then **replies on the ctl's outstanding `push-commit.req` reply inbox** (broker remembers the inbox by `TransferID` from step 4–5). Reply body: `TransferEvent` (proxied agent's event verbatim with broker-validated fields) | cleanup is idempotent; the 10-min deferred delete is the safety net |

**Single-writer invariant** (per arch §C.1 §4): only the broker
publishes `audit.*`. Agents publish runtime facts on `ev.node.*` —
the existing `PermissionsForAgent` already allows this (`ev.node.<nid>.>`).

#### Tier B — pull state machine (ctl is the receiver; ctl signals finalize)

Pull's control flow differs because the **agent is the producer** and
the **ctl is the receiver**. Following the finalization invariant
(see §Wire protocol), the receiver — i.e. ctl — owns the final
audit signal via `transfer.<id>.finalize.req`.

```
              1. pull.req {path, force}
        ctl ─────────────────────────► broker
                                          │
                                          │ 2. ACL + create bucket
                                          ▼   (broker generates TransferID +
                                              creates OBJ_xfer-<sid>-<id>)
                                          │
                                          │ 3. pull.req.forwarded {transfer_id, bucket}
                                          ▼
                                       agent: validate path, lstat,
                                       compute sha256, ObjectStore.Put
                                          │
                                          │ 4. agent reply on the forwarded
                                          ▼   inbox: {OK, size, sha256, tier=b}
                                          │
        broker (proxy reply to ctl):       │
                                          │
              5. PullPrepareResp           │
              {transfer_id, bucket,       │
               size, sha256, tier=b}      │
        ctl ◄─────────────────────────── broker
                                          │
        ctl: ObjectStore.Get(bucket) into  │
        <dst>.tether.tmp.<pid>             │
        ctl: verify SHA256                 │
        ctl: rename(2)                     │
                                          │
              6. transfer.<id>.finalize.req
              {kind=complete, bytes, duration_ms}
              (or kind=failed, code, error)
        ctl ─────────────────────────► broker
                                          │
        broker:                            │
        - validate transfer_id + actor     │
        - write audit.transfer{kind=...}   │
        - DeleteObjectStore(bucket)        │
                                          │
              7. TransferFinalizeResp{OK}  │
        ctl ◄─────────────────────────── broker
        ctl exits.
```

**Differences from push**:
- Steps 2–3: broker creates bucket BEFORE agent runs (because broker
  controls bucket lifecycle, see §Auth callout perms). Agent receives
  pull.req.forwarded with the bucket name pre-populated; agent stats
  + Puts into that bucket; replies on forwarded inbox.
- Step 5: broker proxies the agent's prepare reply to ctl. This
  matches the push pattern's broker-as-reply-proxy in step 8.
- Step 6: **ctl** publishes the finalize event (NOT the agent), because
  ctl is the receiver and only ctl knows whether SHA matched and
  rename succeeded.
- Step 7: broker acks the finalize so ctl can exit cleanly with
  confirmation that audit was written. (This is broker→ctl
  acknowledgement of audit-landed, not a duplicate of step 6.)

**Failure modes**:

| Failure | Detection | Bucket cleanup | Audit |
|---|---|---|---|
| Path/sha/lstat fails on agent | step 4 reply with `OK=false` | broker deletes bucket on receipt | `kind=failed code=<from agent>` written by broker, no finalize from ctl |
| ctl dies between step 5 and step 6 | broker's 5-min finalize timeout | broker deletes bucket on timeout | `kind=failed code=ctl_disconnect` |
| ctl SHA mismatch / rename failure | step 6 with `kind=failed` | broker deletes on receipt | `kind=failed code=sha_mismatch\|io_error` written from finalize body |
| Broker dies after step 2 (bucket created, request in-flight) | G.2 reconcile-on-boot | reconcile deletes orphan bucket | no audit (gap visible by absence) |

For **tier-A pull** the same finalize protocol applies, just collapsed:
agent puts InlineData in `PullPrepareResp` (steps 2–5 collapse to one
agent reply, no bucket); ctl still sends `transfer.<id>.finalize.req`
in step 6; broker writes audit + acks in step 7. Tier-A bucket cleanup
is a no-op (no bucket).

### Tier selection

ctl, before sending `push.req`:

```go
const (
    tierAMaxRawMiB   = 8                 // 8 MiB raw binary
    tierBMaxMiB      = 200               // 200 MiB raw binary
    base64Overhead   = 4.0 / 3.0         // JSON encodes []byte as base64
    metadataReserve  = 1024              // headers, path, hash, json struct fields
)

func chooseTier(nc *nats.Conn, size int64) (string, error) {
    if size > tierBMaxMiB*1024*1024 {
        return "", errFileTooLarge
    }
    // The server's actual MaxPayload from INFO (NOT the broker.yaml
    // template — operators may run an unbumped broker).
    maxPayload := nc.MaxPayload()
    tierABase64Bytes := int64(float64(size)*base64Overhead) + metadataReserve
    if size <= tierAMaxRawMiB*1024*1024 && tierABase64Bytes <= maxPayload {
        return "a", nil
    }
    // Falls through: file too big for A *or* broker max_payload not bumped.
    return "b", nil
}
```

The agent **re-runs** the same check on receipt. Mismatch (ctl
claims A but `len(InlineData) > nc.MaxPayload()` would have
rejected at NATS layer; ctl claims B but `Size` is small) → returns
`tier_mismatch`. **No auto-fall-through** — fall-through hides
operator config drift between ctl and target agent. Operator
retries; ctl picks the right tier the second time.

### Auth callout permissions

**Command-plane subjects need NO additions** — `internal/auth/permissions.go:56`
already has `s.<sid>.cmd.by.<actor>.node.*.*.req` as a wildcard for ctl,
and `s.*.cmd.node.*.*.req.forwarded` at line 122 for agent. Adding push/pull/
push-commit/pull-commit explicitly would be redundant noise; the existing
wildcard already covers them.

**JetStream / Object Store subjects DO need additions** — and the exact
list must be derived from real `nats.go` ObjectStore behavior, not
guessed. Object Store buckets are NATS streams named `OBJ_<bucket>`;
data flows over subjects `$O.<bucket>.C.<chunk>` (chunks) and
`$O.<bucket>.M.<obj>` (metadata). The JS API subjects called by `Put`
/ `Get` / `Watch` / `Delete` are documented per nats.go release.

Plan: **enumerate exact subjects during implementation, with a unit
test that fakes a NATS server and asserts what subjects ObjectStore
actually publishes/subscribes**. Then the permission allow-list
matches that exact set. Tentative shape (validate during impl):

**Bucket lifecycle authority is broker-only**. The permission allow-list
deliberately **does NOT** grant ctl or agent any `STREAM.CREATE` /
`STREAM.DELETE` / `STREAM.PURGE` / `STREAM.UPDATE` for `OBJ_*`
streams — those are the broker's prerogative. The ctl/agent only
need to (a) read stream info to confirm the bucket the broker
created exists, (b) create / drain / delete **consumers** off that
stream (for `Get`), and (c) publish/subscribe object-store data
subjects.

```go
// PermissionsForActivatedMember additions (ctl side, Tier B):
Pub.Allow += {
    "$JS.API.STREAM.INFO.OBJ_xfer-" + sid + "-*",
    "$JS.API.STREAM.MSG.GET.OBJ_xfer-" + sid + "-*",         // ObjectStore.GetInfo → GetLastMsgForSubject
    "$JS.API.CONSUMER.CREATE.OBJ_xfer-" + sid + "-*.>",
    "$JS.API.CONSUMER.MSG.NEXT.OBJ_xfer-" + sid + "-*.>",
    "$JS.API.CONSUMER.INFO.OBJ_xfer-" + sid + "-*.>",
    "$JS.API.CONSUMER.DELETE.OBJ_xfer-" + sid + "-*.>",
    "$O.xfer-" + sid + "-*.M.>",        // metadata stream subjects (Put writes)
    "$O.xfer-" + sid + "-*.C.>",        // chunk stream subjects (Put writes)

    // Caps probe — new control-plane subject, see §Wire protocol "caps":
    subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".caps.req",

    // Pull finalization — ctl signals broker after Get + SHA + rename
    // succeed (or fail). Without this, auth_callout will reject the
    // ctl's finalize publish, and broker will never write audit
    // complete or delete the bucket until 5-min timeout. The wildcard
    // `transfer.*.finalize.req` is intentional: ownership of the
    // specific transfer_id is enforced application-side by the broker
    // matching the publishing actor against the recorded creator (see
    // §Audit and broker.transfer_finalize unit test). NATS-layer
    // ACL enforces sid scoping; broker enforces transfer_id ownership.
    subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".transfer.*.finalize.req",
}
// NB: NO STREAM.CREATE / STREAM.DELETE / STREAM.PURGE / STREAM.UPDATE —
// broker owns bucket lifecycle. Verified against nats.go@v1.52.0
// ObjectStore.Put: it calls GetInfo (needs STREAM.MSG.GET) and
// publishes to $O.<bucket>.M.> / $O.<bucket>.C.> chunks. It does
// NOT call STREAM.CREATE if the bucket already exists (which broker
// will guarantee in state-machine step 2). ObjectStore.Put's error
// path may try STREAM.PURGE for partial-cleanup; that publish will
// fail under our ACL — that's intentional, and the failure is
// harmless because broker bucket-delete is the authoritative
// cleanup. Static guard test asserts this exact set.

// PermissionsForAgent additions (agent side, Tier B):
Pub.Allow += {
    // ev.node.<nid>.transfer.* is already covered by the existing
    // `ev.node.<nid>.>` wildcard in PermissionsForAgent line 76; no new
    // entry needed. Confirmed with a static guard test during impl.
    "$JS.API.STREAM.INFO.OBJ_xfer-" + sid + "-*",
    "$JS.API.STREAM.MSG.GET.OBJ_xfer-" + sid + "-*",         // same reason as ctl
    "$JS.API.CONSUMER.CREATE.OBJ_xfer-" + sid + "-*.>",
    "$JS.API.CONSUMER.MSG.NEXT.OBJ_xfer-" + sid + "-*.>",
    "$JS.API.CONSUMER.INFO.OBJ_xfer-" + sid + "-*.>",
    "$JS.API.CONSUMER.DELETE.OBJ_xfer-" + sid + "-*.>",
    "$O.xfer-" + sid + "-*.M.>",
    "$O.xfer-" + sid + "-*.C.>",
}
// Same exclusion: no STREAM.CREATE/DELETE/PURGE for agents.
```

**Mandatory tests** for this section:
- Cross-session: session-A ctl/agent complete one Tier-B push +
  pull; session-B's ctl cannot read, write, watch, or list
  session-A's bucket.
- **Same-session lifecycle authority**: a normal member of session A
  CANNOT create or delete `OBJ_xfer-A-*` streams even inside its
  own session — only the broker can. Negative test asserts NATS
  rejects `$JS.API.STREAM.CREATE.OBJ_xfer-A-evil` published by an
  activated-member JWT.

Test/p3 (auth_callout) is the right harness pattern; the new e2e
lives in `test/security/transfer_auth_test.go`.

### Audit

The broker is the single audit writer per arch §C.1 §4. Audit entries
follow the **receiver-finalization invariant**: broker writes the
`start` row from its own accepted `prepare`, and writes the `complete`
or `failed` row from a signal sent by whichever party actually receives
the bytes. This is the same pattern used by the state machines in
§Tier B push and §Tier B pull above. The agent and ctl **never**
publish `audit.*` directly; the existing nkey ACL forbids that.

| Verb | Tier | start written from | complete / failed written from |
|---|---|---|---|
| push | A | `cmd.by.<actor>.node.<nid>.push.req` accepted (broker-internal) | agent `ev.node.<nid>.transfer.<id>.complete` / `.failed` |
| push | B | `cmd.by.<actor>.node.<nid>.push.req` accepted | agent `ev.node.<nid>.transfer.<id>.complete` / `.failed` |
| pull | A | `cmd.by.<actor>.node.<nid>.pull.req` accepted | ctl `ctrl.by.<actor>.s.<sid>.transfer.<id>.finalize.req` |
| pull | B | `cmd.by.<actor>.node.<nid>.pull.req` accepted | ctl `ctrl.by.<actor>.s.<sid>.transfer.<id>.finalize.req` |

Push: the agent is the receiver of the bytes (Tier A inline data,
Tier B `Get` from bucket), so the agent emits the final `ev.transfer`
once the file is renamed into place or the operation has irrecoverably
failed; broker subscribes and writes the matching audit row. Pull: the
ctl is the receiver, so the ctl publishes `transfer.<id>.finalize.req`
after rename / failure, which broker handles via
`internal/broker/transfer_finalize.go` (sid-scoped ACL plus
transfer_id↔actor ownership check) and writes the audit row.

Timeout fallback: if the broker does not see the matching final signal
within the configured budget (60s for Tier A push, 5min for Tier B
push and Tier B pull, 30s for Tier A pull), it writes
`kind=failed code=ctl_disconnect` (pull) or `code=agent_no_responders`
(push) on its own and runs the corresponding bucket cleanup. This is
why `transfer.*.finalize.req` must be in the activated-member JWT
allow list — without it, every successful pull would be misreported as
`ctl_disconnect`.

Schema (`internal/schema/audit.go`, follows existing `AuditCall` /
`AuditProc` / `AuditPort` field naming — `session`/`node` not
`sid`/`nid`):

```go
type AuditTransfer struct {
    V         int       `json:"v"`         // schema version
    Kind      string    `json:"kind"`      // start | complete | failed
    Verb      string    `json:"verb"`      // push | pull
    Session   string    `json:"session"`
    Node      string    `json:"node"`
    ActorFP   string    `json:"actor_fp"`
    Path      string    `json:"path,omitempty"`
    Size      int64     `json:"size,omitempty"`
    SHA256    string    `json:"sha256,omitempty"`
    Tier      string    `json:"tier,omitempty"`         // a | b
    Bucket    string    `json:"bucket,omitempty"`       // tier b only
    Bytes     int64     `json:"bytes,omitempty"`        // complete only
    DurationMs int64    `json:"duration_ms,omitempty"`  // complete only
    Code      string    `json:"code,omitempty"`         // failed only
    Error     string    `json:"error,omitempty"`        // failed only
    Ts        time.Time `json:"ts"`
}
```

`tether history --kind transfer` filters to this view via the existing
`kind` switch in `cmd/tether/history.go`; subject is
`tether.v1.s.<sid>.audit.transfer` (mirrors `audit.proc` / `audit.port`).

### Object bucket lifecycle

JetStream Object Store **does not** TTL-delete the underlying stream
when objects expire — `MaxAge` only ages the chunks. The bucket
(NATS stream `OBJ_<name>`) sticks around. **The broker is the sole
owner of bucket creation and deletion**:

| Phase | Bucket state | Owner action |
|---|---|---|
| step 2 (ACL + bucket create) | bucket exists, empty | broker calls `ObjectStore.Create(...)` |
| steps 3–6 (`Put` / `Get`) | bucket fills + drains | none |
| step 7 (agent emits `transfer.complete` or `.failed`) | bucket may have objects | broker calls `ObjectStore.Delete(bucket)` synchronously after writing audit |
| step 8 (ctl gets final reply) | bucket already gone | none |
| **ctl dies after step 3** (Put incomplete) | bucket has partial object | broker times out the `push.commit` wait (60s); deletes bucket; writes audit `kind=failed code=ctl_disconnect` |
| **agent dies after step 5** (Get never starts) | bucket has full object | broker `push.commit` reply timeout (5min for tier B); deletes bucket; writes audit `kind=failed code=agent_no_responders` |
| **broker dies mid-transfer** | bucket may exist with state | **G.2 reconcile-on-boot scan**: at startup, list all `OBJ_xfer-*` streams; for each that has no in-flight transfer in memory (everything after a restart), delete it. Audit silently has no `complete`/`failed` for these — operator sees the gap by absence and reruns. |

The G.2 reconcile scan is added to `internal/broker/reconcile.go`
alongside the existing G.2 path; about 30 LOC.

### Refusing dangerous paths

`filepath.Clean("/data/../etc/passwd")` returns `/etc/passwd` — the `..`
disappears. So a "no `..` after Clean" check catches nothing useful.

**v2.0 mandatory containment**: agent.yaml must declare
`file_transfer.allow_roots` (a non-empty list of absolute paths).
**Empty / missing → file transfer is disabled** on that agent;
push/pull requests return `transfer_disabled` immediately. The
install.sh-generated default agent.yaml ships with:

```yaml
file_transfer:
  allow_roots:
    - /home/<user>                   # the agent's $HOME at install time
    - /tmp
    - /srv/local/<user>              # UIUC-style local-disk root, harmless on others
```

Validation pipeline on the agent. **Same rules apply to push (write)
and pull (read)**: symlinks rejected on both sides, containment check
both sides, regular-file check both sides. Pull is NOT exempt
("symbolic links are a non-goal" applies to both directions).

1. **Absolute path required**: `filepath.IsAbs(path)`, else `path_not_absolute`.
2. **Clean + normalize**: `clean := filepath.Clean(path)`.
3. **Resolve symlinks in the directory chain**:
   `resolved, err := filepath.EvalSymlinks(filepath.Dir(clean))`.
   - Push (write): `ENOENT` on parent dir is rejected
     (`path_parent_missing`). **Parent directories are NOT auto-created
     in v2.0**; operator runs `tether exec <nid> -- mkdir -p ...` first.
     This keeps containment + permission checks bounded to a known
     set of components. `mkdir -p` style is a v2.1 question.
   - Pull (read): `ENOENT` on parent dir is rejected (`path_not_found`).
4. **Containment**: `resolved + "/" + filepath.Base(clean)` must start
   with one of `allow_roots` + `/`. Use explicit
   `strings.HasPrefix(resolved+"/", root+"/")` (`filepath.HasPrefix`
   is broken on case-insensitive FS).
5. **No symlink at the leaf**:
   - Push: open dest tmp with `O_NOFOLLOW|O_EXCL|O_CREAT|O_WRONLY`
     (Linux; equivalent on darwin). Refusal = `not_a_regular_file`.
   - Pull: `lstat(clean)` first — if `Mode()&os.ModeSymlink != 0` →
     `not_a_regular_file`. Then `os.OpenFile(clean, O_RDONLY|O_NOFOLLOW)`
     (re-check via Stat that returns the same dev/inode to close the
     race-window between lstat and open).
6. **Type check**: `IsRegular()` (`Stat().Mode().IsRegular()`).
   Push: destination existence with a non-regular type → reject.
   Pull: source must be regular, else `not_a_regular_file`
   (catches devices / sockets / pipes / dirs).

Container model: operator declares "tether-managed roots"; anything
that, after symlink resolution of the directory chain, lives inside
a root is fair game. The leaf itself is never followed.

TOCTOU note: read-path (pull) double-checks via dev+inode comparison
between `lstat` and `Stat(fd)`; if they differ → `path_race` reject.
Write-path (push) gets `O_NOFOLLOW|O_EXCL`. Directory-component
swaps remain residual and require an attacker who already controls
something inside `allow_roots` — at which point they could write/read
the file directly.

## Files Touched

- **New**: `cmd/tether/push.go` (~150 LOC) — `tether push <local> <node>:<remote>`
- **New**: `cmd/tether/pull.go` (~150 LOC) — `tether pull <node>:<remote> <local>`
- **New**: `cmd/tether/transfer_shared.go` (~100 LOC) — arg parser for `<node>:<path>`, tier chooser, path validation pre-check
- **New**: `internal/agent/transfer.go` (~350 LOC) — handles push/pull/push-commit/pull-commit forwarded, both tiers; calls `agent/path.go` for validation
- **New**: `internal/agent/path.go` (~120 LOC) — EvalSymlinks-based containment against allow_roots
- **New**: `internal/agent/transfer_test.go` (~280 LOC)
- **New**: `internal/agent/path_test.go` (~150 LOC) — symlink traversal, allow_roots, TOCTOU window via O_NOFOLLOW
- **New**: `internal/broker/transfer.go` (~180 LOC) — bucket create + push.commit forward + ev.transfer subscriber + bucket delete + audit write
- **New**: `internal/broker/transfer_test.go` (~200 LOC)
- **New**: `internal/broker/transfer_reconcile.go` (~50 LOC) — G.2 boot scan of OBJ_xfer-* streams
- **Modified**: `internal/proto/messages.go` (+~120 LOC: PushPrepareReq/Resp, PullPrepareReq/Resp, TransferCommitReq, TransferEvent, TransferFinalize/Resp, CapsReq/Resp)
- **New**: `internal/broker/caps.go` (~60 LOC) — handle `caps.req` (membership check + return `JetStreamReady = b.js != nil` + nc.MaxPayload())
- **New**: `internal/broker/transfer_finalize.go` (~80 LOC) — handle pull's `transfer.<id>.finalize.req` (validate transfer_id + actor; write audit; delete bucket if tier B; reply ack)
- **Modified**: `internal/proto/subjects.go` (no new subject helpers needed; existing SubjCmdBy handles new verbs)
- **Modified**: `internal/auth/permissions.go` (~+30 LOC for OBJ_xfer JS subjects on **both** ActivatedMember and Agent templates; command-plane subjects unchanged — existing `node.*.*.req` wildcard covers them)
- **Modified**: `internal/schema/audit.go` (new `AuditTransfer{Session, Node, ActorFP, ...}` — uses existing `session`/`node` field names)
- **Modified**: `internal/broker/broker.go` (~+50 LOC: subscribe to new `push.req` / `pull.req` / `push-commit.req` (push only; pull does NOT use commit per Round-3 finalize redesign) + `ctrl.by.*.s.*.caps.req` + `ctrl.by.*.s.*.transfer.*.finalize.req`; subscribe to `s.*.ev.node.*.transfer.>` for push receiver-side audit + cleanup)
- **Modified**: `internal/broker/reconcile.go` (~+15 LOC: call new `transferReconcile()` from G.2 boot path)
- **Modified**: `internal/agent/agent.go` (~+10 LOC: dispatch new verbs in the forward switch; pass agent.cfg.AllowRoots through)
- **Modified**: `internal/agent/config.go` (`AllowRoots []string` field from agent.yaml)
- **Modified**: `cmd/tether/agent.go` (load `file_transfer.allow_roots` from yaml)
- **Modified**: `cmd/tether/history.go` (+`transfer` kind in the kind filter switch + printer for AuditTransfer)
- **Modified**: `internal/serveconf/serveconf.go` (+`broker.nats.max_payload` default 16 MiB; only used by install.sh template, not enforced at broker runtime since max_payload is a NATS server config)
- **Modified**: `scripts/install.sh` (broker nats.conf template — `max_payload: 16777216`; agent.yaml template — adds `file_transfer.allow_roots: [$HOME, /tmp]`)
- **Modified**: `docs/architecture.md` §F (new §F.9 file transfer)
- **Modified**: `docs/usage.md` (new §3.5 push/pull + §4 速查表 row)
- **New**: `test/cli_e2e/transfer_test.go` (~250 LOC) — non-JS anonymous-NATS coverage of: tier-A push/pull, SHA mismatch via fake corruption, dst_exists, --force, path validation, version_skew on old agent
- **New**: `test/cli_e2e/transfer_js_test.go` (~200 LOC) — JS-enabled (testharness.StartJSNATS) coverage of: tier-B push/pull both directions, bucket created+deleted, 50 MiB random file SHA roundtrip, broker crash mid-transfer (G.2 reconcile cleans bucket)
- **New**: `test/security/transfer_auth_test.go` (~250 LOC) — auth_callout + JS enabled; session-A tier-B works; session-B cannot read/write/watch/delete session-A's bucket; static guard test for permission template shape. Lives in `test/security/` alongside existing security e2e (not `test/p3/`, which is the auth_callout phase-validation suite — file transfer is a v2 feature, not a P3 regression).

**Total**: ~13 new files + ~12 modified files. ~2000 new LOC + ~200 modified.

## Verification

### Unit tests (per-package)

- `proto`: PushPrepareReq / PushPrepareResp / PullPrepareReq / PullPrepareResp / TransferCommitReq / TransferEvent / TransferFinalize / TransferFinalizeResp / CapsReq / CapsResp round-trip; tier-A InlineData preserved through JSON+base64.
- `broker`: caps handler returns `JetStreamReady=true` when `b.js != nil`, `false` when JS disabled; non-member of <sid> denied via existing membership check.
- `broker.transfer_finalize`: validates transfer_id ↔ actor binding (only the original ctl can finalize its own transfer); rejects forged transfer_id; writes correct AuditTransfer; deletes bucket idempotently (a duplicate finalize.req is safe).
- `auth`: new OBJ_xfer subjects scoped per-sid; member of session A can NOT pub to session B's bucket subjects; **member CANNOT** publish `$JS.API.STREAM.CREATE.OBJ_xfer-A-*` even inside its own session (lifecycle authority test).
- `agent.transfer`: tier-A small file end-to-end with stub NATS; SHA verify path; rename-on-match; tmp cleanup on failure; symlink rejection; path-traversal rejection.
- `broker.transfer`: bucket creation with right TTL; ACL gate; bucket deletion on success + failure.

### Cobra-level / CLI tests

- `tether push --help` / `tether pull --help` surface flags + ACTIVE help.
- `<node>:<path>` parser: rejects `:` in local path, accepts `:` only in the remote arg.

### Broker-backed e2e — split by required infra

The completion test harness is anonymous NATS + non-JS. That works
for tier A and command-plane assertions, but tier B + auth_callout
need richer harnesses. Three test files:

**`test/cli_e2e/transfer_test.go` — anonymous NATS, non-JS, tier A only**:

1. Tier-A push 1 KiB file → byte-for-byte match on agent side, audit start+complete.
2. Tier-A pull same.
3. Oversized rejection: 250 MiB → `too_large` immediately, no NATS request issued.
4. `dst_exists` without `--force` → fail; with `--force` → overwrite.
5. SHA mismatch via fake `[]byte` corruption injection on agent side: `sha_mismatch` + tmp cleaned + audit `failed`.
6. `allow_roots` violation (push to `/etc/passwd` when allow_roots = `["/tmp"]`) → `path_outside_roots`.
7. Symlink at leaf → `not_a_regular_file`.
8. `file_transfer` disabled (empty allow_roots) → `transfer_disabled`.
9. `version_skew`: agent reports RELEASE 0.1.4 → ctl refuses with hint.

**`test/cli_e2e/transfer_js_test.go` — JS-enabled (`testharness.StartJSNATS`), tier B, anonymous NATS**:

10. Tier-B push of 50 MiB random file → SHA + content match + bucket exists during step 3, gone after step 8 (broker proxies agent's ev.transfer.<id>.complete back on push-commit reply inbox).
11. Tier-B pull of 50 MiB random file → bucket exists during step 5, gone after step 6 (ctl's transfer.<id>.finalize.req triggers broker delete + audit).
12. **Pull ctl crash after step 5**: start tier-B pull, kill ctl after `Get` but before `finalize.req` → broker hits 5-min finalize timeout, audit `kind=failed code=ctl_disconnect`, bucket deleted.
13. **Push agent crash mid-Get** (tier B): start tier-B push, ctl Put completes, kill agent before agent emits ev.transfer → broker push-commit reply timeout, audit `kind=failed code=agent_no_responders`, bucket deleted.
14. **Broker crash after step 2** (bucket created): start a transfer, kill broker, restart broker → G.2 boot reconcile lists `OBJ_xfer-*` streams + deletes orphans; audit silently has no `complete` for that bucket.
15. Tier-B push when JS context is nil: simulated by overriding `b.js`; expect `jetstream_unavailable` + caps probe pre-flight returns `JetStreamReady=false`.
16. **Caps probe** end-to-end: `caps.req` returns `OK + JetStreamReady + MaxPayload`; non-member receives auth_violation (covered in security file).
17. **Finalize transfer_id forgery**: ctl-A starts a transfer, ctl-B (different actor) tries to `transfer.<A's-id>.finalize.req` → broker validates `actor` against the recorded creator, rejects with `not_owner_or_creator`, audit unchanged.
18. **Duplicate finalize**: ctl sends finalize twice → second one returns OK no-op (idempotent), audit not written twice.
19. 100 MiB tier-B perf smoke: < 30s on localhost; skipped with `-short`.

**`test/security/transfer_auth_test.go` — full auth_callout + JS**:

15. Session-A ctl can complete a tier-B push to session-A agent.
16. Session-A ctl tier-B push to session-B nid → broker callout denies on `cmd.by.<A>.node.<B-nid>.push.req` ACL (existing membership check).
17. Session-B ctl tries to `ObjectStore.Get(OBJ_xfer-A-...)` → NATS rejects at publish time (perm violation on `$O.xfer-A-*.M.>`).
18. Session-B ctl tries to delete session-A bucket via JS API → NATS rejects.
20. Auth-callout JWT permission template static guard (extend existing test in `internal/authcallout/handler_test.go`): asserts `Pub.Allow` for an activated member of sid="lab" contains exactly the expected `$JS.API.{STREAM.INFO,STREAM.MSG.GET,CONSUMER.*}.OBJ_xfer-lab-*` + `$O.xfer-lab-*.{M,C}.>` + `caps.req` set, AND **does not** contain `STREAM.CREATE/DELETE/PURGE/UPDATE` (negative assertion for Round-3 #1+#3 fix).
21. **Real ObjectStore.Put + Get under auth_callout**: a session-A ctl uses real `nats.go` ObjectStore.Put against a broker-pre-created bucket; assertion is "Put completes without permission error" — proves the new `STREAM.MSG.GET` allow is sufficient (Round-3 #3 regression test).
22. **Caps probe under auth_callout**: session-A member receives correct `JetStreamReady`+`MaxPayload`; session-B non-member's request denied at NATS layer.
23. **Put failure → broker bucket-delete despite STREAM.PURGE denial** (Round-4 #3): start a real `ObjectStore.Put` under auth_callout, then cancel the upload mid-stream (close ctx). `nats.go` will attempt `$JS.API.STREAM.PURGE.OBJ_xfer-...` for partial-object cleanup — that publish must be denied at NATS layer (assert via the static guard in #20 already, plus a runtime assertion that the Put returns the original cancel error and not the PURGE permission-violation as a misleading second error). Then trigger broker's failure path (push-commit reply timeout or explicit failed `transfer.<id>.finalize.req` for the pull case): the bucket MUST be gone afterward (`stream.Info` returns "stream not found") even though the client-side purge was denied. This pins the documented behavior in §Object bucket lifecycle that broker bucket-delete is the authoritative cleanup.
24. **Finalize cross-session NATS-layer denial** (Round-4 #1 negative): session-B activated member tries to publish `tether.v1.ctrl.by.<B>.s.<A>.transfer.<id>.finalize.req` against session-A (a sid the publisher is not a member of). The publish must be denied at the NATS callout layer — the JWT's `ctrl.by.<B>.s.<sid>.transfer.*.finalize.req` allow is sid-scoped to sids the actor is a member of, NOT a free wildcard. Asserts the perm is bound to the same sid the activated-member JWT was minted for, complementing #17's same-sid different-actor application-layer check.

### Unit tests (per-package)

- `internal/agent/path_test.go`: 6 cases listed in "Path validation" section. Includes a `t.TempDir()`-based symlink-at-leaf + symlink-at-dir-component + `..`-escape + non-absolute + TOCTOU-best-effort.
- `internal/proto`: all 9 new types (PushPrepareReq/Resp, PullPrepareReq/Resp, TransferCommitReq, TransferEvent, TransferFinalize/Resp, CapsReq/Resp) round-trip; tier-A InlineData survives base64 + JSON correctly.
- `internal/auth`: new permission shape via static guard (the existing pattern from completion's #1 fix).
- `internal/broker/transfer_test.go`: bucket create with right config, 60s ctl-disconnect timeout, ev.transfer subscriber writes audit + deletes bucket.

### Static guard / negative tests

- Permission template MUST NOT grant agents access to `audit.*` (audit single-writer invariant).
- Permission template MUST scope `OBJ_xfer-*` to the caller's sid (no cross-session).
- `tether push --help` shows `<local-path> <node>:<remote-path>` (not reversed).

## Risk

- **NATS max_payload bump from default 1 MB → 16 MiB**: server-side
  config (`max_payload` in nats.conf), NOT a client option — there's
  no agent or ctl flag that can raise the server's cap. ctl reads
  the live `nc.MaxPayload()` and falls back to tier B if a tier-A
  candidate exceeds it; operators on un-bumped brokers still get a
  working transfer (just over JS instead of inline). install.sh
  template sets `max_payload: 16777216` for new installs; existing
  brokers need `sudo sed` + restart, documented in §usage.md
  upgrade notes.
- **JetStream disk pressure**: a 200 MiB bucket × N concurrent
  transfers on a small broker disk = potential OOM-disk.
  **Mitigation**: broker global cap `file_transfer.max_concurrent`
  in broker.yaml (default 4); 5th transfer gets `too_many_in_flight`.
  Existing `disk_pressure` system event also fires.
- **Audit-stream pollution**: 3 events per transfer × frequent
  workflow = audit stream grows fast. Existing `history-<sid>`
  stream config (`internal/jsstream/jsstream.go:101`) is **1 GiB
  per-session cap with DiscardNew, no age expiry**. **DiscardNew
  means a full history-<sid> rejects new audit messages** rather
  than evicting old ones — so heavy `tether push` use could cause
  later `audit.transfer` writes to fail with "max msgs/bytes
  exceeded" while old proc/port history sticks. Existing behavior
  also affects `audit.proc` and `audit.port`; v2.0 adds transfer
  to the same shared budget. Operators tight on quota should bump
  `history-<sid>.MaxBytes` or rotate sessions. The global `events`
  stream has the 30-day age limit, but file transfer doesn't write
  there.
- **Path validation correctness**: classic CVE territory.
  `filepath.Clean` does NOT strip `..` if it would escape the
  semantic root; only the leading-`..` case at the path root gets
  collapsed. The full validation pipeline (allow_roots required,
  EvalSymlinks, `O_NOFOLLOW`) is documented above; tested in
  `internal/agent/path_test.go` with at least the following cases:
  symlink at leaf, symlink at directory component, `..` between
  allow_root segments, TOCTOU swap window (best-effort —
  acknowledges open-rename race is residual).
- **Wire compat**: ctl on v0.2.0 talking to broker/agent on v0.1.x
  → `no_responders`. ctl pre-checks the target agent's `RELEASE`
  field from `tether node ls` cache and surfaces `version_skew` with
  a clear "run `tether node upgrade <nid>` first" hint **before**
  attempting the NATS request.
- **JS unavailability**: a broker started with JS disabled (or
  before its JS context is ready) cannot serve tier B at all, but
  must still serve tier A. Boot ordering: `push.req` / `pull.req`
  handlers are registered **unconditionally** alongside `exec.req`
  etc. The handler, **at request time**, checks `b.js != nil`:
  - Tier-A request: proceed (no JS dependency in the data path).
  - Tier-B request: return `PrepareResp{Code:"jetstream_unavailable"}`
    and audit nothing.
- **ctl-side capability probe**: before deciding tier, the ctl
  issues a one-shot capability request on
  `tether.v1.ctrl.by.<actor>.s.<sid>.caps.req` (new subject,
  `proto.CapsReq` empty body; broker replies with
  `proto.CapsResp{JetStreamReady: bool, MaxPayload: int}`). This
  takes a single sub-second round-trip and is cached for the life
  of the `tether` invocation. If JS=false and the file doesn't fit
  in `MaxPayload`, ctl returns `payload_too_small` locally without
  attempting the transfer. The caps subject is added to existing
  `PermissionsForActivatedMember.Pub.Allow` (1 new entry).

## Rollout

1. Implement on feature branch `feat/file-transfer`.
2. PR review focusing on: (a) path validation + EvalSymlinks
   correctness, (b) bucket lifecycle including G.2 reconcile, (c) SHA
   verification placement (must be at receiver, never sender), (d) the
   auth_callout permission set proven against real `nats.go` ObjectStore.
3. Merge to main → tag `v0.2.0` → goreleaser.
4. **Proto stays v1** — new subjects don't break old ones. But:
5. **Broker MUST upgrade to v0.2.0 for tier A as well.** The current
   broker only subscribes to explicit verb subjects (`exec`/`run`/
   `kill`/`expose`/`expose-rm`/`upgrade`); it has no generic
   `cmd.*.req` router. New `push.req` / `pull.req` / `push-commit.req`
   handlers must be added (~50 LOC in `internal/broker/broker.go`
   wiring + `internal/broker/transfer.go`). Tier A is **not** a
   freebie on v0.1.x brokers — drop the prior plan's claim.
6. **Agent MUST upgrade to v0.2.0**: agent needs the new
   `handleTransferForwarded` dispatch and the new JS API permissions
   in its JWT. (The JWT template change is broker-side, but agents on
   v0.1.x can't do Object Store either — they have zero JS perms
   today.)
7. **Forced fleet upgrade for the feature**: ctl + broker + every
   target agent must be ≥ v0.2.0. Release notes call this out and
   `tether push/pull` returns `version_skew` if the target agent's
   RELEASE field (from `tether node ls`) is < 0.2.0, with a hint to
   `tether node upgrade <nid>`.

## Out of Plan (deferred)

- **Directory transfers** — v2.1
- **`--resume` user-visible flag** — v2.1
- **gzip / zstd compression on the wire** — v2.1
- **Wildcard `allow_roots` (i.e. `/`)** — explicitly NOT supported in
  v2.0. The agent.yaml schema validates that no `allow_root` is `/`
  (would defeat containment). Operators wanting whole-disk transfer
  fall back to expose+rsync, by design. (`allow_roots` itself is in
  v2.0; the deferred bit is allowing the root path.)
- **Bandwidth limiting** — likely never; NATS rate limits + tier cap are enough
- **Multi-file batch** (`tether push a100 file1 file2 file3 /remote/dir/`) — v2.1
- **Symbolic-link follow** (`--dereference`) — design discussion needed; security-sensitive
- **GB-scale transfers** — out of scope; expose + rsync remains
- **Pull-resume after broker restart (G.2 file-transfer reconcile)** — drop on restart; tier-A retries automatically (sub-second); tier-B requires manual retry, audit shows where it failed

## Reviewer Notes - 2026-05-12

Scope: reviewed this plan against the current broker/agent/auth/history
implementation. I did not review code for an implementation because this is
still a plan review.

Conclusion: do not approve as-is. The feature direction is useful, but the
current plan has unresolved protocol, authorization, audit, and path-safety
gaps. Those are design-level gaps, not implementation details, and they should
be closed before writing the 1500 LOC implementation.

### High Concerns

1. `push` CLI argument order is internally inconsistent.

疑虑: Goals and CLI surface say `tether push <node>:<remote-path> <local-path>`
means ctl -> agent. That is the opposite of scp-style source/destination order,
and the same shape as the proposed `pull` command.

原因: The plan explicitly says the path format mirrors `scp`, but scp reads
`source destination`. If the first operand is remote and the second is local,
operators will read this as remote -> local, not local -> remote. This can lead
to a shipped CLI that overwrites the wrong side or needs a breaking CLI change
right after release.

建议: Make `push` source-first: `tether push <local-path> <node>:<remote-path>`.
Keep `pull <node>:<remote-path> <local-path>`. Add parser tests that assert the
remote side is destination for push and source for pull.

2. Tier-B Object Store protocol is not closed enough to implement safely.

疑虑: The plan mixes request/reply forwarding with bucket creation, ctl `Put`,
agent `Get`, and a `push.done` / `Watch()` follow-up that is still TBD.

原因: In the current command model, the broker forwards with the original reply
inbox and then leaves the data path. For Tier B, the broker must create the
bucket before either side uses it, the ctl needs a definite bucket/object name
before `Put`, the agent needs a definite signal for when to `Get`, and the
broker needs a definite status signal for cleanup and audit. The plan does not
define the state machine or `PullResp` shape for those phases.

建议: Specify Tier B as an explicit state machine before implementation. For
example: `prepare` (broker ACL + bucket/object lease) -> upload/produce object
-> `commit` forwarded to receiver -> receiver verifies/renames -> broker
records completion and deletes bucket. Define all request/response structs,
timeouts, retry behavior, and which actor owns cleanup at each phase. Do not
leave `push.done` vs `Watch()` as an implementation-time decision.

3. The auth_callout permission plan is both redundant and incomplete.

疑虑: The command subjects proposed for push/pull are already covered by the
current activated-member wildcard `s.<sid>.cmd.by.<actor>.node.*.*.req`, while
the JetStream/Object Store permissions are guessed and not tied to real
`nats.go` behavior.

原因: Object Store uses normal JS API subjects plus `$O.<bucket>.M.>` and
`$O.<bucket>.C.>` stream subjects, and the exact publish/subscribe/API needs
differ for producer vs consumer and for `Put`, `Get`, `Watch`, delete, and
stream lookup. Agents currently have no JS API permissions at all and are
explicitly documented as having no audit access. A static allow-list test that
only checks string shape will not prove Tier B works under auth_callout.

建议: Derive the final permission set from real `nats.go` Object Store calls
under auth_callout. Add an e2e with real auth_callout + JetStream where a ctl
and an agent can complete one Tier-B push and pull, and a second session cannot
read, write, watch, delete, or list the first session's `xfer-<sid>-*` bucket.

4. Path validation is not safe as described.

疑虑: "After `filepath.Clean`, must not contain `..` traversal" does not catch
the plan's own traversal example.

原因: `filepath.Clean("/data/../etc/passwd")` becomes `/etc/passwd`; the `..`
segment is gone before the check. With empty `allow_roots = anywhere`, there is
no containment boundary, so traversal is not meaningfully defined. Also,
rejecting only the final path with `lstat` misses symlink components in parent
directories and leaves a TOCTOU window before open/rename.

建议: Decide the security model now. Either require `allow_roots` in v2.0 and
validate `EvalSymlinks(clean(path))` stays under an allowed root, or explicitly
state that any absolute path is intentionally allowed and remove the misleading
traversal claim/test. For writes, use a temp file created in the destination
directory with exclusive/no-follow behavior where supported, reject symlink
components, and avoid following a symlink on the final target during overwrite.

5. Rollout statement that a v0.1 broker can support Tier A is wrong.

疑虑: The rollout says the broker can stay on v0.1.x for Tier A.

原因: The current broker subscribes to explicit command verbs (`exec`, `run`,
`kill`, `expose`, `expose-rm`, `upgrade`), not a generic `cmd.*` router. A
v0.1 broker will not subscribe to `push.req` or `pull.req`, so ctl will get
`no responders` even for inline Tier A.

建议: Require broker v0.2.0 for both Tier A and Tier B. The release note should
say ctl, broker, and target agent must all be upgraded for file transfer.

### Medium Concerns

6. Tier-A 10 MB cutoff does not fit a 12 MB JSON/NATS payload.

疑虑: A 10 MiB `[]byte` in JSON becomes base64 of about 13.3 MiB before JSON
object overhead.

原因: NATS `max_payload` applies to the serialized message bytes. A 12 MB cap
will reject many files below the proposed 10 MB cutoff. Also, increasing
`max_payload` is a server-side NATS config change; there is no agent connect
option that can raise the server's payload limit.

建议: Either lower Tier A to a size that fits the configured payload after
base64 overhead, or raise server `max_payload` with clear decimal/MiB math and
tests at the boundary. The ctl should check `nc.MaxPayload()` and choose Tier B
or return a precise config error instead of assuming the broker template is in
effect.

7. Audit ownership conflicts with the single-writer architecture.

疑虑: The lifecycle table says "agent emits `transfer.complete`" and the audit
section says every transfer emits `audit.transfer`.

原因: Current permissions and architecture intentionally make tetherd the
single writer for `audit.*`; agents publish runtime facts under `ev.node.*`,
and the broker transcribes those to audit. If the broker forwards the reply
directly to ctl like `exec`, the broker will not reliably observe receiver
success/failure for `complete` / `failed`.

建议: Keep audit single-writer. Have the agent emit a transfer status event
under an `ev.node.<nid>.transfer...` subject, then have the broker validate it,
write `audit.transfer`, and delete the bucket. Alternatively, have the broker
proxy final status replies. Pick one explicit path and test that agents cannot
publish `audit.transfer` directly.

8. Object bucket lifecycle assumes TTL deletes buckets, which needs proof.

疑虑: The plan says TTL evicts orphaned buckets after 10 minutes.

原因: JetStream/Object Store TTL-style settings typically expire objects or
messages; they do not necessarily delete the bucket/stream itself. Per-transfer
bucket names can therefore leave many empty `OBJ_xfer-*` streams after crashes,
even if object data ages out.

建议: Add broker-owned cleanup: delete bucket on success/failure, scan
`OBJ_xfer-*` on boot, and delete expired transfer buckets by name/metadata.
Tests should assert the stream/bucket itself is gone, not only that objects are
unreadable.

9. JetStream availability and startup ordering are under-specified.

疑虑: Current broker startup closes readiness after subscriptions are installed
and only then probes JetStream. Audit can fall back to core NATS when JS is
missing; Tier B cannot.

原因: A file transfer request can arrive while `b.js` is still nil, or on a
deployment with JetStream disabled. The plan does not define whether Tier B
blocks, retries, or returns an explicit error.

建议: For transfer handlers, make JS readiness explicit. Either initialize JS
before installing push/pull handlers, or have Tier B return
`jetstream_unavailable` until `b.js` is ready. Add e2e coverage for JS-disabled
brokers: Tier A works if payload size fits, Tier B fails immediately with a
clear message.

10. Default overwrite semantics are not implementable as written.

疑虑: "`--force` overrides refuse to overwrite a non-tether-managed file" has
no defined way to tell whether an existing destination is tether-managed.

原因: After a successful temp-file `rename`, the final path does not retain
`.tether.tmp.*` in its name. Without a sidecar marker, xattr, database row, or
other metadata, the receiver cannot distinguish a previous tether-written file
from an operator-created file at the same path.

建议: Simplify v2.0 semantics to "default: fail if destination exists;
`--force`: overwrite existing regular file", or define a real marker mechanism
and its cleanup/compat story. The simpler rule is easier to reason about and
matches the stated single-file scope.

11. Verification plan does not exercise the risky boundaries.

疑虑: The broker-backed e2e section says to reuse the anonymous
`completion_test.go`-style harness.

原因: That harness does not exercise auth_callout permission enforcement and
its default NATS helper is non-JetStream. The highest-risk parts of this plan
are exactly JS Object Store permissions, auth isolation, and bucket cleanup
under JS.

建议: Split tests into: Tier-A anonymous/control-plane tests, Tier-B JS tests
using a JS-enabled harness, and auth_callout + JS tests for permissions and
cross-session isolation. Keep the proposed fake corruption test, but add at
least one real broker + real Object Store transfer in each direction.

12. Audit retention and schema examples drift from current code.

疑虑: The risk section says history retention is "30 days / 1 GB", and the
audit examples use `sid` / `nid`.

原因: Current `history-<sid>` streams have no age expiry and a 1 GiB
per-session cap with `DiscardNew`; the 30-day limit belongs to the global
`events` stream. Existing audit schema fields use `session` and `node`, and
history printing reads those names for call/proc/port.

建议: Align `AuditTransfer` with existing schema names unless there is a
deliberate schema break, and fix the retention statement. Add a history output
test that proves `history --kind transfer` prints useful fields instead of the
raw fallback line.

### Open Questions

1. Is remote file access intended to be "any absolute path by default"? If yes,
the plan should stop framing `/data/../etc/passwd` as traversal. If no,
`allow_roots` should not be deferred.

2. Who is allowed to create and delete Object Store buckets: broker only, or
ctl/agent too? The permission model and audit story depend on this answer.

3. Should file transfer be available without JetStream at all? If yes, define
the exact Tier-A size boundary based on actual `MaxPayload`; if no, require JS
up front and simplify the code path.

---

## Author Response to Reviewer Notes — 2026-05-12

所有 12 条意见逐条 review。**10 条全数采纳并已改正文**；**2 条部分采纳**（#3 拆成两部分、#9 通过显式 boot ordering 解决）。无驳回项。完成自审后再提交。

### High Concerns

**#1 CLI 顺序** — **ACCEPTED**. scp 是 source→destination；旧版的 `push <remote> <local>` 跟 pull 同形又跟 scp 反，确实容易让 operator 把 push 当 pull 用。改：
- `tether push <local-path> <node>:<remote-path>` (source 在前)
- `tether pull <node>:<remote-path> <local-path>` (source 在前)
- Goals #1/#2 + CLI surface 两处全修。

**#2 Tier B 状态机不闭合** — **ACCEPTED**. 旧版的 "push.done vs Watch() TBD during impl" 是设计偷懒。新版加了完整状态机图 + 8-phase 责任表（who creates the bucket / who Puts / who emits commit / who writes audit / who deletes bucket），所有 reqs/replies 类型在 §Wire protocol 列出，每个失败路径写明 cleanup owner。

**#3 Permissions 冗余且不完整** — **PARTIALLY ACCEPTED, split**.
- 冗余部分 reviewer 对：现有 `s.<sid>.cmd.by.<actor>.node.*.*.req` wildcard 已经覆盖 push.req/pull.req/push-commit.req。旧版加 explicit push/pull 是噪声，删了。
- 不完整部分也对：JS Object Store 的 `$O.<bucket>.M.>` / `$O.<bucket>.C.>` 加在 JS API subjects 旁边；agent 也需要这套 perms（之前 plan 只提了 ctl）。
- 但 reviewer 要求"Derive the final permission set from real nats.go behavior"——我接受**实现时**验证而不是 plan 时枚举。原因：nats.go ObjectStore 内部使用的 subject 集合随版本变化，把"理论 subject 列表"硬编码到 plan 不如让 impl 期跑一个 fake NATS 抓 actual subject。我加了 mandatory test：static guard 测试 + cross-session e2e 隔离验证。

**#4 Path validation 不安全** — **ACCEPTED, 严格化**.
- reviewer 给的具体反例 `filepath.Clean("/data/../etc/passwd") = "/etc/passwd"` 是对的，旧版 "no .. after Clean" 完全无效。
- 新版：v2.0 **强制** `allow_roots`(非空必填，空 = transfer disabled)，用 `filepath.EvalSymlinks(filepath.Dir(clean))` 解析所有 dir 链上的 symlink，再用 `strings.HasPrefix` 验证仍在 root 内，leaf 写入用 `O_NOFOLLOW`(Linux) 防 symlink-swap。
- TOCTOU 窗口缩到 open-after-EvalSymlinks 区间；directory-component swap 已经超出 tether 的威胁模型(攻击者已能 control allow_root 内某处)。

**#5 v0.1 broker 不能支持 Tier A** — **ACCEPTED**.
- reviewer 对：broker 是 verb-specific 订阅(`exec.req`/`run.req`/`expose.req`/...)，没有 generic `cmd.*.req` router。新 verb 必须在 broker 端加 handler。
- 改 §Rollout：**broker + ctl + 目标 agent 都要 v0.2.0**。release notes 明确写。ctl 端 pre-check `node ls` cached RELEASE，agent < 0.2.0 直接报 `version_skew` + hint，**不**真发 NATS 请求(免得让 operator 看 NATS no_responders)。

### Medium Concerns

**#6 10 MB cutoff 不匹配 12 MB max_payload** — **ACCEPTED**.
- 计算 base64: 10 MiB raw → ceil(10485760/3)*4 = 13981008 bytes ≈ 13.33 MiB > 12 MiB cap。
- 改：max_payload 升 **16 MiB**(`16777216`，install.sh template)；Tier A raw cap 降 **8 MiB**(→ base64 10.67 MiB + 元数据余量装 16 MiB 充裕)。
- 关键澄清：max_payload 是 **server config**，没有 client-side 提升路径——这条 reviewer 提的是 plan 表述问题，已删除原 "agent's NATS connect option" 那句。
- ctl 用 `nc.MaxPayload()` 在 runtime 读 server 实际值并 auto-fall-back 到 Tier B(JS 可用时)，避免 `max_payload exceeded` 直接漏给 operator。

**#7 Audit 写入违反 single-writer** — **ACCEPTED**.
- reviewer 对：旧版 plan 说"agent emits transfer.complete; broker writes audit" 但又含糊带过 audit 的 publish 是谁——破坏了 §C.1 §4 broker-is-sole-audit-writer 原则。
- 改：agent 只 publish `ev.node.<nid>.transfer.complete/failed`(已被 `PermissionsForAgent.Pub` 的 `ev.node.<nid>.>` wildcard 覆盖，无需新加 perm)；broker 订阅 `s.<sid>.ev.node.*.transfer.>`，验证后 publish `audit.transfer` + 删 bucket。
- 加 negative test: "agent must not be able to publish audit.* directly" 列在 §Static guard / negative tests。

**#8 Bucket TTL 不会自动删 stream** — **ACCEPTED**.
- reviewer 对：JetStream `MaxAge` 只过期 messages/objects，不删 stream。`OBJ_xfer-*` 一堆空 stream 后果严重。
- 改：broker is sole owner of `ObjectStore.Create` AND `ObjectStore.Delete`(列在 §Object bucket lifecycle 表格)。每个完成/失败路径同步删 bucket；broker 重启时 G.2 reconcile 扫所有 `OBJ_xfer-*` stream 并删(in-memory 状态丢了，没法续传)。
- 新文件 `internal/broker/transfer_reconcile.go` ~50 LOC 加在 Files Touched。

**#9 JS readiness 未指定** — **ACCEPTED, via boot ordering**.
- reviewer 对：当前 broker 启动时 subscriptions 装好后才 probe JS。
- 改：transfer handler 注册顺序在 `b.js` 确认 ready 之后；如果 broker 配置禁用 JS(部分 dev 部署)，tier-B handler 直接不注册——这种情况下 `cmd.by.*.node.*.push.req` 仍由 base handler 处理，Tier-B request 在 `chooseTier` 端 ctl 看到 `b.js` 不可用反映为 reply `jetstream_unavailable`(从一个轻量 capability subject 拿)。Tier A 继续工作。

**#10 默认 overwrite 语义不可实现** — **ACCEPTED, 简化**.
- reviewer 对："non-tether-managed file" 没法事后区分(rename 完没 marker)。
- 改 §CLI surface 第二条 bullet：default 拒覆盖(`dst_exists`)，`--force` 覆盖任意 regular file。tmp+rename 仍在使用，但只是实现细节(atomicity)，不再当 UX 契约。`<dst>.tether.tmp.*` "marker" 那段误导词删了。

**#11 Verification 不测高风险边界** — **ACCEPTED**.
- reviewer 对：completion 的 anonymous-NATS harness 不跑 auth_callout，也默认非 JS。文件传输高风险点正是 JS Object Store perms 和 auth 隔离。
- 改：测试拆三个文件——`transfer_test.go` (anon, tier A), `transfer_js_test.go` (JS-enabled, tier B), `test/security/transfer_auth_test.go` (auth_callout + JS, cross-session isolation)。 §Verification 全章重写。

**#12 Audit retention 和 schema 字段错** — **ACCEPTED**.
- schema 字段：改成 `Session` / `Node` 匹配现有 `AuditCall` / `AuditProc` / `AuditPort`。
- retention 数字：原 plan 写 "30 days / 1 GB"，但 history-<sid> 实际是 1 GiB cap + DiscardOld + 无 age expiry；30 天属于全局 `events` 流。改了。
- 加 history 输出 test：`tether history --kind transfer` 走 printer 正确路径而非 raw fallback。

### Open Questions answered

**Q1 "any absolute path by default"?** — **NO**. v2.0 强制 `allow_roots`(非空)。空 → transfer disabled。`/data/../etc/passwd` 这例子在 plan 里换成具体场景：allow_roots=`["/tmp"]` 时 push 到 `/etc/passwd` → `path_outside_roots`。

**Q2 谁创/删 bucket?** — **broker only**。ctl 只 Put 进 broker 已建的 bucket；agent 只 Get；删 bucket 是 broker(transfer-complete handler) 或 boot reconcile。permission template 不给 ctl/agent stream.create / stream.delete 权限——只给 consumer.create + object 读写。

**Q3 没 JS 时还能用吗?** — **Tier A 可以**，前提 server max_payload 装得下；Tier B 不行(显式 `jetstream_unavailable`)。NewCompletionContext-style probe 时 ctl 知道；`nc.MaxPayload()` 是 size 判定的真值来源。

### Self-review pass

写完 Author Response 后逐节复读，发现并修了 3 处旧版 plan 残留的内部不一致：

- Goals #3 "full overwrite semantics" 跟 §CLI surface 的 default 拒覆盖矛盾 → 改成 "atomic destination replace via rename"。
- Goals #7 提 "after compression" 但 Non-Goals 明说不做压缩 → 这段在重写 Goals #7 时已清掉。
- Non-Goals #6 "Atomic destination rename" 列在 non-goals 让人误读成"不做 atomic" → 移出，atomic rename 是 goal 的实现细节，写在 Goals #3。
- Non-Goals #5 "Symbolic links" 写成 non-goal 但内容讲 "reject" → 这是有功能；改成 "Symbolic-link transfers (--dereference): not in v2; symlink at leaf rejected with `not_a_regular_file`"。
- §Files Touched 误写 `test/p3_extra/`，实际项目无此目录 → 改成 `test/security/`(已有 dir)，加注解说为何不放 `test/p3/`。
- §Tier A inline transfer 旧版完整段落仍写"< 10 MB"、"12 MB max_payload bump"、"agent's NATS connect option"——这三处都跟修订后 Goals/Risk 不一致(已升 8 MiB / 16 MiB；max_payload 没 client option)。重写整节。这是我**第二轮自审**才发现的——头一轮只看 Goals/Non-Goals 没扫到正文中段，确实疏忽。下次自审用 grep 找关键数字一致性(`grep -n 'MB\|MiB\|10\|12\|16' plan.md`)而不是只读一遍。

### Status

12 项 concerns 全数解决 + 3 项自审残留修正。**等下一轮 plan review**。

---

## Reviewer Notes Round 2 - 2026-05-12

Scope: re-reviewed the revised plan and Author Response against the current
broker/agent/auth/history implementation. This is still a plan review; no
implementation code was reviewed.

Conclusion: the plan is much closer, and most first-round concerns are now
addressed in the main body. I still would not approve implementation yet. The
remaining issues are narrower but important: bucket ownership contradicts the
proposed JWT permissions, Tier-B pull is still not a fully specified protocol,
and the JS availability story currently contradicts the "Tier A works without
JetStream" goal.

### Blocking Concerns

1. Bucket ownership and permissions still contradict each other.

疑虑: The lifecycle section says the broker is the sole owner of bucket
creation/deletion, and the Author Response explicitly says ctl/agent should not
receive stream create/delete permissions. But the permission sketch still grants
activated members `$JS.API.STREAM.CREATE.OBJ_xfer-...` and
`$JS.API.STREAM.DELETE.OBJ_xfer-...`.

原因: If a session member can create/delete `OBJ_xfer-<sid>-*` streams, the
"broker owns bucket lifecycle" invariant is false. A member could create
conflicting buckets, delete another in-flight transfer in the same session, or
make cleanup/audit reasoning depend on client behavior. This also makes the
cross-session negative tests insufficient; the more immediate bug is
same-session lifecycle authority escaping the broker.

建议: Remove `STREAM.CREATE` / `STREAM.DELETE` / purge-style JS API permissions
from ctl and agent templates unless a real `nats.go` call proves they are
strictly required. If `nats.go` auto-create behavior requires CREATE, do not let
ctl/agent call that path; have the broker create the bucket and pass an existing
bucket name. Add negative tests that a normal member/agent cannot create or
delete an `OBJ_xfer-<sid>-*` stream even inside its own session.

2. Tier-B pull still needs its own state machine and wire envelopes.

疑虑: The new eight-phase diagram describes push. The plan then says pull is
"symmetric, swapping ctl <-> agent in steps 3, 6", but the wire protocol still
only shows `PushReq` / `PushResp` and the old sentence that the agent fills
`PullResp` in the reply.

原因: Pull is not a trivial mirror under this control model. For pull, the
agent is the producer and ctl is the consumer. The plan must say when the broker
forwards the bucket to the agent, how the agent signals "object is ready", when
the ctl starts `ObjectStore.Get`, and which request the broker keeps open for
the final CLI result. The Author Response claims all req/reply types are listed
in the wire protocol, but `PushCommit`, `PullCommit`, bucket-ack, final commit
response, and `TransferEvent` are not actually defined there.

建议: Add explicit structs and separate push/pull phase tables before coding:
for example `TransferPrepareResp{OK, Code, Error, Bucket, ObjectName,
ExpiresAt}`, `PushCommitReq/Resp`, `PullCommitReq/Resp`, and
`TransferEvent{TransferID, Bucket, Verb, Code, Bytes, SHA256}`. Define which
NATS request is open at each phase and which subject carries the final reply.

3. JS-disabled boot behavior contradicts Tier-A availability.

疑虑: The plan says Tier A has no JetStream requirement and remains available
when JS is unavailable, but the risk section says push/pull handlers are
registered only after `b.js` is non-nil.

原因: If the broker registers push/pull handlers only after JS is ready, a
JS-disabled broker has no handler for Tier-A `push.req` / `pull.req` either.
If the intended design is a "base handler" plus Tier-B capability probe, that
subject/protocol is not in the main design or file list. `chooseTier` also
cannot infer broker JS availability from `nc.MaxPayload()` alone.

建议: Register push/pull handlers regardless of JS, and have the handler return
`jetstream_unavailable` only for Tier-B requests when `b.js == nil`. If the ctl
needs preflight capability, define the capability subject and response shape in
the wire protocol and tests. Alternatively, require JS for all file transfer and
drop the Tier-A-without-JS claim.

### Medium Concerns

4. History retention is still misstated.

疑虑: The revised risk section says `history-<sid>` uses `DiscardOld`, but the
current code uses `DiscardNew`.

原因: This changes failure behavior. With `DiscardNew`, a full history stream
rejects new transfer audit entries instead of evicting old entries. That is a
different operational risk from "old transfer events get discarded once history
fills".

建议: Either fix the plan text to `DiscardNew`, or explicitly include
`internal/jsstream/jsstream.go` in Files Touched and call out the retention
behavior change as a separate audit-policy decision.

5. Bucket name and stream name are mixed.

疑虑: The plan alternates between bucket names like `xfer-<sid>-...` and
`OBJ_xfer-<sid>-...`, and even says broker creates an Object Store bucket named
`OBJ_xfer-...`.

原因: In NATS Object Store, the user-facing bucket name and the backing stream
name are distinct: bucket `xfer-...` maps to stream `OBJ_xfer-...`. Passing
`OBJ_xfer-...` as the bucket name risks creating `OBJ_OBJ_xfer-...` or granting
permissions for the wrong subject namespace.

建议: Standardize the plan vocabulary: `bucket = xfer-<sid>-...`,
`stream = OBJ_<bucket>`. Keep proto/audit fields explicit about which one they
carry, and make tests assert the real stream name.

6. Path validation still needs read-side and parent-dir clarity.

疑虑: The validation pipeline explicitly protects the write leaf with
`O_NOFOLLOW`, but symlink rejection is a non-goal for both push and pull. The
plan also says `ENOENT` for a non-existent parent dir means "create-then-write
is OK" without stating whether file transfer creates parent directories.

原因: Pull reads must not follow a symlink leaf either, otherwise `pull
node:/allowed/link-to-secret ./local` bypasses the symlink non-goal. Parent
directory creation changes the feature scope and path-validation surface; if
parents are created implicitly, containment and permissions need tests for each
new component.

建议: State that parent directories must already exist for v2.0 unless
`mkdir -p` is deliberately added. For pull, use `lstat` / no-follow open on the
source leaf and reject symlink/special files before reading.

### Suggested Before Implementation

- Update the main wire-protocol section so it matches the new state machine.
- Remove or justify member/agent stream create/delete permissions.
- Make JS-disabled behavior executable on paper: handler registration,
capability probe, and exact error path.
- Fix the history retention statement to match current code.

---

## Author Response to Reviewer Notes Round 2 — 2026-05-12

6 项 concerns 全数采纳。无驳回项。每条都改了正文，最后再用 grep 做了一遍一致性 sweep（自审改进措施在 Round 1 self-review pass 里承诺过）。

### Blocking Concerns

**#1 Bucket ownership 跟 permission sketch 矛盾** — **ACCEPTED**.
- reviewer 严格对：`broker-only` 跟给 ctl `STREAM.CREATE/DELETE` 不能共存。删了 `STREAM.CREATE` / `STREAM.DELETE` / `STREAM.PURGE` / `STREAM.UPDATE` 从 ctl AND agent 模板。保留 `STREAM.INFO` (read-only 探查) + 完整 `CONSUMER.*` 集合（`Get` 需要建 consumer）+ `$O.*.M.>` / `$O.*.C.>` (data subjects)。
- 加 same-session lifecycle authority 负测试: a member of session A 不能 publish `$JS.API.STREAM.CREATE.OBJ_xfer-A-evil` even inside its own session.
- §Verification 的 unit-test 列表也加了断言。

**#2 Tier-B pull 缺独立状态机和 wire structs** — **ACCEPTED**.
- 加了完整 §Tier B pull state machine — 9 步 phase 图，明确"agent 是 producer"导致控制流跟 push 不同：
  - 步骤 3：agent stat + sha256 + 决定 tier（不像 push 在 ctl 端就能决定）
  - 步骤 6：agent emit `put-done` event
  - 步骤 7：ctl 收到后再发 `pull-commit.req`
  - 步骤 9：ctl 验 sha + rename（agent 完全不碰 destination）
- 完整 wire structs 列在 §Wire protocol 章节: `PushPrepareReq` / `PushPrepareResp` / `PullPrepareReq` / `PullPrepareResp` / `TransferCommitReq` / `TransferCommitResp` / `TransferEvent` / `CapsReq` / `CapsResp`，每个字段都写注释 + tier 适用情况。
- 完整失败 code 集列在 §Wire protocol 末尾（15 codes）。

**#3 JS-disabled 与 Tier A 无 JS 矛盾** — **ACCEPTED, full reflow**.
- reviewer 对：旧版"handlers register after b.js ready"等于 JS 不可用时连 Tier A 都没 handler。
- 改：handlers **无条件注册**(register-time NOT predicated on b.js)；handler 运行时 if b.js==nil → 仅 Tier-B 返 `jetstream_unavailable`，Tier-A 继续走 inline 路径。
- 加新增 capability subject `tether.v1.ctrl.by.<actor>.s.<sid>.caps.req` + `proto.CapsReq` / `proto.CapsResp{JetStreamReady, MaxPayload}`，让 ctl 在 `chooseTier` 之前能 single round-trip 探查 broker 能力，避免猜测。Allow-list 加 1 个 subject。

### Medium Concerns

**#4 DiscardOld vs DiscardNew** — **ACCEPTED, factual fix**.
- 看 `internal/jsstream/jsstream.go:101` 实际是 `DiscardNew`。
- 改了 §Risk 那段：`DiscardNew` 意味着 history-<sid> 满了**拒**写新 audit，**不**是 evict 旧的。运维含义不同。

**#5 Bucket name vs stream name 混用** — **ACCEPTED, standardized vocabulary**.
- 看 `nats.go@v1.52.0/jetstream/object.go:1554-1591`: bucket name = user-facing；nats.go 内部加 `OBJ_` 前缀作 stream name。所以 `xfer-<sid>-...` 是 bucket，`OBJ_xfer-<sid>-...` 是 stream。
- 加了 §Wire protocol "Naming convention" 表，全 plan 统一：proto 字段 / Resp / state machine 里说 "bucket" 都是 user-facing `xfer-<sid>-...`；permissions 和 reconcile scan 用 stream 形式 `OBJ_xfer-<sid>-...`。
- §Object bucket lifecycle 表步骤 2 显式 spell out 两者：bucket name `xfer-<sid>-...` + stream `OBJ_xfer-<sid>-...`。

**#6 Path validation read-side缺失 + parent dir 模糊** — **ACCEPTED**.
- reviewer 对：旧版只把 `O_NOFOLLOW` 用在 write leaf；pull 的 read leaf 也必须 reject symlink，否则 `pull node:/allowed/link-to-secret ./local` 绕过 non-goal。
- §Refusing dangerous paths pipeline 重写：明示 "same rules apply to push (write) and pull (read)"；read leaf 用 `lstat` + `OpenFile(O_RDONLY|O_NOFOLLOW)` + dev/inode 双检关 TOCTOU。
- Parent dir：v2.0 **不自动 mkdir -p**。push 的 parent dir 必须已存在(`path_parent_missing`)；pull 的 parent dir 必须已存在(`path_not_found`)。`mkdir -p` style 留 v2.1。

### Self-review pass (Round 2)

写完上面 6 项的修订后用 grep 做了 5 类一致性扫:

1. `STREAM.(CREATE|DELETE|PURGE).OBJ_` — 剩余命中都在 reviewer block 和 negative-test assert(`member CANNOT publish ...`) 描述里，正文 allow-list 已干净。
2. 旧类型名 `PushReq`/`PullReq`/`PushResp`/`PullResp` 不带 Prepare 前缀的残留 — 找到 6 处 (L227, L244, L283, L608, L632, L679)，全部改成 PushPrepareReq / PullPrepareResp / TransferCommitReq / TransferCommitResp 等。
3. `DiscardOld` — 剩余命中只在 reviewer Round 2 引用块 + Round 1 我的旧 author response 里 (历史记录保留)；正文 Risk 段已写 `DiscardNew`.
4. `pull is symmetric` / `swapping ctl <-> agent` — 剩余命中只在 reviewer Round 2 引用块。正文 §Tier A pull 改成显式"same shape with ctl as receiver"；§Tier B pull 有独立 9-step state machine。
5. 旧 size 数字 (10 MB / 12 MB) — 已在 Round 1 后清干净；新数字 (8 MiB / 16 MiB / 200 MiB) 全文一致。

### Status

3 blocking + 3 medium 全数解决。下一轮 review 期待聚焦：(a) wire struct 是否完整(再检查有无字段漏)，(b) bucket lifecycle 在所有失败路径下的 ownership 是否仍仅在 broker 手中，(c) JS-disabled 路径是否真能并存 Tier A + 拒绝 Tier B 而无副作用。

---

## Reviewer Notes Round 3 - 2026-05-12

Scope: re-reviewed the Round 2 revision against the current repo and the local
`nats.go` v1.52.0 Object Store implementation. This is still a plan review.

Conclusion: the Round 2 fixes resolved most of the previous findings. The plan
is close, but I still would not start implementation until the remaining control
flow is tightened. The problem has moved from broad design gaps to two concrete
state-machine gaps plus one predictable auth permission failure.

### Blocking Concerns

1. Tier-B pull still has no coherent final-result path.

疑虑: The pull state machine now has an agent `put-done`, then ctl sends
`pull-commit.req`, then ctl performs `ObjectStore.Get` and local SHA/rename,
while the broker "waits for ctl commit-reply completion". But ctl is the
requester on `pull-commit.req`; it cannot also provide the commit reply unless
there is a separate ctl-to-broker completion subject.

原因: The final success/failure for pull is determined on the ctl side after
`ObjectStore.Get`, SHA verification, and local rename. The broker cannot write
accurate `audit.transfer{kind=complete|failed}` or safely delete the bucket
based only on agent `put-done` or an agent no-op ack. The current diagram also
mentions `pull-prepare.forwarded` and `put-done` forwarding, but neither subject
nor proto shape is in the subject map.

建议: Make pull a broker-observed three-phase protocol:
`pull.req` -> agent stat/put -> broker replies bucket/object to ctl -> ctl
`Get` + verify + rename -> ctl sends `pull-complete.req` or `pull-failed.req`
to broker. Broker writes final audit and deletes the bucket from that ctl
completion message. Define the subjects, structs, timeout behavior, and tests
for ctl crash after bucket creation and after `Get` starts.

2. Tier-A completion/failure audit is still undefined.

疑虑: `TransferEvent` says tier A does not emit transfer events, but the audit
section still says complete/failed audit comes from agent `ev.transfer.*`.
Tier-A pull later introduces `pull-ack.req` as an option, but that subject and
proto are not in the subject map; Tier-A push has no equivalent ack/event path.

原因: If tier-A push/pull preserve the agent reply directly to ctl, the broker
only sees `kind=start` and cannot reliably write `complete` or `failed`. If the
broker proxies the reply, the plan must say so explicitly because it differs
from the existing exec pattern. If ctl ack is required for pull, the same
principle applies to push success/failure too unless the agent emits an event.

建议: Pick one invariant for all tiers: either agents emit
`ev.transfer.complete/failed` for Tier A and Tier B, or the broker proxies all
transfer replies and writes audit from observed responses, or ctl sends an
explicit final ack/fail for receiver-side outcomes. Add the missing subject(s)
and proto structs before implementation.

3. The Object Store permission sketch is still missing subjects required by
the actual `nats.go` client.

疑虑: The revised allow-list removes stream create/delete, which is correct,
but it also omits `$JS.API.STREAM.MSG.GET.OBJ_xfer-...`.

原因: In local `nats.go@v1.52.0`, `ObjectStore.GetInfo` calls
`GetLastMsg`, which publishes to `STREAM.MSG.GET.<stream>`. `Put` calls
`GetInfo` before writing, and `Get` calls `GetInfo` before subscribing to
chunks. Without `STREAM.MSG.GET`, both producer and consumer paths will fail
under auth_callout before any data moves. `Put` also calls `STREAM.PURGE` for
partial cleanup on error; if the design intentionally denies client purge, the
plan should state that failed client-side cleanup is tolerated because broker
bucket deletion is authoritative.

建议: Add `STREAM.MSG.GET.OBJ_xfer-<sid>-*` to ctl and agent permissions, or
switch to a lower-level Object Store access path that does not require it.
Explicitly document whether client `STREAM.PURGE` remains denied and which test
proves a failed/canceled `Put` still gets cleaned by broker bucket deletion.

### Medium Concerns

4. The capability probe is introduced in Risk but not wired in the main design.

疑虑: `CapsReq/CapsResp` and `ctrl.by.<actor>.s.<sid>.caps.req` appear only in
the risk/files sections. The main subject map, auth section, and broker routing
plan still say command-plane subjects need no additions.

原因: The caps subject is a new control-plane endpoint, not covered by the
existing `cmd.by...node.*.*.req` wildcard. It needs a permission entry,
membership/session gate, broker subscription, proto subject helper or parser,
and e2e coverage. Without it, ctl cannot know JS availability before choosing
Tier B on un-bumped `max_payload` brokers.

建议: Promote caps to the Wire Protocol and Auth sections: define
`CapsReq/CapsResp`, subject, membership rules, permission allow entry, and tests
for active member allowed / non-member denied / JS-disabled returns false.

5. Pull request comments still contradict the new pull state machine.

疑虑: `PullPrepareResp` comments still say ctl proceeds to `Get` then sends
commit for pull, while the pull state machine says agent puts first, emits
`put-done`, then ctl sends `pull-commit`.

原因: These comments will guide implementation. If copied literally, one
developer can implement ctl-first pull while another implements agent-first
pull.

建议: After fixing the pull finalization path, sweep the proto comments and
phase table so they describe exactly one ordering.

### Before Code

- Resolve pull finalization with an explicit ctl completion/failure message.
- Define Tier-A complete/failed audit mechanics instead of relying on implicit
reply observation.
- Add `STREAM.MSG.GET` or avoid `nats.go` ObjectStore APIs that require it.
- Move caps from Risk into the main wire/auth/test plan.

---

## Author Response to Reviewer Notes Round 3 — 2026-05-12

5 项 concerns 全数 ACCEPTED + 落实到正文。

### Blocking Concerns

**#1 Tier-B pull final result path 不闭合** — **ACCEPTED, 重新设计**.

reviewer 严格对：旧版"broker waits for ctl commit-reply completion"是无意义陈述——ctl 是 commit-req 的**发送方**，不能既请求又应答。

修复：定义新的**finalization 不变量**（写在 §Wire protocol 顶部）：
> The broker ALWAYS hears from the receiver side before writing the
> final audit entry, regardless of tier.

具体落到协议：
- **Push** (任何 tier)：agent 是 receiver → agent emits `ev.node.<nid>.transfer.<id>.complete|failed` → broker 写 audit + 删 bucket + 把事件 proxy 回 ctl 的等待 inbox。
- **Pull** (任何 tier)：ctl 是 receiver → ctl 在 Get + verify SHA + rename 完成后**显式**发新 subject `ctrl.by.<actor>.s.<sid>.transfer.<id>.finalize.req` 给 broker，broker 写 audit + 删 bucket（tier B）+ 回 `TransferFinalizeResp{OK}`让 ctl 干净退出。

加新 proto 类型 `TransferFinalize` + `TransferFinalizeResp`；新 broker handler `internal/broker/transfer_finalize.go`。

旧 `pull-commit.req` + agent `put-done` event 被删除——多余且容易让人困惑。重写 §Tier B pull state machine 为简洁 7 步。

**#2 Tier-A complete/failed audit 未定义** — **ACCEPTED, 同一框架**.

reviewer 对：旧版 TransferEvent 注释说"tier A 不发"，但 audit 段又说"complete/failed 来自 agent ev.transfer.*"——逻辑矛盾。

把 finalization 不变量同时应用 tier A：
- Tier-A **push**: agent 处理完写入后还是 emit `ev.node.<nid>.transfer.<id>.complete|failed` (TransferEvent)，broker 写 audit + proxy 回 ctl。tier 不变 receiver 角色。
- Tier-A **pull**: 跟 tier-B pull 一样，ctl 发 `transfer.<id>.finalize.req` 给 broker。tier-A pull 没 bucket，broker delete bucket 是 no-op。

§Wire protocol 加了 receiver 表 4 行，明示 push/pull × tier-A/B 4 个组合的 receiver + final-result publisher + subject。

**#3 Object Store perms 缺 `STREAM.MSG.GET`** — **ACCEPTED, 真实事实**.

我手动 grep nats.go@v1.52.0 确认：
- `object.go:1176/1316`: `obs.stream.GetLastMsgForSubject(ctx, metaSubj/allMeta)` — ObjectStore.GetInfo 内部
- `stream.go:557`: `func (s *stream) GetLastMsgForSubject(...)` → `getMsg(...)` 发到 `STREAM.MSG.GET.<stream>`
- `Put` 和 `Get` 都先调 GetInfo

所以两个 perm 模板都加 `$JS.API.STREAM.MSG.GET.OBJ_xfer-<sid>-*`。注释明示：
- 故意不给 `STREAM.PURGE`（broker bucket-delete 是权威清理）；ObjectStore.Put 错误路径试 PURGE 时 NATS 层拒，无害。
- 加 e2e #21：真 `nats.go` ObjectStore.Put 在 auth_callout 下完成无权限错（Round-3 #3 直接回归测试）。

### Medium Concerns

**#4 Caps probe 没 wire 到 main design** — **ACCEPTED**.

把 caps 从 §Risk 提到正文：
- §Wire protocol "Subject map" 表第 1 行加 `caps.req`
- §Wire protocol 类型定义最后加 `CapsReq` / `CapsResp`
- §Auth callout perms ctl 模板加 `ctrl.by.<actor>.s.<sid>.caps.req`
- §Files Touched 加新文件 `internal/broker/caps.go`
- §Verification e2e #16 加 caps end-to-end，#22 加 auth_callout 隔离测试

broker handler 内容：membership-gate（同现有 PsReq pattern），返回 `JetStreamReady=b.js!=nil` + `MaxPayload=nc.MaxPayload()` + `BrokerVersion`。

**#5 PullPrepareReq/Resp 注释跟新 state machine 矛盾** — **ACCEPTED, 删除重复定义**.

发现是因为我 Round 2 加新版 PullPrepareReq 时**忘了删旧版**——文档里有两个 `type PullPrepareReq struct {...}`，一个 L160 旧（"agent Puts then ctl Gets"），一个 L199 新（"agent decides tier; ctl never pre-decides"）。Go 实现时会 redeclare 编译报错；plan 阅读时会让 reviewer 看到两套语义。

修：删 L160-172 的旧版 PullPrepareReq 块。剩 L199 新版唯一定义。

注释也跟 Round-3 重新设计的 pull state machine 对齐：tier B 写 "agent has already begun (or finished) Put-ing... ctl uses bucket info to start ObjectStore.Get, then verifies + rename, then sends finalize.req"。无 commit phase。

### Self-review pass (Round 3, grep-driven)

按 Round-1 self-review pass 总结的"用 grep 找关键数字一致性"流程，跑 5 类 sweep：

1. ✓ `TransferCommitResp` 已删（旧的 commit reply 类型不再存在；最终结果通过 broker proxy 到 ev.transfer 或 finalize ack 走）。剩余命中只在 Round 2 author response 引用块里。
2. ✓ `put-done` / `pull-commit.req` 在正文已无（`internal/broker/broker.go` 列表也改了，把 pull-commit 删掉，加 `caps.req` 和 `finalize.req`）。Reviewer notes 块里的命中是引用，OK。
3. ✓ `STREAM.MSG.GET` 同时加在 ctl + agent 两个模板（L514, L542）。
4. ✓ `finalize` 在 7 处出现：subject map / receiver 表 / proto 注释 / pull state machine / Files Touched / Verification e2e / negative tests。一致。
5. ✓ 重复 type 定义检查（grep `^type Pull...|^type Push...`）：每个 type 现在只一份。

### Status

3 blocking + 2 medium 全数解决。下一轮 review 期待聚焦：(a) finalize 协议下的 audit start/complete/failed 三元组是否对所有 4 个组合（push×{A,B}, pull×{A,B}）都一致写入，(b) caps 在 ctl 的具体使用点（`chooseTier` 之前？同步还是 lazy？），(c) ObjectStore.Put 错误时 PURGE 失败被 nats.go 当成 fatal 还是 warning（如果 fatal 那 Round-3 #3 的 PURGE 排除策略需要再想）。

---

## Reviewer Notes Round 4 - 2026-05-12

Scope: final pre-implementation review of the Round 3 revision. I checked the
new finalization design against the current auth permission templates and the
existing `ctrl.by` / `cmd.by` subject layout.

Conclusion: not quite ready to approve. The state machine is now substantially
better, but one auth gap will break pull finalization under auth_callout. Fix
that and align the audit prose with the new receiver-finalization invariant;
after that I do not see a need for another large design round.

### Blocking Concern

1. `transfer.<id>.finalize.req` is not allowed by activated-member JWTs.

疑虑: The new pull finalization subject is
`tether.v1.ctrl.by.<actor>.s.<sid>.transfer.<id>.finalize.req`, but the current
activated-member template only allows specific `ctrl.by.<actor>.s.<sid>.*`
subjects for `ps`, `node.list`, `node.*.tag`, plus the separate
`s.<sid>.cmd.by...` command wildcard. The auth section adds `caps.req`, but not
`transfer.*.finalize.req`.

原因: With auth_callout enabled, ctl will complete `ObjectStore.Get` and local
rename, then NATS will reject its finalize publish. The broker will not write
`audit.transfer{kind=complete}`, will not delete the tier-B bucket until timeout
or boot reconcile, and pull will look failed despite the local file being
written.

建议: Add this exact permission to `PermissionsForActivatedMember.Pub.Allow`:

```go
subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".transfer.*.finalize.req"
```

Also add a static auth test and an auth_callout e2e where pull tier-B reaches
finalize successfully, plus a negative test that another actor cannot finalize
someone else's `transfer_id`.

### Minor Follow-Ups

2. The Audit section still describes only push completion.

疑虑: The main Audit section still says final audit comes from the agent's
`ev.node.<nid>.transfer.complete/failed`. That is true for push, but the revised
pull design uses ctl `transfer.<id>.finalize.req`.

建议: Rewrite the audit paragraph around the finalization invariant table:
start is written from broker-accepted prepare; final is written from agent
`ev.transfer.*` for push and ctl `finalize.req` for pull.

3. Keep the `STREAM.PURGE` denial test explicit.

疑虑: The plan correctly notes that `nats.go` may attempt `STREAM.PURGE` on
failed `Put`, and that broker bucket deletion is authoritative. That behavior
should be pinned because otherwise a permission-denied purge can obscure the
original transfer error.

建议: Keep the planned auth/ObjectStore test, and add a canceled/failed `Put`
case that verifies broker cleanup still deletes the bucket even though client
purge is denied.

### Approval Gate

Once the finalize permission and audit prose are fixed, I would approve moving
from plan to implementation. The remaining issues are implementation risks that
the proposed tests are capable of catching.

## Author Response to Reviewer Notes Round 4 — 2026-05-12

Round 4 reviewer flagged 1 blocking + 2 minor. All 3 ACCEPTED, fixed
in this revision before any code is written. Locations cited below.

### Blocking Concern

1. **`transfer.<id>.finalize.req` not allowed by activated-member JWTs** —
   ACCEPTED. This was the actual gap: the Round-3 fix added the new
   `ctrl.by.<actor>.s.<sid>.transfer.<id>.finalize.req` subject to the
   wire protocol and broker subscription, but never extended the JWT
   pub allow-list. Under auth_callout the publish would have been
   denied at NATS, broker would never write `kind=complete`, and pull
   would silently mis-report as `ctl_disconnect` after 5 minutes
   despite the local file being correct.

   **Fix in §Auth callout permissions** (`PermissionsForActivatedMember`
   block, 1 new line):

   ```go
   subjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".transfer.*.finalize.req",
   ```

   Wildcard on `<id>` (not `<actor>` or `<sid>`) is intentional and
   layered:
   - NATS layer enforces sid binding: the JWT is minted for the
     specific sid the actor is a member of, so `s.<other-sid>.*` is
     already denied (verified by new e2e #24 below).
   - Broker layer enforces transfer_id ownership: even within the
     correct sid, the broker matches the publishing actor against the
     recorded creator of `<id>` and rejects mismatches with
     `not_owner_or_creator` (this is the existing `transfer_finalize`
     handler from Round 3, covered by e2e #17).

   This is the exact same two-layer defence pattern Round 3 used for
   `caps.req` — NATS bounds the sid, broker bounds the resource.

   Tests added per reviewer request:
   - **#24 cross-session NATS-layer denial**: session-B member
     attempts to publish `s.<A>.transfer.*.finalize.req` against
     session-A — denied at callout, never reaches broker.
   - Existing **#17 forgery test** (Round 3): same-sid wrong-actor —
     application-layer rejection by broker.
   - Static guard in **#20** extended (no source change required, but
     the assertion list grows by one entry to include
     `transfer.*.finalize.req`).
   - Existing **#11 + #12** already exercise the happy path of
     `finalize.req` under JS without auth_callout; **#15 in security
     file** + a new tier-B pull subtest covers it under auth_callout
     (pinned via the perm-template static guard).

### Minor Follow-Ups

2. **Audit section described only push completion** — ACCEPTED. The
   §Audit prose was inherited from before the Round-3 finalize
   redesign and still claimed all final audit comes from agent
   `ev.transfer.*`. Rewrote around the receiver-finalization
   invariant. New section opens with the invariant statement, then a
   4-row table covering all push×{A,B} and pull×{A,B} combinations
   showing which subject the start vs final audit row is sourced
   from. Closes with the timeout-fallback rule (broker writes
   `code=ctl_disconnect` / `agent_no_responders` autonomously after
   the per-tier budget) and an explicit reminder that this is **why**
   the new `transfer.*.finalize.req` perm matters — without it every
   successful pull would mis-report.

3. **`STREAM.PURGE` denial test stays explicit** — ACCEPTED. Added
   **#23** to `test/security/transfer_auth_test.go`: starts a real
   `ObjectStore.Put` under auth_callout, cancels the upload mid-stream
   to trigger `nats.go`'s partial-cleanup `STREAM.PURGE` attempt,
   asserts (a) the original cancel error is what the caller sees (not
   the misleading PURGE-permission-violation second error), (b) the
   bucket is still present immediately after Put fails (because
   client-side cleanup was denied), (c) after broker's failure path
   runs (push-commit timeout or explicit failed finalize.req for the
   pull case), `stream.Info` returns "stream not found" — proving
   broker bucket-delete is the authoritative cleanup as documented in
   §Object bucket lifecycle.

### Self-review pass (Round 4, grep-driven)

Same approach as Round 3 — exact greps to catch text/code drift:

- `grep -n "finalize.req" docs/reviews/file-transfer-plan.md` → 28
  hits, all consistent (subject map, state machine, perm template,
  audit table, file list, e2e #11-#13, #17-#18, #23-#24, broker
  subscribe list, files-touched).
- `grep -n "ev.transfer\|ev.node.*.transfer" …` → push-only, never
  claimed for pull anywhere in the body.
- `grep -n "STREAM.PURGE" …` → 4 hits: §Auth (allow-list comment
  about partial-cleanup), §Object bucket lifecycle (broker is
  authoritative), e2e #20 (negative static guard — no PURGE in
  template), e2e #23 (the new runtime test). All consistent.
- `grep -n "PermissionsForActivatedMember" …` → 2 hits (§Auth +
  Files Touched), both reference the new `transfer.*.finalize.req`
  line.
- `grep -n "audit.*kind=complete\|kind=failed\|kind=start" …` →
  audit table covers the same set the state machines reference.
- Verified §Audit table column 4 entries match exactly the subjects
  in §Wire protocol subject map (no typos drift).

### Status

1 blocking + 2 minor 全数解决。`transfer.*.finalize.req` 已加入
activated-member JWT pub allow-list（1 行 perm，2 行注释），audit
section 重写为 receiver-finalization invariant 4-row 表 + timeout 兜
底说明，PURGE-denial 测试 #23 + 跨 session NATS 拒绝测试 #24 已纳入
verification list。

按 reviewer 的 Approval Gate："Once the finalize permission and audit
prose are fixed, I would approve moving from plan to implementation."
两项均已就位。如需 Round 5 trip-wire review，建议聚焦：(a) 实际生成的
JWT 字符串里 `transfer.*.finalize.req` 是否真的 expand 出预期 ACL
shape（这要写代码才能 grep），(b) `internal/broker/transfer_finalize.go`
的 idempotency lock（重复 finalize 在 5min timeout fallback path 上是
否仍幂等）。两者都是 implementation-time concerns，本 plan 阶段无需
再改。

待 Round 5 review 或直接进入 implementation。

---

## Reviewer Notes Round 5 - 2026-05-12

Scope: re-reviewed the Round 4 edits against the prior blocking concerns: finalize publish permission, audit source-of-truth invariants, ObjectStore auth verbs, broker-owned cleanup, and verification coverage.

Conclusion: approved to proceed from plan to implementation. I do not see a remaining design-level blocker.

Why this is now acceptable:

1. Finalize permission is no longer an auth/JWT hole.
   - The subject map now includes `transfer.<transfer_id>.finalize.req`.
   - `PermissionsForActivatedMember` now grants the actor-scoped publish subject under `ctrl.by.<actor>.s.<sid>.transfer.*.finalize.req`.
   - The plan keeps the actor/session/transfer ownership check in broker logic, which is the right split: JWT limits the subject envelope, broker state validates the specific transfer.

2. Audit ownership is now coherent.
   - The plan clearly separates start events from final events.
   - Push completion is finalized from agent receiver events.
   - Pull completion is finalized from ctl receiver `finalize.req`.
   - Timeout fallback is explicitly broker-owned and must not fabricate success.
   - This removes the prior sender-side false-success risk.

3. ObjectStore auth is now implementable under auth_callout.
   - `$JS.API.STREAM.MSG.GET.OBJ_xfer-...` is now included for both ctl and target agent.
   - `STREAM.CREATE`, `STREAM.DELETE`, `STREAM.PURGE`, and `STREAM.UPDATE` remain denied for members.
   - The broker remains sole owner for bucket creation/deletion.
   - The plan explicitly acknowledges the NATS ObjectStore client may attempt purge on failed `Put`, and adds tests for the denied-purge path plus broker cleanup.

4. Capability probing and broker finalize handling are no longer hand-waved.
   - `caps.req` and `transfer.*.finalize.req` broker subscriptions are listed in files touched.
   - `internal/broker/caps.go` and `internal/broker/transfer_finalize.go` are called out.
   - Verification includes static auth guards, real JS ObjectStore Put/Get under auth_callout, denied purge cleanup, cross-session finalize denial, and finalize unit tests.

Non-blocking implementation watchpoints:

1. Keep the protocol names consistent while coding.
   - The plan still has minor prose that sounds like `TransferFinalize{OK,...}` in one place, while the struct shape is `TransferFinalize{Kind, Code, Detail, Bytes, Sha256}`. Do not copy the stale shorthand into code or tests.

2. In the failed/canceled ObjectStore `Put` tests, assert both sides of the behavior.
   - The user-visible transfer failure should remain understandable even if the NATS client also reports a denied purge.
   - Broker cleanup must still delete the bucket and emit the expected failed audit state.

3. Make sure pull cleanup is tested, not only push cleanup.
   - The lifecycle table is mostly push-oriented, but the pull state machine contains the important failure paths. Implementation should cover ctl-download/finalize timeout and broker cleanup for tier-B pull.

Approval: proceed to implementation with the verification matrix in this document treated as the acceptance bar.
