package agent

import (
	"context"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/filetransfer"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/transferpath"
	"github.com/LinZiyang666/tether/internal/xferagent"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Transfer handlers delegate byte work while admission and preparation ownership remain here.
// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
func transferHandler(a *Agent) *filetransfer.Handler {
	return filetransfer.LoadWorkerHandler(&a.transfers, a.cfg.TransferWorker, func() *filetransfer.Handler { return transferShell(a) })
}

// transferShell is the handler before its worker exists; an image restore
// builds the same shell around the imported worker (bootstrap.go).
func transferShell(a *Agent) *filetransfer.Handler {
	return &filetransfer.Handler{
		Logger: a.cfg.Logger, Roots: a.canonAllowRoots, Mode: int(a.transferMode), JetStream: func() jetstream.JetStream { return snapshotAgentJetStream(a) }, SID: a.cfg.SID,
		Instance: func() string { return a.instanceID }, NID: func() string { return nidOf(a) }, Admit: func() (func(), bool) { return a.gate.admit(gateTransfer) }, RunContext: a.loadRunCtx, AdmitCreation: func() (func(), bool) { return admitTransferCreation(a) },
		Remember: func(id string, e filetransfer.PushCommitEntry) bool {
			return a.rememberPushCommit(id, pushCommitEntry{vp: e.Validated, path: e.Path, sha256: e.SHA256, size: e.Size, force: e.Force, added: e.Added})
		},
		Lookup: func(id string) (filetransfer.PushCommitEntry, bool) {
			a.pushCacheMu.Lock()
			defer a.pushCacheMu.Unlock()
			e, ok := a.pushCommitCache[id]
			return filetransfer.PushCommitEntry{Validated: e.vp, Path: e.path, SHA256: e.sha256, Size: e.size, Force: e.force, Added: e.added}, ok
		},
		Purge: a.purgePushCommitCache,
		// Keep the frozen Agent forwarding surface in the real dispatch path.
		// Concrete methods dispatch downstream operations only, never their own adapters.
		// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
		Operations: filetransfer.Operations{
			HandlePushTierA: a.handlePushTierA, ObjectStoreGetAndWrite: a.objectStoreGetAndWrite,
			PublishEvent: a.pubTransferEv, PublishFailedEvent: a.pubTransferEvFailed,
			ReplyPush: a.replyPush, ReplyCommit: a.replyCommit, ReplyPull: a.replyPull,
		},
	}
}

var pullBucketPayloadCeiling = filetransfer.PullBucketPayloadCeiling

func pushCommitEntryTTL(size int64) time.Duration { return filetransfer.PushCommitEntryTTL(size) }
func pushCommitEntryExpired(e pushCommitEntry, now time.Time) bool {
	return now.Sub(e.added) >= pushCommitEntryTTL(e.size)
}

// pushCommitEntry caches the per-transfer prep state so push-commit
// (which arrives on a separate subject) knows what bucket to Get from,
// what path to write to, and what SHA to verify.
type pushCommitEntry struct {
	vp     *ValidatedPath
	path   string
	sha256 string
	size   int64
	force  bool
	added  time.Time
}

// rememberPushCommit stores a prep entry. Must be called from
// handlePushForwarded BEFORE replying OK so a fast push-commit can
// race in without losing the entry.
// It returns false when the cache is full of entries that are all still LIVE. The caller must then
// refuse the prepare rather than proceed: the pre-batch-C code evicted the oldest entry instead, and
// with a size-derived budget the oldest live entry is by construction the largest, longest-running
// transfer — the capacity path would silently kill the exact transfer the budget work protects, and a
// constant-inequality test on the TTL would never see it.
func (a *Agent) rememberPushCommit(transferID string, e pushCommitEntry) bool {
	e.added = time.Now()
	return filetransfer.RememberPrepared(&a.pushCacheMu, &a.pushCommitCache, transferID, e, e.added, pushCommitEntryExpired)
}

func (a *Agent) purgePushCommitCache(transferID string) {
	filetransfer.PurgePrepared(&a.pushCacheMu, &a.pushCommitCache, transferID)
}

func (a *Agent) handlePushForwarded(nc *nats.Conn, msg *nats.Msg) {
	transferHandler(a).HandlePushForwarded(nc, msg)
}
func (a *Agent) handlePushTierA(nc *nats.Conn, msg *nats.Msg, req *proto.PushPrepareReq, vp *ValidatedPath) {
	transferHandler(a).HandlePushTierA(nc, msg, req, vp)
}
func (a *Agent) handlePushCommitForwarded(nc *nats.Conn, msg *nats.Msg) {
	transferHandler(a).HandlePushCommitForwarded(nc, msg)
}
func (a *Agent) objectStoreGetAndWrite(ctx context.Context, js jetstream.JetStream, store jetstream.ObjectStore, bucket, objectName string, vp *ValidatedPath, expectedSize int64, expectedSHA string, force bool) (int64, error) {
	return transferHandler(a).ObjectStoreGetAndWrite(ctx, js, store, bucket, objectName, vp, expectedSize, expectedSHA, force)
}
func (a *Agent) handlePullForwarded(nc *nats.Conn, msg *nats.Msg) {
	transferHandler(a).HandlePullForwarded(nc, msg)
}
func (a *Agent) pubTransferEv(nc *nats.Conn, kind, verb string, req *proto.PushPrepareReq,
	tier string, bytes int64, dur time.Duration) {
	transferHandler(a).PublishEvent(nc, kind, verb, req, tier, bytes, dur)
}
func (a *Agent) pubTransferEvFailed(nc *nats.Conn, verb string, req *proto.PushPrepareReq,
	tier, code, errMsg string, dur time.Duration) {
	transferHandler(a).PublishFailedEvent(nc, verb, req, tier, code, errMsg, dur)
}
func (a *Agent) replyPush(nc *nats.Conn, replyTo string, resp proto.PushPrepareResp) {
	transferHandler(a).ReplyPush(nc, replyTo, resp)
}
func (a *Agent) replyCommit(nc *nats.Conn, replyTo string, resp proto.TransferCommitResp) {
	transferHandler(a).ReplyCommit(nc, replyTo, resp)
}
func (a *Agent) replyPull(nc *nats.Conn, replyTo string, resp proto.PullPrepareResp) {
	transferHandler(a).ReplyPull(nc, replyTo, resp)
}

// The lifetime belongs to Agent, never to its replaceable NATS session.
// origin: hot-update S7 plan §8.1 / §11.2 / P20
func stopAgentTransfers(a *Agent) {
	a.cutMu.Lock()
	pristine := a.image != nil && !ownsUpgradeImage(a)
	a.cutMu.Unlock()
	if pristine {
		return // pristine imported resources belong to FinishBoot, not terminal cleanup
	}
	// Network joins never run under the host cut mutex. The worker retains the
	// duty on error and the next authenticated management binding retries it.
	// origin: S7 internal review rv-lifecycle F4 / rv-jsnats F3
	if err := xferagent.StopWithConsumerCleanup(transferHandler(a).Runtime, transferHandler(a).Worker, func() error { return filetransfer.StopWorker(transferHandler(a)) }, snapshotAgentJetStream(a)); err != nil {
		a.cfg.Logger.Error("agent: transfer worker stop incomplete", "err", err)
	}
}

func reapRevokedAgentTransfers(a *Agent) {
	stopAgentTransfers(a)
	a.reapManagedChildren()
}

// transferMode is the agent-wide push/pull path policy, resolved once at
// New() from the RAW agent.yaml file_transfer.allow_roots presence (NEVER
// from len(canonAllowRoots) — see resolveTransferMode). It selects how the
// containment step in ValidateForWrite/ValidateForRead behaves; every
// OTHER mechanism check (absolute, EvalSymlinks-parent, parent-exists,
// leaf-symlink, regular-file, O_NOFOLLOW, dev+inode TOCTOU) runs in ALL
// modes, because those defend a hostile NON-member process on the agent
// host — a threat orthogonal to which files an authenticated member may
// touch (the member already has unrestricted run/exec; requirements.md §9.3).
type transferMode int

const (
	// modeOpen: no allow_roots configured (key absent/null). Whole-FS
	// reach, equal to run/exec; the containment step is SKIPPED but every
	// mechanism check still runs. The default, including fresh installs.
	modeOpen transferMode = iota
	// modeNarrow: non-empty allow_roots. containedIn prefix check;
	// path_outside_roots on miss. An all-dropped narrow list stays here
	// and rejects everything (fail-closed); it never falls to modeOpen.
	modeNarrow
	// modeDisabled: explicit `allow_roots: []`. Every push/pull replies
	// transfer_disabled before any path work.
	modeDisabled
)

// resolveTransferMode decides the policy from RAW config, never from the
// canonicalized root list. rootsConfigured is true iff the operator wrote
// an allow_roots key in agent.yaml (yaml.v3 leaves the slice nil when the
// key is absent/null, non-nil len-0 for an explicit `[]`). rawRoots is the
// pre-CanonAllowRoots list, so a narrow config whose entries all drop
// during canonicalization still resolves to modeNarrow (reject-all),
// NEVER to modeOpen — the single most important security invariant of the
// transfer-unrestrict change.
func resolveTransferMode(configured bool, roots []string) transferMode {
	return transferMode(filetransfer.ResolveTransferMode(configured, roots))
}

// Preserve the agent API while filesystem policy and descriptor ownership live below it.
// origin: hot-update S4 plan §10.
type PathValidationError = transferpath.PathValidationError
type ValidatedPath = transferpath.ValidatedPath
type AtomicWrite = transferpath.AtomicWrite

func IsPathValidationError(err error) bool    { return transferpath.IsPathValidationError(err) }
func CanonAllowRoots(roots []string) []string { return transferpath.CanonAllowRoots(roots) }
func ValidateForWrite(rawPath string, canonRoots []string, mode transferMode) (*ValidatedPath, error) {
	return transferpath.ValidateForWrite(rawPath, canonRoots, int(mode))
}
func ValidateForRead(rawPath string, canonRoots []string, mode transferMode) (*ValidatedPath, error) {
	return transferpath.ValidateForRead(rawPath, canonRoots, int(mode))
}
func OpenForReadAtomic(vp *ValidatedPath) (*os.File, error) {
	return transferpath.OpenForReadAtomic(vp)
}
func OpenForWriteAtomic(vp *ValidatedPath, randSuffix string) (*os.File, *AtomicWrite, error) {
	return transferpath.OpenForWriteAtomic(vp, randSuffix)
}
func RenameForWriteAtomic(vp *ValidatedPath, pending *AtomicWrite, force bool) error {
	return transferpath.RenameForWriteAtomic(vp, pending, force)
}

type transferWorkerDependencies = filetransfer.WorkerDependencies
type transferWorkerOwner = filetransfer.WorkerOwner

func logAgentTransferPolicy(a *Agent) {
	filetransfer.LogTransferPolicy(a.cfg.Logger, int(a.transferMode))
}
func marshalAgentTransferRegister(a *Agent, req proto.NodeRegisterReq) ([]byte, error) {
	return filetransfer.MarshalWorkerRegister(transferHandler(a), req)
}
