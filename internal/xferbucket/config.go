package xferbucket

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const objectRecoveryHorizon = proto.XferTierBMaxBudget + 120*time.Second
const bucketFloor = 256 * 1024 * 1024
const bucketCap = 2*1024*1024*1024 + 64*1024*1024

var bucketName = regexp.MustCompile(`^xfer-[a-zA-Z0-9_-]+$`)

// Refusal exposes only stable public configuration diagnostics. Transport errors
// remain available through Unwrap without crossing the public forwarding bus.
// origin: hot-update S7 plan lead LQ3 / §2.1 / T05
type Refusal struct {
	Code   string
	Detail string
	cause  error
}

func (e *Refusal) Error() string        { return e.Code + ": " + e.Detail }
func (e *Refusal) TransferCode() string { return e.Code }
func (e *Refusal) Unwrap() error        { return e.cause }

func storageHorizonError(horizon time.Duration, reason string) error {
	return &Refusal{Code: xfercontract.CodeStorageHorizon, Detail: fmt.Sprintf("requires Duplicates >= %s and MaxAge = 0 or MaxAge >= %s: %s", horizon, horizon, reason)}
}

func normalizePatch(p xfercontract.ConfigPatch, limits xfercontract.StorageLimits) (xfercontract.ConfigPatch, error) {
	if !bucketName.MatchString(p.Bucket) || p.Replicas < 0 || p.Replicas > 3 || p.MaxBytes < 0 || p.Duplicates < 0 || p.RecoveryHorizon < 0 {
		return p, errors.New("xferbucket: invalid configuration patch")
	}
	p.Replicas = max(p.Replicas, 1)
	p.RecoveryHorizon = max(p.RecoveryHorizon, objectRecoveryHorizon)
	p.Duplicates = max(p.Duplicates, p.RecoveryHorizon)
	// Reject before CREATE/UPDATE and before any first-byte seal. TTL belongs
	// to the operator; neither a larger window nor legacy can repair short TTL.
	// origin: hot-update S7 plan §2.1 / LQ3 / T05
	if limits.MaxDuplicates > 0 && p.Duplicates > limits.MaxDuplicates {
		return p, storageHorizonError(p.Duplicates, "server duplicate-window ceiling is insufficient")
	}
	return p, nil
}

func configDigest(cfg jetstream.StreamConfig) (string, error) {
	data, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

// Copy every field, including nested maps/slices, rather than projecting the
// SDK's smaller ObjectStoreConfig. A new SDK config field survives automatically.
// origin: hot-update S7 plan §2.1 / T05
func copyStreamConfig(cfg jetstream.StreamConfig) (jetstream.StreamConfig, error) {
	data, err := json.Marshal(cfg)
	if err != nil {
		return jetstream.StreamConfig{}, err
	}
	var copied jetstream.StreamConfig
	err = json.Unmarshal(data, &copied)
	return copied, err
}

func checkRetention(cfg jetstream.StreamConfig, horizon time.Duration) error {
	// A long TTL and dedupe window cannot protect an ACKed chunk from eviction.
	// Use the writer's same predicate before any UPDATE or first-byte seal.
	// origin: S7 internal review rv-jsnats F2
	if err := CheckTransferRetention(cfg, horizon); err != nil {
		return storageHorizonError(horizon, err.Error())
	}
	if cfg.MaxBytes <= 0 {
		return &Refusal{Code: xfercontract.CodeCapacityExceeded, Detail: "bucket requires a positive MaxBytes reservation"}
	}
	return nil
}

// idleProofRefusal keeps the public refusal of an unproved reservation change.
func idleProofRefusal(reason string, cause error) error {
	return &Refusal{Code: xfercontract.CodeObjectConflict, Detail: "reservation change requires an authoritative idle proof: " + reason, cause: cause}
}

// idleProof is the authoritative half of a shrink, checked under the bucket
// serializer immediately before the UPDATE: a fresh INFO whose stored bytes fit
// the new reservation AND the owner's own census naming no live, sleeping,
// pending-grant or cleanup responsibility for the bucket. Stored bytes alone
// are not enough: an admitted upload 600 MiB into a 1.4 GiB object looks like
// "600 MiB used" and would then hit DiscardNew at the new ceiling mid-stream.
// origin: hot-update S7 plan §2.1 / lead decision on authoritative shrink proof
func (c *coordinator) idleProof(latest *jetstream.StreamInfo, maxBytes int64, bucket string, deadline controlbudget.Deadline) error {
	if c.census == nil {
		return idleProofRefusal("no live-transfer census is installed", nil)
	}
	if maxBytes <= 0 || latest.State.Bytes > uint64(maxBytes) {
		return idleProofRefusal("stored bytes exceed the new reservation", nil)
	}
	live, err := c.census.Responsible(bucket, deadline)
	if err != nil {
		return idleProofRefusal("live-transfer census is unavailable", err)
	}
	if live != 0 {
		return idleProofRefusal("live transfer responsibility remains", nil)
	}
	return nil
}

// LedgerResponsibility counts every unsettled continuation row (live,
// sleeping, pending-grant, cleanup) of bucket in the owner's ledger census. A
// missing ledger, a scan error or an incomplete census is an error: only
// Ready+Complete evidence can contribute the idle half of a shrink proof.
// origin: hot-update S7 plan §2.1 / lead decision on authoritative shrink proof
func LedgerResponsibility(ledger xfercontract.LedgerPort, bucket string, deadline controlbudget.Deadline) (int, error) {
	if ledger == nil {
		return 0, &Refusal{Code: xfercontract.CodeOwnerRecovering, Detail: "transfer ledger is not installed"}
	}
	census, err := ledger.Scan(deadline)
	if err != nil {
		return 0, err
	}
	if !census.Ready || !census.Complete {
		return 0, &Refusal{Code: xfercontract.CodeOwnerRecovering, Detail: "transfer ledger census is incomplete"}
	}
	live := 0
	for _, row := range census.Rows {
		if proto.XferBucketName(row.Identity.SID) == bucket && row.Phase != xfercontract.PhaseSettled {
			live++
		}
	}
	return live, nil
}

// ShrinkVia routes a legacy reservation shrink to the leader's local owner.
// The forwarding envelope carries intent patches only and a follower holds no
// census of the leader's responsibility, so a non-leader never shrinks; an
// owner that cannot prove idleness never receives the reduction as an Apply.
// origin: hot-update S7 plan §2.1 / lead decision on authoritative shrink proof
func ShrinkVia(local Coordinator, leader bool, patch xfercontract.ConfigPatch, deadline controlbudget.Deadline) (xfercontract.BucketSeal, error) {
	if !leader {
		return xfercontract.BucketSeal{}, &Refusal{Code: xfercontract.CodeOwnerRecovering, Detail: "reservation shrink requires the leader configuration owner"}
	}
	shrinker, ok := local.(Shrinker)
	if !ok {
		return xfercontract.BucketSeal{}, &Refusal{Code: xfercontract.CodeOwnerRecovering, Detail: "leader configuration authority is not installed"}
	}
	return shrinker.Shrink(patch, deadline)
}

func (c *coordinator) configure(ctx context.Context, w *bucketWriter, in configIntent, deadline controlbudget.Deadline) (xfercontract.BucketSeal, error) {
	patch := in.patch
	name := "OBJ_" + patch.Bucket
	stream, err := c.cfg.JS.Stream(ctx, name)
	if errors.Is(err, jetstream.ErrStreamNotFound) && in.shrink {
		return xfercontract.BucketSeal{}, idleProofRefusal("the bucket does not exist", nil)
	}
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		maxBytes, sizeErr := c.createSize(ctx, patch)
		if sizeErr != nil {
			return xfercontract.BucketSeal{}, sizeErr
		}
		err = c.mutate(w, in, "", func() error {
			_, createErr := c.cfg.JS.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: patch.Bucket, MaxBytes: maxBytes, Replicas: patch.Replicas, Storage: jetstream.FileStorage})
			return createErr
		})
		if err != nil && !errors.Is(err, jetstream.ErrBucketExists) && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			return xfercontract.BucketSeal{}, err
		}
		stream, err = c.cfg.JS.Stream(ctx, name)
	}
	// Only a definitive not-found reaches CREATE. Timeouts, no responders and
	// incomplete INFO preserve the old owner instead of creating another bucket.
	// origin: hot-update S7 plan §2.1 / T05
	if err != nil {
		return xfercontract.BucketSeal{}, fmt.Errorf("xferbucket lookup: %w", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return xfercontract.BucketSeal{}, err
	}
	expected, err := copyStreamConfig(info.Config)
	if err != nil {
		return xfercontract.BucketSeal{}, err
	}
	// Only tether's validated xfer namespace reaches this owner. Its finite
	// reservation must refuse new writes instead of evicting ACKed chunks;
	// migrate DiscardOld in the same fenced UPDATE that raises Duplicates.
	// The pinned SDK already creates DiscardNew stores, but the backing config
	// is authoritative for both CREATE readback and existing deployments.
	// An N-1 convergence may reset Discard to its default. The capable writer
	// then refuses fail-closed until this owner re-applies; never change TTL or
	// MaxBytes to repair that refusal, and never fall back to legacy.
	// origin: hot-update S7 plan §2.1 / LQ3; S7 internal review FIX-CONVERGE F1
	expected.Discard = jetstream.DiscardNew
	if err := checkRetention(expected, patch.RecoveryHorizon); err != nil {
		return xfercontract.BucketSeal{}, err
	}
	expected.Replicas = max(expected.Replicas, patch.Replicas)
	expected.Duplicates = max(expected.Duplicates, patch.Duplicates)
	// An ensure target is not permission to normalize an operator's MaxBytes.
	// The frozen patch has no live-transfer/idle proof, so it cannot authorize
	// shrinking even an empty stream: an admitted upload may still be sending.
	// origin: hot-update S7 plan §2.1 / T05
	//
	// Only an explicit Shrink changes MaxBytes, never upward (growth is an
	// operator decision) and only under the idle proof checked before UPDATE.
	// origin: hot-update S7 plan §2.1 / lead decision on authoritative shrink proof
	if in.shrink && patch.MaxBytes > info.Config.MaxBytes {
		return xfercontract.BucketSeal{}, idleProofRefusal("a reservation can only be reduced", nil)
	}
	if in.shrink {
		expected.MaxBytes = patch.MaxBytes
	}
	want, err := configDigest(expected)
	if err != nil {
		return xfercontract.BucketSeal{}, err
	}
	before, err := configDigest(info.Config)
	if err != nil {
		return xfercontract.BucketSeal{}, err
	}
	if want != before {
		latest, err := stream.Info(ctx)
		if err != nil {
			return xfercontract.BucketSeal{}, err
		}
		latestDigest, err := configDigest(latest.Config)
		if err != nil {
			return xfercontract.BucketSeal{}, err
		}
		if latestDigest != before || !latest.Created.Equal(info.Created) {
			return xfercontract.BucketSeal{}, &Refusal{Code: xfercontract.CodeObjectConflict, Detail: "stream configuration changed before update"}
		}
		if expected.MaxBytes < latest.Config.MaxBytes {
			if err := c.idleProof(latest, expected.MaxBytes, patch.Bucket, deadline); err != nil {
				return xfercontract.BucketSeal{}, err
			}
		}
		err = c.mutate(w, in, want, func() error {
			_, updateErr := c.cfg.JS.UpdateStream(ctx, expected)
			return updateErr
		})
		if err != nil {
			var apiErr *jetstream.APIError
			if errors.As(err, &apiErr) && strings.Contains(strings.ToLower(apiErr.Description), "duplicate") {
				return xfercontract.BucketSeal{}, storageHorizonError(patch.Duplicates, apiErr.Description)
			}
			return xfercontract.BucketSeal{}, err
		}
	}
	// Even CREATE must be followed by full INFO -> UPDATE -> INFO: the SDK's
	// initial object-store config omits Duplicates. No seal precedes readback.
	// origin: hot-update S7 plan §2.1 / T05
	confirmed, err := stream.Info(ctx)
	if err != nil {
		return xfercontract.BucketSeal{}, err
	}
	got, err := configDigest(confirmed.Config)
	if err != nil {
		return xfercontract.BucketSeal{}, err
	}
	if got != want || !confirmed.Created.Equal(info.Created) {
		return xfercontract.BucketSeal{}, &Refusal{Code: xfercontract.CodeObjectConflict, Detail: "full configuration readback differs from expected configuration"}
	}
	if confirmed.Config.Duplicates < patch.RecoveryHorizon {
		return xfercontract.BucketSeal{}, storageHorizonError(patch.RecoveryHorizon, "duplicate window readback is insufficient")
	}
	if !w.resolve(got) {
		return xfercontract.BucketSeal{}, &Refusal{Code: xfercontract.CodeResultUnknown, Detail: "prior configuration request has no definite response"}
	}
	seal := xfercontract.BucketSeal{
		Bucket: patch.Bucket, Stream: name, StreamCreated: confirmed.Created.UTC().Format(time.RFC3339Nano), ConfigDigest: got,
		Replicas: confirmed.Config.Replicas, MaxBytes: confirmed.Config.MaxBytes,
		Duplicates: confirmed.Config.Duplicates, MaxAge: confirmed.Config.MaxAge, Proof: w.proof,
		MaxMsgSize: int64(confirmed.Config.MaxMsgSize),
	}
	// A handle without a connection yields MaxPayload 0, which first-byte
	// admission refuses (fail closed) instead of a nil dereference in the owner.
	if conn := c.cfg.JS.Conn(); conn != nil {
		seal.MaxPayload = conn.MaxPayload()
	}
	if c.cfg.Limits.MaxPayload > 0 {
		seal.MaxPayload = min(seal.MaxPayload, c.cfg.Limits.MaxPayload)
	}
	if c.cfg.Limits.MaxMsgSize > 0 && (seal.MaxMsgSize <= 0 || seal.MaxMsgSize > c.cfg.Limits.MaxMsgSize) {
		seal.MaxMsgSize = c.cfg.Limits.MaxMsgSize
	}
	if w.attempt != nil {
		copy := seal
		w.attempt.Seal = &copy
	}
	return seal, nil
}

func (c *coordinator) createSize(ctx context.Context, patch xfercontract.ConfigPatch) (int64, error) {
	size := int64(bucketCap)
	var reserved uint64
	var accountMax int64
	if patch.MaxBytes > 0 {
		// A caller-sized patch already ran the legacy disk/account/server sizing
		// on its own short deadline (G67 #67 face A). A second AccountInfo probe
		// here would spend the load-bearing CREATE's budget on a best-effort
		// question: a stalled meta leader then starves the CREATE, and its
		// timeout is reported as a JetStream verdict. Configured limits below
		// still clamp it; the reservation term is the caller's sizing.
		// origin: hot-update S7 plan §2.1 / lead decision on G67 replay
		size = min(size, patch.MaxBytes)
	} else {
		account, err := c.cfg.JS.AccountInfo(ctx)
		if err != nil {
			return 0, err
		}
		// Account reservation is replica-weighted. Server/disk facts are not;
		// subtracting the weighted total there is conservative when placement is
		// unknown. Unknown/unlimited ceilings never yield an unlimited bucket.
		// origin: hot-update S7 plan §2.1 / T05
		reserved = max(account.ReservedStore, account.Store)
		accountMax = account.Limits.MaxStore
	}
	for _, limit := range []struct {
		ceiling int64
		factor  int64
	}{
		{accountMax, int64(patch.Replicas)},
		{c.cfg.Limits.AccountMaxBytes, int64(patch.Replicas)},
		{c.cfg.Limits.ServerMaxBytes, 1},
		{c.cfg.Limits.DiskBytes, 1},
	} {
		if limit.ceiling <= 0 {
			continue
		}
		if reserved >= uint64(limit.ceiling) {
			return 0, &Refusal{Code: xfercontract.CodeCapacityExceeded, Detail: "no store reservation remains for tier-B"}
		}
		available := (limit.ceiling - int64(reserved)) / limit.factor
		if available < bucketFloor {
			return 0, &Refusal{Code: xfercontract.CodeCapacityExceeded, Detail: "store cannot reserve the tier-B floor"}
		}
		size = min(size, max(int64(bucketFloor), available/4*3))
	}
	if size < bucketFloor {
		return 0, &Refusal{Code: xfercontract.CodeCapacityExceeded, Detail: "requested reservation is below the tier-B floor"}
	}
	return size, nil
}

// configJS keeps legacy SDK-shaped callers on the same intent-only port as v1.
// Embedding preserves data IO; the config overrides never submit a snapshot.
// The raw JS used to construct the coordinator must remain private to that owner.
// origin: hot-update S7 plan §2.1 / T05 / M14
type configJS struct {
	jetstream.JetStream
	port Coordinator
}

// Bind connects a JS-only legacy caller (including replica observation) to the
// already-installed leader router. It never creates another config authority.
func Bind(js jetstream.JetStream, port Coordinator) jetstream.JetStream {
	if bound, ok := js.(*configJS); ok {
		js = bound.JetStream
	}
	return &configJS{JetStream: js, port: port}
}

// Guard refuses unbound legacy writers. A raw JS handle is not retirement proof
// and must never mint an independent writer when the current owner is recovering.
func Guard(js jetstream.JetStream) jetstream.JetStream {
	if _, ok := js.(*configJS); ok {
		return js
	}
	return Bind(js, nil)
}

func (j *configJS) apply(ctx context.Context, p xfercontract.ConfigPatch) error {
	deadline, err := j.deadline(ctx)
	if err != nil {
		return err
	}
	_, err = j.port.Apply(p, deadline)
	return err
}

func (j *configJS) deadline(ctx context.Context) (controlbudget.Deadline, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if j.port == nil {
		return 0, &Refusal{Code: xfercontract.CodeOwnerRecovering, Detail: "bucket coordinator is not installed"}
	}
	budget := configRequestBudget
	if end, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(end))
	}
	return controlbudget.After(budget)
}

// resize routes a legacy reservation change to the owner's proof-gated Shrink.
// Growth or an unlimited (<=0) reservation is never a configuration-owner
// field, and a port that cannot prove idleness keeps the typed refusal.
// origin: hot-update S7 plan §2.1 / lead decision on authoritative shrink proof
func (j *configJS) resize(ctx context.Context, current int64, p xfercontract.ConfigPatch) error {
	if p.MaxBytes <= 0 || p.MaxBytes > current {
		return idleProofRefusal("a reservation can only be reduced", nil)
	}
	deadline, err := j.deadline(ctx)
	if err != nil {
		return err
	}
	shrinker, ok := j.port.(Shrinker)
	if !ok {
		return idleProofRefusal("the configuration owner cannot prove idleness", nil)
	}
	_, err = shrinker.Shrink(p, deadline)
	return err
}

func (j *configJS) CreateObjectStore(ctx context.Context, cfg jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
	if !strings.HasPrefix(cfg.Bucket, "xfer-") {
		return j.JetStream.CreateObjectStore(ctx, cfg)
	}
	if err := j.apply(ctx, xfercontract.ConfigPatch{Bucket: cfg.Bucket, Replicas: cfg.Replicas, MaxBytes: cfg.MaxBytes}); err != nil {
		return nil, err
	}
	return j.ObjectStore(ctx, cfg.Bucket)
}
func (j *configJS) UpdateObjectStore(ctx context.Context, cfg jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
	if !strings.HasPrefix(cfg.Bucket, "xfer-") {
		return j.JetStream.UpdateObjectStore(ctx, cfg)
	}
	// An SDK object projection cannot express the full operator config. Only the
	// owned replica intent may cross this boundary; shrinking needs a live proof.
	stream, err := j.Stream(ctx, "OBJ_"+cfg.Bucket)
	if err != nil {
		return nil, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, err
	}
	patch := xfercontract.ConfigPatch{Bucket: cfg.Bucket, Replicas: cfg.Replicas}
	if cfg.MaxBytes != info.Config.MaxBytes {
		patch.MaxBytes = cfg.MaxBytes
		err = j.resize(ctx, info.Config.MaxBytes, patch)
	} else {
		err = j.apply(ctx, patch)
	}
	if err != nil {
		return nil, err
	}
	return j.ObjectStore(ctx, cfg.Bucket)
}
func (j *configJS) CreateOrUpdateObjectStore(ctx context.Context, cfg jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
	if !strings.HasPrefix(cfg.Bucket, "xfer-") {
		return j.JetStream.CreateOrUpdateObjectStore(ctx, cfg)
	}
	// The coordinator resolves before creating; do not infer absence locally.
	return j.CreateObjectStore(ctx, cfg)
}
func (j *configJS) UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	if !strings.HasPrefix(cfg.Name, "OBJ_xfer-") {
		return j.JetStream.UpdateStream(ctx, cfg)
	}
	stream, err := j.Stream(ctx, cfg.Name)
	if err != nil {
		return nil, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, err
	}
	patch := xfercontract.ConfigPatch{Bucket: strings.TrimPrefix(cfg.Name, "OBJ_"), Replicas: cfg.Replicas, Duplicates: cfg.Duplicates}
	if cfg.MaxBytes != info.Config.MaxBytes {
		patch.MaxBytes = cfg.MaxBytes
		err = j.resize(ctx, info.Config.MaxBytes, patch)
	} else {
		err = j.apply(ctx, patch)
	}
	if err != nil {
		return nil, err
	}
	return j.Stream(ctx, cfg.Name)
}
func (j *configJS) CreateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	if !strings.HasPrefix(cfg.Name, "OBJ_xfer-") {
		return j.JetStream.CreateStream(ctx, cfg)
	}
	if err := j.apply(ctx, xfercontract.ConfigPatch{Bucket: strings.TrimPrefix(cfg.Name, "OBJ_"), Replicas: cfg.Replicas, Duplicates: cfg.Duplicates, MaxBytes: cfg.MaxBytes}); err != nil {
		return nil, err
	}
	return j.Stream(ctx, cfg.Name)
}
func (j *configJS) CreateOrUpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	if !strings.HasPrefix(cfg.Name, "OBJ_xfer-") {
		return j.JetStream.CreateOrUpdateStream(ctx, cfg)
	}
	return j.CreateStream(ctx, cfg)
}

// Pure object format and checkpoint predicates share the bucket admission
// policy without acquiring a writer, connection or cleanup owner. The public
// jsstream entry points delegate here, keeping its lifecycle API stable.
// origin: hot-update S7 plan §2.1–§2.3 / S7 internal review FIX-ARCH F2
func ValidateObjectSpec(s xfercontract.ObjectSpec) error {
	sha, err := hex.DecodeString(s.SHA256)
	_, dateErr := time.Parse(time.RFC3339Nano, s.StreamCreated)
	if !ObjectBucketToken(s.Bucket) || !ObjectChunkToken(s.Identity.ObjectVersion) || len(s.Identity.ObjectVersion) > 256 ||
		s.Stream != "OBJ_"+s.Bucket || dateErr != nil || s.Identity.TransferID == "" ||
		s.Identity.EntryNonce == "" || len(s.Identity.TransferID) > 256 ||
		s.Identity.IntentDigest == "" || len(s.Identity.IntentDigest) > 256 ||
		strings.ContainsAny(s.Identity.IntentDigest, "\r\n") || err != nil || len(sha) != sha256.Size ||
		s.SHA256 != strings.ToLower(s.SHA256) || s.Size < 0 || s.Size > 2<<30 ||
		s.ChunkSize < 32<<10 || s.ChunkSize > 1<<20 || s.Chunks > 65536 {
		return fmt.Errorf("%s: invalid fixed object specification", xfercontract.CodePayloadUnsupported)
	}
	if uint64(s.Chunks) != (uint64(s.Size)+uint64(s.ChunkSize)-1)/uint64(s.ChunkSize) {
		return fmt.Errorf("%s: size and chunk geometry disagree", xfercontract.CodePayloadUnsupported)
	}
	return nil
}

func CheckWriterStream(state xfercontract.WriterState, info *jetstream.StreamInfo) error {
	s := state.ObjectSpec
	created, _ := time.Parse(time.RFC3339Nano, s.StreamCreated)
	floor := uint64(0)
	if n := len(state.Receipts); n != 0 {
		floor = state.Receipts[n-1].StreamSeq
	}
	if p := state.PendingAttempt; p != nil {
		floor = max(floor, p.ExpectedStreamSeq)
	}
	if m := state.MetaReceipt; m != nil {
		floor = max(floor, m.MetadataSeq)
	}
	if fence := state.RetirementFence.Receipt; fence != nil {
		floor = max(floor, fence.Sequence)
	}
	if info == nil || info.Config.Name != s.Stream || !info.Created.Equal(created) || info.State.LastSeq < floor ||
		info.Config.Storage != jetstream.FileStorage || info.Config.NoAck || info.Config.Mirror != nil || len(info.Config.Sources) != 0 {
		return fmt.Errorf("%s: stream incarnation changed", xfercontract.CodeObjectVersionMismatch)
	}
	// Recheck the same retention admission used to issue the bucket seal on
	// this actual incarnation, before first publish and after reconnect/import.
	// Duplicates is deliberately not a correctness fence here: the independent
	// default-window tests exercise the writer's CAS protocol across 120s.
	// origin: hot-update S7 plan §2.1 / LQ3
	// origin: S7 internal review rv-jsnats F2
	horizon := proto.XferTierBMaxBudget + 120*time.Second
	if err := CheckTransferRetention(info.Config, horizon); err != nil {
		return fmt.Errorf("%s: requires Duplicates >= %s and MaxAge = 0 or MaxAge >= %s: %w", xfercontract.CodeStorageHorizon, horizon, horizon, err)
	}
	return nil
}

// CheckTransferRetention checks the storage policy shared by bucket admission
// and the fixed-version writer. It returns a diagnostic reason; the boundary
// attaches transfer_storage_horizon and its required Duplicates/MaxAge values.
// No operator fields are rewritten. Without a complete version-capacity proof,
// finite message limits are refused, including DiscardNew's per-subject limit:
// the server still evicts old subject messages unless DiscardNewPerSubject is set.
// origin: hot-update S7 plan §2.1 / LQ3
// origin: S7 internal review rv-jsnats F2
func CheckTransferRetention(cfg jetstream.StreamConfig, horizon time.Duration) error {
	switch {
	case cfg.MaxAge != 0 && cfg.MaxAge < horizon:
		return errors.New("operator MaxAge is shorter than the recovery horizon")
	case cfg.Retention != jetstream.LimitsPolicy:
		return errors.New("object recovery requires LimitsPolicy retention")
	case cfg.MaxMsgs > 0 || cfg.MaxMsgsPerSubject > 0:
		return errors.New("finite MaxMsgs or MaxMsgsPerSubject lacks a complete version-capacity proof")
	case cfg.Discard != jetstream.DiscardNew && (cfg.Discard != jetstream.DiscardOld || cfg.MaxBytes > 0):
		return errors.New("finite MaxBytes requires DiscardNew to preserve acknowledged chunks")
	}
	return nil
}

func CloneObjectReceipt(r xfercontract.ObjectReceipt) xfercontract.ObjectReceipt {
	r.Metadata = bytes.Clone(r.Metadata)
	return r
}

func CloneWriterState(s xfercontract.WriterState) xfercontract.WriterState {
	s.Receipts = append(make([]xfercontract.ChunkReceipt, 0, s.ObjectSpec.Chunks), s.Receipts...)
	if s.PendingAttempt != nil {
		p := *s.PendingAttempt
		p.Bytes = bytes.Clone(p.Bytes)
		s.PendingAttempt = &p
	}
	if s.MetaReceipt != nil {
		r := CloneObjectReceipt(*s.MetaReceipt)
		s.MetaReceipt = &r
	}
	if s.RetirementFence.Receipt != nil {
		r := *s.RetirementFence.Receipt
		s.RetirementFence.Receipt = &r
	}
	return s
}

func ValidateWriterState(s xfercontract.WriterState) error {
	if err := ValidateObjectSpec(s.ObjectSpec); err != nil {
		return err
	}
	bad := func() error { return fmt.Errorf("%s: invalid writer checkpoint", xfercontract.CodeObjectConflict) }
	if s.AttachmentGeneration == 0 || s.AttachmentGeneration == math.MaxUint64 || len(s.Receipts) > int(s.ObjectSpec.Chunks) {
		return bad()
	}
	previous := uint64(0)
	for index, r := range s.Receipts {
		if r.Index != uint32(index) || r.Offset != int64(index)*int64(s.ObjectSpec.ChunkSize) ||
			r.Length != ChunkLength(s.ObjectSpec, r.Index) || r.StreamSeq <= previous {
			return bad()
		}
		previous = r.StreamSeq
	}
	if p := s.PendingAttempt; p != nil {
		if p.Index != uint32(len(s.Receipts)) || p.Index >= s.ObjectSpec.Chunks ||
			p.Offset != int64(p.Index)*int64(s.ObjectSpec.ChunkSize) || p.Length != ChunkLength(s.ObjectSpec, p.Index) ||
			len(p.Bytes) != int(p.Length) || sha256.Sum256(p.Bytes) != p.SHA256 || p.PreviousSubjectSeq != previous ||
			p.ExpectedStreamSeq < previous || p.AttachmentGeneration == 0 || p.AttachmentGeneration > s.AttachmentGeneration {
			return bad()
		}
	}
	if r := s.MetaReceipt; r != nil {
		if r.Spec != s.ObjectSpec || s.PendingAttempt != nil || len(s.Receipts) != int(s.ObjectSpec.Chunks) ||
			!bytes.Equal(r.Metadata, ObjectMetadata(s.ObjectSpec, false)) {
			return bad()
		}
		if r.MetadataSeq != 0 {
			digest := sha256.Sum256(r.Metadata)
			if r.MetadataSeq <= previous || r.MetadataDigest != hex.EncodeToString(digest[:]) {
				return bad()
			}
		}
	}
	switch s.Phase {
	case xfercontract.WriteOpen:
		if s.PendingAttempt != nil || s.MetaReceipt != nil {
			return bad()
		}
	case xfercontract.WritePending:
		if s.PendingAttempt == nil || s.MetaReceipt != nil {
			return bad()
		}
	case xfercontract.WriteMetadataPending:
		if s.MetaReceipt == nil || s.MetaReceipt.MetadataSeq != 0 {
			return bad()
		}
	case xfercontract.WriteFinalized:
		if s.MetaReceipt == nil || s.MetaReceipt.MetadataSeq == 0 {
			return bad()
		}
	case xfercontract.WriteRetiring, xfercontract.WriteRetired:
		if !s.RetirementFence.WriterRetired {
			return bad()
		}
	default:
		return bad()
	}
	return nil
}

// CheckChunkHeaders keeps reader delivery and writer raw readback on one
// fixed-version header inventory; CAS and MsgID belong only to publication.
// origin: S7 internal review TRANSFER-MAKETEST-2 F2 / S7 plan §2.2 / §3
func CheckChunkHeaders(s xfercontract.ObjectSpec, receipt xfercontract.ChunkReceipt, header nats.Header) error {
	p := xfercontract.PendingAttempt{Index: receipt.Index, Offset: receipt.Offset, Length: receipt.Length, SHA256: receipt.SHA256}
	want := ChunkMessage(s, p)
	for key, values := range want.Header {
		if !strings.HasPrefix(key, "Tether-Xfer-") {
			continue
		}
		if got := header.Values(key); len(got) != 1 || got[0] != values[0] {
			return errors.New("transfer_object_conflict: chunk headers differ from sealed version")
		}
	}
	return nil
}
