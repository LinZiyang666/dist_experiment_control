package xferbucket

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

var _ Factory = New

const configRequestBudget = 5 * time.Second
const configBucketCapacity = 1024
const configAttemptCapacity = 64

type bucketWriter struct {
	gate    chan struct{}
	proof   xfercontract.ConfigWriterProof
	attempt *xfercontract.ConfigAttempt
	unknown []*unresolvedAttempt
}

// configIntent is the desired configuration a request serves. shrink marks
// patch.MaxBytes as an explicit, proof-gated new reservation; in an ensure the
// same field is only a CREATE sizing hint and never shrinks an existing bucket.
type configIntent struct {
	patch  xfercontract.ConfigPatch
	shrink bool
}

// unresolvedAttempt is the private replay key of a sent-unknown request. The
// public ConfigAttempt keeps SentUnknown set until it is resolved, so handoff
// and successor authority refuse exactly as before.
//
// Every unresolved send retains its own ConfigAttempt; a retry response cannot
// answer for an original request still queued at the server.
// origin: S7 internal review rv-jsnats F1
//
// update is the full-config digest an unknown UPDATE asked for. A CREATE leaves
// it empty: JetStream never lets CREATE modify an existing stream (an identical
// config is a no-op, any other is "name already in use"), so once a definite
// answer proves the stream exists, a late copy of that CREATE is inert.
// origin: hot-update S7 plan §2.1 / T05 / lead decision on G67 replay
type unresolvedAttempt struct {
	intent  configIntent
	proof   xfercontract.ConfigWriterProof
	update  string
	attempt *xfercontract.ConfigAttempt
}

// Census is the configuration owner's own live-transfer responsibility view.
// Responsible counts every entry that can still write, read or clean up objects
// in the bucket: live, sleeping (imported, not yet resumed), pending-grant and
// cleanup-pending. An unavailable or incomplete census must return an error,
// never zero: absence of evidence is not an idle proof.
// origin: hot-update S7 plan §2.1 / lead decision on authoritative shrink proof
type Census interface {
	Responsible(bucket string, deadline controlbudget.Deadline) (int, error)
}

// Shrinker is the explicit reservation-shrink request. A ConfigPatch sent to
// Apply cannot express it: an ensure target is not permission to normalize an
// operator's MaxBytes. Shrink treats patch.MaxBytes as the exact new positive
// reservation and applies it only under an authoritative idle proof.
// origin: hot-update S7 plan §2.1 / lead decision on authoritative shrink proof
type Shrinker interface {
	Shrink(xfercontract.ConfigPatch, controlbudget.Deadline) (xfercontract.BucketSeal, error)
}

// The registry fence excludes new Apply calls before retirement starts. Each
// bucket has its own serializer; no network request runs under the registry lock.
// origin: hot-update S7 plan §2.1 / T05
type coordinator struct {
	cfg      Config
	census   Census
	bootID   string
	mu       sync.Mutex
	closed   bool
	freezing bool
	retired  bool
	inflight uint32
	changed  chan struct{}
	buckets  map[string]*bucketWriter
}

var _ Shrinker = (*coordinator)(nil)

// New constructs the sole local config writer. Cluster adapters must supply
// authority belonging to the current leader, backed by the previous writer's
// retirement proof; observing the new leader or an equal INFO is insufficient.
// Without a census no reservation shrink can ever be proved.
// origin: hot-update S7 plan §2.1 / T05
func New(cfg Config) (Coordinator, error) { return NewWithCensus(cfg, nil) }

// NewWithCensus is New with the owner's own live-transfer census, the only
// source of the idle half of a shrink proof. The census must observe the same
// responsibility set the owner admits into; a caller's claim is never a proof.
// origin: hot-update S7 plan §2.1 / lead decision on authoritative shrink proof
func NewWithCensus(cfg Config, census Census) (Coordinator, error) {
	if cfg.JS == nil || cfg.Authority == nil || cfg.Clock == nil {
		return nil, errors.New("xferbucket: JetStream, authority and clock are required")
	}
	bootID, now, err := cfg.Clock()
	if err != nil {
		return nil, fmt.Errorf("xferbucket clock: %w", err)
	}
	if bootID == "" || now <= 0 {
		return nil, errors.New("xferbucket: invalid local clock identity")
	}
	return &coordinator{cfg: cfg, census: census, bootID: bootID, changed: make(chan struct{}), buckets: make(map[string]*bucketWriter)}, nil
}

func (c *coordinator) requestContext(deadline controlbudget.Deadline) (context.Context, context.CancelFunc, controlbudget.Deadline, error) {
	bootID, now, err := c.cfg.Clock()
	if err != nil {
		return nil, nil, 0, err
	}
	if bootID != c.bootID || now <= 0 || deadline <= 0 {
		return nil, nil, 0, controlbudget.ErrInvalidDeadline
	}
	if now >= int64(deadline) {
		return nil, nil, 0, controlbudget.ErrExpired
	}
	remaining := min(time.Duration(int64(deadline)-now), configRequestBudget)
	// ctx-none: synchronous request scope; Apply/Freeze cancel and join all IO before returning.
	ctx, cancel := context.WithTimeout(context.Background(), remaining)
	return ctx, cancel, controlbudget.Deadline(now + int64(remaining)), nil
}

func (c *coordinator) signalLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *coordinator) enter(bucket string) (*bucketWriter, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, &Refusal{Code: xfercontract.CodeOwnerRecovering, Detail: "configuration admission is closed"}
	}
	w := c.buckets[bucket]
	if w == nil {
		if len(c.buckets) >= configBucketCapacity {
			return nil, &Refusal{Code: xfercontract.CodeCapacityExceeded, Detail: "configuration bucket capacity reached"}
		}
		w = &bucketWriter{gate: make(chan struct{}, 1)}
		c.buckets[bucket] = w
	}
	c.inflight++
	return w, nil
}

func (c *coordinator) leave() {
	c.mu.Lock()
	c.inflight--
	c.signalLocked()
	c.mu.Unlock()
}

func (c *coordinator) Apply(patch xfercontract.ConfigPatch, deadline controlbudget.Deadline) (xfercontract.BucketSeal, error) {
	return c.run(configIntent{patch: patch}, deadline)
}

// Shrink applies patch.MaxBytes as the exact new reservation of an existing
// bucket. It shares Apply's serializer, writer proof and sent-unknown rules.
// origin: hot-update S7 plan §2.1 / lead decision on authoritative shrink proof
func (c *coordinator) Shrink(patch xfercontract.ConfigPatch, deadline controlbudget.Deadline) (xfercontract.BucketSeal, error) {
	if patch.MaxBytes <= 0 {
		// MaxBytes<=0 is JetStream's unlimited form: never a shrink target.
		return xfercontract.BucketSeal{}, &Refusal{Code: xfercontract.CodeCapacityExceeded, Detail: "bucket requires a positive MaxBytes reservation"}
	}
	return c.run(configIntent{patch: patch, shrink: true}, deadline)
}

func (c *coordinator) run(in configIntent, deadline controlbudget.Deadline) (xfercontract.BucketSeal, error) {
	patch, err := normalizePatch(in.patch, c.cfg.Limits)
	if err != nil {
		return xfercontract.BucketSeal{}, err
	}
	in.patch = patch
	ctx, cancel, bounded, err := c.requestContext(deadline)
	if err != nil {
		return xfercontract.BucketSeal{}, err
	}
	defer cancel()
	w, err := c.enter(patch.Bucket)
	if err != nil {
		return xfercontract.BucketSeal{}, err
	}
	defer c.leave()
	select {
	case w.gate <- struct{}{}:
		defer func() { <-w.gate }()
	case <-ctx.Done():
		return xfercontract.BucketSeal{}, ctx.Err()
	}
	// An equal readback cannot join a full-config request still in transit. Do
	// not retire it or allow a later intent to be overwritten by its old snapshot.
	// origin: hot-update S7 plan §2.1 / T05
	//
	// The block exists to stop a SECOND writer, or a different desired config,
	// while the first request is unresolved. The same writer generation retrying
	// the same idempotent ensure after a timeout is admitted (G67: every started
	// attempt gets a full budget and transient CREATE timeouts are retried within
	// budget). A definite answer plus matching full readback resolves CREATE
	// existence only; an original UPDATE stays unresolved even after retry ACK.
	// origin: hot-update S7 plan §2.1 / lead decision on G67 replay
	for _, u := range w.unknown {
		if u.proof != w.proof || u.intent != in {
			return xfercontract.BucketSeal{}, &Refusal{Code: xfercontract.CodeResultUnknown, Detail: "prior configuration request has no definite response"}
		}
	}
	if w.proof.Nonce == "" {
		proof, err := c.cfg.Authority.Acquire(patch.Bucket, bounded)
		if err != nil {
			return xfercontract.BucketSeal{}, err
		}
		if !validWriterProof(proof, patch.Bucket) {
			return xfercontract.BucketSeal{}, &Refusal{Code: xfercontract.CodeResultUnknown, Detail: "invalid configuration ownership proof"}
		}
		w.proof = proof
	}
	if err := ctx.Err(); err != nil {
		return xfercontract.BucketSeal{}, err
	}
	return c.configure(ctx, w, in, bounded)
}

func validWriterProof(p xfercontract.ConfigWriterProof, bucket string) bool {
	return p.Bucket == bucket && p.BrokerID != "" && p.InstanceID != "" && p.BootID != "" && p.Nonce != "" && p.Generation != 0 && !p.GateClosed && !p.RequestsJoined
}

// mutate sends one CREATE (update == "") or full-config UPDATE (update is the
// digest of the exact config sent).
func (c *coordinator) mutate(w *bucketWriter, in configIntent, update string, call func() error) error {
	// While an UPDATE is unknown it may still land. Only the identical full
	// config may be re-sent: anything else could later be overwritten by the
	// late original, and a CREATE would target a different stream incarnation.
	// origin: hot-update S7 plan §2.1 / lead decision on G67 replay
	for _, u := range w.unknown {
		if u.update != "" && u.update != update {
			return &Refusal{Code: xfercontract.CodeResultUnknown, Detail: "prior configuration request has no definite response"}
		}
	}
	// Bound retained responsibility before sending; never evict an unresolved
	// original to make room for a retry that could time out as well.
	// origin: S7 internal review rv-jsnats F1
	if len(w.unknown) >= configAttemptCapacity {
		return &Refusal{Code: xfercontract.CodeResultUnknown, Detail: "unresolved configuration request capacity reached"}
	}
	// The image carries the actual config request, including its original
	// ordinal and clock, not a zero-valued placeholder that cannot be decoded.
	// origin: hot-update S7 plan §2.1 / §8.2 / T05 / T20
	boot, started, err := c.cfg.Clock()
	if err != nil || boot != c.bootID || started <= 0 {
		return controlbudget.ErrInvalidDeadline
	}
	ordinal := uint32(1)
	if w.attempt != nil {
		ordinal = w.attempt.Attempt.Ordinal + 1
	}
	if ordinal == 0 {
		return errors.New("xferbucket: config attempt ordinal exhausted")
	}
	phase := controlbudget.PhaseRequest
	if update == "" {
		phase = controlbudget.PhaseCreate
	}
	w.attempt = &xfercontract.ConfigAttempt{Patch: in.patch, Proof: w.proof, Attempt: xfercontract.Attempt{
		LogicalID: w.proof.Nonce + "/" + in.patch.Bucket, Ordinal: ordinal, Phase: phase, StartedMonoNS: started, Sent: true,
	}}
	err = call()
	if finishedBoot, finished, clockErr := c.cfg.Clock(); clockErr == nil && finishedBoot == boot && finished >= started {
		w.attempt.Attempt.FinishedMonoNS = finished
	}
	w.attempt.Attempt.Class = "confirmed"
	if err != nil {
		w.attempt.Attempt.Class = "refused"
	}
	var apiErr *jetstream.APIError
	// The SDK exposes no sent flag for UPDATE/CREATE. Conservatively retain
	// every transport failure, even if it might have happened before the send.
	// A parsed server refusal is the only error that proves this request ended.
	// origin: hot-update S7 plan §2.1 / T05
	if err != nil && !errors.As(err, &apiErr) {
		w.unknown = append(w.unknown, &unresolvedAttempt{intent: in, proof: w.proof, update: update, attempt: w.attempt})
		w.attempt.SentUnknown = true
		w.attempt.Attempt.Class = "sent_unknown"
		return &Refusal{Code: xfercontract.CodeResultUnknown, Detail: "configuration request response unknown", cause: err}
	}
	// A definite answer ends THIS request only. An earlier unknown request of the
	// same intent stays unresolved; resolve can discharge only CREATE existence.
	w.attempt.SentUnknown = false
	return err
}

// resolve checks that every earlier UPDATE is compatible with the readback.
// Definite answers and a matching full readback discharge only CREATE: an
// existing stream makes its late duplicate inert. An unknown UPDATE can still
// land after this retry, so matching its exact digest does not join the send.
// nats-server dequeues JetStream API requests in order but runs them on a
// worker pool, so a straggling original can complete after the retry; the
// same-config rule only protects this intent, never a later config or owner.
// origin: hot-update S7 plan §2.1 / lead decision on G67 replay
// origin: S7 internal review rv-jsnats F1
func (w *bucketWriter) resolve(readback string) bool {
	for _, u := range w.unknown {
		if u.update != "" && u.update != readback {
			return false
		}
	}
	updates := w.unknown[:0]
	for _, u := range w.unknown {
		if u.update != "" {
			updates = append(updates, u)
		} else {
			u.attempt.SentUnknown = false
		}
	}
	clear(w.unknown[len(updates):])
	w.unknown = updates
	return true
}

func (c *coordinator) Freeze(deadline controlbudget.Deadline) (xfercontract.ConfigWriterState, error) {
	ctx, cancel, bounded, err := c.requestContext(deadline)
	if err != nil {
		return xfercontract.ConfigWriterState{}, err
	}
	defer cancel()
	c.mu.Lock()
	if c.retired {
		state := c.snapshotLocked()
		c.mu.Unlock()
		return state, nil
	}
	if c.freezing {
		c.mu.Unlock()
		return xfercontract.ConfigWriterState{}, &Refusal{Code: xfercontract.CodeOwnerRecovering, Detail: "configuration retirement already in progress"}
	}
	c.freezing = true
	defer func() {
		c.mu.Lock()
		c.freezing = false
		c.mu.Unlock()
	}()
	c.closed = true
	for c.inflight != 0 {
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			c.mu.Lock()
			c.closed = false // A deferred cut leaves the original writer usable.
			c.mu.Unlock()
			return xfercontract.ConfigWriterState{}, ctx.Err()
		}
		c.mu.Lock()
	}
	state := c.snapshotLocked()
	c.mu.Unlock()
	for _, attempt := range state.Attempts {
		if attempt.SentUnknown {
			c.mu.Lock()
			c.closed = false
			c.mu.Unlock()
			state.Admission.Closed = false
			return state, &Refusal{Code: xfercontract.CodeResultUnknown, Detail: "cannot retire sent-unknown configuration request"}
		}
	}
	for i, proof := range state.Proofs {
		proof.GateClosed, proof.RequestsJoined = true, true
		if err := c.cfg.Authority.Retire(proof, bounded); err != nil {
			// Partial retirement cannot reopen the old gate. Retain the entire
			// proof set for the authority owner to resolve, never fabricate success.
			return state, err
		}
		state.Proofs[i] = proof
		c.mu.Lock()
		c.buckets[proof.Bucket].proof = proof
		c.mu.Unlock()
	}
	c.mu.Lock()
	c.retired = true
	c.mu.Unlock()
	return state, nil
}

func (c *coordinator) snapshotLocked() xfercontract.ConfigWriterState {
	state := xfercontract.ConfigWriterState{Admission: xfercontract.AdmissionState{Closed: c.closed, Inflight: c.inflight}}
	names := make([]string, 0, len(c.buckets))
	for name := range c.buckets {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		w := c.buckets[name]
		if w.proof.Nonce != "" {
			state.Proofs = append(state.Proofs, w.proof)
			state.Admission.Generation = max(state.Admission.Generation, w.proof.Generation)
		}
		// The retained sends are the source of truth: a definite retry answer
		// cannot erase any earlier unknown request from the retirement census.
		// origin: S7 internal review rv-jsnats F1
		latestPending := false
		for _, u := range w.unknown {
			attempt := *u.attempt
			attempt.SentUnknown = true
			state.Attempts = append(state.Attempts, copyConfigAttempt(attempt))
			latestPending = latestPending || u.attempt == w.attempt
		}
		if w.attempt != nil && !latestPending {
			state.Attempts = append(state.Attempts, copyConfigAttempt(*w.attempt))
		}
	}
	return state
}

func copyConfigAttempt(attempt xfercontract.ConfigAttempt) xfercontract.ConfigAttempt {
	if attempt.Seal != nil {
		seal := *attempt.Seal
		attempt.Seal = &seal
	}
	return attempt
}

func (c *coordinator) Stop(deadline controlbudget.Deadline) error {
	_, err := c.Freeze(deadline)
	return err
}

type configAuthority struct {
	mu     sync.Mutex
	owner  xfercontract.ConfigWriterProof
	proofs map[string]xfercontract.ConfigWriterProof
}

// NewAuthority constructs a trusted local ownership handle. Bootstrap starts at
// generation one; a successor requires the previous coordinator's joined,
// closed state. The cluster forwarding owner must authenticate that state and
// install exactly one handle, rather than constructing one per request.
// origin: hot-update S7 plan §2.1 / T05
func NewAuthority(owner xfercontract.ConfigWriterProof, previous *xfercontract.ConfigWriterState) (xfercontract.ConfigAuthority, error) {
	if owner.BrokerID == "" || owner.InstanceID == "" || owner.BootID == "" || owner.Nonce == "" || owner.Generation == 0 || owner.Bucket != "" || owner.GateClosed || owner.RequestsJoined {
		return nil, errors.New("xferbucket: invalid authority identity")
	}
	if previous == nil {
		if owner.Generation != 1 {
			return nil, &Refusal{Code: xfercontract.CodeResultUnknown, Detail: "previous configuration retirement proof is required"}
		}
	} else {
		if !previous.Admission.Closed || previous.Admission.Inflight != 0 || owner.Generation <= previous.Admission.Generation {
			return nil, &Refusal{Code: xfercontract.CodeResultUnknown, Detail: "previous configuration admission is not retired"}
		}
		proofs := make(map[string]xfercontract.ConfigWriterProof)
		for _, proof := range previous.Proofs {
			live := proof
			live.GateClosed, live.RequestsJoined = false, false
			if !validWriterProof(live, proof.Bucket) || !proof.GateClosed || !proof.RequestsJoined || proof.Generation >= owner.Generation {
				return nil, &Refusal{Code: xfercontract.CodeResultUnknown, Detail: "previous configuration requests are not joined"}
			}
			if _, duplicate := proofs[proof.Bucket]; duplicate {
				return nil, errors.New("xferbucket: duplicate configuration retirement proof")
			}
			proofs[proof.Bucket] = proof
		}
		for _, attempt := range previous.Attempts {
			proof, ok := proofs[attempt.Patch.Bucket]
			joined := attempt.Proof
			joined.GateClosed, joined.RequestsJoined = true, true
			if attempt.SentUnknown || !ok || joined != proof {
				return nil, &Refusal{Code: xfercontract.CodeResultUnknown, Detail: "previous configuration attempt is unresolved"}
			}
		}
	}
	return &configAuthority{owner: owner, proofs: make(map[string]xfercontract.ConfigWriterProof)}, nil
}

func (a *configAuthority) Acquire(bucket string, deadline controlbudget.Deadline) (xfercontract.ConfigWriterProof, error) {
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return xfercontract.ConfigWriterProof{}, err
	}
	if !bucketName.MatchString(bucket) {
		return xfercontract.ConfigWriterProof{}, errors.New("xferbucket: invalid authority bucket")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	proof, exists := a.proofs[bucket]
	if exists {
		// A coordinator caches its one acquisition. Returning the same token
		// here would let a second coordinator become a concurrent config writer.
		return xfercontract.ConfigWriterProof{}, &Refusal{Code: xfercontract.CodeOwnerRecovering, Detail: "configuration authority was already acquired"}
	}
	if !exists {
		if len(a.proofs) >= configBucketCapacity {
			return xfercontract.ConfigWriterProof{}, &Refusal{Code: xfercontract.CodeCapacityExceeded, Detail: "configuration authority capacity reached"}
		}
		proof = a.owner
		proof.Bucket = bucket
		a.proofs[bucket] = proof
	}
	return proof, nil
}

func (a *configAuthority) Retire(proof xfercontract.ConfigWriterProof, deadline controlbudget.Deadline) error {
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	current, exists := a.proofs[proof.Bucket]
	current.GateClosed, current.RequestsJoined = true, true
	if !exists || proof != current {
		return &Refusal{Code: xfercontract.CodeResultUnknown, Detail: "retirement does not prove the original configuration writer joined"}
	}
	a.proofs[proof.Bucket] = proof
	return nil
}

// Pure object format and checkpoint predicates share the bucket admission
// policy without acquiring a writer, connection or cleanup owner. The public
// jsstream entry points delegate here, keeping its lifecycle API stable.
// origin: hot-update S7 plan §2.1–§2.3 / S7 internal review FIX-ARCH F2
func ObjectBucketToken(bucket string) bool {
	if len(bucket) == 0 || len(bucket) > 256 {
		return false
	}
	for _, c := range bucket {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return false
		}
	}
	return true
}

func ObjectChunkToken(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// TransferPayloadLimit combines a known connection ceiling with stream limits.
// Callers with several connections pass their minimum MaxPayload, including any
// unknown value. Non-positive stream limits impose no additional restriction;
// they cannot supply missing connection capability evidence.
// origin: S7 internal review rv-jsnats F4
func TransferPayloadLimit(maxPayload int64, streamLimits ...int64) (int64, error) {
	if maxPayload <= 0 {
		return 0, fmt.Errorf("%s: connection payload limit unknown", xfercontract.CodePayloadUnsupported)
	}
	for _, limit := range streamLimits {
		if limit > 0 {
			maxPayload = min(maxPayload, limit)
		}
	}
	return maxPayload, nil
}

// TransferChunkSize subtracts an encoded worst-case header, including both CAS
// fences. The first limit is the known minimum ctl/agent/broker MaxPayload;
// remaining limits are stream ceilings. Zero or negative means an unlimited
// stream field, not an unknown connection capability.
// origin: hot-update S7 plan §2.1 / T09
func TransferChunkSize(spec xfercontract.ObjectSpec, limits ...int64) (uint32, error) {
	if len(limits) == 0 {
		return 0, fmt.Errorf("%s: payload limits missing", xfercontract.CodePayloadUnsupported)
	}
	limit, err := TransferPayloadLimit(limits[0], limits[1:]...)
	if err != nil {
		return 0, err
	}
	p := xfercontract.PendingAttempt{Index: math.MaxUint32, Offset: math.MaxInt64, Length: math.MaxUint32,
		PreviousSubjectSeq: math.MaxUint64, ExpectedStreamSeq: math.MaxUint64}
	m := ChunkMessage(spec, p)
	overhead := int64(m.Size() - len(m.Subject))
	size := min(int64(1<<20), limit-overhead)
	if size < 32<<10 {
		return 0, fmt.Errorf("%s: headers leave less than 32 KiB", xfercontract.CodePayloadUnsupported)
	}
	return uint32(size), nil
}

func CheckWriterPayload(maxPayload int64, info *jetstream.StreamInfo, msg *nats.Msg, spec xfercontract.ObjectSpec) error {
	limit, err := TransferPayloadLimit(maxPayload, int64(info.Config.MaxMsgSize))
	if err != nil {
		return err
	}
	geometry, err := TransferChunkSize(spec, limit)
	if err != nil {
		return err
	}
	n := int64(msg.Size() - len(msg.Subject) - len(msg.Reply))
	if geometry < spec.ChunkSize || n > limit {
		return fmt.Errorf("%s: fixed geometry exceeds current payload limit", xfercontract.CodePayloadUnsupported)
	}
	return nil
}

func MetadataSubject(s xfercontract.ObjectSpec) string {
	return "$O." + s.Bucket + ".M." + base64.URLEncoding.EncodeToString([]byte(s.Identity.TransferID))
}

func ObjectMetadata(s xfercontract.ObjectSpec, deleted bool) []byte {
	digest, _ := hex.DecodeString(s.SHA256)
	info := jetstream.ObjectInfo{ObjectMeta: jetstream.ObjectMeta{Name: s.Identity.TransferID,
		Opts: &jetstream.ObjectMetaOptions{ChunkSize: s.ChunkSize}}, Bucket: s.Bucket, NUID: s.Identity.ObjectVersion,
		Size: uint64(s.Size), Chunks: s.Chunks, Digest: "SHA-256=" + base64.URLEncoding.EncodeToString(digest), Deleted: deleted}
	if deleted {
		info.Size, info.Chunks, info.Digest = 0, 0, ""
	}
	data, _ := json.Marshal(info) // ObjectInfo contains no unsupported JSON values.
	return data
}

func MetadataReceipt(s xfercontract.ObjectSpec, raw *jetstream.RawStreamMsg) xfercontract.ObjectReceipt {
	digest := sha256.Sum256(raw.Data)
	return xfercontract.ObjectReceipt{Spec: s, MetadataSeq: raw.Sequence, MetadataDigest: hex.EncodeToString(digest[:]), Metadata: bytes.Clone(raw.Data)}
}

func MetadataMessage(s xfercontract.ObjectSpec, data []byte, previous, global uint64) *nats.Msg {
	m := nats.NewMsg(MetadataSubject(s))
	m.Data = bytes.Clone(data)
	m.Header.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)
	m.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(previous, 10))
	m.Header.Set(jetstream.ExpectedLastSeqHeader, strconv.FormatUint(global, 10))
	return m
}

func ChunkSubject(s xfercontract.ObjectSpec) string {
	return "$O." + s.Bucket + ".C." + s.Identity.ObjectVersion
}

func ChunkLength(s xfercontract.ObjectSpec, index uint32) uint32 {
	return uint32(min(int64(s.ChunkSize), s.Size-int64(index)*int64(s.ChunkSize)))
}

func PendingReceipt(p xfercontract.PendingAttempt, seq uint64) xfercontract.ChunkReceipt {
	return xfercontract.ChunkReceipt{Index: p.Index, Offset: p.Offset, Length: p.Length, SHA256: p.SHA256, StreamSeq: seq}
}

func ChunkMessage(s xfercontract.ObjectSpec, p xfercontract.PendingAttempt) *nats.Msg {
	m := nats.NewMsg(ChunkSubject(s))
	m.Data = bytes.Clone(p.Bytes)
	m.Header.Set("Tether-Xfer-Version", "1")
	m.Header.Set("Tether-Xfer-Index", strconv.FormatUint(uint64(p.Index), 10))
	m.Header.Set("Tether-Xfer-Offset", strconv.FormatInt(p.Offset, 10))
	m.Header.Set("Tether-Xfer-Length", strconv.FormatUint(uint64(p.Length), 10))
	m.Header.Set("Tether-Xfer-SHA256", hex.EncodeToString(p.SHA256[:]))
	m.Header.Set("Tether-Xfer-Intent", s.Identity.IntentDigest)
	m.Header.Set(jetstream.MsgIDHeader, "xfer:"+s.Identity.ObjectVersion+":"+strconv.FormatUint(uint64(p.Index), 10))
	// Both headers are required even when their value is zero. MsgID alone
	// becomes reusable after the server's deduplication window expires.
	// origin: hot-update S7 plan §2.2 / T02
	m.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(p.PreviousSubjectSeq, 10))
	m.Header.Set(jetstream.ExpectedLastSeqHeader, strconv.FormatUint(p.ExpectedStreamSeq, 10))
	return m
}

func CheckChunk(s xfercontract.ObjectSpec, receipt xfercontract.ChunkReceipt, raw *jetstream.RawStreamMsg) error {
	if raw == nil || raw.Sequence != receipt.StreamSeq || raw.Subject != ChunkSubject(s) ||
		len(raw.Data) != int(receipt.Length) || sha256.Sum256(raw.Data) != receipt.SHA256 {
		return fmt.Errorf("%s: raw chunk receipt mismatch", xfercontract.CodeObjectConflict)
	}
	// Readers validate transfer headers; only a writer can prove this MsgID.
	// origin: S7 internal review TRANSFER-MAKETEST-2 F2 / S7 plan §2.2 / T03
	msgID := raw.Header.Values(jetstream.MsgIDHeader)
	if len(msgID) != 1 || msgID[0] != "xfer:"+s.Identity.ObjectVersion+":"+strconv.FormatUint(uint64(receipt.Index), 10) {
		return fmt.Errorf("%s: raw chunk header mismatch", xfercontract.CodeObjectConflict)
	}
	return CheckChunkHeaders(s, receipt, raw.Header)
}
