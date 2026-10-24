package xfergc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

type ownerView struct {
	rows      map[xfercontract.Key]xfercontract.LedgerRow
	protected map[xfercontract.Key]bool
	versions  map[string]bool
	blocked   map[xfercontract.Key]bool
}

func isTransferStream(name string) bool { return strings.HasPrefix(name, "OBJ_xfer-") }
func transferBucket(name string) string { return strings.TrimPrefix(name, "OBJ_") }
func identityKey(id xfercontract.Identity) xfercontract.Key {
	return xfercontract.Key{SID: id.SID, TransferID: id.TransferID}
}

// The complete local census supplies origin discovery; each remote origin
// then proves its own current responsibility. A rehome, young process, or
// unreachable origin cannot turn an old version into an unowned object.
// origin: hot-update S7 plan §5.3 / T29
func (c *collector) resolveOwners(ctx context.Context, local xfercontract.LedgerCensus, deadline xfercontract.Deadline) (ownerView, error) {
	v := ownerView{rows: make(map[xfercontract.Key]xfercontract.LedgerRow), protected: make(map[xfercontract.Key]bool), versions: make(map[string]bool), blocked: make(map[xfercontract.Key]bool)}
	var result error
	for _, protection := range local.Entries {
		key := identityKey(protection.Identity)
		v.protected[key] = true
		version := protection.ObjectVersion
		if version == "" {
			version = protection.Identity.ObjectVersion
		}
		if version != "" {
			v.versions[protection.Identity.SID+"/"+version] = true
		}
		if key.SID == "" || key.TransferID == "" || (protection.ObjectVersion != "" && protection.Identity.ObjectVersion != "" && protection.ObjectVersion != protection.Identity.ObjectVersion) {
			v.blocked[key] = true
			result = errors.Join(result, errors.New("xfergc: conflicting protection identity"))
		}
	}
	remote := make(map[xfercontract.Origin]xfercontract.LedgerCensus)
	originals := make(map[xfercontract.Key]xfercontract.LedgerRow)
	for _, row := range local.Rows {
		key := identityKey(row.Identity)
		v.protected[key] = true // Protect before every age or remote RPC decision.
		v.versions[row.Identity.SID+"/"+row.Identity.ObjectVersion] = true
		// Compare primary/outbox evidence before refreshing it from the origin.
		// A newer authoritative revision does not make two identical local
		// snapshots conflict; comparing against refreshed rows would do that.
		if old, exists := originals[key]; exists && !reflect.DeepEqual(old, row) {
			v.blocked[key] = true
			result = errors.Join(result, errors.New("xfergc: conflicting owner rows"))
			continue
		}
		originals[key] = row
		if key.SID == "" || key.TransferID == "" || row.EntryNonce == "" || row.EntryNonce != row.Identity.EntryNonce || row.Revision == 0 || row.Origin == (xfercontract.Origin{}) {
			v.blocked[key] = true
			result = errors.Join(result, errors.New("xfergc: incomplete row identity"))
			continue
		}
		if row.Origin != local.Origin {
			proof, found := remote[row.Origin]
			if !found {
				if ctx.Err() != nil {
					return v, errors.Join(result, ctx.Err())
				}
				var err error
				proof, err = c.cfg.Owners.Census(row.Origin, deadline)
				remote[row.Origin] = proof
				if err != nil {
					result = errors.Join(result, err)
					proof.Complete = false
					remote[row.Origin] = proof
				}
			}
			if !proof.Ready || !proof.Complete || proof.Code != "" || proof.Origin != row.Origin {
				v.blocked[key] = true
				result = errors.Join(result, errors.New("xfergc: original owner unavailable"))
				continue
			}
			// Absence is not a retirement receipt for this v1 version. Only a
			// matching current row can schedule its finalizer; it still performs
			// the revision/gate/consumer proof, rather than trusting this scan.
			matched := false
			for _, current := range proof.Rows {
				if identityKey(current.Identity) != key {
					continue
				}
				if matched || current.Identity != row.Identity || current.Origin != row.Origin || current.EntryNonce != row.EntryNonce || current.Revision < row.Revision {
					v.blocked[key] = true
					result = errors.Join(result, errors.New("xfergc: original owner identity conflict"))
					continue
				}
				matched = true
				row = current
			}
			if !matched {
				v.blocked[key] = true
				result = errors.Join(result, errors.New("xfergc: version retirement proof unavailable"))
				continue
			}
		}
		v.rows[key] = row
	}
	return v, result
}

// Eligibility is separate from canonical Ts/Duration synthesis, which remains
// the finalizer's responsibility. A rate-sealed budget uses its original start;
// legacy zero fields retain the original size formula.
// origin: hot-update S7 plan §5.3 / T29
func (c *collector) advanceRows(ctx context.Context, v ownerView, deadline xfercontract.Deadline) error {
	var result error
	for key, row := range v.rows {
		if v.blocked[key] || !c.rowEligible(row) {
			continue
		}
		if ctx.Err() != nil {
			return errors.Join(result, ctx.Err())
		}
		_, err := c.cfg.Finalizer.Advance(row.Identity, deadline)
		result = errors.Join(result, err)
	}
	return result
}

// A Put without metadata still consumes bucket storage. Its chunk subject
// identifies a NUID, not a business key or origin. Missing ledger mapping is
// therefore unknown responsibility, never an age-based permission to purge.
// The frozen finalizer port takes Identity; the outer adapter must recover the
// original identity before these groups can enter retirement.
// origin: hot-update S7 plan §2.3 / §5.3 / T29 / M13
func (c *collector) inspectUnreferencedChunks(ctx context.Context, name string, objects []*jetstream.ObjectInfo, v ownerView) error {
	stream, err := c.cfg.JS.Stream(ctx, name)
	if err != nil {
		return err
	}
	bucket := transferBucket(name)
	prefix := "$O." + bucket + ".C."
	info, err := stream.Info(ctx, jetstream.WithSubjectFilter(prefix+">"))
	if err != nil {
		return err
	}
	referenced := make(map[string]bool)
	for _, object := range objects {
		if object != nil && !object.Deleted {
			referenced[object.NUID] = true
		}
	}
	sid := strings.TrimPrefix(name, "OBJ_xfer-")
	for subject := range info.State.Subjects {
		version := strings.TrimPrefix(subject, prefix)
		if !strings.HasPrefix(subject, prefix) || version == "" || referenced[version] || v.versions[sid+"/"+version] {
			continue
		}
		return fmt.Errorf("xfergc: %s: chunk version has no retirement identity", xfercontract.CodeResultUnknown)
	}
	return nil
}

func (c *collector) rowEligible(row xfercontract.LedgerRow) bool {
	switch row.Phase {
	case xfercontract.PhaseDecided, xfercontract.PhaseTerminalPending, xfercontract.PhaseHistoryConfirmed, xfercontract.PhaseCleanupPending:
		return true // Retained publication/cleanup needs its own progress owner.
	case xfercontract.PhaseOpen, xfercontract.PhasePrepared, xfercontract.PhaseSending, xfercontract.PhaseReceiving, xfercontract.PhaseCommitReserved:
		start := row.StartedAt
		budget := proto.XferBudget(row.Intent.Tier, row.Intent.Size, proto.XferPushLegs)
		if row.BudgetSeal.EffectiveBudgetNS != 0 {
			start = row.BudgetSeal.StartedAt
			budget = time.Duration(row.BudgetSeal.EffectiveBudgetNS)
		}
		return budget > 0 && !start.IsZero() && !c.cfg.UTC().Before(start.Add(budget))
	case xfercontract.PhaseSettled:
		return false
	}
	return false // Unknown phases retain evidence and cannot authorize cleanup.
}

// Legacy age/uptime rules only nominate an object. The finalizer remains the
// sole mutation owner even for these candidates. A version-bearing chunk is
// never classified as legacy merely because the new home has no tracker.
// origin: hot-update S7 plan §5.3 / T29
func (c *collector) advanceOrphans(ctx context.Context, streamName string, objects []*jetstream.ObjectInfo, v ownerView, complete bool, deadline xfercontract.Deadline) error {
	if !complete {
		return nil
	}
	sid := strings.TrimPrefix(streamName, "OBJ_xfer-")
	stream, err := c.cfg.JS.Stream(ctx, streamName)
	if err != nil {
		return err
	}
	var result error
	for _, object := range objects {
		// Ordinary SDK/v1 tombstones have no remaining metadata responsibility.
		// Only our retained nonempty legacy tombstone can retry an uncertain purge.
		// origin: hot-update S7 plan §5.3 / T29 / INTEG-R repeated-pass guard
		if object == nil || (object.Deleted && object.Chunks == 0) || !c.legacyOldEnough(object) {
			continue
		}
		key := xfercontract.Key{SID: sid, TransferID: object.Name}
		if v.protected[key] || v.versions[sid+"/"+object.NUID] {
			continue
		}
		if object.NUID == "" || object.Name == "" {
			result = errors.Join(result, errors.New("xfergc: orphan object identity incomplete"))
			continue
		}
		if object.Chunks != 0 {
			last, readErr := stream.GetLastMsgForSubject(ctx, "$O."+transferBucket(streamName)+".C."+object.NUID)
			if readErr != nil && (!object.Deleted || !errors.Is(readErr, jetstream.ErrMsgNotFound)) {
				result = errors.Join(result, readErr)
				continue
			}
			if last != nil && last.Header.Get("Tether-Xfer-Version") != "" {
				result = errors.Join(result, fmt.Errorf("xfergc: %s: original version owner unavailable", xfercontract.CodeResultUnknown))
				continue
			}
		} else {
			// Zero-data SDK metadata has no chunk headers to distinguish it
			// from v1. Without retained origin evidence it remains unknown.
			result = errors.Join(result, errors.New("xfergc: zero-data version owner unavailable"))
			continue
		}
		if ctx.Err() != nil {
			return errors.Join(result, ctx.Err())
		}
		// Identity alone cannot carry an unmapped SDK object through Advance.
		// Preserve its actual stream incarnation and exact metadata revision;
		// the finalizer independently rechecks census, age and every raw chunk.
		// origin: hot-update S7 plan §5.3 / T29 / INTEG-R additive P00 amendment
		finalizer, ok := c.cfg.Finalizer.(xfercontract.LegacyOrphanFinalizer)
		if !ok {
			result = errors.Join(result, errors.New("xfergc: legacy orphan retirement port unavailable"))
			continue
		}
		info, readErr := stream.Info(ctx)
		if readErr != nil {
			result = errors.Join(result, readErr)
			continue
		}
		subject := "$O." + transferBucket(streamName) + ".M." + base64.URLEncoding.EncodeToString([]byte(object.Name))
		raw, readErr := stream.GetLastMsgForSubject(ctx, subject)
		if readErr != nil {
			result = errors.Join(result, readErr)
			continue
		}
		sum := sha256.Sum256(raw.Data)
		candidate := xfercontract.LegacyOrphanCandidate{Bucket: transferBucket(streamName), Stream: streamName,
			StreamCreated: info.Created.UTC().Format(time.RFC3339Nano), Name: object.Name, NUID: object.NUID,
			MetadataSequence: raw.Sequence, MetadataDigest: hex.EncodeToString(sum[:])}
		_, retireErr := finalizer.RetireLegacyOrphan(candidate, deadline)
		result = errors.Join(result, retireErr)
	}
	return result
}

func (c *collector) legacyOldEnough(object *jetstream.ObjectInfo) bool {
	if object.ModTime.IsZero() || c.born.IsZero() || object.Size > uint64(proto.XferMaxBytes) {
		return false
	}
	now := c.cfg.UTC()
	uptime := now.Sub(c.born)
	if uptime < 0 {
		return false
	}
	floor := 3 * proto.XferTimeoutTierBFloor
	if extra := proto.XferBudget("b", int64(object.Size), proto.XferPushLegs) - proto.XferTimeoutTierBFloor; extra > 0 {
		floor += extra
	}
	if young := proto.XferTierBMaxBudget - uptime; young > floor {
		floor = young
	}
	return !now.Before(object.ModTime.Add(floor))
}

// origin: S7 internal review rv-txn F13 / hot-update S7 plan §5.3
// LegacyUptime is how long this process has been serving. Zero before Run, which is
// the conservative direction for every caller below: an unknown uptime buys an object
// the FULL protection window rather than none.
func LegacyUptime(bootAt, now time.Time) time.Duration {
	if bootAt.IsZero() {
		return 0
	}
	d := now.Sub(bootAt)
	if d < 0 {
		return 0
	}
	return d
}

// origin: S7 internal review rv-txn F13 / hot-update S7 plan §5.3
// LegacyObjectReapFloor is the minimum age an object must reach before this pass may
// delete it. Pure, so the three-way interaction below can be tested without a
// broker, a bucket or a clock.
//
// THE UPTIME TERM (origin: prerelease audit broker-transfer/BT-F4).
//
// The home-owned reap runs with a short grace (2 minutes) because the per-bucket
// busy re-read is supposed to shield live transfers. That protection is real for a
// transfer THIS process started — it is in the tracker, so the whole bucket is
// skipped. It says nothing about a transfer the PREVIOUS incarnation started: after
// a restart the tracker is empty, and on a single broker with no data_dir the ledger
// did not exist either (see #57, now fixed), so a 2-GiB tier-B push that was 3
// minutes into its 35m08s budget was deleted out from under the agent that was
// reading it.
//
// The fix is not to make this reap size-aware. That was considered and rejected:
// TestHomeReapIsNotSizeAware pins decision N15 in the AST, and a 2-GiB object would
// take a ~32-minute floor while buckets are sized for one max object plus margin, so
// the next tier-B push would be refused as too_large.
//
// Instead the floor falls back toward the ordinary grace as the process ages. Once
// this incarnation has been up for a full maximum budget, no object can still belong
// to a previous one, so the extra protection has done its job and costs nothing in
// steady state. It is also size-INDEPENDENT, so it does not reopen N15.
func LegacyObjectReapFloor(minAge time.Duration, sizeAware bool, size int64, uptime time.Duration) time.Duration {
	floor := minAge
	if sizeAware && floor > 0 {
		floor += max(0, proto.XferBudget("b", size, proto.XferPushLegs)-proto.XferTimeoutTierBFloor)
		return floor
	}
	if floor <= 0 {
		// minAge<=0 is the zero-value test broker's "reap everything now".
		return floor
	}
	if young := proto.XferTierBMaxBudget - uptime; young > floor {
		return young
	}
	return floor
}

// origin: S7 internal review rv-txn F13 / hot-update S7 plan §5.3
// LegacyHasAgedObjects classifies the legacy broker's unreapable-bucket gauge.
// Enumeration errors stay with the caller; they cannot fabricate aged objects.
func LegacyHasAgedObjects(objs []*jetstream.ObjectInfo, minAge time.Duration, now time.Time) bool {
	for _, obj := range objs {
		if obj.Deleted {
			continue
		}
		if minAge <= 0 || obj.ModTime.IsZero() || now.Sub(obj.ModTime) >= minAge {
			return true
		}
	}
	return false
}

// RetirementCheckpoint is a content-addressed journal candidate, not a claim
// that its retained producer, receiver or result responsibility disappeared.
// origin: hot-update S7 plan §5.3 / s7-p9m remaining request 4
type RetirementCheckpoint struct {
	Identity xfercontract.Identity
	Digest   string
}

// Each absence is an authoritative observation for this exact identity. Zero
// values retain the checkpoint; elapsed byte budgets cannot fill any member.
type RetirementResponsibility struct {
	Identity                                    xfercontract.Identity
	Authoritative, PrimaryAbsent, OutboxAbsent  bool
	CourierAbsent, PermitExpired, ResultExpired bool
}

// The durable adapter serializes Consume with Pin/Checkpoint, compares the
// exact candidate digest and syncs the directory after unlink. An uncertain
// checkpoint or deletion returns an error and remains retryable responsibility.
type RetirementCheckpointJournal interface {
	ScanRetirementCheckpoints(int, xfercontract.Deadline) ([]RetirementCheckpoint, error)
	ConsumeRetirementCheckpoint(RetirementCheckpoint, xfercontract.Deadline) error
}

// CollectRetirementCheckpoints bounds both enumeration and authority callbacks
// to a fixed pass. A complete ledger alone cannot exclude a courier or an
// unexpired creation permit, so every responsibility must independently be
// absent before the durable adapter receives permission to consume a record.
// origin: hot-update S7 plan §5.3 / s7-p9m remaining request 4
func CollectRetirementCheckpoints(journal RetirementCheckpointJournal, authority func(xfercontract.Identity, xfercontract.Deadline) (RetirementResponsibility, error), deadline xfercontract.Deadline) error {
	if journal == nil || authority == nil {
		return errors.New("xfergc: retirement journal authority unavailable")
	}
	pass, err := controlbudget.After(5 * time.Second)
	if err != nil {
		return err
	}
	deadline = min(deadline, pass)
	if _, err = controlbudget.Remaining(deadline); err != nil {
		return err
	}
	const limit = 64
	checkpoints, err := journal.ScanRetirementCheckpoints(limit, deadline)
	if err != nil {
		return err
	}
	if len(checkpoints) > limit {
		return errors.New("xfergc: retirement journal scan exceeded its bound")
	}
	var result error
	for _, checkpoint := range checkpoints {
		if _, err = controlbudget.Remaining(deadline); err != nil {
			return errors.Join(result, err)
		}
		id := checkpoint.Identity
		if id.SID == "" || id.TransferID == "" || id.EntryNonce == "" || id.ObjectVersion == "" || checkpoint.Digest == "" {
			result = errors.Join(result, errors.New("xfergc: retirement checkpoint identity is incomplete"))
			continue
		}
		evidence, err := authority(id, deadline)
		if err != nil || evidence.Identity != id || !evidence.Authoritative || !evidence.PrimaryAbsent || !evidence.OutboxAbsent ||
			!evidence.CourierAbsent || !evidence.PermitExpired || !evidence.ResultExpired {
			result = errors.Join(result, err, errors.New("xfergc: retirement checkpoint responsibility remains uncertain"))
			continue
		}
		if _, err = controlbudget.Remaining(deadline); err != nil {
			return errors.Join(result, err)
		}
		result = errors.Join(result, journal.ConsumeRetirementCheckpoint(checkpoint, deadline))
	}
	return result
}

// checkpointStore borrows the journal producer's mutex. Sharing that exact
// lock makes content-CAS and unlink atomic with its Pin/Checkpoint writes.
// It owns no persistent FD or goroutine and opens the scoped root per call.
type checkpointStore struct {
	root   string
	locker checkpointLocker
}

type checkpointLocker interface {
	sync.Locker
	TryLock() bool
}

// NewRetirementCheckpointJournal adapts the existing durable writer-state
// records. The original producer must supply its own lock; a second mutex
// would let a replacement checkpoint race the collector's content proof.
// origin: hot-update S7 plan §5.3 / s7-p9m remaining request 4
func NewRetirementCheckpointJournal(root string, locker sync.Locker) (RetirementCheckpointJournal, error) {
	lock, ok := locker.(checkpointLocker)
	if root == "" || !ok {
		return nil, errors.New("xfergc: retirement journal root or lock unavailable")
	}
	return &checkpointStore{root: root, locker: lock}, nil
}

// The producer's lock must participate in the same pass deadline too; a
// blocking Lock could otherwise wedge every following collector pass.
func (j *checkpointStore) lock(d xfercontract.Deadline) error {
	for {
		remaining, err := controlbudget.Remaining(d)
		if err != nil {
			return err
		}
		if j.locker.TryLock() {
			return nil
		}
		timer := time.NewTimer(min(remaining, 5*time.Millisecond))
		<-timer.C
	}
}
func checkpointName(id xfercontract.Identity) string {
	body, _ := json.Marshal(id)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]) + ".json"
}
func checkpointRecord(root *os.Root, name string) (checkpoint RetirementCheckpoint, resultErr error) {
	file, err := root.Open(name)
	if err != nil {
		return RetirementCheckpoint{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	// The production retirement journal holds geometry/fence receipts, not
	// replay bytes. Oversized or unknown records stay retained rather than
	// allocating an unbounded state or dropping a future responsibility field.
	const maximum = 64 << 10
	body, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return RetirementCheckpoint{}, err
	}
	if len(body) > maximum {
		return RetirementCheckpoint{}, errors.New("xfergc: retirement checkpoint exceeds its read bound")
	}
	var state xfercontract.WriterState
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&state); err != nil {
		return RetirementCheckpoint{}, err
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF || checkpointName(state.ObjectSpec.Identity) != name ||
		state.ObjectSpec.StreamCreated == "" || state.Phase != xfercontract.WriteRetired ||
		!state.RetirementFence.Purged || !state.RetirementFence.WriterRetired || !state.RetirementFence.ConsumersRetired ||
		state.RetirementFence.Receipt == nil || state.RetirementFence.Receipt.Sequence == 0 || state.RetirementFence.Receipt.RecordDigest == "" {
		return RetirementCheckpoint{}, errors.New("xfergc: retirement checkpoint is not a confirmed retirement")
	}
	sum := sha256.Sum256(body)
	return RetirementCheckpoint{Identity: state.ObjectSpec.Identity, Digest: hex.EncodeToString(sum[:])}, nil
}
func (j *checkpointStore) ScanRetirementCheckpoints(limit int, d xfercontract.Deadline) (checkpoints []RetirementCheckpoint, resultErr error) {
	if _, err := controlbudget.Remaining(d); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 64 {
		return nil, errors.New("xfergc: invalid retirement journal scan bound")
	}
	if err := j.lock(d); err != nil {
		return nil, err
	}
	defer j.locker.Unlock()
	root, err := os.OpenRoot(j.root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, root.Close()) }()
	directory, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, directory.Close()) }()
	files, err := directory.ReadDir(limit)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	var records []RetirementCheckpoint
	for _, file := range files {
		if _, err = controlbudget.Remaining(d); err != nil {
			return records, err
		}
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		record, err := checkpointRecord(root, file.Name())
		if err != nil {
			return records, err
		}
		records = append(records, record)
	}
	return records, nil
}
func (j *checkpointStore) ConsumeRetirementCheckpoint(checkpoint RetirementCheckpoint, d xfercontract.Deadline) (resultErr error) {
	if _, err := controlbudget.Remaining(d); err != nil {
		return err
	}
	if err := j.lock(d); err != nil {
		return err
	}
	defer j.locker.Unlock()
	root, err := os.OpenRoot(j.root)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, root.Close()) }()
	name := checkpointName(checkpoint.Identity)
	current, err := checkpointRecord(root, name)
	if err != nil {
		return err
	}
	if current != checkpoint {
		return errors.New("xfergc: retirement checkpoint changed before deletion")
	}
	if _, err = controlbudget.Remaining(d); err != nil {
		return err
	}
	if err = root.Remove(name); err != nil {
		return err
	}
	// Directory durability is part of deletion, not a best-effort maintenance
	// hint. An uncertain Sync returns no successful collection receipt.
	// origin: hot-update S7 plan §5.3 / s7-p9m remaining request 4
	return syncCheckpointDirectory(root)
}
func syncCheckpointDirectory(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
