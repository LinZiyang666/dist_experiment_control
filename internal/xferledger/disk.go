package xferledger

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// Store has one serializer for both directories and retained memory decisions.
// It owns no goroutines or long-lived descriptors; the tracker owns its lifetime.
// A failed write remains visible to a planned cut, but never becomes durable ACK.
// origin: hot-update S7 plan §5.1 / T15 / T22
type ledger struct {
	cfg    Config
	gate   chan struct{}
	bootID string
	rows   map[xfercontract.Key]RowV1
	dirty  map[xfercontract.Key]bool
	// Sync is an internal IO boundary so fault tests can fail real file/dir
	// durability independently. It never changes the public frozen config.
	syncFile func(*os.File) error
}

var _ Factory = New
var _ Store = (*ledger)(nil)

// New opens the two explicitly configured ledger roots. The caller installs
// exactly one store at a time; a sleeping imported tracker precedes its reaper.
func New(cfg Config) (Store, error) {
	if cfg.Clock == nil || cfg.Root == "" || cfg.FallbackRoot == "" {
		return nil, fmt.Errorf("%w: ledger roots and clock are required", ErrUnknown)
	}
	var err error
	cfg.Root, err = filepath.Abs(cfg.Root)
	if err != nil {
		return nil, err
	}
	cfg.FallbackRoot, err = filepath.Abs(cfg.FallbackRoot)
	if err != nil {
		return nil, err
	}
	if cfg.Root == cfg.FallbackRoot || strings.HasPrefix(cfg.Root, cfg.FallbackRoot+string(os.PathSeparator)) ||
		strings.HasPrefix(cfg.FallbackRoot, cfg.Root+string(os.PathSeparator)) {
		return nil, fmt.Errorf("%w: ledger roots must be independent", ErrUnknown)
	}
	boot, now, err := cfg.Clock()
	if err != nil || boot == "" || now <= 0 {
		return nil, fmt.Errorf("%w: invalid ledger clock", ErrUnknown)
	}
	// Creating missing roots is constructor-only. Recreating a missing root
	// during census would turn a temporarily hidden mount into false absence.
	primaryErr := makeDirectory(cfg.Root)
	outboxErr := makeDirectory(cfg.FallbackRoot)
	if primaryErr != nil && outboxErr != nil {
		return nil, fmt.Errorf("%w: neither ledger root is available: %w", ErrDurability, errors.Join(primaryErr, outboxErr))
	}
	return &ledger{cfg: cfg, gate: make(chan struct{}, 1), bootID: boot,
		rows: make(map[xfercontract.Key]RowV1), dirty: make(map[xfercontract.Key]bool), syncFile: (*os.File).Sync}, nil
}

func makeDirectory(path string) error {
	info, err := os.Lstat(path)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%w: ledger root is not a directory", ErrUnknown)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	parent := filepath.Dir(path)
	if err := makeDirectory(parent); err != nil {
		return err
	}
	if err := os.Mkdir(path, 0o700); err != nil && !os.IsExist(err) {
		return err
	}
	return syncDirectory(parent)
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

func (s *ledger) remaining(deadline controlbudget.Deadline) (time.Duration, error) {
	if deadline <= 0 {
		return 0, controlbudget.ErrInvalidDeadline
	}
	boot, now, err := s.cfg.Clock()
	if err != nil {
		return 0, err
	}
	if boot != s.bootID || now <= 0 {
		return 0, fmt.Errorf("%w: ledger clock changed boot", ErrUnknown)
	}
	if now >= int64(deadline) {
		return 0, controlbudget.ErrExpired
	}
	return time.Duration(int64(deadline) - now), nil
}

func (s *ledger) lock(deadline controlbudget.Deadline) error {
	remaining, err := s.remaining(deadline)
	if err != nil {
		return err
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case s.gate <- struct{}{}:
		if _, err := s.remaining(deadline); err != nil {
			s.unlock()
			return err
		}
		return nil
	case <-timer.C:
		return controlbudget.ErrExpired
	}
}

func (s *ledger) unlock() { <-s.gate }

// Hash both components with an unambiguous encoding. A transfer ID is neither
// globally unique across sessions nor a safe pathname supplied by a caller.
func rowName(key xfercontract.Key) string {
	data, _ := json.Marshal(key)
	return digest(data) + ".json"
}

func readRow(root *os.Root, name string) (RowV1, bool, error) {
	info, err := root.Lstat(name)
	if os.IsNotExist(err) {
		return RowV1{}, false, nil
	}
	if err != nil {
		return RowV1{}, false, err
	}
	// Opening a FIFO would wedge the owner beyond the control deadline; a
	// symlink is not a record even if its target happens to decode correctly.
	if !info.Mode().IsRegular() || info.Size() > maxRowBytes {
		return RowV1{}, false, fmt.Errorf("%w: nonregular or oversized ledger row", ErrUnknown)
	}
	file, err := root.Open(name)
	if err != nil {
		return RowV1{}, false, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, maxRowBytes+1))
	if err != nil {
		return RowV1{}, false, err
	}
	row, err := decodeRow(data)
	if err == nil && name != rowName(rowKey(row)) {
		err = fmt.Errorf("%w: ledger filename identity mismatch", ErrConflict)
	}
	return row, err == nil, err
}

// temp -> file Sync -> close -> rename -> SAME directory Sync is the durable
// boundary. os.Root keeps the operations tied to the opened directory if its
// pathname moves. No success receipt can escape between file Sync and dir Sync.
// origin: hot-update S7 plan §5.1 / T22; xfer_inflight.go writeLedgerRecord (F6)
func (s *ledger) writeAt(path string, row RowV1, deadline controlbudget.Deadline) error {
	data, err := encodeRow(row)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	temp := ".xfer-" + rand.Text() + ".tmp"
	file, err := root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = root.Remove(temp) }()
	defer func() { _ = file.Close() }()
	if _, err := s.remaining(deadline); err != nil {
		return err
	}
	if n, err := file.Write(data); err != nil {
		return err
	} else if n != len(data) {
		return io.ErrShortWrite
	}
	if err := s.syncFile(file); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if _, err := s.remaining(deadline); err != nil {
		return err
	}
	if err := root.Rename(temp, rowName(rowKey(row))); err != nil {
		return err
	}
	if err := s.syncFile(dir); err != nil {
		return err
	}
	if s.cfg.Observe != nil {
		obs := xfercontract.Observation{Kind: xfercontract.ObserveLedgerSync, TransferID: row.Identity.TransferID,
			EntryNonce: row.EntryNonce, ObjectVersion: row.Identity.ObjectVersion, Phase: row.Phase,
			Revision: row.Revision, FileSynced: true, DirectorySynced: true}
		if path == s.cfg.Root {
			obs.PrimaryRevision = row.Revision
		} else {
			obs.OutboxRevision = row.Revision
		}
		s.cfg.Observe(obs)
	}
	_, err = s.remaining(deadline)
	return err
}

// Removing the primary is a barrier, not a best-effort cleanup. In particular,
// a missing parent can hide an old START on an unmounted/replaced directory;
// the exact outbox TERMINAL must survive that uncertainty.
// origin: hot-update S7 plan §5.1 / T22; xfer_inflight.go removeLedgerRowDurably (R5-F1)
func (s *ledger) removeAt(path, name string) error {
	root, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	if err := root.Remove(name); err != nil && !os.IsNotExist(err) {
		return err
	}
	return s.syncFile(dir)
}

// Legacy is the cap0 ledger owner. Callbacks borrow the current broker backend;
// disk precedence and recovery decisions stay with this leaf.
// origin: hot-update S7 plan §5.1 / §12.2 / P13
// It does not interpret v1 rows or migrate their authority into a legacy record.
type Legacy struct{ Config LegacyConfig }
type LegacyConfig struct {
	ClusterDataDir, DBPath string
	Logger                 *slog.Logger
	Now                    func() time.Time
	Cluster                bool
	StreamCommitted        func(string, string) (bool, error)
	Live                   func(string) bool
	Delete                 func(string, string) error
	Committed              func(schema.AuditTransfer) (bool, error)
	Commit                 func(schema.AuditTransfer) bool
	Publish                func(string, []byte, string) error
	// Confirm is the exact history writer for one canonical cap0 row: the
	// transfer-subject CAS plus full readback, never a MsgID window or a Raft
	// commit. Nil (no JetStream) keeps every new admission in the pre-S7 shape.
	// origin: hot-update S7 plan §5.2 / L12 (S7 integration oplegacy F2)
	Confirm func(schema.AuditTransfer) error
}

// xfer_inflight.go (R16 #57 Lane B) — a node-local DURABLE in-flight-transfer ledger + a finalize-on-recovery
// pass. The transfer state machine (transferTracker + the watchdog goroutine) is process-memory-only, so a
// HOME broker that crashes between the durable "start" audit row and any terminal row leaves that start row
// DANGLING FOREVER (the tracker is rebuilt empty, the watchdog goroutine is gone, and the receiver's late
// finalization is dropped for want of a tracker entry). This ledger persists the immutable fields of every
// in-flight transfer to `<ClusterDataDir>/xfer-inflight/<hash>.json` at put() time and removes it at every
// terminal path; on the periodic reconcile pass, any ledger file older than its tier timeout with no live
// tracker entry gets a DETERMINISTIC synthetic terminal (Ts = startedAt+timeout) emitted through the normal
// audit path — so the content-addressed TransferRecordReqID collapses every re-emit in the replicated dedup
// ledger. Cluster mode only (ClusterDataDir set); single-broker audit publish is best-effort by design.

// xferStrandedSlack is added to a transfer's tier timeout before the finalizer will synthesize a terminal —
// a grace so a transfer that legitimately took just over its timeout (a slow receiver whose real terminal is
// in flight) is never double-finalized by a too-eager pass.
const LegacyStrandedSlack = 60 * time.Second

// xferInflightRecord is the persisted immutable projection of a transferEntry — everything the finalizer
// needs to synthesize a byte-deterministic terminal audit row for a stranded transfer.
type LegacyRecord struct {
	TransferID string    `json:"transfer_id"`
	Session    string    `json:"session"`
	Node       string    `json:"node"`
	ActorNkey  string    `json:"actor_nkey"`
	ActorFp    string    `json:"actor_fp"`
	Verb       string    `json:"verb"`
	Tier       string    `json:"tier"`
	Bucket     string    `json:"bucket"`
	Path       string    `json:"path"`
	StartedAt  time.Time `json:"started_at"`
	// Size (batch C) is the declared transfer size, so a recovering broker derives the SAME budget the
	// live watchdog was using. Additive omitempty: a record written before batch C has no size, which
	// degrades to the fixed tier floor — the pre-batch-C behaviour, and the fail-SAFE direction (a
	// live transfer is declared stranded no EARLIER than it used to be, never sooner).
	Size int64 `json:"size,omitempty"`

	// Terminal (external RE-REVIEW F1) is the COMPLETE, already-decided terminal audit record, staged
	// here BEFORE it is forwarded. It turns this ledger into an OUTBOX and is what closes the second
	// crash window: the first fix only guaranteed "the ledger outlives the commit", so a process killed
	// AFTER the real terminal committed but BEFORE the unlink came back to a start-only ledger, and the
	// finalizer then GUESSED a different `failed/home_broker_restart` row. TransferRecordReqID hashes the
	// whole normalized record, so a guess never dedups against the real terminal and the audit ends up
	// claiming the same transfer both completed and failed.
	//
	// With the record staged, recovery REPLAYS THIS EXACT ROW instead of guessing: identical bytes ⇒
	// identical reqID ⇒ the replicated dedup ledger collapses it against the already-committed one.
	Terminal *schema.AuditTransfer `json:"terminal,omitempty"`

	// Start is the exact canonical cap0 START, written with its admission nonce BEFORE the prepare is
	// forwarded. Its presence is what makes this row canonical: the terminal binds to that nonce, and the
	// row retires only after exact history holds BOTH rows. Additive omitempty: an older record (or one
	// admitted without an exact history owner) keeps the pre-S7 MsgID replay path unchanged.
	// origin: hot-update S7 plan §4 / §5.1 / §5.2 / L12 (S7 integration oplegacy F2)
	Start *schema.AuditTransfer `json:"start,omitempty"`
}

// A SINGLE BROKER HAS A LEDGER TOO.
//
// origin: prerelease audit broker-transfer/#57. This used to return "" whenever
// ClusterDataDir was unset, and scripts/install.sh renders a production broker.yaml
// with data_dir commented out — so on every single-broker deployment (which is the
// common one) there was no durable ledger at all. Two things silently followed: a
// restart could never finalize a stranded transfer, and ledgerProtectedObjects
// returned an EMPTY protected set, which the orphan reaper reads as "nobody owns
// any of these objects".
//
// The DB file's directory is the right fallback because it is the one path a broker
// always has, is always writable, and already holds the state whose loss would be
// equally fatal.
func (b *Legacy) InflightDir() string {
	return LegacySubdir(b.Config.ClusterDataDir, b.Config.DBPath, "xfer-inflight")
}

// xferLedgerSubdir is the shared root resolution, extracted so both ledger
// directories cannot drift apart and so the fallback is testable without a Broker.
func LegacySubdir(clusterDataDir, dbPath, sub string) string {
	if clusterDataDir != "" {
		return filepath.Join(clusterDataDir, sub)
	}
	if dbPath == "" {
		// Nothing durable to hang it off — an in-memory test broker. Callers already
		// treat "" as "no ledger".
		return ""
	}
	return filepath.Join(filepath.Dir(dbPath), sub)
}

// ── DURABLE PRECEDENCE OF THE TWO LEDGER DIRECTORIES (round-5 疑惑 1) ──────────────────────────────────
//
// Two sibling directories hold rows for the same transfer, so the precedence between them must be a
// written rule rather than a consequence of the order in which a scan happens to visit them:
//
//	AN EXACT TERMINAL OUTRANKS A START ROW, WHEREVER EITHER LIVES, AND A START ROW MAY ONLY BE RETIRED
//	TOGETHER WITH — NEVER BEFORE — THE EXACT TERMINAL THAT SUPERSEDES IT.
//
// The legal states, the crash point each one represents, and what the next pass does:
//
//	primary        outbox        means                                  next pass
//	-----------    ----------    ------------------------------------   ------------------------------------
//	start          -             in flight, or crashed before any        synthesize after timeout+slack (#57)
//	                             terminal was decided
//	terminal       -             terminal decided, staged in place;      replay the exact row, then consume
//	                             crash before/after commit               both rows
//	start          terminal      terminal decided while the primary      replay the OUTBOX row (pass 1);
//	                             directory was unwritable                never synthesize from the start row
//	-              terminal      as above, and the start row was          replay the outbox row
//	                             already retired (or never written)
//	terminal       terminal      two terminal paths raced across a       pass 1 replays once and consumes
//	                             directory failure                       both; pass 2 finds nothing
//
// THE TABLE ABOVE IS NOT THE WHOLE RULE — three more inputs decide every row (B8)
// ------------------------------------------------------------------------------
// The two columns are necessary but not sufficient, and for years the missing inputs lived only in
// the branch order of finalizeStrandedXfers. They are now arguments to ledgerRowDisposition below,
// because a rule that exists only as control flow cannot be tested row by row:
//
//	LIVE     a tracker entry for this transfer in THIS incarnation ⇒ NEVER finalize; its own terminal
//	         path owns it. Checked at BOTH sites.
//	NOW      a start row younger than its tier timeout + xferStrandedSlack is not stranded, it is slow.
//	CENSUS   whether the outbox scan SUCCEEDED. A failed census is UNKNOWN, not empty (R6-F1): a
//	         start-only row must not be synthesized while a higher-precedence exact terminal may still
//	         be hidden in an unreadable outbox.
//
// And the SITE is itself an input, not a detail: the two passes apply these gates in a DIFFERENT
// order (pass 2's staged-terminal branch does not consult the outbox census at all — only its
// start-only branch does), and one transfer can hold a row at each site simultaneously.
//
// Retirement is therefore a single ordered operation (consumeXferLedgerRow): primary FIRST, and the
// outbox row only once the primary row is confirmed gone. If the primary removal cannot be confirmed, the
// outbox row STAYS — it is the only evidence that stops the surviving start row from being turned into a
// second, contradictory terminal. Every disposal path in this file goes through that one function.
//
// xferTerminalOutboxDir is the FALLBACK staging location for a terminal whose in-flight ledger record
// cannot be updated (round-4 external re-review).
//
// Staging is what makes forwarding a terminal recoverable, so the primary directory becoming unwritable
// — replaced by a file, unmounted, gone read-only, wiped by an operator recovery — must not silently
// remove the only durable record of the outcome. It is a SIBLING of the in-flight directory, so it
// survives exactly the failures that take the in-flight directory itself out. Recovery scans both and
// replays an outbox row verbatim, identically to an in-place one.
func (b *Legacy) OutboxDir() string {
	return LegacySubdir(b.Config.ClusterDataDir, b.Config.DBPath, "xfer-terminal-outbox")
}

// xferInflightFilename hashes the transfer_id into a path-safe fixed-width name so a transfer_id can never
// escape the ledger dir (path traversal) or collide with the atomic temp files.
func LegacyFilename(transferID string) string {
	sum := sha256.Sum256([]byte(transferID))
	return hex.EncodeToString(sum[:]) + ".json"
}

// transferTimeoutFor is the budget a RECOVERING broker attributes to a transfer it finds in the
// ledger. It must agree with the live watchdog (proto.XferBudget over the same legs), or crash
// recovery declares a transfer stranded while the process that owned it would still have been
// waiting — writing a `failed` terminal for something that was healthy.
//
// size==0 is an old record (written before batch C added Size) and yields the fixed tier floor: the
// pre-batch-C behaviour, and the direction that can only wait LONGER, never reap sooner.
func LegacyTimeout(tier string, size int64) time.Duration {
	return proto.XferBudget(tier, size, proto.XferPushLegs)
}

// transferTierFloorFor is the FIXED per-tier timeout, and it is deliberately NOT the size-derived
// budget. It has exactly one caller: the synthetic terminal's Ts and DurationMs.
//
// Ts is the dedup carrier. TransferRecordReqID hashes the whole normalized record, so two
// incarnations that disagree about Ts produce two DIFFERENT reqIDs for the same transfer and the
// replicated dedup ledger cannot collapse them — the audit then claims the transfer both failed and
// failed again, at two timestamps. Deriving Ts from the size-derived budget would make it depend on
// XferMinThroughput, so a ROLLBACK (or any future retune of that constant) would have the old and
// new binaries stamp different Ts for the identical ledger record. Pinning Ts to the tier floor
// keeps it a pure function of (StartedAt, tier), which is stable across versions by construction.
//
// The stranded DECISION does use the real budget — that one must track the live watchdog or recovery
// declares a healthy slow transfer stranded. The two uses are genuinely different questions.
func LegacyTierFloor(tier string) time.Duration {
	if tier == "b" {
		return proto.XferTimeoutTierBFloor
	}
	return proto.XferTimeoutTierA
}

// writeLedgerRecord atomically and DURABLY installs a ledger record: temp -> write -> fsync(file) ->
// close -> rename -> fsync(dir). External review F6: the previous version did Write/Close/Rename with no
// sync at all, so a power loss or kernel crash could lose either the bytes or the directory entry while
// the doc comment called the ledger "durable". A ledger that can vanish is exactly the evidence #57 needs.
func (b *Legacy) WriteRecord(dir string, rec LegacyRecord) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".xfer-inflight-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	_ = tmp.Chmod(0o600)
	if _, werr := tmp.Write(payload); werr != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return werr
	}
	if serr := tmp.Sync(); serr != nil { // F6: the bytes must reach stable storage before the rename
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return serr
	}
	if cerr := tmp.Close(); cerr != nil {
		_ = os.Remove(tmpName)
		return cerr
	}
	if rerr := os.Rename(tmpName, filepath.Join(dir, LegacyFilename(rec.TransferID))); rerr != nil {
		_ = os.Remove(tmpName)
		return rerr
	}
	return legacySyncDir(dir) // F6: the rename itself must be durable, not just the bytes
}

// syncDir fsyncs a directory so a rename/unlink inside it is durable.
func legacySyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

// stageXferInflightTerminal writes the DECIDED terminal record into the ledger before it is forwarded
// (external RE-REVIEW F1). Returns false when nothing was staged (no ledger dir at all, or the write
// failed) so the caller can fall back to the pre-outbox behaviour rather than silently lose a row.
//
// Returns (staged, applicable). applicable=false means this deployment keeps NO ledger, which is NOT a
// failure; staged=false with applicable=true is a real durability failure and the caller must NOT proceed
// to an async forward (round-3 R3-F2).
//
// "NO LEDGER" IS NO LONGER "SINGLE-BROKER MODE" — origin: prerelease audit round 2, F-T6. Both sentences
// above said it was, and #57's extension to single brokers made that false in the same commit that
// relied on it: xferLedgerSubdir falls back to the DB's own directory, so a single broker with a
// database on disk DOES keep a ledger. What reaches applicable=false today is a broker with neither a
// ClusterDataDir nor a DBPath — an in-memory test broker. A reader auditing this function was being told,
// in the file itself, that a state it must handle cannot arise.
func (b *Legacy) StageTerminal(transferID string, rec schema.AuditTransfer) (staged, applicable bool) {
	_, staged, applicable = b.Stage(transferID, rec, nil)
	return staged, applicable
}

// Stage is StageTerminal returning the row it now holds. start is the admission
// START the live tracker retained: a canonical row stays self-contained even when
// its primary row cannot be read. A canonical row keeps its FIRST decided
// terminal — an opposite or re-timed decision is refused, never written over the
// staged bytes — and the caller publishes the returned row's terminal.
// origin: hot-update S7 plan §4 / §5.1 / L12 (S7 integration oplegacy F2)
func (b *Legacy) Stage(transferID string, rec schema.AuditTransfer, start *schema.AuditTransfer) (cur LegacyRecord, staged, applicable bool) {
	dir := b.InflightDir()
	if dir == "" {
		// nothing durable to anchor a ledger to — nothing to stage, nothing at risk
		return LegacyRecord{TransferID: transferID, Terminal: &rec, Start: start}, false, false
	}
	cur, err := ReadLegacyRecord(filepath.Join(dir, LegacyFilename(transferID)))
	if err != nil {
		// No start record (single-mode, pre-ledger transfer, or an unreadable file): stage a
		// self-contained record so recovery can still replay the exact terminal.
		// batch C: Size is carried here too. This is the SECOND writer of an xferInflightRecord, and
		// the one easy to miss — a Size filled only on the start path would leave every
		// synthesized-terminal record at 0 and quietly re-fix the budget at the tier floor.
		cur = LegacyRecord{TransferID: transferID, Session: rec.Session, Node: rec.Node,
			ActorNkey: rec.ActorNkey, ActorFp: rec.ActorFp, Verb: rec.Verb, Tier: rec.Tier,
			Bucket: rec.Bucket, Path: rec.Path, StartedAt: rec.Ts, Size: rec.Bytes}
	}
	if cur.Start == nil {
		cur.Start = start
	}
	if cur.Start != nil && cur.Terminal != nil {
		if first, err := schema.TransferRecordReqID(*cur.Terminal); err != nil || !sameTerminal(first, rec) {
			b.Config.Logger.Error("broker: a second, different cap0 terminal was refused — the first staged "+
				"decision is the only one this transfer may publish", "tid", transferID, "kind", rec.Kind,
				"staged_kind", cur.Terminal.Kind)
			return cur, true, true
		}
	}
	cur.Terminal = &rec
	werr := b.WriteRecord(dir, cur)
	if werr == nil {
		return cur, true, true
	}
	// ROUND-4 RE-REVIEW: do not conclude "unstageable" from ONE unwritable directory. Retry in the sibling
	// outbox — that record carries the EXACT terminal, so recovery replays it byte-for-byte instead of
	// inventing a synthetic one that contradicts it.
	outbox := b.OutboxDir()
	if outbox == "" {
		b.Config.Logger.Error("broker: could not stage the terminal into the xfer-inflight ledger",
			"err", werr, "tid", transferID, "kind", rec.Kind)
		return cur, false, true
	}
	if oerr := b.WriteRecord(outbox, cur); oerr != nil {
		b.Config.Logger.Error("broker: could not stage the terminal in EITHER the xfer-inflight ledger or the "+
			"fallback outbox — recovery will have no record of this outcome",
			"err", werr, "outbox_err", oerr, "tid", transferID, "kind", rec.Kind)
		return cur, false, true
	}
	b.Config.Logger.Warn("broker: the xfer-inflight ledger is unwritable — staged this transfer's terminal in the "+
		"fallback outbox instead, so recovery can still replay it exactly",
		"err", werr, "tid", transferID, "kind", rec.Kind)
	return cur, true, true
}

// sameTerminal compares decisions by their content key, which ignores only the
// monotonic clock reading; a re-timed or opposite outcome is a different row.
func sameTerminal(firstReqID string, rec schema.AuditTransfer) bool {
	next, err := schema.TransferRecordReqID(rec)
	return err == nil && next == firstReqID
}

// Admit retains a cap0 admission identity only where this broker has a durable
// ledger AND an exact history writer: the START gains a fresh nonce and the
// best-effort mark and must validate as canonical before anything is forwarded.
// Otherwise it keeps the pre-S7 shape — identity is never minted after admission.
// The second result is the START the tracker retains (nil for the pre-S7 shape).
// origin: hot-update S7 plan §4 / §5.1 / L12 (S7 integration oplegacy F2)
func (b *Legacy) Admit(start schema.AuditTransfer) (schema.AuditTransfer, *schema.AuditTransfer) {
	if b.Config.Confirm == nil || b.InflightDir() == "" {
		return start, nil
	}
	canonical := start
	canonical.EntryNonce, canonical.BestEffort = rand.Text(), true
	if _, err := schema.CanonicalTransferAudit(canonical); err != nil {
		b.Config.Logger.Warn("broker: cap0 START cannot carry a canonical identity — keeping the pre-S7 shape",
			"err", err, "tid", start.TransferID)
		return start, nil
	}
	return canonical, &canonical
}

// Exact publishes one canonical cap0 row (the admission START) through the
// exact writer; the retained ledger row still owns it until Settle confirms it.
// origin: hot-update S7 plan §5.2 / L12 (S7 integration oplegacy F2)
func (b *Legacy) Exact(rec schema.AuditTransfer) error {
	err := errors.New("transfer history writer is unavailable")
	if b.Config.Confirm != nil {
		err = b.Config.Confirm(rec)
	}
	if err != nil {
		b.Config.Logger.Warn("broker: cap0 history is not exactly confirmed yet — its ledger row retains it",
			"err", err, "tid", rec.TransferID, "kind", rec.Kind)
	}
	return err
}

// Confirmed is the only proof that may retire a canonical cap0 row: the exact
// START, then the exact TERMINAL, each through the transfer-subject CAS and full
// readback. A refusal, conflict or unknown answers false and retains every row
// for the next pass. It never publishes a TERMINAL ahead of its START, and never
// accepts a MsgID window, a Raft commit or the mere existence of some terminal
// for this transfer ID.
// origin: hot-update S7 plan §5.1 / §5.2 / L12 (S7 integration oplegacy F2)
func (b *Legacy) Confirmed(rec LegacyRecord) bool {
	if rec.Start == nil || rec.Terminal == nil || b.Config.Confirm == nil {
		b.Config.Logger.Error("broker: a canonical cap0 transfer has no exact START/TERMINAL pair or history "+
			"writer — retaining its ledger rows", "tid", rec.TransferID)
		return false
	}
	for _, row := range []schema.AuditTransfer{*rec.Start, *rec.Terminal} {
		if err := b.Config.Confirm(row); err != nil {
			b.Config.Logger.Warn("broker: cap0 history is not exactly confirmed — retaining its ledger rows for "+
				"the next pass", "err", err, "tid", rec.TransferID, "kind", row.Kind)
			return false
		}
	}
	return true
}

// Settle is recovery's retirement of a canonical cap0 row: exact history first,
// then the object and both ledger rows (primary before outbox). The live path
// deletes its object itself and retires only the rows after Confirmed.
// origin: hot-update S7 plan §5.1 / §5.2 / §5.3 (S7 integration oplegacy F2)
func (b *Legacy) Settle(rec LegacyRecord) bool { return b.Confirmed(rec) && b.RetireStaged(rec) }

// removeXferInflight drops the ledger file for a transfer that reached a terminal (called from every
// terminal path). Idempotent: a missing file is fine.
// consumeXferLedgerRow retires a transfer's ledger credentials in BOTH directories, PRIMARY FIRST.
//
// ROUND-5 R5-F1: the exact terminal parked in the outbox is the only thing that stops recovery from
// synthesizing a contradictory `failed/home_broker_restart` beside an already-committed `complete`. So it
// may be dropped ONLY once the primary start row is confirmed gone. Deleting them independently and
// ignoring failures — which is what the first version did — leaves a hidden start row behind whenever the
// primary directory is momentarily unusable, and the next finalize pass turns that row into a second,
// contradictory terminal. Order and error-propagation ARE the invariant here.
func (b *Legacy) Consume(transferID string) error {
	dir := b.InflightDir()
	if dir == "" {
		return nil
	}
	name := LegacyFilename(transferID)
	if err := RemoveLegacyRowDurably(dir, name, false); err != nil {
		// The start row may survive. KEEP the outbox terminal: it is the tombstone that makes the next
		// pass replay the TRUE outcome instead of inventing one. In particular, ENOENT for the parent
		// directory is not proof that a row hidden by an unmounted/replaced directory is gone.
		return err
	}
	outbox := b.OutboxDir()
	if outbox == "" {
		return nil
	}
	// Once the primary row is durably absent, a missing outbox directory is harmless: there is no start
	// row left for recovery to misinterpret, and any outbox terminal exposed by a later remount replays
	// the already-committed exact bytes. If the directory exists, however, its unlink must be durable too.
	return RemoveLegacyRowDurably(outbox, name, true)
}

// removeLedgerRowDurably removes name relative to an already-open directory and fsyncs that SAME
// directory before reporting success. Opening the directory first is load-bearing: os.Remove on
// "<missing-parent>/<name>" returns ENOENT too, but a missing mount point may still hide a durable row
// that will reappear later. os.Root keeps the operation bound to the directory that was actually opened
// even if its path is renamed while cleanup runs.
func RemoveLegacyRowDurably(dir, name string, missingDirOK bool) error {
	root, err := os.OpenRoot(dir)
	if err != nil {
		if missingDirOK && os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer func() { _ = root.Close() }()

	d, err := root.Open(".")
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()

	if err := root.Remove(name); err != nil && !os.IsNotExist(err) {
		return err
	}
	// F6: Remove returning nil (or a child ENOENT inside this known directory) is not a durable
	// retirement until the directory entry state itself reaches stable storage.
	return d.Sync()
}

// removeXferInflight is the commit-callback face of consumeXferLedgerRow: a terminal has just been
// committed, so both credentials should go. A failure is NOT silently swallowed — it means the outbox row
// is deliberately still there, and recovery will re-drive this transfer from the exact terminal.
func (b *Legacy) Remove(transferID string) {
	if err := b.Consume(transferID); err != nil {
		b.Config.Logger.Warn("broker: could not retire this transfer's ledger rows after its terminal committed "+
			"— the EXACT terminal is deliberately kept in the outbox so a later pass re-drives it rather than "+
			"synthesizing a contradictory one",
			"err", err, "transfer_id", transferID)
	}
}

// finalizeStrandedXfers scans the ledger and, for every transfer whose home broker crashed mid-flight (the
// ledger file is older than its tier timeout + slack AND no live tracker entry exists), emits a
// DETERMINISTIC synthetic terminal (Kind=failed, Code=home_broker_restart, Ts=startedAt+timeout) through the
// normal audit path, deletes any orphan object, and removes the ledger file. The deterministic Ts makes the
// content-addressed TransferRecordReqID collapse every re-emit/crash-window duplicate in the replicated
// dedup ledger. Returns the number of transfers finalized.
// forEachLedgerRecord walks one ledger directory, handing each readable record to fn. It owns the
// housekeeping that must happen on EVERY ledger directory: pruning orphaned atomic temps and
// quarantining unreadable rows. Round-4 factored it out so the fallback outbox gets exactly the same
// treatment as the primary in-flight directory rather than a thinner copy of it.
//
// It also returns the set of ledger filenames that EXIST but could not be turned into a record —
// either unparseable now, or quarantined as <name>.corrupt by an earlier pass. That set is what stops
// an unreadable row from reading as an ABSENT row (B8). The keys are canonical `<hash>.json` names:
// the hash is one-way, so a caller cannot recover a transfer_id from one, but a caller that HAS a
// transfer_id can compute the same name forward and ask.
//
// The `.corrupt` half is what makes the answer durable. Quarantining renames the row out of the
// directory, so a set built only from parse failures would be non-empty on the pass that quarantined
// the row and EMPTY on every pass after it — the synthesizer would simply be delayed by one tick
// instead of blocked.
func (b *Legacy) EachRecord(dir string, fn func(path string, rec LegacyRecord)) (map[string]bool, error) {
	unresolved := map[string]bool{}
	if dir == "" {
		return unresolved, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return unresolved, nil
		}
		return unresolved, err
	}
	for _, de := range entries {
		if de.IsDir() {
			continue
		}
		if !strings.HasSuffix(de.Name(), ".json") {
			// A row quarantined by an earlier pass still means "a ledger row for this transfer exists and
			// this process could not read it". Fold it back under its canonical name so the answer
			// survives the rename that put it here.
			if base, _, ok := strings.Cut(de.Name(), ".json.corrupt"); ok {
				unresolved[base+".json"] = true
				continue
			}
			// nit: prune an orphaned atomic temp (a crash between CreateTemp and Rename) once it is clearly
			// stale, so .xfer-inflight-*.tmp files do not accumulate unbounded.
			if strings.HasPrefix(de.Name(), ".xfer-inflight-") && strings.HasSuffix(de.Name(), ".tmp") {
				if info, ierr := de.Info(); ierr == nil && b.Config.Now().Sub(info.ModTime()) > LegacyStrandedSlack {
					_ = os.Remove(filepath.Join(dir, de.Name()))
				}
			}
			continue
		}
		path := filepath.Join(dir, de.Name())
		rec, rerr := ReadLegacyRecord(path)
		if rerr != nil {
			// Record it BEFORE the quarantine: from here on this row exists only under a .corrupt name.
			unresolved[de.Name()] = true
			// nit: a corrupt ledger would re-warn every pass forever — move it aside ONCE (preserved for
			// inspection) so the log doesn't churn and the finalizer stops re-reading it.
			// EXTERNAL REVIEW F6: when a .corrupt target already existed this left the NEW bad file in
			// place and still logged "moved aside", so every pass re-read it forever while the message
			// said it had been dealt with. Give each one a distinct name, and say what actually happened.
			corrupt := path + ".corrupt"
			if _, se := os.Stat(corrupt); se == nil {
				corrupt = path + ".corrupt." + strconv.FormatInt(b.Config.Now().UnixNano(), 10)
			}
			if merr := os.Rename(path, corrupt); merr != nil {
				b.Config.Logger.Warn("broker: unreadable xfer-inflight ledger could NOT be moved aside — it will be re-read every pass",
					"file", de.Name(), "err", rerr, "move_err", merr)
				continue
			}
			_ = legacySyncDir(dir)
			b.Config.Logger.Warn("broker: unreadable xfer-inflight ledger — moved aside for inspection", "file", de.Name(), "moved_to", filepath.Base(corrupt), "err", rerr)
			continue
		}
		fn(path, rec)
	}
	return unresolved, nil
}
