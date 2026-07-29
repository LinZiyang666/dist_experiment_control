package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xferaudit"
)

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
const xferStrandedSlack = 60 * time.Second

// xferInflightRecord is the persisted immutable projection of a transferEntry — everything the finalizer
// needs to synthesize a byte-deterministic terminal audit row for a stranded transfer.
type xferInflightRecord struct {
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
}

func (b *Broker) xferInflightDir() string {
	if b.cfg.ClusterDataDir == "" {
		return ""
	}
	return filepath.Join(b.cfg.ClusterDataDir, "xfer-inflight")
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
func (b *Broker) xferTerminalOutboxDir() string {
	if b.cfg.ClusterDataDir == "" {
		return ""
	}
	return filepath.Join(b.cfg.ClusterDataDir, "xfer-terminal-outbox")
}

// xferInflightFilename hashes the transfer_id into a path-safe fixed-width name so a transfer_id can never
// escape the ledger dir (path traversal) or collide with the atomic temp files.
func xferInflightFilename(transferID string) string {
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
func transferTimeoutFor(tier string, size int64) time.Duration {
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
func transferTierFloorFor(tier string) time.Duration {
	if tier == "b" {
		return transferTimeoutTierB
	}
	return transferTimeoutTierA
}

// writeLedgerRecord atomically and DURABLY installs a ledger record: temp -> write -> fsync(file) ->
// close -> rename -> fsync(dir). External review F6: the previous version did Write/Close/Rename with no
// sync at all, so a power loss or kernel crash could lose either the bytes or the directory entry while
// the doc comment called the ledger "durable". A ledger that can vanish is exactly the evidence #57 needs.
func (b *Broker) writeLedgerRecord(dir string, rec xferInflightRecord) error {
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
	if rerr := os.Rename(tmpName, filepath.Join(dir, xferInflightFilename(rec.TransferID))); rerr != nil {
		_ = os.Remove(tmpName)
		return rerr
	}
	return syncDir(dir) // F6: the rename itself must be durable, not just the bytes
}

// syncDir fsyncs a directory so a rename/unlink inside it is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

// stageXferInflightTerminal writes the DECIDED terminal record into the ledger before it is forwarded
// (external RE-REVIEW F1). Returns false when nothing was staged (no ledger in single-broker mode, or the
// write failed) so the caller can fall back to the pre-outbox behaviour rather than silently lose a row.
// Returns (staged, applicable): applicable=false means this deployment keeps no ledger at all
// (single-broker mode), which is NOT a failure; staged=false with applicable=true is a real durability
// failure and the caller must NOT proceed to an async forward (round-3 R3-F2).
func (b *Broker) stageXferInflightTerminal(transferID string, rec schema.AuditTransfer) (staged, applicable bool) {
	dir := b.xferInflightDir()
	if dir == "" {
		return false, false // no ledger in this mode — nothing to stage, nothing at risk
	}
	cur, err := readXferInflight(filepath.Join(dir, xferInflightFilename(transferID)))
	if err != nil {
		// No start record (single-mode, pre-ledger transfer, or an unreadable file): stage a
		// self-contained record so recovery can still replay the exact terminal.
		// batch C: Size is carried here too. This is the SECOND writer of an xferInflightRecord, and
		// the one easy to miss — a Size filled only on the start path would leave every
		// synthesized-terminal record at 0 and quietly re-fix the budget at the tier floor.
		cur = xferInflightRecord{TransferID: transferID, Session: rec.Session, Node: rec.Node,
			ActorNkey: rec.ActorNkey, ActorFp: rec.ActorFp, Verb: rec.Verb, Tier: rec.Tier,
			Bucket: rec.Bucket, Path: rec.Path, StartedAt: rec.Ts, Size: rec.Bytes}
	}
	cur.Terminal = &rec
	werr := b.writeLedgerRecord(dir, cur)
	if werr == nil {
		return true, true
	}
	// ROUND-4 RE-REVIEW: do not conclude "unstageable" from ONE unwritable directory. Retry in the sibling
	// outbox — that record carries the EXACT terminal, so recovery replays it byte-for-byte instead of
	// inventing a synthetic one that contradicts it.
	outbox := b.xferTerminalOutboxDir()
	if outbox == "" {
		b.cfg.Logger.Error("broker: could not stage the terminal into the xfer-inflight ledger",
			"err", werr, "tid", transferID, "kind", rec.Kind)
		return false, true
	}
	if oerr := b.writeLedgerRecord(outbox, cur); oerr != nil {
		b.cfg.Logger.Error("broker: could not stage the terminal in EITHER the xfer-inflight ledger or the "+
			"fallback outbox — recovery will have no record of this outcome",
			"err", werr, "outbox_err", oerr, "tid", transferID, "kind", rec.Kind)
		return false, true
	}
	b.cfg.Logger.Warn("broker: the xfer-inflight ledger is unwritable — staged this transfer's terminal in the "+
		"fallback outbox instead, so recovery can still replay it exactly",
		"err", werr, "tid", transferID, "kind", rec.Kind)
	return true, true
}

// writeXferInflight persists the in-flight ledger record for e (called right after transfers.put(), before
// the prepare is forwarded — a crash between put() and this loses only what a crash before put() already
// does). Best-effort + atomic (temp + rename): a failure only degrades #57 recovery, never the transfer.
func (b *Broker) writeXferInflight(e *transferEntry) {
	dir := b.xferInflightDir()
	if dir == "" {
		return // non-cluster: no durable ledger (single-broker audit publish is best-effort anyway)
	}
	if err := b.writeLedgerRecord(dir, xferInflightRecord{
		TransferID: e.transferID, Session: e.sid, Node: e.nid,
		ActorNkey: e.actor, ActorFp: e.actorFP, Verb: e.verb, Tier: e.tier,
		Bucket: e.bucket, Path: e.path, StartedAt: e.startedAt, Size: e.size,
	}); err != nil {
		b.cfg.Logger.Warn("broker: install xfer-inflight ledger", "err", err)
		return
	}
}

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
func (b *Broker) consumeXferLedgerRow(transferID string) error {
	dir := b.xferInflightDir()
	if dir == "" {
		return nil
	}
	name := xferInflightFilename(transferID)
	if err := removeLedgerRowDurably(dir, name, false); err != nil {
		// The start row may survive. KEEP the outbox terminal: it is the tombstone that makes the next
		// pass replay the TRUE outcome instead of inventing one. In particular, ENOENT for the parent
		// directory is not proof that a row hidden by an unmounted/replaced directory is gone.
		return err
	}
	outbox := b.xferTerminalOutboxDir()
	if outbox == "" {
		return nil
	}
	// Once the primary row is durably absent, a missing outbox directory is harmless: there is no start
	// row left for recovery to misinterpret, and any outbox terminal exposed by a later remount replays
	// the already-committed exact bytes. If the directory exists, however, its unlink must be durable too.
	return removeLedgerRowDurably(outbox, name, true)
}

// removeLedgerRowDurably removes name relative to an already-open directory and fsyncs that SAME
// directory before reporting success. Opening the directory first is load-bearing: os.Remove on
// "<missing-parent>/<name>" returns ENOENT too, but a missing mount point may still hide a durable row
// that will reappear later. os.Root keeps the operation bound to the directory that was actually opened
// even if its path is renamed while cleanup runs.
func removeLedgerRowDurably(dir, name string, missingDirOK bool) error {
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
func (b *Broker) removeXferInflight(transferID string) {
	if err := b.consumeXferLedgerRow(transferID); err != nil {
		b.cfg.Logger.Warn("broker: could not retire this transfer's ledger rows after its terminal committed "+
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
func (b *Broker) forEachLedgerRecord(dir string, fn func(path string, rec xferInflightRecord)) (map[string]bool, error) {
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
				if info, ierr := de.Info(); ierr == nil && b.cfg.Now().Sub(info.ModTime()) > xferStrandedSlack {
					_ = os.Remove(filepath.Join(dir, de.Name()))
				}
			}
			continue
		}
		path := filepath.Join(dir, de.Name())
		rec, rerr := readXferInflight(path)
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
				corrupt = path + ".corrupt." + strconv.FormatInt(b.cfg.Now().UnixNano(), 10)
			}
			if merr := os.Rename(path, corrupt); merr != nil {
				b.cfg.Logger.Warn("broker: unreadable xfer-inflight ledger could NOT be moved aside — it will be re-read every pass",
					"file", de.Name(), "err", rerr, "move_err", merr)
				continue
			}
			_ = syncDir(dir)
			b.cfg.Logger.Warn("broker: unreadable xfer-inflight ledger — moved aside for inspection", "file", de.Name(), "moved_to", filepath.Base(corrupt), "err", rerr)
			continue
		}
		fn(path, rec)
	}
	return unresolved, nil
}

// replayStagedTerminal handles a ledger row that already carries the EXACT terminal, from either the
// primary in-flight directory or the fallback outbox. It reports whether the row was disposed of;
// false means RETAINED for a later pass.
// window: the real terminal may already be committed and the process died before the unlink, so
// emitting anything ELSE would produce a second, contradictory terminal that the content-addressed
// reqID cannot collapse. Replaying the identical record is idempotent by construction — and it is
// also the correct answer for the PRE-commit crash, where this row is simply the terminal that was
// decided but never forwarded. No timeout grace applies: the outcome is already decided, so there
// is nothing to wait for.
func (b *Broker) replayStagedTerminal(ctx context.Context, rec xferInflightRecord) bool {
	// DETECT A PRIOR COMMIT (re-review F1). The staged row may already be committed — that is
	// precisely the post-commit crash window. Its reqID is content-addressed, so asking the
	// replicated dedup ledger is an exact answer, and its retention (~1M raft indices, months)
	// dwarfs the 2-minute JetStream Duplicates window a replay would otherwise rely on.
	// THE AUDIT INVARIANT, stated (re-review 疑惑 #1): EXACTLY ONE TRUE TERMINAL per transfer.
	// A contradictory pair is worse than a dangling start — a dangling start is detectable and
	// repairable, whereas two terminals claiming different outcomes corrupt the record itself and
	// no consumer can tell which is true. So a staged terminal is re-emitted ONLY when we can
	// positively establish it was NOT already committed. "Cannot verify" therefore means DROP,
	// not replay. In production the check is always available (the ledger dir exists only in
	// cluster mode, where b.cl is wired), so this branch costs nothing there; it is loud because
	// reaching it means the two are out of step.
	// ROUND-3 R3-F1: only a POSITIVE "already committed" answer may drop the outbox.
	//
	// The previous version folded a lookup ERROR into "seen" and deleted the ledger. That is the
	// worst of both worlds: a transient SQLite/disk/closed-DB error is not evidence of a commit,
	// so if the terminal had NOT committed we destroyed the only exact record of it and left a
	// permanent dangling start — reopening #57, the very defect this file exists to close. It also
	// contradicted this file's own comment, which correctly says an unknown state must degrade to
	// an exact replay.
	//
	// So the three states are now distinct:
	//   committed      -> drop the outbox (nothing more to write)
	//   NOT committed  -> replay the EXACT staged terminal (identical bytes ⇒ identical reqID)
	//   unknown        -> RETAIN and retry on the next pass; never emit, never delete
	seen, serr := b.transferTerminalAlreadyCommitted(*rec.Terminal)
	if serr != nil || b.cl == nil {
		b.cfg.Logger.Warn("broker: cannot determine whether a staged transfer terminal was already committed — RETAINING the outbox for the next pass (deleting it could strand a transfer with no terminal at all)",
			"transfer_id", rec.TransferID, "kind", rec.Terminal.Kind, "err", serr)
		return false
	}
	if seen {
		_ = b.deleteXferObject(ctx, rec.Session, rec.TransferID)
		if cErr := b.consumeXferLedgerRow(rec.TransferID); cErr != nil {
			b.cfg.Logger.Warn("broker: a staged terminal was already committed but its ledger rows could not "+
				"be retired — RETAINING both so the start row can never be synthesized behind it",
				"err", cErr, "transfer_id", rec.TransferID)
			return false
		}
		b.cfg.Logger.Info("broker: staged transfer terminal was already committed before the crash — dropping the ledger without re-emitting (#57)",
			"transfer_id", rec.TransferID, "kind", rec.Terminal.Kind)
		return true
	}
	if !b.commitSyntheticTransferTerminal(*rec.Terminal) {
		b.cfg.Logger.Warn("broker: could not durably commit a STAGED transfer terminal (leaderless?) — leaving the ledger for the next pass (#57)",
			"transfer_id", rec.TransferID, "kind", rec.Terminal.Kind)
		return false
	}
	_ = b.deleteXferObject(ctx, rec.Session, rec.TransferID)
	if cErr := b.consumeXferLedgerRow(rec.TransferID); cErr != nil {
		b.cfg.Logger.Warn("broker: replayed a staged terminal but could not retire its ledger rows — RETAINING "+
			"both; the replay is idempotent (same bytes ⇒ same reqID), a stranded start row would not be",
			"err", cErr, "transfer_id", rec.TransferID)
		return false
	}
	return true
}

// ledgerSite says which of the two sibling directories a row was read from.
type ledgerSite int

const (
	ledgerSiteOutbox  ledgerSite = iota // the fallback staging dir — scanned FIRST (pass 1)
	ledgerSitePrimary                   // the in-flight dir (pass 2)
)

// ledgerDisposition is what a pass must do with ONE ledger row.
type ledgerDisposition int

const (
	ledgerLeave      ledgerDisposition = iota // retain the row untouched; a later pass re-decides
	ledgerReplay                              // replay the EXACT staged terminal, then consume both rows
	ledgerSynthesize                          // no terminal exists anywhere — synthesize a deterministic one
)

// ledgerRowInputs is everything the precedence rule needs about ONE row. Assembling it is the
// caller's job (it owns the tracker, the clock and the outbox census); deciding is this file's.
type ledgerRowInputs struct {
	Site ledgerSite
	Rec  xferInflightRecord

	// Live: a tracker entry exists for this transfer in THIS incarnation.
	Live bool
	// OutboxHasExactTerminal: the outbox census found a readable row carrying an exact terminal for
	// this transfer. Only consulted for a primary start-only row.
	OutboxHasExactTerminal bool
	// OutboxCensusOK: the outbox scan completed. False ⇒ absence there is UNKNOWN, not empty (R6-F1).
	OutboxCensusOK bool
	// OutboxRowUnreadable: the outbox HOLDS a row for this transfer that this process could not turn
	// into a record (unparseable now, or quarantined as .corrupt earlier). The ROW-level twin of
	// OutboxCensusOK: the directory read fine, one file did not. Absence of a parsed terminal is then
	// not evidence of an absent terminal, and the outbox stages terminals ONLY — so whatever is in
	// there is a decided outcome that outranks a start row.
	OutboxRowUnreadable bool
	Now                 time.Time
}

// ledgerRowDisposition applies the durable-precedence rule documented at the top of this file to one
// row. It is PURE — no disk, no clock, no tracker — so every legal combination is reachable from a
// table-driven test instead of only from a crash sequence that has to be staged on disk.
//
// The returned reason is the rule that fired. Production puts it in the finalize log line, and the
// table-driven test asserts on it, so a branch that is reached for the wrong reason is visible rather
// than merely producing the right answer by luck.
func ledgerRowDisposition(in ledgerRowInputs) (ledgerDisposition, string) {
	// A live tracker entry outranks everything at BOTH sites: the transfer is genuinely in flight in
	// this incarnation and its own terminal path will write the terminal and retire the ledger.
	if in.Live {
		return ledgerLeave, "live tracker entry — the in-flight terminal path owns this transfer"
	}

	if in.Site == ledgerSiteOutbox {
		// The outbox only ever stages DECIDED terminals. A row here without one is not a start row to
		// be finalized — it is a row this pass has nothing to say about.
		if in.Rec.Terminal == nil {
			return ledgerLeave, "outbox row carries no terminal — the outbox stages terminals only"
		}
		return ledgerReplay, "exact terminal staged in the outbox"
	}

	// Primary site. A terminal staged in place is replayed without consulting the outbox at all: an
	// exact terminal is an exact terminal wherever it lives, and re-emitting the identical record is
	// idempotent by construction (content-addressed reqID).
	if in.Rec.Terminal != nil {
		return ledgerReplay, "exact terminal staged in place"
	}
	// From here the row is start-only, i.e. the only path that can MANUFACTURE a terminal. Every gate
	// below is fail-closed by design.
	if !in.OutboxCensusOK {
		return ledgerLeave, "outbox census failed — a terminal there would outrank this start row, and " +
			"absence in an unreadable directory is UNKNOWN rather than empty (R6-F1)"
	}
	if in.OutboxHasExactTerminal {
		return ledgerLeave, "an exact terminal in the outbox outranks this start row"
	}
	if in.OutboxRowUnreadable {
		return ledgerLeave, "the outbox holds an UNREADABLE row for this transfer — the outbox stages " +
			"terminals only, so that row is a decided outcome this process cannot read, and synthesizing " +
			"beside it would commit a terminal that contradicts evidence still on disk"
	}
	if in.Now.Sub(in.Rec.StartedAt) < transferTimeoutFor(in.Rec.Tier, in.Rec.Size)+xferStrandedSlack {
		return ledgerLeave, "younger than its tier timeout + slack — slow, not stranded"
	}
	return ledgerSynthesize, "stranded past timeout + slack with no exact terminal at either site"
}

// ledgerProtectedObjects returns the object names the DURABLE ledger says are still inside their
// watchdog budget, keyed by "<bucket>/<object>". The object name IS the transfer id at every writer
// (the ctl's Put and the agent's Put both use it), so this is an exact identity match, not a heuristic.
//
// origin: batch-c EXTERNAL review B2. The orphan reaper decided "nobody owns this object" from the
// IN-MEMORY tracker alone — which a restart rebuilds EMPTY. With the tier-B budget now size-derived
// (up to 35m08s) and the home grace still 2 minutes, a broker restart during a large transfer meant
// the reaper deleted an object whose transfer was very much alive, roughly two minutes in. The
// internal review had recorded that as an accepted residual on the grounds that there was no durable
// evidence of ownership; that was simply wrong. This ledger has existed since #57 and already carries
// the transfer id, bucket, size and start time — everything the decision needs.
//
// Read errors yield an EMPTY set and are reported. The caller must treat "I could not read the
// ledger" as "do not delete anything on evidence I do not have", never as "nothing is protected".
func (b *Broker) ledgerProtectedObjects(now time.Time) (map[string]bool, error) {
	protected := map[string]bool{}
	var firstErr error
	for _, dir := range []string{b.xferInflightDir(), b.xferTerminalOutboxDir()} {
		if dir == "" {
			continue
		}
		unresolved, err := b.forEachLedgerRecord(dir, func(_ string, rec xferInflightRecord) {
			// A row carrying a TERMINAL is a decided outcome — its object is disposable.
			if rec.Terminal != nil || rec.Bucket == "" || rec.TransferID == "" {
				return
			}
			if rec.StartedAt.IsZero() {
				// No start time to judge against. Protect it: a row we cannot age is exactly the kind of
				// evidence that must not be resolved in favour of deletion.
				protected[rec.Bucket+"/"+rec.TransferID] = true
				return
			}
			budget := transferTimeoutFor(rec.Tier, rec.Size) + xferStrandedSlack
			if now.Sub(rec.StartedAt) < budget {
				protected[rec.Bucket+"/"+rec.TransferID] = true
			}
		})
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if len(unresolved) > 0 && firstErr == nil {
			firstErr = fmt.Errorf("%d unreadable ledger row(s) in %s", len(unresolved), dir)
		}
	}
	return protected, firstErr
}

func (b *Broker) finalizeStrandedXfers(ctx context.Context) (int, error) {
	dir := b.xferInflightDir()
	if dir == "" {
		return 0, nil
	}
	outboxDir := b.xferTerminalOutboxDir()
	finalized := 0

	// PASS 1 — THE OUTBOX, AND IT RUNS FIRST (round-5 R5-F2).
	//
	// These rows carry the EXACT terminal, so replaying them needs no guessing and no grace period. More
	// importantly, the outbox exists precisely BECAUSE the primary directory can be unusable — so making
	// its recovery depend on scanning the primary directory first would disable the fallback in the one
	// state it was built for. A primary scan error must never preempt this pass.
	outboxOwned := map[string]bool{}
	obUnreadable, obErr := b.forEachLedgerRecord(outboxDir, func(_ string, rec xferInflightRecord) {
		// The census is maintained regardless of what this row's disposition turns out to be — even a
		// row that is left alone here (a live transfer) still means an exact terminal EXISTS at this
		// site, and pass 2 must not synthesize one from the start row it will find.
		if rec.Terminal != nil {
			outboxOwned[rec.TransferID] = true
		}
		d, _ := ledgerRowDisposition(ledgerRowInputs{
			Site: ledgerSiteOutbox,
			Rec:  rec,
			Live: b.transfers.get(rec.TransferID) != nil,
			Now:  b.cfg.Now(),
		})
		if d == ledgerReplay && b.replayStagedTerminal(ctx, rec) {
			finalized++
		}
	})

	// blockedByUnreadable collects transfers that can never be finalized while an unreadable outbox row
	// outranks their start row. Reported once per pass below — see the ledgerLeave branch.
	var blockedByUnreadable []string

	// PASS 2 — the primary directory: staged terminals, then start-only rows old enough to synthesize.
	_, prErr := b.forEachLedgerRecord(dir, func(_ string, rec xferInflightRecord) {
		in := ledgerRowInputs{
			Site:                   ledgerSitePrimary,
			Rec:                    rec,
			Live:                   b.transfers.get(rec.TransferID) != nil,
			OutboxHasExactTerminal: outboxOwned[rec.TransferID],
			OutboxCensusOK:         obErr == nil,
			// The hash is one-way, so the census cannot name the transfers it failed to read — but this
			// side HAS the transfer_id and computes the same filename forward.
			OutboxRowUnreadable: obUnreadable[xferInflightFilename(rec.TransferID)],
			Now:                 b.cfg.Now(),
		}
		d, why := ledgerRowDisposition(in)
		switch d {
		case ledgerLeave:
			// A row left alone because an UNREADABLE outbox row outranks it is a PERMANENT state, and it
			// must not be a silent one (internal review B8-1).
			//
			// Every other ledgerLeave reason resolves itself: a live transfer finishes, a fresh one ages
			// out, a failed census succeeds on the next pass. This one does not. The unreadable row was
			// quarantined to <name>.corrupt on the pass that found it, the census folds that name back in
			// forever, and nothing removes either file — so the transfer has ZERO terminals in the audit
			// (the true one is in a file no code path will read again, the synthetic one is blocked) and
			// the ledger row is immortal.
			//
			// That is still the right disposition: this file's own argument at replayStagedTerminal is
			// that a dangling start is "detectable and repairable" while a contradictory pair corrupts the
			// record. But "detectable" is a property something has to provide. One Warn on the pass that
			// quarantined the row, possibly weeks ago and possibly rotated away, does not.
			//
			// So: count it, and report it once per pass at Warn with the operator's next step. It is
			// bounded — one line per pass, not per row — and it names the directory to look in.
			if in.OutboxRowUnreadable {
				blockedByUnreadable = append(blockedByUnreadable, rec.TransferID)
			}
			return
		case ledgerReplay:
			if b.replayStagedTerminal(ctx, rec) {
				finalized++
			}
			return
		}
		// DELIBERATELY the tier floor, not the size-derived budget — see transferTierFloorFor.
		timeout := transferTierFloorFor(rec.Tier)
		synthetic := schema.AuditTransfer{
			V: schema.AuditSchemaVersion, Kind: "failed", Verb: rec.Verb,
			Ts: rec.StartedAt.Add(timeout), Session: rec.Session, Node: rec.Node,
			ActorNkey: rec.ActorNkey, ActorFp: rec.ActorFp,
			TransferID: rec.TransferID, Path: rec.Path, Tier: rec.Tier, Bucket: rec.Bucket,
			DurationMs: timeout.Milliseconds(),
			Code:       "home_broker_restart",
			Error:      "broker home restarted while the transfer was in flight; the crashed incarnation wrote no terminal audit (#57 finalize-on-recovery)",
		}
		// M1: the ledger may be deleted ONLY after the synthetic terminal is durably COMMITTED. A
		// fire-and-forget emit would destroy the durable evidence in the leaderless post-crash window
		// (exactly #57's flagship case) — so on a not-yet-committable emit we LEAVE the file and the next
		// pass retries (the deterministic Ts makes the content reqID dedup the re-emit for free).
		if !b.commitSyntheticTransferTerminal(synthetic) {
			b.cfg.Logger.Warn("broker: could not durably commit a stranded-transfer terminal (leaderless?) — leaving the ledger for the next pass (#57)",
				"transfer_id", rec.TransferID, "verb", rec.Verb, "tier", rec.Tier)
			return
		}
		_ = b.deleteXferObject(ctx, rec.Session, rec.TransferID)
		if cErr := b.consumeXferLedgerRow(rec.TransferID); cErr != nil {
			// The synthetic is deterministic (Ts = startedAt+timeout), so a re-emit next pass dedups on
			// content. Leaving the row is therefore safe; losing it would not be.
			b.cfg.Logger.Warn("broker: committed a stranded-transfer terminal but could not retire its ledger row",
				"err", cErr, "transfer_id", rec.TransferID)
		}
		finalized++
		b.cfg.Logger.Info("broker: finalized a stranded in-flight transfer after a home restart (#57)",
			"transfer_id", rec.TransferID, "verb", rec.Verb, "tier", rec.Tier, "session", rec.Session,
			"reason", why)
	})
	// A scan failure of ONE source is a logged, retried condition — not a pass failure. Returning it would
	// hand the caller (the reconcile pass) an error for a run in which the other source made real progress,
	// and would keep doing so for as long as the broken directory stays broken. Only a run in which NO
	// source could be read at all is a genuine failure worth propagating.
	// B8-1: the one ledgerLeave that never resolves itself, surfaced once per pass rather than once ever.
	// Bounded to one line and a capped sample so a hundred stuck rows do not become a hundred log lines.
	if len(blockedByUnreadable) > 0 {
		sample := blockedByUnreadable
		if len(sample) > 5 {
			sample = sample[:5]
		}
		// EXTERNAL REVIEW B2-3: both halves of the previous version of this text were FALSE, and the second
		// half was destructive.
		//
		//   (a) "repair it in place (the next pass replays it)" — forEachLedgerRecord only ever calls fn for
		//       entries ending in ".json" (see the suffix check at the top of its loop). A repaired
		//       *.json.corrupt keeps a name the replay walk skips, so editing the bytes alone changes
		//       nothing, forever. The file has to be RENAMED back to its canonical <hash>.json.
		//
		//   (b) "remove BOTH the .corrupt file and the matching in-flight row" — the in-flight start row is
		//       the ONLY record synthesis can derive a deterministic terminal from (transfer id, verb, tier,
		//       bucket, path, startedAt). Deleting it alongside the corrupt outbox row leaves neither a real
		//       terminal nor anything to synthesize one from: the transfer becomes permanently unauditable,
		//       which is worse than the state this warning is reporting. Removing ONLY the corrupt outbox row
		//       is what unblocks synthesis — the census stops marking the name unresolved, the start row is
		//       then a plain stale start row, and the next pass writes failed/home_broker_restart for it.
		//
		// Stated as two numbered operator actions rather than prose, because the difference between them is
		// one filename and the cost of getting it wrong is an audit record that can never be reconstructed.
		b.cfg.Logger.Warn("broker: transfer(s) can never be finalized — an UNREADABLE terminal is staged in "+
			"the outbox and outranks their in-flight row, so no terminal audit will ever be written for them. "+
			"Inspect the quarantined *.json.corrupt files in outbox_dir. (1) RECOVERABLE: repair the JSON and "+
			"RENAME it back to its canonical <hash>.json name — a repaired file left under the .corrupt name is "+
			"never replayed, because the ledger walk only reads *.json. (2) UNRECOVERABLE: delete ONLY the "+
			".corrupt file in outbox_dir and KEEP the matching row in inflight_dir — that row is the only input "+
			"a synthesized terminal can be derived from, so deleting it makes the transfer permanently "+
			"unauditable instead of repairing it",
			"count", len(blockedByUnreadable), "sample_transfer_ids", sample,
			"outbox_dir", outboxDir, "inflight_dir", dir)
	}
	if obErr != nil {
		b.cfg.Logger.Warn("broker: could not scan the transfer terminal outbox — retrying on the next pass",
			"err", obErr, "dir", outboxDir)
	}
	if prErr != nil {
		b.cfg.Logger.Warn("broker: could not scan the xfer-inflight ledger — retrying on the next pass; any "+
			"exact terminals staged in the fallback outbox were still replayed",
			"err", prErr, "dir", dir)
	}
	if obErr != nil && prErr != nil {
		return finalized, errors.Join(obErr, prErr)
	}
	return finalized, nil
}

// commitSyntheticTransferTerminal SYNCHRONOUSLY forwards a #57 synthetic terminal to the leader and reports
// whether it durably COMMITTED. It uses transferAuditForwardSync (fwd.Forward, error-returning) with the
// same bounded transient-retry as the async sink, so a brief election is absorbed within the pass; a
// persistent not_leader / permanent error returns false and the caller retains the ledger for the next pass.
// Returns false when the sink is not yet attached (never delete the ledger on an unconfirmed emit).
func (b *Broker) commitSyntheticTransferTerminal(rec schema.AuditTransfer) bool {
	fwd := b.transferAuditForwardSync
	if fwd == nil {
		return false // sink not attached yet (pre-wireClusterLate) — retry next pass, never lose the ledger
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		b.cfg.Logger.Warn("broker: marshal synthetic transfer terminal", "err", err, "tid", rec.TransferID)
		return false
	}
	for attempt := 0; attempt < transferAuditForwardAttempts; attempt++ {
		ferr := fwd(payload)
		if ferr == nil {
			return true // committed (content reqID dedups any earlier partial attempt)
		}
		if !errors.Is(ferr, cluster.ErrForwardNotLeader) {
			b.cfg.Logger.Warn("broker: synthetic transfer terminal forward failed (retry next pass)", "err", ferr, "tid", rec.TransferID)
			return false
		}
		time.Sleep(transferAuditForwardBackoff)
	}
	return false // persistent not_leader — retry on the next pass
}

func readXferInflight(path string) (xferInflightRecord, error) {
	var rec xferInflightRecord
	data, err := os.ReadFile(path)
	if err != nil {
		return rec, err
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		return rec, err
	}
	// EXTERNAL REVIEW F6: a decode success is not a valid record. `{}` decodes fine and would drive the
	// finalizer with an empty transfer id / zero StartedAt, which then looks infinitely "stranded". Treat
	// a structurally invalid record as unreadable so it takes the .corrupt path instead.
	if rec.TransferID == "" || rec.Session == "" || rec.Verb == "" || rec.Tier == "" || rec.StartedAt.IsZero() {
		return xferInflightRecord{}, fmt.Errorf("xfer-inflight: incomplete ledger record (transfer_id=%q session=%q verb=%q tier=%q started_at=%v)",
			rec.TransferID, rec.Session, rec.Verb, rec.Tier, rec.StartedAt)
	}
	return rec, nil
}

// transferTerminalAlreadyCommitted asks the replicated dedup ledger whether this exact terminal record
// has already been committed (external RE-REVIEW F1). A nil cluster runtime (single-broker mode) or an
// unreadable ledger answers "unknown" -> false, which degrades to the replay path: replaying is still
// idempotent at Apply, so an unknown answer is safe, never a double-count.
func (b *Broker) transferTerminalAlreadyCommitted(rec schema.AuditTransfer) (bool, error) {
	if b.cl == nil || b.cl.node == nil {
		return false, nil
	}
	reqID, err := xferaudit.TransferRecordReqID(rec)
	if err != nil {
		return false, err
	}
	return cluster.ReqIDCommitted(b.cl.node.RODB(), reqID)
}
