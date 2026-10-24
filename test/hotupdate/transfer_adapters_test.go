//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"
)

// The real binaries acquire observation only from inherited sealed custody.
// These overlays bind the existing ports at the landed production construction
// sites. They leave business predicates and timing intact; evidence is copied
// from the owner at its actual callback, never inferred from eventual bytes.
// The remaining adapters leave parallel owners' source files unchanged.
// origin: S7 internal review INTEG-M F1 / hot-update S7 plan §10.4
func transferObservationOverlay(t *testing.T, root, dir string, replacements map[string]string) {
	t.Helper()
	patch := func(relative string, edits [][2]string, suffix string) {
		t.Helper()
		transferPatchSource(t, root, dir, replacements, relative, edits, suffix)
	}
	const drillImport = `"github.com/LinZiyang666/tether/internal/drillobserve"`
	// Port zero belongs to the broker's real listener. Report its actual bound
	// address so parallel fixtures never close and race to reclaim a free port.
	// origin: S7 internal review INTEG-M F6
	patch("internal/tunnel/tunnel.go", [][2]string{
		{`s.logger.Info("tunnel: server listening", "addr", s.addr)`, `s.logger.Info("tunnel: server listening", "addr", rawLn.Addr().String())`}}, "")
	// The supervisor also links drillobserve and consumes its standard enablers
	// during init. Private adapter enablers survive only until the built product
	// converts them into the same sealed startup protocol; workloads lose them.
	patch("internal/drillobserve/custody_linux.go", [][2]string{
		{`enable, descriptor := os.Getenv(EnableEnv), os.Getenv(CustodyEnv)`, `enable, descriptor := os.Getenv(EnableEnv), os.Getenv(CustodyEnv)
 if os.Getenv("S7_TRANSFER_OBSERVER") == "v1" { enable, descriptor = "v1", os.Getenv("S7_TRANSFER_CUSTODY_FD") }
 _ = os.Unsetenv("S7_TRANSFER_OBSERVER")
 _ = os.Unsetenv("S7_TRANSFER_CUSTODY_FD")`}}, "")
	patch("internal/xferserve/image.go", [][2]string{
		{`"errors"`, `"errors"` + "\n" + drillImport}}, `
// The image port already owns this sleeping, frozen tracker. Read its immutable
// exported state without creating a second tracker or opening a backend.
func (o *ImageOwner) TransferObserveImage(active bool, cutID string) {
 if drillobserve.Hook == nil { return }
 o.mu.Lock(); defer o.mu.Unlock()
 if o.state == nil || o.detached { return }
 state := *o.state
 observe := drillobserve.ProcessTransferObserver[x.Observation](state.Generation+1, "broker")
 for _, row := range state.Entries {
  id := row.Identity
  event := x.Observation{Kind: x.ObserveInstall, TransferID: id.TransferID, EntryNonce: id.EntryNonce,
   ObjectVersion: id.ObjectVersion, Phase: row.Phase, Revision: row.Revision, CutID: cutID,
   Active: active, InstalledBeforeReaper: active}
  for _, watchdog := range state.WatchdogDeadlines { if watchdog.Identity == id { event.OriginalDeadline = watchdog.ByteDeadline } }
  if row.CanonicalTerminal != nil { event.CanonicalDigest = row.CanonicalTerminal.Digest }
  observe(event)
 }
}
`)
	// Both image owners use the strict handoff executor. Startup custody joins
	// its explicit allowlist before flags open under ForkLock; failure restores
	// these originals too. The syscall still rejects every unrelated descriptor.
	// origin: S7 internal review INTEG-M F4 / hot-update S7 plan §10.4
	patch("internal/handoff/image_linux.go", [][2]string{
		{`"errors"`, `"errors"` + "\n" + drillImport},
		{`fds := append(append(make([]int, 0, len(p.fds)+1), p.fds...), p.manifestFD)`, `custodyFDs, err := drillobserve.TransferExecDescriptors()
 if err != nil { return err }
 fds := append(append(make([]int, 0, len(p.fds)+1), p.fds...), p.manifestFD)
 fds = append(fds, custodyFDs...)`},
		{`syscall.Exec(path, args, env)`, `syscall.Exec(path, args, drillobserve.ExecEnvironment(env))`}}, "")
	patch("internal/brokerimage/import.go", [][2]string{
		{`i.private.Runtime.Active = true`, `i.private.Runtime.Active = true
 if observer, ok := i.transfer.(interface{ TransferObserveImage(bool, string) }); ok { observer.TransferObserveImage(true, i.private.Runtime.CutID) }`}}, "")
	// The production composition now binds custody before constructors emit.
	// The composition retains the actual worker generation after import.
	// Preserve that owner wiring in the built artifacts; remaining leaf adapters
	// below still supply their separately owned observation ports.
	// origin: S7 internal review INTEG-O F5
	patch("internal/cli/transfer_client.go", [][2]string{
		{`"errors"`, `"errors"` + "\n" + drillImport},
		{`return cfg, transport.Close, nil`, `cfg.Observe = drillobserve.ProcessTransferObserver[x.Observation](1, "ctl")
 return cfg, transport.Close, nil`}}, "")
	patch("internal/xferhistory/publisher.go", [][2]string{
		{`Kind: xfercontract.ObserveHistory, TransferID: record.Identity.TransferID,`, `Kind: xfercontract.ObserveHistory, Phase: transferHistoryPhase(record), TransferID: record.Identity.TransferID,`}}, `
func transferHistoryPhase(record xfercontract.CanonicalRecord) xfercontract.Phase {
 if record.Kind == "start" { return xfercontract.PhasePrepared }
 return xfercontract.PhaseHistoryConfirmed
}
`)
	patch("internal/xferledger/replay.go", [][2]string{
		{`EntryNonce: nonce, Revision: row.Revision, RetirementDigest: proof.Cleanup.Digest, DirectorySynced: true`, `EntryNonce: nonce, ObjectVersion: row.Identity.ObjectVersion, Phase: xfercontract.PhaseSettled, AdmissionClosed: true, Revision: row.Revision, RetirementDigest: proof.Cleanup.Digest, DirectorySynced: true`}}, "")
	patch("internal/xfertrack/watchdog.go", [][2]string{
		{`o.sleeping, o.pristine = false, false`, `o.sleeping, o.pristine = false, false
 for _, entry := range o.entries { o.observe(x.ObserveInstall, entry) }`},
		{`Phase: e.row.Phase, OriginalDeadline: e.watch.ByteDeadline}`, `Phase: e.row.Phase, OriginalDeadline: e.watch.ByteDeadline,
 Active: !o.sleeping, InstalledBeforeReaper: kind == x.ObserveInstall, CutID: fmt.Sprintf("tracker.%d", o.state.Generation)}`}}, "")
	patch("internal/xferworker/registry.go", [][2]string{
		{`e.value.Status.Phase = x.PhaseSettled`, `e.value.Status.Phase = x.PhaseSettled
 // Retirement owns the retained exact receipt after its actors stop/join.
 // origin: S7 INTEG-O current worker retirement seam / hot-update S7 plan §10.4
 transferWorkerRetired(o, e, *e.value.Status.Terminal)`},
		{`e.value.Status.State, e.value.Status.Phase = x.StatusTerminal, x.PhaseSettled`, `e.value.Status.State, e.value.Status.Phase = x.StatusTerminal, x.PhaseSettled
 transferWorkerRetired(o, e, receipt)`},
		{`Phase: x.PhasePrepared})`, `ObjectVersion: p.Identity.ObjectVersion, OriginalDeadline: e.byteDeadline, Phase: x.PhasePrepared})`}}, `
// Called only after the original data actors and courier returned from Stop.
func transferWorkerRetired(o *owner, e *entry, receipt x.TerminalReceipt) {
 if o.cfg.Observe == nil { return }
 id := e.value.Prepare.Identity
 o.cfg.Observe(x.Observation{Kind: x.ObserveRetirement, TransferID: id.TransferID, EntryNonce: id.EntryNonce,
 ObjectVersion: id.ObjectVersion, Phase: x.PhaseSettled, AdmissionClosed: true,
 HistorySeq: receipt.HistorySeq, CanonicalDigest: receipt.CanonicalDigest, OriginalDeadline: e.byteDeadline})
}
`)
	patch("internal/xferworker/owner.go", [][2]string{
		{`observer(event)`, `for _, row := range transferWorkerObservations(o, event) { observer(row) }`},
		{`o.state.Lineage.Generation = bindings.OwnerGeneration`, `o.state.Lineage.Generation = bindings.OwnerGeneration
 if o.cfg.Observe != nil {
  for _, row := range transferWorkerObservationsLocked(o, x.Observation{Kind: x.ObserveInstall, Active: true, Generation: bindings.OwnerGeneration}) { o.cfg.Observe(row) }
 }`}}, `
func transferWorkerObservations(o *owner, event x.Observation) []x.Observation {
 o.mu.Lock(); defer o.mu.Unlock()
 return transferWorkerObservationsLocked(o, event)
}
func transferWorkerObservationsLocked(o *owner, event x.Observation) []x.Observation {
 var rows []x.Observation
 for _, entry := range o.entries {
  id := entry.value.Prepare.Identity
  if event.TransferID != "" && event.TransferID != id.TransferID { continue }
  row := transferWorkerImageObservation(o.state, entry.value, entry.byteDeadline, event)
  rows = append(rows, row)
 }
 return rows
}
`+transferObservationFunctionSource(t, root, "transferWorkerImageObservation"))
	// Worker checkpoint hashing consumes the exported SHA state, with no file IO.
	patch("internal/xferworker/owner.go", [][2]string{{`"errors"`, `"errors"` + "\n" + `"crypto/sha256"`}}, "")
	patch("internal/transferpath/transfer_atomic_unix.go", [][2]string{
		{`"errors"`, `"errors"` + "\n" + drillImport},
		{`r.state.PrimitiveOutcome.Outcome = xfercontract.OutcomeInflight
	r.emit(xfercontract.ObservePrimitiveEnter, nil)`, `// The observer selects the last unentered point before the real syscall.
 // origin: S7 internal review INTEG-M F7 / hot-update S7 plan §10.4
 r.emit(xfercontract.ObservePrimitiveEnter, nil)
 r.state.PrimitiveOutcome.Outcome = xfercontract.OutcomeInflight`}}, `
// Read-only observation view: never Freeze, reopen or repair a live receiver.
func TransferAtomicView(port AtomicReceiver) (xfercontract.AtomicState, error) {
 r, ok := port.(*Atomic); if !ok { return xfercontract.AtomicState{}, errors.New("foreign atomic observer") }
 r.mu.Lock(); defer r.mu.Unlock(); return r.state, nil
}
func TransferAtomicCustody(port AtomicReceiver, id xfercontract.Identity, deadline xfercontract.BootDeadline, sink func(xfercontract.Observation)) error {
 if sink == nil { return nil }
 r, ok := port.(*Atomic); if !ok { return errors.New("foreign atomic observer") }
 for _, item := range []struct { file *os.File; ref xfercontract.ResourceRef }{{r.tmp, r.state.Tmp}, {r.parent, r.state.Parent}} {
  if item.file == nil { continue }
  witness, err := drillobserve.CaptureTransferOFD(item.file, item.ref)
  if err != nil { return err }
  err = drillobserve.PublishTransferOFD(witness)
  closeErr := witness.Close()
  if err != nil || closeErr != nil { return errors.Join(err, closeErr) }
 }
 return SetAtomicObserver(r, func(o xfercontract.Observation) {
  if o.Kind != xfercontract.ObservePrimitiveEnter && o.Kind != xfercontract.ObservePrimitiveExit && o.Kind != xfercontract.ObserveDirectorySync { return }
  o.TransferID, o.EntryNonce, o.ObjectVersion = id.TransferID, id.EntryNonce, id.ObjectVersion
  o.OriginalDeadline, o.FetchedOffset = deadline, o.DurableOffset
  sink(o)
 })
}
`)
	patch("internal/xferreceive/receiver.go", [][2]string{
		{`"math"`, `"math"` + "\n" + `"github.com/LinZiyang666/tether/internal/transferpath"`},
		{`return &receiver{config: config, state: x.ReceiverState{`, `actor := &receiver{config: config, state: x.ReceiverState{`},
		{`}}, nil`, `}}
 if err := transferpath.TransferAtomicCustody(config.Atomic, config.Identity, actor.state.ByteDeadline, config.Observe); err != nil { return nil, err }
 if port, ok := config.Reader.(interface{ TransferObserve(func(x.Observation)) }); ok { port.TransferObserve(func(o x.Observation) { transferReceiverFetched(actor, o) }) }
 return actor, nil`}}, "")
	patch("internal/xferreceive/checkpoint.go", [][2]string{
		{`"time"`, `"time"` + "\n" + `"github.com/LinZiyang666/tether/internal/transferpath"`},
		{`digest := sha256.Sum256(durable.SHA256State)`, `digest := sha256.Sum256(durable.SHA256State)
 atomic, err := transferpath.TransferAtomicView(r.config.Atomic)
 if err != nil { return }`},
		{`return &receiver{config: config, state: state, frozen: true}, nil`, `actor := &receiver{config: config, state: state, frozen: true}
 if err := transferpath.TransferAtomicCustody(config.Atomic, state.Identity, state.ByteDeadline, config.Observe); err != nil { return nil, err }
 if port, ok := config.Reader.(interface{ TransferObserve(func(x.Observation)) }); ok { port.TransferObserve(func(o x.Observation) { transferReceiverFetched(actor, o) }) }
 return actor, nil`},
		{`HashStateDigest: hex.EncodeToString(digest[:]), FileSynced: synced, OriginalDeadline: original}`, `HashStateDigest: hex.EncodeToString(digest[:]), FileSynced: synced, OriginalDeadline: original, Tmp: atomic.Tmp, Parent: atomic.Parent}`}}, `
func transferReceiverFetched(r *receiver, o x.Observation) {
 if r.config.Observe == nil { return }
 atomic, err := transferpath.TransferAtomicView(r.config.Atomic); if err != nil { return }
 r.mu.Lock(); o.OriginalDeadline = r.state.ByteDeadline; r.mu.Unlock()
 o.Tmp, o.Parent = atomic.Tmp, atomic.Parent
 r.config.Observe(o)
}
`)

	patch("internal/jsstream/object_writer.go", [][2]string{
		// origin: S7 internal review FIX-SDKGET F1 — the observation seam uses strconv after the writer helpers moved.
		{`"fmt"`, `"fmt"` + "\n" + `"strconv"`},
		{`w.observe(e)`, `if port, ok := w.js.(interface{ Conn() *nats.Conn }); ok {
  nc := port.Conn(); cid, err := nc.GetClientID()
  if err == nil { e.ConnectionIdentity, e.ServerIdentity = strconv.FormatUint(cid, 10), nc.ConnectedServerId() }
 }
 w.observe(e)`}}, "")
	// C1 routes real transfer data through an exposed S5 TCP flow while the
	// agent's control connection remains available to resume that carrier.
	// The fixture supplies only a URL, before the original call is launched.
	// origin: S7 internal review CUT-CAPACITY F6 / hot-update S7 plan §10.3
	patch("internal/xferagent/transport.go", [][2]string{
		{`"time"`, `"time"` + "\n" + `"os"` + "\n" + `"strings"`},
		{`opts.Url, opts.Servers = control.Conn().ConnectedUrl(), nil`, `opts.Url, opts.Servers = control.Conn().ConnectedUrl(), nil
 if root := os.Getenv("S7_TRANSFER_FAULT_ROOT"); root != "" {
  if data, err := os.ReadFile(root+"/carrier-data-url"); err == nil { opts.Url = strings.TrimSpace(string(data)) }
 }`}}, "")
	patch("internal/xferagent/actors.go", [][2]string{
		{`imported *x.WriterState`, `imported *x.WriterState
 observe func(x.Observation)`},
		{`w.inner, w.js, w.imported = inner, js, nil`, `if w.observe != nil { if err := jsstream.SetTransferWriterObserver(inner, w.observe, d); err != nil { return nil, err } }
 w.inner, w.js, w.imported = inner, js, nil`},
		{`w.inner = inner`, `if w.observe != nil { if err := jsstream.SetTransferWriterObserver(inner, w.observe, d); err != nil { return nil, err } }
 w.inner = inner`}}, `
func (w *sealedWriter) TransferObserve(observe func(x.Observation)) { w.observe = observe }
`)
	patch("internal/xfersend/source_unix.go", [][2]string{
		{`"fmt"`, `"fmt"` + "\n" + drillImport},
		{`s.state.Budget.RemainingNS = remaining`, `s.state.Budget.RemainingNS = remaining
 if err := transferSenderCustody(s); err != nil { return nil, err }`},
		{`s.startedNS = now - int64(config.UTC().Sub(state.Budget.StartedAt))`, `s.startedNS = now - int64(config.UTC().Sub(state.Budget.StartedAt))
 if err := transferSenderCustody(s); err != nil { return nil, err }`}}, `
func transferSenderCustody(s *sender) error {
 if s.config.Observe == nil { return nil }
 witness, err := drillobserve.CaptureTransferOFD(s.config.Source, s.state.Source); if err != nil { return err }
 err = drillobserve.PublishTransferOFD(witness); closeErr := witness.Close()
 if err != nil || closeErr != nil { return errors.Join(err, closeErr) }
 if port, ok := s.config.Writer.(interface{ TransferObserve(func(x.Observation)) }); ok {
  port.TransferObserve(func(o x.Observation) {
   o.Phase, o.Source, o.OriginalDeadline = x.PhaseSending, s.state.Source, s.state.ByteDeadline
   s.config.Observe(o)
  })
 }
 return nil
}
`)
	patch("internal/xfersend/source_unix.go", [][2]string{{`"fmt"`, `"fmt"` + "\n" + `"errors"`}}, "")
	// Record a source seal only after the typed reply and original budget were
	// accepted. The earlier raw attempt is still the unconfirmed-send window;
	// a received refusal or a replay of an already confirmed seal is no progress.
	// origin: S7 internal review FIX-TIERA2 F2 / hot-update S7 plan §10.2 / T46
	patch("internal/xfersend/source_unix.go", [][2]string{
		{`s.sealed = true`, `s.sealed = true
 confirmed := attempt; confirmed.Class = "ok"
 emit(s, x.ObserveAttempt, confirmed)`}}, "")
	patch("internal/xferclient/push.go", [][2]string{
		{`"errors"`, `"errors"` + "\n" + drillImport + "\n" + `"github.com/LinZiyang666/tether/internal/jsstream"`},
		{`c.writer = writer`, `c.writer = writer
 if writer != nil && c.cfg.Observe != nil {
  observer := func(o x.Observation) { o.Phase, o.OriginalDeadline = x.PhaseSending, x.BootDeadline{BootID: c.bootID, Deadline: c.deadline}; c.cfg.Observe(o) }
  if err := jsstream.SetTransferWriterObserver(writer, observer, deadline); err != nil { return err }
 }`},
		{`c.pushSeal = &x.SourceSeal`, `if err := transferObserveSourcePinned(c); err != nil { return err }
 c.pushSeal = &x.SourceSeal`},
		// A Tier A push sends no object chunk, but its ctl still owns the
		// original pinned source while it publishes the inline seal after the
		// agent prepared the payload. The same producer record names that
		// responsibility; its absence made tier-a/push/source-pinned INCOMPLETE.
		// origin: S7 FIX-TIERA2 / hot-update S7 plan §3 / §10.2 / T35
		{`seal := x.SourceSeal{Size: int64(len(c.inline)),`, `if err := transferObserveSourcePinned(c); err != nil { return err }
 seal := x.SourceSeal{Size: int64(len(c.inline)),`}}, `
// The ctl's pinned original source, witnessed by its own OFD in custody.
func transferObserveSourcePinned(c *Client) error {
 if c.cfg.Observe == nil { return nil }
 ref := x.ResourceRef{ResourceID: "xfer-source-"+c.identity.TransferID, Name: x.SourceFileName, Kind: x.ResourceSource}
 witness, err := drillobserve.CaptureTransferOFD(c.cfg.Source, ref); if err != nil { return err }
 err = drillobserve.PublishTransferOFD(witness); closeErr := witness.Close(); if err != nil || closeErr != nil { return errors.Join(err, closeErr) }
 c.cfg.Observe(x.Observation{Kind: x.ObserveControl, TransferID: c.identity.TransferID, EntryNonce: c.identity.EntryNonce,
  ObjectVersion: c.identity.ObjectVersion, Phase: x.PhaseSending, Source: ref, OriginalDeadline: x.BootDeadline{BootID: c.bootID, Deadline: c.deadline}})
 return nil
}
`)
	patch("internal/jsstream/object_resume.go", [][2]string{
		// The reader helpers moved out of this file; its observation adapter
		// still formats the actual NATS connection ID in this compilation unit.
		// origin: S7 internal review TAGGED-SUITE-SWEEP-A F1
		{`"math"`, `"math"` + "\n" + `"strconv"`},
		{`activeCancel context.CancelFunc`, `activeCancel context.CancelFunc
 observe func(xfercontract.Observation)`},
		// First delivery and a retained final-chunk replay share this return;
		// keep custody attached after the exact metadata gate on both paths.
		// origin: S7 internal review TRANSFER-FOLLOWUPS F1 / hot-update S7 plan §11.4 / T39
		{`chunk.Data = bytes.Clone(chunk.Data)`, `chunk.Data = bytes.Clone(chunk.Data)
 r.transferObserveChunk(chunk)`}}, `
func (r *transferResumeReader) TransferObserve(observe func(xfercontract.Observation)) { r.observe = observe }
func (r *transferResumeReader) transferObserveChunk(chunk xfercontract.Chunk) {
 if r.observe == nil { return }
 r.mu.Lock(); cp, consumer := r.checkpoint, r.consumer; r.mu.Unlock()
 cid, err := r.js.Conn().GetClientID(); if err != nil { return }
 id := r.spec.Identity
 hashStateDigest := sha256.Sum256(cp.SHA256State)
 r.observe(xfercontract.Observation{Kind: xfercontract.ObserveFetched, TransferID: id.TransferID, EntryNonce: id.EntryNonce,
 ObjectVersion: id.ObjectVersion, Phase: xfercontract.PhaseReceiving, ConsumerIdentity: consumer,
 ConnectionIdentity: strconv.FormatUint(cid, 10), ServerIdentity: r.js.Conn().ConnectedServerId(),
 FetchedOffset: chunk.Receipt.Offset+int64(len(chunk.Data)), DurableOffset: cp.DurableOffset, Index: cp.ChunkIndex,
 LastConsumedStreamSeq: cp.LastConsumedStreamSeq, HashStateDigest: hex.EncodeToString(hashStateDigest[:])})
}
`)
	patch("internal/xferagent/actors.go", [][2]string{
		{`frozen   bool`, `frozen   bool
 observe func(x.Observation)`},
		{`return inner.ReadChunk(d)`, `if r.observe != nil {
  if port, ok := inner.(interface{ TransferObserve(func(x.Observation)) }); ok { port.TransferObserve(r.observe) }
 }
 return inner.ReadChunk(d)`}}, `
func (r *dataReader) TransferObserve(observe func(x.Observation)) { r.observe = observe }
`)
	patch("internal/xferclient/pull.go", [][2]string{
		{`c.reader = reader`, `c.reader = reader
 if c.cfg.Observe != nil { if port, ok := reader.(interface{ TransferObserve(func(x.Observation)) }); ok {
  port.TransferObserve(func(o x.Observation) {
   state, err := transferpath.TransferAtomicView(c.cfg.Destination); if err != nil { return }
   o.Tmp, o.Parent, o.OriginalDeadline = state.Tmp, state.Parent, x.BootDeadline{BootID: c.bootID, Deadline: c.deadline}; c.cfg.Observe(o)
  })
 } }`}}, "")
	patch("internal/xferclient/status.go", [][2]string{
		{`if c.cfg.Observe == nil {`, `if c.cfg.Observe == nil || c.identity.EntryNonce == "" {`},
		{`ObjectVersion: c.identity.ObjectVersion, Phase: c.phase, EventSeq: c.eventSeq, Attempt: attempt}`, `ObjectVersion: c.identity.ObjectVersion, Phase: c.phase, OriginalDeadline: x.BootDeadline{BootID: c.bootID, Deadline: c.deadline}, EventSeq: c.eventSeq, Attempt: attempt}`}}, "")
	patch("internal/xferclient/pull.go", [][2]string{
		{`"time"`, `"time"` + "\n" + `"github.com/LinZiyang666/tether/internal/transferpath"`},
		{`c.phase = x.PhaseReceiving`, `c.phase = x.PhaseReceiving
 if c.cfg.Observe != nil {
  if err := transferpath.TransferAtomicCustody(c.cfg.Destination, c.identity, x.BootDeadline{BootID: c.bootID, Deadline: c.deadline}, c.cfg.Observe); err != nil { return err }
 }`},
		// A Tier A pull lands through receiveInline, never the object loop
		// above. Bind the same original sink custody before its first WriteAt
		// so its one primitive and directory Sync are observed, not inferred.
		// origin: hot-update S7 plan §10.4 / FIX-INLINE
		{`if err := c.pace(c.spec.Size, deadline); err != nil {`, `if c.cfg.Observe != nil {
  if err := transferpath.TransferAtomicCustody(c.cfg.Destination, c.identity, x.BootDeadline{BootID: c.bootID, Deadline: c.deadline}, c.cfg.Observe); err != nil { return err }
 }
 if err := c.pace(c.spec.Size, deadline); err != nil {`},
		{`	if err := c.cfg.Destination.Sync(); err != nil {
		// The partial write`, `c.transferObserveCheckpoint(x.ObserveFetched, false, r.Offset+int64(len(chunk.Data)))
 c.transferObserveCheckpoint(x.ObserveSynced, false, r.Offset+int64(len(chunk.Data)))
 if err := c.cfg.Destination.Sync(); err != nil {
		// The partial write`},
		{`c.checkpoint = checkpoint`, `c.checkpoint = checkpoint
 c.transferObserveCheckpoint(x.ObserveSynced, true, checkpoint.DurableOffset)`}}, `
func (c *Client) transferObserveCheckpoint(kind x.ObservationKind, synced bool, fetched int64) {
 if c.cfg.Observe == nil { return }
 state, err := transferpath.TransferAtomicView(c.cfg.Destination); if err != nil { return }
 cp := c.checkpoint
 c.cfg.Observe(x.Observation{Kind: kind, TransferID: c.identity.TransferID, EntryNonce: c.identity.EntryNonce,
 ObjectVersion: c.identity.ObjectVersion, Phase: x.PhaseReceiving, OriginalDeadline: x.BootDeadline{BootID: c.bootID, Deadline: c.deadline},
 FetchedOffset: fetched, DurableOffset: cp.DurableOffset, Index: cp.ChunkIndex, LastConsumedStreamSeq: cp.LastConsumedStreamSeq,
 HashStateDigest: fmt.Sprintf("%x", sha256.Sum256(cp.SHA256State)), Tmp: state.Tmp, Parent: state.Parent, FileSynced: synced})
}
`)
	patch("internal/xferclient/pull.go", [][2]string{{`"errors"`, `"errors"` + "\n" + `"fmt"`}}, "")
}

// Build and unit checks consume one function body, so a projection regression
// cannot be hidden by a second hand-written fixture implementation.
func transferObservationFunctionSource(t *testing.T, root, name string) string {
	t.Helper()
	path := filepath.Join(root, "test/hotupdate/transfer_continuation_test.go")
	data, err := os.ReadFile(path)
	transferMust(t, err)
	positions := token.NewFileSet()
	file, err := parser.ParseFile(positions, path, data, 0)
	transferMust(t, err)
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Name.Name == name {
			return string(data[positions.Position(fn.Pos()).Offset:positions.Position(fn.End()).Offset]) + "\n"
		}
	}
	t.Fatalf("missing transfer observation projection %s", name)
	return ""
}
