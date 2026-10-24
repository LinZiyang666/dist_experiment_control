package xferimage

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"reflect"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// Compare actual exported resources with every nested reference. This verifier
// borrows metadata only; the real builder, rather than a codec fixture, supplies
// the files and remains their terminal cleanup owner.
func assertWorkerResourceClosure(t *testing.T, state xfercontract.WorkerState, resources []handoff.Resource) {
	t.Helper()
	byID := make(map[string]handoff.Resource)
	for _, resource := range resources {
		if _, exists := byID[resource.ID]; exists {
			t.Fatal("duplicate exported resource ID")
		}
		byID[resource.ID] = resource
	}
	used := make(map[string]bool)
	var visit func(reflect.Value)
	visit = func(v reflect.Value) {
		if v.Type() == reflect.TypeFor[time.Time]() {
			return
		}
		if v.Type() == reflect.TypeFor[xfercontract.ResourceRef]() {
			ref := v.Interface().(xfercontract.ResourceRef)
			if ref == (xfercontract.ResourceRef{}) {
				return
			}
			resource, exists := byID[ref.ResourceID]
			if !exists || resource.Kind != ref.Kind || len(resource.Files) != 1 || resource.Files[ref.Name] == nil {
				t.Fatal("nested reference missing its actual exported file")
			}
			used[ref.ResourceID] = true
			return
		}
		switch v.Kind() {
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				visit(v.Field(i))
			}
		case reflect.Pointer:
			if !v.IsNil() {
				visit(v.Elem())
			}
		case reflect.Slice, reflect.Array:
			if v.Type().Elem().Kind() == reflect.Uint8 {
				return
			}
			for i := 0; i < v.Len(); i++ {
				visit(v.Index(i))
			}
		}
	}
	visit(reflect.ValueOf(state))
	for id, resource := range byID {
		if resource.Kind == xfercontract.ResourceState && len(resource.Files) == 1 && resource.Files[xfercontract.StateFileName] != nil {
			continue
		}
		if !used[id] {
			t.Fatal("export contains an unreferenced business file")
		}
	}
}

func workerFixture(mode string) xfercontract.WorkerState {
	started := time.Unix(1700000000, 0).UTC()
	id := xfercontract.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", IntentDigest: codecDigest([]byte("intent")), ObjectVersion: "nuid"}
	origin := xfercontract.Origin{BrokerID: "broker", InstanceID: "broker-instance", BootID: "boot", Generation: 1}
	body := bytes.Repeat([]byte("a"), 65536)
	seal := xfercontract.SourceSeal{Size: int64(len(body)), SHA256: codecDigest(body), ChunkSize: 32768, Chunks: 2, SourceFingerprint: "pinned"}
	budget := xfercontract.BudgetSeal{StartedAt: started, Size: seal.Size, EffectiveBudgetNS: int64(300 * time.Second), RemainingNS: int64(200 * time.Second), SourceSealed: true}
	intent := xfercontract.Intent{Verb: "push", Tier: "b", Path: "/target", Size: seal.Size, SHA256: seal.SHA256, ChunkSize: seal.ChunkSize}
	p := xfercontract.Prepare{CallID: "call", Identity: id, Origin: origin, Intent: intent, Budget: budget, Source: &seal,
		Permit: xfercontract.CreationPermit{Identity: id, Origin: origin, IssuedAt: started, CreateUntil: started.Add(time.Minute), PermitNonce: "permit", Signature: "private-permit"}}
	e := xfercontract.WorkerEntry{Prepare: p, OwnerGeneration: 1, Reservation: "reserved", Status: xfercontract.Status{State: xfercontract.StatusLive, Phase: xfercontract.PhasePrepared, Identity: id, Origin: origin, Budget: budget}}
	s := xfercontract.WorkerState{Schema: xfercontract.StateSchema, Lineage: xfercontract.Lineage{InstanceID: "agent", BootID: "boot", Generation: 1}, Admission: xfercontract.AdmissionState{Closed: true, Generation: 1},
		Entries: []xfercontract.WorkerEntry{e}, CapacityReservations: []xfercontract.CapacityReservation{{ID: "reserved", Reservation: xfercontract.Reservation{SealedBytes: 4 << 20, NonReplayBytes: 8 << 20, FDs: 9, Resources: 3}}}}
	if mode == "empty" {
		s.Entries, s.CapacityReservations = nil, nil
		return s
	}
	if mode == "prepared" {
		return s
	}
	s.Entries[0].Started = true
	spec := xfercontract.ObjectSpec{Identity: id, Bucket: "bucket", Stream: "OBJ_bucket", StreamCreated: "created", SHA256: seal.SHA256, Size: seal.Size, ChunkSize: seal.ChunkSize, Chunks: seal.Chunks}
	deadline := xfercontract.BootDeadline{BootID: "boot", Deadline: 5000000000}
	if mode == "sender" || mode == "inline-sender" || mode == "prepared-source" {
		first := xfercontract.ChunkReceipt{Index: 0, StreamSeq: 7, Length: 32768, SHA256: sha256.Sum256(body[:32768])}
		pending := &xfercontract.PendingAttempt{Index: 1, Offset: 32768, Length: 32768, SHA256: sha256.Sum256(body[32768:]), Bytes: bytes.Clone(body[32768:]), PreviousSubjectSeq: 7, ExpectedStreamSeq: 10, AttachmentGeneration: 1}
		s.Entries[0].Status.Phase = xfercontract.PhaseSending
		s.Senders = []xfercontract.SenderState{{Identity: id, Intent: intent, Budget: budget, ByteDeadline: deadline,
			Source: xfercontract.ResourceRef{ResourceID: "source-1", Name: "source", Kind: xfercontract.ResourceSource}, SourceIdentity: xfercontract.FileIdentity{Dev: 1, Ino: 3, Mode: 0o100600}, SourceSeal: seal,
			Writer: xfercontract.WriterState{ObjectSpec: spec, Phase: xfercontract.WritePending, Receipts: []xfercontract.ChunkReceipt{first}, PendingAttempt: pending, AttachmentGeneration: 1}, Offset: 32768, SHA256State: codecHash(body[:32768])}}
		if mode == "inline-sender" {
			// origin: FIX-INLINE — the real Tier A sender is a pull with an
			// unknown intent size, no object version or writer, and the whole
			// sealed source retained as its copy (cursor and hash at EOF).
			inline := xfercontract.SourceSeal{Size: seal.Size, SHA256: seal.SHA256, SourceFingerprint: seal.SourceFingerprint}
			pull := xfercontract.Intent{Verb: "pull", Tier: "a", Path: "/source"}
			e := &s.Entries[0]
			e.Prepare.Identity.ObjectVersion, e.Prepare.Intent, e.Prepare.Source = "", pull, &inline
			e.Prepare.Permit.Identity, e.Status.Identity = e.Prepare.Identity, e.Prepare.Identity
			s.Senders[0] = xfercontract.SenderState{Identity: e.Prepare.Identity, Intent: pull, Budget: budget, ByteDeadline: deadline,
				Source: s.Senders[0].Source, SourceIdentity: s.Senders[0].SourceIdentity, SourceSeal: inline,
				Offset: seal.Size, SHA256State: codecHash(body), Inline: bytes.Clone(body)}
		}
		if mode == "prepared-source" {
			s.Entries[0].Started, s.Entries[0].Status.Phase = false, xfercontract.PhasePrepared
			s.Entries[0].Prepare.Source = nil
			s.Entries[0].Prepare.Budget.SourceSealed, s.Entries[0].Status.Budget.SourceSealed = false, false
			s.Senders[0].Budget.SourceSealed = false
			s.Senders[0].SourceSeal, s.Senders[0].Writer = xfercontract.SourceSeal{}, xfercontract.WriterState{}
			s.Senders[0].Offset, s.Senders[0].SHA256State = 0, nil
		}
		return s
	}
	if mode == "courier" || mode == "settled" {
		grant := xfercontract.CommitGrant{Identity: id, GrantNonce: "grant", ReceiverInstance: "agent", TerminalID: codecTerminalID(id)}
		outcome := xfercontract.ReceiverOutcome{Grant: grant, Primitive: xfercontract.PrimitiveLink, Outcome: xfercontract.OutcomeLanded, Bytes: seal.Size, SHA256: seal.SHA256, ParentSynced: true}
		canonical := xfercontract.CanonicalRecord{Identity: id, Kind: "complete", ReqID: "terminal-request", Bytes: []byte("canonical terminal")}
		canonical.Digest = codecDigest(canonical.Bytes)
		o := xfercontract.CanonicalOutcome{Identity: id, Outcome: outcome, Canonical: canonical}
		capsule := xfercontract.SettlementCapsule{Identity: id, Origin: origin, ReceiverNkey: "receiver-nkey", Issuer: "issuer", Nonce: "settlement", Signature: "private-settlement"}
		s.Entries[0].Status.Phase, s.Entries[0].GateClosed = xfercontract.PhaseTerminalPending, true
		s.CanonicalOutcomes = []xfercontract.CanonicalOutcome{o}
		s.Couriers = []xfercontract.CourierState{{Identity: id, Outcome: outcome, Canonical: canonical, Capsule: capsule, Reply: "reply", NextAttempt: deadline, Cleanup: xfercontract.CleanupState{Identity: id, PrimaryRetirePending: true}}}
		s.Revocations = []xfercontract.Revocation{{Identity: id, Reason: xfercontract.CancelRevoked, At: deadline}}
		s.ResultRetention = []xfercontract.RetentionState{{Identity: id, CreateUntil: p.Permit.CreateUntil, ResultUntil: started.Add(10 * time.Minute), Deadline: deadline}}
		if mode == "settled" {
			r := codecTerminal(o)
			s.Entries[0].Status.State, s.Entries[0].Status.Phase, s.Entries[0].Status.Terminal = xfercontract.StatusTerminal, xfercontract.PhaseSettled, &r
			s.CanonicalOutcomes[0].Receipt, s.Couriers[0].Receipt, s.Couriers[0].Stopped = &r, &r, true
			s.Couriers[0].Cleanup.PrimaryRetirePending = false
		}
		return s
	}
	cp := xfercontract.SinkCheckpoint{ChunkIndex: 1, DurableOffset: 32768, LastConsumedStreamSeq: 7, SHA256State: codecHash(body[:32768])}
	atomic := xfercontract.AtomicState{Phase: xfercontract.PhaseReceiving, DurableOffset: cp.DurableOffset, TmpName: "tmp-file", Destination: "destination",
		Tmp: xfercontract.ResourceRef{ResourceID: "tmp-1", Name: "tmp", Kind: xfercontract.ResourceTmp}, Parent: xfercontract.ResourceRef{ResourceID: "parent-1", Name: "parent", Kind: xfercontract.ResourceParent},
		TmpIdentity: xfercontract.FileIdentity{Dev: 1, Ino: 4, Mode: 0o100600, Flags: 1}, ParentIdentity: xfercontract.FileIdentity{Dev: 1, Ino: 5, Mode: 0o040700}}
	r := xfercontract.ReceiverState{Identity: id, Budget: budget, ByteDeadline: deadline, Atomic: atomic, Checkpoint: cp,
		Reader: xfercontract.ReaderState{ObjectSpec: spec, Phase: xfercontract.ReaderFetching, SinkCheckpoint: cp, ConsumerIdentity: "consumer", ConnectionIdentity: "connection"}}
	s.Entries[0].Status.Phase, s.Entries[0].Status.DurableOffset, s.Entries[0].Status.ChunkIndex, s.Entries[0].Status.LastConsumedStreamSeq = atomic.Phase, cp.DurableOffset, cp.ChunkIndex, cp.LastConsumedStreamSeq
	if mode == "prepared-receiver" {
		r.Checkpoint = xfercontract.SinkCheckpoint{SHA256State: codecHash(nil)}
		r.Atomic.Phase, r.Atomic.DurableOffset = xfercontract.PhasePrepared, 0
		r.Atomic.Tmp, r.Atomic.TmpIdentity = xfercontract.ResourceRef{}, xfercontract.FileIdentity{}
		r.Reader.Phase, r.Reader.ConsumerIdentity, r.Reader.ConnectionIdentity, r.Reader.SinkCheckpoint = xfercontract.ReaderOpen, "", "", r.Checkpoint
		s.Entries[0].Started = false
		s.Entries[0].Status.Phase, s.Entries[0].Status.DurableOffset, s.Entries[0].Status.ChunkIndex, s.Entries[0].Status.LastConsumedStreamSeq = r.Atomic.Phase, 0, 0, 0
	}
	if mode == "grant" || mode == "landed" {
		r.Grant = xfercontract.CommitGrant{Identity: id, GrantNonce: "grant", ReceiverInstance: "agent", TerminalID: codecTerminalID(id)}
		r.Atomic.CommitGrant, r.Atomic.Phase = r.Grant, xfercontract.PhaseCommitReserved
		s.Entries[0].Status.Phase = r.Atomic.Phase
		if mode == "landed" {
			r.Outcome = xfercontract.ReceiverOutcome{Grant: r.Grant, Primitive: xfercontract.PrimitiveRename, Outcome: xfercontract.OutcomeLanded, Bytes: seal.Size, SHA256: seal.SHA256}
			r.Atomic.PrimitiveOutcome, r.Atomic.DirSyncPending, r.Atomic.Phase = r.Outcome, true, xfercontract.PhaseDecided
			r.Atomic.Tmp, r.Atomic.TmpIdentity = xfercontract.ResourceRef{}, xfercontract.FileIdentity{}
			s.Entries[0].Status.Phase = r.Atomic.Phase
		}
	}
	if mode == "inline" {
		tierAInline(&s.Entries[0], &r, body)
		r.Reader, r.InlineOffset = xfercontract.ReaderState{}, cp.DurableOffset
		r.Checkpoint.ChunkIndex, r.Checkpoint.LastConsumedStreamSeq = 0, 0
		s.Entries[0].Status.ChunkIndex, s.Entries[0].Status.LastConsumedStreamSeq = 0, 0
	}
	s.Receivers = []xfercontract.ReceiverState{r}
	return s
}

func codecDigest(b []byte) string { d := sha256.Sum256(b); return hex.EncodeToString(d[:]) }
func codecTerminalID(id xfercontract.Identity) string {
	return codecDigest([]byte("xfer-terminal-v1\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))
}
func codecHash(b []byte) []byte {
	h := sha256.New()
	_, _ = h.Write(b)
	state, _ := h.(encoding.BinaryMarshaler).MarshalBinary()
	return state
}
func codecTerminal(o xfercontract.CanonicalOutcome) xfercontract.TerminalReceipt {
	return xfercontract.TerminalReceipt{TerminalID: codecTerminalID(o.Identity), Kind: o.Canonical.Kind, CanonicalDigest: o.Canonical.Digest, ReqID: o.Canonical.ReqID, HistoryStream: "history", HistorySeq: 8, Bytes: o.Outcome.Bytes, SHA256: o.Outcome.SHA256, Code: o.Outcome.Code}
}

func rawWorkerImage(t testing.TB, state xfercontract.WorkerState) []byte {
	t.Helper()
	size := workerHeaderBytes
	if err := measureWorkerValue(reflect.ValueOf(state), &size); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, workerHeaderBytes, size)
	copy(data[:8], "TETHXW7\x00")
	binary.BigEndian.PutUint32(data[8:12], 1)
	copy(data[48:80], workerSchemaDigest[:])
	data, err := appendWorkerValue(data, reflect.ValueOf(state))
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint32(data[12:16], uint32(len(data)-workerHeaderBytes))
	repairWorkerDigest(data)
	return data
}
func repairWorkerDigest(data []byte) {
	d := sha256.Sum256(data[workerHeaderBytes:])
	copy(data[16:48], d[:])
}

func fillWorkerProjection(v reflect.Value) {
	if v.Type() == reflect.TypeFor[time.Time]() {
		v.Set(reflect.ValueOf(time.Unix(1700000000, 123).UTC()))
		return
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			fillWorkerProjection(v.Field(i))
		}
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		fillWorkerProjection(v.Elem())
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		fillWorkerProjection(v.Index(0))
	case reflect.Array:
		for i := 0; i < v.Len(); i++ {
			fillWorkerProjection(v.Index(i))
		}
	case reflect.String:
		v.SetString(v.Type().String() + "-sentinel")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int32, reflect.Int64:
		v.SetInt(-123)
	case reflect.Uint8, reflect.Uint32, reflect.Uint64:
		v.SetUint(123)
	default:
		panic("unsupported worker projection")
	}
}

// Each nested test finishes its original-owner cleanup before the next mode or
// round. The parent's direct leak assertions therefore cover live exports, not
// just the synthetic semantic-control images used elsewhere in this package.
// origin: hot-update S7 plan §8.3 / §11.3 / T17
func exerciseWorkerCodecStateBuilders(t *testing.T) {
	t.Helper()
	t.Run("real state builder closure", func(t *testing.T) {
		if workerCodecStateBuilder == nil {
			// origin: S10 internal review NO-SKIP-AS-PASS F3
			t.Fatal("SETUP-RED: NOT_RUN pending S7 P05/P06: actual state builders are required")
		}
		for _, mode := range []string{"prepared", "sender", "receiver", "landed", "courier", "settled"} {
			t.Run(mode, func(t *testing.T) {
				frozen := workerCodecStateBuilder(t, mode)
				if frozen == nil {
					t.Fatal("real builder returned no frozen owner")
				}
				state, resources, err := frozen.Export()
				if err != nil {
					t.Fatal(err)
				}
				assertWorkerCodecStateMode(t, mode, state, resources)
				data, err := EncodeWorker(state)
				if err != nil {
					t.Fatal(err)
				}
				got, err := DecodeWorker(data)
				if err != nil || !reflect.DeepEqual(state, got) {
					t.Fatalf("real builder projection changed: %v", err)
				}
				assertWorkerResourceClosure(t, state, resources)
			})
		}
	})
}

// Nonempty assertions distinguish actual byte/primitive/settlement cuts from
// six labels attached to a single prepared image.
// origin: S7 internal review NO-SKIP-AS-PASS F3
func assertWorkerCodecStateMode(t *testing.T, mode string, s xfercontract.WorkerState, resources []handoff.Resource) {
	t.Helper()
	if len(s.Entries) != 1 || len(s.CapacityReservations) != 1 || len(s.ResultRetention) != 1 {
		t.Fatal("real registry responsibility missing from export")
	}
	switch mode {
	case "prepared":
		if s.Entries[0].Started || s.Entries[0].Status.Phase != xfercontract.PhasePrepared || len(s.Senders)+len(s.Receivers)+len(s.Couriers)+len(resources) != 0 {
			t.Fatal("prepared cut gained actors or lost preparation")
		}
	case "sender":
		if len(s.Senders) != 1 || len(resources) != 1 {
			t.Fatal("sender cut lost its actor or original source")
		}
		a := s.Senders[0]
		if a.Offset != 32<<10 || len(a.SHA256State) == 0 || len(a.Writer.Receipts) != 1 || a.Writer.Receipts[0].StreamSeq == 0 {
			t.Fatal("sender cut has no real acknowledged prefix")
		}
	case "receiver":
		if len(s.Receivers) != 1 || len(resources) != 2 {
			t.Fatal("receiver cut lost its original tmp/parent closure")
		}
		r := s.Receivers[0]
		if r.Checkpoint.DurableOffset != 32<<10 || r.Checkpoint.ChunkIndex != 1 || r.Checkpoint.LastConsumedStreamSeq == 0 || len(r.Checkpoint.SHA256State) == 0 {
			t.Fatal("receiver cut has no real synced prefix")
		}
	case "landed":
		if len(s.Receivers) != 1 || s.Receivers[0].Outcome.Outcome != xfercontract.OutcomeLanded || !s.Receivers[0].Outcome.ParentSynced || s.Receivers[0].Grant.GrantNonce == "" {
			t.Fatal("landed cut lost its actual primitive outcome")
		}
		fallthrough
	case "courier":
		if len(s.Couriers) != 1 || len(s.CanonicalOutcomes) != 1 || s.Couriers[0].Attempt.Ordinal == 0 || s.Couriers[0].Receipt != nil || s.Couriers[0].Stopped {
			t.Fatal("pending courier cut lost its exact retry responsibility")
		}
		if mode == "courier" && (len(s.Senders)+len(s.Receivers)+len(resources) != 0 || len(s.Revocations) != 1) {
			t.Fatal("courier-only cut retained byte actors or lost revocation")
		}
	case "settled":
		status := s.Entries[0].Status
		if status.Phase != xfercontract.PhaseSettled || status.Terminal == nil || status.Terminal.Kind != "complete" || status.Terminal.HistorySeq == 0 || len(s.CanonicalOutcomes) != 1 || s.CanonicalOutcomes[0].Receipt == nil || len(s.Senders)+len(s.Receivers)+len(s.Couriers)+len(resources) != 0 {
			t.Fatal("settled cut retained data responsibility or lost exact history")
		}
	default:
		t.Fatal("unknown builder cut")
	}
}
