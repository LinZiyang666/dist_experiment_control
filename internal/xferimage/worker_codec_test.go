package xferimage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontract/xferport"
)

// P05/P06 real fixture builders are supplied by the integration owner. Absence
// is SETUP-RED, never a synthetic successful export. The builder owns Stop/cleanup
// of all original resources; codec projections below cannot replace live exports.
var workerCodecStateBuilder func(*testing.T, string) xfercontract.WorkerFrozen

// SetWorkerCodecStateBuilder is a test-only bridge: the external test package
// can compose xferworker (which imports this codec) without an import cycle.
func SetWorkerCodecStateBuilder(build func(*testing.T, string) xfercontract.WorkerFrozen) {
	workerCodecStateBuilder = build
}

// origin: hot-update S7 plan §8.3 / T17 / M08
// origin: S7 internal review NO-SKIP-AS-PASS F3
func TestTransferWorkerImageCodecMatchesDescriptors(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseWorkerCodecStateBuilders(t)
	}
	testharness.AssertNoGoroutineLeak(t, "worker codec real state builders", g)
	testharness.AssertNoFDLeak(t, "worker codec real state builders", fd, 0)
	t.Run("descriptor closure and actual field traversal", func(t *testing.T) {
		// This independent field list pins all reachable records, including
		// fields that have zero values in a prepared-only production image.
		expected := map[string]string{
			"WorkerState":          "Schema Lineage Admission Revocations Entries Senders Receivers CanonicalOutcomes Couriers ResultRetention CapacityReservations ConsumerCleanup",
			"ConsumerCleanupState": "OwnerID Deletes",
			"ConsumerDelete":       "Stream StreamCreated Consumer",
			"Lineage":              "InstanceID BootID Generation", "AdmissionState": "Closed Generation Inflight",
			"Revocation": "Identity Reason At", "WorkerEntry": "Prepare Status OwnerGeneration Started GateClosed Reservation",
			"SenderState":      "Identity Intent Budget ByteDeadline Source SourceIdentity SourceSeal Writer Offset SHA256State Stopped Inline",
			"ReceiverState":    "Identity Budget ByteDeadline Reader Atomic Checkpoint InlineOffset GateClosed Grant Outcome Stopped",
			"CanonicalOutcome": "Identity Outcome Canonical Receipt",
			"CourierState":     "Identity Outcome Canonical Capsule Reply Attempt Receipt NextAttempt Cleanup Stopped",
			"RetentionState":   "Identity CreateUntil ResultUntil Deadline", "CapacityReservation": "ID Reservation",
			"Prepare":  "CallID Identity Origin Intent Permit Budget Source Settlement Inline",
			"Status":   "State Phase Identity Origin Budget DurableOffset ChunkIndex LastConsumedStreamSeq Terminal CleanupPending Code Error",
			"Identity": "SID NID ActorFP AgentInstance TransferID EntryNonce IntentDigest ObjectVersion",
			"Origin":   "BrokerID InstanceID BootID Generation", "Intent": "Verb Tier Path SHA256 Size Force RateLimitBPS ChunkSize",
			"CreationPermit":    "Identity Origin IssuedAt CreateUntil PermitNonce Signature",
			"BudgetSeal":        "StartedAt Size RateLimitBPS EffectiveBudgetNS RemainingNS SourceSealed",
			"SourceSeal":        "Size SHA256 ChunkSize Chunks RateLimitBPS SourceFingerprint",
			"SettlementCapsule": "Identity Origin ReceiverNkey Issuer Nonce Signature",
			"BootDeadline":      "BootID Deadline", "ResourceRef": "ResourceID Name Kind", "FileIdentity": "Dev Ino Mode Flags",
			"WriterState":     "ObjectSpec Phase Receipts PendingAttempt MetaReceipt AttachmentGeneration RetirementFence",
			"ReaderState":     "ObjectSpec Phase SinkCheckpoint ConsumerIdentity ConnectionIdentity DirtyBarrier DeletePending",
			"AtomicState":     "Phase Tmp Parent Source TmpIdentity ParentIdentity SourceIdentity TmpName Destination Force DurableOffset CommitGrant PrimitiveOutcome DirSyncPending UnlinkPending",
			"ObjectSpec":      "Identity Bucket Stream StreamCreated SHA256 Size ChunkSize Chunks",
			"ChunkReceipt":    "Index StreamSeq Offset Length SHA256",
			"PendingAttempt":  "Index Offset Length SHA256 Bytes PreviousSubjectSeq ExpectedStreamSeq AttachmentGeneration",
			"ObjectReceipt":   "Spec MetadataSeq MetadataDigest Metadata",
			"RetirementFence": "ExpectedStreamSeq PreviousMetadataSeq Receipt WriterRetired ConsumersRetired Purged",
			"HistoryReceipt":  "Stream RecordDigest Sequence",
			"SinkCheckpoint":  "ChunkIndex DurableOffset LastConsumedStreamSeq SHA256State",
			"CommitGrant":     "Identity GrantNonce ReceiverInstance TerminalID",
			"ReceiverOutcome": "Grant Primitive Outcome Bytes SHA256 Code Error ParentSynced TmpCleanupPending",
			"CanonicalRecord": "Identity Kind ReqID Digest Bytes",
			"TerminalReceipt": "TerminalID Kind CanonicalDigest ReqID HistoryStream HistorySeq Bytes SHA256 Code OutcomeUnknown",
			"CleanupState":    "Identity WriterFence Consumers TmpUnlinkPending DirectorySyncPending ObjectPurgePending PrimaryRetirePending OutboxRetirePending",
			"Reservation":     "ReplayBytes NonReplayBytes SealedBytes PublicBytes HandoffBytes FDs Resources",
			"Attempt":         "LogicalID Ordinal Phase StartedMonoNS FinishedMonoNS Sent Class BackoffNS",
		}
		seen := make(map[string]map[string]bool)
		// origin: FIX-INLINE — the only intended drift is SenderState.Inline, a
		// Tier A pull's retained copy (hot-update S7 plan §8.1); no other
		// record's field, type, tag or encoding changed with it.
		if hex.EncodeToString(workerSchemaDigest[:]) != "c1ee84e1a8b1e8ca9703b380164ab73fb19470c2d1cd54c1cbec0161d1f83ec7" {
			t.Fatalf("worker descriptor field/type/tag/encoding drift: %x", workerSchemaDigest)
		}
		for _, d := range WorkerDescriptors() {
			record := strings.TrimPrefix(strings.TrimPrefix(d.Record, "xfercontract."), "controlbudget.")
			if seen[record] == nil {
				seen[record] = make(map[string]bool)
			}
			if seen[record][d.Name] || d.Encoding != "binary-v1" || d.Type == "" {
				t.Fatalf("invalid descriptor: %s.%s", record, d.Name)
			}
			seen[record][d.Name] = true
		}
		if len(seen) != len(expected) {
			t.Fatalf("record closure: got %d, want %d", len(seen), len(expected))
		}
		for record, names := range expected {
			if len(seen[record]) != len(strings.Fields(names)) {
				t.Fatalf("field closure: %s", record)
			}
			for _, name := range strings.Fields(names) {
				if !seen[record][name] {
					t.Fatalf("missing %s.%s", record, name)
				}
			}
		}
		// Each field gets a nonzero value through reflection, independently of
		// the actor validators. Exercise the same binary traversal used by both
		// public codec entry points and compare the ENTIRE decoded projection.
		var input xfercontract.WorkerState
		fillWorkerProjection(reflect.ValueOf(&input).Elem())
		data := rawWorkerImage(t, input)
		var output xfercontract.WorkerState
		remaining, heap := data[workerHeaderBytes:], MaxWorkerStateBytes
		if err := readWorkerValue(&remaining, reflect.ValueOf(&output).Elem(), &heap); err != nil {
			t.Fatal(err)
		}
		if len(remaining) != 0 || !reflect.DeepEqual(input, output) {
			t.Fatal("binary traversal dropped a reachable field")
		}
		copyTable := WorkerDescriptors()
		copyTable[0].Name = "tampered"
		if WorkerDescriptors()[0].Name == "tampered" {
			t.Fatal("descriptor caller mutated codec schema")
		}
	})

	t.Run("valid responsibility cuts and independent storage", func(t *testing.T) {
		for _, mode := range []string{"empty", "prepared", "prepared-receiver", "prepared-source", "sender", "inline-sender", "receiver", "grant", "landed", "courier", "settled", "inline"} {
			t.Run(mode, func(t *testing.T) {
				state := workerFixture(mode)
				data, err := EncodeWorker(state)
				if err != nil {
					t.Fatal(err)
				}
				again, err := EncodeWorker(state)
				if err != nil || !bytes.Equal(data, again) {
					t.Fatal("encoder mutated or reordered owner state")
				}
				got, err := DecodeWorker(data)
				if err != nil || !reflect.DeepEqual(state, got) {
					t.Fatalf("roundtrip changed %s: %v", mode, err)
				}
				clear(data)
				if !reflect.DeepEqual(state, got) {
					t.Fatal("decoded state aliases sealed input")
				}
			})
		}
	})

	t.Run("semantic refusals before import", func(t *testing.T) {
		cases := []struct {
			name, mode string
			mutate     func(*xfercontract.WorkerState)
		}{
			{"missing phase", "receiver", func(s *xfercontract.WorkerState) { s.Receivers[0].Atomic.Phase = "" }},
			{"unknown phase", "receiver", func(s *xfercontract.WorkerState) { s.Entries[0].Status.Phase = "future" }},
			{"missing tmp closure", "receiver", func(s *xfercontract.WorkerState) { s.Receivers[0].Atomic.Tmp = xfercontract.ResourceRef{} }},
			{"missing parent closure", "receiver", func(s *xfercontract.WorkerState) { s.Receivers[0].Atomic.Parent = xfercontract.ResourceRef{} }},
			{"prepared parent closure", "prepared-receiver", func(s *xfercontract.WorkerState) { s.Receivers[0].Atomic.Parent = xfercontract.ResourceRef{} }},
			{"extra landed tmp closure", "landed", func(s *xfercontract.WorkerState) {
				a := workerFixture("receiver").Receivers[0].Atomic
				s.Receivers[0].Atomic.Tmp, s.Receivers[0].Atomic.TmpIdentity = a.Tmp, a.TmpIdentity
			}},
			{"wrong resource basename", "receiver", func(s *xfercontract.WorkerState) { s.Receivers[0].Atomic.Tmp.Name = "source" }},
			{"wrong resource kind", "receiver", func(s *xfercontract.WorkerState) { s.Receivers[0].Atomic.Tmp.Kind = xfercontract.ResourceSource }},
			{"duplicate resource ID", "receiver", func(s *xfercontract.WorkerState) {
				s.Receivers[0].Atomic.Tmp.ResourceID = s.Receivers[0].Atomic.Parent.ResourceID
			}},
			{"wrong tmp mode", "receiver", func(s *xfercontract.WorkerState) { s.Receivers[0].Atomic.TmpIdentity.Mode = 0o040700 }},
			{"wrong tmp access", "receiver", func(s *xfercontract.WorkerState) { s.Receivers[0].Atomic.TmpIdentity.Flags = 2 }},
			{"missing source closure", "sender", func(s *xfercontract.WorkerState) { s.Senders[0].Source = xfercontract.ResourceRef{} }},
			{"dropped sender actor", "sender", func(s *xfercontract.WorkerState) { s.Senders = nil }},
			{"unsealed source advanced cursor", "prepared-source", func(s *xfercontract.WorkerState) { s.Senders[0].Offset = 1 }},
			{"dropped receiver actor", "receiver", func(s *xfercontract.WorkerState) { s.Receivers = nil }},
			{"wrong synced offset", "receiver", func(s *xfercontract.WorkerState) {
				r := &s.Receivers[0]
				r.Checkpoint.DurableOffset++
				r.Checkpoint.SHA256State = codecHash(bytes.Repeat([]byte("a"), int(r.Checkpoint.DurableOffset)))
				r.Reader.SinkCheckpoint, r.Atomic.DurableOffset = r.Checkpoint, r.Checkpoint.DurableOffset
				s.Entries[0].Status.DurableOffset = r.Checkpoint.DurableOffset
			}},
			{"hash ahead of synced prefix", "receiver", func(s *xfercontract.WorkerState) {
				s.Receivers[0].Checkpoint.SHA256State = codecHash(make([]byte, 32769))
				s.Receivers[0].Reader.SinkCheckpoint = s.Receivers[0].Checkpoint
			}},
			{"dropped hash checkpoint", "receiver", func(s *xfercontract.WorkerState) {
				s.Receivers[0].Checkpoint.SHA256State = nil
				s.Receivers[0].Reader.SinkCheckpoint = s.Receivers[0].Checkpoint
			}},
			{"consumed seq without synced chunk", "receiver", func(s *xfercontract.WorkerState) {
				s.Receivers[0].Checkpoint.ChunkIndex = 0
				s.Receivers[0].Reader.SinkCheckpoint = s.Receivers[0].Checkpoint
			}},
			{"reader checkpoint disagrees", "receiver", func(s *xfercontract.WorkerState) { s.Receivers[0].Reader.SinkCheckpoint.LastConsumedStreamSeq++ }},
			{"pending bytes digest", "sender", func(s *xfercontract.WorkerState) { s.Senders[0].Writer.PendingAttempt.SHA256[0] ^= 1 }},
			{"pending attachment changed", "sender", func(s *xfercontract.WorkerState) { s.Senders[0].Writer.PendingAttempt.AttachmentGeneration++ }},
			// origin: FIX-INLINE — a Tier A copy is the whole sealed source or absent.
			{"inline copy digest", "inline-sender", func(s *xfercontract.WorkerState) { s.Senders[0].Inline[0] ^= 1 }},
			{"inline copy truncated", "inline-sender", func(s *xfercontract.WorkerState) { s.Senders[0].Inline = s.Senders[0].Inline[1:] }},
			{"inline cursor without copy", "inline-sender", func(s *xfercontract.WorkerState) { s.Senders[0].Inline = nil }},
			{"stopped sender retains copy", "inline-sender", func(s *xfercontract.WorkerState) { s.Senders[0].Stopped = true }},
			{"object sender carries inline", "sender", func(s *xfercontract.WorkerState) { s.Senders[0].Inline = []byte("x") }},
			{"receipt physical sequence duplicate", "sender", func(s *xfercontract.WorkerState) {
				a := &s.Senders[0]
				r := a.Writer.Receipts[0]
				r.Index++
				r.Offset += int64(r.Length)
				a.Writer.Receipts = append(a.Writer.Receipts, r)
				a.Writer.PendingAttempt, a.Writer.Phase = nil, xfercontract.WriteMetadataPending
				a.Offset, a.SHA256State = a.SourceSeal.Size, codecHash(bytes.Repeat([]byte("a"), int(a.SourceSeal.Size)))
			}},
			{"foreign object version", "sender", func(s *xfercontract.WorkerState) { s.Senders[0].Writer.ObjectSpec.Identity.ObjectVersion = "foreign" }},
			{"inflight primitive cannot cut", "grant", func(s *xfercontract.WorkerState) {
				o := xfercontract.ReceiverOutcome{Grant: s.Receivers[0].Grant, Primitive: xfercontract.PrimitiveRename, Outcome: xfercontract.OutcomeInflight}
				s.Receivers[0].Atomic.PrimitiveOutcome = o
			}},
			{"grant nonce identity", "grant", func(s *xfercontract.WorkerState) { s.Receivers[0].Grant.Identity.EntryNonce = "foreign" }},
			{"landed missing parent sync closure", "landed", func(s *xfercontract.WorkerState) { s.Receivers[0].Atomic.Parent = xfercontract.ResourceRef{} }},
			{"settled retains data FD", "settled", func(s *xfercontract.WorkerState) {
				s.Receivers = workerFixture("receiver").Receivers
				s.Receivers[0].Atomic.Phase = xfercontract.PhaseSettled
			}},
			{"canonical digest corruption", "courier", func(s *xfercontract.WorkerState) {
				s.CanonicalOutcomes[0].Canonical.Digest = strings.Repeat("0", 64)
				s.Couriers[0].Canonical = s.CanonicalOutcomes[0].Canonical
			}},
			{"opposite receipt", "courier", func(s *xfercontract.WorkerState) {
				r := codecTerminal(s.CanonicalOutcomes[0])
				r.Kind = "failed"
				s.Couriers[0].Receipt = &r
			}},
			{"stopped courier lacks ACK", "courier", func(s *xfercontract.WorkerState) { s.Couriers[0].Stopped = true }},
			{"pending outcome lost courier", "courier", func(s *xfercontract.WorkerState) { s.Couriers = nil }},
			{"wrong terminal identity", "settled", func(s *xfercontract.WorkerState) { s.Entries[0].Status.Terminal.TerminalID = strings.Repeat("0", 64) }},
			{"settled cleanup obligation", "settled", func(s *xfercontract.WorkerState) { s.Couriers[0].Cleanup.OutboxRetirePending = true }},
			{"understated codec copies", "prepared", func(s *xfercontract.WorkerState) { s.CapacityReservations[0].Reservation.NonReplayBytes = 1 }},
			{"understated FD peak", "receiver", func(s *xfercontract.WorkerState) { s.CapacityReservations[0].Reservation.FDs = 1 }},
			{"open frozen admission", "prepared", func(s *xfercontract.WorkerState) { s.Admission.Closed = false }},
			{"callback still inflight", "prepared", func(s *xfercontract.WorkerState) { s.Admission.Inflight = 1 }},
			{"foreign agent instance", "prepared", func(s *xfercontract.WorkerState) {
				s.Entries[0].Prepare.Identity.AgentInstance = "foreign"
				s.Entries[0].Prepare.Permit.Identity = s.Entries[0].Prepare.Identity
				s.Entries[0].Status.Identity = s.Entries[0].Prepare.Identity
			}},
			{"inline source digest", "inline", func(s *xfercontract.WorkerState) { s.Entries[0].Prepare.Inline[0] ^= 1 }},
			{"wrong boot deadline", "receiver", func(s *xfercontract.WorkerState) { s.Receivers[0].ByteDeadline.BootID = "foreign" }},
			{"orphan actor", "receiver", func(s *xfercontract.WorkerState) { s.Receivers[0].Identity.EntryNonce = "foreign" }},
			{"duplicate actor", "receiver", func(s *xfercontract.WorkerState) { s.Receivers = append(s.Receivers, s.Receivers[0]) }},
			{"duplicate entry", "prepared", func(s *xfercontract.WorkerState) { s.Entries = append(s.Entries, s.Entries[0]) }},
			{"missing reservation", "prepared", func(s *xfercontract.WorkerState) { s.CapacityReservations = nil }},
			{"aggregate sealed capacity", "prepared", func(s *xfercontract.WorkerState) {
				s.CapacityReservations[0].Reservation.SealedBytes = MaxWorkerStateBytes + 1
			}},
			{"oversized geometry", "sender", func(s *xfercontract.WorkerState) { s.Senders[0].Writer.ObjectSpec.Chunks = 65537 }},
			{"budget reset beyond maximum", "prepared", func(s *xfercontract.WorkerState) {
				s.Entries[0].Prepare.Budget.EffectiveBudgetNS = int64(2109 * time.Second)
			}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				s := workerFixture(tc.mode)
				tc.mutate(&s)
				if data, err := EncodeWorker(s); err == nil || data != nil {
					t.Fatal("encoder accepted invalid responsibility")
				}
				// Repair the transport digest to independently reach semantic
				// validation on decode rather than only an envelope checksum.
				if got, err := DecodeWorker(rawWorkerImage(t, s)); err == nil || !reflect.DeepEqual(got, xfercontract.WorkerState{}) {
					t.Fatal("decoder accepted invalid responsibility or returned partial state")
				}
			})
		}
	})

	t.Run("bounded envelope and hostile lengths", func(t *testing.T) {
		valid, err := EncodeWorker(workerFixture("receiver"))
		if err != nil {
			t.Fatal(err)
		}
		for i := range valid {
			if _, err := DecodeWorker(valid[:i]); err == nil {
				t.Fatalf("accepted truncation at %d", i)
			}
		}
		for _, index := range []int{0, 8, 12, 16, 48, len(valid) - 1} {
			bad := bytes.Clone(valid)
			bad[index] ^= 1
			if _, err := DecodeWorker(bad); err == nil {
				t.Fatalf("accepted envelope corruption at %d", index)
			}
		}
		bad := append(bytes.Clone(valid), 0)
		if _, err := DecodeWorker(bad); err == nil {
			t.Fatal("accepted trailing data")
		}
		bad = bytes.Clone(valid)
		binary.BigEndian.PutUint32(bad[workerHeaderBytes:], ^uint32(0)-1)
		repairWorkerDigest(bad)
		if _, err := DecodeWorker(bad); err == nil {
			t.Fatal("untrusted string length allocated")
		}
		if _, err := DecodeWorker(make([]byte, MaxWorkerStateBytes+1)); !errors.Is(err, ErrWorkerCapacity) {
			t.Fatal("missing decode capacity guard")
		}
		large := workerFixture("prepared")
		large.Entries[0].Prepare.Inline = make([]byte, MaxWorkerStateBytes+1)
		// Exercise measurement independently of the decoded-heap guard: an
		// oversized buffer must be refused before semantic checks or output.
		size := workerHeaderBytes
		if !errors.Is(measureWorkerValue(reflect.ValueOf(large.Entries[0].Prepare.Inline), &size), ErrWorkerCapacity) {
			t.Fatal("oversized state passed binary size measurement")
		}
		if data, err := EncodeWorker(large); !errors.Is(err, ErrWorkerCapacity) || data != nil {
			t.Fatal("missing preallocation encode capacity guard")
		}
		// Length tags and heap charges are independently checked, even when
		// the envelope checksum has been repaired by a hostile producer.
		var slice []xfercontract.WorkerEntry
		payload := binary.BigEndian.AppendUint32(nil, 1024)
		payload = append(payload, make([]byte, 1024)...)
		heap := 1
		if readWorkerValue(&payload, reflect.ValueOf(&slice).Elem(), &heap) == nil || slice != nil {
			t.Fatal("allocated untrusted slice before heap check")
		}
		var pointer *xfercontract.Prepare
		payload, heap = []byte{2}, MaxWorkerStateBytes
		if readWorkerValue(&payload, reflect.ValueOf(&pointer).Elem(), &heap) == nil {
			t.Fatal("accepted noncanonical pointer presence")
		}
		var flag bool
		payload = []byte{2}
		if readWorkerValue(&payload, reflect.ValueOf(&flag).Elem(), &heap) == nil {
			t.Fatal("accepted noncanonical boolean")
		}
		var text string
		payload = []byte{0, 0, 0, 1, 0xff}
		if readWorkerValue(&payload, reflect.ValueOf(&text).Elem(), &heap) == nil {
			t.Fatal("accepted invalid UTF-8")
		}
	})

	t.Run("EOF hash and time framing", func(t *testing.T) {
		body := []byte("whole synced prefix")
		state := codecHash(body)
		if !workerHash(state, int64(len(body)), int64(len(body)), codecDigest(body)) {
			t.Fatal("valid EOF hash rejected")
		}
		state[4] ^= 1
		if workerHash(state, int64(len(body)), int64(len(body)), codecDigest(body)) {
			t.Fatal("EOF hash corruption accepted")
		}
		for _, tm := range []time.Time{time.Time{}, time.Now(), time.Unix(1, 123).In(time.FixedZone("seconds", 61))} {
			size := 0
			if measureWorkerValue(reflect.ValueOf(tm), &size) != nil {
				t.Fatal("valid time rejected")
			}
			data, err := appendWorkerValue(nil, reflect.ValueOf(tm))
			if err != nil {
				t.Fatalf("valid time did not encode: %v", err)
			}
			var got time.Time
			heap := MaxWorkerStateBytes
			if len(data) != size || readWorkerValue(&data, reflect.ValueOf(&got).Elem(), &heap) != nil || !tm.Equal(got) {
				t.Fatal("time framing changed original instant")
			}
		}
	})

	t.Run("minimum geometry receipt reservation", func(t *testing.T) {
		// All 65536 receipts for 2GiB/32KiB fit without inventing a transfer
		// pool. This also exercises all array digest bytes and large counts.
		s := workerFixture("sender")
		a := &s.Senders[0]
		a.SourceSeal.Size, a.SourceSeal.Chunks = 2<<30, 65536
		a.Writer.ObjectSpec.Size, a.Writer.ObjectSpec.Chunks = 2<<30, 65536
		a.Writer.Receipts = make([]xfercontract.ChunkReceipt, 65536)
		for i := range a.Writer.Receipts {
			a.Writer.Receipts[i] = xfercontract.ChunkReceipt{Index: uint32(i), StreamSeq: uint64(i + 1), Offset: int64(i) * 32768, Length: 32768, SHA256: sha256.Sum256([]byte{byte(i)})}
		}
		// Low-level framing independently verifies worst-case receipt size;
		// a fabricated full-file SHA state would not be a valid public cut.
		data := rawWorkerImage(t, s)
		if len(data) > 4<<20 {
			t.Fatalf("receipt codec exceeds reservation: %d", len(data))
		}
		var got xfercontract.WorkerState
		remaining, heap := data[workerHeaderBytes:], MaxWorkerStateBytes
		if readWorkerValue(&remaining, reflect.ValueOf(&got).Elem(), &heap) != nil || !reflect.DeepEqual(s, got) {
			t.Fatal("maximum geometry lost receipts")
		}
	})
}

// The sealed schema names the contract vocabulary, never the Go package that
// hosts a declaration. P00fix moved Identity/CommitGrant/AtomicState behind
// aliases into xferport without changing a field, yet the digest every reader
// compares flipped; composite and foreign types must keep their exact spelling.
// origin: hot-update S7 plan §8.3 / T17 / M08 — P00fix alias move regression
func TestWorkerDescriptorNamesIgnoreContractHostPackage(t *testing.T) {
	descriptors := WorkerDescriptors()
	aliased := 0
	for _, d := range descriptors {
		if strings.Contains(d.Record, "xferport.") || strings.Contains(d.Type, "xferport.") {
			t.Fatalf("descriptor names a host package: %s.%s %s", d.Record, d.Name, d.Type)
		}
		if d.Record == "xfercontract.AtomicState" || d.Type == "xfercontract.CommitGrant" {
			aliased++
		}
	}
	if aliased == 0 {
		t.Fatal("no xferport-hosted record reached the descriptor closure; this guard went vacuous")
	}
	for _, tc := range []struct {
		typ  reflect.Type
		want string
	}{
		{reflect.TypeFor[xferport.Identity](), "xfercontract.Identity"},
		{reflect.TypeFor[xfercontract.Status](), "xfercontract.Status"},
		{reflect.TypeFor[*xferport.CommitGrant](), "*xfercontract.CommitGrant"},
		{reflect.TypeFor[[]xferport.ResourceRef](), "[]xfercontract.ResourceRef"},
		{reflect.TypeFor[[2]xferport.FileIdentity](), "[2]xfercontract.FileIdentity"},
		{reflect.TypeFor[map[xferport.Phase]*xfercontract.Status](), "map[xfercontract.Phase]*xfercontract.Status"},
		{reflect.TypeFor[handoff.Resource](), "handoff.Resource"},
		{reflect.TypeFor[[]time.Time](), "[]time.Time"},
		{reflect.TypeFor[[32]uint8](), "[32]uint8"},
	} {
		if got := contractTypeName(tc.typ); got != tc.want {
			t.Errorf("contractTypeName(%s) = %q, want %q", tc.typ, got, tc.want)
		}
	}
}

// The jsstream reader exports an open reader on its attachment with no
// consumer (not yet fetched, or retired by a refused cut before the next
// fetch). It owes no deletion and must cut: refusing it failed live Tier B
// pushes' agent cuts at random. A consumer without its connection, or an
// identity that is not a token, still refuses.
// origin: hot-update S7 plan §3 / §8.2 / T17 / FIX-INLINE (agent cut push flake)
func TestTransferWorkerImageAcceptsOpenReaderWithoutConsumer(t *testing.T) {
	for _, tc := range []struct {
		name, consumer, connection string
		valid                      bool
	}{
		{"unopened", "", "", true},
		{"attached without consumer", "", "connection", true},
		{"consumer without connection", "consumer", "", false},
		{"forged connection token", "", "connection/forged", false},
	} {
		s := workerFixture("receiver")
		r := &s.Receivers[0]
		r.Reader.Phase, r.Reader.ConsumerIdentity, r.Reader.ConnectionIdentity = xfercontract.ReaderOpen, tc.consumer, tc.connection
		if _, err := EncodeWorker(s); (err == nil) != tc.valid {
			t.Fatalf("%s: encode err=%v, want valid=%t", tc.name, err, tc.valid)
		}
	}
}

// A courier started from a real receiver outcome waits for the deciding
// broker's canonical bytes before its first exact receipt; xfercourier.Import
// carries that state, so the image must too. A stopped, acknowledged or
// canonical-bearing courier still needs its retained decision.
// origin: hot-update S7 plan §4 / §8.2 / T14 / FIX-INLINE (agent cut push flake)
func TestTransferWorkerImageCarriesCourierAwaitingCanonical(t *testing.T) {
	awaiting := func() xfercontract.WorkerState {
		s := workerFixture("courier")
		s.CanonicalOutcomes = nil
		s.Couriers[0].Canonical, s.Couriers[0].NextAttempt = xfercontract.CanonicalRecord{}, xfercontract.BootDeadline{}
		return s
	}
	state := awaiting()
	data, err := EncodeWorker(state)
	if err != nil {
		t.Fatalf("courier awaiting its canonical decision is not representable: %v", err)
	}
	if got, err := DecodeWorker(data); err != nil || !reflect.DeepEqual(got, state) {
		t.Fatalf("awaiting courier did not round-trip: %v", err)
	}
	for name, mutate := range map[string]func(*xfercontract.WorkerState){
		"stopped without receipt": func(s *xfercontract.WorkerState) { s.Couriers[0].Stopped = true },
		"canonical without decision": func(s *xfercontract.WorkerState) {
			s.Couriers[0].Canonical = workerFixture("courier").CanonicalOutcomes[0].Canonical
		},
		"no receiver outcome": func(s *xfercontract.WorkerState) { s.Couriers[0].Outcome = xfercontract.ReceiverOutcome{} },
		"foreign retry boot": func(s *xfercontract.WorkerState) {
			s.Couriers[0].NextAttempt = xfercontract.BootDeadline{BootID: "other", Deadline: 1}
		},
		"landed without grant": func(s *xfercontract.WorkerState) { s.Couriers[0].Outcome.Grant = xfercontract.CommitGrant{} },
	} {
		s := awaiting()
		mutate(&s)
		if _, err := EncodeWorker(s); err == nil {
			t.Fatalf("%s: undecided courier image accepted", name)
		}
	}
}
