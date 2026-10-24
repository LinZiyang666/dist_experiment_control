package xferimage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// origin: hot-update S7 plan §5.1 / §5.3 / §8.2–§8.3 / T20
func TestTransferTrackerImagePreservesBudgetsAndLiveProtection(t *testing.T) {
	t.Run("current revisions preserve every retained responsibility", func(t *testing.T) {
		for _, tier := range []string{"a", "b"} {
			for _, verb := range []string{"push", "pull"} {
				for generation := uint64(7); generation < 10; generation++ {
					s := trackerFixture(tier, verb)
					s.Generation = generation
					s.Admission.Generation = generation
					s.ProvisioningFence.Generation = generation
					// This is a fresh current cut with an advanced canonical revision,
					// not a repack of the pristine imported state.
					s.Entries[0].Revision = generation + 12
					s.LedgerRevision = generation + 12
					trackerRefreshCensus(&s)
					encoded, err := EncodeTracker(s)
					if err != nil {
						t.Fatalf("encode %s/%s generation %d: %v", tier, verb, generation, err)
					}
					decoded, err := DecodeTracker(encoded)
					if err != nil || !reflect.DeepEqual(s, decoded) {
						t.Fatalf("lost original start, grant, live protection or cleanup: %v", err)
					}
					if !decoded.Entries[0].Cleanup.PrimaryRetirePending || !decoded.Entries[0].Cleanup.OutboxRetirePending ||
						decoded.Entries[0].StartDelivery != xfercontract.DeliveryRaftCommitted ||
						decoded.Entries[0].BudgetSeal.RemainingNS == decoded.Entries[0].BudgetSeal.EffectiveBudgetNS {
						t.Fatal("fixture lost adversarial pending/consumed-budget window")
					}
				}
			}
		}
	})
	t.Run("tier budget bounds", func(t *testing.T) {
		for _, tier := range []string{"a", "b"} {
			s := trackerFixture(tier, "push")
			s.Entries[0].BudgetSeal.EffectiveBudgetNS = int64(time.Minute)
			s.Entries[0].BudgetSeal.RemainingNS = int64(9 * time.Second)
			trackerRefreshCensus(&s)
			if encoded, err := EncodeTracker(s); err == nil || encoded != nil {
				t.Fatalf("tier %s admitted an impossible sealed budget", tier)
			}
			body, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			trackerAssertRefused(t, trackerFrame(body))
		}
	})
	t.Run("empty frozen tracker retains lineage", func(t *testing.T) {
		s := trackerFixture("b", "push")
		s.Entries, s.Reaped, s.PendingStart, s.PendingTerminal = nil, nil, nil, nil
		s.GrantSlots, s.WatchdogDeadlines, s.Cleanup, s.ConfigAttempts = nil, nil, nil, nil
		s.Census.Rows, s.Census.Entries = nil, nil
		encoded, err := EncodeTracker(s)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeTracker(encoded)
		if err != nil || !reflect.DeepEqual(s, decoded) {
			t.Fatalf("empty tracker lost frozen lineage: %v", err)
		}
	})
	t.Run("phase and history proof closure", func(t *testing.T) {
		for _, phase := range []xfercontract.Phase{xfercontract.PhaseOpen, xfercontract.PhasePrepared,
			xfercontract.PhaseSending, xfercontract.PhaseReceiving, xfercontract.PhaseCommitReserved,
			xfercontract.PhaseDecided, xfercontract.PhaseTerminalPending, xfercontract.PhaseHistoryConfirmed,
			xfercontract.PhaseCleanupPending, xfercontract.PhaseSettled} {
			s := trackerFixture("b", "pull")
			s.Entries[0].Phase = phase
			if phase == xfercontract.PhaseOpen || phase == xfercontract.PhasePrepared ||
				phase == xfercontract.PhaseSending || phase == xfercontract.PhaseReceiving || phase == xfercontract.PhaseCommitReserved {
				s.Entries[0].ReceiverOutcome, s.Entries[0].CanonicalTerminal = nil, nil
				s.Entries[0].TerminalDelivery = ""
				s.PendingTerminal = nil
				if phase != xfercontract.PhaseCommitReserved {
					s.Entries[0].Grant = nil
					s.GrantSlots = nil
				}
			}
			if phase == xfercontract.PhaseHistoryConfirmed || phase == xfercontract.PhaseCleanupPending || phase == xfercontract.PhaseSettled {
				row := &s.Entries[0]
				row.StartDelivery, row.TerminalDelivery = xfercontract.DeliveryHistoryConfirmed, xfercontract.DeliveryHistoryConfirmed
				row.HistoryReceipts = []xfercontract.HistoryReceipt{{Stream: "history-session", Sequence: 10, RecordDigest: row.CanonicalStart.Digest},
					{Stream: "history-session", Sequence: 31, RecordDigest: row.CanonicalTerminal.Digest}}
				s.PendingStart, s.PendingTerminal = nil, nil
			}
			if phase == xfercontract.PhaseSettled {
				s.Entries[0].Cleanup = xfercontract.CleanupState{Identity: s.Entries[0].Identity}
				s.Cleanup, s.GrantSlots = nil, nil
			}
			trackerRefreshCensus(&s)
			encoded, err := EncodeTracker(s)
			if err != nil {
				t.Fatalf("phase %s: %v", phase, err)
			}
			decoded, err := DecodeTracker(encoded)
			if err != nil || !reflect.DeepEqual(s, decoded) {
				t.Fatalf("phase %s changed proof: %v", phase, err)
			}
		}
	})
	t.Run("abandonment preserves unknown outcome", func(t *testing.T) {
		s := trackerFixture("b", "push")
		s.Entries[0].OutcomeUnknown = true
		s.Entries[0].ReceiverOutcome = nil
		terminal := trackerCanonical(s.Entries[0].Identity, "failed", `{"kind":"failed","code":"settlement_abandoned","outcome_unknown":true}`)
		s.Entries[0].CanonicalTerminal = &terminal
		s.PendingTerminal = []xfercontract.CanonicalRecord{terminal}
		trackerRefreshCensus(&s)
		encoded, err := EncodeTracker(s)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeTracker(encoded)
		if err != nil || !reflect.DeepEqual(s, decoded) || !decoded.Entries[0].OutcomeUnknown {
			t.Fatalf("abandonment changed an unresolved result: %v", err)
		}
	})
	t.Run("every actual root field is encoded and mandatory", func(t *testing.T) {
		s := trackerFixture("b", "push")
		encoded, err := EncodeTracker(s)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(encoded[trackerHeaderBytes:], &fields); err != nil {
			t.Fatal(err)
		}
		typ := reflect.TypeFor[xfercontract.TrackerState]()
		if len(fields) != typ.NumField() {
			t.Fatal("actual encoder and P00 field inventory disagree")
		}
		for i := 0; i < typ.NumField(); i++ {
			name := typ.Field(i).Name
			original, exists := fields[name]
			if !exists {
				t.Fatalf("missing encoded field %s", name)
			}
			delete(fields, name)
			body, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			trackerAssertRefused(t, trackerFrame(body))
			fields[name] = original
		}
	})
	t.Run("hostile private states fail on both boundaries", func(t *testing.T) {
		mutations := []struct {
			name string
			edit func(*xfercontract.TrackerState)
		}{
			{"schema", func(s *xfercontract.TrackerState) { s.Schema = "worker-state-v1" }},
			{"origin boot missing", func(s *xfercontract.TrackerState) { s.Origin.BootID = "" }},
			{"stale generation", func(s *xfercontract.TrackerState) { s.Generation = 1 }},
			{"open admission", func(s *xfercontract.TrackerState) { s.Admission.Closed = false }},
			{"callback not joined", func(s *xfercontract.TrackerState) { s.Admission.Inflight = 1 }},
			{"provisioner not joined", func(s *xfercontract.TrackerState) { s.ProvisioningFence.Inflight = 1 }},
			{"duplicate entry", func(s *xfercontract.TrackerState) { s.Entries = append(s.Entries, s.Entries[0]) }},
			{"entry nonce changed", func(s *xfercontract.TrackerState) { s.Entries[0].EntryNonce = "new-nonce" }},
			{"permit identity changed", func(s *xfercontract.TrackerState) { s.Entries[0].Permit.Identity.ActorFP = "another-actor" }},
			{"budget start reset", func(s *xfercontract.TrackerState) {
				s.Entries[0].BudgetSeal.StartedAt = s.Entries[0].StartedAt.Add(time.Second)
			}},
			{"rate changed", func(s *xfercontract.TrackerState) { s.Entries[0].BudgetSeal.RateLimitBPS++ }},
			{"remaining budget enlarged", func(s *xfercontract.TrackerState) { s.Entries[0].BudgetSeal.RemainingNS = int64(time.Hour) }},
			{"budget exceeds product ceiling", func(s *xfercontract.TrackerState) { s.Entries[0].BudgetSeal.EffectiveBudgetNS = int64(time.Hour) }},
			{"source geometry changed", func(s *xfercontract.TrackerState) { s.Entries[0].SourceSeal.Chunks++ }},
			{"watchdog boot mismatch", func(s *xfercontract.TrackerState) { s.WatchdogDeadlines[0].ByteDeadline.BootID = "different-boot" }},
			{"retention boot mismatch", func(s *xfercontract.TrackerState) { s.Entries[0].Retention.Deadline.BootID = "different-boot" }},
			{"reaped boot mismatch", func(s *xfercontract.TrackerState) { s.Reaped[0].Retention.Deadline.BootID = "different-boot" }},
			{"duplicate reaped", func(s *xfercontract.TrackerState) { s.Reaped = append(s.Reaped, s.Reaped[0]) }},
			{"reaped live identity", func(s *xfercontract.TrackerState) { s.Reaped[0].Identity = s.Entries[0].Identity }},
			{"retention before permit expiry", func(s *xfercontract.TrackerState) {
				s.Reaped[0].Retention.ResultUntil = s.Reaped[0].Retention.CreateUntil.Add(-time.Second)
			}},
			{"watchdog dangling", func(s *xfercontract.TrackerState) { s.WatchdogDeadlines[0].Identity.EntryNonce = "new" }},
			{"duplicate watchdog", func(s *xfercontract.TrackerState) {
				s.WatchdogDeadlines = append(s.WatchdogDeadlines, s.WatchdogDeadlines[0])
			}},
			{"grant lost", func(s *xfercontract.TrackerState) { s.GrantSlots = nil }},
			{"grant nonce replaced", func(s *xfercontract.TrackerState) { s.GrantSlots[0].Grant.GrantNonce = "replacement" }},
			{"grant future revision", func(s *xfercontract.TrackerState) { s.GrantSlots[0].Revision++ }},
			{"ledger stale revision", func(s *xfercontract.TrackerState) { s.LedgerRevision-- }},
			{"start publisher lost", func(s *xfercontract.TrackerState) { s.PendingStart = nil }},
			{"terminal publisher lost", func(s *xfercontract.TrackerState) { s.PendingTerminal = nil }},
			{"terminal bytes corrupted", func(s *xfercontract.TrackerState) { s.Entries[0].CanonicalTerminal.Bytes[7] = 'x' }},
			{"start copied over terminal", func(s *xfercontract.TrackerState) { s.Entries[0].CanonicalTerminal = s.Entries[0].CanonicalStart }},
			{"publication kind changed", func(s *xfercontract.TrackerState) { s.PendingTerminal[0].Kind = "failed" }},
			{"duplicate terminal obligation", func(s *xfercontract.TrackerState) {
				s.PendingTerminal = append(s.PendingTerminal, s.PendingTerminal[0])
			}},
			{"history ACK absent", func(s *xfercontract.TrackerState) {
				s.Entries[0].TerminalDelivery = xfercontract.DeliveryHistoryConfirmed
			}},
			{"complete without directory durability", func(s *xfercontract.TrackerState) { s.Entries[0].ReceiverOutcome.ParentSynced = false }},
			{"unknown outcome marked complete", func(s *xfercontract.TrackerState) { s.Entries[0].OutcomeUnknown = true }},
			{"inflight grant guessed failed", func(s *xfercontract.TrackerState) {
				terminal := trackerCanonical(s.Entries[0].Identity, "failed", `{"kind":"failed","code":"deadline"}`)
				s.Entries[0].CanonicalTerminal = &terminal
				s.Entries[0].ReceiverOutcome.Outcome = xfercontract.OutcomeInflight
				s.PendingTerminal = []xfercontract.CanonicalRecord{terminal}
				trackerRefreshCensus(s)
			}},
			{"unknown outcome without abandonment", func(s *xfercontract.TrackerState) {
				terminal := trackerCanonical(s.Entries[0].Identity, "failed", `{"kind":"failed","code":"deadline","outcome_unknown":true}`)
				s.Entries[0].CanonicalTerminal = &terminal
				s.Entries[0].OutcomeUnknown = true
				s.PendingTerminal = []xfercontract.CanonicalRecord{terminal}
				trackerRefreshCensus(s)
			}},
			{"opposite receiver grant", func(s *xfercontract.TrackerState) { s.Entries[0].ReceiverOutcome.Grant.GrantNonce = "another" }},
			{"phase unknown", func(s *xfercontract.TrackerState) { s.Entries[0].Phase = "unknown-phase" }},
			{"terminal in byte phase", func(s *xfercontract.TrackerState) {
				s.Entries[0].Phase = xfercontract.PhaseSending
				trackerRefreshCensus(s)
			}},
			{"delivery unknown", func(s *xfercontract.TrackerState) { s.Entries[0].StartDelivery = "sent" }},
			{"outcome unknown", func(s *xfercontract.TrackerState) { s.Entries[0].ReceiverOutcome.Outcome = "guessed" }},
			{"cleanup owner lost", func(s *xfercontract.TrackerState) { s.Cleanup = nil }},
			{"outbox responsibility lost", func(s *xfercontract.TrackerState) { s.Cleanup[0].OutboxRetirePending = false }},
			{"primary responsibility lost", func(s *xfercontract.TrackerState) { s.Cleanup[0].PrimaryRetirePending = false }},
			{"census falsely empty", func(s *xfercontract.TrackerState) { s.Census.Rows = nil }},
			{"live protection lost", func(s *xfercontract.TrackerState) { s.Census.Entries = nil }},
			{"foreign object version", func(s *xfercontract.TrackerState) { s.Census.Entries[0].ObjectVersion = "another-object" }},
			{"config boot mismatch", func(s *xfercontract.TrackerState) { s.ConfigAttempts[0].Proof.BootID = "another-boot" }},
			{"sent unknown falsely joined", func(s *xfercontract.TrackerState) { s.ConfigAttempts[0].Proof.RequestsJoined = true }},
			{"config attempt not sent", func(s *xfercontract.TrackerState) { s.ConfigAttempts[0].Attempt.Sent = false }},
			{"consumer identity replaced", func(s *xfercontract.TrackerState) {
				s.Entries[0].Cleanup.Consumers[0].ObjectSpec.Identity.EntryNonce = "replacement"
			}},
			{"synced hash changed", func(s *xfercontract.TrackerState) {
				s.Entries[0].Cleanup.Consumers[0].SinkCheckpoint.SHA256State[0] = 'x'
			}},
			{"synced offset changed", func(s *xfercontract.TrackerState) {
				checkpoint := &s.Entries[0].Cleanup.Consumers[0].SinkCheckpoint
				checkpoint.DurableOffset++
				hash := sha256.New()
				hash.Write(make([]byte, checkpoint.DurableOffset))
				checkpoint.SHA256State, _ = hash.(interface{ MarshalBinary() ([]byte, error) }).MarshalBinary()
			}},
			{"purge without retirement", func(s *xfercontract.TrackerState) {
				s.Entries[0].Cleanup.WriterFence.Purged = true
				s.Cleanup[0] = s.Entries[0].Cleanup
				trackerRefreshCensus(s)
			}},
			{"history receipt missing sequence", func(s *xfercontract.TrackerState) { s.Entries[0].Cleanup.WriterFence.Receipt.Sequence = 0 }},
			{"config seal changed owner", func(s *xfercontract.TrackerState) { s.ConfigAttempts[0].Seal.Proof.Nonce = "different-config-owner" }},
			{"too many entries", func(s *xfercontract.TrackerState) { s.Entries = make([]xfercontract.LedgerRow, trackerMaxEntries+1) }},
			{"huge canonical input", func(s *xfercontract.TrackerState) {
				s.Entries[0].CanonicalTerminal.Bytes = make([]byte, trackerMaxBytes)
			}},
		}
		for _, mutation := range mutations {
			t.Run(mutation.name, func(t *testing.T) {
				s := trackerFixture("b", "push")
				mutation.edit(&s)
				if encoded, err := EncodeTracker(s); err == nil || encoded != nil {
					t.Fatal("encoder accepted inconsistent ownership")
				}
				body, err := json.Marshal(s)
				if err != nil {
					t.Fatal(err)
				}
				trackerAssertRefused(t, trackerFrame(body))
			})
		}
	})
	t.Run("strict bounded frame", func(t *testing.T) {
		encoded, err := EncodeTracker(trackerFixture("b", "push"))
		if err != nil {
			t.Fatal(err)
		}
		for n := 0; n < trackerHeaderBytes; n++ {
			trackerAssertRefused(t, encoded[:n])
		}
		trackerAssertRefused(t, encoded[:len(encoded)-1])
		trackerAssertRefused(t, append(bytes.Clone(encoded), ' '))
		wrongVersion := bytes.Clone(encoded)
		wrongVersion[11]++
		trackerAssertRefused(t, wrongVersion)
		wrongLength := bytes.Clone(encoded)
		binary.BigEndian.PutUint32(wrongLength[12:16], ^uint32(0))
		trackerAssertRefused(t, wrongLength)
		wrongRole := bytes.Clone(encoded)
		wrongRole[5] = 'W'
		trackerAssertRefused(t, wrongRole)
		body := encoded[trackerHeaderBytes:]
		for _, hostile := range [][]byte{
			append(bytes.Clone(body), []byte("{}")...), append([]byte(" "), body...),
			bytes.Replace(body, []byte(`"Schema":`), []byte(`"Schema":"xfer-state-v1","Schema":`), 1),
			bytes.Replace(body, []byte(`"Schema":`), []byte(`"schema":`), 1),
			bytes.Replace(body, []byte(`"Schema":`), []byte(`"Unexpected":false,"Schema":`), 1),
			bytes.Replace(body, []byte(`"Closed":true`), []byte(`"Closed":true,"Closed":false`), 1),
		} {
			trackerAssertRefused(t, trackerFrame(hostile))
		}
		oversized := trackerFrame(make([]byte, trackerMaxBytes))
		if trackerValidFrame(oversized) {
			t.Fatal("oversized frame reached JSON allocation stage")
		}
		trackerAssertRefused(t, oversized)
		// Many tiny objects can otherwise allocate a huge LedgerRow backing
		// array from a small JSON input before semantic validation runs.
		hostile := []byte(`{"Entries":[` + strings.Repeat(`{},`, trackerMaxEntries) + `{}]}`)
		scanner := json.NewDecoder(bytes.NewReader(hostile))
		scanner.UseNumber()
		// Isolate the collection bound from the independent heap bound so
		// either guard's removal has a non-vacuous adversarial control.
		allocation := int64(1 << 60)
		if trackerScan(scanner, reflect.TypeFor[xfercontract.TrackerState](), &allocation) {
			t.Fatal("unbounded collection reached typed allocation stage")
		}
		trackerAssertRefused(t, trackerFrame(hostile))
		scanner = json.NewDecoder(bytes.NewReader(body))
		scanner.UseNumber()
		allocation = 1
		if trackerScan(scanner, reflect.TypeFor[xfercontract.TrackerState](), &allocation) {
			t.Fatal("unreserved backing storage reached typed allocation stage")
		}
	})
}

func trackerAssertRefused(t *testing.T, encoded []byte) {
	t.Helper()
	state, err := DecodeTracker(encoded)
	if err == nil || !reflect.DeepEqual(state, xfercontract.TrackerState{}) || err.Error() != errTrackerImage.Error() {
		t.Fatal("decoder accepted malformed state or returned partial/private evidence")
	}
}

func trackerFrame(body []byte) []byte {
	encoded := make([]byte, trackerHeaderBytes+len(body))
	copy(encoded, trackerMagic)
	binary.BigEndian.PutUint32(encoded[8:12], trackerCodecVersion)
	binary.BigEndian.PutUint32(encoded[12:16], uint32(len(body)))
	copy(encoded[trackerHeaderBytes:], body)
	return encoded
}

func trackerFixture(tier, verb string) xfercontract.TrackerState {
	started := time.Date(2026, 10, 12, 1, 2, 3, 400, time.UTC)
	digest := sha256.Sum256([]byte("sealed intent"))
	sha := hex.EncodeToString(digest[:])
	id := xfercontract.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent-instance",
		TransferID: "transfer", EntryNonce: "entry-nonce", IntentDigest: sha}
	if tier == "b" {
		id.ObjectVersion = "fixed-nuid"
	}
	origin := xfercontract.Origin{BrokerID: "original-broker", InstanceID: "broker-instance", BootID: "boot-original", Generation: 3}
	grant := xfercontract.CommitGrant{Identity: id, GrantNonce: "grant-original", ReceiverInstance: "receiver-instance", TerminalID: "terminal-slot"}
	start := trackerCanonical(id, "start", `{"ts":"original-ts","kind":"start"}`)
	terminal := trackerCanonical(id, "complete", `{"ts":"original-terminal-ts","kind":"complete"}`)
	deadline := xfercontract.BootDeadline{BootID: origin.BootID, Deadline: 99123456789}
	retention := xfercontract.RetentionState{Identity: id, CreateUntil: started.Add(time.Minute), ResultUntil: started.Add(20 * time.Minute), Deadline: deadline}
	cleanup := xfercontract.CleanupState{Identity: id, TmpUnlinkPending: true, DirectorySyncPending: true,
		ObjectPurgePending: tier == "b", PrimaryRetirePending: true, OutboxRetirePending: true,
		WriterFence: xfercontract.RetirementFence{ExpectedStreamSeq: 67, PreviousMetadataSeq: 55, WriterRetired: true,
			Receipt: &xfercontract.HistoryReceipt{Stream: "OBJ_xfer-session", Sequence: 76, RecordDigest: sha}}}
	row := xfercontract.LedgerRow{Identity: id, Origin: origin, EntryNonce: id.EntryNonce, Revision: 19,
		Permit: xfercontract.CreationPermit{Identity: id, Origin: origin, IssuedAt: started.Add(-time.Second),
			CreateUntil: retention.CreateUntil, PermitNonce: "creation-nonce", Signature: "sealed-signature"},
		Intent:    xfercontract.Intent{Tier: tier, Verb: verb, Path: "destination", Size: 1048576, SHA256: sha, RateLimitBPS: 1048576, ChunkSize: 32768},
		StartedAt: started, BudgetSeal: xfercontract.BudgetSeal{StartedAt: started, Size: 1048576, RateLimitBPS: 1048576,
			EffectiveBudgetNS: int64(300 * time.Second), RemainingNS: int64(90 * time.Second), SourceSealed: true},
		SourceSeal: &xfercontract.SourceSeal{Size: 1048576, SHA256: sha, RateLimitBPS: 1048576, SourceFingerprint: "pinned-source"},
		Phase:      xfercontract.PhaseTerminalPending, Grant: &grant,
		ReceiverOutcome: &xfercontract.ReceiverOutcome{Grant: grant, Primitive: xfercontract.PrimitiveLink, Outcome: xfercontract.OutcomeLanded,
			Bytes: 1048576, SHA256: sha, ParentSynced: true, TmpCleanupPending: true},
		CanonicalStart: &start, CanonicalTerminal: &terminal, StartDelivery: xfercontract.DeliveryRaftCommitted,
		TerminalDelivery: xfercontract.DeliveryPending, Cleanup: cleanup, Retention: retention,
		Settlement: &xfercontract.SettlementCapsule{Identity: id, Origin: origin, ReceiverNkey: "receiver-key", Issuer: "original-issuer", Nonce: "capsule-nonce", Signature: "private-capsule-signature"}}
	if tier == "a" {
		row.BudgetSeal.EffectiveBudgetNS = int64(30 * time.Second)
		row.BudgetSeal.RemainingNS = int64(9 * time.Second)
	}
	if tier == "b" {
		row.SourceSeal.ChunkSize, row.SourceSeal.Chunks = 32768, 32
		hash := sha256.New()
		hash.Write(make([]byte, 32768))
		hashState, _ := hash.(interface{ MarshalBinary() ([]byte, error) }).MarshalBinary()
		cleanup.Consumers = []xfercontract.ReaderState{{ObjectSpec: xfercontract.ObjectSpec{Identity: id, Bucket: "xfer-session", Stream: "OBJ_xfer-session",
			StreamCreated: "original-stream", SHA256: sha, Size: 1048576, ChunkSize: 32768, Chunks: 32},
			Phase: xfercontract.ReaderDeletePending, ConsumerIdentity: "original-consumer", ConnectionIdentity: "original-connection", DirtyBarrier: true, DeletePending: true,
			SinkCheckpoint: xfercontract.SinkCheckpoint{ChunkIndex: 1, DurableOffset: 32768, LastConsumedStreamSeq: 61, SHA256State: hashState}}}
		row.Cleanup = cleanup
	}
	reapedID := id
	reapedID.TransferID, reapedID.EntryNonce = "reaped-transfer", "reaped-nonce"
	reapedRetention := retention
	reapedRetention.Identity = reapedID
	proof := xfercontract.ConfigWriterProof{BrokerID: origin.BrokerID, InstanceID: origin.InstanceID, BootID: origin.BootID,
		Bucket: "xfer-session", Nonce: "config-nonce", Generation: 4, GateClosed: true}
	s := xfercontract.TrackerState{Schema: xfercontract.StateSchema, Origin: origin, Generation: 7,
		Admission: xfercontract.AdmissionState{Closed: true, Generation: 7}, ProvisioningFence: xfercontract.AdmissionState{Closed: true, Generation: 7},
		Entries: []xfercontract.LedgerRow{row}, Reaped: []xfercontract.ReapedEntry{{Identity: reapedID, At: started.Add(12 * time.Minute), Retention: reapedRetention}},
		WatchdogDeadlines: []xfercontract.WatchdogDeadline{{Identity: id, ByteDeadline: deadline, Fired: true}},
		PendingStart:      []xfercontract.CanonicalRecord{start}, PendingTerminal: []xfercontract.CanonicalRecord{terminal},
		GrantSlots: []xfercontract.GrantSlot{{Identity: id, Grant: grant, Revision: row.Revision}}, LedgerRevision: row.Revision,
		Census: xfercontract.LedgerCensus{Origin: origin, Ready: true, Complete: true, Digest: sha}, Cleanup: []xfercontract.CleanupState{cleanup},
		ConfigAttempts: []xfercontract.ConfigAttempt{{Patch: xfercontract.ConfigPatch{Bucket: proof.Bucket, Replicas: 3, MaxBytes: 1 << 30,
			Duplicates: 2228 * time.Second, RecoveryHorizon: 2228 * time.Second}, Proof: proof,
			Seal: &xfercontract.BucketSeal{Bucket: proof.Bucket, Stream: "OBJ_xfer-session", StreamCreated: "original-stream", ConfigDigest: sha,
				Replicas: 3, MaxBytes: 1 << 30, Duplicates: 2228 * time.Second, MaxAge: 48 * time.Hour, MaxPayload: 1 << 20, MaxMsgSize: 1 << 20, Proof: proof},
			Attempt: controlbudget.Attempt{LogicalID: "config-attempt", Ordinal: 2, Phase: controlbudget.PhaseRequest,
				StartedMonoNS: 12345, Sent: true, Class: "unknown", BackoffNS: int64(time.Second)}, SentUnknown: true}}}
	trackerRefreshCensus(&s)
	return s
}

func trackerRefreshCensus(s *xfercontract.TrackerState) {
	s.Census.Rows = append([]xfercontract.LedgerRow(nil), s.Entries...)
	s.Census.Entries = nil
	for _, row := range s.Entries {
		s.Census.Entries = append(s.Census.Entries, xfercontract.Protection{Identity: row.Identity, Phase: row.Phase,
			ObjectVersion: row.Identity.ObjectVersion, CreationUntil: row.Permit.CreateUntil,
			WriterRetired: row.Cleanup.WriterFence.WriterRetired, OwnerGeneration: s.Generation})
	}
}

func trackerCanonical(id xfercontract.Identity, kind, payload string) xfercontract.CanonicalRecord {
	digest := sha256.Sum256([]byte(payload))
	return xfercontract.CanonicalRecord{Identity: id, Kind: kind, ReqID: "original-" + kind + "-request",
		Digest: hex.EncodeToString(digest[:]), Bytes: []byte(payload)}
}
