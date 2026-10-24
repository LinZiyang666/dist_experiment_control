package xferhistory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: hot-update S7 plan §5.2 / T25 / M12
func TestTransferHistoryCASRejectsLateSinglePublish(t *testing.T) {
	t.Run("late_retry_after_default_window", exerciseLateHistoryPublish)
	t.Run("exact_complete_and_failed", func(t *testing.T) {
		for _, kind := range []string{"complete", "failed"} {
			t.Run(kind, func(t *testing.T) {
				js, stream := historyFixture(t, "exact", nil)
				var observations []xfercontract.Observation
				p, err := New(Config{JS: js, SID: "exact", Base: xfercontract.Base{
					Observe: func(o xfercontract.Observation) { observations = append(observations, o) },
				}})
				if err != nil {
					t.Fatal(err)
				}
				for _, slot := range []string{"start", kind} {
					record := historyRecord(t, "exact", "task", slot, nil)
					first, err := p.Confirm(record, historyDeadline(t))
					if err != nil {
						t.Fatal(err)
					}
					again, err := newPublisher(t, js, "exact").Confirm(record, historyDeadline(t))
					if err != nil || first != again || first.RecordDigest != record.Digest || first.Stream != "custom_history" {
						t.Fatalf("exact replay changed receipt: %+v %+v %v", first, again, err)
					}
					if len(observations) == 0 {
						t.Fatal("exact receipt has no history observation")
					}
					o := observations[len(observations)-1]
					if o.Kind != xfercontract.ObserveHistory || o.StreamIdentity != first.Stream ||
						o.ActualACKSeq != first.Sequence || o.HistorySeq != first.Sequence || o.CanonicalDigest != first.RecordDigest ||
						o.SHA256 != first.RecordDigest || o.EntryNonce != record.Identity.EntryNonce {
						t.Fatalf("history observation lost physical identity: %+v", o)
					}
				}
				if len(observations) != 2 {
					t.Fatalf("history observations=%d, want 2", len(observations))
				}
				assertRawHistory(t, stream, "exact", "task", 1, 1)
			})
		}
	})
	// origin: hot-update S7 plan §5.2 / T25
	t.Run("raw_bytes_include_additive_fields", func(t *testing.T) {
		js, stream := historyFixture(t, "raw", nil)
		record := historyRecord(t, "raw", "task", "failed", nil)
		record.Bytes = changedHistoryField("future_audit_field", "retained")(t, record)[0]
		record.Digest = testHistoryDigest(record.Bytes)
		record.ReqID = testHistoryDigest(append([]byte("xferaudit-record:"), record.Bytes...))
		receipt, err := newPublisher(t, js, "raw").Confirm(record, historyDeadline(t))
		if err != nil || receipt.RecordDigest != record.Digest {
			t.Fatalf("history normalized away original canonical bytes: %+v %v", receipt, err)
		}
		msg, err := stream.GetMsg(t.Context(), receipt.Sequence)
		if err != nil || !bytes.Equal(msg.Data, record.Bytes) {
			t.Fatalf("physical history dropped additive audit bytes: %v", err)
		}
	})
	// origin: hot-update S7 plan §4 / §5.2 / T25
	t.Run("start_cannot_claim_terminal_slot", func(t *testing.T) {
		js, stream := historyFixture(t, "start-slot", nil)
		record := historyRecord(t, "start-slot", "task", "start", nil)
		record.Bytes = changedHistoryField("terminal_id", schema.TransferTerminalID("start-slot", "task", "entry"))(t, record)[0]
		record.Digest = testHistoryDigest(record.Bytes)
		record.ReqID = testHistoryDigest(append([]byte("xferaudit-record:"), record.Bytes...))
		if receipt, err := newPublisher(t, js, "start-slot").Confirm(record, historyDeadline(t)); !errors.Is(err, ErrConflict) || receipt.Sequence != 0 {
			t.Fatalf("START claimed the terminal slot: %+v %v", receipt, err)
		}
		assertRawHistory(t, stream, "start-slot", "task", 0, 0)
	})

	t.Run("conflicts", exerciseHistoryConflicts)
	t.Run("retention", exerciseHistoryRetention)
	t.Run("scan_failure_is_unknown", exerciseHistoryScanFailure)
	t.Run("concurrent_subject_append", exerciseHistoryConcurrentAppend)
	t.Run("dedupe_receipt_is_not_exact", func(t *testing.T) {
		js, stream := historyFixture(t, "dedupe", nil)
		record := historyRecord(t, "dedupe", "task", "complete", nil)
		foreign := historyRecord(t, "dedupe", "other", "complete", nil)
		if _, err := js.Publish(t.Context(), proto.SubjAuditTransfer("dedupe"), foreign.Bytes, jetstream.WithMsgID(record.ReqID)); err != nil {
			t.Fatal(err)
		}
		receipt, err := newPublisher(t, js, "dedupe").Confirm(record, historyDeadline(t))
		if !errors.Is(err, ErrUnknown) || receipt.Sequence != 0 {
			t.Fatalf("foreign dedupe ACK certified absent record: %+v %v", receipt, err)
		}
		assertRawHistory(t, stream, "dedupe", "task", 0, 0)
	})
	t.Run("invalid_canonical", func(t *testing.T) {
		js, stream := historyFixture(t, "canonical", nil)
		p := newPublisher(t, js, "canonical")
		for field, value := range map[string]any{
			"terminal_id": "wrong", "agent_instance": nil, "ts": "0001-01-01T00:00:00Z",
			"v": -1, "node": "other", "actor_fp": "other", "transfer_id": "other",
			"verb": "other", "tier": "other", "object_version": nil, "entry_nonce": nil, "intent_digest": nil,
		} {
			record := historyRecord(t, "canonical", "task", "complete", nil)
			record.Bytes = changedHistoryField(field, value)(t, record)[0]
			// A self-consistent content hash cannot legitimize the wrong terminal
			// slot, missing continuation, or contradictory scope inside the bytes.
			record.Digest = testHistoryDigest(record.Bytes)
			record.ReqID = testHistoryDigest(append([]byte("xferaudit-record:"), record.Bytes...))
			if receipt, err := p.Confirm(record, historyDeadline(t)); !errors.Is(err, ErrConflict) || receipt.Sequence != 0 {
				t.Fatalf("invalid canonical %s acknowledged: %+v %v", field, receipt, err)
			}
		}
		assertRawHistory(t, stream, "canonical", "task", 0, 0)
	})
	t.Run("invalid_request", func(t *testing.T) {
		js, stream := historyFixture(t, "invalid", nil)
		p := newPublisher(t, js, "invalid")
		for _, mutate := range []func(*xfercontract.CanonicalRecord){
			func(r *xfercontract.CanonicalRecord) { r.ReqID = "other" },
			func(r *xfercontract.CanonicalRecord) { r.Digest = "other" },
			func(r *xfercontract.CanonicalRecord) { r.Identity.EntryNonce = "other" },
			func(r *xfercontract.CanonicalRecord) { r.Identity.SID = "other" },
			func(r *xfercontract.CanonicalRecord) { r.Kind = "failed" },
			func(r *xfercontract.CanonicalRecord) { r.Bytes = []byte("{") },
		} {
			record := historyRecord(t, "invalid", "task", "complete", nil)
			mutate(&record)
			receipt, err := p.Confirm(record, historyDeadline(t))
			if !errors.Is(err, ErrConflict) || receipt.Sequence != 0 {
				t.Fatalf("invalid input acknowledged: %+v %v", receipt, err)
			}
		}
		if _, err := p.Lookup(xfercontract.Key{SID: "other", TransferID: "task"}, historyDeadline(t)); !errors.Is(err, ErrConflict) {
			t.Fatalf("foreign lookup accepted: %v", err)
		}
		for _, sid := range []string{"", "wild.*", "line\nbreak", "space token"} {
			if _, err := New(Config{JS: js, SID: sid}); err == nil {
				t.Fatalf("invalid session %q accepted", sid)
			}
		}
		if _, err := New(Config{SID: "valid"}); err == nil {
			t.Fatal("nil JetStream accepted")
		}
		assertRawHistory(t, stream, "invalid", "task", 0, 0)
	})
}

func exerciseLateHistoryPublish(t *testing.T) {
	js, stream := historyFixture(t, "late", nil)
	info, err := stream.Info(t.Context())
	if err != nil || info.Config.Duplicates != 120*time.Second {
		t.Fatalf("test must cross the real default 120-second window: %+v %v", info, err)
	}
	subject := proto.SubjAuditTransfer("late")
	relay := startHistoryRelay(t, js.Conn().ConnectedUrl(), subject)
	record := historyRecord(t, "late", "task", "complete", nil)
	results := make(chan error, 2)
	for range 2 {
		p := newPublisher(t, connectHistory(t, "nats://"+relay.listener.Addr().String()), "late")
		d, err := controlbudget.After(2 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			receipt, err := p.Confirm(record, d)
			if receipt.Sequence != 0 {
				results <- fmt.Errorf("lost ACK returned a receipt: %+v", receipt)
				return
			}
			results <- err
		}()
	}
	original, retry := receiveHistoryFrame(t, relay.frames), receiveHistoryFrame(t, relay.frames)
	for _, frame := range []*heldHistoryFrame{original, retry} {
		if !bytes.Contains(frame.data, []byte("Nats-Expected-Last-Subject-Sequence: 0\r\n")) {
			t.Errorf("sent attempt did not freeze H=0: %q", frame.data)
		}
	}
	if err := original.release(); err != nil {
		t.Fatal(err)
	}
	select {
	case reply := <-original.reply:
		if !bytes.Contains(reply, []byte(`"seq":1`)) || bytes.Contains(reply, []byte(`"error"`)) {
			t.Fatalf("original publish was not accepted: %q", reply)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not capture the real ACK")
	}
	accepted := time.Now()
	for range 2 {
		select {
		case err := <-results:
			if !errors.Is(err, ErrUnknown) {
				t.Fatalf("sent-unknown result lost responsibility: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("history operation exceeded its original deadline")
		}
	}
	// Reconciliation already sees the exact row, but the retry packet still
	// exists on the wire. Only its original CAS can stop it after MsgID expiry.
	p := newPublisher(t, js, "late")
	if _, err := p.Confirm(record, historyDeadline(t)); err != nil {
		t.Fatal(err)
	}
	// This is a product dedupe-window test, not a readiness sleep. Real control
	// audit traffic advances the STREAM sequence at 10/s throughout the wait.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	wait := time.NewTimer(time.Until(accepted.Add(125 * time.Second)))
	defer wait.Stop()
	calls := 0
waitWindow:
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			_, err := js.Publish(ctx, "unrelated.call", []byte("control audit"))
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			calls++
		case <-wait.C:
			break waitWindow
		case <-t.Context().Done():
			t.Fatal("test canceled before crossing dedupe window")
		}
	}
	if time.Since(accepted) < 125*time.Second || calls < 1000 {
		t.Fatalf("dedupe/control-traffic premise missing: elapsed=%v calls=%d", time.Since(accepted), calls)
	}
	// The publisher's request subscription has expired. An independent observer
	// subscribes to its ORIGINAL reply subject so the real rejection is still
	// witnessed; no synthetic ACK or cross-connection timing barrier is needed.
	fields := strings.Fields(string(retry.data[:bytes.Index(retry.data, []byte("\r\n"))]))
	replyObserver, err := js.Conn().SubscribeSync(fields[2])
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replyObserver.Unsubscribe() }()
	if err := js.Conn().FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if err := retry.release(); err != nil {
		t.Fatal(err)
	}
	reply, err := replyObserver.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(reply.Data, []byte(`"err_code":10071`)) {
		t.Errorf("late retry escaped subject CAS: %q", reply.Data)
	}
	assertRawHistory(t, stream, "late", "task", 0, 1)
	if _, err := p.Confirm(record, historyDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

func exerciseHistoryConflicts(t *testing.T) {
	cases := []struct {
		name string
		rows func(*testing.T, xfercontract.CanonicalRecord) [][]byte
	}{
		{"opposite", func(t *testing.T, _ xfercontract.CanonicalRecord) [][]byte {
			return [][]byte{historyRecord(t, "conflict", "task", "failed", nil).Bytes}
		}},
		{"bytes", changedHistoryField("bytes", float64(99))},
		{"actor", changedHistoryField("actor_nkey", "different-actor")},
		{"identity", changedHistoryField("actor_fp", "different-actor")},
		{"scope", changedHistoryField("session", "different-session")},
		{"timestamp", changedHistoryField("ts", "2026-10-12T01:02:04Z")},
		{"terminal_id", changedHistoryField("terminal_id", "wrong")},
		{"duplicate", func(_ *testing.T, r xfercontract.CanonicalRecord) [][]byte { return [][]byte{r.Bytes, r.Bytes} }},
		{"late_opposite", func(t *testing.T, r xfercontract.CanonicalRecord) [][]byte {
			return [][]byte{r.Bytes, historyRecord(t, "conflict", "task", "failed", nil).Bytes}
		}},
		{"malformed_after_exact", func(_ *testing.T, r xfercontract.CanonicalRecord) [][]byte {
			return [][]byte{r.Bytes, []byte(`{"transfer_id":"task",`)}
		}},
		{"duplicate_start", func(t *testing.T, r xfercontract.CanonicalRecord) [][]byte {
			s := historyRecord(t, "conflict", "task", "start", nil)
			return [][]byte{s.Bytes, r.Bytes, s.Bytes}
		}},
		{"partial_identity", changedHistoryField("agent_instance", nil)},
		{"legacy", func(t *testing.T, r xfercontract.CanonicalRecord) [][]byte {
			var audit schema.AuditTransfer
			if err := json.Unmarshal(r.Bytes, &audit); err != nil {
				t.Fatal(err)
			}
			audit.AgentInstance, audit.EntryNonce, audit.IntentDigest, audit.ObjectVersion, audit.TerminalID = "", "", "", "", ""
			body, err := json.Marshal(audit)
			if err != nil {
				t.Fatal(err)
			}
			return [][]byte{body}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			js, stream := historyFixture(t, "conflict", nil)
			record := historyRecord(t, "conflict", "task", "complete", nil)
			rows := tc.rows(t, record)
			for _, body := range rows {
				if _, err := js.Publish(t.Context(), proto.SubjAuditTransfer("conflict"), body); err != nil {
					t.Fatal(err)
				}
			}
			receipt, err := newPublisher(t, js, "conflict").Confirm(record, historyDeadline(t))
			if err == nil || receipt.Sequence != 0 || (!errors.Is(err, ErrConflict) && !errors.Is(err, ErrUnknown)) {
				t.Fatalf("conflicting history acknowledged: %+v %v", receipt, err)
			}
			info, err := stream.Info(t.Context())
			if err != nil || info.State.Msgs != uint64(len(rows)) {
				t.Fatalf("conflict changed physical history: %+v %v", info, err)
			}
		})
	}
}

func changedHistoryField(field string, value any) func(*testing.T, xfercontract.CanonicalRecord) [][]byte {
	return func(t *testing.T, record xfercontract.CanonicalRecord) [][]byte {
		t.Helper()
		var raw map[string]any
		if err := json.Unmarshal(record.Bytes, &raw); err != nil {
			t.Fatal(err)
		}
		raw[field] = value
		body, err := json.Marshal(raw)
		if err != nil {
			t.Fatal(err)
		}
		return [][]byte{body}
	}
}

func exerciseHistoryRetention(t *testing.T) {
	for name, change := range map[string]func(*jetstream.StreamConfig){
		"ttl":       func(c *jetstream.StreamConfig) { c.MaxAge = time.Hour },
		"eviction":  func(c *jetstream.StreamConfig) { c.Discard = jetstream.DiscardOld },
		"count":     func(c *jetstream.StreamConfig) { c.MaxMsgs = 100 },
		"subject":   func(c *jetstream.StreamConfig) { c.MaxMsgsPerSubject = 100 },
		"interest":  func(c *jetstream.StreamConfig) { c.Retention = jetstream.InterestPolicy },
		"workqueue": func(c *jetstream.StreamConfig) { c.Retention = jetstream.WorkQueuePolicy },
		"rollup":    func(c *jetstream.StreamConfig) { c.AllowRollup = true },
		"msg_ttl":   func(c *jetstream.StreamConfig) { c.AllowMsgTTL = true },
	} {
		t.Run(name, func(t *testing.T) {
			js, stream := historyFixture(t, "retention", change)
			receipt, err := newPublisher(t, js, "retention").Confirm(historyRecord(t, "retention", "task", "complete", nil), historyDeadline(t))
			if !errors.Is(err, ErrStorageHorizon) || receipt.Sequence != 0 {
				t.Fatalf("unsafe history accepted: %+v %v", receipt, err)
			}
			info, err := stream.Info(t.Context())
			if err != nil || info.State.Msgs != 0 {
				t.Fatalf("retention refusal published: %+v %v", info, err)
			}
		})
	}
	t.Run("full_keeps_evidence", func(t *testing.T) {
		js, stream := historyFixture(t, "full", func(c *jetstream.StreamConfig) { c.MaxBytes = 1000 })
		p := newPublisher(t, js, "full")
		first := historyRecord(t, "full", "first", "complete", nil)
		if _, err := p.Confirm(first, historyDeadline(t)); err != nil {
			t.Fatal(err)
		}
		second := historyRecord(t, "full", "second", "complete", nil)
		if _, err := p.Confirm(second, historyDeadline(t)); !errors.Is(err, ErrUnknown) {
			t.Fatalf("capacity exhaustion must stay pending: %v", err)
		}
		assertRawHistory(t, stream, "full", "first", 0, 1)
	})
}

func exerciseHistoryScanFailure(t *testing.T) {
	js, stream := historyFixture(t, "scan", nil)
	record := historyRecord(t, "scan", "task", "complete", nil)
	if _, err := js.Publish(t.Context(), proto.SubjAuditTransfer("scan"), record.Bytes); err != nil {
		t.Fatal(err)
	}
	for _, timeout := range []bool{false, true} {
		binding := historyBinding{JetStream: js, stream: scanFailure{Stream: stream,
			read: func(ctx context.Context, _ uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
				if timeout {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return nil, jetstream.ErrMsgNotFound
			},
		}}
		d, err := controlbudget.After(80 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		set, err := newPublisher(t, binding, "scan").Lookup(xfercontract.Key{SID: "scan", TransferID: "task"}, d)
		if !errors.Is(err, ErrUnknown) || set.Complete {
			t.Fatalf("incomplete scan became absence: %+v %v", set, err)
		}
		if time.Since(started) > time.Second {
			t.Fatal("history lookup renewed the caller's deadline")
		}
	}
	if err := js.DeleteStream(t.Context(), "custom_history"); err != nil {
		t.Fatal(err)
	}
	if _, err := newPublisher(t, js, "scan").Confirm(record, historyDeadline(t)); !errors.Is(err, ErrUnknown) {
		t.Fatalf("missing stream acknowledged: %v", err)
	}
}

func exerciseHistoryConcurrentAppend(t *testing.T) {
	for _, subjectRace := range []bool{false, true} {
		t.Run(fmt.Sprint(subjectRace), func(t *testing.T) {
			js, stream := historyFixture(t, "race", nil)
			calls := 0
			binding := historyBinding{JetStream: js, publish: func(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
				calls++
				subject, body := "unrelated.call", []byte("not transfer JSON")
				if subjectRace && calls == 1 {
					subject, body = proto.SubjAuditTransfer("race"), historyRecord(t, "race", "other", "complete", nil).Bytes
				}
				if _, err := js.Publish(ctx, subject, body); err != nil {
					return nil, err
				}
				return js.PublishMsg(ctx, msg, opts...)
			}}
			if _, err := newPublisher(t, binding, "race").Confirm(historyRecord(t, "race", "task", "complete", nil), historyDeadline(t)); err != nil {
				t.Fatal(err)
			}
			wantCalls := 1
			if subjectRace {
				wantCalls = 2
			}
			if calls != wantCalls {
				t.Fatalf("subject CAS retry count=%d, want %d", calls, wantCalls)
			}
			assertRawHistory(t, stream, "race", "task", 0, 1)
		})
	}
}

func historyRecord(t *testing.T, sid, id, kind string, change func(*schema.AuditTransfer)) xfercontract.CanonicalRecord {
	t.Helper()
	identity := xfercontract.Identity{SID: sid, NID: "node", ActorFP: "actor", AgentInstance: "instance",
		TransferID: id, EntryNonce: "entry", IntentDigest: "intent", ObjectVersion: "object"}
	audit := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: kind, Verb: "push",
		Ts: time.Date(2026, 10, 12, 1, 2, 3, 0, time.UTC), Session: sid, Node: identity.NID,
		ActorFp: identity.ActorFP, TransferID: id, Bytes: 123, Tier: "b"}
	if change != nil {
		change(&audit)
	}
	audit.AgentInstance, audit.EntryNonce = identity.AgentInstance, identity.EntryNonce
	audit.IntentDigest, audit.ObjectVersion = identity.IntentDigest, identity.ObjectVersion
	if kind != "start" {
		audit.TerminalID = schema.TransferTerminalID(sid, id, identity.EntryNonce)
	}
	// The same encoder supplies ledger/Raft/history bytes. A private test-only
	// envelope would miss a decoder mismatch at the real publication boundary.
	// origin: hot-update S7 plan §5.2 / T23 / T25
	record, err := schema.CanonicalTransferAudit(audit)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func testHistoryDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// Independent physical-row oracle: it walks the actual stream, decodes raw
// transfer rows, and does not call Lookup or the production record decoder.
func assertRawHistory(t *testing.T, stream jetstream.Stream, sid, id string, starts, terminals int) {
	t.Helper()
	info, err := stream.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var gotStarts, gotTerminals int
	for seq := info.State.FirstSeq; seq != 0 && seq <= info.State.LastSeq; seq++ {
		msg, err := stream.GetMsg(t.Context(), seq)
		if err != nil {
			t.Fatal(err)
		}
		if msg.Subject != proto.SubjAuditTransfer(sid) {
			continue
		}
		var row schema.AuditTransfer
		if err := json.Unmarshal(msg.Data, &row); err != nil {
			t.Fatal(err)
		}
		if row.TransferID == id {
			switch row.Kind {
			case "start":
				gotStarts++
			case "complete", "failed":
				gotTerminals++
			default:
				t.Fatalf("unknown physical kind %q", row.Kind)
			}
		}
	}
	if gotStarts != starts || gotTerminals != terminals {
		t.Fatalf("physical history count start=%d terminal=%d; want %d/%d", gotStarts, gotTerminals, starts, terminals)
	}
	if strings.Contains(info.Config.Name, "history-") {
		t.Fatal("fixture lost nonstandard history-name premise")
	}
}
