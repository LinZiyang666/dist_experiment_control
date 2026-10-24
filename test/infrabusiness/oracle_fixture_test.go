//go:build linux && infrabusiness_integration

package infrabusiness_test

import (
	"encoding/hex"
	"fmt"
	i "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infratransfer"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"time"
)

// The finite control trace tests only the reader's predicates. It is never
// supplied to runLive or described as a product continuity receipt.
func oracleReceipt() businessReceipt {
	schema := infratransfer.ObservationSchema()
	r := businessReceipt{Version: businessReceiptVersion, ObserverBootID: "observer-boot", SourceSHA256: digest([]byte("source")), SchemaSHA256: hex.EncodeToString(schema[:]), DependencyOrder: []string{"agent", "broker", "nats", "caddy"},
		Executed: []string{"nats", "caddy"}, Hosts: []string{"host"}, WindowNS: int64(6 * time.Second)}
	for role, count := range map[string]int{"ssh": 1, "tcp": 4, "ss-alice": 1, "ss-bob": 1, "pty": 1, "exec": 1, "transfer": 4} {
		for n := 0; n < count; n++ {
			id := businessIdentity{ID: fmt.Sprintf("%s-%d", role, n), Role: role, Nonce: "original", PID: 1, StartTicks: 1,
				CTLPID: 2, CTLStartTicks: 2, Device: 1, Inode: 1, Cookie: 1, TaskID: "task", TransferID: fmt.Sprint(n), NUID: fmt.Sprint(n)}
			if role == "transfer" {
				id.Size, id.RateLimitBPS, id.StartedNS = []int64{66549440, 1 << 30}[n/2], 1<<20, 1
				id.Direction, id.Session, id.Bucket, id.SHA256 = []string{"push", "pull"}[n%2], "session"+id.TransferID, "bucket"+id.TransferID, digest([]byte("source-"+id.TransferID))
				budget, err := proto.TransferBudget("b", id.Size, id.RateLimitBPS)
				if err != nil {
					panic(err)
				}
				id.DeadlineNS = id.StartedNS + int64(budget)
				id.AuditIdentity = x.Identity{SID: id.Session, NID: "node", ActorFP: "actor", AgentInstance: "instance", TransferID: id.TransferID, EntryNonce: id.Nonce, IntentDigest: digest([]byte("intent-" + id.TransferID)), ObjectVersion: id.NUID}
			}
			r.Required = append(r.Required, id)
			if role != "transfer" {
				s := streamReceipt{Original: id, Current: id, Accepts: 1, CarrierTransition: "unchanged", UpBytes: 12, DownBytes: 12,
					UpSent: digest([]byte("up")), UpRead: digest([]byte("up")), DownSent: digest([]byte("down")), DownRead: digest([]byte("down")), FINUp: true, FINDown: true}
				s.Revoked = role == "ss-alice"
				if role == "exec" {
					s.NativeRC = 23
				}
				if role == "pty" {
					s.Controls = []string{"input", "output", "resize", "signal", "keepalive", "ps", "kill-victim"}
				}
				r.Streams = append(r.Streams, s)
			}
		}
	}
	for n := 0; n < 6; n++ {
		offset := byte(1)
		if n >= 3 {
			offset = 100
		}
		before, after := oracleProcess(offset+byte(n%3)), oracleProcess(offset+byte(n%3+1))
		before.ExeSHA256, after.ExeSHA256 = i.Digest{1}, i.Digest{2}
		if n%3 == 1 {
			before.ExeSHA256, after.ExeSHA256 = i.Digest{2}, i.Digest{1}
		}
		r.Hops = append(r.Hops, hopReceipt{Step: uint32(n + 1), Tx: i.ID{byte(n + 1)}, BootID: "boot", Host: "host", Component: r.Executed[n/3], Action: []string{"upgrade", "rollback", "upgrade"}[n%3],
			StartNS: int64(n) * int64(time.Second), CutNS: int64(n)*int64(time.Second) + int64(200*time.Millisecond), EndNS: int64(n+1) * int64(time.Second),
			Before: before, Current: after, OldExited: true, TemporaryExited: true, LiveBusiness: 13, Evidence: digest([]byte("independent census"))})
	}
	r.Hops[0].StartNS = 100
	for n := 0; n < 60; n++ {
		start := int64(n) * int64(100*time.Millisecond)
		s := slotReceipt{Index: n, ScheduledNS: start, DispatchedNS: start, TerminalNS: start + int64(time.Millisecond),
			Kind: []string{"node-ls", "ps", "read", "write", "short-exec"}[n%5], ID: fmt.Sprint(n), RequestDigest: digest([]byte(fmt.Sprint(n))),
			Attempts: []attemptReceipt{{StartNS: start, EndNS: start + int64(time.Millisecond), Code: "ok", Sent: true}}}
		if n%5 >= 3 {
			s.Effects, s.EffectDigest = 1, digest([]byte("effect"))
		}
		r.Slots = append(r.Slots, s)
	}
	for n, pair := range [][2]string{{"broker", "auth"}, {"broker", "control"}, {"agent", "control"}, {"agent", "data"}, {"ctl", "control"}, {"ctl", "data"}, {"ctl", "consumer"}} {
		id := ncIdentity{Participant: i.ID{byte(n + 1)}, Owner: pair[0], Role: pair[1], HomeHost: "host"}
		r.RequiredNC = append(r.RequiredNC, id)

	}
	populateAttachmentControls(&r)
	return r
}

func populateAttachmentControls(r *businessReceipt) {
	for n, id := range r.RequiredNC {
		old := routeAttachment(1)
		old.Participant = id.Participant
		old.CID += uint64(n * 100)
		old.Ingress = r.Hops[3].Before
		oldRuntime := r.Hops[0].Before
		for k := range r.Hops {
			h := &r.Hops[k]
			next, home := routeAttachment(byte(k*2+2)), routeAttachment(byte(k*2+3))
			next.Participant, home.Participant = id.Participant, id.Participant
			nextRuntime, homeRuntime := oldRuntime, oldRuntime
			next.Ingress, home.Ingress = old.Ingress, old.Ingress
			if h.Component == "nats" {
				nextRuntime, homeRuntime = h.Current, h.Current
			} else {
				next.Ingress, home.Ingress = oracleProcess(byte(200+k)), h.Current
			}
			for _, a := range []*i.Attachment{&next, &home} {
				a.CID += uint64(n * 100)
				a.Raw.Cookie += uint64(n * 100)
				a.Raw.Inode += uint64(n * 100)
			}
			row := attachmentReceipt{Step: h.Step, evidenceScope: scopeFor(*h, "round-trip"), Identity: id, Old: old, New: next, Returned: home,
				OldRuntime: oldRuntime, NewRuntime: nextRuntime, ReturnedRuntime: homeRuntime,
				PrepareACK: true, BindACK: true, ReturnACK: true, Complete: true, CensusDigest: digest([]byte("independent full NC registry"))}
			h.RequiredNC = append(h.RequiredNC, cutNC{Identity: id, Before: ncTarget{old.Ingress, oldRuntime}, Moving: ncTarget{next.Ingress, nextRuntime}, Returning: ncTarget{home.Ingress, homeRuntime}})
			if id.Owner == "broker" && id.Role == "auth" {
				for d, direction := range []string{"outbound", "return"} {
					target, runtime := next, nextRuntime
					name := "hot"
					if d == 1 {
						target, runtime, name = home, homeRuntime, "ordinary-reconnect"
					}
					if k == 0 && d == 0 {
						name = "cold"
					}
					h.RequiredAuth = append(h.RequiredAuth, authRequirement{Participant: id.Participant, Case: name, Direction: direction, ServerID: target.ServerID,
						Runtime: runtime, Generation: target.Generation, AuthCID: target.CID, BusinessCID: target.CID + 100})
				}
			}
			r.Attachments = append(r.Attachments, row)
			old, oldRuntime = home, homeRuntime
		}
	}
}

func populateAuthControls(r *businessReceipt) {
	for _, h := range r.Hops {
		for _, target := range h.RequiredAuth {
			start := h.CutNS
			row := authReceipt{Step: h.Step, evidenceScope: scopeFor(h, target.Direction), Scenario: "normal", Case: target.Case,
				Participant: target.Participant, Runtime: target.Runtime, Evidence: digest([]byte(authKey(h.Step, target))),
				Admitted: true, LocalQueue: true, Queue: "tether-authcallout", ServerID: target.ServerID,
				Generation: target.Generation, AuthCID: target.AuthCID, BusinessCID: target.BusinessCID,
				AuthSUBNS: start + 1, AuthPONGNS: start + 2, OrdinaryAuthNS: start + 3, PrefixReadNS: start + 4, RepairNS: start + 5,
				BusinessSUBNS: start + 6, MemberRPCNS: start + 7, AgentRPCNS: start + 8,
				MemberRequest: digest([]byte("member")), AgentRequest: digest([]byte("agent")), PrefixHash: digest([]byte("prefix"))}
			r.Auth = append(r.Auth, row)
		}
	}
	for _, name := range []string{"missing-normal-queue", "js503", "ack-prefix-loss", "replica-insufficient"} {
		h := r.Hops[0]
		r.Auth = append(r.Auth, authReceipt{Step: h.Step, evidenceScope: scopeFor(h, "return"), Scenario: name, Case: name, Evidence: digest([]byte(name))})
	}
	for _, kind := range []string{"publish", "request", "pty-input", "pty-resize", "pty-signal", "pty-keepalive"} {
		r.Faults = append(r.Faults, faultReceipt{Kind: kind, RequestDigest: digest([]byte(kind)), SentEvidence: digest([]byte("sent")), EffectEvidence: digest([]byte("actual-effect")), ReplyCutEvidence: digest([]byte("cut-reply")), Effects: 1,
			SentRequestDigest: digest([]byte(kind)), EffectRequestDigest: digest([]byte(kind)), ReplyRequestDigest: digest([]byte(kind))})
	}
}

func populateTransferControls(r *businessReceipt) {
	var originals []businessIdentity
	for _, id := range r.Required {
		if id.Role == "transfer" {
			originals = append(originals, id)
		}
	}
	r.TransferSealNS, r.TransferSealSHA256 = 2, contentDigest(originals)
	offsets := make(map[string]int64)
	for _, h := range r.Hops {
		for p, phase := range []string{"before", "moving", "returning"} {
			for _, id := range originals {
				previous := offsets[id.TransferID]
				durable := previous + 32768
				// Adjacent hops share an observation at their boundary; it cannot
				// manufacture another durable write at the exact same timestamp.
				if p == 0 && h.Step > 1 {
					durable = previous
				}
				prefix := digest([]byte(fmt.Sprint(id.TransferID, "/", durable)))
				at := []int64{h.StartNS, h.CutNS, h.EndNS}[p]
				row := transferReceipt{Step: h.Step, evidenceScope: scopeFor(h, phase), Phase: phase, Direction: id.Direction, TransferID: id.TransferID, NUID: id.NUID,
					CTLPID: id.CTLPID, CTLStartTicks: id.CTLStartTicks, Bucket: id.Bucket, Session: id.Session,
					Size: id.Size, Durable: durable, PreviousDurable: previous, RateLimitBPS: id.RateLimitBPS, OriginalRate: id.RateLimitBPS,
					StartedNS: id.StartedNS, ByteDeadlineNS: id.DeadlineNS, OriginalDeadlineNS: id.DeadlineNS, Synced: true, StartRecords: 1,
					PrefixSHA256: prefix, RawPrefixSHA256: prefix, Consumer: "new", RetiredConsumer: "old",
					ConsumerDeleted: true, PONGConfirmed: true, ConsumerEvidence: digest([]byte("unsub-pong-delete")), SchemaSHA256: r.SchemaSHA256, SourceSHA256: r.SourceSHA256,
					InvocationEvidence: contentDigest(id), Coverage: []string{"writer-pending-ack", "reader-partial-write", "reader-synced", "commit-rename", "terminal-courier"},
					Checkpoints: []durableCheckpoint{{BootID: r.ObserverBootID, AtNS: at, Durable: durable, PrefixSHA256: prefix, Synced: true}}}
				row.ReceiptEvidence = transferDigest(row)
				r.Transfers = append(r.Transfers, row)
				offsets[id.TransferID] = durable
			}
		}
	}
	for _, id := range originals {
		r.TransferFinals = append(r.TransferFinals, finalControl(id, r.ObserverBootID, r.WindowNS+1, []string{"old", "new"}))
	}
	for n, phase := range []string{"writer-pending-ack", "reader-partial-write", "reader-synced", "commit-rename", "terminal-courier"} {
		id := originals[n%len(originals)]
		if n >= 3 {
			id.ID, id.TransferID, id.NUID, id.Nonce = phase, phase, phase, phase
			id.Session, id.Bucket = phase, phase
			id.AuditIdentity.SID, id.AuditIdentity.TransferID, id.AuditIdentity.ObjectVersion, id.AuditIdentity.EntryNonce = id.Session, id.TransferID, id.NUID, id.Nonce
		}
		r.RequiredPhases = append(r.RequiredPhases, phaseRequirement{Phase: phase, Original: id, Consumer: "phase-" + id.TransferID})
		h := r.Hops[0]
		lane := transferPhaseReceipt{Step: h.Step, evidenceScope: scopeFor(h, phase), Phase: phase, Original: id,
			Before: transferPhaseState{BootID: r.ObserverBootID, AtNS: h.CutNS - 1, Written: 32768, Synced: 32768},
			After:  transferPhaseState{BootID: r.ObserverBootID, AtNS: h.CutNS + 1, Written: 65536, Synced: 65536},
			Final:  finalControl(id, r.ObserverBootID, h.EndNS+1, []string{"phase-" + id.TransferID})}
		switch phase {
		case "writer-pending-ack":
			lane.Before.Published, lane.Before.ACKed, lane.After.Published, lane.After.ACKed = 2, 1, 2, 2
		case "reader-partial-write":
			lane.Before.Synced = 1
		case "commit-rename":
			lane.Before.Written, lane.Before.Synced, lane.After.Written, lane.After.Synced = id.Size, id.Size, id.Size, id.Size
			lane.After.Renamed, lane.After.ParentSynced = true, true
		case "terminal-courier":
			lane.Before.Terminal, lane.After.Terminal, lane.After.Delivered = lane.Final.History.Terminal[0], lane.Final.History.Terminal[0], true
		}
		if n < 3 {
			lane.Final = transferFinalReceipt{}
		}
		lane.ReceiptEvidence = phaseDigest(lane)
		r.TransferPhases = append(r.TransferPhases, lane)
	}
}

func finalControl(id businessIdentity, boot string, at int64, consumers []string) transferFinalReceipt {
	row := transferFinalReceipt{Checkpoints: []durableCheckpoint{{BootID: boot, AtNS: at, Durable: id.Size, PrefixSHA256: id.SHA256, Synced: true}}, Original: id, BootID: boot, CompletedNS: at, Bytes: id.Size, SHA256: id.SHA256, History: x.HistorySet{Complete: true, ScannedThrough: 2}}
	for n, kind := range []string{"start", "complete"} {
		audit := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: kind, Verb: id.Direction, Tier: "b", Ts: time.Unix(1+int64(n), 0).UTC(),
			Session: id.Session, Node: id.AuditIdentity.NID, ActorFp: id.AuditIdentity.ActorFP, AgentInstance: id.AuditIdentity.AgentInstance,
			TransferID: id.TransferID, EntryNonce: id.Nonce, IntentDigest: id.AuditIdentity.IntentDigest, ObjectVersion: id.NUID,
			Size: id.Size, SHA256: id.SHA256, Bucket: id.Bucket}
		if kind == "complete" {
			audit.TerminalID, audit.Bytes = schema.TransferTerminalID(id.Session, id.TransferID, id.Nonce), id.Size
		}
		canonical, err := schema.CanonicalTransferAudit(audit)
		if err != nil {
			panic(err)
		}
		if kind == "start" {
			row.History.Start = append(row.History.Start, canonical)
		} else {
			row.History.Terminal = append(row.History.Terminal, canonical)
		}
		row.History.Receipts = append(row.History.Receipts, x.HistoryReceipt{Stream: jsstream.HistoryStreamName(id.Session), Sequence: uint64(n + 1), RecordDigest: canonical.Digest})
	}
	for _, consumer := range consumers {
		row.Consumers = append(row.Consumers, consumerRetirement{Consumer: consumer, Stream: "OBJ_" + id.Bucket, UnsubNS: at - 3, PONGNS: at - 2, DeleteNS: at - 1, Absent: true})
	}
	row.ReceiptEvidence = finalDigest(row)
	return row
}
