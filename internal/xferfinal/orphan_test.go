package xferfinal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The legacy policy must actually reclaim an unmapped SDK object, but cannot
// borrow that policy for v1, changed storage, live owners or incomplete census.
// origin: hot-update S7 plan §5.3 / T29 / INTEG-R orphan retirement regression
func TestTransferLegacyOrphanRetirementRequiresExactStorageAndCensus(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseLegacyOrphanRetirement(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "legacy orphan finalizer", g)
	testharness.AssertNoFDLeak(t, "legacy orphan finalizer", fd, 0)
}

type orphanOwners struct {
	local, remote x.LedgerCensus
	err           error
	calls         int
	afterFence    bool
}

func (o *orphanOwners) Census(origin x.Origin, _ x.Deadline) (x.LedgerCensus, error) {
	o.calls++
	if o.afterFence && o.calls > 1 {
		return x.LedgerCensus{}, errors.New("owner census changed after fence")
	}
	if origin != (x.Origin{}) {
		return o.remote, o.err
	}
	return o.local, o.err
}

type orphanLedger struct {
	x.LedgerPort
	fail bool
}

func (l orphanLedger) Scan(d x.Deadline) (x.LedgerCensus, error) {
	if l.fail {
		return x.LedgerCensus{}, errors.New("primary directory unreadable")
	}
	return l.LedgerPort.Scan(d)
}

// Faults wrap only transport boundaries. All accepted metadata, fences and
// purges still execute on the embedded server and are independently inspected.
type orphanJS struct {
	jetstream.JetStream
	purgeFail, purgeNoop, publishFail, wrongACK, foreignReadback bool
	published                                                    bool
}

type orphanStream struct {
	jetstream.Stream
	js *orphanJS
}

func (j *orphanJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	s, err := j.JetStream.Stream(ctx, name)
	if err != nil {
		return nil, err
	}
	return orphanStream{Stream: s, js: j}, nil
}
func (j *orphanJS) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ack, err := j.JetStream.PublishMsg(ctx, msg, opts...)
	j.published = true
	if j.publishFail {
		return nil, errors.New("lost real tombstone acknowledgement")
	}
	if j.wrongACK && ack != nil {
		copy := *ack
		copy.Stream = "foreign"
		return &copy, nil
	}
	return ack, err
}
func (s orphanStream) GetLastMsgForSubject(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	raw, err := s.Stream.GetLastMsgForSubject(ctx, subject)
	if err == nil && s.js.published && s.js.foreignReadback && strings.Contains(subject, ".M.") {
		copy := *raw
		copy.Data = []byte(`{"name":"foreign"}`)
		return &copy, nil
	}
	return raw, err
}
func (s orphanStream) Purge(ctx context.Context, opts ...jetstream.StreamPurgeOpt) error {
	if s.js.purgeFail {
		return errors.New("purge transport unavailable")
	}
	if s.js.purgeNoop {
		return nil // Lost physical effect despite an apparently successful API reply.
	}
	return s.Stream.Purge(ctx, opts...)
}

func exerciseLegacyOrphanRetirement(t *testing.T) {
	nc, err := nats.Connect(testharness.StartJSNATS(t), nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"none", "sequence", "digest", "incarnation", "version", "local_incomplete", "local_unready", "local_error", "local_code", "malformed_protection", "conflicting_protection", "malformed_row", "live_name", "live_version", "retained_row", "remote_unavailable", "remote_live", "ledger_unreadable", "young", "untrusted_mtime", "clock_backwards", "v1_first_chunk", "partial_body", "consumer", "stopped", "missing_binding", "publish_unknown", "wrong_ack", "foreign_readback", "post_fence_census", "purge_retry", "purge_noop"} {
		t.Run(fault, func(t *testing.T) {
			bucket := "xfer-" + fault
			store, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: bucket, Storage: jetstream.FileStorage})
			if err != nil {
				t.Fatal(err)
			}
			object, err := store.Put(t.Context(), jetstream.ObjectMeta{Name: "original", Opts: &jetstream.ObjectMetaOptions{ChunkSize: 3}}, bytes.NewBufferString("abcdef"))
			if err != nil {
				t.Fatal(err)
			}
			stream, err := js.Stream(t.Context(), "OBJ_"+bucket)
			if err != nil {
				t.Fatal(err)
			}
			candidate := orphanCandidate(t, stream, bucket, object.NUID)
			chunkSubject := "$O." + bucket + ".C." + object.NUID
			owners := &orphanOwners{local: x.LedgerCensus{Ready: true, Complete: true}}
			ledger, err := xferledger.New(x.LedgerConfig{Base: x.Base{Clock: fixtureClock}, Root: t.TempDir(), FallbackRoot: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().Add(4 * time.Hour)
			f := newFinalFixture(t, "unused")
			cfg := f.config()
			backend := &orphanJS{JetStream: js}
			cfg.UTC = func() time.Time { return now }
			cfg.Ledger = orphanLedger{LedgerPort: ledger, fail: fault == "ledger_unreadable"}
			cfg.LegacyOrphans = &x.LegacyOrphanConfig{JS: backend, Owners: owners}
			id := x.Identity{SID: fault, TransferID: "original", ObjectVersion: object.NUID}
			switch fault {
			case "sequence":
				candidate.MetadataSequence++
			case "digest":
				candidate.MetadataDigest = "foreign"
			case "incarnation":
				candidate.StreamCreated = "old-incarnation"
			case "version":
				foreign, err := store.Put(t.Context(), jetstream.ObjectMeta{Name: "foreign", Opts: &jetstream.ObjectMetaOptions{ChunkSize: 3}}, bytes.NewBufferString("abcdef"))
				if err != nil {
					t.Fatal(err)
				}
				candidate.NUID = foreign.NUID
			case "local_incomplete":
				owners.local.Complete = false
			case "local_unready":
				owners.local.Ready = false
			case "local_error":
				owners.err = errors.New("census unavailable")
			case "local_code":
				owners.local.Code = x.CodeOwnerRecovering
			case "malformed_protection":
				owners.local.Entries = []x.Protection{{Identity: x.Identity{SID: fault}}}
			case "conflicting_protection":
				owners.local.Entries = []x.Protection{{Identity: x.Identity{SID: "other", TransferID: "other", ObjectVersion: "first"}, ObjectVersion: "second"}}
			case "malformed_row":
				owners.local.Rows = []x.LedgerRow{{Identity: x.Identity{SID: fault}}}
			case "live_name", "live_version":
				if fault == "live_version" {
					id.TransferID = "different"
				}
				owners.local.Entries = []x.Protection{{Identity: id}}
			case "retained_row":
				owners.local.Rows = []x.LedgerRow{{Identity: id}}
			case "remote_unavailable", "remote_live":
				origin := x.Origin{BrokerID: "old-owner", InstanceID: "process", BootID: "boot", Generation: 1}
				owners.local.Rows = []x.LedgerRow{{Identity: x.Identity{SID: "other", TransferID: "unrelated"}, Origin: origin}}
				if fault == "remote_live" {
					owners.remote = x.LedgerCensus{Origin: origin, Ready: true, Complete: true, Entries: []x.Protection{{Identity: id}}}
				}
			case "young":
				now = time.Now()
			case "untrusted_mtime":
				now = time.Now()
				object.ModTime = now.Add(-24 * time.Hour)
				body, err := json.Marshal(object)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := js.Publish(t.Context(), "$O."+bucket+".M."+base64.URLEncoding.EncodeToString([]byte("original")), body); err != nil {
					t.Fatal(err)
				}
				candidate = orphanCandidate(t, stream, bucket, object.NUID)
			case "v1_first_chunk":
				// Put two physical chunks with only the first marked v1. A
				// last-chunk classifier alone would incorrectly permit retirement.
				if err := stream.Purge(t.Context(), jetstream.WithPurgeSubject(chunkSubject)); err != nil {
					t.Fatal(err)
				}
				msg := nats.NewMsg(chunkSubject)
				msg.Data = []byte("abc")
				msg.Header.Set("Tether-Xfer-Version", "1")
				if _, err := js.PublishMsg(t.Context(), msg); err != nil {
					t.Fatal(err)
				}
				if _, err := js.Publish(t.Context(), chunkSubject, []byte("def")); err != nil {
					t.Fatal(err)
				}
			case "partial_body":
				first, err := stream.GetMsg(t.Context(), 1, jetstream.WithGetMsgSubject(chunkSubject))
				if err != nil {
					t.Fatal(err)
				}
				if err := stream.DeleteMsg(t.Context(), first.Sequence); err != nil {
					t.Fatal(err)
				}
			case "consumer":
				if _, err := stream.CreateConsumer(t.Context(), jetstream.ConsumerConfig{Name: "reader", FilterSubject: chunkSubject, AckPolicy: jetstream.AckExplicitPolicy}); err != nil {
					t.Fatal(err)
				}
			case "missing_binding":
				cfg.LegacyOrphans = nil
			case "publish_unknown":
				backend.publishFail = true
			case "wrong_ack":
				backend.wrongACK = true
			case "foreign_readback":
				backend.foreignReadback = true
			case "post_fence_census":
				owners.afterFence = true
			case "purge_retry":
				backend.purgeFail = true
			case "purge_noop":
				backend.purgeNoop = true
			}
			port, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = port.Stop(finalDeadline(t, time.Second)) }()
			if fault == "clock_backwards" {
				now = now.Add(-time.Minute)
			}
			if fault == "stopped" {
				if err := port.Stop(finalDeadline(t, time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			retirer := port.(x.LegacyOrphanFinalizer)
			receipt, retireErr := retirer.RetireLegacyOrphan(candidate, finalDeadline(t, 5*time.Second))
			_, rawErr := stream.GetLastMsgForSubject(t.Context(), chunkSubject)
			if fault == "none" {
				if retireErr != nil || receipt.Candidate != candidate || receipt.FenceSequence <= candidate.MetadataSequence || !errors.Is(rawErr, jetstream.ErrMsgNotFound) {
					t.Fatalf("legacy orphan did not retire exactly: %+v %v chunks=%v", receipt, retireErr, rawErr)
				}
			} else {
				if retireErr == nil || receipt != (x.LegacyOrphanReceipt{}) || rawErr != nil {
					t.Fatalf("unsafe legacy retirement %s: %+v %v chunks=%v", fault, receipt, retireErr, rawErr)
				}
			}
			if fault == "purge_retry" || fault == "publish_unknown" {
				// A new finalizer, with no old in-memory candidate or fence,
				// recovers the persisted tombstone after the cap0 age floor.
				now = now.Add(proto.XferTierBMaxBudget)
				backend.purgeFail, backend.publishFail = false, false
				fresh, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = fresh.Stop(finalDeadline(t, time.Second)) }()
				candidate = orphanCandidate(t, stream, bucket, object.NUID)
				if _, err := fresh.(x.LegacyOrphanFinalizer).RetireLegacyOrphan(candidate, finalDeadline(t, 5*time.Second)); err != nil {
					t.Fatalf("retained legacy fence did not retry: %v", err)
				}
				if _, err := stream.GetLastMsgForSubject(t.Context(), chunkSubject); !errors.Is(err, jetstream.ErrMsgNotFound) {
					t.Fatalf("retry did not purge original chunks: %v", err)
				}
			}
		})
	}
}

func orphanCandidate(t *testing.T, stream jetstream.Stream, bucket, nuid string) x.LegacyOrphanCandidate {
	t.Helper()
	info, err := stream.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := stream.GetLastMsgForSubject(t.Context(), "$O."+bucket+".M."+base64.URLEncoding.EncodeToString([]byte("original")))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw.Data)
	return x.LegacyOrphanCandidate{Bucket: bucket, Stream: info.Config.Name, StreamCreated: info.Created.UTC().Format(time.RFC3339Nano), Name: "original", NUID: nuid, MetadataSequence: raw.Sequence, MetadataDigest: hex.EncodeToString(sum[:])}
}
