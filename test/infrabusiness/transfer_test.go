//go:build linux && infrabusiness_integration

package infrabusiness_test

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	i "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infratransfer"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type transferReceipt struct {
	Step uint32
	evidenceScope
	Checkpoints                                         []durableCheckpoint
	Phase, Direction, TransferID, NUID, Bucket, Session string
	CTLPID                                              int32
	CTLStartTicks                                       uint64
	Size, Durable, PreviousDurable, RateLimitBPS        int64
	OriginalRate, StartedNS, ByteDeadlineNS             int64
	OriginalDeadlineNS, MaxSilenceNS                    int64
	Consumer, RetiredConsumer, ConsumerEvidence         string
	PrefixSHA256, RawPrefixSHA256, SchemaSHA256         string
	SourceSHA256, InvocationEvidence, ReceiptEvidence   string
	StartRecords, TerminalRecords                       uint32
	Synced, ConsumerDeleted, PONGConfirmed, Final       bool
	Coverage                                            []string
}

// origin: hot-update S9 plan §8 / §9.1 / T71
func TestInfraFourTransferEveryHop(t *testing.T) {
	t.Run("retained_ports", exerciseRetainedTransferPorts)
	t.Run("ack_prefix_loss", exerciseACKPrefixLoss)
	t.Run("oracle_controls", func(t *testing.T) {
		r := transferControls()
		if err := checkTransfers(r); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"new_nuid", "new_ctl", "early_complete", "missing_transfer", "missing_phase", "renewed_budget", "changed_rate", "unsynced_prefix", "consumer_alive", "repair_polluted", "final_hash_only", "producer_pause"} {
			t.Run(name, func(t *testing.T) {
				m := copyReceipt(r)
				switch name {
				case "new_nuid":
					m.Transfers[0].NUID = "replacement"
				case "new_ctl":
					m.Transfers[0].CTLPID++
				case "early_complete":
					m.Transfers[0].Durable = m.Transfers[0].Size
				case "missing_transfer":
					m.Transfers = m.Transfers[1:]
				case "missing_phase":
					m.Transfers[0].Phase = "final"
				case "renewed_budget":
					m.Transfers[0].ByteDeadlineNS++
				case "changed_rate":
					m.Transfers[0].RateLimitBPS++
				case "unsynced_prefix":
					m.Transfers[0].Synced = false
				case "consumer_alive":
					m.Transfers[4].ConsumerDeleted = false
				case "repair_polluted":
					m.Transfers[0].RawPrefixSHA256 = digest([]byte("repaired"))
				case "final_hash_only":
					m.Transfers[0].ReceiptEvidence = ""
				case "producer_pause":
					m.Transfers[0].MaxSilenceNS = int64(upgradebudget.TransferProgress) + 1
				}
				if checkTransfers(m) == nil {
					t.Fatal("transfer oracle accepted replacement, incomplete coverage or altered original pacing")
				}
			})
		}
	})
	t.Run("live", func(t *testing.T) {
		r := runLive(t, "four-transfer")
		if err := checkTransfers(r); err != nil {
			t.Fatal(err)
		}
		if err := checkMixed(r); err != nil {
			t.Fatal(err)
		}
	})
}

// Delete an independently observed ACKed raw chunk after sealing. A retained
// source could recreate its bytes, but rebind/release must refuse before ANY
// replay. This lane does not use a harness repair or a final-SHA substitute.
// origin: hot-update S9 plan §4.3 / T70 / T71 / M20
func exerciseACKPrefixLoss(t *testing.T) {
	s := embedded(t)
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	nc, js := transferConnection(t, s.ClientURL())
	defer nc.Close()
	if _, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: "prefix", Storage: jetstream.FileStorage}); err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(t.Context(), "OBJ_prefix")
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("p"), 2*(32<<10))
	spec := x.ObjectSpec{Identity: x.Identity{TransferID: "original", EntryNonce: "original", ObjectVersion: "originalNUID", IntentDigest: digest(body)},
		Bucket: "prefix", Stream: "OBJ_prefix", StreamCreated: stream.CachedInfo().Created.Format(time.RFC3339Nano), Size: int64(len(body)), SHA256: digest(body), ChunkSize: 32 << 10, Chunks: 2}
	port, err := jsstream.NewTransferWriter(js, spec)
	if err != nil {
		t.Fatal(err)
	}
	w, err := infratransfer.WrapWriter(port, func(i.Deadline) (jetstream.JetStream, error) { return js, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Stop(deadline(t)); err != nil {
			t.Error(err)
		}
	}()
	receipt, err := w.Append(0, body[:32<<10], deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := stream.GetMsg(t.Context(), receipt.StreamSeq)
	if err != nil || sha256.Sum256(raw.Data) != receipt.SHA256 {
		t.Fatal("initial raw ACK prefix was never present")
	}
	a, err := infratransfer.New(infratransfer.Config{SourceSHA256: sha256.Sum256(body), SchemaSHA256: infratransfer.ObservationSchema(), Writers: []*infratransfer.Writer{w},
		FenceData: func(i.ID, i.Deadline) error { return nil }, // This caller has joined its synchronous Append.
		BeforeReplay: func(i.ID, i.Deadline) error {
			raw, err := stream.GetMsg(t.Context(), receipt.StreamSeq)
			if err != nil || sha256.Sum256(raw.Data) != receipt.SHA256 {
				return errors.New("ACK prefix lost before repair")
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	seal, err := a.Fence(i.ID{31}, deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.DeleteMsg(t.Context(), receipt.StreamSeq); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.GetMsg(t.Context(), receipt.StreamSeq); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatal("ACK prefix loss was not actually injected")
	}
	if err := a.Rebind(seal, deadline(t)); err == nil {
		t.Fatal("rebind repaired or ignored an absent original ACK prefix")
	}
	if err := a.Release(seal, deadline(t)); err == nil {
		t.Fatal("release opened business across a failed prefix validation")
	}
	if _, err := stream.GetMsg(t.Context(), receipt.StreamSeq); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatal("a repair producer erased original loss evidence")
	}
	state, err := w.Freeze(deadline(t))
	if err != nil || state.ObjectSpec != spec || len(state.Receipts) != 1 || state.Receipts[0] != receipt {
		t.Fatal("prefix loss reset original invocation or receipt authority")
	}
}

type retainedTransfer struct {
	spec     x.ObjectSpec
	body     []byte
	writer   *infratransfer.Writer
	reader   *infratransfer.Reader
	sink     *os.File
	hash     hash.Hash
	cp       x.SinkCheckpoint
	receipts []x.ChunkReceipt
	nc       *nats.Conn
	js       jetstream.JetStream
}

func transferConnection(t *testing.T, url string) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	nc, err := nats.Connect(url, nats.NoReconnect(), nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	return nc, js
}

// This L1 seam uses four real S7 writers/readers, raw JS messages, named
// consumers and fsynced files through nine backend replacements. Its bounded
// payload is not the 66,549,440B/1GiB live acceptance profile: that complete
// original-invocation profile remains mandatory in the separate live lane.
func exerciseRetainedTransferPorts(t *testing.T) {
	s := embedded(t)
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	oldNC, current := transferConnection(t, s.ClientURL())
	defer func() { oldNC.Close() }()
	var rows []*retainedTransfer
	for n := 0; n < 4; n++ {
		body := bytes.Repeat([]byte{byte(n + 1)}, 12*(32<<10))
		bucket := fmt.Sprintf("original%d", n)
		if _, err := current.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: bucket, Storage: jetstream.FileStorage}); err != nil {
			t.Fatal(err)
		}
		stream, err := current.Stream(t.Context(), "OBJ_"+bucket)
		if err != nil {
			t.Fatal(err)
		}
		spec := x.ObjectSpec{Identity: x.Identity{SID: "session" + fmt.Sprint(n), NID: "node", ActorFP: "actor", AgentInstance: "agent",
			TransferID: "transfer" + fmt.Sprint(n), EntryNonce: "nonce" + fmt.Sprint(n), IntentDigest: digest([]byte(bucket)), ObjectVersion: "originalNUID" + fmt.Sprint(n)},
			Bucket: bucket, Stream: "OBJ_" + bucket, StreamCreated: stream.CachedInfo().Created.Format(time.RFC3339Nano),
			Size: int64(len(body)), SHA256: digest(body), ChunkSize: 32 << 10, Chunks: 12}
		row := &retainedTransfer{spec: spec, body: body, hash: sha256.New()}
		row.nc, row.js = transferConnection(t, s.ClientURL())
		defer func() { row.nc.Close() }()
		port, err := jsstream.NewTransferWriter(row.js, spec)
		if err != nil {
			t.Fatal(err)
		}
		w, err := infratransfer.WrapWriter(port, func(i.Deadline) (jetstream.JetStream, error) { return row.js, nil })
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := w.Stop(deadline(t)); err != nil {
				t.Error(err)
			}
		}()
		row.writer = w
		rows = append(rows, row)
		// A legal foreign NUID makes raw sequence differ from logical ordinal.
		if _, err := current.Publish(t.Context(), "$O."+bucket+".C.foreign", []byte("foreign gap")); err != nil {
			t.Fatal(err)
		}
		for idx := uint32(0); idx < 2; idx++ {
			r, err := w.Append(idx, body[int(idx)*int(spec.ChunkSize):int(idx+1)*int(spec.ChunkSize)], deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			row.receipts = append(row.receipts, r)
		}
		// A reader legitimately opens only a metadata-committed object. Its
		// separate phase lane reads a completed source while the original writer
		// above is still partial; no single invocation is placed in incompatible
		// writer/reader phases just to manufacture coverage.
		readSpec := spec
		readSpec.Identity.ObjectVersion += "ReadPhase"
		readSpec.Identity.TransferID += "ReadPhase"
		readSpec.Identity.EntryNonce += "ReadPhase"
		readPort, err := jsstream.NewTransferWriter(current, readSpec)
		if err != nil {
			t.Fatal(err)
		}
		for idx := uint32(0); idx < readSpec.Chunks; idx++ {
			if _, err := readPort.Append(idx, body[int(idx)*int(readSpec.ChunkSize):int(idx+1)*int(readSpec.ChunkSize)], deadline(t)); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := readPort.Finalize(deadline(t)); err != nil {
			t.Fatal(err)
		}
		if err := readPort.Stop(deadline(t)); err != nil {
			t.Fatal(err)
		}
		reader, err := jsstream.NewTransferReader(row.js, readSpec, x.SinkCheckpoint{})
		if err != nil {
			t.Fatal(err)
		}
		row.reader, err = infratransfer.WrapReader(reader, func(i.Deadline) (jetstream.JetStream, error) { return row.js, nil })
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := row.reader.Stop(deadline(t)); err != nil {
				t.Error(err)
			}
		}()
		row.sink, err = os.CreateTemp(t.TempDir(), "original-synced-sink")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = row.sink.Close() }()
		syncNextChunk(t, row)
		if row.cp.LastConsumedStreamSeq == uint64(row.cp.ChunkIndex) {
			t.Fatal("raw stream gap was not exercised")
		}
		if unsynced, err := row.reader.ReadChunk(deadline(t)); err != nil || unsynced.Receipt.Index != 1 {
			t.Fatal("unsynced fetched suffix was not exercised")
		}
	}
	var replayReads int
	cfg := infratransfer.Config{SourceSHA256: sha256.Sum256([]byte("real retained port exercise")), SchemaSHA256: infratransfer.ObservationSchema(),
		// All byte/Sync/ConfirmSynced calls above/below finish synchronously on
		// this one caller before Fence. This joins no hidden or fake producer.
		FenceData: func(i.ID, i.Deadline) error { return nil },
		BeforeReplay: func(_ i.ID, _ i.Deadline) error {
			for _, row := range rows {
				stream, err := row.js.Stream(t.Context(), row.spec.Stream)
				if err != nil {
					return err
				}
				for _, receipt := range row.receipts {
					raw, err := stream.GetMsg(t.Context(), receipt.StreamSeq)
					if err != nil || sha256.Sum256(raw.Data) != receipt.SHA256 || raw.Header.Get("Tether-Xfer-Index") != fmt.Sprint(receipt.Index) {
						return errors.New("original ACK prefix is absent before any repair")
					}
				}
			}
			replayReads++
			return nil
		}}
	for _, row := range rows {
		cfg.Writers = append(cfg.Writers, row.writer)
		cfg.Readers = append(cfg.Readers, row.reader)
	}
	adapter, err := infratransfer.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for hop := 0; hop < 9; hop++ {
		seal, err := adapter.Fence(i.ID{byte(hop + 1)}, deadline(t))
		if err != nil || !seal.Complete {
			t.Fatal("four retained ports did not seal: ", err)
		}
		for _, row := range rows {
			state, err := row.reader.Freeze(deadline(t))
			if err != nil || !reflect.DeepEqual(state.SinkCheckpoint, row.cp) || state.ConsumerIdentity == "" || state.DeletePending || state.DirtyBarrier {
				t.Fatal("original synced prefix/consumer responsibility changed: ", err)
			}
			stream, err := current.Stream(t.Context(), row.spec.Stream)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Consumer(t.Context(), state.ConsumerIdentity); !errors.Is(err, jetstream.ErrConsumerNotFound) {
				t.Fatal("old consumer still exists before backend replacement: ", err)
			}
		}
		for _, row := range rows {
			nextNC, next := transferConnection(t, s.ClientURL())
			oldCID, _ := row.nc.GetClientID()
			newCID, _ := nextNC.GetClientID()
			if newCID == 0 || newCID == oldCID {
				nextNC.Close()
				t.Fatal("independent data NC was not physically replaced")
			}
			row.nc.Close()
			row.nc, row.js = nextNC, next
		}
		if err := adapter.Rebind(seal, deadline(t)); err != nil {
			t.Fatal(err)
		}
		if err := adapter.Release(seal, deadline(t)); err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			state, err := row.writer.Snapshot(deadline(t))
			if err != nil || state.ObjectSpec != row.spec || !reflect.DeepEqual(state.Receipts, row.receipts) {
				t.Fatal("port rebind changed original NUID or raw receipt prefix: ", err)
			}
			syncNextChunk(t, row)
			idx := uint32(hop + 2)
			r, err := row.writer.Append(idx, row.body[int(idx)*int(row.spec.ChunkSize):int(idx+1)*int(row.spec.ChunkSize)], deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			row.receipts = append(row.receipts, r)
		}
	}
	if replayReads != 18 {
		t.Fatal("pre-repair gate was bypassed during rebind or release")
	}
	for _, row := range rows {
		last, err := row.writer.Append(11, row.body[11*int(row.spec.ChunkSize):], deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		row.receipts = append(row.receipts, last)
		if _, err := row.writer.Finalize(deadline(t)); err != nil {
			t.Fatal(err)
		}
		for row.cp.ChunkIndex < row.spec.Chunks {
			syncNextChunk(t, row)
		}
		if _, err := row.reader.ReadChunk(deadline(t)); err != io.EOF {
			t.Fatal("original suffix did not reach definite EOF: ", err)
		}
		actual, err := os.ReadFile(row.sink.Name())
		if err != nil || !bytes.Equal(actual, row.body) {
			t.Fatal("fsynced original sink has loss or duplicate bytes")
		}
		// Retire product ports before server cleanup; the five-round lifecycle
		// separately checks process resources, not just t.Cleanup's eventual IO.
		if err := row.reader.Stop(deadline(t)); err != nil {
			t.Fatal(err)
		}
		if err := row.writer.Stop(deadline(t)); err != nil {
			t.Fatal(err)
		}
	}
}

func syncNextChunk(t *testing.T, row *retainedTransfer) {
	t.Helper()
	chunk, err := row.reader.ReadChunk(deadline(t))
	if err != nil || chunk.Receipt.Index != row.cp.ChunkIndex {
		t.Fatal("successor resumed a delivery ordinal or lost the synced suffix: ", err)
	}
	if _, err := row.sink.WriteAt(chunk.Data, row.cp.DurableOffset); err != nil {
		t.Fatal(err)
	}
	if err := row.sink.Sync(); err != nil {
		t.Fatal(err)
	}
	_, _ = row.hash.Write(chunk.Data)
	hashState, err := row.hash.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	row.cp = x.SinkCheckpoint{ChunkIndex: chunk.Receipt.Index + 1, DurableOffset: row.cp.DurableOffset + int64(len(chunk.Data)), LastConsumedStreamSeq: chunk.Receipt.StreamSeq, SHA256State: hashState}
	if err := row.reader.ConfirmSynced(row.cp); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S9 plan §9.1 / T71 / M21 / M30 / lead LQ3
func checkTransfers(r businessReceipt) error {
	originals, err := transferOriginals(r)
	if err != nil {
		return err
	}
	if err := checkTransferProgress(r, originals); err != nil {
		return err
	}
	if err := checkTransferFinals(r, originals); err != nil {
		return err
	}
	if err := checkTransferPhases(r, originals); err != nil {
		return err
	}
	if len(originals) != 4 || len(r.Hops) == 0 || len(r.Transfers) != 4*3*len(r.Hops) {
		return errors.New("four ORIGINAL transfers are not live in every hop and phase")
	}
	rows := make(map[string]bool)
	buckets, sessions := make(map[string]string), make(map[string]string)
	directions := make(map[string]string)
	last := make(map[string]int64)
	for _, row := range r.Transfers {
		original, ok := originals[row.TransferID]
		if !ok || original.NUID != row.NUID || original.CTLPID != row.CTLPID || original.CTLStartTicks != row.CTLStartTicks || original.Size != row.Size || original.RateLimitBPS != row.RateLimitBPS ||
			original.StartedNS != row.StartedNS || original.DeadlineNS != row.ByteDeadlineNS || row.Step == 0 || int(row.Step) > len(r.Hops) ||
			row.Phase != "before" && row.Phase != "moving" && row.Phase != "returning" || row.Direction != "push" && row.Direction != "pull" ||
			row.Size != 66549440 && row.Size != 1<<30 || row.Durable <= 0 || row.Durable >= row.Size || row.Durable < row.PreviousDurable ||
			row.RateLimitBPS <= 0 || row.RateLimitBPS != row.OriginalRate || row.StartedNS <= 0 || row.ByteDeadlineNS != row.OriginalDeadlineNS ||
			row.ByteDeadlineNS <= row.StartedNS || row.MaxSilenceNS < 0 || row.MaxSilenceNS > int64(upgradebudget.TransferProgress) ||
			!row.Synced || row.Final || row.StartRecords != 1 || row.TerminalRecords != 0 || !validDigest(row.PrefixSHA256) || row.PrefixSHA256 != row.RawPrefixSHA256 ||
			row.SchemaSHA256 != r.SchemaSHA256 || row.SourceSHA256 != r.SourceSHA256 || !validDigest(row.SchemaSHA256) || !validDigest(row.SourceSHA256) || !validDigest(row.InvocationEvidence) || !validDigest(row.ReceiptEvidence) {
			return errors.New("original transfer identity, ACK/synced prefix, rate or absolute budget changed")
		}
		key := fmt.Sprintf("%d/%s/%s", row.Step, row.Phase, row.TransferID)
		if rows[key] || buckets[row.Bucket] != "" && buckets[row.Bucket] != row.TransferID || sessions[row.Session] != "" && sessions[row.Session] != row.TransferID || row.Bucket == "" || row.Session == "" {
			return errors.New("transfer phase duplicated or isolation was lost")
		}
		if direction, exists := directions[row.TransferID]; exists && direction != row.Direction {
			return errors.New("original direction changed")
		}
		if row.Durable < last[row.TransferID] {
			return errors.New("durable progress went backwards")
		}
		last[row.TransferID], directions[row.TransferID] = row.Durable, row.Direction
		if row.Phase != "before" && (!row.ConsumerDeleted || !row.PONGConfirmed || row.RetiredConsumer == "" || row.RetiredConsumer == row.Consumer || !validDigest(row.ConsumerEvidence)) {
			return errors.New("consumer retirement was replaced by socket close or empty census")
		}
		rows[key], buckets[row.Bucket], sessions[row.Session] = true, row.TransferID, row.TransferID
	}
	var pushes, pulls int
	for _, direction := range directions {
		if direction == "push" {
			pushes++
		} else {
			pulls++
		}
	}
	if pushes != 2 || pulls != 2 || len(buckets) != 4 || len(sessions) != 4 {
		return errors.New("four transfer directions/isolation are incomplete")
	}
	return nil
}

func transferControls() businessReceipt {
	r := oracleReceipt()
	populateTransferControls(&r)
	return r
}
