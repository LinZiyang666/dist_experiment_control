package xferclient

import (
	"crypto/sha256"
	"encoding"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

func (c *Client) pull(deadline x.Deadline) error {
	// A landed primitive is an irreversible local fact, regardless of whether
	// its terminal/finalize response was delivered. Reentry only settles it.
	// origin: hot-update S7 plan §3 / §7 / T33
	if c.outcome != nil {
		return c.settle(deadline)
	}
	if c.cfg.Intent.Tier == "a" {
		if err := c.receiveInline(deadline); err != nil {
			return err
		}
		return c.commitPull(deadline)
	}
	if err := c.bindPull(deadline); err != nil {
		return err
	}
	if c.reader == nil {
		reader, err := c.cfg.MakeReader(c.js, *c.spec, c.checkpoint)
		c.reader = reader
		if err != nil {
			return c.failure(x.CodeOwnerRecovering, err)
		}
		if reader == nil {
			return c.failure(x.CodeResultUnknown, errors.New("reader factory returned no reader"))
		}
	}
	if c.checkpoint.ChunkIndex != 0 {
		// A failed ConfirmSynced may leave the reader behind the real sink. Re-
		// confirm that retained prefix before any fetch; do not rewrite it.
		// origin: hot-update S7 plan §3 / T33
		if err := c.reader.ConfirmSynced(c.checkpoint); err != nil {
			return c.failure(x.CodeObjectConflict, err)
		}
	}
	hashState, err := sinkHash(c.checkpoint)
	if err != nil {
		return c.failure(x.CodeObjectConflict, err)
	}
	c.phase = x.PhaseReceiving
	for c.checkpoint.ChunkIndex < c.spec.Chunks {
		if err := c.checkBytes(deadline); err != nil {
			return err
		}
		limit, err := c.attemptDeadline(deadline)
		if err != nil {
			return err
		}
		chunk, err := c.reader.ReadChunk(limit)
		if err != nil {
			if waitErr := c.wait(deadline, 500*time.Millisecond, true); waitErr != nil {
				return waitErr
			}
			continue
		}
		if err := c.writeChunk(hashState, chunk, deadline); err != nil {
			return err
		}
	}
	if c.checkpoint.DurableOffset != c.spec.Size || hex.EncodeToString(hashState.Sum(nil)) != c.spec.SHA256 {
		return c.failure(x.CodeObjectConflict, errors.New("durable sink size or SHA-256 differs"))
	}
	// Empty objects still require a real file Sync before commit admission.
	if c.spec.Chunks == 0 {
		if err := c.cfg.Destination.Sync(); err != nil {
			return c.failure(x.CodeDurabilityUnknown, err)
		}
	}
	return c.commitPull(deadline)
}

func (c *Client) commitPull(deadline x.Deadline) error {
	if c.grant == nil {
		// Bytes alone cannot authorize the rename. The same original reserve
		// is reconciled on ACK loss and must return the same durable grant slot.
		// origin: hot-update S7 plan §4 / §7 / T33
		if err := c.checkBytes(deadline); err != nil {
			return err
		}
		// P00 has no separate ctl instance field. The original actor plus this
		// nonreusable invocation ID names the local receiver; it is never reminted.
		// A transport that authenticates its actor names it instead: the broker
		// binds a ctl reservation to that subject actor, never to a body string.
		// origin: hot-update S7 plan §4 / §6.3 / INTEG-D outer adapter
		receiver := "ctl-" + c.identity.TransferID
		if named, ok := c.cfg.Transport.(interface{ ReceiverInstance() string }); ok && named.ReceiverInstance() != "" {
			receiver = named.ReceiverInstance()
		}
		request := x.ReserveRequest{CallID: c.callID(x.OpReserve), Origin: c.origin, ReceiverInstance: receiver}
		response, err := c.mutate(x.Request{Op: x.OpReserve, CallID: request.CallID, Identity: c.identity, Origin: c.origin, Reserve: &request}, deadline)
		if err != nil || c.receipt != nil {
			return err
		}
		grant := response.Grant
		if grant == nil || grant.Identity != c.identity || grant.GrantNonce == "" || grant.TerminalID == "" || grant.ReceiverInstance != request.ReceiverInstance {
			return c.failure(x.CodeIntentConflict, errors.New("commit grant does not match the original receiver"))
		}
		copy := *grant
		c.grant = &copy
		// The Grant authorizes landing only the source the original sender
		// sealed. On a mismatch the gate closes here, before any primitive, and
		// that not-entered proof is settled so the broker decides failed rather
		// than retaining an unresolvable reservation.
		// origin: hot-update S7 plan §2.2 / §4 / INTEG-D pull source seal
		if err := matchesClientSealedSource(c, response.Source); err != nil {
			// Not-entered names no landed bytes: the private tmp prefix never
			// became the destination, so the decision carries none.
			outcome := x.ReceiverOutcome{Grant: copy, Outcome: x.OutcomeNotEntered,
				Code: x.CodeSourceChanged, Error: "received bytes differ from the sealed source"}
			c.outcome = &outcome
			if settleErr := c.settle(deadline); settleErr != nil {
				return settleErr
			}
			return c.failure(x.CodeSourceChanged, err)
		}
	}
	if err := c.checkBytes(deadline); err != nil {
		return err
	}
	c.phase = x.PhaseCommitReserved
	// Save every outcome, including inflight or landed+directory-sync failure.
	// A second Run must not enter the primitive even when no history receipt
	// arrived; the original Atomic may still need to finish directory settlement.
	// origin: hot-update S7 plan §4 / §7 / T33
	outcome := c.cfg.Destination.Commit(*c.grant, minDeadline(deadline, c.deadline))
	c.outcome = &outcome
	if outcome.Grant != *c.grant || outcome.Bytes != 0 && outcome.Bytes != c.spec.Size || outcome.SHA256 != "" && outcome.SHA256 != c.spec.SHA256 {
		return c.failure(x.CodeTerminalConflict, errors.New("primitive outcome does not match the reserved source"))
	}
	// The atomic port knows the primitive and directory state. The receiver
	// owns the independently verified, synced size/hash; a WRONLY tmp cannot
	// be reread by the primitive owner to manufacture those source facts.
	outcome.Bytes, outcome.SHA256 = c.checkpoint.DurableOffset, c.spec.SHA256
	switch outcome.Outcome {
	case x.OutcomeLanded, x.OutcomeNotEntered:
	case x.OutcomeInflight:
		return c.failure(x.CodeCommitInflight, nil)
	default:
		return c.failure(x.CodeResultUnknown, errors.New("primitive returned no outcome"))
	}
	return c.settle(deadline)
}

// matchesSealedSource compares the Grant's sealed source with the bytes this
// receiver verified. A seal already accepted at prepare must be repeated
// exactly; without one, the Grant's seal is the only authenticated source.
func matchesClientSealedSource(c *Client, s *x.SourceSeal) error {
	if s == nil {
		if c.preparedSource != nil {
			return nil
		}
		return errors.New("grant names no sealed source")
	}
	if c.preparedSource != nil && *c.preparedSource != *s {
		return errors.New("sealed source changed after prepare")
	}
	if c.spec == nil || s.Size != c.spec.Size || s.SHA256 != c.spec.SHA256 || s.RateLimitBPS != c.cfg.Intent.RateLimitBPS ||
		(c.cfg.Intent.Tier == "b" && (s.ChunkSize != c.spec.ChunkSize || s.Chunks != c.spec.Chunks)) ||
		(c.cfg.Intent.Tier == "a" && (s.ChunkSize != 0 || s.Chunks != 0)) {
		return errors.New("received object differs from the sealed source")
	}
	return nil
}

func (c *Client) bindPull(deadline x.Deadline) error {
	if err := c.checkBytes(deadline); err != nil {
		return err
	}
	if c.spec.Identity.TransferID != "" {
		return nil
	}
	if err := c.openData(deadline); err != nil {
		return err
	}
	for {
		if err := c.checkBytes(deadline); err != nil {
			return err
		}
		ctx, cancel, err := c.lookupContext(deadline)
		if err != nil {
			return err
		}
		bucket := proto.XferBucketName(c.identity.SID)
		objects, lookupErr := c.js.Stream(ctx, "OBJ_"+bucket)
		if lookupErr != nil {
			cancel()
			return c.failure(x.CodeOwnerRecovering, lookupErr)
		}
		// The capable path never constructs an SDK ObjectStore handle (L1: no
		// call graph from here can reach Put). The fixed name's metadata is
		// read raw at its exact subject — the record GetInfo would decode —
		// and validated below against the fixed NUID and the sealed source.
		// origin: hot-update S7 plan L1 / §7 / T49 (capable graph has no ObjectStore)
		raw, lookupErr := objects.GetLastMsgForSubject(ctx, "$O."+bucket+".M."+base64.URLEncoding.EncodeToString([]byte(c.identity.TransferID)))
		cancel()
		var info jetstream.ObjectInfo
		if lookupErr == nil && json.Unmarshal(raw.Data, &info) != nil {
			return c.failure(x.CodeObjectConflict, errors.New("object metadata cannot be decoded"))
		}
		if lookupErr != nil {
			// Source publication may still be running at the original agent.
			// Poll the original responsibility rather than starting another Get.
			if _, err := c.reconcile(deadline); err != nil {
				return err
			}
			if c.receipt != nil {
				return c.failure(x.CodeTerminalConflict, errors.New("pull ended before the local receiver"))
			}
			if err := c.wait(deadline, 500*time.Millisecond, true); err != nil {
				return err
			}
			continue
		}
		if info.NUID != c.identity.ObjectVersion || info.Deleted || info.Bucket != bucket || info.Name != c.identity.TransferID || info.Size > proto.XferMaxBytes {
			return c.failure(x.CodeObjectVersionMismatch, errors.New("metadata does not name the fixed object version"))
		}
		if !c.budget.SourceSealed {
			if _, err := c.reconcile(deadline); err != nil {
				return err
			}
			if !c.budget.SourceSealed {
				return c.failure(x.CodeResultUnknown, errors.New("pull source budget is not sealed"))
			}
		}
		if int64(info.Size) != c.budget.Size || c.cfg.Intent.Size != 0 && int64(info.Size) != c.cfg.Intent.Size || info.Opts == nil || info.Opts.Link != nil || info.Opts.ChunkSize != c.spec.ChunkSize {
			return c.failure(x.CodeObjectConflict, errors.New("metadata geometry differs from the source seal"))
		}
		chunks := uint32((info.Size + uint64(info.Opts.ChunkSize) - 1) / uint64(info.Opts.ChunkSize))
		if chunks != info.Chunks || chunks > 65536 {
			return c.failure(x.CodeObjectConflict, errors.New("invalid object chunk count"))
		}
		digest, err := base64.URLEncoding.DecodeString(strings.TrimPrefix(info.Digest, "SHA-256="))
		if err != nil || len(digest) != sha256.Size || !strings.HasPrefix(info.Digest, "SHA-256=") {
			return c.failure(x.CodeObjectConflict, errors.New("invalid object digest"))
		}
		sha := hex.EncodeToString(digest)
		if c.preparedSource != nil && (c.preparedSource.SHA256 != sha || c.preparedSource.Size != int64(info.Size) || c.preparedSource.Chunks != chunks || c.preparedSource.ChunkSize != info.Opts.ChunkSize) || c.cfg.Intent.SHA256 != "" && c.cfg.Intent.SHA256 != sha {
			return c.failure(x.CodeSourceChanged, errors.New("source digest differs from the original intent"))
		}
		ctx, cancel, err = c.lookupContext(deadline)
		if err != nil {
			return err
		}
		streamInfo, lookupErr := objects.Info(ctx)
		cancel()
		if lookupErr != nil {
			return c.failure(x.CodeOwnerRecovering, lookupErr)
		}
		if err := c.reserveBuffer(info.Opts.ChunkSize); err != nil {
			return err
		}
		c.spec = &x.ObjectSpec{Identity: c.identity, Bucket: bucket, Stream: streamInfo.Config.Name, StreamCreated: streamInfo.Created.UTC().Format(time.RFC3339Nano),
			Size: int64(info.Size), SHA256: sha, ChunkSize: info.Opts.ChunkSize, Chunks: chunks}
		return nil
	}
}

func sinkHash(checkpoint x.SinkCheckpoint) (hash.Hash, error) {
	h := sha256.New()
	if len(checkpoint.SHA256State) != 0 {
		decoder, ok := h.(encoding.BinaryUnmarshaler)
		if !ok {
			return nil, errors.New("SHA-256 checkpoint decoder is unavailable")
		}
		if err := decoder.UnmarshalBinary(checkpoint.SHA256State); err != nil {
			return nil, err
		}
	} else if checkpoint.DurableOffset != 0 || checkpoint.ChunkIndex != 0 {
		return nil, errors.New("durable checkpoint has no digest state")
	}
	return h, nil
}

func (c *Client) writeChunk(h hash.Hash, chunk x.Chunk, deadline x.Deadline) error {
	r := chunk.Receipt
	want := min(int64(c.spec.ChunkSize), c.spec.Size-c.checkpoint.DurableOffset)
	if r.Index != c.checkpoint.ChunkIndex || r.Offset != c.checkpoint.DurableOffset || r.StreamSeq <= c.checkpoint.LastConsumedStreamSeq ||
		r.Length != uint32(len(chunk.Data)) || int64(len(chunk.Data)) != want || r.SHA256 != sha256.Sum256(chunk.Data) {
		return c.failure(x.CodeObjectConflict, errors.New("fetched chunk differs from the durable prefix"))
	}
	if err := c.pace(r.Offset+int64(len(chunk.Data)), deadline); err != nil {
		return err
	}
	for written := 0; written < len(chunk.Data); {
		if err := c.checkBytes(deadline); err != nil {
			return err
		}
		n, err := c.cfg.Destination.WriteAt(chunk.Data[written:], r.Offset+int64(written))
		if n < 0 || n > len(chunk.Data)-written {
			return c.failure(x.CodeObjectConflict, errors.New("invalid sink write count"))
		}
		written += n
		if err != nil || n == 0 {
			return c.failure(x.CodeDurabilityUnknown, err)
		}
	}
	if err := c.cfg.Destination.Sync(); err != nil {
		// The partial write remains a private tmp suffix, never a checkpoint.
		// The reader may refetch from the prior durable prefix on reentry.
		// origin: hot-update S7 plan §3 / T33
		return c.failure(x.CodeDurabilityUnknown, err)
	}
	_, _ = h.Write(chunk.Data)
	encoder, ok := h.(encoding.BinaryMarshaler)
	if !ok {
		return c.failure(x.CodeObjectConflict, errors.New("SHA-256 checkpoint encoder is unavailable"))
	}
	state, err := encoder.MarshalBinary()
	if err != nil {
		return c.failure(x.CodeObjectConflict, err)
	}
	checkpoint := x.SinkCheckpoint{ChunkIndex: r.Index + 1, DurableOffset: r.Offset + int64(len(chunk.Data)), LastConsumedStreamSeq: r.StreamSeq, SHA256State: state}
	// The complete, synced prefix is retained even if the reader rejects its
	// local confirmation. Reentry reconciles it before asking for another chunk.
	// origin: hot-update S7 plan §3 / T33
	c.checkpoint = checkpoint
	if err := c.reader.ConfirmSynced(checkpoint); err != nil {
		return c.failure(x.CodeObjectConflict, err)
	}
	return nil
}

func (c *Client) settle(deadline x.Deadline) error {
	if c.receipt != nil {
		return nil
	}
	if c.outcome == nil {
		return c.failure(x.CodeResultUnknown, errors.New("no retained receiver outcome"))
	}
	switch c.outcome.Outcome {
	case x.OutcomeLanded, x.OutcomeNotEntered:
	case x.OutcomeInflight:
		return c.failure(x.CodeCommitInflight, nil)
	default:
		return c.failure(x.CodeResultUnknown, errors.New("no resolved primitive outcome to settle"))
	}
	if c.grant == nil || c.outcome.Grant != *c.grant {
		return c.failure(x.CodeTerminalConflict, errors.New("retained outcome has a different grant"))
	}
	// A landed effect survives the byte deadline. Drive only the original
	// Atomic's idempotent unlink/Sync branch with this settlement attempt's
	// deadline, preserving the grant, parent OFD and independently verified hash.
	// origin: S7 internal review rv-recovery F04
	if c.outcome.Outcome == x.OutcomeLanded && (!c.outcome.ParentSynced || c.outcome.TmpCleanupPending) {
		limit, err := c.attemptDeadline(deadline)
		if err != nil {
			return err
		}
		previous := *c.outcome
		next := c.cfg.Destination.Commit(previous.Grant, limit)
		if next.Grant != previous.Grant || next.Outcome != previous.Outcome || next.Primitive != previous.Primitive ||
			(next.Bytes != 0 && next.Bytes != previous.Bytes) || (next.SHA256 != "" && next.SHA256 != previous.SHA256) ||
			(previous.ParentSynced && !next.ParentSynced) || (!previous.TmpCleanupPending && next.TmpCleanupPending) {
			return c.failure(x.CodeTerminalConflict, errors.New("atomic settlement changed retained evidence"))
		}
		next.Bytes, next.SHA256 = previous.Bytes, previous.SHA256
		c.outcome = &next
		if !next.ParentSynced || next.TmpCleanupPending {
			return c.failure(x.CodeDurabilityUnknown, errors.New("landed receiver settlement is pending"))
		}
	}
	// The invocation retains its capsule and primitive outcome across ordinary
	// transport failure. The transport binds settlement authentication separately;
	// no status reply can introduce a new capsule.
	// origin: hot-update S7 plan §7 / INTEG-D
	// The dedicated terminal carrier includes the grant and primitive outcome;
	// the old finalize DTO cannot carry that proof. Both reach the same decision
	// owner through the outer adapter, rather than creating another terminal.
	// origin: hot-update S7 plan §6.2–§6.3 / §7 / T33
	_, err := c.mutate(x.Request{Op: x.OpTerminal, CallID: c.callID(x.OpTerminal), Identity: c.identity, Origin: c.origin, Outcome: c.outcome}, deadline)
	if err != nil {
		return err
	}
	if c.receipt == nil {
		return c.failure(x.CodeResultUnknown, errors.New("finalize has no exact history acknowledgement"))
	}
	return nil
}

// acceptPrepared binds source evidence independently of object metadata. A
// forged object with internally consistent chunks must not replace the source
// pinned by the authenticated agent. Capsules are accepted only at prepare.
// origin: hot-update S7 plan §3 / §6.4 / INTEG-D
func (c *Client) acceptPrepared(r x.Response) error {
	// A Tier B pull's agent seals its source only after the sender starts:
	// hashing a 2 GiB source cannot fit the prepare's control window. Its seal
	// then reaches this invocation through the tracker's sealed budget and the
	// fixed object's metadata; a seal that IS attached is still fully checked.
	// origin: hot-update S7 plan §1.2 / §6.2 / INTEG-D pull source seal
	if c.cfg.Intent.Verb == "pull" && (r.Source != nil || c.cfg.Intent.Tier == "a") {
		s := r.Source
		if s == nil || !validSHA(s.SHA256) || s.SourceFingerprint == "" || s.Size < 0 || s.Size > proto.XferMaxBytes || s.RateLimitBPS != c.cfg.Intent.RateLimitBPS || !c.budget.SourceSealed || c.budget.Size != s.Size {
			return c.failure(x.CodeSourceChanged, errors.New("prepare has no authenticated source seal"))
		}
		if c.cfg.Intent.Tier == "a" {
			sum := sha256.Sum256(r.Inline)
			if s.Size > proto.XferTierAMaxBytes || s.Chunks != 0 || s.ChunkSize != 0 || int64(len(r.Inline)) != s.Size || hex.EncodeToString(sum[:]) != s.SHA256 {
				return c.failure(x.CodeSourceChanged, errors.New("inline source does not match prepare seal"))
			}
			if err := c.reserveBuffer(uint32(max(s.Size, 32*1024))); err != nil {
				return err
			}
			c.inline = append([]byte{}, r.Inline...)
			c.spec = &x.ObjectSpec{Identity: c.identity, Size: s.Size, SHA256: s.SHA256}
		} else if s.ChunkSize != c.spec.ChunkSize || s.Chunks != uint32((s.Size+int64(s.ChunkSize)-1)/int64(s.ChunkSize)) || s.Chunks > 65536 {
			return c.failure(x.CodeSourceChanged, errors.New("prepared source geometry changed"))
		}
		copy := *s
		c.preparedSource = &copy
	}
	if r.Settlement != nil {
		capsule := *r.Settlement
		if capsule.Identity != c.identity || capsule.Origin != c.origin || capsule.ReceiverNkey == "" || capsule.Signature == "" || capsule.Nonce == "" || capsule.Issuer == "" {
			return c.failure(x.CodeIntentConflict, errors.New("prepare settlement binding changed"))
		}
		// The transport owns the NKey and can establish the restricted connection;
		// the leaf keeps no credential and never supplies a capsule through status.
		if binder, ok := c.cfg.Transport.(interface {
			BindSettlement(x.SettlementCapsule) error
		}); ok {
			if err := binder.BindSettlement(capsule); err != nil {
				return c.failure(x.CodeIntentConflict, err)
			}
		}
		c.settlement = &capsule
	}
	return nil
}

func (c *Client) receiveInline(deadline x.Deadline) error {
	if c.preparedSource == nil || c.spec == nil {
		return c.failure(x.CodeSourceChanged, nil)
	}
	if c.checkpoint.DurableOffset == c.spec.Size && c.checkpoint.ChunkIndex == 1 {
		return nil
	}
	if err := c.pace(c.spec.Size, deadline); err != nil {
		return err
	}
	for offset := 0; offset < len(c.inline); {
		if err := c.checkBytes(deadline); err != nil {
			return err
		}
		n, err := c.cfg.Destination.WriteAt(c.inline[offset:], int64(offset))
		if n < 0 || n > len(c.inline)-offset || err != nil || n == 0 {
			return c.failure(x.CodeDurabilityUnknown, err)
		}
		offset += n
	}
	if err := c.cfg.Destination.Sync(); err != nil {
		return c.failure(x.CodeDurabilityUnknown, err)
	}
	c.checkpoint = x.SinkCheckpoint{DurableOffset: c.spec.Size, ChunkIndex: 1}
	return nil
}
