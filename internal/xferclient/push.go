package xferclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// Stop's closed gate and successful join precede Freeze. Keep the exact frozen
// witness and the original attachment until the retained owner acknowledges
// it; a lost reply must never reopen the writer or forget its pending fences.
// origin: hot-update S7 plan §2.3 / FIX-TERM request 3
func stopClientWriter(c *Client, d x.Deadline) error {
	if err := c.writer.Stop(d); err != nil {
		return err
	}
	// A complete object's receiver already supplied positive finalized-writer
	// evidence. Its eager cleanup can have consumed the durable row by Stop.
	if c.receipt != nil && c.receipt.Kind == "complete" {
		c.writer = nil
		return nil
	}
	if c.writerWitness == nil {
		state, err := c.writer.Freeze(d)
		if err != nil {
			return err
		}
		if !state.RetirementFence.WriterRetired || state.ObjectSpec.Identity != c.identity {
			return c.failure(x.CodeReconcileRequired, errors.New("writer has no positive original retirement witness"))
		}
		c.writerWitness = &state
	}
	producer, ok := c.cfg.Transport.(interface {
		RetireWriter(x.Identity, x.Origin, x.WriterState, x.Deadline) error
	})
	if !ok {
		return c.failure(x.CodeOwnerRecovering, errors.New("authenticated writer retirement channel unavailable"))
	}
	if err := producer.RetireWriter(c.identity, c.origin, *c.writerWitness, d); err != nil {
		return err
	}
	c.writer, c.writerWitness = nil, nil
	return nil
}

func (c *Client) push(deadline x.Deadline) error {
	if c.committed {
		return c.waitTerminal(deadline)
	}
	if c.cfg.Intent.Tier == "a" {
		return pushInline(c, deadline)
	}
	if c.pushSeal == nil {
		if err := c.sealPush(deadline); err != nil {
			return err
		}
	}
	if !c.sourcePublished {
		response, err := c.mutate(x.Request{Op: x.OpSource, CallID: c.callID(x.OpSource), Identity: c.identity, Origin: c.origin, Source: c.pushSeal}, deadline)
		if err != nil || c.receipt != nil {
			return err
		}
		if response.Status == nil || !response.Status.Budget.SourceSealed {
			return c.failure(x.CodeResultUnknown, errors.New("source seal has no durable acknowledgement"))
		}
		c.sourcePublished = true
	}
	if !c.objectFinalized {
		if err := uploadObject(c, deadline); err != nil {
			return err
		}
	}
	if err := c.checkBytes(deadline); err != nil {
		return err
	}
	_, err := c.mutate(x.Request{Op: x.OpCommit, CallID: c.callID(x.OpCommit), Identity: c.identity, Origin: c.origin}, deadline)
	if err != nil || c.receipt != nil {
		return err
	}
	c.committed = true
	return c.waitTerminal(deadline)
}

func (c *Client) sealPush(deadline x.Deadline) error {
	// Account for source verification and the retained pending chunk before
	// allocating either. A changed source still owns this until explicit Stop.
	// origin: hot-update S7 plan §8.3 / T33
	if err := c.reserveBuffer(c.spec.ChunkSize); err != nil {
		return err
	}
	info, err := c.cfg.Source.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != c.cfg.Intent.Size {
		return c.failure(x.CodeSourceChanged, err)
	}
	c.sourceInfo = info
	if err := c.verifySource(deadline); err != nil {
		return err
	}
	fingerprint, err := sourceFingerprint(info)
	if err != nil {
		return c.failure(x.CodeSourceChanged, err)
	}
	chunkSize := c.spec.ChunkSize
	chunks := uint32((c.cfg.Intent.Size + int64(chunkSize) - 1) / int64(chunkSize))
	if chunks > 65536 {
		return c.failure(x.CodePayloadUnsupported, errors.New("object exceeds 65536 chunks"))
	}
	c.pushSeal = &x.SourceSeal{Size: info.Size(), SHA256: c.cfg.Intent.SHA256, ChunkSize: chunkSize,
		Chunks: chunks, RateLimitBPS: c.cfg.Intent.RateLimitBPS, SourceFingerprint: fingerprint}
	return nil
}

// sourceFingerprint is a stat-identity digest, never the path or its contents.
func sourceFingerprint(info os.FileInfo) (string, error) {
	if info == nil {
		return "", errors.New("source was not pinned")
	}
	encoded, err := json.Marshal(info.Sys())
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func (c *Client) verifySource(deadline x.Deadline) error {
	hash := sha256.New()
	buffer := make([]byte, 32*1024)
	for offset := int64(0); offset < c.cfg.Intent.Size; {
		if err := c.checkBytes(deadline); err != nil {
			return err
		}
		length := min(int64(len(buffer)), c.cfg.Intent.Size-offset)
		n, err := c.cfg.Source.ReadAt(buffer[:length], offset)
		if int64(n) != length || err != nil && !errors.Is(err, io.EOF) {
			return c.failure(x.CodeSourceChanged, err)
		}
		_, _ = hash.Write(buffer[:n])
		offset += int64(n)
	}
	info, err := c.cfg.Source.Stat()
	if err != nil {
		return c.failure(x.CodeSourceChanged, err)
	}
	if !os.SameFile(c.sourceInfo, info) || info.Size() != c.sourceInfo.Size() || !info.ModTime().Equal(c.sourceInfo.ModTime()) ||
		info.Mode() != c.sourceInfo.Mode() || hex.EncodeToString(hash.Sum(nil)) != c.cfg.Intent.SHA256 {
		return c.failure(x.CodeSourceChanged, errors.New("pinned source changed before metadata publication"))
	}
	return nil
}

func (c *Client) bindPush(deadline x.Deadline) error {
	if err := c.checkBytes(deadline); err != nil {
		return err
	}
	if c.spec.Identity.TransferID != "" {
		return nil
	}
	if err := c.openData(deadline); err != nil {
		return err
	}
	ctx, cancel, err := c.lookupContext(deadline)
	if err != nil {
		return err
	}
	defer cancel()
	bucket := proto.XferBucketName(c.identity.SID)
	stream, err := c.js.Stream(ctx, "OBJ_"+bucket)
	if err != nil {
		return c.failure(x.CodeOwnerRecovering, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return c.failure(x.CodeOwnerRecovering, err)
	}
	// The permit's bucket coordinator is the policy owner. This readback also
	// prevents writing into a recreated/unsafe stream during client binding.
	// origin: hot-update S7 plan §2.1 / lead LQ3
	horizon := proto.XferTierBMaxBudget + 120*time.Second
	if info.Config.Duplicates < horizon || info.Config.MaxAge != 0 && info.Config.MaxAge < horizon {
		return c.failure(x.CodeStorageHorizon, fmt.Errorf("required Duplicates >= %s and MaxAge = 0 or >= %s", horizon, horizon))
	}
	if info.Config.MaxMsgSize > 0 && int64(c.pushSeal.ChunkSize)+4096 > int64(info.Config.MaxMsgSize) {
		return c.failure(x.CodePayloadUnsupported, errors.New("stream message limit cannot carry the sealed chunk"))
	}
	if err := c.reserveBuffer(c.pushSeal.ChunkSize); err != nil {
		return err
	}
	c.spec = &x.ObjectSpec{Identity: c.identity, Bucket: bucket, Stream: info.Config.Name, StreamCreated: info.Created.UTC().Format(time.RFC3339Nano),
		Size: c.pushSeal.Size, SHA256: c.pushSeal.SHA256, ChunkSize: c.pushSeal.ChunkSize, Chunks: c.pushSeal.Chunks}
	// Retain the complete spec once bound. A later attachment must not replace
	// its creation timestamp, NUID or source seal with a fresh stream lookup.
	// origin: hot-update S7 plan §2.1 / §7
	return nil
}

func (c *Client) lookupContext(deadline x.Deadline) (context.Context, context.CancelFunc, error) {
	remaining, err := c.remaining(deadline)
	if err != nil {
		return nil, nil, c.failure(x.CodeResultUnknown, err)
	}
	// The serialized owner joins this lookup before Stop releases its attachment.
	// ctx-none: frozen Invocation ports have no context for a bounded lookup.
	ctx, cancel := context.WithTimeout(context.Background(), min(remaining, 5*time.Second))
	return ctx, cancel, nil
}

func (c *Client) reserveBuffer(chunkSize uint32) error {
	if c.reservation != "" {
		return nil
	}
	reservation, err := c.cfg.Capacity.Reserve(x.Reservation{NonReplayBytes: int64(chunkSize) * 3})
	if err != nil {
		return c.failure(x.CodeCapacityExceeded, err)
	}
	if reservation == "" {
		return c.failure(x.CodeCapacityExceeded, errors.New("capacity returned no reservation identity"))
	}
	c.reservation = reservation
	return nil
}

func (c *Client) acceptChunk(receipt x.ChunkReceipt, data []byte) error {
	if receipt.Index != c.pushIndex || receipt.Offset != c.pushOffset || receipt.Length != uint32(len(data)) ||
		receipt.StreamSeq <= c.pushLastSeq || receipt.SHA256 != sha256.Sum256(data) {
		return c.failure(x.CodeObjectConflict, errors.New("chunk receipt differs from the pending bytes"))
	}
	return nil
}

func (c *Client) pace(bytes int64, deadline x.Deadline) error {
	if c.cfg.Intent.RateLimitBPS == 0 {
		return c.checkBytes(deadline)
	}
	_, now, err := c.cfg.Clock()
	if err != nil {
		return c.failure(x.CodeResultUnknown, err)
	}
	if c.sendStarted == 0 {
		c.sendStarted = now
	}
	rate := c.cfg.Intent.RateLimitBPS
	// Quotient/remainder avoid multiplying a 2 GiB total by nanoseconds when
	// an operator supplied a very large rate value.
	duration := time.Duration(bytes/rate)*time.Second + time.Duration(float64(bytes%rate)/float64(rate)*float64(time.Second))
	if delay := duration - time.Duration(now-c.sendStarted); delay > 0 {
		if err := c.wait(deadline, delay, true); err != nil {
			return err
		}
	}
	return c.checkBytes(deadline)
}

// prepareInline retains the exact bytes under capacity before a prepare can
// escape. Reentry reuses this copy even if the source pathname is replaced.
// origin: hot-update S7 plan §8.1 / INTEG-D
func (c *Client) prepareInline(deadline x.Deadline) error {
	if c.inline != nil {
		return nil
	}
	if c.cfg.Intent.Size > proto.XferTierAMaxBytes {
		return c.failure(x.CodePayloadUnsupported, nil)
	}
	if err := c.reserveBuffer(uint32(max(c.cfg.Intent.Size, 32*1024))); err != nil {
		return err
	}
	info, err := c.cfg.Source.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != c.cfg.Intent.Size {
		return c.failure(x.CodeSourceChanged, err)
	}
	c.sourceInfo = info
	if err := c.verifySource(deadline); err != nil {
		return err
	}
	data := make([]byte, int(c.cfg.Intent.Size))
	if _, err = io.ReadFull(io.NewSectionReader(c.cfg.Source, 0, c.cfg.Intent.Size), data); err != nil {
		return c.failure(x.CodeSourceChanged, err)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != c.cfg.Intent.SHA256 {
		return c.failure(x.CodeSourceChanged, nil)
	}
	if err = c.pace(int64(len(data)), deadline); err != nil {
		return err
	}
	c.inline = data
	return nil
}

func pushInline(c *Client, deadline x.Deadline) error {
	if err := c.checkBytes(deadline); err != nil {
		return err
	}
	// The broker reserves a receiver Grant only for a sealed source. The
	// inline seal names exactly the retained prepared bytes and, being Tier A,
	// carries no OBJ geometry; it is published once, like a Tier B seal.
	// origin: hot-update S7 plan §8.1 / INTEG-D Tier A inline seal
	if !c.sourcePublished {
		fingerprint, err := sourceFingerprint(c.sourceInfo)
		if err != nil {
			return c.failure(x.CodeSourceChanged, err)
		}
		seal := x.SourceSeal{Size: int64(len(c.inline)), SHA256: c.cfg.Intent.SHA256, RateLimitBPS: c.cfg.Intent.RateLimitBPS, SourceFingerprint: fingerprint}
		response, err := c.mutate(x.Request{Op: x.OpSource, CallID: c.callID(x.OpSource), Identity: c.identity, Origin: c.origin, Source: &seal}, deadline)
		if err != nil || c.receipt != nil {
			return err
		}
		if response.Status == nil || !response.Status.Budget.SourceSealed {
			return c.failure(x.CodeResultUnknown, errors.New("inline source seal has no durable acknowledgement"))
		}
		c.sourcePublished = true
	}
	_, err := c.mutate(x.Request{Op: x.OpCommit, CallID: c.callID(x.OpCommit), Identity: c.identity, Origin: c.origin}, deadline)
	if err != nil || c.receipt != nil {
		return err
	}
	c.committed = true
	return c.waitTerminal(deadline)
}

// Upload retains its original writer and pending chunk through every retry.
// origin: hot-update S10 plan §0 / LINT-L2 maintidx
func uploadObject(c *Client, deadline x.Deadline) error {
	if err := c.bindPush(deadline); err != nil {
		return err
	}
	if c.writer == nil {
		writer, err := c.cfg.MakeWriter(c.js, *c.spec)
		// Constructors returning a resource with an error still transfer
		// cleanup responsibility. Keep it for Stop instead of leaking it.
		c.writer = writer
		if err != nil {
			return c.failure(x.CodeOwnerRecovering, err)
		}
		if writer == nil {
			return c.failure(x.CodeResultUnknown, errors.New("writer factory returned no writer"))
		}
	}
	c.phase = x.PhaseSending
	if err := appendPushChunks(c, deadline); err != nil {
		return err
	}
	// Detect even an in-place edit to an already acknowledged source prefix.
	// Validation rereads the pinned FD; it never rewinds a byte writer.
	if err := c.verifySource(deadline); err != nil {
		return err
	}
	for {
		if err := c.checkBytes(deadline); err != nil {
			return err
		}
		limit, err := c.attemptDeadline(deadline)
		if err != nil {
			return err
		}
		receipt, err := c.writer.Finalize(limit)
		if err == nil {
			if receipt.Spec != *c.spec || receipt.MetadataSeq == 0 || !validSHA(receipt.MetadataDigest) {
				return c.failure(x.CodeObjectVersionMismatch, errors.New("metadata receipt does not match the original object"))
			}
			c.objectFinalized = true
			break
		}
		if err := c.wait(deadline, 500*time.Millisecond, true); err != nil {
			return err
		}
	}
	return nil
}

func appendPushChunks(c *Client, deadline x.Deadline) error {
	for c.pushOffset < c.spec.Size {
		if err := c.checkBytes(deadline); err != nil {
			return err
		}
		if c.pushPending == nil {
			length := min(int64(c.spec.ChunkSize), c.spec.Size-c.pushOffset)
			data := make([]byte, int(length))
			n, err := c.cfg.Source.ReadAt(data, c.pushOffset)
			if int64(n) != length || err != nil && !errors.Is(err, io.EOF) {
				return c.failure(x.CodeSourceChanged, err)
			}
			c.pushPending = data
		}
		if err := c.pace(c.pushOffset+int64(len(c.pushPending)), deadline); err != nil {
			return err
		}
		limit, err := c.attemptDeadline(deadline)
		if err != nil {
			return err
		}
		receipt, err := c.writer.Append(c.pushIndex, c.pushPending, limit)
		if err != nil {
			// Raw receipt resolution precedes every retry. The immutable
			// pending chunk stays in the original writer and source cursor.
			// origin: hot-update S7 plan §2.2 / §7 / M01
			for {
				limit, limitErr := c.attemptDeadline(deadline)
				if limitErr != nil {
					return limitErr
				}
				resolved, accepted, resolveErr := c.writer.ResolvePending(limit)
				if resolveErr == nil {
					if accepted {
						receipt, err = resolved, nil
					}
					break
				}
				if waitErr := c.wait(deadline, 500*time.Millisecond, true); waitErr != nil {
					return waitErr
				}
			}
			if err != nil {
				if waitErr := c.wait(deadline, 500*time.Millisecond, true); waitErr != nil {
					return waitErr
				}
				continue
			}
		}
		if err := c.acceptChunk(receipt, c.pushPending); err != nil {
			return err
		}
		c.pushOffset += int64(len(c.pushPending))
		c.pushIndex++
		c.pushLastSeq = receipt.StreamSeq
		c.pushPending = nil
	}
	return nil
}
