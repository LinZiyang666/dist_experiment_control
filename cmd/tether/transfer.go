// File transfer (P11) CLI: `tether push <local> <node>:<remote>` and
// `tether pull <node>:<remote> <local>`. See file-transfer-plan v0.2.0.
//
// One file holds both verbs because they share the parser, the caps
// probe, the tier chooser, and the SHA / write-atomic helpers — the
// previous push.go / pull.go / transfer_shared.go split inflated file
// count without separating concerns.

package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/tokenhash"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"
)

func newPushCmd() *cobra.Command {
	var (
		natsURL string
		home    string
		force   bool
		timeout time.Duration
	)
	cmd := &cobra.Command{
		Use:   "push <local-path> <node>:<remote-path>",
		Short: "Upload a local file to a remote node",
		Long: `tether push — upload a local file to a remote node.

File size limits (file-transfer-plan v0.2.0):

  - tier A (inline over NATS, no JetStream needed): up to HALF the NATS
    server's max_payload minus 1 KiB — NOT a flat 8 MiB. With the stock
    max_payload of 1 MiB that is ~511 KiB. 8 MiB is the DESIGN ceiling,
    reached only where max_payload is raised to 16 MiB + 2 KiB (16779264
    bytes) — at exactly 16 MiB the formula still yields 8387584, a KiB short. This
    command prints the tier it chose for every transfer.
  - tier B (JetStream Object Store; broker must have JetStream enabled):
    everything above the tier-A ceiling, up to 2 GiB. On a default
    broker that is most real files, so tier B is the common path, not
    the exception.
  - > 2 GiB            : refused; use ` + "`tether expose`" + ` + rsync.

The remote path must be absolute. By default it may be any path the
agent's user can reach (the same reach as run/exec); an operator may
optionally narrow push/pull to file_transfer.allow_roots in agent.yaml.
Symlinks at the destination are refused; intermediate symlinked dirs are
followed and (when narrowing is configured) must still resolve inside an
allow_root.
`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			localPath := args[0]
			spec, err := parseRemoteSpec(args[1])
			if err != nil {
				return err
			}
			return runPush(cmd, home, natsURL, localPath, spec, force, timeout)
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().Bool("ack-alerts", false, "proceed despite an active severe cluster alert (quorum_lost / force_single_active)")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite remote destination if it exists")
	cmd.Flags().DurationVar(&timeout, "timeout", cliTransferTimeoutDefault,
		"upper bound on each phase of the transfer (tier A: ~30s; tier B: derived from the file size — "+
			"the default covers the broker's own worst-case budget)")
	cmd.ValidArgsFunction = func(c *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		// First positional is the local path: shell file completion.
		if len(args) == 0 {
			return nil, cobra.ShellCompDirectiveDefault
		}
		// Second positional is the remote spec; pre-`:` we offer node
		// completion. Post-`:` we have nothing helpful (the remote
		// fs is opaque to the local CLI).
		if len(args) == 1 {
			return completePushTarget(c, home, natsURL, toComplete)
		}
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return cmd
}

// completePushTarget handles the `<node>:` completion. When `toComplete`
// has no `:`, we suggest ONLINE nodes followed by `:` (so the user
// types `gpu-01<TAB>` and gets `gpu-01:`). When it already contains
// `:`, we return nothing — we don't have a remote-fs picker.
func completePushTarget(c *cobra.Command, home, natsURL, toComplete string) ([]string, cobra.ShellCompDirective) {
	if i := indexByte(toComplete, ':'); i >= 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	cctx := cli.NewCompletionContext(home, natsURL, c.Flags().Changed("nats-url"))
	t := cli.NewCompletionTransport(home, natsURL, c.Flags().Changed("nats-url"))
	defer t.Close()
	nodes, dir := cli.CompleteOnlineNodes(t, cctx, toComplete)
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n+":")
	}
	return out, dir | cobra.ShellCompDirectiveNoSpace
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// runPush implements the push flow:
//
//  1. Read+SHA the local file; bail on > 2 GiB.
//  2. Caps probe; chooseTier.
//  3. Tier A: PushPrepareReq{inline_data} → wait for resp; done.
//  4. Tier B: ObjectStore.Put → push-commit.req → wait for resp.
//     The agent's ev.transfer flow is what writes audit complete.
func runPush(cmd *cobra.Command, home, natsURL, localPath string, spec remoteSpec, force bool, timeout time.Duration) error {
	sid := cli.ReadCurrentSession(home)
	if sid == "" {
		return fmt.Errorf("no active session — run `tether login -s <sid>` first")
	}
	natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)

	abs, err := filepath.Abs(localPath)
	if err != nil {
		return fmt.Errorf("resolve local path: %w", err)
	}
	st, err := os.Stat(abs)
	if err != nil {
		return fmt.Errorf("local file: %w", err)
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("local file %s: not a regular file", abs)
	}
	if st.Size() > cliMaxBytes {
		return fmt.Errorf("local file %s: size %d > %d (use `tether expose` + rsync)",
			abs, st.Size(), cliMaxBytes)
	}

	id, err := cli.EnsureIdentity(home)
	if err != nil {
		return err
	}
	nc, err := connectCtl(cmd, "push", home, natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
	if err != nil {
		return err
	}
	defer nc.Close()

	ackAlerts, _ := cmd.Flags().GetBool("ack-alerts")
	if gerr := gateDestructive(nc, id.PublicKey, ackAlerts); gerr != nil { // D8b §10.4
		return gerr
	}

	// G67 #67 face B: the probe RESULT is classified, never silently zero-valued. A failed probe used
	// to fall back to proto.CapsResp{} — JetStreamReady=false, MaxPayload=0 — which made "the probe
	// failed" indistinguishable from "the broker has no JetStream", and the CLI then asserted the
	// latter with advice the broker never gave.
	probe := probeCapsClassified(cmd.Context(), nc, id.PublicKey, sid, 3*time.Second)
	tier, err := chooseTier(st.Size(), probe, nc.MaxPayload())
	if err != nil {
		return err
	}
	if note := probe.warning(); note != "" {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "tether push: %s\n", note)
	}

	transferID := newTransferID()
	out := cmd.OutOrStdout()
	_, _ = fmt.Fprintf(out, "tether push: %s -> %s:%s (tier=%s, %d bytes, transfer_id=%s)\n",
		abs, spec.Node, spec.Path, tier, st.Size(), transferID)

	if tier == "a" {
		return pushTierA(cmd, nc, id.PublicKey, sid, spec, transferID, abs, force, timeout)
	}
	return pushTierB(cmd, nc, id.PublicKey, sid, spec, transferID, abs, st.Size(), force, timeout)
}

func pushTierA(cmd *cobra.Command, nc *nats.Conn, actor, sid string, spec remoteSpec,
	transferID, abs string, force bool, timeout time.Duration) error {
	f, err := os.Open(abs)
	if err != nil {
		return fmt.Errorf("read local: %w", err)
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, cliTierAMaxBytes+1))
	if err != nil {
		return fmt.Errorf("read local: %w", err)
	}
	if len(data) > cliTierAMaxBytes {
		return fmt.Errorf("local file grew beyond tier-A limit while reading; retry")
	}
	sha := hexSHA256(data)
	body, _ := json.Marshal(proto.PushPrepareReq{
		TransferID: transferID, Path: spec.Path,
		Size: int64(len(data)), SHA256: sha,
		Force: force, Tier: "a", InlineData: data,
	})
	ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
	defer cancel()
	resp, err := nc.RequestWithContext(ctx,
		proto.SubjCmdBy(sid, actor, spec.Node, "push"), body)
	if err != nil {
		return fmt.Errorf("push (tier A): request: %w", err)
	}
	var pr proto.PushPrepareResp
	if err := json.Unmarshal(resp.Data, &pr); err != nil {
		return fmt.Errorf("push (tier A): parse: %w", err)
	}
	if !pr.OK {
		return transferRefusalErr(pr.Code, "push (tier A) refused: code=%s %s", pr.Code, pr.Error)
	}
	_, _ = fmt.Fprintln(cmd.OutOrStdout(), "tether push: OK (tier A)")
	return nil
}

func pushTierB(cmd *cobra.Command, nc *nats.Conn, actor, sid string, spec remoteSpec,
	transferID, abs string, size int64, force bool, timeout time.Duration) error {
	// Pre-compute sha over the file.
	f, err := os.Open(abs)
	if err != nil {
		return fmt.Errorf("read local: %w", err)
	}
	h := sha256.New()
	hashed, err := io.Copy(h, io.LimitReader(f, cliMaxBytes+1))
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("sha local: %w", err)
	}
	_ = f.Close()
	if hashed != size {
		return fmt.Errorf("local file changed while hashing: initial size=%d hashed=%d; retry", size, hashed)
	}
	sha := hex.EncodeToString(h.Sum(nil))

	// Step 1: PushPrepareReq with Tier=b. Broker ensures the per-session
	// bucket exists and forwards; agent validates path + replies OK.
	body, _ := json.Marshal(proto.PushPrepareReq{
		TransferID: transferID, Path: spec.Path,
		Size: size, SHA256: sha, Force: force, Tier: "b",
	})
	ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
	defer cancel()
	resp, err := nc.RequestWithContext(ctx,
		proto.SubjCmdBy(sid, actor, spec.Node, "push"), body)
	if err != nil {
		return fmt.Errorf("push (tier B prepare): %w", err)
	}
	var pr proto.PushPrepareResp
	if err := json.Unmarshal(resp.Data, &pr); err != nil {
		return fmt.Errorf("push (tier B prepare): parse: %w", err)
	}
	if !pr.OK {
		return transferRefusalErr(pr.Code, "push (tier B) refused at prepare: code=%s %s", pr.Code, pr.Error)
	}

	// Step 2: ObjectStore.Put into the per-session bucket xfer-<sid>,
	// keyed by transferID.
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("push (tier B): jetstream new: %w", err)
	}
	bucket := proto.XferBucketName(sid)
	putCtx, putCancel := context.WithTimeout(cmd.Context(), timeout)
	defer putCancel()
	store, err := js.ObjectStore(putCtx, bucket)
	if err != nil {
		return fmt.Errorf("push (tier B): bind bucket %s: %w", bucket, err)
	}
	uploadFile, err := os.Open(abs)
	if err != nil {
		return fmt.Errorf("push (tier B): reopen local: %w", err)
	}
	defer func() { _ = uploadFile.Close() }()
	info, err := store.Put(putCtx, jetstream.ObjectMeta{Name: transferID},
		io.LimitReader(uploadFile, size+1))
	if err != nil {
		return fmt.Errorf("push (tier B): Put: %w", err)
	}
	if int64(info.Size) != size {
		_ = store.Delete(putCtx, transferID)
		return fmt.Errorf("local file changed while uploading: prepared size=%d uploaded=%d; retry",
			size, info.Size)
	}

	// Subscribe before commit. The agent acks commit before its async Get,
	// but a fast local Object Store can still publish ev.transfer before a
	// post-commit subscription reaches the server.
	evSub, subErr := nc.SubscribeSync(proto.SubjEvTransfer(sid, spec.Node, transferID, "*"))
	if subErr == nil {
		if err := nc.FlushTimeout(2 * time.Second); err != nil {
			_ = evSub.Unsubscribe()
			evSub = nil
		} else {
			defer func() { _ = evSub.Unsubscribe() }()
		}
	}

	// Step 3: push-commit. Agent Gets, verifies, emits ev.transfer.
	body, _ = json.Marshal(proto.TransferCommitReq{
		TransferID: transferID, Bucket: bucket, ObjectKey: transferID,
	})
	commitCtx, commitCancel := context.WithTimeout(cmd.Context(), timeout)
	defer commitCancel()
	resp, err = nc.RequestWithContext(commitCtx,
		proto.SubjCmdBy(sid, actor, spec.Node, "push-commit"), body)
	if err != nil {
		return fmt.Errorf("push (tier B commit): %w", err)
	}
	var cr proto.TransferCommitResp
	if err := json.Unmarshal(resp.Data, &cr); err != nil {
		return fmt.Errorf("push (tier B commit): parse: %w", err)
	}
	if !cr.OK {
		return transferRefusalErr(cr.Code, "push (tier B) commit refused: code=%s %s", cr.Code, cr.Error)
	}

	// Wait for ev.transfer.<id>.{complete,failed}. If subscription was
	// unavailable, retain the historical best-effort commit-acked result.
	if evSub != nil {
		waitCtx, waitCancel := context.WithTimeout(cmd.Context(), timeout)
		defer waitCancel()
		msg, err := evSub.NextMsgWithContext(waitCtx)
		if err == nil {
			var ev proto.TransferEvent
			if json.Unmarshal(msg.Data, &ev) == nil {
				if ev.Kind == "failed" {
					// G67 m14: the SIXTH refusal site (plan §2 IN item 5 counts six). This one reports
					// the transfer's TERMINAL outcome rather than a prepare-time refusal, so it is the
					// one place a failure code arrives after the data plane already ran.
					return transferRefusalErr(ev.Code, "push (tier B) failed: code=%s %s", ev.Code, ev.Error)
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(),
					"tether push: OK (tier B, %d bytes, %dms)\n", ev.Bytes, ev.DurationMs)
				return nil
			}
		}
	}
	// Couldn't subscribe or no event arrived in time — report
	// best-effort success since commit acked.
	_, _ = fmt.Fprintln(cmd.OutOrStdout(), "tether push: commit acked (no ev.transfer received within timeout)")
	return nil
}

// ─── tether pull ──────────────────────────────────────────────────

func newPullCmd() *cobra.Command {
	var (
		natsURL string
		home    string
		force   bool
		timeout time.Duration
	)
	cmd := &cobra.Command{
		Use:   "pull <node>:<remote-path> <local-path>",
		Short: "Download a file from a remote node",
		Long: `tether pull — download a file from a remote node.

Same tier rules as ` + "`tether push`" + ` (see ` + "`tether push --help`" + `).
The remote path must be absolute; by default any path the agent's user can
read, optionally narrowed by file_transfer.allow_roots. The local path may
be relative.
`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			spec, err := parseRemoteSpec(args[0])
			if err != nil {
				return err
			}
			localPath := args[1]
			return runPull(cmd, home, natsURL, spec, localPath, force, timeout)
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().Bool("ack-alerts", false, "proceed despite an active severe cluster alert (quorum_lost / force_single_active)")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite local destination if it exists")
	// origin: batch-c EXTERNAL review M1. This used to say "tier B is derived from the file size",
	// which is true for PUSH and false for PULL: proto.PullPrepareReq carries no size, so both the
	// agent and the broker budget a pull at the fixed floor. Promising the push behaviour here made a
	// large slow pull look protected by the generous CLI default when the server side would end it in
	// five minutes.
	cmd.Flags().DurationVar(&timeout, "timeout", cliTransferTimeoutDefault,
		"upper bound on each phase of the transfer. NOTE: pull is bounded by a FIXED "+
			proto.XferTimeoutTierBFloor.String()+" on the agent and broker (the pull request carries no "+
			"file size, so neither end can derive a budget from it) — raising this flag alone will not "+
			"make a large slow pull succeed; use `tether expose` + rsync for those")
	cmd.ValidArgsFunction = func(c *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return completePushTarget(c, home, natsURL, toComplete)
		}
		if len(args) == 1 {
			return nil, cobra.ShellCompDirectiveDefault
		}
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return cmd
}

func runPull(cmd *cobra.Command, home, natsURL string, spec remoteSpec, localPath string, force bool, timeout time.Duration) error {
	sid := cli.ReadCurrentSession(home)
	if sid == "" {
		return fmt.Errorf("no active session — run `tether login -s <sid>` first")
	}
	natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)

	// Local-path safety: refuse non-absolute? No — relative is fine
	// for pull; user is the one choosing where to write.
	localAbs, err := filepath.Abs(localPath)
	if err != nil {
		return fmt.Errorf("resolve local path: %w", err)
	}
	if !force {
		if _, err := os.Stat(localAbs); err == nil {
			return fmt.Errorf("local path %s already exists; pass --force to overwrite", localAbs)
		}
	}

	id, err := cli.EnsureIdentity(home)
	if err != nil {
		return err
	}
	nc, err := connectCtl(cmd, "pull", home, natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
	if err != nil {
		return err
	}
	defer nc.Close()

	ackAlerts, _ := cmd.Flags().GetBool("ack-alerts")
	if gerr := gateDestructive(nc, id.PublicKey, ackAlerts); gerr != nil { // D8b §10.4
		return gerr
	}

	// G67: same classified probe + the SHARED ceiling helper (the duplicated arithmetic that used to
	// live here silently WIDENED the inline budget to the 8 MiB default whenever the probe failed,
	// because a zero-value CapsResp has MaxPayload=0 and the old code only clamped when it was > 0).
	probe := probeCapsClassified(cmd.Context(), nc, id.PublicKey, sid, 3*time.Second)
	maxInline := tierAInlineCeiling(nc.MaxPayload(), probe)
	if note := probe.warning(); note != "" {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "tether pull: %s\n", note)
	}

	transferID := newTransferID()
	startedAt := time.Now().UTC()
	_, _ = fmt.Fprintf(cmd.OutOrStdout(),
		"tether pull: %s:%s -> %s (transfer_id=%s)\n",
		spec.Node, spec.Path, localAbs, transferID)

	body, _ := json.Marshal(proto.PullPrepareReq{
		TransferID: transferID, Path: spec.Path,
		MaxInline: maxInline, Force: force,
	})
	prepCtx, prepCancel := context.WithTimeout(cmd.Context(), timeout)
	defer prepCancel()
	resp, err := nc.RequestWithContext(prepCtx,
		proto.SubjCmdBy(sid, id.PublicKey, spec.Node, "pull"), body)
	if err != nil {
		return fmt.Errorf("pull prepare: %w", err)
	}
	var pr proto.PullPrepareResp
	if err := json.Unmarshal(resp.Data, &pr); err != nil {
		return fmt.Errorf("pull prepare: parse: %w", err)
	}
	if !pr.OK {
		// Agent-side prepare failures happen after the broker accepted and
		// tracked this transfer, so finalize them for audit + object cleanup.
		// A duplicate transfer ID is rejected before insertion; finalizing that
		// ID would instead claim and terminate the original in-flight pull.
		if pullPrepareFailureNeedsFinalize(pr.Code) {
			_ = sendFinalize(nc, id.PublicKey, sid, transferID, proto.TransferFinalize{
				Kind: "failed", TransferID: transferID,
				Code: pr.Code, Error: pr.Error,
			}, 3*time.Second)
		}
		return transferRefusalErr(pr.Code, "pull refused: code=%s %s", pr.Code, pr.Error)
	}

	if pr.Tier == "a" {
		return finishPullTierA(cmd, nc, id.PublicKey, sid, spec, transferID, localAbs, startedAt, pr, force, timeout)
	}
	return finishPullTierB(cmd, nc, id.PublicKey, sid, spec, transferID, localAbs, startedAt, pr, force, timeout)
}

func finishPullTierA(cmd *cobra.Command, nc *nats.Conn, actor, sid string,
	spec remoteSpec, transferID, localAbs string, startedAt time.Time,
	pr proto.PullPrepareResp, force bool, timeout time.Duration) error {
	if pr.Size < 0 || pr.Size > cliTierAMaxBytes {
		failAndFinalize(nc, actor, sid, transferID, "a", "too_large",
			fmt.Sprintf("inline size=%d exceeds tier-A limit", pr.Size), startedAt)
		return fmt.Errorf("pull (tier A): invalid size %d", pr.Size)
	}
	if int64(len(pr.InlineData)) != pr.Size {
		failAndFinalize(nc, actor, sid, transferID, "a", "size_mismatch",
			fmt.Sprintf("inline_data len=%d vs size=%d", len(pr.InlineData), pr.Size), startedAt)
		return fmt.Errorf("pull (tier A): size mismatch")
	}
	got := hexSHA256(pr.InlineData)
	if got != pr.SHA256 {
		failAndFinalize(nc, actor, sid, transferID, "a", "sha_mismatch",
			fmt.Sprintf("want=%s got=%s", pr.SHA256, got), startedAt)
		return fmt.Errorf("pull (tier A): sha mismatch")
	}
	if err := writeLocalAtomic(localAbs, pr.InlineData, force); err != nil {
		failAndFinalize(nc, actor, sid, transferID, "a", "io_error", err.Error(), startedAt)
		return fmt.Errorf("pull (tier A) write local: %w", err)
	}
	_ = sendFinalize(nc, actor, sid, transferID, proto.TransferFinalize{
		Kind: "complete", TransferID: transferID, Tier: "a",
		Path: localAbs, Bytes: pr.Size,
		DurationMs: time.Since(startedAt).Milliseconds(),
	}, 5*time.Second)
	_, _ = fmt.Fprintf(cmd.OutOrStdout(),
		"tether pull: OK (tier A, %d bytes, %dms)\n", pr.Size, time.Since(startedAt).Milliseconds())
	return nil
}

func finishPullTierB(cmd *cobra.Command, nc *nats.Conn, actor, sid string,
	spec remoteSpec, transferID, localAbs string, startedAt time.Time,
	pr proto.PullPrepareResp, force bool, timeout time.Duration) error {
	if pr.Size < 0 || pr.Size > cliMaxBytes {
		failAndFinalize(nc, actor, sid, transferID, "b", "too_large",
			// origin: batch-c internal review F4 — DERIVED, not hand-copied. See proto.HumanBytes.
			fmt.Sprintf("object size=%d exceeds the %s limit", pr.Size, proto.HumanBytes(cliMaxBytes)), startedAt)
		return fmt.Errorf("pull (tier B): invalid size %d", pr.Size)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		failAndFinalize(nc, actor, sid, transferID, "b", "jetstream_unavailable", err.Error(), startedAt)
		return fmt.Errorf("pull (tier B): jetstream new: %w", err)
	}
	getCtx, getCancel := context.WithTimeout(cmd.Context(), timeout)
	defer getCancel()
	store, err := js.ObjectStore(getCtx, pr.Bucket)
	if err != nil {
		failAndFinalize(nc, actor, sid, transferID, "b", "bucket_unknown", err.Error(), startedAt)
		return fmt.Errorf("pull (tier B): bind bucket: %w", err)
	}
	result, err := store.Get(getCtx, pr.ObjectKey)
	if err != nil {
		failAndFinalize(nc, actor, sid, transferID, "b", "object_get_failed", err.Error(), startedAt)
		return fmt.Errorf("pull (tier B): Get: %w", err)
	}
	defer func() { _ = result.Close() }()

	// Stream into a tmp sibling of localAbs; sha as we go.
	if !force {
		if _, err := os.Stat(localAbs); err == nil {
			failAndFinalize(nc, actor, sid, transferID, "b", "dst_exists",
				fmt.Sprintf("local file %s already exists", localAbs), startedAt)
			return fmt.Errorf("local file %s already exists; pass --force", localAbs)
		}
	}
	tmp := localAbs + ".tmp." + transferID
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		failAndFinalize(nc, actor, sid, transferID, "b", "io_error", err.Error(), startedAt)
		return fmt.Errorf("pull (tier B): open local tmp: %w", err)
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, h), io.LimitReader(result, pr.Size+1))
	if copyErr == nil {
		copyErr = f.Sync()
	}
	// CARRY THE DESTINATION'S MODE — the tier-B sibling of the inline path's fix.
	//
	// origin: prerelease audit round 2, I-F3. L3-F1 landed on writeLocalAtomic only, which
	// is the INLINE path, so `pull --force` of anything over the 8 MiB tier-A ceiling —
	// i.e. every large file, the case the flag exists for — still reset the destination's
	// permission bits to 0600. AFTER the content is written and before Close, so the tmp
	// never sits in the destination directory at a widened mode holding partial content
	// (round 2, J8).
	if copyErr == nil {
		if st, serr := os.Lstat(localAbs); serr == nil && st.Mode().IsRegular() {
			_ = f.Chmod(st.Mode().Perm())
		}
	}
	closeErr := f.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(tmp)
		errMsg := firstErr(copyErr, closeErr).Error()
		failAndFinalize(nc, actor, sid, transferID, "b", "io_error", errMsg, startedAt)
		return fmt.Errorf("pull (tier B): write local: %s", errMsg)
	}
	if n != pr.Size {
		_ = os.Remove(tmp)
		failAndFinalize(nc, actor, sid, transferID, "b", "size_mismatch",
			fmt.Sprintf("want=%d got=%d", pr.Size, n), startedAt)
		return fmt.Errorf("pull (tier B): size mismatch want=%d got=%d", pr.Size, n)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != pr.SHA256 {
		_ = os.Remove(tmp)
		failAndFinalize(nc, actor, sid, transferID, "b", "sha_mismatch",
			fmt.Sprintf("want=%s got=%s", pr.SHA256, got), startedAt)
		return fmt.Errorf("pull (tier B): sha mismatch want=%s got=%s", pr.SHA256, got)
	}
	if err := commitLocalTemp(tmp, localAbs, force); err != nil {
		_ = os.Remove(tmp)
		failAndFinalize(nc, actor, sid, transferID, "b", "io_error", err.Error(), startedAt)
		return fmt.Errorf("pull (tier B): commit local: %w", err)
	}
	_ = sendFinalize(nc, actor, sid, transferID, proto.TransferFinalize{
		Kind: "complete", TransferID: transferID, Tier: "b",
		Bucket: pr.Bucket, Path: localAbs, Bytes: n,
		DurationMs: time.Since(startedAt).Milliseconds(),
	}, 5*time.Second)
	_, _ = fmt.Fprintf(cmd.OutOrStdout(),
		"tether pull: OK (tier B, %d bytes, %dms)\n", n, time.Since(startedAt).Milliseconds())
	return nil
}

// failAndFinalize emits a tier-aware finalize.req{kind:failed} so the
// broker writes audit failed + reaps the bucket. Best-effort; we
// don't return an error from the finalize itself because the original
// caller already has one.
func failAndFinalize(nc *nats.Conn, actor, sid, transferID, tier, code, errMsg string, startedAt time.Time) {
	_ = sendFinalize(nc, actor, sid, transferID, proto.TransferFinalize{
		Kind: "failed", TransferID: transferID, Tier: tier,
		Code: code, Error: errMsg,
		DurationMs: time.Since(startedAt).Milliseconds(),
	}, 3*time.Second)
}

func pullPrepareFailureNeedsFinalize(code string) bool {
	return code != "transfer_id_in_flight"
}

func sendFinalize(nc *nats.Conn, actor, sid, transferID string, fin proto.TransferFinalize, timeout time.Duration) error {
	body, _ := json.Marshal(fin)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	subj := proto.SubjCtrlTransferFinalize(actor, sid, transferID)
	resp, err := nc.RequestWithContext(ctx, subj, body)
	if err != nil {
		return err
	}
	var fr proto.TransferFinalizeResp
	if json.Unmarshal(resp.Data, &fr) == nil && !fr.OK {
		return transferRefusalErr(fr.Code, "finalize refused: code=%s %s", fr.Code, fr.Error)
	}
	return nil
}

func writeLocalAtomic(localAbs string, data []byte, force bool) error {
	tmp := localAbs + ".tmp." + newTransferID()
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	// CARRY THE EXISTING FILE'S PERMISSION BITS, symmetrically with the agent's
	// OpenForWriteAtomic (internal/agent/transfer.go).
	//
	// origin: prerelease audit agent-transfer/L3-F1. The tmp is created 0600 and renamed
	// over the destination, so `pull --force` onto an existing file silently reset its
	// mode — a 0755 script came back non-executable. The operator asked to replace the
	// CONTENT.
	//
	// Masked to 0o777, not 0o7777: setuid/setgid/sticky belong to the file that was
	// there, and re-applying them to bytes that just arrived over the network would hand
	// the sender whatever privilege the old file carried. Best-effort — a filesystem
	// that refuses the chmod is not a reason to lose the transfer. Only reachable with
	// --force: without it commitLocalTemp's link refuses an existing destination.
	// Applied AFTER the content lands (see below), not here — round 2, J8.
	if _, err = f.Write(data); err == nil {
		// Widen to the destination's mode only once the bytes are in, so the tmp never
		// sits in the destination directory at a group- or world-writable mode holding
		// partial content (round 2, J8 — the ordering is hygiene, not a privilege fix:
		// the mode it takes is the one the committed file will have anyway).
		if st, serr := os.Lstat(localAbs); serr == nil && st.Mode().IsRegular() {
			_ = f.Chmod(st.Mode().Perm())
		}
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := commitLocalTemp(tmp, localAbs, force); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// commitLocalTemp atomically installs tmp. Without --force, hard-linking
// is a portable create-if-absent primitive; it closes the Stat+Rename race
// that could overwrite a destination created concurrently.
func commitLocalTemp(tmp, dst string, force bool) error {
	if force {
		return os.Rename(tmp, dst)
	}
	if err := os.Link(tmp, dst); err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("local file %s already exists", dst)
		}
		return err
	}
	_ = os.Remove(tmp)
	return nil
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

// ─── shared helpers (parser, caps probe, tier chooser, hashes) ────

// Tier-A inline ceiling (mirrors broker + agent ceilings). Files
// <= this go inline; > this require JetStream tier B.
// batch C: single source in internal/proto — this end must not offer what the broker will refuse.
const cliTierAMaxBytes = proto.XferTierAMaxBytes

// cliTransferTimeoutDefault must not be TIGHTER than the broker's own worst-case budget, or the ctl
// aborts a transfer the broker is still patiently waiting on and the user reads a client-side timeout
// for a server-side success in progress. The old default was a flat 10 minutes with help text that
// said "tier B: ~5min" — true only while the broker's budget was a fixed 5 minutes. It is an upper
// bound per phase, not a wait, so covering the worst case costs a small transfer nothing.
const cliTransferTimeoutDefault = proto.XferTierBMaxBudget + 2*time.Minute

// cliMaxBytes is the hard upper bound for a single transfer. Past this
// the user is expected to use `tether expose` + rsync (file-transfer-
// plan §Goals). Bumped from 200 MiB → 2 GiB in v0.2.5; per-session JS
// bucket MaxBytes scales accordingly (see internal/broker/transfer.go
// ensureXferBucket).
const cliMaxBytes = proto.XferMaxBytes

// remoteSpec is one parsed `<node>:<remote-path>` argument.
type remoteSpec struct {
	Node string
	Path string
}

// parseRemoteSpec splits "<node>:<path>" into its parts. The leading
// segment up to the FIRST `:` is the node id; everything after is
// the path. We reject empty node, empty path, and a missing colon —
// `tether push foo` (no colon) is almost certainly user error and the
// helpful message is better than guessing.
func parseRemoteSpec(s string) (remoteSpec, error) {
	idx := strings.IndexByte(s, ':')
	if idx <= 0 {
		return remoteSpec{}, fmt.Errorf("missing 'node:' prefix in %q (expected <node>:<path>)", s)
	}
	nid := s[:idx]
	path := s[idx+1:]
	if path == "" {
		return remoteSpec{}, fmt.Errorf("empty remote path in %q", s)
	}
	if err := proto.ValidateNID(nid); err != nil {
		return remoteSpec{}, fmt.Errorf("invalid node id %q: %w", nid, err)
	}
	return remoteSpec{Node: nid, Path: path}, nil
}

// probeCapsCtx issues `ctrl.by.<actor>.s.<sid>.caps.req` and returns the broker's reported
// capabilities. Used by chooseTier so we don't shoot a tier-B request at a broker without JetStream,
// or a tier-A request that exceeds the server max_payload.
//
// #67 face B: its ERROR IS LOAD-BEARING and callers must not discard it. Failing this probe means we
// learned NOTHING; it does not mean the broker has no JetStream. Go through probeCapsClassified,
// which encodes that distinction in the type. The parent context makes Ctrl-C interrupt the probe
// instead of waiting out its timeout.
func probeCapsCtx(parent context.Context, nc *nats.Conn, actor, sid string, timeout time.Duration) (proto.CapsResp, error) {
	body, _ := json.Marshal(proto.CapsReq{})
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	subj := proto.SubjCtrlCaps(actor, sid)
	msg, err := nc.RequestWithContext(ctx, subj, body)
	if err != nil {
		return proto.CapsResp{}, err
	}
	var resp proto.CapsResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return proto.CapsResp{}, fmt.Errorf("caps: parse: %w", err)
	}
	return resp, nil
}

// capsStatus classifies what a caps probe actually established. #67 face B existed because these
// three collapsed into one zero-value CapsResp, making "no answer" indistinguishable from
// "authoritatively: no JetStream".
type capsStatus int

const (
	capsUndetermined capsStatus = iota // transport or parse failure — NO usable answer
	capsRefused                        // the broker answered but declined (OK=false)
	capsOK                             // authoritative
)

type capsProbe struct {
	Status capsStatus
	Resp   proto.CapsResp
	Err    error
}

// warning returns the stderr note for a probe that produced no authoritative answer, or "" when the
// answer was authoritative. It is a WARNING, never an error: the broker is one RPC away and is the
// single adjudicator of whether tier B can be served.
func (p capsProbe) warning() string {
	switch p.Status {
	case capsUndetermined:
		return fmt.Sprintf("warning: could not read broker capabilities (%v); proceeding and letting the broker decide", p.Err)
	case capsRefused:
		return fmt.Sprintf("warning: the broker declined to report its capabilities (%s); that says nothing about JetStream, so proceeding and letting the broker decide", p.Resp.Code)
	case capsOK:
		return "" // the probe answered; nothing to warn about
	default:
		return ""
	}
}

// probeCapsClassified runs the caps probe and CLASSIFIES the outcome instead of discarding the error.
// It deliberately does NOT retry: once a failed probe means "let the broker decide", a retry buys
// nothing but latency on every transfer.
func probeCapsClassified(ctx context.Context, nc *nats.Conn, actor, sid string, timeout time.Duration) capsProbe {
	return classifyCapsResp(probeCapsCtx(ctx, nc, actor, sid, timeout))
}

// classifyCapsResp is the pure half of the probe, split out so the three-way classification is
// directly testable. Internal review B1: with it folded into the RPC wrapper, flipping the
// `!resp.OK` arm to capsOK left every Go test green — and that flip alone reinstates #67 face B,
// because a broker that merely DECLINED to answer (`not_a_member`) would be treated as authoritative
// and chooseTier would refuse with a permanent capability claim the broker never made.
func classifyCapsResp(resp proto.CapsResp, err error) capsProbe {
	switch {
	case err != nil:
		return capsProbe{Status: capsUndetermined, Err: err}
	case !resp.OK:
		return capsProbe{Status: capsRefused, Resp: resp}
	default:
		return capsProbe{Status: capsOK, Resp: resp}
	}
}

// tierAInlineCeiling clamps the inline (tier-A) budget by EVERY measurement actually available and
// NEVER by one that is missing.
//
// #67 face B, second half: the old code clamped only `if caps.MaxPayload > 0`, so a zero-value
// CapsResp from a FAILED probe silently RAISED the ceiling back to the 8 MiB design default and moved
// the tier-A/B boundary without telling anyone. nc.MaxPayload() is ground truth for what THIS client
// may publish and is populated from server INFO on any connected conn, so it is always available;
// the broker's own caps.MaxPayload is honoured only when the probe was authoritative.
func tierAInlineCeiling(connMaxPayload int64, probe capsProbe) int64 {
	ceiling := int64(cliTierAMaxBytes)
	clamp := func(p int64) {
		if p <= 0 {
			return
		}
		half := p/2 - 1024
		if half < 0 {
			half = 0
		}
		if half < ceiling {
			ceiling = half
		}
	}
	clamp(connMaxPayload)
	if probe.Status == capsOK {
		clamp(probe.Resp.MaxPayload)
	}
	return ceiling
}

// chooseTier decides tier A vs B for a given file size and a CLASSIFIED caps probe.
//
//   - size <= the inline ceiling            -> tier A. A failed probe must never block a tier-A
//     transfer, so this is decided first.
//   - authoritative + JetStream ready       -> tier B.
//   - authoritative + JetStream NOT ready   -> refuse. This is the ONLY case where the broker really
//     told us tier B cannot be served, so it is the only case entitled to say so.
//   - no authoritative answer               -> tier B, with a stderr warning. The broker adjudicates
//     (handlePushReq refuses with real prose if it has no JetStream). Refusing locally would mean
//     inventing a second claim — which is exactly what #67 was.
//
// Returns ("a"|"b", err). It used to also return maxInline, which no caller -- production or test --
// ever read.
func chooseTier(size int64, probe capsProbe, connMaxPayload int64) (string, error) {
	if size > cliMaxBytes {
		return "", fmt.Errorf("too_large: file size %d > %d (use `tether expose` + rsync)", size, cliMaxBytes)
	}
	maxInline := tierAInlineCeiling(connMaxPayload, probe)
	if size <= maxInline {
		return "a", nil
	}
	if probe.Status == capsOK && !probe.Resp.JetStreamReady {
		// The max_payload clause is offered ONLY when raising it would actually help — i.e. when the
		// file could travel inline. For a file above the 8 MiB tier-A design ceiling it never can,
		// and offering it (as the pre-#67 message did unconditionally) is misdirection.
		if size <= cliTierAMaxBytes {
			return "", usageErr("jetstream_unavailable: this broker reports JetStream is not available, so tier B cannot be served, and this %d-byte file does not fit the current tier-A inline budget of %d bytes. Raise the broker's nats max_payload to at least %d bytes so this file travels inline as tier A, enable JetStream on the broker (docs/broker-ops.md), or use `tether expose` + rsync",
				size, maxInline, size*2+2048)
		}
		return "", usageErr("jetstream_unavailable: this broker reports JetStream is not available, so tier B (files larger than %d bytes) cannot be served. Enable JetStream on the broker (docs/broker-ops.md), or use `tether expose` + rsync",
			maxInline)
	}
	return "b", nil
}

// transferRefusalErr attaches an EXIT CLASS to a transfer refusal WITHOUT touching its text.
//
// G67 step 6. The transfer refusals deliberately do NOT go through brokerErrorMessage: that renders
// `<verb> failed: <msg> (<code>)`, dropping the literal `code=<X>` token that drills/61-transfer-edges
// greps for (it is GREEN and must stay so), and it also DISCARDS the raw broker error whenever a hint
// exists. So the message is formatted exactly as before and only the class is added — the class is
// what turns `jetstream_not_ready` into exit 75 (retry me) instead of the unclassified 70 (tether
// bug). Codes with no mapping keep 70, which is the pre-G67 behaviour for every one of them.
func transferRefusalErr(code, format string, args ...any) error {
	return &ExitError{Class: brokerCodeExitClass(code), Err: fmt.Errorf(format, args...)}
}

// newTransferID makes a 16-hex random id. Not a ULID (we don't need
// time ordering for transfers) but identical shape for visual parity
// with the other audit columns.
func newTransferID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// hexSHA256 hashes file content for the transfer sha256 check.
//
// Batch-A A11: its doc used to call itself "the canonical" pair while three
// other byte-identical copies existed elsewhere, none of them aware of it or of
// each other. It now delegates to internal/tokenhash, which is the actual
// single implementation. Note this call site hashes FILE CONTENT, not a bearer
// token — same function, different namespace, so it must not be assumed to move
// in lockstep with the token hashes if that scheme ever changes.
func hexSHA256(b []byte) string { return tokenhash.SumBytes(b) }
