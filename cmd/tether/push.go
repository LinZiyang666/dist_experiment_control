package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
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

  - Up to 8 MiB        : tier A (inline over NATS, no JetStream needed).
  - 8 MiB – 200 MiB    : tier B (JetStream Object Store; broker must
                         have JetStream enabled).
  - > 200 MiB          : refused; use ` + "`tether expose`" + ` + rsync.

The remote path must be absolute and under one of the agent's
file_transfer.allow_roots (configured in agent.yaml). Symlinks at
the destination are refused; intermediate symlinked dirs are
followed and must still resolve inside an allow_root.
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
	cmd.Flags().BoolVar(&force, "force", false, "overwrite remote destination if it exists")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Minute,
		"upper bound on the whole transfer (tier A: ~30s; tier B: ~5min — keep some slack)")
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
//   1. Read+SHA the local file; bail on > 200 MiB.
//   2. Caps probe; chooseTier.
//   3. Tier A: PushPrepareReq{inline_data} → wait for resp; done.
//   4. Tier B: ObjectStore.Put → push-commit.req → wait for resp.
//      The agent's ev.transfer flow is what writes audit complete.
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
	nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
	if err != nil {
		return connectError("push", natsURL, err)
	}
	defer nc.Close()

	caps, _ := probeCaps(nc, id.PublicKey, sid, 3*time.Second)
	tier, _, err := chooseTier(st.Size(), caps)
	if err != nil {
		return err
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
	data, err := os.ReadFile(abs)
	if err != nil {
		return fmt.Errorf("read local: %w", err)
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
		return fmt.Errorf("push (tier A) refused: code=%s %s", pr.Code, pr.Error)
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
	if _, err := io.Copy(h, f); err != nil {
		_ = f.Close()
		return fmt.Errorf("sha local: %w", err)
	}
	_ = f.Close()
	sha := hex.EncodeToString(h.Sum(nil))

	// Step 1: PushPrepareReq with Tier=b. Broker creates bucket and
	// forwards; agent validates path + replies OK.
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
		return fmt.Errorf("push (tier B) refused at prepare: code=%s %s", pr.Code, pr.Error)
	}

	// Step 2: ObjectStore.Put into bucket xfer-<sid>-<transferID>.
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("push (tier B): jetstream new: %w", err)
	}
	bucket := "xfer-" + sid + "-" + transferID
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
	if _, err := store.Put(putCtx, jetstream.ObjectMeta{Name: "object"}, uploadFile); err != nil {
		return fmt.Errorf("push (tier B): Put: %w", err)
	}

	// Step 3: push-commit. Agent Gets, verifies, emits ev.transfer.
	body, _ = json.Marshal(proto.TransferCommitReq{
		TransferID: transferID, Bucket: bucket, ObjectKey: "object",
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
		return fmt.Errorf("push (tier B) commit refused: code=%s %s", cr.Code, cr.Error)
	}

	// Wait for ev.transfer.<id>.{complete,failed}. We already have a
	// member sub allow on ev.>; just subscribe + wait.
	evSub, err := nc.SubscribeSync(proto.SubjEvTransfer(sid, spec.Node, transferID, "*"))
	if err != nil {
		// Subscribe wildcard not allowed by ACL? Fall back to two subs.
		evSub = nil
	}
	if evSub != nil {
		defer func() { _ = evSub.Unsubscribe() }()
		waitCtx, waitCancel := context.WithTimeout(cmd.Context(), timeout)
		defer waitCancel()
		msg, err := evSub.NextMsgWithContext(waitCtx)
		if err == nil {
			var ev proto.TransferEvent
			if json.Unmarshal(msg.Data, &ev) == nil {
				if ev.Kind == "failed" {
					return fmt.Errorf("push (tier B) failed: code=%s %s", ev.Code, ev.Error)
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
