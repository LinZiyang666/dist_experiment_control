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
The remote path must be absolute and inside one of the agent's
allow_roots; the local path may be relative.
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
	cmd.Flags().BoolVar(&force, "force", false, "overwrite local destination if it exists")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Minute,
		"upper bound on the whole transfer")
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
	nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
	if err != nil {
		return connectError("pull", natsURL, err)
	}
	defer nc.Close()

	caps, _ := probeCaps(nc, id.PublicKey, sid, 3*time.Second)
	maxInline := int64(cliTierAMaxBytes)
	if caps.MaxPayload > 0 {
		half := caps.MaxPayload/2 - 1024
		if half > 0 && half < maxInline {
			maxInline = half
		}
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
		// Tell broker we failed so audit + bucket cleanup happens.
		_ = sendFinalize(nc, id.PublicKey, sid, transferID, proto.TransferFinalize{
			Kind: "failed", TransferID: transferID,
			Code: pr.Code, Error: pr.Error,
		}, 3*time.Second)
		return fmt.Errorf("pull refused: code=%s %s", pr.Code, pr.Error)
	}

	if pr.Tier == "a" {
		return finishPullTierA(cmd, nc, id.PublicKey, sid, spec, transferID, localAbs, startedAt, pr, force, timeout)
	}
	return finishPullTierB(cmd, nc, id.PublicKey, sid, spec, transferID, localAbs, startedAt, pr, force, timeout)
}

func finishPullTierA(cmd *cobra.Command, nc *nats.Conn, actor, sid string,
	spec remoteSpec, transferID, localAbs string, startedAt time.Time,
	pr proto.PullPrepareResp, force bool, timeout time.Duration) error {
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
	n, copyErr := io.Copy(io.MultiWriter(f, h), result)
	if copyErr == nil {
		copyErr = f.Sync()
	}
	closeErr := f.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(tmp)
		errMsg := firstErr(copyErr, closeErr).Error()
		failAndFinalize(nc, actor, sid, transferID, "b", "io_error", errMsg, startedAt)
		return fmt.Errorf("pull (tier B): write local: %s", errMsg)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != pr.SHA256 {
		_ = os.Remove(tmp)
		failAndFinalize(nc, actor, sid, transferID, "b", "sha_mismatch",
			fmt.Sprintf("want=%s got=%s", pr.SHA256, got), startedAt)
		return fmt.Errorf("pull (tier B): sha mismatch want=%s got=%s", pr.SHA256, got)
	}
	if err := os.Rename(tmp, localAbs); err != nil {
		_ = os.Remove(tmp)
		failAndFinalize(nc, actor, sid, transferID, "b", "io_error", err.Error(), startedAt)
		return fmt.Errorf("pull (tier B): rename local: %w", err)
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
		return fmt.Errorf("finalize refused: code=%s %s", fr.Code, fr.Error)
	}
	return nil
}

func writeLocalAtomic(localAbs string, data []byte, force bool) error {
	if !force {
		if _, err := os.Stat(localAbs); err == nil {
			return fmt.Errorf("local file %s already exists", localAbs)
		}
	}
	tmp := localAbs + ".tmp.pull"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, localAbs); err != nil {
		_ = os.Remove(tmp)
		return err
	}
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
