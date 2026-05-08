package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

func newExecCmd() *cobra.Command {
	var (
		natsURL string
		home    string
		nidFlag string
		cwd     string
		timeout time.Duration
	)
	cmd := &cobra.Command{
		Use:   "exec <node> <argv...>",
		Short: "Run an argv on a node, stream stdout/stderr, propagate exit code",
		Long: `tether exec — non-interactive remote command (P4).

The command runs on <node> in the active session (TETHER_SESSION env or
current_session file). stdout/stderr stream back as they arrive; the
exit code of the remote process becomes the local exit code.

PTY mode (vim, htop, progress bars with cursor moves) lands in P5 as
'tether run'.
`,
		Args: cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			sid := cli.ReadCurrentSession(home)
			if sid == "" {
				return fmt.Errorf("no active session — run `tether login -s <sid>` first")
			}
			nid := args[0]
			argv := args[1:]
			_ = nidFlag // reserved (future: --node override)

			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := cli.ConnectNATSWithNkey(natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return fmt.Errorf("exec: connect: %w", err)
			}
			defer nc.Close()

			body, err := json.Marshal(proto.ExecReq{Argv: argv, Cwd: cwd})
			if err != nil {
				return err
			}

			// Streaming reply: subscribe to a fresh inbox, then publish
			// the request with that inbox as Reply. Read chunks until we
			// see "exit" (success path) or "error" (broker / agent reject).
			inbox := nc.NewInbox()
			sub, err := nc.SubscribeSync(inbox)
			if err != nil {
				return err
			}
			defer func() { _ = sub.Unsubscribe() }()

			subject := proto.SubjCmdBy(sid, id.PublicKey, nid, "exec")
			if err := nc.PublishRequest(subject, inbox, body); err != nil {
				return fmt.Errorf("exec: publish: %w", err)
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()

			out := cmd.OutOrStdout()
			errOut := cmd.ErrOrStderr()

			for {
				msg, err := sub.NextMsgWithContext(ctx)
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						return fmt.Errorf("exec: timed out after %s waiting for chunk", timeout)
					}
					return fmt.Errorf("exec: receive: %w", err)
				}
				var chunk proto.ExecChunk
				if err := json.Unmarshal(msg.Data, &chunk); err != nil {
					return fmt.Errorf("exec: malformed chunk: %w", err)
				}
				switch chunk.Kind {
				case "started":
					// Optional. Some users want to know the assigned PID.
				case "stdout":
					_, _ = out.Write(chunk.Data)
				case "stderr":
					_, _ = errOut.Write(chunk.Data)
				case "exit":
					// Propagate remote exit code as our own. cobra's RunE
					// returning nil keeps exit 0; for non-zero we use os.Exit
					// here so we don't print cobra's own error wrapper.
					if chunk.ExitCode != 0 {
						os.Exit(chunk.ExitCode)
					}
					return nil
				case "error":
					return fmt.Errorf("exec: %s", chunk.Error)
				default:
					// Forward-compat: ignore unknown chunk kinds.
				}
			}
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().StringVar(&nidFlag, "node", "", "(reserved; first positional arg is the node)")
	cmd.Flags().StringVar(&cwd, "cwd", "", "working directory on the agent (default: agent's)")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Minute, "max time to wait for output / exit")
	return cmd
}
