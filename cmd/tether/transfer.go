// File transfer (P11) CLI: `tether push <local> <node>:<remote>` and
// `tether pull <node>:<remote> <local>`. See file-transfer-plan v0.2.0.
//
// One file holds both verbs because they share the parser, the caps
// probe, the tier chooser, and the SHA / write-atomic helpers — the
// previous push.go / pull.go / transfer_shared.go split inflated file
// count without separating concerns.
//
// S7: both verbs run only through the invocation engine (cli.RunTransferCommand):
// the capable engine writes the fixed object version, and an explicitly
// negotiated cap0 peer gets the one-shot legacy owner. The pre-S7 direct
// flow (runPush/pushTierB/runPull) retried SDK Put with a rewound source —
// exactly the "Put retry pretending to be resume" that L1 forbids — and was
// no longer reachable from any command, so it was removed rather than kept
// as a second, untested-in-production transfer graph.
// origin: hot-update S7 plan L1 / §7 / T49 / M01

package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xferclient"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

func newPushCmd() *cobra.Command { return newPushCmdWithPorts(transferCLI) }

func newPushCmdWithPorts(ports transferCommandPorts) *cobra.Command {
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

  - tier A (inline over NATS): up to HALF the negotiated max_payload
    minus 4 KiB for the authenticated carrier, capped at 8 MiB. With
    max_payload of 1 MiB that is 508 KiB. The full 8 MiB requires
    max_payload of at least 16 MiB + 8 KiB (16785408 bytes).
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
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.ExactArgs(2)(cmd, args); err != nil {
				return usageErr("transfer: %v", err)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			localPath := args[0]
			spec, err := parseRemoteSpec(args[1])
			if err != nil {
				return usageErr("remote path: %v", err)
			}
			return runTransferCommand(cmd, ports, home, natsURL, "push", localPath, spec, force, timeout)
		},
	}
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error { return usageErr("transfer flags: %v", err) })
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().Int64("rate-limit", 0, "maximum transfer bytes per second (0: default; refused if the total budget exceeds 2108s)")
	cmd.Flags().Bool("ack-alerts", false, "proceed despite an active severe cluster alert (quorum_lost / force_single_active)")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite remote destination if it exists")
	cmd.Flags().DurationVar(&timeout, "timeout", cliTransferTimeoutDefault,
		"upper bound on the invocation; capable transfers also use their sealed rate and size budget")
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

// ─── tether pull ──────────────────────────────────────────────────

func newPullCmd() *cobra.Command { return newPullCmdWithPorts(transferCLI) }

func newPullCmdWithPorts(ports transferCommandPorts) *cobra.Command {
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

Capable pulls use JetStream Object Store, up to 2 GiB. A peer explicitly
negotiated as legacy may use inline or Object Store delivery.
The remote path must be absolute; by default any path the agent's user can
read, optionally narrowed by file_transfer.allow_roots. The local path may
be relative.
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.ExactArgs(2)(cmd, args); err != nil {
				return usageErr("transfer: %v", err)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			spec, err := parseRemoteSpec(args[0])
			if err != nil {
				return usageErr("remote path: %v", err)
			}
			localPath := args[1]
			return runTransferCommand(cmd, ports, home, natsURL, "pull", localPath, spec, force, timeout)
		},
	}
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error { return usageErr("transfer flags: %v", err) })
	cmd.Flags().StringVar(&natsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().Int64("rate-limit", 0, "maximum transfer bytes per second (0: default; refused if the total budget exceeds 2108s)")
	cmd.Flags().Bool("ack-alerts", false, "proceed despite an active severe cluster alert (quorum_lost / force_single_active)")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite local destination if it exists")
	// origin: batch-c EXTERNAL review M1; hot-update S7 plan §1.2 / T34.
	// Legacy pull retains its fixed floor. A capable pull seals discovered
	// source size before byte work and tightens the original total deadline;
	// the ctl flag cannot extend either broker budget.
	cmd.Flags().DurationVar(&timeout, "timeout", cliTransferTimeoutDefault,
		"upper bound on the invocation; capable pulls seal the source budget before the first byte (legacy pulls retain their fixed broker budget)")
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

// Legacy probe helpers retain their native test surface; their policy owner
// is shared with CLI transport after the S7 responsibility split.
// origin: hot-update S7 plan §11.4 / INTEG-D
type capsStatus = cli.TransferCapsStatus

const (
	capsUndetermined = cli.TransferCapsUndetermined
	capsRefused      = cli.TransferCapsRefused
	capsOK           = cli.TransferCapsOK
)

type capsProbe cli.TransferCapsProbe

func (p capsProbe) warning() string { return cli.TransferCapsProbe(p).Warning() }

func classifyCapsResp(resp proto.CapsResp, err error) capsProbe {
	return capsProbe(cli.ClassifyTransferCaps(resp, err))
}
func tierAInlineCeiling(max int64, p capsProbe) int64 {
	return cli.TransferInlineCeiling(max, cli.TransferCapsProbe(p))
}
func chooseTier(size int64, p capsProbe, max int64) (string, error) {
	tier, err := cli.ChooseTransferTier(size, cli.TransferCapsProbe(p), max)
	return tier, adaptTransferCommandError(err)
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
	class := brokerCodeExitClass(code)
	// origin: hot-update S7 plan §7 / T34. Transfer DELETING means
	// settlement is pending; exec's pre-admission class remains unchanged.
	if code == xfercontract.CodeSessionDeleting {
		class = exitTransient
	}
	// 74 is presentation-only: an accepted transfer whose result is unresolved
	// is classified HERE, never in the pre-accept broker-code table
	// (TestExitUnsettledIsPresentationOnly), so those codes carry no table entry.
	// origin: hot-update S7 plan §7 / T34; INTEG-D cmd/tether race suite
	switch code {
	case xfercontract.CodeResultUnknown, xfercontract.CodeTerminalConflict, xfercontract.CodeCommitInflight,
		xfercontract.CodeDurabilityUnknown, xfercontract.CodeReconcileRequired:
		class = exitUnsettled
	}
	return &ExitError{Class: class, Code: code, Err: fmt.Errorf(format, args...)}
}

// cli.NewTransferID makes a 16-hex random id. Not a ULID (we don't need
// time ordering for transfers) but identical shape for visual parity
// with the other audit columns.

// transferCommandPorts is the outer composition seam. P00 deliberately defines
// only contracts: production must bind the real P25/P26 owners here, never the
// contractstub witnesses. An unbound composition refuses before network or IO.
// origin: hot-update S7 plan §8.1 / §12.2 P27
// Open supplies authenticated identity, Base and data factories; its close joins
// attachment cleanup. Receiver creates the P05 atomic destination for a pull.
type transferCommandPorts = cli.TransferCommandPorts

var transferCLI = transferCommandPorts{
	Client: xferclient.NewClient, Receiver: cli.NewTransferReceiver, Select: cli.SelectTransferClient,
	Classify: cli.ClassifyTransferError,
	Open: func(cmd *cobra.Command, home, url, sid string) (xferclient.Config, func() error, error) {
		id, err := cli.EnsureIdentity(home)
		if err != nil {
			return xferclient.Config{}, nil, err
		}
		url = cli.ResolveNATSURLFromHome(url, cmd.Flags().Changed("nats-url"), home)
		nc, err := connectCtlOpts(cmd, "transfer", home, url, id, nats.Name(cli.CtlNameForSession(sid)), nats.FlusherTimeout(5*time.Second), nats.ReconnectBufSize(-1))
		if err != nil {
			return xferclient.Config{}, nil, err
		}
		if cmd.Flags().Lookup("ack-alerts") != nil {
			ack, _ := cmd.Flags().GetBool("ack-alerts")
			if err = gateDestructive(nc, id.PublicKey, ack); err != nil {
				nc.Close()
				return xferclient.Config{}, nil, err
			}
		}
		return cli.OwnTransferClient(nc, id, sid)
	},
}

// Thin adapters retain the existing command/test surface after moving the
// invocation owner into cli. The legacy helpers above remain isolated from it.
// origin: hot-update S7 plan §7 / §11.4 / INTEG-D
func newTransferCmd(ports transferCommandPorts) *cobra.Command {
	root := cli.NewTransferStatusCommand(ports)
	for _, cmd := range root.Commands() {
		run, args := cmd.RunE, cmd.Args
		cmd.RunE = func(c *cobra.Command, a []string) error { return adaptTransferCommandError(run(c, a)) }
		cmd.Args = func(c *cobra.Command, a []string) error { return adaptTransferCommandError(args(c, a)) }
		cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error { return usageErr("transfer status flags: %v", err) })
	}
	return root
}
func runTransferCommand(cmd *cobra.Command, ports transferCommandPorts, home, url, verb, path string, spec remoteSpec, force bool, timeout time.Duration) error {
	return adaptTransferCommandError(cli.RunTransferCommand(cmd, ports, home, url, verb, path, cli.RemoteTransferSpec{Node: spec.Node, Path: spec.Path}, force, timeout))
}
func transferLeafError(ports transferCommandPorts, err error) error {
	return adaptTransferCommandError(cli.TransferLeafError(ports, err))
}
func renderTransferStatus(cmd *cobra.Command, s xfercontract.Status, machine bool) error {
	return adaptTransferCommandError(cli.RenderTransferStatus(cmd, s, machine))
}
func adaptTransferCommandError(err error) error {
	var transfer *cli.TransferCommandError
	if errors.As(err, &transfer) {
		return &ExitError{Class: transfer.Class, Code: transfer.Code, Err: err}
	}
	return err
}

func validTransferReceipt(r xfercontract.TerminalReceipt) bool { return cli.ValidTransferReceipt(r) }

func transferWireExitClasses(classes map[string]int) map[string]int {
	return cli.WithTransferWireExitClasses(classes)
}
