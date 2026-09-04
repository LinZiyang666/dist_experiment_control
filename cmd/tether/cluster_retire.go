package main

import (
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/spf13/cobra"
)

// cluster_retire.go (C4) — `cluster retire <node> --wait`: a RECOVERABLE retire operation. The F==0
// typed-confirm gate is identical to `cluster drain --retire` (never `--yes`). On a crash/leadership
// change the operation resumes on the new leader with no operator re-run.

func newClusterRetireCmd(socketPath *string) *cobra.Command {
	var wait bool
	var timeout time.Duration
	var compromised, requireRotation bool
	var secretsDir string
	cmd := &cobra.Command{
		Use:   "retire <node-id>",
		Short: "Retire a node as a RECOVERABLE operation (drain + rehome + remove; resumes after a crash)",
		Example: "  tether cluster retire brk-b --wait\n" +
			"  tether cluster retire brk-b --compromised --require-credential-rotation   # guided key rotation",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			node := args[0]
			// C7: --require-credential-rotation needs --compromised (it is the teeth of the advisory flag).
			if requireRotation && !compromised {
				return usageErr("--require-credential-rotation requires --compromised")
			}

			// C7 require path is SEPARATE (C7-ROT-1): the plain retire's first call is op-CREATING for
			// F>0 (Confirmed only matters at the F==0 gate), so the two-call F==0 dance would double-
			// create the op + fail "already in flight" on F>0 → no alert/guide/banner (a #5 false-safe).
			// Require mode therefore: read-only pre-check (fail-fast single mode WITHOUT prompting, M4) →
			// typed-confirm (subsumes the F==0 confirm) → ONE Confirmed:true create → alert + guide + banner.
			if requireRotation {
				if pre, perr := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpClusterStatus}); perr != nil {
					return perr
				} else if pre.Error != "" {
					return clusterAdminError("retire", pre) // e.g. single mode: "cluster mode not enabled"
				}
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
					"WARNING: retire is NOT a credential revocation — %s keeps account.nk + the cluster CA until\n"+
						"  you rotate. You will get a guided rotation checklist + a persistent NOT-SAFE alert.\n", node)
				if !confirmTypedNodeID(cmd, node, "", false, "") {
					return usageErr("aborted (type the node_id to confirm; --yes is not accepted)")
				}
				resp, err := callAdmin(*socketPath, adminsock.Request{Op: adminsock.OpClusterRetire, NodeID: node, Confirmed: true})
				if err != nil {
					return err
				}
				if err := leaderRedirect(cmd, resp); err != nil {
					return err
				}
				if resp.Error != "" {
					return clusterAdminError("retire", resp)
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "retire operation %s created (driving to RETIRED).\n", resp.OpID)
				// Raise the persistent severe NOT-SAFE alert. Mega-audit MAJ-5: a leadership loss between
				// the op commit and this raise would silently drop the durable manual:credrot row, leaving
				// `cluster status`/`alert ls` reading clean = a rejection-#5 false-safe. So (a) bounded-retry
				// the raise, (b) tell the guide the TRUTH about whether it was raised, and (c) exit NON-ZERO
				// on a persistent failure so a monitor/CI never reads "safe restored."
				var raiseErr error
				for attempt := 0; attempt < 3; attempt++ {
					if raiseErr = raiseCredRotationAlert(*socketPath, node); raiseErr == nil {
						break
					}
				}
				if raiseErr != nil {
					_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
						"WARNING: the persistent NOT-SAFE tracking alert was NOT raised (%v). The retire COMPLETED but the\n"+
							"  durable not-safe surface is MISSING — `cluster status` / `alert ls` will NOT reflect this node.\n"+
							"  Manually raise it (or re-run after a leader is elected); do NOT treat %s as rotated until you do.\n", raiseErr, node)
				}
				// N-4c-adjacent (record-only, external review L-3): the printed credential-rotation guide
				// still hardcodes the DEFAULT nats.conf path — on a custom-conf deployment the operator
				// would re-render the wrong file if they copy it verbatim. Like runSelfInit / online-doctor,
				// this is a stock-install-only flow; threading a custom --nats-conf through retire is out of
				// this batch's scope (a leaf follow-up), recorded here rather than silently left.
				printCredentialRotationGuide(cmd.OutOrStdout(), node, secretsDir, defaultNatsConfPath, raiseErr == nil /* alert actually raised */)
				printNotSafeBanner(cmd.ErrOrStderr(), node)
				if wait {
					if werr := waitForOp(cmd, *socketPath, resp.OpID, timeout); werr != nil {
						return werr
					}
				} else {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  watch: tether cluster ops show %s\n", resp.OpID)
				}
				if raiseErr != nil {
					return &ExitError{Class: exitInternal, Err: fmt.Errorf("retire %s completed but the NOT-SAFE tracking alert was not raised: %w", node, raiseErr)}
				}
				return nil
			}

			// --- non-require path: BYTE-IDENTICAL to pre-C7 (the F==0 two-call dance), plus the advisory
			// --compromised guide instead of the plain NOTE. ---
			req := adminsock.Request{Op: adminsock.OpClusterRetire, NodeID: node, Confirmed: false}
			resp, err := callAdmin(*socketPath, req)
			if err != nil {
				return err
			}
			if err := leaderRedirect(cmd, resp); err != nil {
				return err
			}
			// F==0 confirm gate: NEVER honored by --yes; require a typed node_id (same as drain).
			if resp.QuorumProj != nil {
				p := resp.QuorumProj
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
					"WARNING: after this retire the cluster has %d voters, quorum=%d, tolerates %d failures.\n",
					p.Voters, p.Quorum, p.FaultTolerance)
				if !confirmTypedNodeID(cmd, node, "", false, "") {
					return usageErr("aborted (type the node_id to confirm an F==0 retire; --yes is not accepted)")
				}
				req.Confirmed = true
				resp, err = callAdmin(*socketPath, req)
				if err != nil {
					return err
				}
			}
			if resp.Error != "" {
				return clusterAdminError("retire", resp)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "retire operation %s created (driving to RETIRED).\n", resp.OpID)
			if compromised {
				// Advisory: print the guide (no alert raised → raised=false suppresses the alert lines, M2).
				printCredentialRotationGuide(cmd.OutOrStdout(), node, secretsDir, defaultNatsConfPath, false)
			} else {
				// Byte-identical to the pre-C7 plain-retire NOTE.
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
					"NOTE: retire is a TOPOLOGY change, NOT a credential revocation — the retired host still holds\n"+
						"  account.nk + the cluster CA until you rotate keys (docs/cluster-runbook.md §2.1).\n")
			}
			if wait {
				return waitForOp(cmd, *socketPath, resp.OpID, timeout)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  watch: tether cluster ops show %s\n", resp.OpID)
			return nil
		},
	}
	cmd.Flags().BoolVar(&wait, "wait", false, "block until the node reaches RETIRED (or --timeout)")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Minute, "with --wait: give up after this long")
	cmd.Flags().BoolVar(&compromised, "compromised", false, "the node is compromised: after retire, print the guided credential-rotation checklist")
	cmd.Flags().BoolVar(&requireRotation, "require-credential-rotation", false, "with --compromised: typed-confirm + raise a persistent NOT-SAFE alert until you rotate + clear it")
	cmd.Flags().StringVar(&secretsDir, "secrets-dir", defaultClusterSecretsDir, "secrets dir to read PUBLIC fingerprints from (this host only; no private key is read or moved)")
	return cmd
}
