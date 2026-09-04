package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// cluster_unlock.go (R7b) — the explicit stale-membership-lock clear.
//
// WHY A VERB EXISTS AT ALL WHEN THERE IS ALREADY A TTL
// -----------------------------------------------------
// The lease makes an abandoned lock decay on its own, which turns a permanent outage into a bounded one.
// It does not make it a NON-outage. Two cases need a human-speed out:
//
//	(1) IMPATIENCE, legitimately. LockLeaseTTL is 15 min by derivation (lock_lease.go), and it is sized for
//	    SAFETY — long enough that a live-but-briefly-leaderless holder is never robbed. An operator whose
//	    roll just HALTed and who knows perfectly well that nothing is running should not have to sit out a
//	    conservative timer before the next `cluster join`. Telling them "wait 15 minutes" is how operators
//	    learn to hand-edit replicated SQLite rows, which is the failure mode this whole batch is about.
//	(2) A LOCK THAT WILL NEVER EXPIRE. A marker acquired by a pre-R7b broker carries no lease row, and the
//	    reaper is fail-closed on exactly that (it must be: an old broker's marker VALUE is an acquire time,
//	    i.e. always in the past, so a reaper that guessed would expire live locks instantly). Those markers
//	    do not decay. Ever. Without this verb the only remedy is the hand-edit.
//
// THE SAFETY THIS VERB MUST NOT GIVE AWAY
// ---------------------------------------
// Ripping out a LIVE lock re-admits the concurrent membership change the lock exists to fence. So the
// default refuses to touch a lock whose lease is still in the future — somebody IS renewing it, right now,
// which is a fact and not a guess. `--force` overrides, because an operator who can see the other terminal
// is dead knows something the cluster does not; it is deliberately not the default and it says so loudly.
//
// rc DISCIPLINE
// -------------
// rc=0 only when every requested lock is CONFIRMED gone on a re-probe. A refusal (live lease, no seed, no
// leader) and an unconfirmed clear are both non-zero: this verb's entire purpose is to leave the operator
// certain about membership, and "probably cleared" is not certainty.

// unlockConfirmAttempts / unlockConfirmDelay bound the post-clear re-probe. The clear is a raft Propose
// that has already COMMITTED by the time the trigger replies OK, but the health probe answers from each
// broker's applied state, so a follower can lag it by an apply. A few hundred ms covers that without
// turning a genuinely-failed clear into a long hang.
const (
	unlockConfirmAttempts = 6
	unlockConfirmDelay    = 250 * time.Millisecond
)

// lockView is one membership lock's state as corroborated across every replying broker.
type lockView struct {
	name   string    // "upgrade" | "grow"
	held   bool      // ANY broker still advertises the marker
	expiry time.Time // the LATEST lease expiry any broker reported (zero ⇒ none reported)
	leased bool      // some broker reported a lease
}

// live reports whether somebody is still renewing. Fail-closed: a lock whose lease is in the future is
// live, and the LATEST expiry across brokers is used, so a lagging follower's stale row can never make a
// live lock look dead.
func (v lockView) live(now time.Time) bool { return v.leased && now.Before(v.expiry) }

// probeLocks corroborates both locks across every broker that answers the health probe,
// and returns HOW MANY brokers answered. The responder count is load-bearing (external
// review H3/H-4): probeClusterHealth collapses subscribe/publish/503/timeout/malformed
// into an empty slice, so zero replies is indistinguishable from "cluster unreachable"
// unless the caller sees the count. A verb that returns rc=0 only when membership is
// CONFIRMED unfenced must not read "nobody answered" as "no locks held".
func probeLocks(nc *nats.Conn, actor string) (upgrade, grow lockView, responders int) {
	upgrade.name, grow.name = "upgrade", "grow"
	// A probe that could not be MADE reports zero responders, which this verb already
	// treats as "membership not confirmed unfenced" and refuses to return rc=0 for. That
	// is the correct handling, and it is now reached for the right reason rather than by
	// the collapse the doc comment above describes.
	replies, err := probeClusterHealth(nc, actor)
	if err != nil {
		return upgrade, grow, 0
	}
	responders = len(replies)
	for _, h := range replies {
		if h.UpgradeLockActive {
			upgrade.held = true
		}
		if h.GrowLockActive {
			grow.held = true
		}
		mergeLease(&upgrade, h.UpgradeLeaseExpiry)
		mergeLease(&grow, h.GrowLeaseExpiry)
	}
	return upgrade, grow, responders
}

// mergeLease folds one broker's reported expiry into the view, keeping the LATEST (most conservative).
// An unparseable or empty value contributes nothing rather than being treated as "expired".
func mergeLease(v *lockView, raw string) {
	if raw == "" {
		return
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return
	}
	if !v.leased || t.After(v.expiry) {
		v.expiry, v.leased = t, true
	}
}

func newClusterUnlockCmd() *cobra.Command {
	var (
		natsURL, home, seedPath          string
		doUpgrade, doGrow, force, dryRun bool
	)
	cmd := &cobra.Command{
		Use:   "unlock",
		Short: "Clear a stale membership lock (roll lock / grow lock) left by an abandoned operation",
		Example: "  tether cluster unlock                                                        # report + clear anything already abandoned\n" +
			"  tether cluster unlock --upgrade --account-seed /etc/tether/secrets/account.nk\n" +
			"  tether cluster unlock --grow --force --account-seed <path>                  # DANGER: rip out a lock somebody is still renewing",
		Long: `Clears the cluster-scoped membership locks that ` + "`cluster upgrade`" + ` (roll lock) and
` + "`cluster add`" + ` (grow lock) take. While either is held, join/retire/upgrade are all refused.

Both locks carry a LEASE that their orchestrator renews while it is running, so an
abandoned lock releases itself within about ` + cluster.LockLeaseTTL.String() + ` on its own. This verb is for
the two cases where waiting is not good enough: an operator who KNOWS the operation is
dead and does not want to wait out a deliberately-conservative timer, and a lock taken
by a broker released before leases existed — which carries no lease and therefore never
expires at all.

By default a lock whose lease is still being renewed is REFUSED, because clearing it
would drop the mutex under a running operation and let a second membership change
interleave with it. --force overrides that; only use it when you can see the other
orchestrator is gone.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// No selector ⇒ act on BOTH. An operator who just wants "unwedge membership" should not have
			// to know which of the two verbs left the lock behind.
			if !doUpgrade && !doGrow {
				doUpgrade, doGrow = true, true
			}
			sid := cli.ReadCurrentSession(home)
			if sid == "" {
				return fmt.Errorf("no active session — run `tether login -s <sid>` first")
			}
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := connectCtl(cmd, "cluster unlock", home, natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return err
			}
			defer nc.Close()

			var seed []byte
			if seedPath != "" {
				if seed, err = os.ReadFile(seedPath); err != nil {
					return unavailErr("read account seed: %v", err)
				}
			}
			return runClusterUnlock(cmd.Context(), nc, id.PublicKey, seed, cmd.OutOrStdout(),
				time.Now(), doUpgrade, doGrow, force, dryRun)
		},
	}
	cmd.Flags().StringVar(&natsURL, "nats-url", "", "broker NATS URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().StringVar(&seedPath, "account-seed", "", "cluster account seed (signs the release; required unless --dry-run)")
	cmd.Flags().BoolVar(&doUpgrade, "upgrade", false, "act on the `cluster upgrade` roll lock only")
	cmd.Flags().BoolVar(&doGrow, "grow", false, "act on the `cluster add` grow lock only")
	cmd.Flags().BoolVar(&force, "force", false, "DANGER: clear even a lock whose lease is still being renewed")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "report what would be cleared and touch nothing")
	return cmd
}

// runClusterUnlock is the whole verb, taking `now` and the connection explicitly so it is hermetically
// testable against a fake broker.
func runClusterUnlock(ctx context.Context, nc *nats.Conn, actor string, seed []byte, out io.Writer,
	now time.Time, doUpgrade, doGrow, force, dryRun bool) error {

	upgradeLock, growLock, responders := probeLocks(nc, actor)
	// H3/H-4: fail CLOSED on zero responders. An empty health set is "no broker answered"
	// (cluster unreachable / leaderless), NOT "no locks held". Reporting rc=0 here — the
	// exact moment this verb is reached for during an outage — would tell automation that
	// membership is unfenced when we have NO evidence either way. Enforced before the
	// dry-run and held==0 paths so neither can leak a false all-clear.
	if responders == 0 {
		return unavailErr("cluster unlock: no broker answered the cluster-health probe — the membership lock state is " +
			"UNKNOWN (the cluster may be unreachable or leaderless). Refusing to report 'nothing held' without evidence; " +
			"check `tether cluster status` and retry once a broker is reachable")
	}
	var targets []lockView
	if doUpgrade {
		targets = append(targets, upgradeLock)
	}
	if doGrow {
		targets = append(targets, growLock)
	}

	held := 0
	for _, v := range targets {
		if !v.held {
			_, _ = fmt.Fprintf(out, "  %s lock: not held\n", v.name)
			continue
		}
		held++
		_, _ = fmt.Fprintf(out, "  %s lock: HELD — %s\n", v.name, describeLease(v, now))
	}
	if held == 0 {
		_, _ = fmt.Fprintln(out, "no membership locks are held — nothing to clear.")
		return nil
	}

	// REFUSE anything still being renewed, before touching a thing. Reported as one refusal covering every
	// live lock so an operator does not clear one and then get stopped on the next.
	if !force {
		for _, v := range targets {
			if v.held && v.live(now) {
				return usageErr("cluster unlock: the %s lock's lease is still being renewed (expires %s, in %s) — "+
					"an orchestrator IS running and clearing the lock now would let a second membership change "+
					"interleave with it. Wait for it to finish or die (the lock then clears itself within ~%s), "+
					"or pass --force if you can confirm that orchestrator is gone",
					v.name, v.expiry.UTC().Format(time.RFC3339), v.expiry.Sub(now).Truncate(time.Second), cluster.LockLeaseTTL)
			}
		}
	}
	if dryRun {
		_, _ = fmt.Fprintln(out, "(dry-run — nothing was touched)")
		return nil
	}
	if seed == nil {
		return usageErr("cluster unlock: --account-seed <path> is required to execute (the cluster account seed signs the release trigger)")
	}

	for _, v := range targets {
		if !v.held {
			continue
		}
		if v.live(now) { // reachable only under --force
			_, _ = fmt.Fprintf(out, "  ⚠ --force: clearing the %s lock even though its lease is LIVE until %s\n",
				v.name, v.expiry.UTC().Format(time.RFC3339))
		}
		leader, lerr := currentLeader(ctx, nc, actor)
		if lerr != nil {
			return unavailErr("cluster unlock: the %s lock is held but no writable leader is visible to clear it (%v) — "+
				"membership stays fenced until it is released", v.name, lerr)
		}
		if err := sendUnlockTrigger(ctx, nc, actor, seed, leader, v); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(out, "  cleared the %s lock.\n", v.name)
	}

	// CONFIRM. rc=0 must mean "membership is unfenced", not "the trigger said OK".
	return confirmUnlocked(ctx, nc, actor, out, doUpgrade, doGrow)
}

// sendUnlockTrigger issues the EXISTING release-lock trigger for one lock — the same signed path
// `cluster upgrade` and `cluster add` use on their own clean-completion release. This verb adds no new
// broker-side command; it only reaches the existing idempotent one at a moment the orchestrators cannot.
func sendUnlockTrigger(ctx context.Context, nc *nats.Conn, actor string, seed []byte, leader string, v lockView) error {
	switch v.name {
	case "upgrade":
		resp, err := sendUpgradeTrigger(ctx, nc, actor, seed, &proto.ClusterUpgradeReq{Op: "release-lock", TargetNode: leader})
		if err != nil || resp == nil || !resp.OK {
			detail := "no reply"
			if resp != nil {
				detail = resp.Code + " " + resp.Error
			}
			return unavailErr("cluster unlock: releasing the upgrade lock did NOT confirm (leader=%s): %s — membership stays fenced",
				leader, triggerDetail(err, detail))
		}
	case "grow":
		// JoinerNode is deliberately EMPTY, which makes PlanClearGrowActive's clear unconditional.
		//
		// The orchestrators' own P9 release is value-predicated on the joiner they are growing, and must be:
		// it fires on a normal completion, where wiping a DIFFERENT joiner's live marker would drop that
		// grow's mutex. This verb is the opposite situation. The operator has been shown the lock's lease
		// state and is explicitly asking for it to go, and the health reply does not carry the marker's
		// joiner id, so predicating on a joiner we would have to guess would produce a silent no-op that
		// still reports OK — the "cleared it, membership is still fenced" outcome this verb exists to
		// eliminate. The live-lease refusal above is what protects an in-flight grow; the confirm re-probe
		// is what makes the clear honest.
		resp, err := sendGrowTrigger(ctx, nc, actor, seed, &proto.ClusterGrowReq{Op: "release-lock", TargetNode: leader})
		if err != nil || resp == nil || !resp.OK {
			detail := "no reply"
			if resp != nil {
				detail = resp.Code + " " + resp.Error
			}
			return unavailErr("cluster unlock: releasing the grow lock did NOT confirm (leader=%s): %s — membership stays fenced",
				leader, triggerDetail(err, detail))
		}
	}
	return nil
}

// confirmUnlocked re-probes until every requested lock reads clear, and FAILS if one does not.
func confirmUnlocked(ctx context.Context, nc *nats.Conn, actor string, out io.Writer, doUpgrade, doGrow bool) error {
	var upgradeLock, growLock lockView
	var responders int
	for attempt := 0; attempt < unlockConfirmAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(unlockConfirmDelay):
			}
		}
		upgradeLock, growLock, responders = probeLocks(nc, actor)
		// H3/H-4: a zero-responder confirm probe is NOT a confirmation — "the lock reads
		// clear" must come from a broker that actually answered, never from silence. Keep
		// retrying; if every attempt is silent we fall through to the fail-closed error.
		if responders == 0 {
			continue
		}
		if (!doUpgrade || !upgradeLock.held) && (!doGrow || !growLock.held) {
			_, _ = fmt.Fprintln(out, "membership locks cleared — `cluster join`/`cluster retire`/`cluster upgrade` are unblocked.")
			return nil
		}
	}
	if responders == 0 {
		return unavailErr("cluster unlock: the release was accepted but NO broker answered the confirm probe after %s — "+
			"the membership lock state is UNKNOWN; re-run and check the leader with `tether cluster status`",
			time.Duration(unlockConfirmAttempts-1)*unlockConfirmDelay)
	}
	var still []string
	if doUpgrade && upgradeLock.held {
		still = append(still, "upgrade")
	}
	if doGrow && growLock.held {
		still = append(still, "grow")
	}
	return unavailErr("cluster unlock: the release was accepted but %v lock(s) are STILL advertised as held after %s — "+
		"membership remains fenced; re-run, and check the leader with `tether cluster status`",
		still, time.Duration(unlockConfirmAttempts-1)*unlockConfirmDelay)
}

// describeLease renders the lease state an operator needs to decide what to do.
func describeLease(v lockView, now time.Time) string {
	switch {
	case !v.leased:
		return "NO LEASE (acquired by a broker released before leases existed) — this lock will NEVER expire on its own; clearing it here is the only remedy"
	case now.Before(v.expiry):
		return fmt.Sprintf("lease LIVE until %s (%s from now) — an orchestrator is still renewing it",
			v.expiry.UTC().Format(time.RFC3339), v.expiry.Sub(now).Truncate(time.Second))
	default:
		return fmt.Sprintf("lease EXPIRED at %s (%s ago) — nobody is renewing; the leader's reaper would also clear it shortly",
			v.expiry.UTC().Format(time.RFC3339), now.Sub(v.expiry).Truncate(time.Second))
	}
}

// triggerDetail renders whichever of the two failure shapes actually occurred.
func triggerDetail(err error, detail string) string {
	if err != nil {
		return err.Error()
	}
	if detail != "" {
		return detail
	}
	return "no reply / refused"
}
