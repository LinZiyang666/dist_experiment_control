//go:build linux && hotupdate_integration

package hotupdate_test

// Oracles for the live-cut barrier and the grant-reply drop witness. They read
// producer receipts only; none of them can supply a missing producer record.

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/transferharness"
)

// parseTransferHoldEntry reads the producer's own process/generation/ordinal.
func parseTransferHoldEntry(body string) (pid int, start, gen, ordinal uint64, at int64, err error) {
	f := strings.Fields(body)
	if len(f) != 5 {
		return 0, 0, 0, 0, 0, errors.New("malformed live-cut barrier entry")
	}
	values := make([]uint64, 5)
	for i, field := range f {
		if values[i], err = strconv.ParseUint(field, 10, 64); err != nil || values[i] == 0 {
			return 0, 0, 0, 0, 0, errors.New("live-cut barrier entry lacks the producer identity")
		}
	}
	return int(values[0]), values[1], values[2], values[3], int64(values[4]), nil
}

// transferHoldResumed reads the producer's own wake receipt, stamped with the
// same host BOOTTIME as the harness release.
func transferHoldResumed(path string, releasedNS, enteredNS int64) (int64, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	woke, err := strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
	if err != nil || woke < releasedNS || woke < enteredNS {
		return 0, errors.New("live-cut barrier producer resumed before the harness released it")
	}
	return woke, nil
}

// validateTransferHoldOrder proves the cut began while the producer was parked
// and, unless the cut itself released it for a later phase, ended before it.
func validateTransferHoldOrder(h *transferHold, cutStartNS, cutEndNS int64, releasedInCut bool) error {
	if h.cutEntryRelease {
		return nil
	}
	if h.enteredNS <= 0 || h.enteredNS >= cutStartNS || h.releasedNS <= cutStartNS || h.wokeNS < h.releasedNS {
		return errors.New("live cut did not begin while its boundary producer was parked")
	}
	if !releasedInCut && h.releasedNS < cutEndNS {
		return errors.New("live-cut barrier released its producer before the cut completed")
	}
	return nil
}

// Both single-mode cut routes are refused by the S8 legacy policy until its
// authorized transaction route lands (rv3-leaves F2). Name that dependency
// exactly; every other failure stays an ordinary red with its native output.
// origin: S7 FIX-CUT / hot-update S8 plan §5 / §7 / P15 / P19
func transferBrokerReloadRefused(t *testing.T, rc int, output string, err error) {
	t.Helper()
	if strings.Contains(output, "legacy_quiescence_unproved") {
		t.Fatalf("NOT_RUN pending S8 P15/P19 authorized broker transaction route: `broker upgrade reload` legacy envelope refused before arm: native_rc=%d %s", rc, output)
	}
	t.Fatalf("live transfer broker cut failed: native_rc=%d err=%v %s", rc, err, output)
}

func transferAgentUpgradeRefused(t *testing.T, err error) {
	t.Helper()
	if text := err.Error(); strings.Contains(text, "native rc=75") && strings.Contains(text, "(try_again)") {
		t.Fatalf("NOT_RUN pending S8 P19 authorized agent upgrade transaction route: legacy `node upgrade --url --sha256` deferred rc75: %v", err)
	}
	t.Fatalf("live transfer agent cut failed: %v", err)
}

// The parked witness must name a reply that was actually dropped and a grant
// the tracker durably reserved before it. An ordinary reserve receipt or
// eventual bytes cannot fill it. A pull receiver holds no grant until it
// replays the same logical reserve. A push receiver may already have landed
// with the delivered grant, and a same-store restart loses the ctl's own
// commit reply anyway; there the broker's link attempt itself must have ended
// without any reply after the witness, which a non-dropping edge cannot fake.
// origin: S7 FIX-CUT grant-reply drop witness / hot-update S7 plan §4 / §10.2 / T13
func validateTransferReplyDrop(root, direction string, witness x.Observation, broker []x.Observation, requests map[string]int) error {
	if _, err := os.Stat(filepath.Join(root, "fault-grant-reply.entered")); err != nil {
		return errors.New("grant reply was never dropped at the requester's receive edge")
	}
	if witness.Kind != x.ObserveReserve || witness.GrantNonce == "" || !witness.Attempt.Sent || witness.Attempt.Class != "reply" || witness.Attempt.LogicalID == "" {
		return errors.New("drop witness lacks the requester's sent attempt")
	}
	durable := false
	for _, row := range broker {
		if row.TransferID == witness.TransferID && row.Kind == x.ObserveReserve && row.GrantNonce == witness.GrantNonce &&
			row.Attempt == (x.Attempt{}) && row.Phase == x.PhaseCommitReserved && row.BoottimeNS <= witness.BoottimeNS {
			durable = true
		}
	}
	if !durable {
		return errors.New("dropped reply names no grant the tracker durably reserved before it")
	}
	if direction == "pull" && requests[witness.Attempt.LogicalID] < 2 {
		return errors.New("lost reply was not reconciled by replaying the same logical request")
	}
	if direction == "push" {
		lost := false
		for _, row := range broker {
			a := row.Attempt
			if row.TransferID == witness.TransferID && row.Kind == x.ObserveAttempt && row.GrantNonce == witness.GrantNonce &&
				a.LogicalID == witness.Attempt.LogicalID && a.Sent && a.Class == "reply" && row.BoottimeNS > witness.BoottimeNS {
				lost = true
			}
		}
		if !lost {
			return errors.New("grant link attempt received its reply although the witness claimed it was dropped")
		}
	}
	return nil
}

// A ctl client and a broker tracker each own one deadline per transfer. The
// agent's worker entry and the byte actor it later constructs derive theirs
// from the same durable BudgetSeal at different instants; the frozen
// ReceiveConfig/SendConfig carry no absolute deadline, so cross-owner identity
// is unattainable there. Each owner's own deadline never moves later.
// origin: S7 FIX-CUT / hot-update S7 plan §1.2 / §10.2 / M11
func transferSameDeadlineOwner(a, b x.Observation) bool {
	// The sender leaf names its responsibility rather than its host role.
	// Normalize it before comparing the agent's registry and byte deadlines.
	// origin: S7 internal review CUT-CAPACITY F3
	if a.Leg == "sender" {
		a.Leg = "agent"
	}
	if b.Leg == "sender" {
		b.Leg = "agent"
	}
	return a.Leg != "agent" || b.Leg != "agent" || transferDeadlineOwner(a) == transferDeadlineOwner(b)
}

// transferDeadlineOwner names the agent responsibility a row reports for. The
// worker's own import/install/rollback/freeze rows project the registry entry
// together with the actor it still holds, and carry that actor's deadline, so
// their owner follows the projected actor (Source: sender, Tmp: receiver), not
// the row kind. Classifying them all as "entry" compared a receiver's deadline
// with the entry's, which the receiver derives later from the same seal.
// origin: S7 FIX-CUT / hot-update S7 plan §1.2 / §10.2 / M11
// origin: S7 internal review CUT-CAPACITY F3
func transferDeadlineOwner(o x.Observation) string {
	switch o.Kind {
	case x.ObserveFetched, x.ObserveSynced, x.ObservePrimitiveEnter, x.ObservePrimitiveExit, x.ObserveDirectorySync, x.ObserveReaderStop:
		return "receiver"
	case x.ObserveAppend, x.ObserveResolve, x.ObserveFinalize:
		return "sender"
	case x.ObserveAttempt:
		if o.Source.ResourceID != "" {
			return "sender"
		}
		return "control"
	case x.ObserveCallbackEnter, x.ObserveCallbackJoin, x.ObserveAcknowledge:
		return "courier"
	case x.ObserveImport, x.ObserveInstall, x.ObserveRollback, x.ObserveFreeze:
		if o.Source.ResourceID != "" {
			return "sender"
		}
		if o.Tmp.ResourceID != "" {
			return "receiver"
		}
		return "entry"
	default:
		return "entry"
	}
}

// A sender's durable progress is the stream's ACK of the original object, not
// a receiver offset; a ctl whose bytes rode inline progresses by an
// acknowledged later mutation of the same invocation.
// origin: S7 FIX-CUT / hot-update S7 plan §2.2 / §7 / §10.2
func transferSenderProgress(before, row x.Observation) bool {
	if (row.Kind == x.ObserveResolve || row.Kind == x.ObserveFinalize) && row.ActualACKSeq > 0 && row.Index >= before.Index {
		return true
	}
	a := row.Attempt
	if row.Kind != x.ObserveAttempt || !a.Sent || a.Class != "ok" || a.Phase != "request" ||
		a.LogicalID == before.Attempt.LogicalID && before.Attempt.Class == "ok" {
		return false
	}
	// The agent's sender names the original source operation by entry nonce;
	// ctl mutations use transfer ID. Both ACK the original durable seal.
	// origin: S7 internal review FIX-TIERA2 F2 / hot-update S7 plan §10.2 / T46
	if row.Source.ResourceID != "" && a.LogicalID == "xfer-source-"+row.EntryNonce {
		return true
	}
	for _, op := range []x.Operation{x.OpSource, x.OpReserve, x.OpCommit, x.OpTerminal, x.OpFinalize} {
		if a.LogicalID == row.TransferID+"-"+string(op) {
			return true
		}
	}
	return false
}

// These are oracle controls, not product mutation receipts: synthetic receipts
// prove each predicate rejects an unparked cut or an unproven reply loss.
// origin: S7 FIX-CUT live-cut barrier / grant-reply drop witness / hot-update S7 plan §10.2 / §10.5
func TestTransferLiveCutOraclesRejectUnprovenCuts(t *testing.T) {
	t.Run("producers", func(t *testing.T) {
		for _, layer := range []string{"agent", "broker", "js"} {
			for _, cell := range transferCells(layer) {
				producers := transferWindowProducers(cell)
				roles, cutEntry := transferHoldRoles(cell)
				if len(producers) == 0 || len(roles) == 0 {
					t.Fatalf("%s/%s has no boundary producer", layer, cell.name())
				}
				for _, role := range roles {
					if role == layer && !cutEntry {
						t.Fatalf("%s/%s parks the process it cuts", layer, cell.name())
					}
				}
				if cutEntry && (layer == "js" || strings.Join(roles, ",") != strings.Join(producers, ",")) {
					t.Fatalf("%s/%s claims a cut-entry release without a sole cut producer", layer, cell.name())
				}
				if !strings.Contains(transferCutHoldOverlay, "case \""+cell.window+"\":") {
					t.Fatalf("barrier predicate lacks declared window %s", cell.window)
				}
			}
		}
	})
	t.Run("entry", func(t *testing.T) {
		pid, start, gen, ordinal, at, err := parseTransferHoldEntry("11 22 3 4 55\n")
		if err != nil || pid != 11 || start != 22 || gen != 3 || ordinal != 4 || at != 55 {
			t.Fatalf("valid barrier entry rejected: %v", err)
		}
		for _, bad := range []string{"", "11 22 3 4", "11 22 0 4 55", "11 22 3 x 55", "11 22 3 4 55 6"} {
			if _, _, _, _, _, err := parseTransferHoldEntry(bad); err == nil {
				t.Fatalf("barrier entry without a complete producer identity accepted: %q", bad)
			}
		}
	})
	t.Run("resume", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "woke")
		if _, err := transferHoldResumed(path, 100, 50); err == nil {
			t.Fatal("missing wake receipt accepted")
		}
		transferMust(t, os.WriteFile(path, []byte("150\n"), 0600))
		if woke, err := transferHoldResumed(path, 100, 50); err != nil || woke != 150 {
			t.Fatalf("producer that resumed after release rejected: %d %v", woke, err)
		}
		for _, early := range []string{"90\n", "40\n", "early\n"} {
			transferMust(t, os.WriteFile(path, []byte(early), 0600))
			if _, err := transferHoldResumed(path, 100, 50); err == nil {
				t.Fatalf("producer that resumed before release accepted: %q", early)
			}
		}
	})
	t.Run("order", func(t *testing.T) {
		parked := transferHold{enteredNS: 10, releasedNS: 40, wokeNS: 41}
		transferMust(t, validateTransferHoldOrder(&parked, 20, 30, false))
		for name, tc := range map[string]struct {
			h          transferHold
			start, end int64
			inCut      bool
		}{
			"entered-after-cut-start": {transferHold{enteredNS: 25, releasedNS: 40, wokeNS: 41}, 20, 30, false},
			"released-before-cut":     {transferHold{enteredNS: 10, releasedNS: 15, wokeNS: 16}, 20, 30, true},
			"released-before-cut-end": {transferHold{enteredNS: 10, releasedNS: 25, wokeNS: 26}, 20, 30, false},
			"woke-before-release":     {transferHold{enteredNS: 10, releasedNS: 40, wokeNS: 39}, 20, 30, false},
			"no-entry-receipt-at-all": {transferHold{releasedNS: 40, wokeNS: 41}, 20, 30, false},
		} {
			h := tc.h
			if validateTransferHoldOrder(&h, tc.start, tc.end, tc.inCut) == nil {
				t.Fatalf("%s: cut outside the parked boundary accepted", name)
			}
		}
		later := transferHold{enteredNS: 10, releasedNS: 25, wokeNS: 26}
		transferMust(t, validateTransferHoldOrder(&later, 20, 30, true))
	})
	// Only a successful original ctl in the broker's cleanup window may finish
	// while its boundary is parked or exit inside the parked identity sample.
	t.Run("ctl-completion", func(t *testing.T) {
		for _, layer := range []string{"agent", "broker", "js"} {
			for _, cell := range transferCells(layer) {
				for _, rc := range []int{0, 1, 74, 75} {
					want := cell.window == "cleanup-pending" && rc == 0
					if got := transferCtlMayComplete(cell, transferharness.Receipt{NativeRC: rc}); got != want {
						t.Fatalf("%s/%s rc=%d: ctl completion allowed=%v", layer, cell.name(), rc, got)
					}
				}
			}
		}
	})
	t.Run("reply-drop", func(t *testing.T) {
		root := t.TempDir()
		transferMust(t, os.WriteFile(filepath.Join(root, "fault-grant-reply.entered"), []byte("armed\n"), 0600))
		witness := x.Observation{Kind: x.ObserveReserve, Phase: x.PhaseCommitReserved, TransferID: "id", GrantNonce: "grant", BoottimeNS: 20,
			Attempt: x.Attempt{LogicalID: "id-reserve", Ordinal: 1, Phase: "request", StartedMonoNS: 1, FinishedMonoNS: 2, Sent: true, Class: "reply"}}
		reserved := x.Observation{Kind: x.ObserveReserve, Phase: x.PhaseCommitReserved, TransferID: "id", GrantNonce: "grant", BoottimeNS: 10}
		lost := x.Observation{Kind: x.ObserveAttempt, Phase: x.PhaseCommitReserved, TransferID: "id", GrantNonce: "grant", BoottimeNS: 30,
			Attempt: x.Attempt{LogicalID: "id-reserve", Ordinal: 1, Phase: "request", StartedMonoNS: 1, FinishedMonoNS: 9, Sent: true, Class: "reply"}}
		replayed := map[string]int{"id-reserve": 2}
		transferMust(t, validateTransferReplyDrop(root, "pull", witness, []x.Observation{reserved}, replayed))
		transferMust(t, validateTransferReplyDrop(root, "push", witness, []x.Observation{reserved, lost}, nil))
		type input struct {
			root, direction string
			witness         x.Observation
			broker          []x.Observation
			requests        map[string]int
		}
		cases := map[string]func(*input){
			"never-dropped":    func(in *input) { in.root = t.TempDir() },
			"unsent-witness":   func(in *input) { in.witness.Attempt.Sent = false },
			"ordinary-timeout": func(in *input) { in.witness.Attempt.Class = "timeout" },
			"foreign-grant":    func(in *input) { in.broker[0].GrantNonce = "other" },
			"grant-after-drop": func(in *input) { in.broker[0].BoottimeNS = 30 },
			"pull-never-replayed": func(in *input) {
				in.direction, in.requests = "pull", map[string]int{"id-reserve": 1}
			},
			"push-reply-received":      func(in *input) { in.broker = in.broker[:1] },
			"push-loss-before-witness": func(in *input) { in.broker[1].BoottimeNS = 15 },
			"push-loss-other-call":     func(in *input) { in.broker[1].Attempt.LogicalID = "other" },
		}
		for name, mutate := range cases {
			in := input{root: root, direction: "push", witness: witness, broker: []x.Observation{reserved, lost}, requests: replayed}
			mutate(&in)
			if validateTransferReplyDrop(in.root, in.direction, in.witness, in.broker, in.requests) == nil {
				t.Fatalf("%s: unproven grant-reply loss accepted", name)
			}
		}
	})
}
