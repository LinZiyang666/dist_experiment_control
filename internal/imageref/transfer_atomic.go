package imageref

import (
	"errors"
	"fmt"

	// Transport-free contract values; see transfer_atomic_unix.go.
	"github.com/LinZiyang666/tether/internal/xfercontract/xferport"
)

// inlineAtomicMaxBytes is the Tier A byte ceiling, independent of filesystem
// or transport owners. Object-backed receivers still require a fixed version.
// origin: hot-update S7 plan §1.2 / §3 / INTEG-D2
// inlineAtomicMaxBytes mirrors proto.XferTierAMaxBytes (8 MiB). transferpath
// cannot import proto without a transport closure; a test pins equality.
// Tier A is the small inline path: an empty object version is never a way to
// commit a larger, object-backed prefix.
// origin: hot-update S7 plan §1.2 / §8.1

const InlineAtomicMaxBytes = 8 << 20

// validAtomicGrant is shape-only: the identity must be complete, and a present
// object version must be a single token. Whether an EMPTY version is admissible
// is a tier decision, made by grantAdmissible against the declared binding.
// Inline tier A has no OBJ version; its same identity-bound Grant still
// authorizes the original filesystem receiver.
// origin: hot-update S7 plan §4 / T12 / INTEG-E inline receiver regression
func ValidAtomicGrant(g xferport.CommitGrant) bool {
	i := g.Identity
	return i.SID != "" && i.NID != "" && i.ActorFP != "" && i.AgentInstance != "" &&
		i.TransferID != "" && i.EntryNonce != "" && i.IntentDigest != "" &&
		(i.ObjectVersion == "" || atomicVersionToken(i.ObjectVersion)) &&
		g.GrantNonce != "" && g.ReceiverInstance != "" && g.TerminalID != ""
}

func atomicVersionToken(s string) bool {
	if len(s) == 0 || len(s) > 128 || s == "." || s == ".." {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' && c != '.' {
			return false
		}
	}
	return true
}

// validateAtomicDecision checks only immutable decision/phase consistency.
// The filesystem owner separately validates every descriptor and fingerprint
// before Take; this value check never opens, repairs or commits a resource.
// origin: hot-update S7 plan §3 / §8.2 / T10
func ValidateAtomicDecision(s xferport.AtomicState) error {
	tmp, parent, source := s.Tmp.ResourceID != "", s.Parent.ResourceID != "", s.Source.ResourceID != ""
	if s.CommitGrant != (xferport.CommitGrant{}) && !ValidAtomicGrant(s.CommitGrant) {
		return errors.New("transferpath: incomplete atomic commit grant")
	}
	// An empty object version is the Tier A inline path; it can never carry an
	// object-sized prefix through an image, just as it cannot commit one.
	// origin: hot-update S7 plan §8.1 / INTEG-D Tier A grant regression
	if s.CommitGrant != (xferport.CommitGrant{}) && s.CommitGrant.Identity.ObjectVersion == "" && s.DurableOffset > InlineAtomicMaxBytes {
		return errors.New("transferpath: inline commit grant exceeds the tier A ceiling")
	}
	o := s.PrimitiveOutcome
	if o.Primitive != "" && o.Primitive != xferport.PrimitiveRename && o.Primitive != xferport.PrimitiveLink {
		return errors.New("transferpath: unknown atomic primitive")
	}
	switch o.Outcome {
	case "":
		if o != (xferport.ReceiverOutcome{}) || s.DirSyncPending || s.UnlinkPending || (s.CommitGrant != (xferport.CommitGrant{}) && s.Phase != xferport.PhaseCommitReserved) {
			return errors.New("transferpath: empty primitive has decision evidence")
		}
	case xferport.OutcomeNotEntered:
		if o.ParentSynced || s.DirSyncPending || o.TmpCleanupPending != s.UnlinkPending || o.Grant != s.CommitGrant || o.Bytes != s.DurableOffset || o.Code == "" || (s.UnlinkPending && (!tmp || !parent)) {
			return errors.New("transferpath: unentered primitive has landed evidence")
		}
	case xferport.OutcomeLanded:
		primitive := xferport.PrimitiveLink
		if s.Force {
			primitive = xferport.PrimitiveRename
		}
		if !ValidAtomicGrant(s.CommitGrant) || o.Grant != s.CommitGrant || o.Primitive != primitive || o.Bytes != s.DurableOffset ||
			s.DirSyncPending == o.ParentSynced || o.TmpCleanupPending != s.UnlinkPending || (s.UnlinkPending && (s.Force || !tmp)) ||
			((s.DirSyncPending || s.UnlinkPending) && !parent) {
			return errors.New("transferpath: inconsistent landed atomic decision")
		}
	case xferport.OutcomeInflight:
		return errors.New("transferpath: in-flight primitive cannot be imported")
	default:
		return errors.New("transferpath: unknown primitive outcome cannot be imported")
	}
	switch s.Phase {
	case xferport.PhaseOpen:
		return errors.New("transferpath: unprepared atomic phase cannot be imported")
	case xferport.PhasePrepared:
		if !parent || tmp || source || s.DurableOffset != 0 || o.Outcome != "" {
			return errors.New("transferpath: prepared atomic closure differs")
		}
	case xferport.PhaseSending:
		if !source || tmp || parent || o.Outcome != "" {
			return errors.New("transferpath: sending atomic closure differs")
		}
	case xferport.PhaseReceiving, xferport.PhaseCommitReserved:
		if !tmp || !parent || o.Outcome != "" {
			return errors.New("transferpath: receiving atomic closure differs")
		}
	case xferport.PhaseDecided:
		if o.Outcome == "" || o.Outcome == xferport.OutcomeLanded {
			return errors.New("transferpath: decided atomic closure differs")
		}
	case xferport.PhaseCleanupPending:
		if !parent || (!s.DirSyncPending && !s.UnlinkPending) || o.Outcome == "" {
			return errors.New("transferpath: cleanup atomic closure differs")
		}
	case xferport.PhaseTerminalPending, xferport.PhaseHistoryConfirmed, xferport.PhaseSettled:
		if tmp || parent || source || o.Outcome == "" || s.DirSyncPending || s.UnlinkPending {
			return errors.New("transferpath: terminal atomic closure retains data descriptors")
		}
	default:
		return fmt.Errorf("transferpath: invalid atomic phase %q", s.Phase)
	}
	return nil
}
