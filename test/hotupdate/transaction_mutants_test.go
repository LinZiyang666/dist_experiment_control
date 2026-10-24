//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Test-only evidence is intentionally separate from product disk/wire codecs.
// The independently captured original seal is never taken from an observer's
// smaller final set. Payloads contain identities/digests, never sealed secrets.
// origin: hot-update S8 plan §8.3 / §9.0 / P25
// P26 supplies these companion receipts from its one supervised fixture.
type transactionOriginalSeal struct {
	RunID, SourceCommit, SourceDigest, DirtyDiffDigest string
	BinarySHA, ExecutedPath, ScopeDigest               string
	OriginalDigest, ExpectedDigest                     string
	Round                                              int
	Original                                           upgradetx.Scope
	Attempt                                            upgradetx.Attempt
	OriginalOwner                                      upgradetx.Owner
	FaultOwner                                         upgradetx.Owner
	Admissions                                         []transactionAdmission
}

type transactionAdmission struct {
	Resource              upgradetx.Resource
	Begin, Effect, Finish uint64
}

type transactionACK struct {
	Key, Digest string
	Sequence    uint64
	Absent      bool
}

type transactionFleetMember struct {
	ID, Kind, Release string
	Known             bool
}

type transactionProof struct {
	Attempt                             upgradetx.Attempt
	Observation                         upgradetx.Observation
	QueryNonce                          string
	Live                                []upgradetx.Resource
	ExpectedSubscriptions               []string
	ObservedSubscriptions               []string
	Flush                               bool
	ProbeNonce                          string
	Admissions                          []transactionAdmission
	TerminalDigests                     map[string]string
	Adopt, Cleanup, Unlock, Release     uint64
	Plan, ReopenedPlan                  upgradetx.Plan
	LateBefore, LateAfter               upgradetx.Record
	UnknownStartAccepted                bool
	UnknownStartEffects                 uint64
	HistoricalRollbackAccepted          bool
	ArtifactGrant, SelectedGrant        upgradetx.Handle
	IncarnationBefore, IncarnationAfter string
	FleetExpected, FleetObserved        []transactionFleetMember
	ThirdReleaseAccepted                bool
	LatestACK, BeforeReplayACK          []transactionACK
	ACKReadNS, FirstReplayNS            int64
	Descendants                         []upgradetx.Owner
	Survivors                           []upgradetx.Owner
	LifelineEOFNS, ReapedNS             int64
}

type transactionCaseEvidence struct {
	SchemaVersion           int
	RunID, Arm, ScopeDigest string
	Round                   int
	Phase, Outcome          string
	NativeRC                int
	OriginalOwner           upgradetx.Owner
	CurrentOwner            upgradetx.Owner
	CurrentKnown            bool
	OriginalLive            bool
	Hit                     transactionFaultHit
	Facts                   map[string]string
	Proof                   transactionProof
}

func transactionOwnerKnown(o upgradetx.Owner) bool {
	return o.BootID != "" && o.PID > 0 && o.StartTicks > 0 && o.InstanceID != ""
}

func transactionTestOwner(generation int) upgradetx.Owner {
	return upgradetx.Owner{BootID: strings.Repeat("b", 32), PID: 1200 + generation, StartTicks: uint64(100 + generation), InstanceID: fmt.Sprintf("%032x", generation)}
}

// This hashes the independent test artifact, not the production scope/census
// hash algorithm. Those two digests are captured verbatim from the real owner.
func transactionScopeDigest(scope upgradetx.Scope) string {
	body, err := json.Marshal(scope)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(body))
}

func transactionHex(s string, bytes int) bool {
	body, err := hex.DecodeString(s)
	return err == nil && len(body) == bytes && s == strings.ToLower(s)
}

func judgeTransactionProvenance(seal transactionOriginalSeal) error {
	if !transactionHex(seal.SourceCommit, 20) || !transactionHex(seal.SourceDigest, 32) || !transactionHex(seal.DirtyDiffDigest, 32) || !transactionHex(seal.BinarySHA, 32) || !filepath.IsAbs(seal.ExecutedPath) || !transactionHex(seal.ScopeDigest, 32) || !transactionHex(seal.ExpectedDigest, 32) || seal.OriginalDigest != transactionScopeDigest(seal.Original) || len(seal.Original.Expected) == 0 || !transactionOwnerKnown(seal.OriginalOwner) {
		return fmt.Errorf("missing source/executable/original-set provenance")
	}
	return nil
}

func transactionIdentity(r upgradetx.Resource) string {
	return string(r.Domain) + "/" + r.ID + "/" + r.Origin
}

func transactionHasResource(resources []upgradetx.Resource, want upgradetx.Resource) bool {
	for _, r := range resources {
		if transactionIdentity(r) == transactionIdentity(want) {
			return true
		}
	}
	return false
}

// Each defect has a separate rejection code. A missing control receipt cannot
// accidentally count as the proof that a generation, admission or DB mutant
// was killed. The positive control reaches every guard first.
// origin: hot-update S8 plan §8.3 / M01–M21
func judgeTransactionProof(seal transactionOriginalSeal, p transactionProof) error {
	generations, active := map[uint64]bool{}, map[string]upgradetx.Resource{}
	for _, r := range p.Live {
		generations[r.Generation] = true
		id := transactionIdentity(r)
		if old, exists := active[id]; exists && (old.Owner != r.Owner || old.Generation != r.Generation) {
			return fmt.Errorf("M01: two ACTIVE owners for one original resource")
		}
		active[id] = r
	}
	if len(generations) > 2 {
		return fmt.Errorf("M01: third live generation")
	}
	if p.Observation.RunningSHA != p.Attempt.TargetSHA || p.Observation.EffectiveSHA != p.Attempt.TargetSHA || !p.Observation.OwnerKnown {
		return fmt.Errorf("M02: disk selection without current running-image proof")
	}
	if len(p.Attempt.Released.Census.OldOwners) != 0 {
		return fmt.Errorf("M03: old worker still has execution or retained custody")
	}
	if !reflect.DeepEqual(seal.Admissions, p.Admissions) {
		return fmt.Errorf("M04: externally acknowledged request disappeared from admissions")
	}
	for _, a := range p.Admissions {
		if a.Begin == 0 || a.Effect <= a.Begin || a.Finish <= a.Effect || a.Finish > p.Observation.Census.Stamp.Sequence || !transactionHasResource(p.Observation.Census.Resources, a.Resource) {
			return fmt.Errorf("M04: effect escaped the shared admission sequence")
		}
	}
	if len(p.ExpectedSubscriptions) == 0 || !reflect.DeepEqual(p.ExpectedSubscriptions, p.ObservedSubscriptions) || !p.Flush || p.ProbeNonce != p.QueryNonce {
		return fmt.Errorf("M05: register cannot stand in for subscriptions/Flush/control request")
	}
	if !reflect.DeepEqual(p.Attempt.Scope.Domains, seal.Original.Domains) {
		return fmt.Errorf("M06: an applicable domain disappeared")
	}
	for _, r := range []upgradetx.Receipt{p.Attempt.Control, p.Attempt.Resources, p.Attempt.Released} {
		if r.Stamp.Attempt != p.Attempt.Handle || r.Census.Stamp.Attempt != p.Attempt.Handle {
			return fmt.Errorf("M07: forward receipt reused on reverse cut")
		}
	}
	if p.Adopt == 0 || p.Cleanup <= p.Adopt || p.Unlock < p.Release || p.Release <= p.Cleanup {
		return fmt.Errorf("M08: auxiliary cleanup unlocked before reservation release")
	}
	if len(p.Plan.Steps) != len(p.Plan.Scope.Participants) || len(p.Plan.Steps) == 0 || !reflect.DeepEqual(p.Plan, p.ReopenedPlan) {
		return fmt.Errorf("M09: resumed plan lost a participant or regenerated child IDs")
	}
	if p.UnknownStartAccepted || p.UnknownStartEffects != 0 {
		return fmt.Errorf("M10: expired/GC grant recreated execution")
	}
	if !reflect.DeepEqual(p.LateBefore, p.LateAfter) {
		return fmt.Errorf("M11: old watchdog/report/unlink changed the next transaction")
	}
	if p.HistoricalRollbackAccepted {
		return fmt.Errorf("M12: historical A rollback accepted against current B")
	}
	if len(p.LatestACK) == 0 || !reflect.DeepEqual(p.LatestACK, p.BeforeReplayACK) || p.ACKReadNS <= 0 || p.FirstReplayNS <= p.ACKReadNS {
		return fmt.Errorf("M13: latest overwrite/delete/ACL state lost before first Replay")
	}
	for _, id := range []string{"transfer-prepare", "transfer-pending-terminal", "transfer-result", "transfer-gc-permit"} {
		found := false
		for _, r := range p.Attempt.Resources.Census.Resources {
			found = found || r.ID == id
		}
		if !found {
			return fmt.Errorf("M14: S7 prepare/terminal/result/GC custody omitted")
		}
	}
	if p.QueryNonce == "" || p.Observation.Nonce != p.QueryNonce || p.Observation.Census.Stamp.Nonce != p.QueryNonce {
		return fmt.Errorf("M15: stale completed response nonce")
	}
	if p.ArtifactGrant != p.SelectedGrant {
		return fmt.Errorf("M16: same SHA adopted across grants")
	}
	if p.IncarnationBefore == "" || p.IncarnationBefore != p.IncarnationAfter {
		return fmt.Errorf("M17: old G.1 cleanup changed a recreated incarnation")
	}
	if len(p.FleetExpected) == 0 || !reflect.DeepEqual(p.FleetExpected, p.FleetObserved) || p.ThirdReleaseAccepted {
		return fmt.Errorf("M18: release reservation counted only voters or accepted a third release")
	}
	releases := map[string]bool{}
	for _, member := range p.FleetObserved {
		if member.Known && member.Release != "" {
			releases[member.Release] = true
		}
	}
	if len(releases) > 2 {
		return fmt.Errorf("M18: non-voter/agent/sibling/helper introduced a third live release")
	}
	for _, original := range seal.Original.Expected {
		if !transactionHasResource(p.Observation.Census.Resources, original) || !transactionHasResource(p.Attempt.Resources.Census.Resources, original) {
			return fmt.Errorf("M20: observer deleted an original resource")
		}
	}
	if len(p.Descendants) == 0 || len(p.Survivors) != 0 || p.LifelineEOFNS <= 0 || p.ReapedNS <= p.LifelineEOFNS {
		return fmt.Errorf("M21: external supervisor did not reap setsid descendants after lifeline EOF")
	}
	for i, receipt := range []upgradetx.Receipt{p.Attempt.Control, p.Attempt.Resources, p.Attempt.Released} {
		kind := []upgradetx.ReceiptKind{upgradetx.ReceiptControlReady, upgradetx.ReceiptResourcesMigrated, upgradetx.ReceiptOldGenerationReleased}[i]
		if receipt.Kind != kind || !receipt.Known || !receipt.Complete || receipt.EvidenceDigest == "" {
			return fmt.Errorf("%s: missing independent completion receipt", kind)
		}
		if !receipt.Census.Known || !receipt.Census.Complete || receipt.Census.Admitting != 0 || receipt.Stamp.ScopeDigest != seal.ScopeDigest || receipt.Census.ExpectedDigest != seal.ExpectedDigest || receipt.Stamp.Sequence != p.Observation.Census.Stamp.Sequence {
			return fmt.Errorf("unknown_census: unknown/incomplete/count-zero or stale admission barrier")
		}
	}
	if !p.Observation.Census.Known || !p.Observation.Census.Complete || p.Observation.Census.Admitting != 0 || p.Observation.Current.Handle != p.Attempt.Handle || p.Attempt.CutStage != upgradetx.CutReleased {
		return fmt.Errorf("unknown_census: completion lacks current released scope")
	}
	return nil
}

func judgeTransactionCase(c transactionFaultCase, seal transactionOriginalSeal, e transactionCaseEvidence, round int) error {
	if e.SchemaVersion != 1 || seal.RunID == "" || e.RunID != seal.RunID || !strings.HasSuffix(e.RunID, fmt.Sprintf("/round-%d", round)) || e.Round != round || seal.Round != round || e.Arm != c.Arm {
		return fmt.Errorf("case/round identity mismatch")
	}
	if err := judgeTransactionProvenance(seal); err != nil {
		return err
	}
	if e.ScopeDigest != seal.ScopeDigest {
		return fmt.Errorf("missing source/executable/original-set provenance")
	}
	if e.Phase != c.Phase || e.Outcome != c.Outcome || e.NativeRC != c.RC {
		return fmt.Errorf("native result changed: %s/%s/%d, require %s/%s/%d", e.Phase, e.Outcome, e.NativeRC, c.Phase, c.Outcome, c.RC)
	}
	if e.OriginalOwner != seal.OriginalOwner || !transactionOwnerKnown(e.OriginalOwner) || e.OriginalLive != (c.OwnerLane == "L") || e.CurrentKnown != (c.CurrentOwner == "known") || (e.CurrentKnown && !transactionOwnerKnown(e.CurrentOwner)) {
		return fmt.Errorf("original/current owners were collapsed")
	}
	if err := judgeTransactionFaultHit(c, e.Hit, seal.FaultOwner); err != nil {
		return err
	}
	for _, required := range strings.Split(c.Proof, ",") {
		if !transactionHex(e.Facts[required], 32) {
			return fmt.Errorf("missing independent proof digest: %s", required)
		}
	}
	if c.Outcome == "success" || c.Outcome == "rolled_back" {
		if e.Proof.Attempt.Direction == upgradetx.DirectionReverse {
			if e.Proof.Attempt.Handle.CutID == seal.Attempt.Handle.CutID || e.Proof.Attempt.Handle.Generation <= seal.Attempt.Handle.Generation || e.Proof.Attempt.SourceSHA != seal.Attempt.TargetSHA || e.Proof.Attempt.TargetSHA != seal.Attempt.SourceSHA {
				return fmt.Errorf("reverse did not export current state into a new generation/cut")
			}
		} else if c.Outcome == "rolled_back" || e.Proof.Attempt.Handle != seal.Attempt.Handle {
			return fmt.Errorf("wrong forward/reverse completion identity")
		}
		return judgeTransactionCompletion(seal, e.Proof)
	}
	if c.CurrentOwner == "unknown" && (e.Proof.Attempt.Outcome == upgradetx.OutcomeSuccess || e.Proof.Attempt.Outcome == upgradetx.OutcomeRolledBack || e.Proof.Attempt.Direction == upgradetx.DirectionReverse) {
		return fmt.Errorf("unknown owner falsely authorized automatic return to old code")
	}
	return nil
}

// Completion checks inspect the neutral DTOs directly. The product mutation
// portfolio additionally checks restart/cleanup/host/DB obligations that need
// separate exercises; a standalone fault cell cannot fabricate those effects.
// origin: hot-update S8 plan §4.2 / §8.3 / P25
func judgeTransactionCompletion(seal transactionOriginalSeal, p transactionProof) error {
	a, o := p.Attempt, p.Observation
	if a.Phase != upgradetx.PhaseCompleted || a.Outcome != upgradetx.OutcomeSuccess || a.CutStage != upgradetx.CutReleased || !o.OwnerKnown || o.RunningSHA != a.TargetSHA || o.EffectiveSHA != a.TargetSHA || !reflect.DeepEqual(o.Current, a) {
		return fmt.Errorf("completion lacks current released running-image proof")
	}
	if !reflect.DeepEqual(a.Scope, seal.Original) || !transactionHex(a.TargetSHA, 32) || !transactionHex(a.SourceSHA, 32) || p.QueryNonce == "" || o.Nonce != p.QueryNonce || !o.Census.Known || !o.Census.Complete || o.Census.Admitting != 0 || o.Census.Stamp.Sequence == 0 || o.Census.Stamp.Attempt != a.Handle || o.Census.Stamp.Nonce != p.QueryNonce || o.Census.ExpectedDigest != seal.ExpectedDigest || !transactionOwnerKnown(o.Census.Stamp.Producer) {
		return fmt.Errorf("completion scope/nonce/admission barrier unknown")
	}
	if len(p.ExpectedSubscriptions) == 0 || !reflect.DeepEqual(p.ExpectedSubscriptions, p.ObservedSubscriptions) || !p.Flush || p.ProbeNonce != p.QueryNonce {
		return fmt.Errorf("control_ready: register lacks subscriptions/Flush/real control probe")
	}
	for i, receipt := range []upgradetx.Receipt{a.Control, a.Resources, a.Released} {
		kind := []upgradetx.ReceiptKind{upgradetx.ReceiptControlReady, upgradetx.ReceiptResourcesMigrated, upgradetx.ReceiptOldGenerationReleased}[i]
		if receipt.Kind != kind || !receipt.Known || !receipt.Complete || !transactionHex(receipt.EvidenceDigest, 32) || receipt.RunningSHA != a.TargetSHA || receipt.Stamp.Attempt != a.Handle || receipt.Stamp.Producer != o.Census.Stamp.Producer || receipt.Stamp.Nonce != p.QueryNonce || receipt.Stamp.At <= 0 || receipt.Stamp.At > o.At || receipt.Stamp.Sequence != o.Census.Stamp.Sequence || receipt.Stamp.ScopeDigest != seal.ScopeDigest || receipt.Census.Stamp != receipt.Stamp || !receipt.Census.Known || !receipt.Census.Complete || receipt.Census.Admitting != 0 || receipt.Census.ExpectedDigest != seal.ExpectedDigest {
			return fmt.Errorf("%s: independent receipt identity/nonce/sequence incomplete", kind)
		}
	}
	if len(a.Released.Census.OldOwners) != 0 {
		return fmt.Errorf("completion retains an old owner")
	}
	for _, live := range p.Live {
		if live.Generation != a.Handle.Generation || live.Owner != o.Census.Stamp.Producer {
			return fmt.Errorf("completion retains old execution/custody despite released receipt")
		}
	}
	for _, resources := range [][]upgradetx.Resource{o.Census.Resources, a.Resources.Census.Resources} {
		seen := map[string]bool{}
		for _, r := range resources {
			id := transactionIdentity(r)
			migrated := r.Generation == a.Handle.Generation && r.Owner == o.Census.Stamp.Producer && r.Custody == "migrated"
			terminal := r.State == "terminal_durable" && r.Custody == "released" && transactionHex(p.TerminalDigests[id], 32)
			if seen[id] || (!migrated && !terminal) {
				return fmt.Errorf("completion substituted/duplicated or retained resource custody")
			}
			seen[id] = true
		}
		for _, original := range seal.Original.Expected {
			if !seen[transactionIdentity(original)] {
				return fmt.Errorf("completion omitted an original identity")
			}
		}
	}
	if !reflect.DeepEqual(seal.Admissions, p.Admissions) {
		return fmt.Errorf("completion omitted externally acknowledged admission")
	}
	for _, admission := range p.Admissions {
		if admission.Begin == 0 || admission.Effect <= admission.Begin || admission.Finish <= admission.Effect || admission.Finish > o.Census.Stamp.Sequence || !transactionHasResource(o.Census.Resources, admission.Resource) {
			return fmt.Errorf("completion omitted a new admission")
		}
	}
	return nil
}

// Synthetic witnesses are oracle controls only. Their fixed, synthetic PID
// provenance is never written to the live receipt directory or called a cut.
func transactionOracleControl() (transactionOriginalSeal, transactionProof) {
	key := upgradetx.Key{DomainID: strings.Repeat("d", 32), Component: upgradetx.ComponentAgent, SID: "test-session", ConfiguredNID: "test-agent", UID: 1000}
	h := upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 9}
	domains := []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}
	scope := upgradetx.Scope{ID: strings.Repeat("3", 32), Participants: []upgradetx.Key{key}, Domains: domains}
	for _, d := range domains {
		scope.Expected = append(scope.Expected, upgradetx.Resource{Domain: d, ID: string(d) + "-original", Origin: "sealed-before-arm", Owner: transactionTestOwner(1), Generation: 1, State: "active", Custody: "original"})
	}
	for _, id := range []string{"transfer-prepare", "transfer-pending-terminal", "transfer-result", "transfer-gc-permit"} {
		scope.Expected = append(scope.Expected, upgradetx.Resource{Domain: upgradetx.DomainTransferTracker, ID: id, Origin: "sealed-before-arm", Owner: transactionTestOwner(1), Generation: 1, State: "retained", Custody: "original"})
	}
	sort.Slice(scope.Expected, func(i, j int) bool {
		return transactionIdentity(scope.Expected[i]) < transactionIdentity(scope.Expected[j])
	})
	digest := transactionScopeDigest(scope)
	stamp := upgradetx.Stamp{Attempt: h, Producer: transactionTestOwner(2), Nonce: strings.Repeat("4", 32), Sequence: 9, At: 100, ScopeDigest: digest}
	migrated := append([]upgradetx.Resource(nil), scope.Expected...)
	for i := range migrated {
		migrated[i].Owner, migrated[i].Generation, migrated[i].Custody = transactionTestOwner(2), 2, "migrated"
	}
	newRequest := upgradetx.Resource{Domain: upgradetx.DomainExec, ID: "new-request", Origin: "after-activation", Owner: transactionTestOwner(2), Generation: 2, State: "active", Custody: "migrated"}
	migrated = append(migrated, newRequest)
	census := upgradetx.Census{Stamp: stamp, Known: true, Complete: true, Resources: migrated, ExpectedDigest: digest}
	a := upgradetx.Attempt{Handle: h, Direction: upgradetx.DirectionForward, SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64), SourceGeneration: 1, Phase: upgradetx.PhaseCompleted, CutStage: upgradetx.CutReleased, Outcome: upgradetx.OutcomeSuccess, Scope: scope}
	for i, kind := range []upgradetx.ReceiptKind{upgradetx.ReceiptControlReady, upgradetx.ReceiptResourcesMigrated, upgradetx.ReceiptOldGenerationReleased} {
		r := upgradetx.Receipt{Kind: kind, Stamp: stamp, Known: true, Complete: true, RunningSHA: a.TargetSHA, Census: census, EvidenceDigest: fmt.Sprintf("%064x", i+1)}
		switch i {
		case 0:
			a.Control = r
		case 1:
			a.Resources = r
		case 2:
			a.Released = r
		}
	}
	plan := upgradetx.Plan{Parent: h, Scope: scope, Steps: []upgradetx.Step{{Ordinal: 0, Participant: key, Grant: upgradetx.Grant{Handle: h}, Kind: upgradetx.StepAgentCut}}, Deadline: 900}
	late := upgradetx.Record{Version: 2, Key: key, Revision: 10, Forward: a}
	ack := []transactionACK{{Key: "overwrite", Digest: strings.Repeat("c", 64), Sequence: 3}, {Key: "delete", Sequence: 4, Absent: true}, {Key: "acl", Digest: strings.Repeat("d", 64), Sequence: 5}}
	fleet := []transactionFleetMember{{"voter", "voter", "A", true}, {"non-voter", "non-voter", "B", true}, {"agent", "agent", "B", true}, {"sibling", "sibling", "A", true}, {"helper", "retained-helper", "A", true}, {"offline", "unknown", "", false}}
	p := transactionProof{Attempt: a, Observation: upgradetx.Observation{Nonce: stamp.Nonce, At: 110, Current: a, Census: census, OwnerKnown: true, RunningSHA: a.TargetSHA, EffectiveSHA: a.TargetSHA}, QueryNonce: stamp.Nonce, Live: migrated, ExpectedSubscriptions: []string{"forward", "intake", "proxy", "reconcile", "sysEvents"}, ObservedSubscriptions: []string{"forward", "intake", "proxy", "reconcile", "sysEvents"}, Flush: true, ProbeNonce: stamp.Nonce, Admissions: []transactionAdmission{{newRequest, 6, 7, 8}}, Adopt: 1, Cleanup: 2, Release: 3, Unlock: 4, Plan: plan, ReopenedPlan: plan, LateBefore: late, LateAfter: late, ArtifactGrant: h, SelectedGrant: h, IncarnationBefore: strings.Repeat("5", 64), IncarnationAfter: strings.Repeat("5", 64), FleetExpected: fleet, FleetObserved: fleet, LatestACK: ack, BeforeReplayACK: ack, ACKReadNS: 100, FirstReplayNS: 110, Descendants: []upgradetx.Owner{transactionTestOwner(10), transactionTestOwner(11)}, LifelineEOFNS: 100, ReapedNS: 110}
	seal := transactionOriginalSeal{RunID: "oracle-control/round-0", SourceCommit: strings.Repeat("a", 40), SourceDigest: strings.Repeat("1", 64), DirtyDiffDigest: strings.Repeat("2", 64), BinarySHA: strings.Repeat("3", 64), ExecutedPath: "/test-only/never-executed", ScopeDigest: digest, OriginalDigest: digest, ExpectedDigest: digest, Original: scope, Attempt: a, OriginalOwner: transactionTestOwner(1)}
	seal.Admissions = append([]transactionAdmission(nil), p.Admissions...)
	return seal, p
}

type transactionMutant struct{ ID, Defect, Owner string }

// This is a responsibility map, not a claim of production mutation execution.
// The independent M19 hit check lives beside the fault-window rules.
var transactionMutants = []transactionMutant{
	{"M01", "double ACTIVE generation", "P01/P03/P09/P10"},
	{"M02", "disk selected without exec", "P14/P15/P17/P23"},
	{"M03", "retained old worker", "P03/P17"},
	{"M04", "effect before Registry.Begin", "P03/P14/P15"},
	{"M05", "register substituted for control probe", "P14/P15"},
	{"M06", "missing domain or expected identity", "P01/P03/P17"},
	{"M07", "forward receipt reused for reverse", "P01/P09/P10/P23"},
	{"M08", "auxiliary cleanup unlocks lease", "P04/P07/P17/P23"},
	{"M09", "parent participant omitted or child regenerated", "P11/P23"},
	{"M10", "GC grant recreates Start", "P02"},
	{"M11", "late callback changes next transaction", "P02/P14/P23"},
	{"M12", "historical rollback acts on current B", "P02/P09/P10/P23"},
	{"M13", "old DB replay hides lost latest ACK", "P10/P17/P23"},
	{"M14", "S7 prepare terminal result GC permit omitted", "P17"},
	{"M15", "stale completed nonce", "P03/P13/P18"},
	{"M16", "same SHA slot adopted across grants", "P05/P23"},
	{"M17", "old G.1 cleanup deletes new incarnation", "P16/P24"},
	{"M18", "fleet counted only voters", "P06/P11"},
	{"M19", "fault event outside actual hit interval", "P26"},
	{"M20", "observer deletes original resource", "P26"},
	{"M21", "Pdeathsig alone lets setsid descendant escape", "P21"},
}

// origin: hot-update S8 plan §8.3 / P25
func TestTransactionMutantMappingComplete(t *testing.T) {
	if len(transactionMutants) != 21 {
		t.Fatal("mutant mapping is incomplete")
	}
	for i, m := range transactionMutants {
		if m.ID != fmt.Sprintf("M%02d", i+1) || m.Defect == "" || m.Owner == "" {
			t.Fatalf("invalid mutant mapping: %+v", m)
		}
	}
}

func mutateTransactionProof(id string, p *transactionProof) {
	switch id {
	case "M01":
		r := p.Live[0]
		r.Owner = transactionTestOwner(3)
		p.Live = append(p.Live, r)
	case "M02":
		p.Observation.RunningSHA = p.Attempt.SourceSHA
	case "M03":
		p.Attempt.Released.Census.OldOwners = []upgradetx.Owner{transactionTestOwner(1)}
	case "M04":
		p.Admissions[0].Begin = 0
	case "M05":
		p.ObservedSubscriptions = p.ObservedSubscriptions[:1]
	case "M06":
		p.Attempt.Scope.Domains = p.Attempt.Scope.Domains[1:]
	case "M07":
		p.Attempt.Control.Stamp.Attempt.CutID = strings.Repeat("9", 32)
	case "M08":
		p.Unlock = p.Cleanup
	case "M09":
		p.ReopenedPlan.Steps[0].Grant.Handle.TxID = strings.Repeat("9", 32)
	case "M10":
		p.UnknownStartAccepted, p.UnknownStartEffects = true, 1
	case "M11":
		p.LateAfter.Revision++
	case "M12":
		p.HistoricalRollbackAccepted = true
	case "M13":
		p.BeforeReplayACK[0].Sequence--
	case "M14":
		for i, r := range p.Attempt.Resources.Census.Resources {
			if r.ID == "transfer-prepare" {
				p.Attempt.Resources.Census.Resources = append(p.Attempt.Resources.Census.Resources[:i], p.Attempt.Resources.Census.Resources[i+1:]...)
				break
			}
		}
	case "M15":
		p.Observation.Nonce = strings.Repeat("9", 32)
	case "M16":
		p.SelectedGrant.TxID = strings.Repeat("9", 32)
	case "M17":
		p.IncarnationAfter = strings.Repeat("9", 64)
	case "M18":
		p.FleetObserved = p.FleetObserved[:1]
	case "M20":
		p.Observation.Census.Resources = p.Observation.Census.Resources[1:]
	case "M21":
		p.Survivors = []upgradetx.Owner{p.Descendants[1]}
	default:
		panic("unknown proof mutant " + id)
	}
}

func copyTransactionProof(p transactionProof) transactionProof {
	body, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	var result transactionProof
	if err := json.Unmarshal(body, &result); err != nil {
		panic(err)
	}
	return result
}

// Every mutant has a distinct assertion-red oracle control. These checks do
// not certify the product mutations; that receipt test below requires different
// executable/source digests and the native failure from the mutated product.
// origin: hot-update S8 plan §8.3 / M01–M21
func TestTransactionMutantOracleControls(t *testing.T) {
	seal, good := transactionOracleControl()
	if err := judgeTransactionProof(seal, good); err != nil {
		t.Fatal(err)
	}
	for _, m := range transactionMutants {
		if m.ID == "M19" {
			continue
		}
		t.Run(m.ID, func(t *testing.T) {
			bad := copyTransactionProof(good)
			mutateTransactionProof(m.ID, &bad)
			err := judgeTransactionProof(seal, bad)
			if err == nil || !strings.HasPrefix(err.Error(), m.ID+":") {
				t.Fatalf("%s was not independently rejected: %v", m.ID, err)
			}
		})
	}
	for _, kind := range []string{"control_ready", "resources_migrated", "old_generation_released", "unknown_census", "incomplete_census", "admitting", "stale_sequence"} {
		t.Run(kind, func(t *testing.T) {
			bad := copyTransactionProof(good)
			switch kind {
			case "control_ready":
				bad.Attempt.Control.Known = false
			case "resources_migrated":
				bad.Attempt.Resources.Complete = false
			case "old_generation_released":
				bad.Attempt.Released.EvidenceDigest = ""
			case "unknown_census":
				bad.Attempt.Resources.Census.Known = false
				bad.Attempt.Resources.Census.Resources = nil
			case "incomplete_census":
				bad.Attempt.Control.Census.Complete = false
			case "admitting":
				bad.Observation.Census.Admitting = 1
			case "stale_sequence":
				bad.Attempt.Control.Stamp.Sequence--
			}
			if judgeTransactionProof(seal, bad) == nil {
				t.Fatal("missing/unknown completion evidence accepted")
			}
		})
	}
	for _, id := range []string{"transfer-prepare", "transfer-pending-terminal", "transfer-result", "transfer-gc-permit"} {
		t.Run("M14/"+id, func(t *testing.T) {
			bad := copyTransactionProof(good)
			for i, r := range bad.Attempt.Resources.Census.Resources {
				if r.ID == id {
					bad.Attempt.Resources.Census.Resources = append(bad.Attempt.Resources.Census.Resources[:i], bad.Attempt.Resources.Census.Resources[i+1:]...)
					break
				}
			}
			if err := judgeTransactionProof(seal, bad); err == nil || !strings.HasPrefix(err.Error(), "M14:") {
				t.Fatalf("S7 custody omission was not independently rejected: %v", err)
			}
		})
	}
	for _, member := range []int{1, 2, 3, 4} {
		t.Run("M18/"+good.FleetExpected[member].Kind, func(t *testing.T) {
			bad := copyTransactionProof(good)
			bad.FleetExpected[member].Release, bad.FleetObserved[member].Release = "C", "C"
			if err := judgeTransactionProof(seal, bad); err == nil || !strings.HasPrefix(err.Error(), "M18:") {
				t.Fatalf("third non-voter release was not rejected: %v", err)
			}
		})
	}
}

type transactionMutationEvidence struct {
	SchemaVersion                                       int
	Mutant, Test, SourceDigest, BinarySHA, ExecutedPath string
	NativeRC                                            int
	Seal                                                transactionOriginalSeal
	Case                                                transactionFaultCase
	Proof                                               transactionProof
	Hit                                                 transactionFaultHit
}

// Production mutants cannot be signed by changing JSON alone. The supplied
// receipt must identify the distinct built/executed mutation and native red.
// origin: hot-update S8 plan §8.3 / P25
func TestTransactionMutantReceipts(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransactionMutantReceipts(t)
	}
	testharness.AssertNoGoroutineLeak(t, "mutant receipt reads x5", g)
	testharness.AssertNoFDLeak(t, "mutant receipt reads x5", fd, 0)
}

func exerciseTransactionMutantReceipts(t *testing.T) {
	t.Helper()
	for _, m := range transactionMutants {
		t.Run(m.ID, func(t *testing.T) {
			var e transactionMutationEvidence
			if err := readTransactionEvidence("mutants/"+m.ID, &e); err != nil {
				t.Fatal(err)
			}
			if err := judgeTransactionProvenance(e.Seal); err != nil {
				t.Fatal(err)
			}
			if e.SchemaVersion != 1 || e.Mutant != m.ID || e.Test == "" || e.NativeRC == 0 || !transactionHex(e.SourceDigest, 32) || e.SourceDigest == e.Seal.SourceDigest || !transactionHex(e.BinarySHA, 32) || e.BinarySHA == e.Seal.BinarySHA || !filepath.IsAbs(e.ExecutedPath) {
				t.Fatal("mutation lacks actual source/executable/native-red provenance")
			}
			var err error
			if m.ID == "M19" {
				err = judgeTransactionFaultHit(e.Case, e.Hit, e.Seal.OriginalOwner)
			} else {
				err = judgeTransactionProof(e.Seal, e.Proof)
			}
			if err == nil || !strings.HasPrefix(err.Error(), m.ID+":") {
				t.Fatalf("mutation did not reach its independent oracle: %v", err)
			}
		})
	}
}
