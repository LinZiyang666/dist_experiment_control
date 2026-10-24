// resources_test.go exercises pure predicates with synthetic, fully stamped
// observations. It starts no runtime, substitutes no product implementation,
// and does not sign P25/P26's real same-image or deployment evidence.
package acceptance

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// origin: hot-update S10 plan §4.1–§4.3 / P03
func TestFullstackResourceJudgeSeparatesRetention(t *testing.T) {
	t.Run("three independent axes preserve lease metadata and permanent store lock", func(t *testing.T) {
		e := resourceJudgeEvidence(2)
		before := resourceJudgeEvidence(2)
		result := requireResourceVerdict(t, e, "GREEN", "")
		if len(result.Inputs) < len(e.Samples) || !reflect.DeepEqual(e, before) {
			t.Fatal("resource judge lost raw inputs or mutated the caller's evidence")
		}
		again, err := JudgeResources(e)
		if err != nil || !reflect.DeepEqual(result, again) {
			t.Fatal("resource judge is not deterministic")
		}
	})
	for _, tc := range []struct {
		name, verdict, diagnostic string
		change                    func(*contract.Evidence)
	}{
		{"old image leak cannot be cancelled by exec reset", "ASSERT-FAIL", "same-image goroutine floor", func(e *contract.Evidence) {
			resourceChangeFloors(e, "agent", 1, func(s *contract.Sample, round int) { s.Goroutines += int64(round) })
		}},
		{"broker plus one each round is not CPU noise", "ASSERT-FAIL", "same-image goroutine floor", func(e *contract.Evidence) {
			resourceChangeFloors(e, "broker", 2, func(s *contract.Sample, round int) { s.Goroutines += int64(round) })
		}},
		{"parent leak is independent of product floors", "ASSERT-FAIL", "same-image goroutine floor", func(e *contract.Evidence) {
			resourceChangeFloors(e, "fixture", 1, func(s *contract.Sample, round int) { s.Goroutines += int64(round) })
		}},
		{"one agent FD per cycle cannot be retained metadata", "ASSERT-FAIL", "classified FD census", func(e *contract.Evidence) {
			resourceChangeFloors(e, "agent", 1, func(s *contract.Sample, round int) { s.FDs += int64(round) })
		}},
		{"broker FD leak survives a balanced total", "ASSERT-FAIL", "sealed background census", func(e *contract.Evidence) {
			resourceChangeFloors(e, "broker", 1, func(s *contract.Sample, _ int) {
				s.Resources[0].OFD = "replacement-dup"
			})
		}},
		{"settled replay must release its reservation", "ASSERT-FAIL", "settled responsibility retains", func(e *contract.Evidence) {
			s := resourceFirstFloor(e)
			r := resourceRow(s.Stamp, "task-agent-1-0", "replay", "exec", "settled-unexpired", -1, 8<<20)
			r.ReleaseAfterNS = resourceTestExpiry
			s.Resources = append(s.Resources, r)
		}},
		{"settled pipe is not allowed retention", "ASSERT-FAIL", "settled responsibility retains", func(e *contract.Evidence) {
			s := resourceFirstFloor(e)
			r := resourceRow(s.Stamp, "task-agent-1-0", "pipe", "exec", "settled-unexpired", 42, 0)
			r.ReleaseAfterNS = resourceTestExpiry
			s.Resources = append(s.Resources, r)
			s.FDs++
		}},
		{"empty replay still needs a capacity reservation", "ASSERT-FAIL", "replay lacks its capacity reservation", func(e *contract.Evidence) {
			for i := range e.Samples[1].Resources {
				if e.Samples[1].Resources[i].Kind == "replay" {
					e.Samples[1].Resources[i].Bytes = 0
				}
			}
		}},
		{"forgotten live exec replay is not zero budget", "ASSERT-FAIL", "three pipes or three replay", func(e *contract.Evidence) {
			for i := range e.Samples {
				s := &e.Samples[i]
				if s.Stamp.Process.Key.Role == "agent" && s.Stamp.Sequence == 2 {
					s.Resources = resourceRemoveKind(s.Resources, "replay")
					s.FDs -= 3
					break
				}
			}
		}},
		{"forgotten PTY replay cannot retire the master", "ASSERT-FAIL", "two replay reservations", func(e *contract.Evidence) {
			for i := range e.Samples {
				s := &e.Samples[i]
				if s.Stamp.Process.Key.Role == "agent" && s.Stamp.Sequence == 4 {
					s.Resources = resourceRemoveKind(s.Resources, "replay")
					s.FDs -= 2
					break
				}
			}
		}},
		{"background socket cannot close between equal floors", "ASSERT-FAIL", "background reference disappeared", func(e *contract.Evidence) {
			e.Samples[1].Resources = resourceRemoveID(e.Samples[1].Resources, "original-flow")
			e.Samples[1].FDs--
		}},
		{"settlement cannot use the transfer state namespace", "ASSERT-FAIL", "transfer-only retention state", func(e *contract.Evidence) {
			s := resourceFirstFloor(e)
			for i := range s.Resources {
				if s.Resources[i].ID == "task-agent-1-0" {
					s.Resources[i].State = "settled"
				}
			}
		}},
		{"retained task metadata cannot have an infinite invented clock", "ASSERT-FAIL", "unexpired original deadline", func(e *contract.Evidence) {
			resourceFirstFloor(e).Resources[len(resourceFirstFloor(e).Resources)-1].ReleaseAfterNS = 0
		}},
		{"new metadata cannot be classified as background noise", "INCOMPLETE", "observed lifecycle origin", func(e *contract.Evidence) {
			s := resourceFirstFloor(e)
			for i := range e.Samples {
				later := &e.Samples[i]
				if later.Stamp.Process == s.Stamp.Process && later.Stamp.AtBootNS >= s.Stamp.AtBootNS {
					r := resourceRow(later.Stamp, "unknown-history", "history", "transfer_tracker", "settled", -1, 0)
					r.ReleaseAfterNS = resourceTestExpiry
					later.Resources = append(later.Resources, r)
				}
			}
		}},
		{"early registry collection is not leak cleanup", "ASSERT-FAIL", "metadata collected before original expiry", func(e *contract.Evidence) {
			s := resourceFirstFloor(e)
			for i := range e.Samples {
				later := &e.Samples[i]
				if later.Stamp.Process.Key == s.Stamp.Process.Key && later.Stamp.Generation == 1 && later.Stamp.Sequence == 4 {
					later.Resources = resourceRemoveID(later.Resources, "task-agent-1-0")
					break
				}
			}
		}},
		{"settled registry requires its original lease", "ASSERT-FAIL", "settled task lost original lease", func(e *contract.Evidence) {
			s := resourceFirstFloor(e)
			s.Resources = resourceRemoveKind(s.Resources, "lease")
		}},
		{"retention cannot be extended to hide old metadata", "ASSERT-FAIL", "retention deadline changed", func(e *contract.Evidence) {
			s := resourceFirstFloor(e)
			for i := range e.Samples {
				later := &e.Samples[i]
				if later.Stamp.Process.Key == s.Stamp.Process.Key && later.Stamp.Sequence == 4 && later.Stamp.Generation == 1 {
					for j := range later.Resources {
						if later.Resources[j].ID == "task-agent-1-0" && later.Resources[j].Kind == "lease" {
							later.Resources[j].ReleaseAfterNS++
						}
					}
				}
			}
		}},
		{"exec cannot erase unexpired metadata", "ASSERT-FAIL", "metadata collected before original expiry", func(e *contract.Evidence) {
			for i := range e.Samples {
				s := &e.Samples[i]
				if s.Stamp.Process.Key.Role == "agent" && s.Stamp.Generation == 2 {
					s.Resources = resourceRemoveID(s.Resources, "task-agent-1-0")
				}
			}
		}},
		{"resource owner cannot be the collecting parent", "ASSERT-FAIL", "foreign process", func(e *contract.Evidence) {
			resourceFirstFloor(e).Resources[0].Owner.Process.PID++
		}},
		{"old generation owner cannot sign the current floor", "ASSERT-FAIL", "foreign process", func(e *contract.Evidence) {
			resourceFirstFloor(e).Resources[0].Owner.Generation--
		}},
		{"runtime restart cannot reset same-image floors", "ASSERT-FAIL", "generation transition lost original process", func(e *contract.Evidence) {
			for i := range e.Samples {
				if e.Samples[i].Stamp.Generation == 2 {
					e.Samples[i].Stamp.Process.PID++
					for j := range e.Samples[i].Resources {
						e.Samples[i].Resources[j].Owner.Process.PID++
					}
				}
			}
			for i := range e.Intervals {
				if e.Intervals[i].Stamp.Generation == 2 {
					e.Intervals[i].Stamp.Process.PID++
				}
			}
		}},
		{"missing product cannot be replaced by parent sample", "INCOMPLETE", "missing observed runtime", func(e *contract.Evidence) {
			kept := e.Samples[:0]
			for _, s := range e.Samples {
				if s.Stamp.Process.Key.Role != "agent" {
					kept = append(kept, s)
				}
			}
			e.Samples = kept
		}},
		{"missing harness cannot be replaced by product sample", "INCOMPLETE", "harness axis", func(e *contract.Evidence) {
			kept := e.Samples[:0]
			for _, s := range e.Samples {
				if s.Stamp.Process.Key.Role != "fixture" {
					kept = append(kept, s)
				}
			}
			e.Samples = kept
		}},
		{"unknown child gauge cannot become zero", "ASSERT-FAIL", "invalid observed identity or counters", func(e *contract.Evidence) {
			resourceFirstFloor(e).Goroutines = 0
		}},
		{"same PID and SHA do not erase a cut boundary", "ASSERT-FAIL", "generation transition", func(e *contract.Evidence) {
			for i := range e.Samples {
				s := &e.Samples[i]
				if s.Stamp.Process.Key.Role == "agent" && s.Stamp.Generation == 2 {
					s.Stamp.Generation = 1
					s.ExeSHA = e.Samples[0].ExeSHA
					for j := range s.Resources {
						s.Resources[j].Owner.Generation = 1
					}
				}
			}
		}},
		{"same image needs five independent cycles", "INCOMPLETE", "fewer than five", func(e *contract.Evidence) {
			kept := e.Samples[:0]
			for _, s := range e.Samples {
				if s.Stamp.Process.Key.Role != "agent" || s.Stamp.Generation != 1 || s.Stamp.Sequence < 10 {
					kept = append(kept, s)
				}
			}
			e.Samples = kept
		}},
		{"resource cycle needs actual IO", "INCOMPLETE", "cycle lacks IO and retirement", func(e *contract.Evidence) {
			for i := range e.Intervals {
				if e.Intervals[i].Event == "write" {
					e.Intervals[i].Event = "activate"
				}
			}
		}},
		{"IO after retirement cannot prove a cycle", "INCOMPLETE", "cycle lacks IO and retirement", func(e *contract.Evidence) {
			for i := 0; i+1 < len(e.Intervals); i += 2 {
				if e.Intervals[i].Event == "write" && e.Intervals[i+1].Event == "retire" {
					e.Intervals[i].AtNS = e.Intervals[i+1].AtNS + 1
				}
			}
		}},
		{"incomplete census is not an empty census", "INCOMPLETE", "incomplete resource census", func(e *contract.Evidence) {
			resourceFirstFloor(e).Complete = false
		}},
		{"raw sample reference cannot disappear", "INCOMPLETE", "complete raw reference", func(e *contract.Evidence) {
			resourceFirstFloor(e).Raw = contract.RawRef{}
		}},
		{"raw path cannot be rebound to different bytes", "ASSERT-FAIL", "immutable resource raw reference changed", func(e *contract.Evidence) {
			e.Samples[1].Raw.Path = e.Samples[0].Raw.Path
		}},
		{"required population cannot contain duplicate rows", "ASSERT-FAIL", "duplicate required resource runtime", func(e *contract.Evidence) {
			e.Profile.Required = append(e.Profile.Required, e.Profile.Required[0])
		}},
		{"empty population is missing evidence", "INCOMPLETE", "resource population is empty", func(e *contract.Evidence) {
			e.Profile.Required, e.Samples = nil, nil
		}},
		{"kernel FD requires its independent OFD witness", "ASSERT-FAIL", "missing OFD witness", func(e *contract.Evidence) {
			resourceFirstFloor(e).Resources[0].OFD = ""
		}},
		{"negative resource quantity fails closed", "ASSERT-FAIL", "negative resource quantity", func(e *contract.Evidence) {
			resourceFirstFloor(e).Resources[0].Bytes = -1
		}},
		{"final collector goroutine must join before teardown", "ASSERT-FAIL", "final same-image goroutine floor", func(e *contract.Evidence) {
			e.Samples[len(e.Samples)-1].Goroutines += 3
		}},
		{"unknown resource class fails closed", "ASSERT-FAIL", "unknown resource class", func(e *contract.Evidence) {
			resourceFirstFloor(e).Resources[0].Kind = "allowed_retained"
		}},
		{"unknown resource state fails closed", "ASSERT-FAIL", "unknown resource class, state", func(e *contract.Evidence) {
			resourceFirstFloor(e).Resources[0].State = "allowed_retained"
		}},
		{"unknown resource domain fails closed", "ASSERT-FAIL", "unknown resource class, state, domain", func(e *contract.Evidence) {
			resourceFirstFloor(e).Resources[0].Domain = "other"
		}},
		{"handoff memfd must close before the serving census", "ASSERT-FAIL", "activated owner retains handoff", func(e *contract.Evidence) {
			resourceFirstFloor(e).Resources[3].Kind = "handoff-memfd"
		}},
		{"fixed label cannot hide a replay leak", "ASSERT-FAIL", "disguised as fixed baseline", func(e *contract.Evidence) {
			s := resourceFirstFloor(e)
			s.Resources = append(s.Resources, resourceRow(s.Stamp, "leak", "replay", "observer", "fixed", -1, 8<<20))
		}},
		{"permanent store lock cannot be released to satisfy zero", "ASSERT-FAIL", "permanent store-lock ownership", func(e *contract.Evidence) {
			for i := range e.Samples {
				s := &e.Samples[i]
				if s.Stamp.Process.Key.Role == "broker" && s.Stamp.Generation == 2 && s.Stamp.Sequence == 12 {
					s.Resources = resourceRemoveID(s.Resources, "store-lock")
					s.FDs--
					s.Locks = nil
				}
			}
		}},
		{"live private FD must be CLOEXEC", "ASSERT-FAIL", "lacks CLOEXEC", func(e *contract.Evidence) {
			resourceFirstFloor(e).Resources[3].Flags = 0
		}},
		{"metadata cannot smuggle an open FD", "ASSERT-FAIL", "metadata hides a kernel reference", func(e *contract.Evidence) {
			s := resourceFirstFloor(e)
			s.Resources[len(s.Resources)-1].FD = 43
			s.Resources[len(s.Resources)-1].OFD = "leaked"
			s.Resources[len(s.Resources)-1].Flags = 1
			s.FDs++
		}},
		{"duplicate FD census is not a multiset reference", "ASSERT-FAIL", "duplicate FD row", func(e *contract.Evidence) {
			s := resourceFirstFloor(e)
			s.Resources = append(s.Resources, s.Resources[0])
		}},
		{"duplicate retained record is not background noise", "ASSERT-FAIL", "duplicate retained metadata", func(e *contract.Evidence) {
			s := resourceFirstFloor(e)
			s.Resources = append(s.Resources, s.Resources[len(s.Resources)-1])
		}},
		{"pending transfer responsibility cannot disappear at final drain", "ASSERT-FAIL", "pending settlement responsibility", func(e *contract.Evidence) {
			for i := range e.Samples {
				s := &e.Samples[i]
				s.Resources = append(s.Resources, resourceRow(s.Stamp, "pending-grant", "transfer-grant", "transfer_worker", "pending", -1, 0))
			}
		}},
		{"temporary upgrade lock must retire", "ASSERT-FAIL", "final census changed", func(e *contract.Evidence) {
			e.Samples[len(e.Samples)-1].Locks = append(e.Samples[len(e.Samples)-1].Locks, "upgrade-lock")
		}},
		{"consumer and temp sets must end empty", "ASSERT-FAIL", "final consumer, temp or old-image", func(e *contract.Evidence) {
			s := &e.Samples[len(e.Samples)-1]
			s.Consumers, s.Temps = []string{"pull-consumer"}, []string{"partial-target"}
		}},
		{"old executable must retire independently", "ASSERT-FAIL", "final consumer, temp or old-image", func(e *contract.Evidence) {
			e.Samples[len(e.Samples)-1].OldImages = []string{"old-caddy"}
		}},
		{"fresh sample nonce cannot be replayed", "ASSERT-FAIL", "replayed or nonmonotonic", func(e *contract.Evidence) {
			e.Samples[1].Stamp.QueryNonce = e.Samples[0].Stamp.QueryNonce
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := resourceJudgeEvidence(2)
			tc.change(&e)
			requireResourceVerdict(t, e, tc.verdict, tc.diagnostic)
		})
	}
	t.Run("distinct retention clocks do not collect unacked replay", func(t *testing.T) {
		e := resourceJudgeEvidence(1)
		// Tombstones and revocations have separate deadlines. A terminal-unacked
		// task still keeps replay even though another task has already settled.
		resourceAddRetainedBackground(&e)
		requireResourceVerdict(t, e, "GREEN", "")
		for i := range e.Samples {
			s := &e.Samples[i]
			if s.Stamp.Process.Key.Role == "agent" && s.Stamp.Sequence == 2 {
				s.Resources = resourceRemoveKind(s.Resources, "replay")
				s.FDs--
				break
			}
		}
		requireResourceVerdict(t, e, "ASSERT-FAIL", "unacknowledged terminal lost exact receipt or replay")
	})
	t.Run("goroutine tolerance two remains frozen", func(t *testing.T) {
		e := resourceJudgeEvidence(2)
		resourceChangeFloors(&e, "agent", 1, func(s *contract.Sample, _ int) { s.Goroutines += 2 })
		requireResourceVerdict(t, e, "GREEN", "")
	})
	t.Run("rollback SHA and remapped FD preserve the original OFD", func(t *testing.T) {
		e := resourceJudgeEvidence(3)
		for i := range e.Samples {
			s := &e.Samples[i]
			if s.Stamp.Generation == 3 {
				s.ExeSHA = e.Samples[0].ExeSHA
				s.Stamp.Direction = "reverse"
				for j := range s.Resources {
					s.Resources[j].Owner.Direction = "reverse"
					if s.Resources[j].ID == "original-flow" {
						s.Resources[j].FD = 50
					}
				}
			}
		}
		for i := range e.Intervals {
			if e.Intervals[i].Stamp.Generation == 3 {
				e.Intervals[i].Stamp.Direction = "reverse"
			}
		}
		requireResourceVerdict(t, e, "GREEN", "")
	})
	t.Run("distinct expiries need actual sweep before collection", func(t *testing.T) {
		e := resourceJudgeEvidence(1)
		for i := range e.Samples {
			s := &e.Samples[i]
			if s.Stamp.Process.Key.Role != "agent" {
				continue
			}
			for k, kind := range []string{"tombstone", "revocation"} {
				if s.Stamp.Sequence < 12 {
					r := resourceRow(s.Stamp, kind, kind, "authority", "revoked", -1, 0)
					r.ReleaseAfterNS = 215 + int64(k)
					s.Resources = append(s.Resources, r)
				} else {
					resourceAddEvent(&e, s.Stamp, kind, "retire", 217+int64(k))
					s.Resources = append(s.Resources, resourceRow(s.Stamp, kind, "no-reexec-fence", "authority", "collected", -1, 0))
				}
			}
		}
		requireResourceVerdict(t, e, "GREEN", "")
		for i := range e.Intervals {
			if e.Intervals[i].ResourceID == "revocation" {
				e.Intervals[i].Event = "activate"
			}
		}
		requireResourceVerdict(t, e, "INCOMPLETE", "sweep witness")
	})
	t.Run("collector batches consumers temps and upgrade locks really retire", func(t *testing.T) {
		e := resourceJudgeEvidence(1)
		for i := range e.Samples {
			s := &e.Samples[i]
			if s.Stamp.Process.Key.Role != "fixture" || s.Stamp.Sequence%2 != 0 || s.Stamp.Sequence == 12 {
				continue
			}
			id := fmt.Sprintf("collector-%d", s.Stamp.Sequence)
			s.Resources = append(s.Resources, resourceRow(s.Stamp, id, "sealed-batch", "observer", "live", 42, 0),
				resourceRow(s.Stamp, id, "consumer", "observer", "live", -1, 0),
				resourceRow(s.Stamp, id, "temp", "observer", "live", -1, 0),
				resourceRow(s.Stamp, id, "lock", "upgrade", "live", 43, 0))
			s.FDs += 2
			s.Consumers, s.Temps = []string{id}, []string{id}
			s.Locks = append(s.Locks, id)
			resourceAddEvent(&e, s.Stamp, id, "read", s.Stamp.AtBootNS+1)
			resourceAddEvent(&e, e.Samples[i+1].Stamp, id, "retire", e.Samples[i+1].Stamp.AtBootNS-1)
		}
		requireResourceVerdict(t, e, "GREEN", "")
		for i := range e.Samples {
			if e.Samples[i].Stamp.Process.Key.Role == "fixture" && e.Samples[i].Stamp.Sequence == 2 {
				e.Samples[i].Consumers = nil
				break
			}
		}
		requireResourceVerdict(t, e, "ASSERT-FAIL", "classified resource sets")
	})
	t.Run("ReapHeld tail keeps its original waiter until explicit cleanup", func(t *testing.T) {
		e := resourceJudgeEvidence(1)
		for i := range e.Samples {
			s := &e.Samples[i]
			if s.Stamp.Process.Key.Role != "agent" {
				continue
			}
			if s.Stamp.Sequence == 12 {
				resourceAddEvent(&e, s.Stamp, "held-tail", "retire", s.Stamp.AtBootNS-1)
			} else {
				s.Resources = append(s.Resources, resourceRow(s.Stamp, "held-tail", "child", "exec", "reap-held", -1, 0),
					resourceRow(s.Stamp, "held-tail", "sole-waiter", "exec", "reap-held", -1, 0),
					resourceRow(s.Stamp, "held-tail", "pipe", "exec", "reap-held", 50, 0))
				s.FDs++
			}
		}
		requireResourceVerdict(t, e, "GREEN", "")
		e.Samples[1].Resources = resourceRemoveKind(e.Samples[1].Resources, "sole-waiter")
		requireResourceVerdict(t, e, "ASSERT-FAIL", "ReapHeld task lost its child, waiter or tail source")
	})
	t.Run("release receipt cannot hide a reference still owned by the old image", func(t *testing.T) {
		e := resourceJudgeEvidence(2)
		r := contract.Receipt{Kind: "old_generation_released", Stamp: e.Samples[11].Stamp,
			Known: true, Complete: true, RunningSHA: e.Samples[11].ExeSHA,
			ResourceIDs: []string{"original-flow"}, Evidence: resourceRaw("released")}
		e.Views = []contract.View{{Stamp: r.Stamp, Released: r}}
		requireResourceVerdict(t, e, "GREEN", "")
		e.Views[0].Released.Stamp.AtBootNS = e.Samples[10].Stamp.AtBootNS
		requireResourceVerdict(t, e, "ASSERT-FAIL", "released receipt still has an old owner reference")
	})
	t.Run("runtime kernel descriptors are fixed costs", func(t *testing.T) {
		e := resourceJudgeEvidence(1)
		for i := range e.Samples {
			for n, kind := range []string{"listener", "eventpoll", "eventfd", "file", "directory"} {
				s := &e.Samples[i]
				s.Resources = append(s.Resources, resourceRow(s.Stamp, "runtime-"+kind, kind, "control", "fixed", 60+n, 0))
				s.FDs++
			}
		}
		requireResourceVerdict(t, e, "GREEN", "")
	})
	t.Run("infra replacement cannot reset the host clock", func(t *testing.T) {
		e := resourceJudgeEvidence(1)
		a := e.Samples[0]
		a.Stamp.Process.Key.Role, a.Stamp.Process.Key.Unit = "nats", "nats-unit"
		a.Stamp.Process.PID = 500
		a.Resources, a.Goroutines, a.FDs = nil, 0, 0
		a.Raw = resourceRaw("nats-before")
		b := a
		b.Stamp.Process.PID++
		b.Stamp.Process.StartTicks++
		b.Stamp.Process.InstanceID = "infra-successor"
		b.Stamp.Generation, b.Stamp.CutID, b.Stamp.TxID = 2, "infra-cut", "infra-tx"
		b.Stamp.AtBootNS += 100
		b.Raw = resourceRaw("nats-after")
		e.Profile.Required = append(e.Profile.Required, a.Stamp.Process.Key)
		e.Samples = append(e.Samples, a, b)
		requireResourceVerdict(t, e, "GREEN", "")
		e.Samples[len(e.Samples)-1].Stamp.Process.BootID = "different-host-boot"
		requireResourceVerdict(t, e, "ASSERT-FAIL", "generation transition lost original process or cut identity")
	})
}

const resourceTestExpiry int64 = 1000000

func resourceJudgeEvidence(generations int) contract.Evidence {
	e := contract.Evidence{Profile: contract.Profile{ID: "resource-control", Topology: "single", Regime: "control", Tier: "hermetic"}}
	for roleIndex, role := range []string{"agent", "broker", "fixture"} {
		key := contract.Key{Role: role, Host: "sandbox", Unit: role + "-unit", SID: "session", NID: "node", Domain: role, UID: 1000}
		if role != "fixture" {
			e.Profile.Required = append(e.Profile.Required, key)
		}
		process := contract.Process{Key: key, BootID: "boot", PIDNS: "namespace", InstanceID: role + "-instance", PID: 101 + roleIndex, StartTicks: 42}
		var metadata []contract.Resource
		at := int64(100)
		for generation := 1; generation <= generations; generation++ {
			stamp := contract.Stamp{Process: process, TxID: fmt.Sprintf("tx-%d", generation), CutID: fmt.Sprintf("cut-%d", generation), Direction: "forward", ScopeDigest: "sealed-census", Generation: uint64(generation), Revision: 1}
			background := resourceFixedRows(stamp)
			background = append(background, resourceRow(stamp, "original-flow", "raw-socket", "flow", "live", len(background), 0))
			appendSample := func(rows []contract.Resource, g int64) contract.Sample {
				at += 10
				stamp.Sequence++
				stamp.AtBootNS = at
				stamp.QueryNonce = fmt.Sprintf("%s-%d-%d", role, generation, stamp.Sequence)
				s := contract.Sample{Stamp: stamp, ExeSHA: contract.Digest(sha256.Sum256([]byte(fmt.Sprint(generation)))), Goroutines: g, Complete: true, Raw: resourceRaw("sample-" + stamp.QueryNonce)}
				s.Resources = append([]contract.Resource{}, rows...)
				for i := range s.Resources {
					s.Resources[i].Owner = stamp
					if s.Resources[i].FD >= 0 {
						s.FDs++
					}
					if s.Resources[i].Kind == "lock" {
						s.Locks = append(s.Locks, s.Resources[i].ID)
					}
				}
				e.Samples = append(e.Samples, s)
				return s
			}
			baselineG := int64(12 - generation*2) // Exec resets heap costs; it cannot settle the old image's verdict.
			appendSample(append(append([]contract.Resource{}, background...), metadata...), baselineG)
			for round := 0; round < 5; round++ {
				id := fmt.Sprintf("task-%s-%d-%d", role, generation, round)
				domain := "exec"
				if round%2 == 1 {
					domain = "pty"
				}
				rows := append(append([]contract.Resource{}, background...), metadata...)
				rows = append(rows, resourceLiveTask(stamp, id, domain, len(background))...)
				live := appendSample(rows, baselineG+10)
				metadata = append(metadata, resourceSettledTask(stamp, id, domain)...)
				floor := appendSample(append(append([]contract.Resource{}, background...), metadata...), baselineG)
				resourceAddEvent(&e, live.Stamp, id, "write", live.Stamp.AtBootNS+1)
				resourceAddEvent(&e, floor.Stamp, id, "retire", floor.Stamp.AtBootNS-1)
			}
			if generation == generations {
				idle := appendSample(append(resourceFixedRows(stamp), metadata...), baselineG)
				resourceAddEvent(&e, idle.Stamp, "original-flow", "retire", idle.Stamp.AtBootNS-1)
			}
		}
	}
	return e
}

func resourceRow(stamp contract.Stamp, id, kind, domain, state string, fd int, bytes int64) contract.Resource {
	r := contract.Resource{ID: id, Kind: kind, Domain: domain, State: state, Owner: stamp, FD: fd, Bytes: bytes}
	if fd >= 0 {
		r.OFD = fmt.Sprintf("ofd-%s-%s-%d", id, kind, fd)
		if fd >= 3 {
			r.Flags = 1
		}
	}
	return r
}

func resourceFixedRows(stamp contract.Stamp) []contract.Resource {
	var rows []contract.Resource
	for fd := 0; fd < 3; fd++ {
		rows = append(rows, resourceRow(stamp, fmt.Sprintf("stdio-%d", fd), "pipe", "control", "fixed", fd, 0))
	}
	rows = append(rows, resourceRow(stamp, "custody", "custody-memfd", "custody", "fixed", 3, 0), resourceRow(stamp, "custody", "custody-seqpacket", "custody", "fixed", 4, 0))
	if stamp.Process.Key.Role == "broker" {
		rows = append(rows, resourceRow(stamp, "store-lock", "lock", "broker_store", "fixed", 5, 0))
	}
	return rows
}

func resourceLiveTask(stamp contract.Stamp, id, domain string, firstFD int) []contract.Resource {
	rows := []contract.Resource{resourceRow(stamp, id, "child", domain, "live", -1, 0), resourceRow(stamp, id, "sole-waiter", domain, "live", -1, 0)}
	n := 3
	if domain == "pty" {
		n = 2
		rows = append(rows, resourceRow(stamp, id, "pty-master", domain, "live", firstFD, 0), resourceRow(stamp, id, "pty-input", domain, "live", -1, 0), resourceRow(stamp, id, "pty-output", domain, "live", -1, 0))
		firstFD++
	} else {
		for i := 0; i < 3; i++ {
			rows = append(rows, resourceRow(stamp, id, "pipe", domain, "live", firstFD, 0))
			firstFD++
		}
	}
	for i := 0; i < n; i++ {
		rows = append(rows, resourceRow(stamp, id, "replay", domain, "live", firstFD, 8<<20))
		firstFD++
	}
	for _, kind := range []string{"lease", "retained-domain"} {
		r := resourceRow(stamp, id, kind, domain, "live", -1, 0)
		r.ReleaseAfterNS = resourceTestExpiry
		rows = append(rows, r)
	}
	return rows
}

func resourceSettledTask(stamp contract.Stamp, id, domain string) []contract.Resource {
	var rows []contract.Resource
	for _, kind := range []string{"lease", "terminal-receipt", "retained-domain"} {
		r := resourceRow(stamp, id, kind, domain, "settled-unexpired", -1, 0)
		r.ReleaseAfterNS = resourceTestExpiry
		rows = append(rows, r)
	}
	return rows
}

func resourceRaw(name string) contract.RawRef {
	return contract.RawRef{Path: "synthetic/" + name, SHA: contract.Digest(sha256.Sum256([]byte(name))), Bytes: int64(len(name)), Schema: "resource-control-v1"}
}

func resourceAddEvent(e *contract.Evidence, stamp contract.Stamp, id, event string, at int64) {
	stamp.AtBootNS = at
	e.Intervals = append(e.Intervals, contract.Interval{Stamp: stamp, ResourceID: id, Event: event, AtNS: at, Raw: resourceRaw(fmt.Sprintf("%s-%s-%s-%d", stamp.Process.Key.Role, id, event, at))})
}

func resourceFirstFloor(e *contract.Evidence) *contract.Sample {
	return &e.Samples[2]
}

func resourceChangeFloors(e *contract.Evidence, role string, generation uint64, change func(*contract.Sample, int)) {
	for i := range e.Samples {
		s := &e.Samples[i]
		if s.Stamp.Process.Key.Role == role && s.Stamp.Generation == generation && s.Stamp.Sequence >= 3 && s.Stamp.Sequence%2 == 1 {
			change(s, int(s.Stamp.Sequence/2))
		}
	}
}

func resourceRemoveKind(rows []contract.Resource, kind string) []contract.Resource {
	var kept []contract.Resource
	for _, r := range rows {
		if r.Kind != kind {
			kept = append(kept, r)
		}
	}
	return kept
}

func resourceRemoveID(rows []contract.Resource, id string) []contract.Resource {
	var kept []contract.Resource
	for _, r := range rows {
		if r.ID != id {
			kept = append(kept, r)
		}
	}
	return kept
}

func requireResourceVerdict(t *testing.T, e contract.Evidence, verdict, diagnostic string) contract.Result {
	t.Helper()
	r, err := JudgeResources(e)
	if err != nil || r.Verdict != verdict {
		t.Fatalf("want %s, got %s/%d, failures=%v missing=%v, err=%v", verdict, r.Verdict, r.RC, r.Failures, r.Missing, err)
	}
	wantRC := map[string]int{"GREEN": 0, "ASSERT-FAIL": 1, "INCOMPLETE": 4}[verdict]
	if r.RC != wantRC {
		t.Fatalf("native verdict/RC mismatch: %+v", r)
	}
	if diagnostic != "" && !strings.Contains(strings.Join(append(append([]string{}, r.Failures...), r.Missing...), "\n"), diagnostic) {
		t.Fatalf("missing diagnostic %q: failures=%v missing=%v", diagnostic, r.Failures, r.Missing)
	}
	return r
}

func resourceAddRetainedBackground(e *contract.Evidence) {
	for i := range e.Samples {
		s := &e.Samples[i]
		if s.Stamp.Process.Key.Role != "agent" {
			continue
		}
		for _, kind := range []string{"tombstone", "revocation", "history"} {
			r := resourceRow(s.Stamp, kind, kind, "authority", "tombstone", -1, 0)
			r.ReleaseAfterNS = resourceTestExpiry + int64(len(kind))
			s.Resources = append(s.Resources, r)
		}
		if s.Stamp.Sequence == 12 {
			s.Resources = append(s.Resources, resourceSettledTask(s.Stamp, "unacked", "exec")...)
			resourceAddEvent(e, s.Stamp, "unacked", "retire", s.Stamp.AtBootNS-1)
		} else {
			r := resourceRow(s.Stamp, "unacked", "terminal-receipt", "exec", "terminal-unacked", -1, 0)
			r.ReleaseAfterNS = resourceTestExpiry
			s.Resources = append(s.Resources, r, resourceRow(s.Stamp, "unacked", "replay", "exec", "terminal-unacked", 50, 8<<20))
			s.FDs++
		}
	}
}
