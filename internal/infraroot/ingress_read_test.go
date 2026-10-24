package infraroot

import (
	"crypto/sha256"
	"encoding/json"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/ingressswitch"
)

type sealReadFixture struct {
	rootFacts
	rules  ingressswitch.Snapshot
	change ingressswitch.Change
	seal   ingressswitch.Seal
	census c.Census
	reads  int
	shift  int
}

func (f *sealReadFixture) Read(c.Deadline, c.Stop) (ingressswitch.Snapshot, error) {
	f.reads++
	if f.shift != 0 && f.reads == f.shift {
		f.rules.Generation++
	}
	return f.rules, nil
}
func (*sealReadFixture) CompareAndSwap(ingressswitch.Snapshot, ingressswitch.Snapshot, c.Deadline, c.Stop) (ingressswitch.Snapshot, error) {
	panic("a read-only root verb attempted to change ingress")
}
func (f *sealReadFixture) Load(c.Digest) (ingressswitch.Change, error) { return f.change, nil }
func (*sealReadFixture) Probe(c.Process, bool, c.Deadline, c.Stop) (ingressswitch.ProbeProof, error) {
	panic("seal read attempted a route mutation probe")
}
func (f *sealReadFixture) Seal(ingressswitch.Snapshot, c.Process, c.Deadline, c.Stop) (ingressswitch.Seal, error) {
	return f.seal, nil
}
func (f *sealReadFixture) Census(c.ID, c.Deadline) (c.Census, error)  { return f.census, nil }
func (f *sealReadFixture) Inspect(c.ID, c.Deadline) (c.Census, error) { return f.census, nil }

func newSealReadFixture(returning bool) (*sealReadFixture, c.HostRequest) {
	primary, shadow := knownProcess("boot"), knownProcess("boot")
	shadow.PID--
	shadow.ExeInode++
	change := ingressswitch.Change{Version: 1, Domain: c.ID{1}, Tx: c.ID{2}, Step: 1,
		Before: ingressswitch.Snapshot{Version: 1, Generation: 4, Digest: digest("before rules"), Redirect: returning},
		After:  ingressswitch.Snapshot{Version: 1, Generation: 5, Redirect: !returning}, Primary: primary, Shadow: shadow,
		ReadyProof: digest("ready"), ExpectedJournal: digest("journal"), BeforeCensus: digest("before"), WarmCensus: digest("warm")}
	if returning {
		change.ReturnProof = digest("return")
	}
	change.After.Digest = ingressswitch.ChangeDigest(change)
	old, target := primary, shadow
	if returning {
		old, target = shadow, primary
	}
	census := c.Census{Complete: true, Generation: 7, Digest: digest("live census"), Processes: []c.Process{primary, shadow}}
	f := &sealReadFixture{rules: change.After, change: change, census: census,
		seal: ingressswitch.Seal{Version: 1, Rules: change.After, Owner: old, BarrierClosed: true,
			AcceptedComplete: true, ListenersComplete: true, SYNComplete: true, ConntrackComplete: true, Census: census}}
	encoded, _ := json.Marshal(target)
	return f, c.HostRequest{Version: 1, Op: c.VerifyIngress, Component: c.Caddy, Tx: change.Tx, Domain: change.Domain,
		Step: change.Step, Expected: old, BeforeCensus: census.Digest, Payload: sha256.Sum256(encoded)}
}

// origin: hot-update S9 plan §5.2 / WIRE-2 R11
func TestIngressReadRequiresCurrentSeal(t *testing.T) {
	for _, returning := range []bool{false, true} {
		for _, scenario := range []string{"current", "accepted", "syn", "conntrack", "incomplete", "open-barrier", "changed-rules", "changed-after-census", "wrong-target", "wrong-old", "wrong-tx", "stale-census"} {
			t.Run(string(rune('0'+boolInt(returning)))+"/"+scenario, func(t *testing.T) {
				f, q := newSealReadFixture(returning)
				switch scenario {
				case "accepted":
					f.seal.AcceptedTCP++
				case "syn":
					f.seal.PendingSYN++
				case "conntrack":
					f.seal.PendingConntrack++
				case "incomplete":
					f.seal.ConntrackComplete = false
				case "open-barrier":
					f.seal.BarrierClosed = false
				case "changed-rules":
					f.shift = 3
				case "changed-after-census":
					f.shift = 4
				case "wrong-target":
					q.Payload = digest("foreign target")
				case "wrong-old":
					q.Expected.PID--
				case "wrong-tx":
					q.Tx = c.ID{99}
				case "stale-census":
					q.BeforeCensus = digest("previous census")
				}
				switcher, err := ingressswitch.New(f, f, f)
				if err != nil {
					t.Fatal(err)
				}
				reader := &ingressReader{switcher, f}
				receipt, err := reader.Do(q, rootDeadline(t), nil)
				if scenario == "current" {
					if err != nil || receipt.Census.Digest != f.census.Digest || receipt.Current == q.Expected || receipt.Applied || f.reads < 4 {
						t.Fatalf("fresh sealed owner was not read: %+v reads=%d %v", receipt, f.reads, err)
					}
				} else if err == nil {
					t.Fatalf("%s authorized retirement from stale or incomplete evidence", scenario)
				}
			})
		}
	}
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
