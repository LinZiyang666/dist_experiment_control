package infrapriv

import (
	"crypto/ed25519"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
)

type managementTransport func(c.HostRequest, c.Deadline, c.Stop) (c.HostReceipt, error)

func (f managementTransport) Do(r c.HostRequest, d c.Deadline, stop c.Stop) (c.HostReceipt, error) {
	return f(r, d, stop)
}

// origin: hot-update S9 plan §5.2 / WIRE-2 R11
func TestManagementHostVerifiesFreshIngressReceipt(t *testing.T) {
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	authority := HostAuthority{Domain: c.ID{1}, BootID: "boot", Revision: 7, KeyID: "current",
		Public: key.Public().(ed25519.PublicKey), Sign: func(b []byte) ([]byte, error) { return ed25519.Sign(key, b), nil }}
	p := c.Plan{Tx: c.ID{2}, Step: 1, Domain: authority.Domain, Component: c.Caddy, PolicyRevision: 7, Before: c.Process{BootID: "boot"}}
	old := c.Process{BootID: "boot", PID: 10, StartTicks: 1, InvocationID: "unit", ExeDevice: 1, ExeInode: 2, ExeSHA256: c.Digest{3}, Cgroup: "/old"}
	target := old
	target.PID++
	census := c.Census{Complete: true, Generation: 1, Digest: c.Digest{4}, Processes: []c.Process{old, target}}
	for _, scenario := range []string{"current", "old-target", "cached-census", "incomplete", "pending", "effect", "wrong-digest", "refused"} {
		t.Run(scenario, func(t *testing.T) {
			calls := 0
			transport := managementTransport(func(r c.HostRequest, _ c.Deadline, _ c.Stop) (c.HostReceipt, error) {
				calls++
				if r.Op != c.VerifyIngress || r.Expected != old || r.BeforeCensus != census.Digest || c.VerifyRequest(r, authority.Public) != nil {
					t.Fatal("unsigned or mis-bound ingress request reached IPC")
				}
				data, err := c.EncodeRequest(r)
				if err != nil {
					t.Fatal(err)
				}
				out := c.HostReceipt{Tx: r.Tx, Sequence: r.Sequence, Revision: 3, Op: r.Op,
					RequestDigest: sha256.Sum256(data), Journal: c.Digest{5}, Current: target, Census: census}
				switch scenario {
				case "old-target":
					out.Current = old
				case "cached-census":
					out.Census.Digest = c.Digest{9}
				case "incomplete":
					out.Census.Complete = false
				case "pending":
					out.Census.OldOwnerPending++
				case "effect":
					out.Applied = true
				case "wrong-digest":
					out.RequestDigest = c.Digest{8}
				case "refused":
					return c.HostReceipt{}, c.ErrUnsupported
				}
				return out, nil
			})
			host, err := NewManagementHost(transport, authority)
			if err != nil {
				t.Fatal(err)
			}
			d, err := controlbudget.After(time.Second)
			if err != nil {
				t.Fatal(err)
			}
			err = host.VerifyCaddyIngress(p, old, target, census, d, nil)
			if scenario == "current" && err != nil {
				t.Fatal(err)
			}
			if scenario != "current" && err == nil {
				t.Fatal("unproven ingress authorized retirement")
			}
			if calls != 1 {
				t.Fatalf("current seal used %d IPC reads, want one", calls)
			}
		})
	}
}
