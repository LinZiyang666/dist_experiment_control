// These tests run the real controller and proof guards with typed physical and
// rule seams. They do not claim kernel nft, systemd or stock-Caddy continuity;
// those receipts belong to P29/P31/86. No installed binary or host rules run.
package ingressswitch

import (
	"crypto/sha256"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
)

// Linux tests register the privileged syscall/tool seams without executing
// them. Other platforms must explicitly prove unsupported instead of signing
// a kernel mechanism from a cross-build.
var ruleChecks func(*testing.T)
var markChecks func(*testing.T)

type memoryRules struct {
	current      Snapshot
	commits      int
	readErr      error
	casErr       error
	wrongAfter   bool
	readCount    int
	changeOnRead int
}

func (b *memoryRules) Read(_ infracontract.Deadline, _ infracontract.Stop) (Snapshot, error) {
	b.readCount++
	if b.readCount == b.changeOnRead {
		b.current.Generation++
	}
	return b.current, b.readErr
}

func (b *memoryRules) CompareAndSwap(before, after Snapshot, _ infracontract.Deadline, _ infracontract.Stop) (Snapshot, error) {
	if b.casErr != nil {
		return Snapshot{}, b.casErr
	}
	if b.current != before {
		return Snapshot{}, ErrConflict
	}
	b.commits++
	b.current = after
	if b.wrongAfter {
		after.Generation++
	}
	return after, nil
}

type oneRecord struct {
	change Change
	err    error
}

func (r *oneRecord) Load(_ infracontract.Digest) (Change, error) { return r.change, r.err }

type physicalEvidence struct {
	proof      ProbeProof
	seal       Seal
	census     infracontract.Census
	probeErr   error
	sealErr    error
	censusErr  error
	probes     int
	lastTarget infracontract.Process
	lastBypass bool
	rawClock   bool
}

func (e *physicalEvidence) Probe(p infracontract.Process, bypass bool, _ infracontract.Deadline, _ infracontract.Stop) (ProbeProof, error) {
	e.probes++
	e.lastTarget = p
	e.lastBypass = bypass
	if !e.rawClock {
		e.proof.StartedNS, _ = controlbudget.Now()
		e.proof.FinishedNS, _ = controlbudget.Now()
	}
	return e.proof, e.probeErr
}
func (e *physicalEvidence) Seal(_ Snapshot, _ infracontract.Process, _ infracontract.Deadline, _ infracontract.Stop) (Seal, error) {
	return e.seal, e.sealErr
}
func (e *physicalEvidence) Census(_ infracontract.ID, _ infracontract.Deadline) (infracontract.Census, error) {
	return e.census, e.censusErr
}

func digest(value byte) infracontract.Digest { return infracontract.Digest{value} }
func identity(pid int32) infracontract.Process {
	return infracontract.Process{BootID: "boot", PID: pid, StartTicks: uint64(pid) * 10, InvocationID: "invocation", ExeDevice: 2,
		ExeInode: uint64(pid) + 100, ExeSHA256: digest(3), Cgroup: "/system.slice/caddy.service"}
}
func completeCensus() infracontract.Census {
	return infracontract.Census{Complete: true, Generation: 7, Digest: digest(9), Processes: []infracontract.Process{identity(11), identity(22)}, LiveBusiness: 4}
}
func changeFixture(returning bool) Change {
	c := Change{Version: 1, Domain: infracontract.ID{1}, Tx: infracontract.ID{2}, Step: 3,
		Before: Snapshot{Version: 1, Generation: 4, Digest: digest(4), Redirect: returning},
		After:  Snapshot{Version: 1, Generation: 5, Redirect: !returning}, Primary: identity(11), Shadow: identity(22),
		ReadyProof: digest(6), ExpectedJournal: digest(7), BeforeCensus: digest(8), WarmCensus: digest(9)}
	if returning {
		c.ReturnProof = digest(10)
	}
	c.After.Digest = ChangeDigest(c)
	return c
}
func requestFixture(c Change) infracontract.HostRequest {
	return infracontract.HostRequest{Version: 1, Op: infracontract.SwitchIngress, Component: infracontract.Caddy, Tx: c.Tx, Step: c.Step, Domain: c.Domain,
		Sequence: 6, PolicyRevision: 1, KeyID: "local", HostBootID: "boot", Nonce: infracontract.ID{3}, Expected: c.Primary,
		ExpectedJournal: c.ExpectedJournal, BeforeCensus: c.BeforeCensus, WarmCensus: c.WarmCensus, ReturnProof: c.ReturnProof,
		Payload: c.After.Digest, RemainingNS: uint64(time.Second)}
}
func proofFixture(p infracontract.Process, bypass bool) ProbeProof {
	port, mark := "8443", uint32(0)
	if bypass {
		port, mark = "443", PrimaryMark
	}
	local := "127.0.0.1:" + port
	return ProbeProof{Version: 1, TLS: true, FirstAttempt: true, Mark: mark, StartedNS: 10, FinishedNS: 11, ReadyProof: digest(6),
		Client:   infracontract.Socket{NetNS: 1, Device: 2, Inode: 3, Cookie: 4, Local: "127.0.0.1:50000", Remote: local, Owner: identity(33)},
		Accepted: infracontract.Socket{NetNS: 1, Device: 2, Inode: 5, Cookie: 6, Local: local, Remote: "127.0.0.1:50000", Owner: p},
		Listener: infracontract.Socket{NetNS: 1, Device: 2, Inode: 7, Cookie: 8, Local: "0.0.0.0:" + port, Owner: p}}
}
func sealFixture(c Change) Seal {
	owner := c.Primary
	if !c.After.Redirect {
		owner = c.Shadow
	}
	return Seal{Version: 1, Rules: c.After, Owner: owner, BarrierClosed: true, AcceptedComplete: true,
		ListenersComplete: true, SYNComplete: true, ConntrackComplete: true, Census: completeCensus()}
}
func deadline(t *testing.T) infracontract.Deadline {
	t.Helper()
	value, err := controlbudget.After(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func controllerFixture(t *testing.T, returning bool) (*Switch, *memoryRules, *oneRecord, *physicalEvidence, infracontract.HostRequest) {
	t.Helper()
	c := changeFixture(returning)
	backend, records := &memoryRules{current: c.Before}, &oneRecord{change: c}
	target := c.Shadow
	if returning {
		target = c.Primary
	}
	e := &physicalEvidence{proof: proofFixture(target, returning), seal: sealFixture(c), census: completeCensus()}
	s, err := New(backend, records, e)
	if err != nil {
		t.Fatal(err)
	}
	return s, backend, records, e, requestFixture(c)
}

// origin: hot-update S9 plan §5.2 / T16
func TestCanonicalIngressAtomicSwitch(t *testing.T) {
	t.Run("closed-error-codes", func(t *testing.T) {
		if ErrConflict.Error() != "infra_busy" || ErrResultUnknown.Error() != "infra_result_unknown" || ErrBudgetExceeded.Error() != "infra_budget_exceeded" || !errors.Is(ErrProbe, infracontract.ErrIncomplete) {
			t.Fatal("ingress error classification drift")
		}
	})
	t.Run("immutable-record-size-bound", func(t *testing.T) {
		c := changeFixture(false)
		c.Shadow.Cgroup = strings.Repeat("x", infracontract.MaxRequestBytes)
		if ChangeDigest(c) != (infracontract.Digest{}) {
			t.Fatal("unbounded immutable record accepted")
		}
	})
	t.Run("kernel-rule-seam", func(t *testing.T) {
		if ruleChecks != nil {
			ruleChecks(t)
			return
		}
		if backend, err := NewLinux(); backend != nil || !errors.Is(err, infracontract.ErrUnsupported) {
			t.Fatalf("unsupported platform accepted: %v", err)
		}
	})
	t.Run("outbound-and-return-bind-real-target", func(t *testing.T) {
		for _, returning := range []bool{false, true} {
			s, b, r, e, req := controllerFixture(t, returning)
			receipt, err := s.Do(req, deadline(t), nil)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := infracontract.EncodeRequest(req)
			if err != nil {
				t.Fatal(err)
			}
			if b.commits != 1 || b.current != r.change.After || !receipt.Applied || receipt.Revision != 5 || receipt.Tx != req.Tx ||
				receipt.Op != infracontract.SwitchIngress || receipt.Sequence != req.Sequence || receipt.RequestDigest != sha256.Sum256(encoded) ||
				e.lastBypass != returning || e.lastTarget != receipt.Current {
				t.Fatalf("unbound switch receipt: %+v", receipt)
			}
			seal, err := s.RetirementSeal(r.change, deadline(t), nil)
			if err != nil || seal.Census.LiveBusiness != 4 {
				t.Fatalf("retirement must allow original live business: %v", err)
			}
		}
	})
	t.Run("malformed-request-has-no-effect", func(t *testing.T) {
		cases := []struct {
			name string
			edit func(*infracontract.HostRequest)
		}{
			{"version", func(r *infracontract.HostRequest) { r.Version++ }}, {"op", func(r *infracontract.HostRequest) { r.Op = infracontract.Inspect }},
			{"component", func(r *infracontract.HostRequest) { r.Component = infracontract.NATS }}, {"domain", func(r *infracontract.HostRequest) { r.Domain[0]++ }},
			{"tx", func(r *infracontract.HostRequest) { r.Tx[0]++ }}, {"step", func(r *infracontract.HostRequest) { r.Step++ }},
			{"sequence", func(r *infracontract.HostRequest) { r.Sequence = 0 }}, {"key", func(r *infracontract.HostRequest) { r.KeyID = "" }},
			{"nonce", func(r *infracontract.HostRequest) { r.Nonce = infracontract.ID{} }}, {"policy", func(r *infracontract.HostRequest) { r.PolicyRevision = 0 }},
			{"boot", func(r *infracontract.HostRequest) { r.HostBootID = "other" }}, {"payload", func(r *infracontract.HostRequest) { r.Payload[0]++ }},
			{"journal", func(r *infracontract.HostRequest) { r.ExpectedJournal[0]++ }}, {"before-census", func(r *infracontract.HostRequest) { r.BeforeCensus[0]++ }},
			{"warm-census", func(r *infracontract.HostRequest) { r.WarmCensus[0]++ }}, {"expected", func(r *infracontract.HostRequest) { r.Expected.StartTicks++ }},
			{"budget", func(r *infracontract.HostRequest) { r.RemainingNS = 0 }}, {"forbidden-config", func(r *infracontract.HostRequest) { r.Config = digest(1) }},
			{"return-proof", func(r *infracontract.HostRequest) { r.ReturnProof[0]++ }},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				s, b, _, _, req := controllerFixture(t, false)
				tc.edit(&req)
				if _, err := s.Do(req, deadline(t), nil); err == nil || b.commits != 0 {
					t.Fatalf("malformed request applied: %v", err)
				}
			})
		}
	})
	t.Run("immutable-record-and-generation", func(t *testing.T) {
		cases := []struct {
			name string
			edit func(*Change)
		}{
			{"version", func(c *Change) { c.Version++ }}, {"before-version", func(c *Change) { c.Before.Version++ }}, {"after-version", func(c *Change) { c.After.Version++ }},
			{"zero-domain", func(c *Change) { c.Domain = infracontract.ID{} }}, {"zero-tx", func(c *Change) { c.Tx = infracontract.ID{} }},
			{"skip-generation", func(c *Change) { c.After.Generation++ }}, {"overflow", func(c *Change) { c.Before.Generation = math.MaxUint64; c.After.Generation = 0 }},
			{"same-direction", func(c *Change) { c.After.Redirect = c.Before.Redirect }}, {"before-digest", func(c *Change) { c.Before.Digest = infracontract.Digest{} }},
			{"unknown-primary", func(c *Change) { c.Primary.Cgroup = "" }}, {"unknown-shadow", func(c *Change) { c.Shadow.ExeInode = 0 }},
			{"same-owner", func(c *Change) { c.Shadow = c.Primary }}, {"cross-boot", func(c *Change) { c.Shadow.BootID = "other" }},
			{"ready-proof", func(c *Change) { c.ReadyProof = infracontract.Digest{} }}, {"missing-return", func(c *Change) { c.ReturnProof = infracontract.Digest{} }},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				s, b, r, _, req := controllerFixture(t, true)
				tc.edit(&r.change)
				r.change.After.Digest = ChangeDigest(r.change)
				if validChange(r.change) {
					t.Fatal("invalid immutable root record passed admission")
				}
				req.Payload = r.change.After.Digest
				req.Expected = r.change.Primary
				req.ReturnProof = r.change.ReturnProof
				if _, err := s.Do(req, deadline(t), nil); err == nil || b.commits != 0 {
					t.Fatalf("invalid root record applied: %v", err)
				}
			})
		}
		s, b, r, _, req := controllerFixture(t, false)
		r.change.Before.Digest[0]++
		b.current = r.change.Before
		if _, err := s.Do(req, deadline(t), nil); err == nil || b.commits != 0 {
			t.Fatalf("changed immutable record applied: %v", err)
		}
	})
	t.Run("lost-reply-resolves-before-new-readiness", func(t *testing.T) {
		s, b, r, e, req := controllerFixture(t, false)
		b.current = r.change.After
		e.probeErr = errors.New("target later unavailable")
		receipt, err := s.Do(req, deadline(t), nil)
		if err != nil || !receipt.Applied || receipt.Revision != r.change.After.Generation || b.commits != 0 || e.probes != 0 {
			t.Fatalf("committed effect reapplied or lost: err=%v commits=%d probes=%d", err, b.commits, e.probes)
		}
	})
	t.Run("complete-process-and-socket-identities", func(t *testing.T) {
		for _, field := range []string{"boot", "pid", "ticks", "invocation", "device", "inode", "sha", "cgroup"} {
			p := identity(11)
			switch field {
			case "boot":
				p.BootID = ""
			case "pid":
				p.PID = 0
			case "ticks":
				p.StartTicks = 0
			case "invocation":
				p.InvocationID = ""
			case "device":
				p.ExeDevice = 0
			case "inode":
				p.ExeInode = 0
			case "sha":
				p.ExeSHA256 = infracontract.Digest{}
			case "cgroup":
				p.Cgroup = ""
			}
			if validProcess(p) {
				t.Fatalf("unknown %s became a process identity", field)
			}
		}
		for _, field := range []string{"netns", "device", "inode", "cookie", "local", "owner"} {
			s := proofFixture(identity(11), true).Accepted
			switch field {
			case "netns":
				s.NetNS = 0
			case "device":
				s.Device = 0
			case "inode":
				s.Inode = 0
			case "cookie":
				s.Cookie = 0
			case "local":
				s.Local = ""
			case "owner":
				s.Owner = infracontract.Process{}
			}
			if validSocket(s) {
				t.Fatalf("unknown %s became a socket identity", field)
			}
		}
	})
	t.Run("late-return-and-cancel", func(t *testing.T) {
		s, b, _, e, req := controllerFixture(t, true)
		b.current.Generation++
		if _, err := s.Do(req, deadline(t), nil); !errors.Is(err, ErrConflict) || b.commits != 0 || e.probes != 0 {
			t.Fatalf("late unflip: %v", err)
		}
		s, b, _, _, req = controllerFixture(t, false)
		stop := make(chan struct{})
		close(stop)
		if _, err := s.Do(req, deadline(t), stop); err == nil || b.commits != 0 {
			t.Fatal("canceled effect")
		}
		if _, err := s.Do(req, 0, nil); err == nil || b.commits != 0 {
			t.Fatal("zero deadline effect")
		}
		if _, err := s.Do(req, 1, nil); err == nil || b.commits != 0 {
			t.Fatal("expired effect")
		}
		if _, err := s.Do(req, 0, nil); !errors.Is(err, ErrBudgetExceeded) {
			t.Fatalf("invalid deadline lacks budget code: %v", err)
		}
	})
	t.Run("retirement-covers-entire-entrance", func(t *testing.T) {
		cases := []struct {
			name string
			edit func(*Seal)
		}{
			{"version", func(s *Seal) { s.Version++ }}, {"rules", func(s *Seal) { s.Rules.Generation++ }}, {"owner", func(s *Seal) { s.Owner.StartTicks++ }},
			{"no-barrier", func(s *Seal) { s.BarrierClosed = false }}, {"accepted-unknown", func(s *Seal) { s.AcceptedComplete = false }},
			{"listen-backlog-unknown", func(s *Seal) { s.ListenersComplete = false }}, {"syn-unknown", func(s *Seal) { s.SYNComplete = false }},
			{"conntrack-unknown", func(s *Seal) { s.ConntrackComplete = false }}, {"late-http-upgrade", func(s *Seal) { s.AcceptedTCP = 1 }},
			{"late-SYN", func(s *Seal) { s.PendingSYN = 1 }}, {"old-conntrack", func(s *Seal) { s.PendingConntrack = 1 }},
			{"incomplete", func(s *Seal) { s.Census.Complete = false }}, {"empty", func(s *Seal) { s.Census.Processes = nil }},
			{"generation", func(s *Seal) { s.Census.Generation = 0 }}, {"digest", func(s *Seal) { s.Census.Digest = infracontract.Digest{} }},
			{"old-owner-pending", func(s *Seal) { s.Census.OldOwnerPending = 1 }}, {"unknown-process", func(s *Seal) { s.Census.Processes[0].PID = 0 }},
			{"old-socket", func(s *Seal) { s.Census.Sockets = []infracontract.Socket{proofFixture(s.Owner, false).Accepted} }},
			{"duplicate-process", func(s *Seal) { s.Census.Processes = append(s.Census.Processes, s.Census.Processes[0]) }},
			{"socket-unobserved-owner", func(s *Seal) { s.Census.Sockets = []infracontract.Socket{proofFixture(identity(99), false).Accepted} }},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				s, b, r, e, _ := controllerFixture(t, false)
				b.current = r.change.After
				tc.edit(&e.seal)
				for round := 0; round < 2; round++ {
					if _, err := s.RetirementSeal(r.change, deadline(t), nil); err == nil {
						t.Fatal("unsealed old entrance retired")
					}
				}
			})
		}
		s, b, r, _, _ := controllerFixture(t, false)
		b.current = r.change.After
		b.changeOnRead = 2
		if _, err := s.RetirementSeal(r.change, deadline(t), nil); !errors.Is(err, ErrConflict) {
			t.Fatalf("changed barrier accepted: %v", err)
		}
	})
	t.Run("failures-never-manufacture-success", func(t *testing.T) {
		for _, stage := range []string{"load", "probe", "cas", "receipt"} {
			t.Run(stage, func(t *testing.T) {
				s, b, r, e, req := controllerFixture(t, false)
				injected := errors.New("injected")
				switch stage {
				case "load":
					r.err = injected
				case "probe":
					e.probeErr = injected
				case "cas":
					b.casErr = injected
				case "receipt":
					b.wrongAfter = true
				}
				if _, err := s.Do(req, deadline(t), nil); err == nil {
					t.Fatal("failure became success")
				}
			})
		}
		if _, err := New(nil, &oneRecord{}, &physicalEvidence{}); !errors.Is(err, infracontract.ErrContractStub) {
			t.Fatal("missing backend accepted")
		}
		if _, err := New(&memoryRules{}, nil, &physicalEvidence{}); !errors.Is(err, infracontract.ErrContractStub) {
			t.Fatal("missing records accepted")
		}
		if _, err := New(&memoryRules{}, &oneRecord{}, nil); !errors.Is(err, infracontract.ErrContractStub) {
			t.Fatal("missing evidence accepted")
		}
		var backend *memoryRules
		var records *oneRecord
		var evidence *physicalEvidence
		if _, err := New(backend, &oneRecord{}, &physicalEvidence{}); !errors.Is(err, infracontract.ErrContractStub) {
			t.Fatal("typed nil backend accepted")
		}
		if _, err := New(&memoryRules{}, records, &physicalEvidence{}); !errors.Is(err, infracontract.ErrContractStub) {
			t.Fatal("typed nil records accepted")
		}
		if _, err := New(&memoryRules{}, &oneRecord{}, evidence); !errors.Is(err, infracontract.ErrContractStub) {
			t.Fatal("typed nil evidence accepted")
		}
		s, _, _, e, _ := controllerFixture(t, false)
		e.census.Complete = false
		if _, err := s.Inspect(infracontract.ID{1}, deadline(t)); err == nil {
			t.Fatal("incomplete inspect accepted")
		}
	})
}

// origin: hot-update S9 plan §5.2 / T17
func TestPrimaryBypassCannotHitShadow(t *testing.T) {
	t.Run("healthy-main-return-positive", func(t *testing.T) {
		s, b, r, e, req := controllerFixture(t, true)
		receipt, err := s.Do(req, deadline(t), nil)
		if err != nil || b.commits != 1 || !e.lastBypass || e.lastTarget != r.change.Primary || receipt.Current != r.change.Primary {
			t.Fatalf("healthy main was not independently probed: %v", err)
		}
	})
	t.Run("mark-before-SYN", func(t *testing.T) {
		if markChecks != nil {
			markChecks(t)
		}
	})
	t.Run("main-dead-shadow-healthy", func(t *testing.T) {
		s, b, _, e, req := controllerFixture(t, true)
		e.proof = proofFixture(identity(22), true)
		if _, err := s.Do(req, deadline(t), nil); !errors.Is(err, ErrProbe) || b.commits != 0 {
			t.Fatalf("shadow signed main readiness: %v", err)
		}
	})
	cases := []struct {
		name string
		edit func(*ProbeProof)
	}{
		{"version", func(p *ProbeProof) { p.Version++ }}, {"tls", func(p *ProbeProof) { p.TLS = false }}, {"retry", func(p *ProbeProof) { p.FirstAttempt = false }},
		{"unmarked", func(p *ProbeProof) { p.Mark = 0 }}, {"other-mark", func(p *ProbeProof) { p.Mark++ }}, {"clock", func(p *ProbeProof) { p.StartedNS = 0 }},
		{"reversed-clock", func(p *ProbeProof) { p.FinishedNS = p.StartedNS - 1 }}, {"readiness", func(p *ProbeProof) { p.ReadyProof[0]++ }},
		{"cached-clock", func(p *ProbeProof) { p.StartedNS = 1; p.FinishedNS = 2 }},
		{"future-clock", func(p *ProbeProof) { p.StartedNS = math.MaxInt64 - 1; p.FinishedNS = math.MaxInt64 }},
		{"accepted-pid", func(p *ProbeProof) { p.Accepted.Owner.PID++ }}, {"accepted-ticks", func(p *ProbeProof) { p.Accepted.Owner.StartTicks++ }},
		{"accepted-exe", func(p *ProbeProof) { p.Accepted.Owner.ExeSHA256[0]++ }}, {"accepted-invocation", func(p *ProbeProof) { p.Accepted.Owner.InvocationID += "x" }},
		{"listener-owner", func(p *ProbeProof) { p.Listener.Owner.PID++ }}, {"listener-ns", func(p *ProbeProof) { p.Listener.NetNS++ }},
		{"client-cookie", func(p *ProbeProof) { p.Client.Cookie = 0 }}, {"accepted-inode", func(p *ProbeProof) { p.Accepted.Inode = 0 }},
		{"listener-device", func(p *ProbeProof) { p.Listener.Device = 0 }}, {"client-netns", func(p *ProbeProof) { p.Client.NetNS = 0 }},
		{"same-socket", func(p *ProbeProof) { p.Client.Inode = p.Accepted.Inode }}, {"same-cookie", func(p *ProbeProof) { p.Client.Cookie = p.Accepted.Cookie }},
		{"not-reversed-local", func(p *ProbeProof) { p.Client.Local = "127.0.0.1:50001" }}, {"not-reversed-remote", func(p *ProbeProof) { p.Client.Remote = "127.0.0.2:443" }},
		{"shadow-port", func(p *ProbeProof) { p.Accepted.Local = "127.0.0.1:8443"; p.Client.Remote = p.Accepted.Local }},
		{"wrong-listener-port", func(p *ProbeProof) { p.Listener.Local = "0.0.0.0:8443" }}, {"non-listener", func(p *ProbeProof) { p.Listener.Remote = "127.0.0.1:12345" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, b, r, e, req := controllerFixture(t, true)
			tc.edit(&e.proof)
			if tc.name == "clock" || tc.name == "reversed-clock" {
				if err := VerifyProbe(e.proof, r.change.Primary, true, r.change.ReadyProof); err == nil {
					t.Fatal("invalid standalone probe clock accepted")
				}
			}
			if tc.name == "clock" || tc.name == "reversed-clock" || tc.name == "cached-clock" || tc.name == "future-clock" {
				e.rawClock = true
			}
			if _, err := s.Do(req, deadline(t), nil); !errors.Is(err, ErrProbe) || b.commits != 0 {
				t.Fatalf("invalid bypass proof applied: %v", err)
			}
		})
	}
	t.Run("ipv6-reversed-pair", func(t *testing.T) {
		p := proofFixture(identity(11), true)
		p.Client.Local = "[::1]:50000"
		p.Client.Remote = "[::1]:443"
		p.Accepted.Local = p.Client.Remote
		p.Accepted.Remote = p.Client.Local
		p.Listener.Local = "[::]:443"
		if err := VerifyProbe(p, identity(11), true, digest(6)); err != nil {
			t.Fatal(err)
		}
	})
}
