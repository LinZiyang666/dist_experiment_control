package upgradecli

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

func describedScope(key upgradetx.Key) upgradetx.Scope {
	return upgradetx.Scope{ID: strings.Repeat("9", 32), Participants: []upgradetx.Key{key}, Domains: allTestDomains(), Expected: []upgradetx.Resource{}}
}

func describedTarget(key upgradetx.Key, nonce string) upgradeport.Description {
	return upgradeport.Description{Key: key, Nonce: nonce, At: 1, RunningSHA: strings.Repeat("1", 64), SourceRelease: "v1", Scope: describedScope(key)}
}

// origin: S8 FIX-DESCRIBE (P19 request R1). The ctl builds a grant only from a
// description that answers its own selector and fresh nonce; a reply naming
// another component, a replayed nonce, a partial scope or an attached View is
// refused (75) before any candidate download, and a lost describe reply is a
// retryable deferral, never an accepted unknown (74).
func TestDescribeTargetRefusesForeignOrStaleDescriptions(t *testing.T) {
	key := remoteKey()
	selector := upgradewire.DescribeSelector(key)
	answer := func(mutate func(*upgradewire.Response, string)) Remote {
		return Remote{Exchange: func(_ context.Context, r upgradewire.Request) (upgradewire.Response, error) {
			if r.Op != upgradewire.OpDescribe || r.Key != selector || len(r.QueryNonce) != 32 {
				t.Fatalf("describe sent another envelope: %+v", r)
			}
			d := describedTarget(key, r.QueryNonce)
			resp := upgradewire.Response{Description: &d}
			mutate(&resp, r.QueryNonce)
			return resp, nil
		}}
	}
	target, candidate, err := answer(func(*upgradewire.Response, string) {}).DescribeTarget(t.Context(), selector)
	if err != nil || target.Key != key || target.SourceSHA != strings.Repeat("1", 64) || target.SourceRelease != "v1" || candidate != nil || len(target.Scope.Domains) != len(allTestDomains()) {
		t.Fatalf("honest description not adopted: %+v %v", target, err)
	}
	for name, mutate := range map[string]func(*upgradewire.Response, string){
		"other_component": func(r *upgradewire.Response, _ string) { r.Description.Key.ConfiguredNID = "other" },
		"replayed_nonce":  func(r *upgradewire.Response, _ string) { r.Description.Nonce = strings.Repeat("0", 32) },
		"missing_domain": func(r *upgradewire.Response, _ string) {
			r.Description.Scope.Domains = slices.DeleteFunc(r.Description.Scope.Domains, func(d upgradetx.Domain) bool { return d == upgradetx.DomainFlow })
		},
		"no_description":  func(r *upgradewire.Response, _ string) { r.Description = nil },
		"attached_view":   func(r *upgradewire.Response, _ string) { r.View.Code = upgradetx.CodeUnsettled },
		"unknown_running": func(r *upgradewire.Response, _ string) { r.Description.RunningSHA = "" },
	} {
		if _, _, err := answer(mutate).DescribeTarget(t.Context(), selector); ExitCode(err) != 75 {
			t.Fatalf("%s: foreign or stale description adopted (rc %d): %v", name, ExitCode(err), err)
		}
	}
	refused := Remote{Exchange: func(context.Context, upgradewire.Request) (upgradewire.Response, error) {
		return upgradewire.Response{Code: upgradetx.CodeDeferred, Error: "domain_census_unavailable"}, nil
	}}
	var typed *upgradetx.Error
	if _, _, err := refused.DescribeTarget(t.Context(), selector); !errors.As(err, &typed) || typed.Detail != "domain_census_unavailable" || ExitCode(err) != 75 {
		t.Fatalf("target refusal lost its reason: %v", err)
	}
	lost := Remote{Exchange: func(context.Context, upgradewire.Request) (upgradewire.Response, error) {
		return upgradewire.Response{}, errors.New("reply lost")
	}}
	if _, _, err := lost.DescribeTarget(t.Context(), selector); ExitCode(err) != 75 || errors.As(err, &typed) && typed.Accepted {
		t.Fatalf("a lost describe reply became an accepted unknown: %v", err)
	}
}

// origin: hot-update S8 plan §5.2 / §11.2 P19 (request R1); S8 FIX-DESCRIBE.
// `broker upgrade reload --prepare-only` prepares only from the broker's own
// description: its Key, running source and scope, and the reader declarations
// of the candidate staged at its canonical path, which must be exactly the
// operator's --to-version/--expect-sha256. A missing or mismatched candidate,
// a describe refusal or another role refuses before Prepare is sent.
func TestBrokerPrepareBindsTheStagedCandidate(t *testing.T) {
	key := upgradetx.Key{DomainID: strings.Repeat("d", 32), Component: upgradetx.ComponentBroker, ConfigPath: "/etc/tether/broker.yaml", UID: 0}
	staged := &upgradeport.CandidateDescription{SHA: strings.Repeat("2", 64), Release: "v2", CapabilityDigest: strings.Repeat("4", 64)}
	opts := UpgradeOptions{ToVersion: "v2", ExpectSHA256: staged.SHA, PrepareOnly: true}
	for _, tc := range []struct {
		name      string
		describe  func(*upgradewire.Response)
		opts      func(*UpgradeOptions)
		rc        int
		prepared  bool
		detailHas string
	}{
		{name: "staged_candidate", rc: 0, prepared: true},
		{name: "nothing_staged", describe: func(r *upgradewire.Response) { r.Description.Candidate = nil }, rc: 75, detailHas: "candidate_declarations_unavailable"},
		{name: "other_sha_staged", opts: func(o *UpgradeOptions) { o.ExpectSHA256 = strings.Repeat("3", 64) }, rc: 64},
		{name: "other_release_staged", opts: func(o *UpgradeOptions) { o.ToVersion = "v3" }, rc: 64},
		{name: "census_unavailable", describe: func(r *upgradewire.Response) {
			*r = upgradewire.Response{Code: upgradetx.CodeDeferred, Error: "domain_census_unavailable"}
		}, rc: 75, detailHas: "domain_census_unavailable"},
		{name: "agent_described", describe: func(r *upgradewire.Response) {
			r.Description.Key, r.Description.Scope = remoteKey(), describedScope(remoteKey())
		}, rc: 75},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			o := opts
			o.GrantOut = filepath.Join(t.TempDir(), "grant.json")
			if tc.opts != nil {
				tc.opts(&o)
			}
			prepares := 0
			var sent upgradetx.PrepareRequest
			ports := BrokerPorts{Actor: func(string) (string, error) { return "SHA256:local", nil },
				Call: func(_ context.Context, _ string, req adminsock.Request) (*adminsock.Response, error) {
					switch req.Op {
					case upgradewire.OpDescribe:
						d := describedTarget(key, req.Upgrade.QueryNonce)
						d.Candidate = staged
						resp := upgradewire.Response{Description: &d}
						if tc.describe != nil {
							tc.describe(&resp)
						}
						return &adminsock.Response{Upgrade: &resp}, nil
					case upgradewire.OpPrepare:
						prepares++
						sent = req.Upgrade.Prepare
						g := upgradetx.Grant{Handle: upgradetx.Handle{Key: key, TxID: strings.Repeat("a", 32), CutID: strings.Repeat("b", 32), Generation: 2, Revision: 1},
							ClientNonce: sent.ClientNonce, ActorFP: sent.ActorFP, Mode: sent.Mode, SourceRelease: sent.SourceRelease, TargetRelease: sent.TargetRelease,
							SourceSHA: sent.SourceSHA, TargetSHA: sent.TargetSHA, CapabilityDigest: sent.CapabilityDigest, ScopeDigest: strings.Repeat("3", 64), ExpiresAt: sent.ExpiresAt,
							Issuer: upgradetx.Owner{BootID: "boot", PID: 1, StartTicks: 2, InstanceID: strings.Repeat("e", 32)}}
						resp := upgradewire.Response{View: upgradetx.View{Historical: upgradetx.Record{Key: key, Grant: g}}}
						return &adminsock.Response{Upgrade: &resp}, nil
					}
					t.Fatalf("unexpected admin op %s", req.Op)
					return nil, nil
				}}
			ports.Describe = DescribeBroker(ports)
			grant, err := brokerPrepare(t.Context(), ports, "/run/tether/admin.sock", home, o, AdminEndpoint(ports, "/run/tether/admin.sock"))
			if ExitCode(err) != tc.rc || (prepares == 1) != tc.prepared || tc.detailHas != "" && (err == nil || !strings.Contains(err.Error(), tc.detailHas)) {
				t.Fatalf("rc=%d prepares=%d err=%v", ExitCode(err), prepares, err)
			}
			if !tc.prepared {
				if _, statErr := os.Stat(o.GrantOut); statErr == nil {
					t.Fatal("a refused prepare wrote a grant")
				}
				return
			}
			if sent.TargetSHA != staged.SHA || sent.TargetRelease != staged.Release || sent.CapabilityDigest != staged.CapabilityDigest ||
				sent.SourceSHA != strings.Repeat("1", 64) || sent.SourceRelease != "v1" || sent.ActorFP != "SHA256:local" || len(sent.Scope.Participants) != 1 || sent.Scope.Participants[0] != key {
				t.Fatalf("prepare was not built from the described broker and staged candidate: %+v", sent)
			}
			if recalled, err := RecallGrant(home, grant.Handle.TxID); err != nil || recalled != grant {
				t.Fatalf("prepared grant not remembered: %v", err)
			}
			if _, err := os.Stat(o.GrantOut); err != nil {
				t.Fatalf("grant not written for the stage step: %v", err)
			}
		})
	}
}

// origin: hot-update S8 plan §3.1 / §5.2; S8 FIX-DESCRIBE (P19 requests R3/R4).
// A registered agent binds exactly its registered journal under the owner the
// agent supplies (the journal directory is created owner-only inside the
// controlled control directory), an inherited image resolves the same
// registration without running the selector, and an unregistered component
// gets no ports at all (it runs exactly as before).
func TestRegisteredComponentsBindTheirOwnJournal(t *testing.T) {
	role, key, _ := bootFixture(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	plan, err := AgentBoot(role, true, logger)
	if err != nil || !plan.Registered || plan.Key != key || plan.Exec || plan.Journal != role.Journal(key, upgradelock.Registration{}) {
		t.Fatalf("inherited image did not resolve its registration: %+v %v", plan, err)
	}
	ports := AgentPorts(plan)
	if ports == nil || ports.Key != key || ports.Store != nil || ports.Open == nil {
		t.Fatalf("registered agent got no owner-late opener: %+v", ports)
	}
	owner, err := ProcessOwner(os.Getpid())
	if err != nil {
		if runtime.GOOS != "linux" {
			t.Skip("registered process identity requires Linux")
		}
		// origin: S10 internal review NO-SKIP-AS-PASS F3
		t.Fatalf("SETUP-RED: NOT_RUN pending S8 P18: process identity unavailable: %v", err)
	}
	owner.InstanceID = strings.Repeat("7", 32)
	store, err := ports.Open(owner)
	if err != nil {
		t.Fatalf("registered journal did not open: %v", err)
	}
	info, err := os.Stat(filepath.Dir(plan.Journal))
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("journal directory is not owner-only: %v %v", info, err)
	}
	var typed *upgradetx.Error
	if _, err := store.Load(key, ""); !errors.As(err, &typed) || typed.Code != upgradetx.CodeGrantUnknown {
		t.Fatalf("a fresh journal is not empty: %v", err)
	}
	if AgentPorts(BootPlan{}) != nil {
		t.Fatal("unregistered agent received transaction ports")
	}
	if deps, err := BrokerDependencies(BootPlan{}, nil); deps != nil || err != nil {
		t.Fatal("unregistered broker received transaction ports")
	}
	if plan, err := BrokerBoot("", ":memory:", "/etc/tether/broker.yaml", false, false, logger); plan.Registered || err != nil {
		t.Fatal("a broker without a stable data directory was registered")
	}
	unregistered := AgentBootRole(t.TempDir(), "lab", "n1", uint32(os.Geteuid()))
	if plan, err := AgentBoot(unregistered, true, logger); plan.Registered || err != nil {
		t.Fatalf("an unregistered inherited image claimed a registration: %+v %v", plan, err)
	}
}
