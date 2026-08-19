package agent

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: docs/reviews/cloned-credential-instances-plan.md §2 D2
//
// D2 requires the instance lineage to be STRIPPED from managed child
// environments: a user command that itself starts an agent must begin a fresh
// lineage, or the broker reads the two processes as one instance reconnecting
// and hands them a single name — the clone fan-out, restored through the
// environment.
//
// effectiveChildEnv models what the kernel will actually hand the child:
// os/exec uses the CURRENT process environment verbatim when cmd.Env is nil.
func effectiveChildEnv(cmdEnv []string) []string {
	if cmdEnv == nil {
		return os.Environ()
	}
	return cmdEnv
}

func TestExecChildNeverInheritsTheInstanceLineage(t *testing.T) {
	t.Setenv(instanceIDEnv, "abcdefghijklmnopqrstuvwxyz")
	t.Setenv(routingNIDEnv, "lab-1-02")

	a := newTestAgent(t, fakeMountinfo([2]string{"/", "ext4"}), nil)
	cmd, d, err := a.buildExecCmd(&proto.ExecReq{Argv: []string{"echo", "hi"}})
	if err != nil {
		t.Fatalf("buildExecCmd: %v", err)
	}
	t.Logf("spawn decision: active=%v outage=%v cmd.Env==nil: %v", d.Active, d.Outage, cmd.Env == nil)

	env := effectiveChildEnv(cmd.Env)
	if v := envGet(env, instanceIDEnv); v != "" {
		t.Errorf("exec child inherits %s=%q; the lineage must not reach a managed child", instanceIDEnv, v)
	}
	if v := envGet(env, routingNIDEnv); v != "" {
		t.Errorf("exec child inherits %s=%q; the lineage must not reach a managed child", routingNIDEnv, v)
	}
}

// Same question for the healthy-hangable branch: a hangable mount exists (the
// reference deployment's ~/.tether IS an NFS mount) but no $PATH dir is dead,
// so the code takes the "byte-identical to legacy" arm.
func TestExecChildOnAHangableHostNeverInheritsTheInstanceLineage(t *testing.T) {
	t.Setenv(instanceIDEnv, "abcdefghijklmnopqrstuvwxyz")
	t.Setenv(routingNIDEnv, "lab-1-02")

	a := newTestAgent(t, fakeMountinfo([2]string{"/", "ext4"}, [2]string{"/mnt/nfs", "nfs"}), nil)
	cmd, d, err := a.buildExecCmd(&proto.ExecReq{Argv: []string{"echo", "hi"}})
	if err != nil {
		t.Fatalf("buildExecCmd: %v", err)
	}
	t.Logf("spawn decision: active=%v outage=%v cmd.Env==nil: %v", d.Active, d.Outage, cmd.Env == nil)

	env := effectiveChildEnv(cmd.Env)
	if v := envGet(env, instanceIDEnv); v != "" {
		t.Errorf("exec child inherits %s=%q on a hangable host", instanceIDEnv, v)
	}
	if v := envGet(env, routingNIDEnv); v != "" {
		t.Errorf("exec child inherits %s=%q on a hangable host", routingNIDEnv, v)
	}
}

// The PTY path (mergeChildEnv) is the one the plan's `tether run node -- bash`
// scenario names, and it DOES strip. Kept as the positive control so the two
// failures above cannot be read as a broken test harness.
func TestRunPTYChildNeverInheritsTheInstanceLineage(t *testing.T) {
	t.Setenv(instanceIDEnv, "abcdefghijklmnopqrstuvwxyz")
	t.Setenv(routingNIDEnv, "lab-1-02")

	env := mergeChildEnv(nil)
	if v := envGet(env, instanceIDEnv); v != "" {
		t.Errorf("pty child inherits %s=%q", instanceIDEnv, v)
	}
	if v := envGet(env, routingNIDEnv); v != "" {
		t.Errorf("pty child inherits %s=%q", routingNIDEnv, v)
	}
}

// origin: docs/reviews/cloned-credential-instances-plan.md §2 D2
//
// base32 with the lowercased standard alphabet emits [a-z2-7] only. The
// acceptor is [0-9a-z], so it admits 0/1/8/9 which the minter can never
// produce. Documented as [0-9a-z] in both places; this pins what is actually
// emitted so a future narrowing of the validator cannot silently reject live ids.
func TestMintedInstanceIDsUseTheDocumentedAlphabet(t *testing.T) {
	t.Setenv(instanceIDEnv, "")
	seen := map[rune]bool{}
	for i := 0; i < 400; i++ {
		id, err := mintInstanceID()
		if err != nil {
			t.Fatalf("mintInstanceID: %v", err)
		}
		if len(id) != instanceIDLen {
			t.Fatalf("length %d, want %d", len(id), instanceIDLen)
		}
		if err := proto.ValidateInstanceID(id); err != nil {
			t.Fatalf("minted id rejected by the wire validator: %v", err)
		}
		for _, r := range id {
			seen[r] = true
		}
	}
	var extra []string
	for r := range seen {
		if !strings.ContainsRune("abcdefghijklmnopqrstuvwxyz234567", r) {
			extra = append(extra, string(r))
		}
	}
	if len(extra) > 0 {
		t.Errorf("minted ids contain characters outside the base32 alphabet: %v", extra)
	}
	for _, r := range "0189" {
		if seen[r] {
			t.Errorf("minted id contained %q, which lowercase base32 cannot emit", string(r))
		}
	}
	t.Logf("distinct characters observed across 400 mints: %d", len(seen))
}

// origin: docs/reviews/cloned-credential-instances-plan.md §2 D2
//
// TETHER_ROUTING_NID exists for exactly one purpose: to carry a lease name
// THIS agent already adopted across its own syscall.Exec. The only legal
// values are cfg.NID itself and `<cfg.NID>-NN`. The restore accepts any
// ValidateNID-legal string, so a value inherited from an unrelated agent — or
// left in a unit file — retargets this agent's whole control plane (register
// subject, CONNECT name, heartbeat, tunnel REGISTER line) at a name it has no
// relationship to.
func TestRestoredRoutingNIDMustBelongToThisAgentsBasename(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  string
		want string
	}{
		{"foreign basename", "other-node", "lab-1"},
		{"foreign lease name", "other-node-02", "lab-1"},
		{"own lease name", "lab-1-02", "lab-1-02"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(routingNIDEnv, tc.env)
			a, err := New(Config{NATSURL: "nats://127.0.0.1:4222", SID: "lab", NID: "lab-1", RegisterTimeout: time.Second})
			if err != nil {
				t.Fatalf("agent.New: %v", err)
			}
			if got := nidOf(a); got != tc.want {
				t.Errorf("TETHER_ROUTING_NID=%q ⇒ routing nid %q, want %q", tc.env, got, tc.want)
			}
		})
	}
}
