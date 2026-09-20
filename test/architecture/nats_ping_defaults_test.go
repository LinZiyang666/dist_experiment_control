package architecture

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
)

// nats_ping_defaults_test.go — the NATS liveness probe is ONE contract written in three places,
// and they must say the same numbers.
//
// origin: simcluster-speed plan §5.4 H.
//
// The agent pings its broker every internal/agent.AgentPingInterval and declares the link dead
// after AgentMaxPingsOut unanswered PINGs (client half). The nats-server on the broker host runs
// `ping_interval`/`ping_max` from the nats.conf that scripts/install.sh writes (server half). The
// deploy-tier drill 98-stuck-redial-recovery derives its recovery budget from the same interval
// (`PING_INTERVAL_S`, present once that drill's budget is re-derived — see plan X14; until then the
// third leg is reported as absent, not as a mismatch).
//
// Why a gate and not a comment: the two product halves live in a Go constant and a shell heredoc.
// Nothing links them; one can be tuned (an operator "just bumping the server keepalive", a
// developer adjusting the agent) while the other keeps the old number, and the failure is not a
// broken build but a detection window silently different from the one the runbook and the drill
// describe. This is the same class as nats_server_pin_test.go (two names for one thing drifting
// four minors apart), applied to a timing contract.

// gate-control: TestNatsPingDefaultsGateSeesADrift

// install.sh carries ONE definition of the pair (NATS_PING_INTERVAL / NATS_PING_MAX, top of the script)
// that both the nats.conf template and the KEPT merge hint expand; the gate reads the definition and
// then requires the template lines to carry the variable in the load-bearing SHAPE (quoted duration,
// bare integer) and the hint to name the variables rather than hand-copied values (internal review
// round 1 R3-5 / R4-F4).
var (
	installPingIntervalRe = regexp.MustCompile(`(?m)^NATS_PING_INTERVAL="([^"]+)"\s*$`)
	installPingMaxRe      = regexp.MustCompile(`(?m)^NATS_PING_MAX=(\d+)\s*$`)
	installTemplateRefsRe = regexp.MustCompile(`(?m)^ping_interval: "\$NATS_PING_INTERVAL"\n(?:.*\n)*?ping_max: \$NATS_PING_MAX\s*$`)
	installHintRefsRe     = regexp.MustCompile(`ping_interval: \\"\$NATS_PING_INTERVAL\\"[\s\S]*?ping_max: \$NATS_PING_MAX`)
	// The positive hint pattern is satisfied by ANY one occurrence, and install.sh carries the hint
	// twice (a two-line form and a one-line form); the R3-5 regression — one of them re-typed as
	// literals — therefore stayed green (round-2 review R4-2-F4). The negative pattern closes it: no
	// operator-facing `log "…"` line may carry a literal ping value, whatever the other lines say.
	installHintLiteralRe = regexp.MustCompile(`(?m)^\s*log ".*ping_(?:interval: \\"[0-9]|max: [0-9])`)
	drillPingIntervalRe  = regexp.MustCompile(`(?m)^PING_INTERVAL_S=(\d+)\b`)
	drillPingMaxRe       = regexp.MustCompile(`(?m)^PING_MAX=(\d+)\b`)
)

// pingContract reads the three legs. A leg that is present but unparseable is returned with
// ok=false and reported as a failure. The drill leg used to be allowed to be absent ("nothing to
// reconcile yet"); since plan X14 landed drill 98 declares BOTH `PING_INTERVAL_S` and `PING_MAX`, and an
// absent or half-present pair is BLIND, not agreement (internal review round 1 R1-F2 / R3-2 / R4-F3:
// the first version reconciled three copies of a four-copy contract and read a deleted drill leg as
// green).
type pingContract struct {
	installInterval time.Duration
	installMax      int
	installOK       bool
	drillInterval   time.Duration
	drillMax        int
	drillOK         bool // both drill values present and integers
}

func readPingContract(t *testing.T, root string) pingContract {
	t.Helper()
	var c pingContract
	inst, err := os.ReadFile(filepath.Join(root, "scripts/install.sh"))
	if err != nil {
		t.Fatalf("read scripts/install.sh: %v", err)
	}
	if m := installPingIntervalRe.FindStringSubmatch(string(inst)); len(m) == 2 {
		if d, perr := time.ParseDuration(m[1]); perr == nil {
			c.installInterval = d
			if mm := installPingMaxRe.FindStringSubmatch(string(inst)); len(mm) == 2 {
				if n, nerr := strconv.Atoi(mm[1]); nerr == nil {
					c.installMax = n
					// The definition alone proves nothing: the template must EXPAND it in the two
					// accepted shapes, and EVERY KEPT hint must name the variables, not literals.
					c.installOK = installTemplateRefsRe.MatchString(string(inst)) && installHintRefsRe.MatchString(string(inst)) &&
						!installHintLiteralRe.MatchString(string(inst))
				}
			}
		}
	}
	drill, err := os.ReadFile(filepath.Join(root, "test/simcluster/drills/98-stuck-redial-recovery.sh"))
	if err != nil {
		t.Fatalf("read drill 98: %v", err)
	}
	if m := drillPingIntervalRe.FindStringSubmatch(string(drill)); len(m) == 2 {
		if n, nerr := strconv.Atoi(m[1]); nerr == nil {
			c.drillInterval = time.Duration(n) * time.Second
			if mm := drillPingMaxRe.FindStringSubmatch(string(drill)); len(mm) == 2 {
				if k, kerr := strconv.Atoi(mm[1]); kerr == nil {
					c.drillMax = k
					c.drillOK = true
				}
			}
		}
	}
	return c
}

func TestNatsPingDefaultsAgreeAcrossAgentInstallerAndDrill(t *testing.T) {
	c := readPingContract(t, repoRoot(t))
	if !c.installOK {
		t.Fatalf("scripts/install.sh no longer defines NATS_PING_INTERVAL=\"<dur>\" / NATS_PING_MAX=<n> once and expands " +
			"them into the nats.conf template as `ping_interval: \"$NATS_PING_INTERVAL\"` / `ping_max: $NATS_PING_MAX` " +
			"and into the KEPT merge hint.\n\n" +
			"The gate is blind rather than green: restore the single definition and both references (quoted duration, " +
			"bare integer — internal/natsconf refuses the other shapes) or fix the patterns in this file.")
	}
	if c.installInterval != agent.AgentPingInterval {
		t.Errorf("install.sh nats.conf ping_interval = %s, internal/agent.AgentPingInterval = %s.\n\n"+
			"The server half and the client half of the liveness probe disagree: a silent link is now "+
			"detected on a window neither the runbook nor drill 98 describes. Change both, in the same "+
			"commit.", c.installInterval, agent.AgentPingInterval)
	}
	if c.installMax != agent.AgentMaxPingsOut {
		t.Errorf("install.sh nats.conf ping_max = %d, internal/agent.AgentMaxPingsOut = %d — same rule.",
			c.installMax, agent.AgentMaxPingsOut)
	}
	if !c.drillOK {
		t.Fatalf("test/simcluster/drills/98-stuck-redial-recovery.sh no longer declares `PING_INTERVAL_S=<n>` and " +
			"`PING_MAX=<n>` at line start.\n\nThe gate is blind rather than green: the drill's recovery budget is " +
			"derived from those two values (plan X14); restore them or fix the pattern in this file.")
	}
	if c.drillInterval != agent.AgentPingInterval {
		t.Errorf("drill 98 PING_INTERVAL_S = %s, internal/agent.AgentPingInterval = %s.\n\n"+
			"The drill's recovery budget is derived from the product's interval; a stale value there "+
			"makes the budget measure a product that no longer exists (T7).", c.drillInterval, agent.AgentPingInterval)
	}
	if c.drillMax != agent.AgentMaxPingsOut {
		t.Errorf("drill 98 PING_MAX = %d, internal/agent.AgentMaxPingsOut = %d — same rule.", c.drillMax, agent.AgentMaxPingsOut)
	}
}

// gate-control for the gate above: each leg's reader must see a drift, on a synthetic tree so the
// control cannot depend on the repository being broken; and an agreeing tree must read as agreement.
func TestNatsPingDefaultsGateSeesADrift(t *testing.T) {
	dir := t.TempDir()
	write := func(rel, body string) {
		t.Helper()
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// Drifted installer (server half slower than the agent), drill absent → the drill leg is BLIND.
	write("scripts/install.sh", "#!/bin/sh\nNATS_PING_INTERVAL=\"2m\"\nNATS_PING_MAX=2\nlog \"ping_interval: \\\"$NATS_PING_INTERVAL\\\"\"\nlog \"ping_max: $NATS_PING_MAX\"\ncat <<EOF\nping_interval: \"$NATS_PING_INTERVAL\"\nping_max: $NATS_PING_MAX\nEOF\n")
	write("test/simcluster/drills/98-stuck-redial-recovery.sh", "#!/bin/sh\necho no budget here\n")
	c := readPingContract(t, dir)
	if !c.installOK || c.installInterval != 2*time.Minute || c.installMax != 2 {
		t.Fatalf("installer reader did not see 2m/2: %+v", c)
	}
	if c.drillOK {
		t.Fatalf("an absent PING_INTERVAL_S/PING_MAX pair was read as OK: %+v", c)
	}
	// Half a pair (interval without max, or max without interval) is blind too — R4-F3.
	write("test/simcluster/drills/98-stuck-redial-recovery.sh", "#!/bin/sh\nPING_INTERVAL_S=20\n")
	if readPingContract(t, dir).drillOK {
		t.Fatal("PING_INTERVAL_S without PING_MAX was read as OK")
	}
	write("test/simcluster/drills/98-stuck-redial-recovery.sh", "#!/bin/sh\nPING_MAX=2\n")
	if readPingContract(t, dir).drillOK {
		t.Fatal("PING_MAX without PING_INTERVAL_S was read as OK")
	}
	if c.installInterval == agent.AgentPingInterval {
		t.Fatalf("the synthetic drift equals the product constant; the control proves nothing")
	}
	// The wrong shapes the installer could regress to must read as BLIND (installOK=false), never as
	// agreement: a bare integer and an unquoted duration are exactly what natsconf refuses.
	write("scripts/install.sh", "#!/bin/sh\nNATS_PING_INTERVAL=20\nNATS_PING_MAX=2\ncat <<EOF\nping_interval: $NATS_PING_INTERVAL\nping_max: $NATS_PING_MAX\nEOF\n")
	if readPingContract(t, dir).installOK {
		t.Fatal("a bare-integer ping_interval was accepted by the installer reader")
	}
	// A definition whose template line or KEPT hint hand-copies a literal is blind too (R3-5 / R4-F4).
	write("scripts/install.sh", "#!/bin/sh\nNATS_PING_INTERVAL=\"20s\"\nNATS_PING_MAX=2\nlog \"ping_interval: \\\"20s\\\"\"\nlog \"ping_max: 2\"\ncat <<EOF\nping_interval: \"$NATS_PING_INTERVAL\"\nping_max: $NATS_PING_MAX\nEOF\n")
	if readPingContract(t, dir).installOK {
		t.Fatal("a KEPT hint with hand-copied literals was accepted by the installer reader")
	}
	write("scripts/install.sh", "#!/bin/sh\nNATS_PING_INTERVAL=\"20s\"\nNATS_PING_MAX=2\nlog \"ping_interval: \\\"$NATS_PING_INTERVAL\\\"\"\nlog \"ping_max: $NATS_PING_MAX\"\ncat <<EOF\nping_interval: \"20s\"\nping_max: 2\nEOF\n")
	if readPingContract(t, dir).installOK {
		t.Fatal("a template with hand-copied literals was accepted by the installer reader")
	}
	// round-2 review R4-2-F4: the REAL install.sh carries the hint more than once. One correct pair must
	// not launder a second, hand-copied one — in either of the two shapes the regression can take
	// (the whole two-line pair re-typed, or just the ping_max line).
	twoHints := func(second string) string {
		return "#!/bin/sh\nNATS_PING_INTERVAL=\"20s\"\nNATS_PING_MAX=2\n" +
			"log \"ping_interval: \\\"$NATS_PING_INTERVAL\\\"\"\nlog \"ping_max: $NATS_PING_MAX\"\n" +
			second +
			"cat <<EOF\nping_interval: \"$NATS_PING_INTERVAL\"\nping_max: $NATS_PING_MAX\nEOF\n"
	}
	write("scripts/install.sh", twoHints("log \"ping_interval: \\\"20s\\\"\"\nlog \"ping_max: 2\"\n"))
	if readPingContract(t, dir).installOK {
		t.Fatal("a second, fully hand-copied KEPT hint was laundered by the first correct one")
	}
	write("scripts/install.sh", twoHints("log \"ping_interval: \\\"$NATS_PING_INTERVAL\\\"\"\nlog \"ping_max: 2\"\n"))
	if readPingContract(t, dir).installOK {
		t.Fatal("a second KEPT hint with only ping_max hand-copied was laundered by the first correct one")
	}
	write("scripts/install.sh", twoHints("log \"assume ping_interval: \\\"$NATS_PING_INTERVAL\\\" / ping_max: $NATS_PING_MAX (one-line form)\"\n"))
	if !readPingContract(t, dir).installOK {
		t.Fatal("two variable-named KEPT hints (two-line and one-line forms) must read as OK")
	}
	// Agreeing tree, drill present and equal: no drift on any leg.
	write("scripts/install.sh", "#!/bin/sh\nNATS_PING_INTERVAL=\""+agent.AgentPingInterval.String()+"\"\nNATS_PING_MAX="+strconv.Itoa(agent.AgentMaxPingsOut)+"\nlog \"ping_interval: \\\"$NATS_PING_INTERVAL\\\"\"\nlog \"ping_max: $NATS_PING_MAX\"\ncat <<EOF\nping_interval: \"$NATS_PING_INTERVAL\"\nping_max: $NATS_PING_MAX\nEOF\n")
	write("test/simcluster/drills/98-stuck-redial-recovery.sh", "#!/bin/sh\nPING_INTERVAL_S="+strconv.Itoa(int(agent.AgentPingInterval/time.Second))+"\nPING_MAX="+strconv.Itoa(agent.AgentMaxPingsOut)+"\n")
	c = readPingContract(t, dir)
	if !c.installOK || c.installInterval != agent.AgentPingInterval || c.installMax != agent.AgentMaxPingsOut {
		t.Fatalf("agreeing installer read as drift: %+v", c)
	}
	if !c.drillOK || c.drillInterval != agent.AgentPingInterval || c.drillMax != agent.AgentMaxPingsOut {
		t.Fatalf("agreeing drill read as drift: %+v", c)
	}
	// And a drifted drill leg must be seen on EITHER value.
	write("test/simcluster/drills/98-stuck-redial-recovery.sh", "#!/bin/sh\nPING_INTERVAL_S=120\nPING_MAX="+strconv.Itoa(agent.AgentMaxPingsOut)+"\n")
	if c = readPingContract(t, dir); !c.drillOK || c.drillInterval != 120*time.Second {
		t.Fatalf("drifted drill interval not seen: %+v", c)
	}
	write("test/simcluster/drills/98-stuck-redial-recovery.sh", "#!/bin/sh\nPING_INTERVAL_S="+strconv.Itoa(int(agent.AgentPingInterval/time.Second))+"\nPING_MAX=7\n")
	if c = readPingContract(t, dir); !c.drillOK || c.drillMax != 7 {
		t.Fatalf("drifted drill PING_MAX not seen: %+v", c)
	}
}
