package architecture

import (
	"fmt"
	"go/ast"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
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

// Evaluate option operands from their actual constant declarations, not from
// the presence of a comment naming today's values. This accepts responsibility
// moves/aliases without requiring an infra leaf to import the Agent host.
// origin: hot-update S9 plan §10.2 / T83
func infraIntegerOperand(all map[string]transferDeclaration, decl transferDeclaration, expr ast.Expr, seen map[string]bool) (int64, error) {
	switch value := expr.(type) {
	case *ast.BasicLit:
		if value.Kind == token.INT {
			return strconv.ParseInt(value.Value, 0, 64)
		}
	case *ast.ParenExpr:
		return infraIntegerOperand(all, decl, value.X, seen)
	case *ast.BinaryExpr:
		a, err := infraIntegerOperand(all, decl, value.X, seen)
		if err != nil {
			return 0, err
		}
		b, err := infraIntegerOperand(all, decl, value.Y, seen)
		if err != nil {
			return 0, err
		}
		switch value.Op {
		case token.MUL:
			return a * b, nil
		case token.ADD:
			return a + b, nil
		case token.SUB:
			return a - b, nil
		}
	}
	pkg := filepath.ToSlash(filepath.Dir(decl.path))
	name := ""
	switch value := expr.(type) {
	case *ast.Ident:
		name = value.Name
	case *ast.SelectorExpr:
		if id, ok := value.X.(*ast.Ident); ok {
			imp := decl.imports[id.Name]
			if imp == "time" && value.Sel.Name == "Second" {
				return int64(time.Second), nil
			}
			pkg = strings.TrimPrefix(imp, modPrefix)
			name = value.Sel.Name
		}
	}
	key := pkg + ":" + name
	if name == "" || seen[key] {
		return 0, fmt.Errorf("unresolved/cyclic NATS ping operand %s", key)
	}
	nextSeen := map[string]bool{}
	for k, v := range seen {
		nextSeen[k] = v
	}
	nextSeen[key] = true
	for _, candidate := range all {
		if filepath.ToSlash(filepath.Dir(candidate.path)) == pkg && candidate.name == name {
			if value, ok := candidate.node.(*ast.ValueSpec); ok {
				for i, id := range value.Names {
					if id.Name == name && i < len(value.Values) {
						return infraIntegerOperand(all, candidate, value.Values[i], nextSeen)
					}
				}
			}
		}
	}
	return 0, fmt.Errorf("missing actual NATS ping constant %s", key)
}

func checkInfraPingProfile(sources map[string]string, root string) error {
	all, err := transferDeclarations(sources)
	if err != nil {
		return err
	}
	// The SDK declares package nats although its import path ends in nats.go.
	// A basename-only import map loses both the real call graph and operands;
	// explicit aliases already have their correct Go identifier and stay intact.
	// origin: S9 internal review GATE-CONVERGE-sliminfra F4
	for _, candidate := range all {
		if candidate.imports["nats.go"] == "github.com/nats-io/nats.go" {
			candidate.imports["nats"] = candidate.imports["nats.go"]
			delete(candidate.imports, "nats.go")
		}
	}
	decl, ok := all[root]
	if !ok {
		return fmt.Errorf("empty infra dial profile root %s", root)
	}
	calls, err := infraCallsFromDeclarations(all, root)
	if err != nil {
		return err
	}
	for _, name := range []string{"PingInterval", "MaxPingsOutstanding"} {
		if !calls["github.com/nats-io/nats.go:"+name] {
			return fmt.Errorf("%s: infra dial profile omits explicit %s", root, name)
		}
	}
	seen := map[string]int{}
	var failure error
	// The direct profile must own both final options. Passing a caller's SDK
	// defaults through cannot establish agreement with the server contract.
	ast.Inspect(decl.node, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok || decl.imports[id.Name] != "github.com/nats-io/nats.go" || (sel.Sel.Name != "PingInterval" && sel.Sel.Name != "MaxPingsOutstanding") {
			return true
		}
		seen[sel.Sel.Name]++
		if len(call.Args) != 1 {
			failure = fmt.Errorf("invalid ping profile operand")
			return false
		}
		value, err := infraIntegerOperand(all, decl, call.Args[0], map[string]bool{})
		want := int64(agent.AgentMaxPingsOut)
		if sel.Sel.Name == "PingInterval" {
			want = int64(agent.AgentPingInterval)
		}
		if err != nil || value != want {
			failure = fmt.Errorf("%s: %s=%d want=%d (%v)", root, sel.Sel.Name, value, want, err)
		}
		return true
	})
	if failure != nil {
		return failure
	}
	for _, name := range []string{"PingInterval", "MaxPingsOutstanding"} {
		if seen[name] != 1 {
			return fmt.Errorf("%s: missing/duplicate final ping option %s", root, name)
		}
	}
	return nil
}

// origin: hot-update S9 plan §10.2 / T83
// gate-control: TestInfraPingProfileControls
func TestInfraDialProfilesKeepPingContract(t *testing.T) {
	sources := brokerGateSources(t, repoRoot(t))
	for _, root := range []string{"internal/infraauth/owner.go:Owner.connect", "internal/infratransport/dial.go:Transport.Connect"} {
		if err := checkInfraPingProfile(sources, root); err != nil {
			t.Error(err)
		}
	}
}

// origin: hot-update S9 plan §11 / T83 / M31
func TestInfraPingProfileControls(t *testing.T) {
	path := "internal/infratransport/dial_other.go"
	site := path + ":Connect"
	good := `package owner;import n "github.com/nats-io/nats.go";import "time";func Connect(){n.PingInterval(20*time.Second);n.MaxPingsOutstanding(2)}`
	root := transferFixtureTree(t, map[string]string{path: good})
	check := func(source string) error {
		transferWriteFixture(t, root, path, source)
		return checkInfraPingProfile(brokerGateSources(t, root), site)
	}
	if err := check(good); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{strings.Replace(good, "20*time.Second", "21*time.Second", 1), strings.Replace(good, "n.MaxPingsOutstanding(2)", "", 1), strings.Replace(good, "n.PingInterval(20*time.Second)", "/* n.PingInterval(20*time.Second) */", 1), strings.Replace(good, "n.MaxPingsOutstanding(2)", "n.MaxPingsOutstanding(2);n.MaxPingsOutstanding(3)", 1)} {
		if check(bad) == nil {
			t.Fatal("on-disk ping profile mutation escaped")
		}
	}
}

// Exercise the exact production roots as well as the synthetic operand cases.
// A responsibility move must not leave a passing scanner pointed at old code.
// origin: S9 internal review GATE-CONVERGE-sliminfra F4
func TestInfraPingProfileControlsKeepProductionRoots(t *testing.T) {
	for _, site := range []struct{ path, name string }{
		{"internal/infraauth/owner.go", "Owner.connect"},
		{"internal/infratransport/dial.go", "Transport.Connect"},
	} {
		t.Run(site.name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(repoRoot(t), site.path))
			if err != nil {
				t.Fatal(err)
			}
			original := string(raw)
			root := transferFixtureTree(t, map[string]string{
				site.path:                           original,
				"internal/infratransport/unused.go": "package transport\nfunc unused() {}\n",
			})
			check := func(source string) error {
				transferWriteFixture(t, root, site.path, source)
				return checkInfraPingProfile(brokerGateSources(t, root), site.path+":"+site.name)
			}
			if err := check(original); err != nil {
				t.Fatal(err)
			}
			for _, option := range []string{"nats.PingInterval(20*time.Second),", "nats.MaxPingsOutstanding(2)"} {
				if strings.Contains(original, option+",") {
					option += ","
				}
				mutant := strings.Replace(original, option, "", 1)
				err := check(mutant)
				if mutant == original || err == nil || !strings.Contains(err.Error(), "omits explicit") {
					t.Fatalf("production ping option removal escaped: %s", option)
				}
			}
			if err := check(original); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(root, site.path)); err != nil {
				t.Fatal(err)
			}
			if checkInfraPingProfile(brokerGateSources(t, root), site.path+":"+site.name) == nil {
				t.Fatal("missing production profile root escaped")
			}
		})
	}
}
