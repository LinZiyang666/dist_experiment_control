package broker

import (
	"database/sql"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// acct_nk_honesty_test.go (batch B, B4 — plan §15.2 "诚实的 ACCT.NK 列")
//
// WHAT WAS WRONG
//
// `cluster status` has always rendered an ACCT.NK column. Its producer was:
//
//	ns := adminsock.ClusterNodeStatus{... AccountNkMatch: true ...}
//
// — a literal, on every roster row, whether or not that node had answered anything. The legend even
// documented it ("currently always Y — per-node verification not yet wired"), which turned a
// fabricated signal into a documented one instead of a fixed one.
//
// The mirror half was undocumented and worse: the OTHER two construction sites (the orphan-voter row
// in clusterstatus.go, and the offline disk-snapshot row in cmd/tether/cluster.go) left the bool at
// its zero value, so they rendered **N** — "this broker's account key is WRONG" — for every node,
// without ever having compared a key. That is the view an operator runs during an outage.
//
// So one column carried two fabrications in opposite directions, and had no third state for
// "no answer". These tests pin the three-state contract against the REAL StatusReport, and
// specifically pin the two things a happy-path test would not notice: that "no answer" never renders
// as a verdict, and that the producer does not go back to stamping a literal.

const (
	acctKeySelf = "AC-SELF-KEY-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	acctKeyOdd  = "AC-ODD-KEY-BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
)

// acctAdmin builds a real single-voter ClusterAdmin whose roster carries `self` plus every peer in
// peers, so StatusReport produces a multi-row report without needing a real multi-node raft cluster.
// The peer rows are written directly (the b5SeedPortParent pattern) — the test needs the rows
// present, not replicated.
func acctAdmin(t *testing.T, self string, peers ...string) (*ClusterAdmin, *cluster.Node) {
	t.Helper()
	n, addr := d7SingleNode(t, self)
	admin := NewClusterAdmin(n, nil)
	in := d7JoinInput(t, self, addr)
	caughtUp := func(b uint64) (bool, error) { c, e := n.AppliedIndex(); return c >= b, e }
	if err := admin.AddNode(in, addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	for i, p := range peers {
		peer := p
		idx := i
		if err := n.BoundedStaleRead(func(db *sql.DB) error {
			_, err := db.Exec(`INSERT INTO cluster_nodes
				(node_id,name,node_ident_pub,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at)
				VALUES(?,?,?,?,?,?,?,?,?,?)`,
				peer, peer, "NIDENT-"+peer, "127.0.0.1:1900"+itoaSmall(idx), "nats://127.0.0.1:622"+itoaSmall(idx),
				"127.0.0.1:1443", "127.0.0.1", "SHA256:certfp-"+peer, phaseVoter, time.Now().UTC())
			return err
		}); err != nil {
			t.Fatalf("seed peer roster row %q: %v", peer, err)
		}
	}
	return admin, n
}

func itoaSmall(i int) string { return string(rune('0' + i)) }

func acctRow(t *testing.T, rep *adminsock.ClusterStatusReport, nodeID string) adminsock.ClusterNodeStatus {
	t.Helper()
	for _, n := range rep.Nodes {
		if n.NodeID == nodeID {
			return n
		}
	}
	t.Fatalf("no row for %q in %+v", nodeID, rep.Nodes)
	return adminsock.ClusterNodeStatus{}
}

// TestStatusReportAcctNkIsAThreeStateAnswer drives the REAL StatusReport and checks every
// combination that decides a row's verdict. This is the test the original code could not have
// passed: with `AccountNkMatch: true` stamped in the literal, the "peer did not answer" and "peer
// reported a different key" rows would both have come back Y.
func TestStatusReportAcctNkIsAThreeStateAnswer(t *testing.T) {
	admin, _ := acctAdmin(t, "brk-self", "brk-same", "brk-odd", "brk-silent", "brk-noseed")
	admin.accountPubSelf = func() string { return acctKeySelf }
	admin.healthPoll = func() map[string]proto.ClusterHealthResp {
		return map[string]proto.ClusterHealthResp{
			"brk-same": {NodeID: "brk-same", AccountNkPub: acctKeySelf, AccountNkReported: true},
			"brk-odd":  {NodeID: "brk-odd", AccountNkPub: acctKeyOdd, AccountNkReported: true},
			// Answered the probe, but predates the field: no account key reported.
			"brk-silent": {NodeID: "brk-silent"},
			// Answered, reports it has NO account seed at all. That is a genuine finding, not a
			// missing answer — a clustered broker with no account key cannot serve auth_callout.
			"brk-noseed": {NodeID: "brk-noseed", AccountNkReported: true},
		}
	}

	rep, err := admin.StatusReport("ctl-nats")
	if err != nil {
		t.Fatalf("StatusReport: %v", err)
	}

	for _, c := range []struct {
		node         string
		wantReported bool
		wantMatch    bool
		why          string
	}{
		{"brk-self", true, true, "the viewing broker's own key trivially equals itself; rendering ? on the node you are standing on reads as 'verification unavailable'"},
		{"brk-same", true, true, "reported the same key"},
		{"brk-odd", true, false, "reported a DIFFERENT key — the whole point of the column"},
		{"brk-silent", false, false, "answered the probe but predates the field: no verdict is possible, and Y here is the original defect"},
		{"brk-noseed", true, false, "reported that it holds NO account key: a real mismatch, distinguishable from silence"},
	} {
		row := acctRow(t, rep, c.node)
		if row.AccountNkReported != c.wantReported || row.AccountNkMatch != c.wantMatch {
			t.Errorf("%s: got (reported=%v match=%v), want (reported=%v match=%v) — %s",
				c.node, row.AccountNkReported, row.AccountNkMatch, c.wantReported, c.wantMatch, c.why)
		}
	}
}

// TestStatusReportGivesNoAcctVerdictWhenTheViewCannotNameItsOwnKey is the half most likely to be
// "simplified" away, and it is the one that protects the outage path.
//
// A view that does not know its own account key has no standing to call anyone else mismatched. If
// it compared against "" it would report N for every broker in the cluster — recreating the exact
// fabricated-N defect, during the incident where the operator trusts the output most.
func TestStatusReportGivesNoAcctVerdictWhenTheViewCannotNameItsOwnKey(t *testing.T) {
	for _, c := range []struct {
		name   string
		getter func() string
	}{
		{"getter unwired", nil},
		{"getter wired but no account seed", func() string { return "" }},
	} {
		t.Run(c.name, func(t *testing.T) {
			admin, _ := acctAdmin(t, "brk-self", "brk-odd")
			admin.accountPubSelf = c.getter
			admin.healthPoll = func() map[string]proto.ClusterHealthResp {
				return map[string]proto.ClusterHealthResp{
					"brk-odd": {NodeID: "brk-odd", AccountNkPub: acctKeyOdd, AccountNkReported: true},
				}
			}
			rep, err := admin.StatusReport("ctl-nats")
			if err != nil {
				t.Fatalf("StatusReport: %v", err)
			}
			for _, row := range rep.Nodes {
				if row.AccountNkReported {
					t.Errorf("%s reported an ACCT.NK verdict (match=%v) from a view that cannot "+
						"name its own account key. Comparing against \"\" makes every broker in the "+
						"cluster render N.", row.NodeID, row.AccountNkMatch)
				}
			}
			if adv := accountKeySkewAdvisory(rep.Nodes); adv != "" {
				t.Errorf("and it raised a skew advisory: %q", adv)
			}
		})
	}
}

// TestStatusReportSelfRowIsAuthoritativeWithoutAHealthPoll covers N=1, which is the entire
// production fleet today (racknerd is the sole broker). There is no healthPoll at all there, so a
// self row that waited for a health echo would render "?" on the one broker that can never be
// mismatched.
func TestStatusReportSelfRowIsAuthoritativeWithoutAHealthPoll(t *testing.T) {
	admin, _ := acctAdmin(t, "brk-solo")
	admin.accountPubSelf = func() string { return acctKeySelf }
	admin.healthPoll = nil // exactly the single-broker production shape

	rep, err := admin.StatusReport("ctl-nats")
	if err != nil {
		t.Fatalf("StatusReport: %v", err)
	}
	row := acctRow(t, rep, "brk-solo")
	if !row.AccountNkReported || !row.AccountNkMatch {
		t.Fatalf("the self row on a single-broker cluster must be a definite Y (reported=%v "+
			"match=%v). It is derived locally, not from a health echo, precisely because there is no "+
			"poll at N=1.", row.AccountNkReported, row.AccountNkMatch)
	}
}

// TestAccountNkFieldsSurviveTheWire pins that the two fields actually cross the JSON boundary.
// `omitempty` on a bool is the shape that silently drops a false, so this also pins that a
// reported-but-empty answer decodes as reported, and that a pre-v6 reply decodes as NOT reported
// rather than as a match.
func TestAccountNkFieldsSurviveTheWire(t *testing.T) {
	for _, c := range []struct {
		name string
		in   proto.ClusterHealthResp
	}{
		{"key + reported", proto.ClusterHealthResp{AccountNkPub: acctKeySelf, AccountNkReported: true}},
		{"reported with an empty key", proto.ClusterHealthResp{AccountNkReported: true}},
		{"not reported", proto.ClusterHealthResp{}},
	} {
		t.Run(c.name, func(t *testing.T) {
			raw, err := json.Marshal(c.in)
			if err != nil {
				t.Fatal(err)
			}
			var back proto.ClusterHealthResp
			if err := json.Unmarshal(raw, &back); err != nil {
				t.Fatal(err)
			}
			if back.AccountNkPub != c.in.AccountNkPub {
				t.Errorf("AccountNkPub %q survived as %q (%s)", c.in.AccountNkPub, back.AccountNkPub, raw)
			}
			if back.AccountNkReported != c.in.AccountNkReported {
				t.Errorf("AccountNkReported %v survived as %v (%s) — losing the flag makes a "+
					"reported-but-empty answer indistinguishable from an old broker's silence",
					c.in.AccountNkReported, back.AccountNkReported, raw)
			}
		})
	}

	var old proto.ClusterHealthResp
	if err := json.Unmarshal([]byte(`{"node_id":"brk-old","applied_index":7}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.AccountNkReported {
		t.Error("a pre-v6 reply decoded as REPORTED; the renderer would print a verdict for a " +
			"broker that never answered")
	}
	if proto.ClusterHealthSchemaVersion < 6 {
		t.Errorf("ClusterHealthSchemaVersion is %d; the account-key self-report is v6 and the "+
			"const is the documentation ledger for exactly this kind of additive field",
			proto.ClusterHealthSchemaVersion)
	}
}

// TestNoConstructionSiteStampsAccountNkLiterally is the structural half, and it is the one that
// would have caught the original defect. No behavioural test can: a hardcoded
// `AccountNkMatch: true` produces a green, plausible, fully-populated status report.
//
// testing-standards §S1 — this does not replace the behavioural checks above; it covers what they
// structurally cannot see.
func TestNoConstructionSiteStampsAccountNkLiterally(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "clusterstatus.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}

	sites := 0
	ast.Inspect(f, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := cl.Type.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if x, ok := sel.X.(*ast.Ident); !ok || x.Name != "adminsock" || sel.Sel.Name != "ClusterNodeStatus" {
			return true
		}
		sites++
		for _, elt := range cl.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := kv.Key.(*ast.Ident)
			if !ok || (key.Name != "AccountNkMatch" && key.Name != "AccountNkReported") {
				continue
			}
			if id, ok := kv.Value.(*ast.Ident); ok && (id.Name == "true" || id.Name == "false") {
				t.Errorf("%s: ClusterNodeStatus literal stamps %s: %s. That literal is how the "+
					"column came to say Y for every node it had never contacted — the value must be "+
					"DERIVED from a self-report, or left unset (which renders \"?\"), never asserted "+
					"in the constructor.", fset.Position(kv.Pos()), key.Name, id.Name)
			}
		}
		return true
	})

	// Non-vacuity: the walk must actually reach the construction sites it claims to inspect. There
	// are two in this file (the roster row and the orphan-voter row); a walk that found neither
	// would report "no literals" for the wrong reason.
	if sites < 2 {
		t.Fatalf("found only %d adminsock.ClusterNodeStatus literal(s) in clusterstatus.go; the "+
			"walk is not reaching the construction sites, so a stamped AccountNkMatch would pass "+
			"unnoticed", sites)
	}
}

// TestAccountKeySkewAdvisoryOnlyFiresOnRealMismatches pins the banner. The advisory exists because
// nobody diagnoses "agents fail to join at random" by scanning a table column — but an advisory that
// fired on unreported rows would be worse than none, since the offline view reports nothing at all
// and would carry a permanent false alarm.
func TestAccountKeySkewAdvisoryOnlyFiresOnRealMismatches(t *testing.T) {
	reported := func(id string, match bool) adminsock.ClusterNodeStatus {
		return adminsock.ClusterNodeStatus{NodeID: id, AccountNkReported: true, AccountNkMatch: match}
	}
	silent := func(id string) adminsock.ClusterNodeStatus {
		return adminsock.ClusterNodeStatus{NodeID: id}
	}

	if got := accountKeySkewAdvisory([]adminsock.ClusterNodeStatus{reported("a", true), reported("b", true)}); got != "" {
		t.Errorf("advisory fired on a fully-matching cluster: %q", got)
	}
	if got := accountKeySkewAdvisory([]adminsock.ClusterNodeStatus{silent("a"), silent("b"), silent("c")}); got != "" {
		t.Errorf("advisory fired on a view where NOTHING was reported: %q. That is every offline "+
			"disk-snapshot view and every pre-v6 cluster — a permanent false alarm.", got)
	}
	if got := accountKeySkewAdvisory(nil); got != "" {
		t.Errorf("advisory fired on an empty node list: %q", got)
	}

	adv := accountKeySkewAdvisory([]adminsock.ClusterNodeStatus{reported("brk-a", true), reported("brk-odd", false), silent("brk-c")})
	if adv == "" {
		t.Fatal("advisory did NOT fire on a real reported mismatch — the finding is then visible " +
			"only to an operator who happens to read the ACCT.NK column")
	}
	if !strings.Contains(adv, "brk-odd") {
		t.Errorf("the advisory must NAME the divergent broker, got %q", adv)
	}
	for _, mustNotName := range []string{"brk-a", "brk-c"} {
		if strings.Contains(adv, mustNotName) {
			t.Errorf("the advisory names %q, which is not divergent (brk-a matches, brk-c never "+
				"answered) — an operator would restart the wrong broker. Got %q", mustNotName, adv)
		}
	}
	if !strings.Contains(adv, "reconcile nats") {
		t.Errorf("the advisory must name the remedy, got %q", adv)
	}
}

// TestAccountKeyAdvisoryDoesNotTouchHealthOrExit pins the deliberate restraint. Health/ExitCode are a
// monitoring contract (`cluster status --json || alert`), and a key divergence — however real — must
// not silently start failing gates that never failed before. Stated as a test so the restraint is a
// decision on record rather than an omission someone "fixes".
func TestAccountKeyAdvisoryDoesNotTouchHealthOrExit(t *testing.T) {
	nodes := []adminsock.ClusterNodeStatus{
		{NodeID: "a", Phase: phaseVoter, Role: "leader", AccountNkReported: true, AccountNkMatch: true},
		{NodeID: "b", Phase: phaseVoter, Role: "voter", AccountNkReported: true, AccountNkMatch: false},
	}
	skewHealth, _, _ := computeHealth(false, "a", 2, 0, nodes)

	nodes[1].AccountNkMatch = true
	cleanHealth, _, _ := computeHealth(false, "a", 2, 0, nodes)

	if skewHealth != cleanHealth || healthExitCode(skewHealth) != healthExitCode(cleanHealth) {
		t.Fatalf("an ACCT.NK mismatch changed the health verdict (%q/%d vs %q/%d). It is an "+
			"ADVISORY by decision: widening what makes ExitCode non-zero is a monitoring-contract "+
			"change and belongs in its own increment with a runbook note.",
			skewHealth, healthExitCode(skewHealth), cleanHealth, healthExitCode(cleanHealth))
	}
}
