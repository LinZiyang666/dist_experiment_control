package architecture

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// layering_test.go — every package-layering rule in the repo, in ONE table (S3 G3.5).
//
// WHERE THESE CAME FROM
// ---------------------
// They were four files: test/d{5,6,7,8}/regression_test.go, 380 lines holding 10 test functions and
// FOUR copies of goListDeps + moduleRoot. Each D-phase that touched the layering added its own file
// with its own copy of the helpers, so "how many layering rules does tether have?" had no single
// answer, and the same invariant was asserted up to four times in four wordings:
//
//	internal/cluster must stay NATS-free    asserted in d5, d6, d7 AND d8
//	internal/jsstream must not import cluster   asserted in d5 AND d8
//
// The duplicates were not identical, which is the real cost: d8's version of the cluster rule banned
// only nats.go, while d5's also banned nats.go/jetstream, nats-server, internal/broker and
// internal/jsstream. A reader who found the d8 one first would conclude the boundary was narrower
// than it is. This table carries the UNION of every clause from all four files -- nothing was dropped
// in the merge -- so the narrow restatements cannot mislead anyone again.
//
// Adding a rule is now one row instead of one file.
//
// NOT MOVED HERE (deliberately): the raft-determinism confinement lint in
// test/determinism/lint_skeleton_test.go. It is already in the right place and carries its own
// self-check; S3 §5 explicitly said to leave it and point at it from here, which this comment does.

// TestStackharnessIsImportedOnlyFromTests is the REVERSE direction the table below cannot express:
// test/stackharness may import product packages (that is why it exists — see its header), so
// nothing that ships may import it back. Any non-test .go file under cmd/ or internal/ naming it is
// red; and, G2, at least one _test.go must import it or the rule guards nothing.
// origin: docs/reviews/test-system-overhaul-plan.md B5 (§2.3).
func TestStackharnessIsImportedOnlyFromTests(t *testing.T) {
	root := repoRoot(t)
	const imp = `"github.com/LinZiyang666/tether/test/stackharness"`
	var offenders []string
	importers := 0
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") {
			return nil
		}
		src, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		if !strings.Contains(string(src), imp) {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if strings.HasSuffix(p, "_test.go") || strings.HasPrefix(rel, "test/") {
			importers++
			return nil
		}
		offenders = append(offenders, rel)
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(offenders) > 0 {
		t.Errorf("%d shipping file(s) import test/stackharness:\n  %s\n\nIt pulls product packages in the "+
			"other direction and is test scaffolding by construction.", len(offenders), strings.Join(offenders, "\n  "))
	}
	if importers < 8 {
		t.Fatalf("only %d test file(s) import test/stackharness — the eight absorbed seedSession forwarders "+
			"should, so either they moved or this scan is blind", importers)
	}
}

// Unlike stackharness, handofftest has no test/ directory exception: its descriptor
// owners are test fixtures, even when the importing package itself is a test helper.
// Parse imports so a comment or this gate's own path literal cannot satisfy non-vacuity.
// origin: hot-update S3 plan §9 (docs/reviews/hot-update-agent-bootstrap-plan.md).
func TestHandofftestIsImportedOnlyFromTests(t *testing.T) {
	root := repoRoot(t)
	const imp = modPrefix + "internal/handoff/handofftest"
	var offenders []string
	importers := 0
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), p, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range file.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			if path != imp {
				continue
			}
			if strings.HasSuffix(p, "_test.go") {
				importers++
			} else {
				rel, err := filepath.Rel(root, p)
				if err != nil {
					return err
				}
				offenders = append(offenders, filepath.ToSlash(rel))
			}
			break
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk imports: %v", err)
	}
	if len(offenders) > 0 {
		t.Errorf("%d non-test file(s) import internal/handoff/handofftest:\n  %s\nOnly _test.go importers may own these fixtures.",
			len(offenders), strings.Join(offenders, "\n  "))
	}
	if importers == 0 {
		t.Fatal("no _test.go file imports internal/handoff/handofftest — the gate is guarding an unused helper")
	}
}

// layerRule is one package's boundary.
//
// requiredTransitive is not decoration: it is the per-row self-check. `go list -deps` returning an
// empty or truncated set would make every bannedTransitive clause pass vacuously, so each row names
// something the package provably DOES depend on. A broken query then fails loudly instead of
// certifying the boundary it never examined.
type layerRule struct {
	pkg                string
	why                string
	bannedTransitive   []string
	requiredTransitive []string
	bannedDirect       []string
}

const modPrefix = "github.com/LinZiyang666/tether/"

var layerRules = []layerRule{

	// Extracted cluster responsibilities stay below the runtime and its carriers.
	// Direct parent bans are exact seams: a transitive parent-prefix ban would also name these leaves.
	// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM
	{
		pkg:                modPrefix + "internal/cluster/state",
		why:                "literal SQL planning and reads cannot acquire Raft, NATS, carrier or runtime ownership",
		bannedDirect:       []string{modPrefix + "internal/cluster"},
		bannedTransitive:   []string{"github.com/hashicorp/raft", "github.com/hashicorp/yamux", "github.com/nats-io/nats.go", "github.com/nats-io/nats-server/v2", modPrefix + "internal/broker", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/tunnel", modPrefix + "internal/flow", modPrefix + "internal/brokerimage", modPrefix + "internal/cli"},
		requiredTransitive: []string{"database/sql", modPrefix + "internal/auth", modPrefix + "internal/brokermaint"},
	},
	{
		pkg:                modPrefix + "internal/cluster/storelock",
		why:                "original lock OFD custody cannot acquire store, Raft, NATS, carrier or runtime ownership",
		bannedDirect:       []string{modPrefix + "internal/cluster"},
		bannedTransitive:   []string{"database/sql", "github.com/hashicorp/raft", "github.com/hashicorp/yamux", "github.com/nats-io/nats.go", modPrefix + "internal/storage", modPrefix + "internal/broker", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/tunnel", modPrefix + "internal/flow", modPrefix + "internal/brokerimage"},
		requiredTransitive: []string{"os", modPrefix + "internal/handoff"},
	},
	{
		pkg:                modPrefix + "internal/cluster/logbridge",
		why:                "bounded hclog rendering receives plain values without importing the Raft runtime or product owners",
		bannedTransitive:   []string{modPrefix + "internal", "github.com/hashicorp/raft", "github.com/hashicorp/yamux", "github.com/nats-io/nats.go"},
		requiredTransitive: []string{"log/slog", "github.com/hashicorp/go-hclog"},
	},
	{
		pkg:                modPrefix + "internal/cluster/routecert",
		why:                "route certificate preflight uses standard x509 verification below cluster and NATS runtime ownership",
		bannedTransitive:   []string{modPrefix + "internal", "github.com/hashicorp/raft", "github.com/hashicorp/yamux", "github.com/nats-io/nats.go"},
		requiredTransitive: []string{"crypto/x509", "encoding/pem"},
	},
	// The tunnel runtime supplies policy while these leaves retain byte and
	// descriptor responsibility. Direct parent bans preserve the package seam
	// without banning the legitimate sibling imports under internal/tunnel.
	// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
	{
		pkg:                modPrefix + "internal/tunnel/flowstate",
		why:                "flow wire and image codecs validate private values below live carrier and runtime owners",
		bannedDirect:       []string{modPrefix + "internal/tunnel"},
		bannedTransitive:   []string{modPrefix + "internal/tunnel/flowcarrier", modPrefix + "internal/tunnel/transport", modPrefix + "internal/broker", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/brokerimage", modPrefix + "internal/cluster", modPrefix + "cmd", "github.com/nats-io", "github.com/hashicorp/yamux", "github.com/hashicorp/raft"},
		requiredTransitive: []string{modPrefix + "internal/flow", modPrefix + "internal/sealedstate", modPrefix + "internal/rawsocket"},
	},
	{
		pkg:                modPrefix + "internal/tunnel/flowcarrier",
		why:                "replaceable yamux carriers consume flow values and cannot own authorization, listener or broker policy",
		bannedDirect:       []string{modPrefix + "internal/tunnel"},
		bannedTransitive:   []string{modPrefix + "internal/tunnel/transport", modPrefix + "internal/broker", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/brokerimage", modPrefix + "internal/cluster", modPrefix + "cmd", "github.com/nats-io", "github.com/hashicorp/raft"},
		requiredTransitive: []string{modPrefix + "internal/tunnel/flowstate", modPrefix + "internal/flow", "github.com/hashicorp/yamux"},
	},
	{
		pkg:                modPrefix + "internal/tunnel/transport",
		why:                "TLS negotiation, REGISTER byte retention and observations stay below endpoint and client runtimes",
		bannedDirect:       []string{modPrefix + "internal/tunnel"},
		bannedTransitive:   []string{modPrefix + "internal/tunnel/flowcarrier", modPrefix + "internal/broker", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/brokerimage", modPrefix + "internal/cluster", modPrefix + "cmd", "github.com/nats-io", "github.com/hashicorp/raft"},
		requiredTransitive: []string{modPrefix + "internal/tunnel/flowstate", "crypto/tls", modPrefix + "internal/drillobserve"},
	},

	// origin: hot-update S6 plan §9.3 / §12.1 P31 / T53.
	// Composition is below runtime owners; these bans also apply to AllOS
	// imports, where a Darwin twin cannot quietly reach back into the broker.
	{pkg: modPrefix + "internal/brokerimage", requiredTransitive: []string{modPrefix + "internal/brokerimage"}, why: "broker image composition cannot own a broker, CLI or agent runtime", bannedTransitive: []string{modPrefix + "internal/broker", modPrefix + "cmd", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy"}},
	{pkg: modPrefix + "internal/clusterio", requiredTransitive: []string{modPrefix + "internal/clusterio"}, why: "RPC accounting is a net/io leaf; raft wrappers belong in cluster", bannedTransitive: []string{"github.com/hashicorp/raft", "github.com/nats-io", modPrefix + "internal/cluster", modPrefix + "internal/broker", modPrefix + "cmd"}},
	{pkg: modPrefix + "internal/brokermaint", requiredTransitive: []string{modPrefix + "internal/brokermaint"}, why: "maintenance values and codecs are neutral leaves", bannedTransitive: []string{"github.com/nats-io", "github.com/hashicorp/raft", modPrefix + "internal/broker", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/tunnel", modPrefix + "internal/cluster", modPrefix + "internal/brokerimage", modPrefix + "cmd"}},
	{pkg: modPrefix + "internal/controlbudget", requiredTransitive: []string{modPrefix + "internal/controlbudget"}, why: "deadlines and attempts cannot depend on a live transport", bannedTransitive: []string{"github.com/nats-io", "github.com/hashicorp/raft", modPrefix + "internal/cli", modPrefix + "internal/broker", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/tunnel", modPrefix + "internal/cluster", modPrefix + "internal/brokerimage", modPrefix + "cmd"}},
	{pkg: modPrefix + "internal/listenerimage", requiredTransitive: []string{modPrefix + "internal/listenerimage"}, why: "listener keepers are below tunnel and runtime owners", bannedTransitive: []string{modPrefix + "internal/tunnel", modPrefix + "internal/brokerimage", modPrefix + "internal/broker", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", "github.com/nats-io", "github.com/hashicorp/raft"}},
	{pkg: modPrefix + "internal/cli", requiredTransitive: []string{modPrefix + "internal/cli"}, why: "CLI invokes product contracts without importing broker image implementation", bannedTransitive: []string{modPrefix + "internal/brokerimage"}},

	// The private codec cannot acquire live runtime or carrier dependencies.
	// origin: hot-update S5 plan §5.1 / §6.6 / §10.1 / T49
	{
		pkg:                modPrefix + "internal/ssstate",
		why:                "descriptor-validated private SS cuts use only sealed records and proxy metadata below live owners",
		bannedTransitive:   []string{modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/broker", modPrefix + "internal/flow", modPrefix + "internal/tunnel", modPrefix + "internal/execsession", modPrefix + "internal/agentimage", modPrefix + "internal/agentboot", "github.com/nats-io/nats.go", "github.com/hashicorp/yamux"},
		requiredTransitive: []string{modPrefix + "internal/sealedstate", modPrefix + "internal/proxyimage", "encoding/binary"},
	},
	// origin: hot-update S5 plan §6.6 / DRILL-OBSERVER.
	{
		pkg:                modPrefix + "internal/drillobserve",
		why:                "private drill custody cannot depend on data-plane or control-plane owners",
		bannedTransitive:   []string{modPrefix + "internal", "github.com/nats-io/nats.go", "github.com/hashicorp/yamux"},
		requiredTransitive: []string{"encoding/binary", "os"},
	},
	// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
	// The separate SS leaf needs an explicit reverse-edge ban as well as agent.
	{
		pkg:                modPrefix + "internal/agentidentity",
		why:                "process lineage and lease decisions cannot persist identity or depend on a control session",
		bannedTransitive:   []string{modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/broker", modPrefix + "internal/agentstate", modPrefix + "internal/tunnel", "github.com/nats-io/nats.go"},
		requiredTransitive: []string{modPrefix + "internal/proto", "crypto/rand"},
	},

	// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
	{
		pkg:                modPrefix + "internal/agentstate",
		why:                "durable state and discovery cache cannot depend on daemon or carrier lifetimes",
		bannedTransitive:   []string{modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/broker", modPrefix + "internal/tunnel", "github.com/nats-io/nats.go", "github.com/hashicorp/yamux"},
		requiredTransitive: []string{modPrefix + "internal/proto", "os"},
	},
	{
		pkg:                modPrefix + "internal/filetransfer",
		why:                "transfer byte work receives admission and cache callbacks without importing their agent owner",
		bannedTransitive:   []string{modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/broker", modPrefix + "internal/cluster", modPrefix + "internal/tunnel", "github.com/hashicorp/yamux"},
		requiredTransitive: []string{modPrefix + "internal/transferpath", modPrefix + "internal/jsstream", "github.com/nats-io/nats.go/jetstream"},
	},
	{
		pkg:                modPrefix + "internal/connrecovery",
		why:                "raw connection tracking and bounded teardown use standard-library callbacks below all product owners",
		bannedTransitive:   []string{modPrefix + "internal", "github.com/nats-io/nats.go", "github.com/hashicorp/yamux"},
		requiredTransitive: []string{"net", "context"},
	},

	// Replay and process actors are shared leaves, never clients of their
	// carrier adapters. A reverse import couples survival to session teardown.
	// origin: hot-update S5 plan §10.1 / T49.
	{
		pkg:                modPrefix + "internal/workload",
		why:                "shared replay/process owners cannot depend on their carrier or agent callers",
		bannedTransitive:   []string{modPrefix + "internal/flow", modPrefix + "internal/tunnel", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/broker", "github.com/hashicorp/yamux", "github.com/nats-io/nats.go"},
		requiredTransitive: []string{"os"},
	},

	// origin: hot-update S5 plan §2 / §10.1 — only descriptor and byte owners below carriers.
	{
		pkg:                modPrefix + "internal/rawsocket",
		why:                "original sockets cannot depend on the logical-flow or carrier owners that borrow them",
		bannedTransitive:   []string{modPrefix + "internal/flow", modPrefix + "internal/tunnel", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/broker", "github.com/hashicorp/yamux", "github.com/nats-io/nats.go"},
		requiredTransitive: []string{"os", "net/netip"},
	},
	{
		pkg:                modPrefix + "internal/sealedstate",
		why:                "private sealed cuts hold resources without knowing their flow, SS or carrier runtime",
		bannedTransitive:   []string{modPrefix + "internal/flow", modPrefix + "internal/tunnel", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/broker", "github.com/hashicorp/yamux", "github.com/nats-io/nats.go"},
		requiredTransitive: []string{modPrefix + "internal/handoff", modPrefix + "internal/rawsocket"},
	},
	{
		pkg:                modPrefix + "internal/flow",
		why:                "logical byte accounting and retirement must survive loss of any carrier or control session",
		bannedTransitive:   []string{modPrefix + "internal/tunnel", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/broker", "github.com/hashicorp/yamux", "github.com/nats-io/nats.go"},
		requiredTransitive: []string{modPrefix + "internal/rawsocket", modPrefix + "internal/sealedstate", modPrefix + "internal/workload"},
	},
	{
		pkg:                modPrefix + "internal/tunnel",
		why:                "yamux adapters are above flow owners and cannot reach the agent or broker that configure them",
		bannedTransitive:   []string{modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/broker"},
		requiredTransitive: []string{modPrefix + "internal/flow", "github.com/hashicorp/yamux"},
	},
	{
		pkg:                modPrefix + "internal/agent/ssproxy",
		why:                "SS receives typed binding/revoke adapters and cannot depend on agent or logical-flow runtimes",
		bannedTransitive:   []string{modPrefix + "internal/agent", modPrefix + "internal/broker", modPrefix + "internal/flow", modPrefix + "internal/tunnel", "github.com/nats-io/nats.go", "github.com/hashicorp/yamux"},
		requiredTransitive: []string{modPrefix + "internal/rawsocket", modPrefix + "internal/sealedstate"},
	},

	{
		pkg: modPrefix + "internal/testharness",
		why: "harness cycle rule (harness.go header; docs/testing-standards.md §七): fourteen package-broker " +
			"tests, seven package-agent tests and session's own test import testharness, so one import back " +
			"from it is a build-breaking cycle. Product-dependent primitives live in test/stackharness. " +
			"Internal review L5-F5: the rule was prose in the plan and in no header; now it is both.",
		bannedTransitive: []string{
			modPrefix + "internal/broker",
			modPrefix + "internal/agent",
			modPrefix + "internal/agent/ssproxy",
			modPrefix + "internal/session",
		},
		requiredTransitive: []string{
			modPrefix + "internal/storage",
			"github.com/nats-io/nats-server/v2/server",
		},
	},
	{
		pkg: modPrefix + "internal/cluster",
		why: "L-2: raft stays confined and NATS-free. The audit publisher (which needs NATS) lives in " +
			"internal/broker and reads the cluster through the raft-free primitives. internal/cluster " +
			"does import internal/auth for the join-PoP verify, and auth pulls in nats-io/nkeys (crypto) " +
			"-- that is NOT the nats.go client, and the distinction is the whole point of this row.",
		bannedTransitive: []string{
			"github.com/nats-io/nats.go",
			"github.com/nats-io/nats.go/jetstream",
			"github.com/nats-io/nats-server/v2",
			modPrefix + "internal/broker",
			modPrefix + "internal/jsstream",
			modPrefix + "internal/clusternodes",
		},
		requiredTransitive: []string{
			"github.com/hashicorp/raft",
			modPrefix + "internal/auth",
		},
	},
	{
		pkg: modPrefix + "internal/jsstream",
		why: "R-23: ReplicasFor takes nVoters as a plain int, so the stream layer never learns about raft.",
		bannedTransitive: []string{
			modPrefix + "internal/cluster",
		},
		requiredTransitive: []string{
			"github.com/nats-io/nats.go/jetstream",
		},
	},
	{
		pkg: modPrefix + "internal/clusternodes",
		why: "R-3 / L-2: the home seam is a pure-SQL leaf. It survived D6 and D7 adding membership, and " +
			"it must keep surviving.",
		bannedTransitive: []string{
			"github.com/nats-io/nats.go",
			"github.com/nats-io/nats.go/jetstream",
			modPrefix + "internal/cluster",
			modPrefix + "internal/broker",
		},
		requiredTransitive: []string{"database/sql"},
	},
	{
		pkg: modPrefix + "internal/xferaudit",
		why: "A pure render/replay leaf: it returns *cluster.Command and owns the AuditTransfer Aux, " +
			"which is what keeps internal/schema out of internal/cluster. It must never reach NATS.",
		bannedTransitive: []string{"github.com/nats-io/nats.go"},
		requiredTransitive: []string{
			modPrefix + "internal/cluster",
			modPrefix + "internal/schema",
		},
	},
	{
		pkg: modPrefix + "internal/clusteroffline",
		why: "L-2 confines raft to internal/cluster. The offline tool goes through " +
			"cluster.RecoverSingleNode, never through raft itself. This is a DIRECT-import rule: the " +
			"package legitimately reaches raft transitively via internal/cluster.",
		bannedDirect:       []string{"github.com/hashicorp/raft"},
		requiredTransitive: []string{modPrefix + "internal/cluster"},
	},
	// Extracted cluster control services receive broker policy through callbacks;
	// importing a runtime would turn those adapters into a dependency cycle.
	// origin: hot-update S6 plan §9.3 / BROKER-SLIM
	{
		pkg:          modPrefix + "internal/brokercontrol",
		why:          "cluster administration, audit and auth barriers stay below the broker runtime; raft is accessed only through cluster.Node.",
		bannedDirect: []string{"github.com/hashicorp/raft", modPrefix + "internal/tunnel"},
		bannedTransitive: []string{
			modPrefix + "internal/broker", modPrefix + "cmd", modPrefix + "internal/agent",
			modPrefix + "internal/agent/ssproxy", modPrefix + "internal/brokerimage",
		},
		requiredTransitive: []string{modPrefix + "internal/cluster"},
	},

	{
		pkg: modPrefix + "internal/broker",
		why: "L-2 confines raft to internal/cluster. cutover.go imports internal/cluster, not raft; the " +
			"mTLS transport and the Node both live in cluster.NewProduction. DIRECT-import rule for the " +
			"same reason as internal/clusteroffline.",
		bannedDirect:       []string{"github.com/hashicorp/raft"},
		requiredTransitive: []string{modPrefix + "internal/cluster"},
	},
	{
		pkg: modPrefix + "internal/agentimage",
		why: "hot-update S2 plan §3 / §6: the composed agent image is a leaf BELOW the agent — the agent " +
			"implements its courier adapter and calls Freeze/Restore; the image never reaches back into " +
			"internal/agent, internal/broker or the NATS client, so a bootstrap can run it before any " +
			"connection exists and its tests never need a carrier.",
		bannedTransitive: []string{
			modPrefix + "internal/agent",
			modPrefix + "internal/broker",
			"github.com/nats-io/nats.go",
			"github.com/nats-io/nats-server/v2/server",
		},
		requiredTransitive: []string{
			modPrefix + "internal/handoff",
			modPrefix + "internal/execsession",
			modPrefix + "internal/spawnsafe",
			modPrefix + "internal/workload",
		},
	},
	// origin: hot-update S4 plan §1.2 / §10 / T47.
	{
		pkg: modPrefix + "internal/ptysession",
		why: "the terminal actor owns duplex bytes and kernel state below execsession; it must " +
			"freeze and restore without an agent, broker or control connection.",
		bannedTransitive: []string{
			modPrefix + "internal/agent",
			modPrefix + "internal/agent/ssproxy",
			modPrefix + "internal/broker",
			modPrefix + "internal/execsession",
			"github.com/nats-io/nats.go",
			"github.com/nats-io/nats-server/v2",
		},
		requiredTransitive: []string{modPrefix + "internal/pty", modPrefix + "internal/workload"},
	},
	// origin: hot-update S4 plan §1.2 / §10 / T47.
	{
		pkg: modPrefix + "internal/pty",
		why: "the kernel terminal owner is below the agent and terminal actor; importing either " +
			"would bind descriptor ownership to a control-plane runtime.",
		bannedTransitive:   []string{modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/ptysession"},
		requiredTransitive: []string{modPrefix + "internal/workload"},
	},
	// origin: hot-update S4 plan §1.2 / §7.3 / §10 / T47.
	{
		pkg: modPrefix + "internal/terminalio",
		why: "local terminal cancellation and termios restoration use structural I/O interfaces; " +
			"neither network transport nor the agent or execclient may own their lifetime.",
		bannedTransitive: []string{
			"net",
			"github.com/nats-io/nats.go",
			"github.com/nats-io/nats-server/v2",
			modPrefix + "internal/agent",
			modPrefix + "internal/agent/ssproxy",
			modPrefix + "internal/execclient",
		},
		requiredTransitive: []string{"golang.org/x/term", "os"},
	},
	// origin: hot-update S3 plan §9 (docs/reviews/hot-update-agent-bootstrap-plan.md).
	{
		pkg: modPrefix + "internal/agentboot",
		why: "the bootstrap holds and restores the image before the agent or any control connection " +
			"exists; reaching agent, broker or NATS would reverse that ownership boundary.",
		bannedTransitive: []string{
			modPrefix + "internal/agent",
			modPrefix + "internal/broker",
			"github.com/nats-io/nats.go",
		},
		requiredTransitive: []string{modPrefix + "internal/agentimage", modPrefix + "internal/handoff"},
	},
	// origin: hot-update S4 plan §10.
	{
		pkg:                modPrefix + "internal/binaryupgrade",
		why:                "durable executable transactions receive image admission callbacks; disk state must not depend on an agent or carrier.",
		bannedTransitive:   []string{modPrefix + "internal/agent", modPrefix + "internal/broker", "github.com/nats-io/nats.go"},
		requiredTransitive: []string{modPrefix + "internal/agentimage", modPrefix + "internal/agentboot"},
	},
	// origin: hot-update S4 plan §10.
	{
		pkg:                modPrefix + "internal/transferpath",
		why:                "filesystem containment and directory-pinned operations must stay independent of agents and transport.",
		bannedTransitive:   []string{modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/broker", "github.com/nats-io/nats.go"},
		requiredTransitive: []string{"golang.org/x/sys/unix"},
	},
	// origin: hot-update S3 plan §6 / §9 (docs/reviews/hot-update-agent-bootstrap-plan.md).
	{
		pkg: modPrefix + "internal/upgradebudget",
		why: "every layer reads the upgrade budgets, so they must remain a leaf with no dependency " +
			"back into this module.",
		bannedTransitive:   []string{strings.TrimSuffix(modPrefix, "/")},
		requiredTransitive: []string{"time"},
	},
}

// goListDeps returns the transitive import set of pkg.
func goListDeps(t *testing.T, root, pkg string) map[string]bool {
	t.Helper()
	return runGoList(t, root, pkg, "-deps", "{{.ImportPath}}")
}

// goListDirectImports returns only pkg's own import statements.
func goListDirectImports(t *testing.T, root, pkg string) map[string]bool {
	t.Helper()
	return runGoList(t, root, pkg, "", "{{join .Imports \"\\n\"}}")
}

func runGoList(t *testing.T, root, pkg, mode, format string) map[string]bool {
	t.Helper()
	args := []string{"list"}
	if mode != "" {
		args = append(args, mode)
	}
	args = append(args, "-f", format, pkg)
	cmd := exec.Command("go", args...)
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go %s: %v", strings.Join(args, " "), err)
	}
	set := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			set[line] = true
		}
	}
	return set
}

// cacheKeyBallast makes this test's result depend on the CONTENT of the tree, so Go's test cache
// cannot hand back a stale PASS after an import edit.
//
// origin: line-2 external review F4 (deletions lane), independently reproduced by the severity lane.
// Every judgement here comes from `exec.Command("go", "list", ...)`, and a subprocess's output does not
// enter the test binary's testlog — so it is not part of the cache key. Measured: inject a real
// violation, run `go test ./test/architecture/ -run TestPackageLayering`, and Go replies `(cached)`
// GREEN. `-count=1` reddens, and the whole package reddens, but `-run <one gate>` is exactly the
// command a developer types to check the gate they just touched.
//
// It happened to be safe only by accident: a SIBLING test in this package (build_tags_test.go)
// ReadFile()s the whole tree, which put every .go file in the testlog and invalidated the package's
// cache entry for free. That is coupling, not a guarantee — deleting or narrowing that test would have
// silently reintroduced the stale PASS here. Reading the files this gate actually reasons about makes
// the dependency explicit and local.
//
// IT MUST COVER THE TRANSITIVE CLOSURE, NOT THE SIX RULED DIRECTORIES. origin: line-2 closure
// verification M11. The first version read only the packages named in layerRules, while five of the six
// rules judge `go list -deps` — the TRANSITIVE import set. So an import added in a package one hop away
// (internal/auth is the row's own documented near-miss edge: internal/cluster is allowed to reach it, and
// auth pulling in nats.go would violate the cluster row) changed the verdict without changing any file
// this function had read. `-run TestPackageLayering` then returned a cached PASS on a real violation, and
// the comment above claimed the dependency was explicit.
//
// The set comes from `go list -deps` on each ruled package, filtered to this module — i.e. exactly the
// files whose content the gate's judgement depends on. Reading more than the six directories is the
// point; reading the whole tree would work too but would put this gate's cache key at the mercy of every
// unrelated edit, which is the coupling the comment above objects to.
func cacheKeyBallast(t *testing.T, root string) {
	t.Helper()

	// Collect every in-module package the ruled packages transitively depend on, plus the ruled ones.
	dirs := map[string]bool{}
	for _, rule := range layerRules {
		dirs[strings.TrimPrefix(rule.pkg, modPrefix)] = true
		for dep := range goListDeps(t, root, rule.pkg) {
			if strings.HasPrefix(dep, modPrefix) {
				dirs[strings.TrimPrefix(dep, modPrefix)] = true
			}
		}
	}
	if len(dirs) <= len(layerRules) {
		t.Fatalf("cache ballast covers %d package(s) for %d rules — `go list -deps` returned nothing "+
			"in-module, so the ballast is back to the six ruled directories and a violation one hop away "+
			"would again return a cached PASS", len(dirs), len(layerRules))
	}

	var names []string
	for d := range dirs {
		names = append(names, d)
	}
	sort.Strings(names)
	for _, rel := range names {
		dir := filepath.Join(root, rel)
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("cache ballast: readdir %s: %v", dir, err)
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
				continue
			}
			// The READ is the point: os.ReadFile is what testlog records. The bytes are discarded.
			if _, err := os.ReadFile(filepath.Join(dir, e.Name())); err != nil {
				t.Fatalf("cache ballast: read %s/%s: %v", dir, e.Name(), err)
			}
		}
	}
}

// gate-control: TestPTYLayeringControls
func TestPackageLayering(t *testing.T) {
	root := repoRoot(t)
	cacheKeyBallast(t, root)
	for _, rule := range layerRules {
		t.Run(strings.TrimPrefix(rule.pkg, modPrefix), func(t *testing.T) {
			var deps map[string]bool
			if len(rule.bannedTransitive) > 0 || len(rule.requiredTransitive) > 0 {
				deps = goListDeps(t, root, rule.pkg)
			}
			for _, req := range rule.requiredTransitive {
				if !deps[req] {
					t.Fatalf("self-check FAILED: %s does not depend on %q.\n"+
						"Either the go list query broke (in which case every ban below passed vacuously) or "+
						"this edge really went away and the row needs updating.\nRule: %s",
						rule.pkg, req, rule.why)
				}
			}
			// PREFIX match, not an exact map lookup. A banned entry names a module or package ROOT and
			// what shows up in `go list -deps` are its subpackages: nothing in the tree has an import
			// path exactly equal to "github.com/nats-io/nats-server/v2", so an exact lookup on that
			// string could never fire. That is not hypothetical — the clause below sat here as an exact
			// key for the whole life of the four files this table replaced, and a review lane proved it
			// dead by importing the entire embedded NATS server into internal/cluster (16 nats-server
			// packages in deps) with the gate still green. Worse, TestLayeringRulesAreWellFormed was
			// asserting the string set verbatim, so the dead clause had been CERTIFIED as "the union is
			// preserved".
			//
			// Prefix matching makes a root-path ban mean what it reads as. `dep == banned` is kept for
			// the case where the banned path is itself an importable package (internal/broker etc.).
			for _, banned := range rule.bannedTransitive {
				for dep := range deps {
					// go list -deps includes the queried package itself. A module-wide ban
					// forbids imports, not the package's own existence (upgradebudget).
					// origin: hot-update S3 plan §9 (docs/reviews/hot-update-agent-bootstrap-plan.md).
					// The same holds for the package's OWN subtree (a responsibility split such as
					// drillobserve/receipt): the subpackage path itself is not a foreign owner, and every
					// import it makes still appears in `deps` and is checked against the ban here.
					// origin: S7 GATE-CONVERGE slimobs — lead layering decision
					if dep == rule.pkg || strings.HasPrefix(dep, rule.pkg+"/") {
						continue
					}
					if layerDependencyMatches(dep, banned) {
						t.Errorf("%s transitively imports %q (via %q) — layering violated.\n%s",
							rule.pkg, banned, dep, rule.why)
						break
					}
				}
			}
			if len(rule.bannedDirect) > 0 {
				direct := goListDirectImports(t, root, rule.pkg)
				for _, banned := range rule.bannedDirect {
					if direct[banned] {
						t.Errorf("%s DIRECTLY imports %q — layering violated.\n%s", rule.pkg, banned, rule.why)
					}
				}
			}
		})
	}
}

// TestLayeringRulesAreWellFormed keeps the table itself honest. A row with no bans asserts nothing; a
// row whose required set is empty has no self-check, so a broken `go list` would let its bans pass
// silently; and a package listed twice means one of the two rows is being maintained by nobody.
// originalUnion is what the four deleted regression files asserted BETWEEN them, per package. It is the
// merge's receipt, kept as data so the claim "no clause was lost" is re-checkable mechanically instead of
// by redoing the hand comparison.
//
// origin: line-2 external review M9 / IDG-7 / PF-12. The first version of this guard had a lower bound of
// `< 5` on a six-row table and full-equality on ONE row (internal/cluster). Both holes were measured: a
// lane deleted the entire internal/xferaudit row (all of the old TestD8XferauditIsLeaf [deleted]) leaving
// five rows, and separately deleted "nats.go/jetstream" from the internal/clusternodes row (a clause copied
// verbatim out of TestD6ClusternodesNoNATSNoCluster [deleted]) — BOTH mutations left every test green.
// The merge itself was
// clean; the guard against the NEXT merge was not, and the entire argument for merging four narrow files
// into one table was that a table cannot quietly lose an assertion.
//
// TEST-NAME MAP — plan §5 asked for this in the file's comments and it was not written. Ten Test functions
// across four files became six rows. It lives in `deletedRegressionTests` below rather than in prose,
// because prose is how the first version of this map came to be FABRICATED.
//
// THE FIRST VERSION OF THIS MAP WAS INVENTED. Eight of its ten names never existed in this repository. It
// was written from memory of what the tests probably were called, it read as authoritative, and its whole
// stated purpose — "so someone reading a d5/d6/d7/d8 review report can find where the assertion went" — was
// therefore inverted: a reader grepping those names finds nothing and concludes the assertion is gone.
//
// Worse, the `[deleted]` marker invented in the same change made promised_guard_test.go structurally unable
// to notice, because that marker's whole job is to say "do not check whether this test exists". A mechanism
// for suppressing a check was paired with an unverifiable claim, in the same commit, by the same author.
// The closure verification caught it with one command: `git log --all -S<name>`.
//
// So the map is now DATA, and TestDeletedRegressionTestNamesAreReal checks every name against
// `git show HEAD:<file>`. The marker still suppresses promised_guard — it has to, the tests really are gone
// — but the claim it suppresses is now asserted somewhere else instead of merely asserted by me.
//
// The assertion below is ⊇, not equality: adding a NEW ban to a row is an improvement and must not fail.
var originalUnion = map[string][]string{
	// CLUSTER-SLIM leaves inherit no deleted regression tests.
	// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM
	"internal/cluster/state": {}, "internal/cluster/storelock": {},
	"internal/cluster/logbridge": {}, "internal/cluster/routecert": {},
	// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM — new leaves inherit no deleted regression rows.
	"internal/tunnel/flowstate":   {},
	"internal/tunnel/flowcarrier": {},
	"internal/tunnel/transport":   {},
	// origin: hot-update S6 plan §9.3; extracted control owner adds no inherited boundary.
	"internal/brokercontrol": {},
	// S6 leaves inherit no deleted regression tests; their new bans have
	// same-file controls and the AllOS scan below.
	// origin: hot-update S6 plan §9.3 / T53.
	"internal/brokerimage": {}, "internal/clusterio": {},
	"internal/brokermaint": {}, "internal/controlbudget": {},
	"internal/listenerimage": {}, "internal/cli": {},
	"internal/ssstate": {},
	// Private drill custody inherited no deleted regression rule.
	"internal/drillobserve":  {},
	"internal/agentidentity": {},
	// S5 extractions inherited no deleted layering regression.
	"internal/agentstate":   {},
	"internal/filetransfer": {},
	"internal/connrecovery": {},

	"internal/workload": {},
	// New S5 owners inherited no deleted regression assertion.
	"internal/rawsocket":     {},
	"internal/sealedstate":   {},
	"internal/flow":          {},
	"internal/tunnel":        {},
	"internal/agent/ssproxy": {},

	"internal/cluster": {
		"github.com/nats-io/nats.go",
		"github.com/nats-io/nats.go/jetstream",
		"github.com/nats-io/nats-server/v2",
		modPrefix + "internal/broker",
		modPrefix + "internal/jsstream",
		modPrefix + "internal/clusternodes",
	},
	"internal/jsstream":  {modPrefix + "internal/cluster"},
	"internal/xferaudit": {"github.com/nats-io/nats.go"},
	"internal/clusternodes": {
		"github.com/nats-io/nats.go",
		"github.com/nats-io/nats.go/jetstream",
		modPrefix + "internal/cluster",
		modPrefix + "internal/broker",
	},
	"internal/clusteroffline": {"github.com/hashicorp/raft"},
	"internal/broker":         {"github.com/hashicorp/raft"},
	// Added 2026-09-01 (test-system overhaul, internal review L5-F5), inherited from no deleted file:
	// the rule lived only in prose (plan §2.3) until then. Empty on purpose — the receipt records
	// what the four regression files asserted, and they asserted nothing about testharness.
	"internal/testharness": {},
	// Added 2026-09-21 (hot-update S2), a new package: inherited from no deleted file, so the
	// receipt is empty for the same reason as testharness.
	"internal/agentimage": {},
	// S4 adds terminal layers; none inherited a deleted regression rule.
	"internal/ptysession": {},
	"internal/pty":        {},
	"internal/terminalio": {},
	// Added for hot-update S3; neither package inherited a deleted regression rule.
	"internal/agentboot":     {},
	"internal/upgradebudget": {},
	// New S4 filesystem owners inherited no deleted regression rule.
	"internal/binaryupgrade": {},
	"internal/transferpath":  {},
}

func TestLayeringRulesAreWellFormed(t *testing.T) {
	if len(layerRules) != len(originalUnion) {
		t.Fatalf("%d layering rules but originalUnion records %d packages — a row was added without "+
			"recording what it inherited, or a row was LOST in a merge. The four regression files this "+
			"table replaced asserted boundaries on %d distinct packages.",
			len(layerRules), len(originalUnion), len(originalUnion))
	}
	seen := map[string]bool{}
	for _, rule := range layerRules {
		name := strings.TrimPrefix(rule.pkg, modPrefix)
		if seen[rule.pkg] {
			t.Errorf("%s appears twice — merge the rows, or one of them will drift unmaintained", name)
		}
		seen[rule.pkg] = true
		if len(rule.bannedTransitive) == 0 && len(rule.bannedDirect) == 0 {
			t.Errorf("%s bans nothing — the row asserts no boundary at all", name)
		}
		if len(rule.requiredTransitive) == 0 {
			t.Errorf("%s has no requiredTransitive self-check — a broken go list would make its bans "+
				"pass vacuously", name)
		}
		if strings.TrimSpace(rule.why) == "" {
			t.Errorf("%s has no `why` — the failure message would tell the next person what broke but "+
				"not what the boundary is for", name)
		}
		if !strings.HasPrefix(rule.pkg, modPrefix) {
			t.Errorf("%s is not a package of this module", rule.pkg)
		}
	}
	// UNION PRESERVATION, for EVERY row rather than one.
	byPkg := map[string]*layerRule{}
	for i := range layerRules {
		byPkg[strings.TrimPrefix(layerRules[i].pkg, modPrefix)] = &layerRules[i]
	}
	for name, inherited := range originalUnion {
		rule, ok := byPkg[name]
		if !ok {
			t.Errorf("originalUnion records %d clause(s) inherited for %s, but the table has no row for it. "+
				"See the TEST-NAME MAP above for which deleted test asserted them.", len(inherited), name)
			continue
		}
		bans := map[string]bool{}
		for _, b := range rule.bannedTransitive {
			bans[b] = true
		}
		for _, b := range rule.bannedDirect {
			bans[b] = true
		}
		var missing []string
		for _, want := range inherited {
			if !bans[want] {
				missing = append(missing, want)
			}
		}
		sort.Strings(missing)
		if len(missing) > 0 {
			t.Errorf("the %s row no longer bans %d clause(s) it inherited from the deleted regression "+
				"files:\n  %s\n\nThe merge's whole justification was that a table cannot quietly lose an "+
				"assertion four narrow files were each holding. Restore the clause, or — if the boundary "+
				"genuinely changed — delete it from originalUnion in the same commit and say why.",
				name, len(missing), strings.Join(missing, "\n  "))
		}
	}
}

// deletedRegressionTests is the TEST-NAME MAP: which deleted regression test each merged layerRules row
// came from. Keyed by the file it lived in, so a reader holding a d5/d6/d7/d8 review report can find where
// its assertion went.
//
// The names live in STRING LITERALS, not in a comment, so promised_guard_test.go does not read them as
// promises and no suppression marker is needed — which is the point. The prose version of this map needed
// ten `[deleted]` markers, and those markers are what let eight fabricated names sit unchallenged. Data can
// be reconciled; prose can only be believed. TestDeletedRegressionTestNamesAreReal does the reconciling.
var deletedRegressionTests = map[string][]struct{ name, pkg string }{
	"test/d5/regression_test.go": {
		{"TestD5ClusterNoNATSImport", "internal/cluster"},
		{"TestD5JsstreamNoClusterImport", "internal/jsstream"},
	},
	"test/d6/regression_test.go": {
		{"TestD6ClusternodesNoNATSNoCluster", "internal/clusternodes"},
		{"TestD6ClusterStillNoNATS", "internal/cluster"},
	},
	"test/d7/regression_test.go": {
		{"TestD7ClusternodesStaysLeaf", "internal/clusternodes"},
		{"TestD7ClusterStaysNATSFreeAfterAuthEdge", "internal/cluster"},
		// This one asserted a DIRECT-import rule over two packages at once, which is why the merge produced
		// two rows from it (internal/clusteroffline and internal/broker) rather than one.
		{"TestD7RaftConfinedToCluster", "internal/clusteroffline"},
		{"TestD7RaftConfinedToCluster", "internal/broker"},
	},
	"test/d8/regression_test.go": {
		{"TestD8ClusterStaysNATSFree", "internal/cluster"},
		{"TestD8JsstreamStaysClusterFree", "internal/jsstream"},
		{"TestD8XferauditIsLeaf", "internal/xferaudit"},
	},
}

// TestDeletedRegressionTestNamesAreReal reconciles the TEST-NAME MAP against git history.
//
// origin: line-2 closure verification, M9's second half. This is the check whose absence let the first
// version of the map be fabricated — eight invented names, in a map whose purpose is to be greppable,
// behind a marker that told the promise-checker to look away.
//
// IT READS A FROZEN COMMIT, NOT HEAD. origin: line-2 INDEPENDENT EXTERNAL REVIEW B1 (BLOCKER).
//
// The first version ran `git show HEAD:test/d5/regression_test.go`. In the uncommitted working tree that
// happened to work, because HEAD still contained the four files being deleted. The moment this increment
// is committed, HEAD no longer has them: all four `git show` calls exit 128, `checked` drops from 11 to 0,
// and the gate fails on every subsequent checkout and every CI run.
//
// So `make gates` was green only in the instant before the commit. The comment that used to sit here even
// said the gate would expire — and then shipped it as a release gate anyway, which is worse than not
// noticing: it is a defect that was seen, written down, and delivered.
//
// deletedRegressionTestsCommit is the last commit that contains all four paths. It is an ANCESTOR of every
// future HEAD, so it stays reachable. If a shallow clone cannot reach it the gate says so explicitly rather
// than silently verifying nothing.
const deletedRegressionTestsCommit = "0b1ec070e68e302a24b9b449823953a3c545102a"

func TestDeletedRegressionTestNamesAreReal(t *testing.T) {
	root := repoRoot(t)

	// The defect encoded as an assertion: this gate must not depend on the working tree's commit state.
	// If someone "simplifies" the ref back to HEAD, this fires immediately rather than after the commit.
	if deletedRegressionTestsCommit == "HEAD" {
		t.Fatal("deletedRegressionTestsCommit is HEAD — that is external review B1 exactly: the four paths " +
			"exist at HEAD only while their deletion is uncommitted, so the gate self-destructs on commit")
	}
	head, herr := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if herr == nil && strings.TrimSpace(string(head)) == deletedRegressionTestsCommit {
		t.Errorf("deletedRegressionTestsCommit equals the current HEAD (%s).\n\n"+
			"That is the state B1 described: it works now and breaks the moment anything is committed. "+
			"Freeze the last commit that CONTAINS the four paths instead — `git rev-list -1 HEAD -- "+
			"test/d5/regression_test.go`.", deletedRegressionTestsCommit)
	}
	// And the other half of the premise: the files must be GONE from the working tree. If they came back,
	// this gate is verifying history that is no longer history.
	for rel := range deletedRegressionTests {
		if _, err := os.Stat(filepath.Join(root, rel)); err == nil {
			t.Errorf("%s exists in the working tree, but deletedRegressionTests records it as deleted — "+
				"either the merge was reverted or this map is describing the wrong thing", rel)
		}
	}

	// Map every layerRules row to the packages it covers, so the map's `pkg` column is checked too.
	ruled := map[string]bool{}
	for _, r := range layerRules {
		ruled[strings.TrimPrefix(r.pkg, modPrefix)] = true
	}

	checked := 0
	for file, entries := range deletedRegressionTests {
		cmd := exec.Command("git", "show", deletedRegressionTestsCommit+":"+file)
		cmd.Dir = root
		out, err := cmd.Output()
		if err != nil {
			t.Errorf("git show %s:%s failed (%v).\n\n"+
				"This gate reads the deleted files from a FROZEN ancestor commit. A failure here means that "+
				"commit is unreachable — most likely a shallow clone (`git fetch --unshallow`), or the SHA was "+
				"edited to something that does not contain these paths.", deletedRegressionTestsCommit, file, err)
			continue
		}
		src := string(out)
		for _, e := range entries {
			checked++
			if !strings.Contains(src, "func "+e.name+"(") {
				t.Errorf("deletedRegressionTests names %s as having lived in %s, but that file at commit %s "+
					"contains no such function.\n\n"+
					"An INVENTED provenance entry is worse than no map: its whole purpose is that a reader "+
					"of an old review report can grep the name and find where the assertion went. Get the "+
					"real name from `git show %s:%s | grep '^func Test'`.",
					e.name, file, deletedRegressionTestsCommit, deletedRegressionTestsCommit, file)
			}
			if !ruled[e.pkg] {
				t.Errorf("deletedRegressionTests maps %s to package %q, which has no row in layerRules — "+
					"either the package name is wrong or the row it became was lost", e.name, e.pkg)
			}
		}
	}

	// Non-vacuity, both directions. The map must cover every deleted test, and the count is pinned so a
	// silently shrinking map cannot pass.
	const deletedTestAssertions = 11 // 10 distinct functions; TestD7RaftConfinedToCluster [deleted] covers 2 pkgs
	if checked != deletedTestAssertions {
		t.Errorf("checked %d name→package assertions, expected %d — the map shrank, grew, or a `git show` "+
			"failed silently above", checked, deletedTestAssertions)
	}
}

// layerDependencyMatches shares the module-root predicate with the controls:
// a ban includes subpackages, but never a merely similar package name.
func layerDependencyMatches(dep, banned string) bool {
	// SS is a separate leaf owner despite its historical directory under agent.
	// Exclude only that exact package: agent itself, other children and any new
	// ssproxy child still belong to the banned control-plane namespace. Its own
	// row independently forbids reaching agent, flow, tunnel, yamux or NATS.
	// origin: hot-update S5 plan §10.1 / P26 / T49.
	if banned == modPrefix+"internal/agent" && dep == modPrefix+"internal/agent/ssproxy" {
		return false
	}
	return dep == banned || strings.HasPrefix(dep, banned+"/")
}

// The new rows must retain every promised boundary, even when the current
// dependency graph does not happen to exercise a missing clause yet.
// origin: hot-update S4 plan §10 / T47.
func TestPTYLayeringControls(t *testing.T) {
	boundaries := map[string][]string{
		"internal/ptysession": {
			modPrefix + "internal/agent", modPrefix + "internal/broker", modPrefix + "internal/execsession",
			"github.com/nats-io/nats.go", "github.com/nats-io/nats-server/v2",
		},
		"internal/terminalio": {
			"net", "github.com/nats-io/nats.go", "github.com/nats-io/nats-server/v2",
			modPrefix + "internal/agent", modPrefix + "internal/execclient",
		},
		"internal/pty":        {modPrefix + "internal/agent", modPrefix + "internal/ptysession"},
		"internal/agentimage": {modPrefix + "internal/agent"},
	}
	for pkg, boundaries := range boundaries {
		t.Run(pkg, func(t *testing.T) {
			var row *layerRule
			for i := range layerRules {
				if layerRules[i].pkg == modPrefix+pkg {
					row = &layerRules[i]
				}
			}
			if row == nil {
				t.Fatal("missing terminal-layer rule")
			}
			for _, banned := range boundaries {
				found := false
				for _, registered := range row.bannedTransitive {
					found = found || registered == banned
				}
				if !found {
					t.Errorf("missing transitive ban on %s", banned)
				}
				for _, dep := range []string{banned, banned + "/child"} {
					if !layerDependencyMatches(dep, banned) {
						t.Errorf("forbidden dependency %s escaped %s", dep, banned)
					}
				}
				if layerDependencyMatches(banned+"neighbor", banned) {
					t.Errorf("similar package name incorrectly matches %s", banned)
				}
				for _, required := range row.requiredTransitive {
					if layerDependencyMatches(required, banned) {
						t.Errorf("required dependency %s is also banned by %s", required, banned)
					}
				}
			}
		})
	}
}

// Pin every S5 row and ban individually. The graph may happen to be clean
// after a clause is deleted; that is why row completeness needs its own gate.
// origin: hot-update S5 plan §10.1 / T49.
// gate-control: TestFlowSSLayeringControls
func TestFlowSSLayeringControls(t *testing.T) {
	// The leaf's path cannot turn into a blanket exemption for agent children,
	// nor can it bypass another package's explicit ban on SS itself.
	ss := modPrefix + "internal/agent/ssproxy"
	agent := modPrefix + "internal/agent"
	if layerDependencyMatches(ss, agent) || !layerDependencyMatches(ss, ss) {
		t.Fatal("SS leaf is confused with its control-plane parent or escapes an explicit SS ban")
	}
	for _, dep := range []string{agent, agent + "/courier", ss + "/child", ss + "neighbor"} {
		if !layerDependencyMatches(dep, agent) {
			t.Errorf("agent namespace dependency %s escaped", dep)
		}
	}
	// Reclassifying SS as a leaf must not open reverse edges from its own
	// dependencies or unrelated data-plane owners. Only the image composition,
	// bootstrap and executable transaction layers may reach that SS owner.
	// origin: hot-update S5 plan §10.1 / P26 / T49.
	for _, rule := range layerRules {
		allowsSS := rule.pkg == ss || rule.pkg == modPrefix+"internal/agentimage" ||
			rule.pkg == modPrefix+"internal/agentboot" || rule.pkg == modPrefix+"internal/binaryupgrade"
		bansAgent, bansSS := false, false
		for _, ban := range rule.bannedTransitive {
			bansAgent = bansAgent || ban == agent
			bansSS = bansSS || ban == ss
		}
		if bansAgent && !allowsSS && !bansSS {
			t.Errorf("%s lost its SS reverse-edge ban when the leaf was reclassified", rule.pkg)
		}
	}
	common := []string{modPrefix + "internal/agent", modPrefix + "internal/broker", "github.com/hashicorp/yamux", "github.com/nats-io/nats.go"}
	boundaries := map[string][]string{
		"internal/ssstate":       append(append([]string{}, common...), modPrefix+"internal/flow", modPrefix+"internal/tunnel", modPrefix+"internal/agent/ssproxy", modPrefix+"internal/execsession", modPrefix+"internal/agentimage", modPrefix+"internal/agentboot"),
		"internal/rawsocket":     append(append([]string{}, common...), modPrefix+"internal/flow", modPrefix+"internal/tunnel"),
		"internal/sealedstate":   append(append([]string{}, common...), modPrefix+"internal/flow", modPrefix+"internal/tunnel"),
		"internal/workload":      append(append([]string{}, common...), modPrefix+"internal/flow", modPrefix+"internal/tunnel"),
		"internal/flow":          append(append([]string{}, common...), modPrefix+"internal/tunnel"),
		"internal/tunnel":        {modPrefix + "internal/agent", modPrefix + "internal/broker"},
		"internal/agent/ssproxy": append(append([]string{}, common...), modPrefix+"internal/flow", modPrefix+"internal/tunnel"),
	}
	for pkg, bans := range boundaries {
		t.Run(pkg, func(t *testing.T) {
			var rule *layerRule
			for i := range layerRules {
				if layerRules[i].pkg == modPrefix+pkg {
					rule = &layerRules[i]
				}
			}
			if rule == nil {
				t.Fatal("missing flow/SS layer rule")
			}
			for _, ban := range bans {
				found := false
				for _, registered := range rule.bannedTransitive {
					found = found || registered == ban
				}
				if !found {
					t.Errorf("missing flow/SS transitive ban %s", ban)
				}
				for _, dep := range []string{ban, ban + "/child"} {
					if !layerDependencyMatches(dep, ban) {
						t.Errorf("reverse import %s escaped", dep)
					}
				}
				if layerDependencyMatches(ban+"neighbor", ban) {
					t.Errorf("unrelated import matched %s", ban)
				}
				for _, required := range rule.requiredTransitive {
					if layerDependencyMatches(required, ban) {
						t.Errorf("self-check depends on forbidden %s", required)
					}
				}
			}
		})
	}
}

// The regular go-list gate retains external transitive closure. This companion
// walks internal source edges without build selection, then applies the same
// S6 bans. External imports are terminal nodes here, not guessed dependencies.
// origin: hot-update S6 plan §9.3 / T53
// gate-control: TestBrokerLayeringControls
func checkBrokerAllOSLayering(sources map[string]string) error {
	graph := map[string]map[string]bool{}
	for path, source := range sources {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		// P00's isolated compile adapter is test-only by construction. Its
		// production reachability is forbidden by checkBrokerProductionStubs;
		// counting its fixture import as shipping wiring would reject P00 itself.
		// origin: hot-update S6 plan §2.1 / T53.
		if strings.HasPrefix(path, "internal/brokerimage/contractstub/") {
			continue
		}
		// The P00 witness is test scaffolding; shipping reachability is checked
		// separately and must never acquire this exception.
		// origin: hot-update S7 plan §8.1 / T50
		if strings.HasPrefix(path, "internal/xfercontract/contractstub/") {
			continue
		}

		file, err := brokerParse(path, source)
		if err != nil {
			return err
		}
		pkg := modPrefix + filepath.ToSlash(filepath.Dir(path))
		if graph[pkg] == nil {
			graph[pkg] = map[string]bool{}
		}
		for _, imp := range brokerImports(file) {
			if imp == modPrefix+"test/clusterharness" || strings.HasPrefix(imp, modPrefix+"test/clusterharness/") {
				return fmt.Errorf("%s: production imports clusterharness", path)
			}
			graph[pkg][imp] = true
		}
	}
	for _, rule := range append(append([]layerRule{}, layerRules...), transferLayerRules()...) {
		// Apply the existing table to the whole AllOS source union as well:
		// tunnel and legacy leaves must not hide reverse edges in a twin.
		// origin: hot-update S6 plan §9.3 / T53.
		for dep := range graph[rule.pkg] {
			for _, ban := range rule.bannedDirect {
				// Direct bans name exact packages, as in TestPackageLayering.
				// A leaf must reject its runtime parent while retaining sibling imports.
				// origin: hot-update S6 plan §9.3 / T53
				if dep == ban {
					return fmt.Errorf("%s: AllOS direct import %s", rule.pkg, dep)
				}
			}
		}
		visited := map[string]bool{}
		var visit func(string) error
		visit = func(pkg string) error {
			if visited[pkg] {
				return nil
			}
			visited[pkg] = true
			for dep := range graph[pkg] {
				// Match the direct gate's self-subtree rule without skipping the
				// leaf's imports: a receipt cannot launder a foreign runtime.
				// origin: S7 internal review GATES-PROD F2 / S7 GATE-CONVERGE
				owned := dep == rule.pkg || strings.HasPrefix(dep, rule.pkg+"/")
				for _, ban := range rule.bannedTransitive {
					if !owned && layerDependencyMatches(dep, ban) {
						return fmt.Errorf("%s: AllOS layering reaches %s through %s", rule.pkg, dep, pkg)
					}
				}
				if err := visit(dep); err != nil {
					return err
				}
			}
			return nil
		}
		if err := visit(rule.pkg); err != nil {
			return err
		}
	}
	return nil
}

func isBrokerLayerRule(pkg string) bool {
	for _, name := range []string{"brokerimage", "clusterio", "brokermaint", "controlbudget", "listenerimage", "cli"} {
		if pkg == modPrefix+"internal/"+name {
			return true
		}
	}
	return false
}

// origin: hot-update S6 plan §9.3 / T53
func TestBrokerLayeringAllOS(t *testing.T) {
	sources := brokerGateSources(t, repoRoot(t))
	for _, name := range []string{"brokerimage", "clusterio", "brokermaint", "controlbudget", "listenerimage", "cli"} {
		exists := false
		for path := range sources {
			exists = exists || filepath.ToSlash(filepath.Dir(path)) == "internal/"+name
		}
		if !exists {
			t.Fatalf("no AllOS sources for %s", name)
		}
	}
	if err := checkBrokerAllOSLayering(sources); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S6 plan §11 / T53
func TestBrokerLayeringControls(t *testing.T) {

	required := map[string][]string{
		"brokerimage":   {modPrefix + "internal/broker", modPrefix + "cmd", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy"},
		"clusterio":     {"github.com/hashicorp/raft", "github.com/nats-io", modPrefix + "internal/cluster", modPrefix + "internal/broker", modPrefix + "cmd"},
		"brokermaint":   {"github.com/nats-io", "github.com/hashicorp/raft", modPrefix + "internal/broker", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/tunnel", modPrefix + "internal/cluster", modPrefix + "internal/brokerimage", modPrefix + "cmd"},
		"controlbudget": {"github.com/nats-io", "github.com/hashicorp/raft", modPrefix + "internal/cli", modPrefix + "internal/broker", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "internal/tunnel", modPrefix + "internal/cluster", modPrefix + "internal/brokerimage", modPrefix + "cmd"},
		"listenerimage": {modPrefix + "internal/tunnel", modPrefix + "internal/brokerimage", modPrefix + "internal/broker", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", "github.com/nats-io", "github.com/hashicorp/raft"},
		"cli":           {modPrefix + "internal/brokerimage"},
	}
	for name, bans := range required {
		found := false
		for _, rule := range layerRules {
			if rule.pkg != modPrefix+"internal/"+name {
				continue
			}
			found = true
			if len(rule.bannedTransitive) != len(bans) {
				t.Fatalf("%s frozen layering clauses drifted", name)
			}
			for _, ban := range bans {
				present := false
				for _, actual := range rule.bannedTransitive {
					present = present || actual == ban
				}
				if !present {
					t.Fatalf("%s lost frozen ban %s", name, ban)
				}
			}
		}
		if !found {
			t.Fatalf("missing S6 leaf rule %s", name)
		}
	}
	rows := 0
	for _, rule := range layerRules {
		if !isBrokerLayerRule(rule.pkg) {
			continue
		}
		rows++
		pkg := strings.TrimPrefix(rule.pkg, modPrefix)
		path := pkg + "/owner_other.go"
		good := map[string]string{path: "package leaf;import \"io\";var _ io.Reader"}
		if err := checkBrokerAllOSLayering(good); err != nil {
			t.Fatal(err)
		}
		for _, ban := range rule.bannedTransitive {
			for _, target := range []string{ban, ban + "/child"} {
				direct := map[string]string{path: "package leaf;import _ " + strconv.Quote(target)}
				if checkBrokerAllOSLayering(direct) == nil {
					t.Fatalf("direct AllOS ban escaped %s -> %s", pkg, target)
				}
				indirect := map[string]string{path: "package leaf;import _ " + strconv.Quote(modPrefix+"internal/helper"), "internal/helper/helper.go": "package helper;import _ " + strconv.Quote(target)}
				if checkBrokerAllOSLayering(indirect) == nil {
					t.Fatalf("transitive AllOS ban escaped %s -> %s", pkg, target)
				}
			}
		}
	}
	// Slim leaves reject their exact runtime parent while permitting the
	// flowstate sibling. Both OS scans must enforce the same table semantics.
	// origin: hot-update S6 plan §9.3 / T53
	for _, leaf := range []string{"flowstate", "flowcarrier", "transport"} {
		path := "internal/tunnel/" + leaf + "/owner_other.go"
		if checkBrokerAllOSLayering(map[string]string{path: "package leaf;import _ " + strconv.Quote(modPrefix+"internal/tunnel")}) == nil {
			t.Fatalf("slim leaf %s imported its runtime parent", leaf)
		}
		if leaf != "flowstate" {
			if err := checkBrokerAllOSLayering(map[string]string{path: "package leaf;import _ " + strconv.Quote(modPrefix+"internal/tunnel/flowstate")}); err != nil {
				t.Fatalf("slim leaf %s lost its codec dependency: %v", leaf, err)
			}
		}
	}
	for _, edge := range [][2]string{{"internal/tunnel", modPrefix + "internal/broker"}, {"internal/broker", "github.com/hashicorp/raft"}} {
		path := edge[0] + "/image_other.go"
		source := "package owner;import _ " + strconv.Quote(edge[1])
		if checkBrokerAllOSLayering(map[string]string{path: source}) == nil {
			t.Fatalf("existing AllOS boundary escaped %v", edge)
		}
	}
	if rows != 6 {
		t.Fatalf("S6 layering registry: got %d want 6", rows)
	}
	for _, suffix := range []string{"", "/private"} {
		if checkBrokerAllOSLayering(map[string]string{"internal/x/x.go": "package x;import _ " + strconv.Quote(modPrefix+"test/clusterharness"+suffix)}) == nil {
			t.Fatal("production test helper import escaped")
		}
	}
	if err := checkBrokerAllOSLayering(map[string]string{"internal/x/x_test.go": "package x;import _ " + strconv.Quote(modPrefix+"test/clusterharness")}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "internal/clusterio/contracts.go"))
	if err != nil {
		t.Fatal(err)
	}
	// Mutate a copied real family, on disk, through the source reader.
	root := t.TempDir()
	for _, dir := range []string{"internal/clusterio", "cmd"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	source := strings.Replace(string(raw), "import (", "import (\n_ \"github.com/hashicorp/raft\"", 1)
	if source == string(raw) {
		t.Fatal("real import mutation did not apply")
	}
	if err := os.WriteFile(filepath.Join(root, "internal/clusterio/contracts_other.go"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	if checkBrokerAllOSLayering(brokerGateSources(t, root)) == nil {
		t.Fatal("real-family raft import escaped")
	}
}

// Runtime adapters sit above these owners. The bridge may compose brokerimage
// and transfer leaves, but may never import the broker whose callbacks it binds.
// origin: hot-update S7 plan §11.1 / T50
// gate-control: TestTransferLayeringControls
func transferLayerRules() []layerRule {
	var rows []layerRule
	for _, pkg := range transferLeafRoots {
		rows = append(rows, layerRule{
			pkg:                modPrefix + "internal/" + pkg,
			why:                "transfer owners survive outer runtime and transport replacement",
			requiredTransitive: []string{modPrefix + "internal/" + pkg},
			bannedDirect:       []string{modPrefix + "internal/broker", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy"},
			bannedTransitive:   []string{modPrefix + "internal/broker", modPrefix + "internal/agent", modPrefix + "internal/agent/ssproxy", modPrefix + "cmd"},
		})
	}
	for _, pkg := range []string{"proto", "schema", "jsstream", "transferpath", "xferfinal", "xfergc", "xferimage", "xfertrack"} {
		bans := []string{modPrefix + "internal/xferworker"}
		if pkg != "xfertrack" {
			bans = append(bans, modPrefix+"internal/xfertrack")
		}
		if pkg == "proto" || pkg == "schema" {
			bans = append(bans, modPrefix+"internal/xferimage", modPrefix+"internal/agentimage", modPrefix+"internal/brokerimage")
		}
		rows = append(rows, layerRule{pkg: modPrefix + "internal/" + pkg, why: "values and port consumers cannot acquire a manager implementation", requiredTransitive: []string{modPrefix + "internal/" + pkg}, bannedTransitive: bans})
	}
	return rows
}

func checkTransferProductionImports(sources map[string]string) error {
	for path, source := range sources {
		if strings.HasPrefix(path, "internal/xfercontract/contractstub/") || strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := brokerParse(path, source)
		if err != nil {
			return err
		}
		for _, imp := range brokerImports(file) {
			for _, banned := range []string{"internal/xfercontract/contractstub", "internal/testharness", "test/clusterharness", "test/transferharness"} {
				if layerDependencyMatches(imp, modPrefix+banned) {
					return fmt.Errorf("%s: shipping code imports test witness %s", path, imp)
				}
			}
			// The contract's own value closures (xferport, split out so transferpath can
			// name filesystem ports without the jetstream-bearing ports.go, and any later
			// NATS-free value split of the same kind) are part of the neutral contract:
			// they may import each other, controlbudget and handoff, and nothing else of
			// the module. Unlike xfercontract itself — whose ports.go legitimately names
			// jetstream.JetStream — a value-closure subpackage is neutral only while it
			// imports no third-party package at all, which is checked here, not assumed.
			// origin: hot-update S7 plan §8.1 / §11.1 / P00 / T50
			dir := filepath.Dir(path)
			subpackage := strings.HasPrefix(dir, "internal/xfercontract/")
			if (dir == "internal/xfercontract" || subpackage) && strings.HasPrefix(imp, modPrefix) && imp != modPrefix+"internal/controlbudget" && imp != modPrefix+"internal/handoff" && !strings.HasPrefix(imp, modPrefix+"internal/xfercontract/") {
				return fmt.Errorf("%s: neutral contracts import runtime %s", path, imp)
			}
			if subpackage && !strings.HasPrefix(imp, modPrefix) && strings.Contains(strings.SplitN(imp, "/", 2)[0], ".") {
				return fmt.Errorf("%s: contract value closure imports non-standard %s", path, imp)
			}
		}
	}
	return nil
}

// origin: hot-update S7 plan §11.1 / T50
func TestTransferLayeringAllOS(t *testing.T) {
	sources := brokerGateSources(t, repoRoot(t))
	if err := checkTransferProductionImports(sources); err != nil {
		t.Fatal(err)
	}
	if err := checkBrokerAllOSLayering(sources); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §11.1 / T50
func TestTransferLayeringControls(t *testing.T) {
	for _, rule := range transferLayerRules() {
		path := strings.TrimPrefix(rule.pkg, modPrefix) + "/owner_other.go"
		root := transferFixtureTree(t, map[string]string{path: "package owner;import \"io\";var _ io.Reader"})
		if err := checkBrokerAllOSLayering(brokerGateSources(t, root)); err != nil {
			t.Fatal(err)
		}
		for _, ban := range rule.bannedTransitive {
			transferWriteFixture(t, root, path, "package owner;import _ "+strconv.Quote(ban))
			if checkBrokerAllOSLayering(brokerGateSources(t, root)) == nil {
				t.Fatalf("on-disk AllOS reverse edge escaped %s -> %s", rule.pkg, ban)
			}
		}
	}
	for _, helper := range []string{"internal/xfercontract/contractstub", "internal/testharness", "test/clusterharness", "test/transferharness"} {
		root := transferFixtureTree(t, map[string]string{"internal/xferclient/owner_other.go": "package owner;import _ " + strconv.Quote(modPrefix+helper)})
		if checkTransferProductionImports(brokerGateSources(t, root)) == nil {
			t.Fatalf("on-disk production witness import escaped %s", helper)
		}
	}
	// The neutral contract may name its value closures, and nothing else of the
	// module; a closure may not grow a third-party (NATS) or runtime import.
	// origin: hot-update S7 plan §8.1 / §11.1 / P00 / T50
	// Distinct import names: brokerImports keys by name, so two blank imports
	// would collapse into one and leave the allowance untested.
	goodNeutral := map[string]string{
		"internal/xfercontract/contracts.go":       "package xfercontract;import value " + strconv.Quote(modPrefix+"internal/xfercontract/xfervalue") + ";import sdk \"github.com/nats-io/nats.go/jetstream\"",
		"internal/xfercontract/xferport/port.go":   "package xferport;import _ \"encoding/json\"",
		"internal/xfercontract/xfervalue/value.go": "package xfervalue;import port " + strconv.Quote(modPrefix+"internal/xfercontract/xferport") + ";import budget " + strconv.Quote(modPrefix+"internal/controlbudget"),
	}
	for path, source := range map[string]string{
		"internal/xfercontract/contracts.go":       "package xfercontract;import _ " + strconv.Quote(modPrefix+"internal/proto"),
		"internal/xfercontract/xferport/port.go":   "package xferport;import _ \"github.com/nats-io/nats.go/jetstream\"",
		"internal/xfercontract/xfervalue/value.go": "package xfervalue;import _ " + strconv.Quote(modPrefix+"internal/broker"),
	} {
		neutral := transferFixtureTree(t, goodNeutral)
		if err := checkTransferProductionImports(brokerGateSources(t, neutral)); err != nil {
			t.Fatalf("std-only contract value closure refused: %v", err)
		}
		transferWriteFixture(t, neutral, path, source)
		if checkTransferProductionImports(brokerGateSources(t, neutral)) == nil {
			t.Fatalf("on-disk neutral contract import drift escaped: %s", path)
		}
	}
}

// The AllOS import union includes indirect local imports and rejects cycles.
// Privileged binaries cannot reach business bootstrap even through a new leaf.
// Neutral DTOs admit only stdlib/controlbudget; witnesses never ship.
// origin: hot-update S9 plan §10.2 / T83
func checkInfraLayering(sources map[string]string) error {
	graph := map[string]map[string]bool{}
	for _, path := range brokerSortedSourcePaths(sources) {
		if !isProdGoFile(path) || strings.Contains(path, "/contractstub/") {
			continue
		}
		file, err := brokerParse(path, sources[path])
		if err != nil {
			return err
		}
		pkg := filepath.ToSlash(filepath.Dir(path))
		if graph[pkg] == nil {
			graph[pkg] = map[string]bool{}
		}
		for _, imp := range brokerImports(file) {
			dep := strings.TrimPrefix(imp, modPrefix)
			graph[pkg][dep] = true
			if pkg == "internal/infracontract" && strings.Contains(imp, ".") && dep != "internal/controlbudget" {
				return fmt.Errorf("%s: neutral S9 contract imports %s", path, imp)
			}
			if infraOwner(path) {
				for _, ban := range []string{"internal/agent", "internal/broker", "cmd/tether", "internal/testharness", "test/", "internal/infracontract/contractstub"} {
					if dep == ban || strings.HasPrefix(dep, ban+"/") || strings.HasSuffix(ban, "/") && strings.HasPrefix(dep, ban) {
						return fmt.Errorf("%s: infrastructure leaf imports runtime/helper %s", path, imp)
					}
				}
			}
		}
	}
	state := map[string]int{}
	var visit func(string, bool) error
	visit = func(pkg string, privileged bool) error {
		key := fmt.Sprintf("%t:%s", privileged, pkg)
		if state[key] == 1 {
			return fmt.Errorf("AllOS package cycle at %s", pkg)
		}
		if state[key] == 2 {
			return nil
		}
		state[key] = 1
		for dep := range graph[pkg] {
			if privileged {
				for _, ban := range []string{"internal/agentboot", "internal/broker", "internal/agent", "internal/infraupgrade", "internal/proto", "internal/infraattach", "internal/infraauth", "internal/infratransfer", "internal/testharness", "test/", "github.com/nats-io/nats.go", "github.com/nats-io/nats-server/v2", "github.com/hashicorp/raft", "github.com/hashicorp/yamux"} {
					if dep == ban || strings.HasPrefix(dep, ban+"/") || strings.HasSuffix(ban, "/") && strings.HasPrefix(dep, ban) {
						return fmt.Errorf("%s: privileged closure reaches %s", pkg, dep)
					}
				}
			}
			if strings.HasPrefix(dep, "internal/") || strings.HasPrefix(dep, "cmd/") {
				if graph[dep] == nil {
					return fmt.Errorf("%s: missing local dependency source %s", pkg, dep)
				}
				if err := visit(dep, privileged); err != nil {
					return err
				}
			}
		}
		state[key] = 2
		return nil
	}
	for pkg := range graph {
		if err := visit(pkg, pkg == "cmd/tether-infra-helper" || pkg == "cmd/tether-infra-launch"); err != nil {
			return err
		}
	}
	return nil
}

// origin: hot-update S9 plan §10.2 / T83
// gate-control: TestInfraLayeringControls
func TestInfraLayeringAllOS(t *testing.T) {
	sources := brokerGateSources(t, repoRoot(t))
	for _, path := range []string{"internal/infracontract/types.go", "cmd/tether-infra-helper/main.go", "cmd/tether-infra-launch/main.go"} {
		if sources[path] == "" {
			t.Fatalf("empty S9 dependency root %s", path)
		}
	}
	if err := checkInfraLayering(sources); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S9 plan §11 / T83 / M31
func TestInfraLayeringControls(t *testing.T) {
	good := map[string]string{
		"cmd/tether-infra-helper/main.go":    `package main;import _ "github.com/LinZiyang666/tether/internal/infrapriv";func main(){}`,
		"internal/infrapriv/policy_other.go": `package priv;import "io";var _ io.Reader`,
	}
	root := transferFixtureTree(t, good)
	read := func() map[string]string { return brokerGateSources(t, root) }
	if err := checkInfraLayering(read()); err != nil {
		t.Fatal(err)
	}
	for _, ban := range []string{"internal/agentboot", "internal/infraupgrade", "internal/proto", "internal/testharness", "github.com/nats-io/nats.go", "github.com/nats-io/nats-server/v2"} {
		imp := ban
		if strings.HasPrefix(ban, "internal/") {
			imp = modPrefix + ban
		}
		transferWriteFixture(t, root, "internal/infrapriv/policy_other.go", "package priv;import _ "+strconv.Quote(imp))
		if checkInfraLayering(read()) == nil {
			t.Fatalf("on-disk indirect root import escaped: %s", ban)
		}
	}
	transferWriteFixture(t, root, "internal/infrapriv/policy_other.go", `package priv;import _ "github.com/LinZiyang666/tether/internal/infrahost"`)
	transferWriteFixture(t, root, "internal/infrahost/unit.go", `package host;import _ "github.com/LinZiyang666/tether/internal/infrapriv"`)
	if checkInfraLayering(read()) == nil {
		t.Fatal("on-disk package cycle escaped")
	}
	transferWriteFixture(t, root, "internal/infrapriv/policy_other.go", good["internal/infrapriv/policy_other.go"])
	for _, imp := range []string{"internal/proto", "internal/infraupgrade", "github.com/nats-io/nats.go"} {
		if strings.HasPrefix(imp, "internal/") {
			imp = modPrefix + imp
		}
		transferWriteFixture(t, root, "internal/infracontract/types.go", "package contract;import _ "+strconv.Quote(imp))
		if checkInfraLayering(read()) == nil {
			t.Fatal("on-disk neutral contract widening escaped")
		}
	}
	transferWriteFixture(t, root, "internal/infracontract/types.go", "package contract;import \"io\";var _ io.Reader")
	if err := checkInfraLayering(read()); err != nil {
		t.Fatal(err)
	}
}

// S10 fullstack owners, on the AllOS import graph (Darwin twins and indirect
// local edges included). Contracts and pure judges are stdlib-only below every
// product owner; the single evidence judge never links a runtime or harness; the
// harness consumes contracts, neutral leaves, testharness and the real CLI but no
// agent/broker kernel; the store witness sits above storage (the lead's
// storage→storewitness cycle fix) and below every runtime; product code never
// imports the test tree. Each layer row is a non-empty actual root.
// origin: hot-update S10 plan §12.2 layering / P28
// gate-control: TestFullstackLayeringControls
type fullstackLayer struct {
	pkg     string
	allowed []string // non-nil: the complete non-stdlib direct-import set
	banned  []string // transitive closure bans (module-relative or third-party roots)
	why     string
}

var fullstackKernelBans = []string{
	"internal/agent", "internal/broker", "internal/brokercontrol", "internal/brokerimage", "internal/agentboot",
	"internal/agentimage", "internal/execsession", "internal/ptysession", "internal/flow", "internal/tunnel",
	"cmd", "test/hotupdate/acceptance/contractstub",
}

var fullstackLayers = []fullstackLayer{
	{pkg: "test/hotupdate/acceptance/contract", allowed: []string{}, why: "neutral evidence DTOs and schema are stdlib-only"},
	{pkg: "test/hotupdate/acceptance/contractstub", allowed: []string{"test/hotupdate/acceptance/contract"}, why: "inert stubs name only the contract"},
	{pkg: "test/hotupdate/acceptance", allowed: []string{"test/hotupdate/acceptance/contract"}, why: "pure judges cannot import a product control implementation"},
	{pkg: "test/hotupdate/evidencecmd", banned: append([]string{"internal/testharness", "internal/cluster", "test/fullstackharness", "github.com/nats-io", "github.com/hashicorp/raft", "github.com/hashicorp/yamux"}, fullstackKernelBans...), why: "the evidence judge decodes raw evidence and never links a runtime or harness"},
	{pkg: "test/fullstackharness", banned: fullstackKernelBans, why: "the harness drives real binaries through the CLI and never links an agent/broker kernel"},
	{pkg: "test/fullstackharness/cmd/supervise", banned: fullstackKernelBans, why: "the supervisor launcher owns only processes"},
	{pkg: "internal/storewitness", allowed: []string{"internal/drillobserve", "internal/rawsocket", "internal/sealedstate", "internal/storage"}, banned: []string{"internal/broker", "internal/agent", "internal/brokercontrol", "internal/cluster", "cmd", "test", "github.com/nats-io", "github.com/hashicorp/raft", "github.com/hashicorp/yamux"}, why: "the witness observes the current-store RO boundary without owning a store or runtime"},
}

func fullstackLayerMatch(dep, ban string) bool { return dep == ban || strings.HasPrefix(dep, ban+"/") }

// fullstackImportGraph maps each production package directory to its imports,
// module-relative for local packages. Local membership is kept separately so a
// third-party root can never pass for a missing local source.
func fullstackImportGraph(sources map[string]string) (map[string]map[string]bool, map[string]bool, error) {
	graph, local := map[string]map[string]bool{}, map[string]bool{}
	for _, path := range brokerSortedSourcePaths(sources) {
		if !isProdGoFile(path) {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, sources[path], parser.ImportsOnly)
		if err != nil {
			return nil, nil, err
		}
		pkg := filepath.ToSlash(filepath.Dir(path))
		if graph[pkg] == nil {
			graph[pkg] = map[string]bool{}
		}
		for _, spec := range file.Imports {
			imp, _ := strconv.Unquote(spec.Path.Value)
			name := strings.TrimPrefix(imp, modPrefix)
			graph[pkg][name] = true
			if name != imp {
				local[name] = true
				if (strings.HasPrefix(pkg, "internal/") || strings.HasPrefix(pkg, "cmd/")) && fullstackLayerMatch(name, "test") {
					return nil, nil, fmt.Errorf("%s: product code imports the test tree %s", path, imp)
				}
			}
		}
	}
	return graph, local, nil
}

func checkFullstackLayering(sources map[string]string) error {
	graph, local, err := fullstackImportGraph(sources)
	if err != nil {
		return err
	}
	for _, layer := range fullstackLayers {
		if graph[layer.pkg] == nil {
			return fmt.Errorf("empty S10 layering root %s", layer.pkg)
		}
		for dep := range graph[layer.pkg] {
			stdlib := !local[dep] && !strings.Contains(strings.Split(dep, "/")[0], ".")
			if layer.allowed == nil || stdlib || slices.ContainsFunc(layer.allowed, func(allow string) bool { return fullstackLayerMatch(dep, allow) }) {
				continue
			}
			return fmt.Errorf("%s imports %s outside its allowed set: %s", layer.pkg, dep, layer.why)
		}
		seen := map[string]bool{layer.pkg: true}
		queue := []string{layer.pkg}
		for len(queue) > 0 {
			pkg := queue[0]
			queue = queue[1:]
			for dep := range graph[pkg] {
				for _, ban := range layer.banned {
					if fullstackLayerMatch(dep, ban) {
						return fmt.Errorf("%s reaches %s through %s: %s", layer.pkg, dep, pkg, layer.why)
					}
				}
				if local[dep] && !seen[dep] {
					if graph[dep] == nil {
						return fmt.Errorf("%s: missing local dependency source %s", pkg, dep)
					}
					seen[dep] = true
					queue = append(queue, dep)
				}
			}
		}
	}
	// Go rejects a cycle per platform; the AllOS union also catches a Darwin-only
	// back edge, such as storage importing the witness that observes it.
	state := map[string]int{}
	var visit func(string) error
	visit = func(pkg string) error {
		switch state[pkg] {
		case 1:
			return fmt.Errorf("AllOS package cycle through %s", pkg)
		case 2:
			return nil
		}
		state[pkg] = 1
		for dep := range graph[pkg] {
			if local[dep] && graph[dep] != nil {
				if err := visit(dep); err != nil {
					return err
				}
			}
		}
		state[pkg] = 2
		return nil
	}
	for pkg := range graph {
		if err := visit(pkg); err != nil {
			return err
		}
	}
	return nil
}

// fullstackProductionGo is the AllOS production Go view of one tree.
func fullstackProductionGo(tree fullstackTree) map[string]string {
	sources := map[string]string{}
	for path, source := range tree.files {
		if isProdGoFile(path) {
			sources[path] = source
		}
	}
	return sources
}

// origin: hot-update S10 plan §12.2 layering / P28
func TestFullstackLayeringAllOS(t *testing.T) {
	if err := checkFullstackLayering(fullstackProductionGo(fullstackRepoTree(t))); err != nil {
		t.Fatal(err)
	}
}

// The fixture is the real closure of every S10 layer root, copied to disk; each
// mutation adds one production file and re-reads the copy with the gate's reader.
// origin: hot-update S10 plan §10.2 P28 / §12.1 bypass-transitive-layering
func TestFullstackLayeringControls(t *testing.T) {
	real := fullstackRepoTree(t)
	graph, local, err := fullstackImportGraph(fullstackProductionGo(real))
	if err != nil {
		t.Fatal(err)
	}
	closure := []string{"internal/storage"}
	for _, layer := range fullstackLayers {
		closure = append(closure, layer.pkg)
	}
	for i := 0; i < len(closure); i++ {
		for dep := range graph[closure[i]] {
			if local[dep] && !slices.Contains(closure, dep) {
				closure = append(closure, dep)
			}
		}
	}
	root := t.TempDir()
	for path, source := range fullstackProductionGo(real) {
		if slices.Contains(closure, filepath.ToSlash(filepath.Dir(path))) {
			transferWriteFixture(t, root, path, source)
		}
	}
	check := func() error { return checkFullstackLayering(fullstackProductionGo(readFullstackTree(t, root))) }
	leaf := func(path, pkg, imp, want string) fullstackMutation {
		return fullstackMutation{path: path, new: "package " + pkg + "\n\nimport _ " + strconv.Quote(modPrefix+imp) + "\n", want: want}
	}
	proveFullstackMutations(t, root, check,
		leaf("test/hotupdate/acceptance/contract/owner_other.go", "contract", "internal/proto", "outside its allowed set"),
		fullstackMutation{path: "test/hotupdate/acceptance/contract/owner_other.go", new: "package contract\n\nimport _ \"golang.org/x/sys/unix\"\n", want: "outside its allowed set"},
		leaf("test/hotupdate/acceptance/contractstub/wire.go", "contractstub", "test/hotupdate/acceptance", "outside its allowed set"),
		leaf("test/hotupdate/acceptance/control.go", "acceptance", "internal/upgradetx", "outside its allowed set"),
		leaf("test/hotupdate/evidencecmd/harness.go", "main", "internal/testharness", "reaches internal/testharness"),
		leaf("test/fullstackharness/kernel_other.go", "fullstackharness", "internal/agent", "reaches internal/agent"),
		leaf("test/fullstackharness/stub.go", "fullstackharness", "test/hotupdate/acceptance/contractstub", "reaches test/hotupdate/acceptance/contractstub"),
		leaf("test/fullstackharness/cmd/supervise/kernel.go", "main", "internal/broker", "reaches internal/broker"),
		leaf("internal/storewitness/runtime_other.go", "storewitness", "internal/broker", "outside its allowed set"),
		leaf("internal/storage/witness_other.go", "storage", "internal/storewitness", "cycle"),
		leaf("internal/rawsocket/acceptance.go", "rawsocket", "test/fullstackharness", "imports the test tree"),
	)
	// A neutral-looking new leaf cannot launder a kernel edge: the closure walk
	// reaches through it although the harness's own import names no banned owner.
	transferWriteFixture(t, root, "internal/neutralleaf/leaf.go", "package neutralleaf\n\nimport _ "+strconv.Quote(modPrefix+"internal/broker")+"\n")
	transferWriteFixture(t, root, "internal/broker/broker.go", "package broker\n")
	proveFullstackMutations(t, root, check,
		leaf("test/fullstackharness/neutral.go", "fullstackharness", "internal/neutralleaf", "reaches internal/broker through internal/neutralleaf"),
		leaf("test/hotupdate/evidencecmd/neutral.go", "main", "internal/neutralleaf", "reaches internal/broker through internal/neutralleaf"),
		leaf("internal/storewitness/neutral.go", "storewitness", "internal/neutralleaf", "outside its allowed set"),
	)
	// The witness's allowed storage edge still cannot launder a raft owner.
	proveFullstackMutations(t, root, check,
		leaf("internal/storage/raft_other.go", "storage", "internal/cluster", "reaches internal/cluster through internal/storage"),
	)
}
