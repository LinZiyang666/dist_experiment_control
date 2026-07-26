package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Batch-A A1: the wire error-code coverage gate.
//
// WHAT THIS GUARDS
//
// A broker/agent reply code that no table in error_hints.go knows falls through
// brokerCodeExitClass to exitInternal (70) — and docs/usage.md §9.13 tells
// automation to treat 70 as RETRYABLE. So an unclassified terminal failure
// (too_large, self_path, verb_mismatch...) reads to a monitor as "retry me
// forever". This gate makes adding an unclassified code a test failure.
//
// WHY THE SCANNER LOOKS THE WAY IT DOES (read before "simplifying" it)
//
// Codes are not emitted in one syntactic shape. Measured forms in this repo:
//
//	 1. proto.XResp{Code: "literal"}                      — a KeyValueExpr
//	 2. proto.XResp{Code: proto.CodeFoo}                   — KeyValueExpr, const
//	 3. b.replyExposeErr(msg, "port_taken", …)             — a helper argument
//	 4. return codeXferBrokerRestarting, …                 — a returned const
//	 5. return "too_many_in_flight"                        — a returned literal
//	 6. return fmt.Errorf("try_again")                     — wrapped in an error
//	 7. proto.ClusterGrowResp{Code: ar.Code}               — pass-through
//	 8. RunChunk{Reason: "pty_alloc_failed"}               — a DIFFERENT field
//
// A scanner that only understands form 1 sees well under half of them. That
// matters more than it sounds: a gate which claims to cover "every code" while
// silently missing most of them is worse than no gate, because reviewers stop
// looking. (This repo already had one such false promise — proto.RehomeDirective's
// doc claimed "a guard test asserts it has no live publisher"; no such test
// existed.)
//
// Forms 4-6 cannot be recovered by syntax alone: a code can travel through any
// chain of return values, so enumerating them exactly is undecidable without
// full data-flow analysis. This scanner therefore covers forms 1-3 and 8
// EXACTLY, and is HONEST about the rest: form 7 (pass-through) and any Code:
// whose value is a non-constant expression are reported as unresolved and must
// be listed in unresolvedCodeSites with a reason. It does not pretend to be
// complete — see TestErrorCodeScannerDeclaresItsLimits.
//
// The self-check (TestErrorCodeCoverageSelfCheck) synthesises one sample per
// supported form. Without it a scanner that silently degenerates to matching
// nothing would report a perfectly green "0 unclassified codes".

// codeCarryingHelpers maps a helper function name to the 0-based index of the
// parameter that becomes a reply Code. Derived mechanically: a function whose
// body assigns one of its own parameters to a `Code:` field is code-carrying.
// TestCodeCarryingHelperListIsComplete re-derives this from source and fails if
// a new helper appears, so the list cannot rot silently.
var codeCarryingHelpers = map[string]int{
	"replyErr":            1,
	"replyExposeErr":      1,
	"replyExposeRmErr":    1,
	"replyPushErr":        1,
	"replyPullErr":        1,
	"replyCommitErr":      1,
	"replyUpgradeErr":     1,
	"fsRefuse":            1,
	"pathErr":             0,
	"cleanupEntry":        1,
	"pubTransferEvFailed": 4,
	"pubPtyFailed":        2,
	"replyRunFailed":      1,
}

// unclassifiedCodeAllowlist lists emitted codes that deliberately have no exit
// class. EVERY entry needs a reason — an allowlist without reasons is just a
// slower way of having no gate. TestAllowlistEntriesStillHaveEmitters fails if
// an entry stops being emitted, so this cannot become a graveyard.
var unclassifiedCodeAllowlist = map[string]string{
	"agent_rejected": "NOT a terminal code: it is the documented PREFIX of " +
		"`\"agent_rejected:\" + agentResp.Code`, surfaced here because the scanner now reads " +
		"literal prefixes out of concatenations (form 9). cmd/tether/error_hints.go:158-166 " +
		"strips it before the class lookup — pinned by error_hints_test.go:80 — so the agent's " +
		"own code is what actually classifies, and that code is scanned on the agent side. " +
		"Giving the prefix its own exit class would shadow every underlying code with one class.",
	"bucket_create_failed": "the deliberately-UNCLASSIFIED remainder of the tier-B split: " +
		"jetstream_not_ready (75) and tier_b_store_too_small (64) are its classified halves. " +
		"Documented in error_hints.go:90-97; 70 is the correct answer for 'we do not know'.",
	"home_broker_restart": "audit-only: emitted into schema.AuditTransfer at xfer_inflight.go:504, " +
		"never onto a ctl reply, so no exit class applies.",

	// The three below are CATCH-ALL branches: both a permanent cause (disk full,
	// quota) and a transient one (momentary I/O pressure) land here, and the
	// code carries no way to tell them apart. Guessing 75 would tell a monitor
	// to retry a full disk forever — the exact failure mode A1 exists to remove
	// — and guessing 64 would make a recoverable blip terminal. 70 is the
	// honest answer, and it is the same call error_hints.go already made for
	// bucket_create_failed. Split them into classified halves (as G67 did with
	// jetstream_not_ready / tier_b_store_too_small) before classifying.
	"alloc_failed": "catch-all `case err != nil` of public-port allocation (expose.go:262-264); " +
		"covers both DB faults and exhaustion-shaped races with no way to distinguish them.",
	"io_error": "catch-all I/O failure of the agent's pull path (transfer.go:335-337); a full disk " +
		"and a momentary EIO are indistinguishable here. Retrying a full disk is exactly what A1 removes.",
	"object_put_failed": "catch-all JetStream object-store Put failure (transfer.go:420-422). Its " +
		"classified halves already exist (jetstream_not_ready=75, tier_b_store_too_small=64); this is the remainder.",

	// External review M2: these two were classified 75 (self-healing transient)
	// on the strength of their most common cause, but each emitter funnels
	// several outcomes with OPPOSITE remedies into one code:
	//
	//   pty_alloc_failed  — transient fd/pty pressure, a permanently absent
	//                       /dev/ptmx, and a failed NATS SubscribeSync
	//                       (internal/agent/run.go:96-123)
	//   download_failed   — a network blip, a permanent HTTP 404/non-2xx, and
	//                       exceeding the 64 MiB artifact ceiling
	//                       (internal/agent/upgrade.go:241-260)
	//
	// Calling those 75 tells automation to retry a missing /dev/ptmx and a 404
	// forever — the same defect A1 set out to remove, introduced by A1 itself.
	// Reverted to unclassified: 70 is the honest answer while one code covers
	// both a retryable and a terminal cause. Splitting them at the emitter
	// (download_http_status / download_too_large / pty_unavailable /
	// attach_subscribe_failed) adds new wire values, which is its own increment.
	"pty_alloc_failed": "mixes transient PTY pressure with a permanently absent /dev/ptmx and a failed " +
		"SubscribeSync; no single retry semantics is correct until the emitter splits it.",
	"download_failed": "mixes network blips with permanent HTTP 404/non-2xx and the 64 MiB ceiling; " +
		"same reason.",
}

// unresolvedCodeSites lists the exact SITES whose code the scanner cannot
// resolve statically. Each needs a reason.
//
// External review (and internal M3): these used to be keyed by FILE, so one
// exemption blanket-covered every unresolved site in that file — and the files
// involved are the hottest reply paths in broker and agent. Keyed by file:line,
// a new dynamic site in an already-exempted file has to be looked at.
//
// Line numbers do drift. That is deliberate: a stale key fails the test and
// forces someone to re-read the site, which is cheaper than an exemption that
// silently widens.
var unresolvedCodeSites = map[string]string{
	// Agent run reasons are finite locals selected immediately above each site.
	"internal/agent/run.go:169": "Reason is remoteFSFailReason(ferr); its literal outcomes are scanned at their definitions.",
	"internal/agent/run.go:170": "pubPtyFailed forwards the same remoteFSFailReason value as :169.",
	"internal/agent/run.go:211": "Reason is exec_failed or remoteFSFailReason(startErr), selected immediately above.",
	"internal/agent/run.go:213": "pubPtyFailed forwards the same finite reason value as :211.",

	// Agent transfer codes come from PathValidationError or the local transfer
	// state machine; this scanner does not perform interprocedural data flow.
	"internal/agent/transfer.go:85":  "code is io_error or PathValidationError.Code; both origins are scanned.",
	"internal/agent/transfer.go:89":  "audit emission forwards the same code as :85.",
	"internal/agent/transfer.go:123": "finalize's code parameter is supplied by literal/PathValidationError call sites in this function.",
	"internal/agent/transfer.go:124": "audit emission forwards finalize's same code parameter.",
	"internal/agent/transfer.go:230": "PathValidationError.Code is defined by the path validator's finite code set.",
	"internal/agent/transfer.go:334": "transfer failure code is selected by the local state machine before this reply.",
	"internal/agent/transfer.go:346": "transfer failure audit forwards the same locally selected code.",

	// Cluster trigger/admin handlers pass through the typed adminsock namespace.
	"internal/broker/cluster_grow_trigger.go:149":    "verbatim adminsock response code; adminsock has its own registry.",
	"internal/broker/cluster_grow_trigger.go:180":    "verbatim adminsock response code; adminsock has its own registry.",
	"internal/broker/cluster_grow_trigger.go:204":    "verbatim adminsock response code; adminsock has its own registry.",
	"internal/broker/cluster_upgrade_trigger.go:116": "verbatim adminsock response code; adminsock has its own registry.",
	"internal/broker/cluster_upgrade_trigger.go:119": "verbatim adminsock response code; adminsock has its own registry.",
	"internal/broker/cluster_upgrade_trigger.go:180": "codeDataplaneNotConverged is a local alias of the classified proto code.",
	"internal/broker/cluster_manifest.go:114":        "clusterCodeFor returns the typed adminsock error namespace.",

	// Every site below is an adminsock.Response.Code populated by clusterCodeFor.
	"internal/broker/clusterstatus.go:667":       "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:687":       "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:692":       "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:700":       "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:712":       "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:717":       "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:722":       "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:727":       "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:732":       "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:747":       "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:827":       "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/clusterstatus.go:890":       "adminsock response; clusterCodeFor maps typed cluster errors.",
	"internal/broker/force_single_online.go:212": "force-single refusal code is a typed adminsock constant returned by fsArmVerdict.",

	"internal/broker/proxy_cluster_wire.go:138": "proxyDegradedCode returns the finite proxy cluster status code set.",
	"internal/broker/topology_reconcile.go:149": "false positive: topoSelfReport.Reason is an internal report field, not a wire reply code.",

	// Transfer reply helpers receive finite values from transferGate,
	// xferProvisionRefusal, or transfers.put; their literal origins are scanned.
	"internal/broker/transfer.go:435":  "audit-only watchdog code selected by the switch immediately above.",
	"internal/broker/transfer.go:565":  "transferGate returns the finite session/member/node/store refusal set.",
	"internal/broker/transfer.go:620":  "xferProvisionRefusal returns the finite bucket provisioning refusal set.",
	"internal/broker/transfer.go:644":  "transfers.put returns duplicate or in-flight-cap refusal codes.",
	"internal/broker/transfer.go:717":  "transferGate returns the finite session/member/node/store refusal set.",
	"internal/broker/transfer.go:746":  "xferProvisionRefusal returns the finite bucket provisioning refusal set.",
	"internal/broker/transfer.go:778":  "transfers.put returns duplicate or in-flight-cap refusal codes.",
	"internal/broker/transfer.go:850":  "transferGate returns the finite session/member/node/store refusal set.",
	"internal/broker/transfer.go:1090": "transferGate returns the finite session/member/store refusal set.",
	"internal/broker/transfer.go:1172": "transferGate returns the finite session/member/store refusal set.",
}

type emittedCode struct {
	code string
	site string
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	// cmd/tether -> repo root
	root := filepath.Dir(filepath.Dir(wd))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repo root %q has no go.mod: %v", root, err)
	}
	return root
}

// scanTree walks the given package dirs (relative to root) and returns every
// statically resolvable emitted code, plus the set of files holding a Code:
// whose value could not be resolved.
// literalCodePrefix reads the machine-readable code out of a `"code: " + detail`
// concatenation. Only a STRING literal on the left counts, and only up to the
// first colon — the same split cmd/tether performs when classifying the reply,
// so the gate and the consumer agree on where the code ends. A concatenation
// that does not start with a literal carries no statically visible code and is
// reported as unresolved rather than guessed at.
func literalCodePrefix(e *ast.BinaryExpr) (string, bool) {
	if e.Op != token.ADD {
		return "", false
	}
	lit, ok := e.X.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	code, _, found := strings.Cut(s, ":")
	if !found || code == "" {
		return "", false
	}
	return code, true
}

func scanTree(t *testing.T, root string, dirs []string) (codes []emittedCode, unresolved map[string]string) {
	t.Helper()
	unresolved = map[string]string{}

	// Pass 0: string constants (name -> value), used to resolve form 2 and 4.
	// internal/adminsock is included even when it is not being scanned for
	// emissions: broker replies reference adminsock.Code* by selector, and
	// error_hints.go really does classify those values (see its "adminsock
	// cluster codes" block), so they must resolve rather than land in the
	// unresolved list.
	constVal := map[string]string{}
	constDirs := append(append([]string{}, dirs...), "internal/adminsock")
	forEachGoFile(t, root, constDirs, func(rel string, f *ast.File, fset *token.FileSet) {
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, s := range gd.Specs {
				vs, ok := s.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, nm := range vs.Names {
					if i >= len(vs.Values) {
						continue
					}
					if lit, ok := vs.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						if v, err := strconv.Unquote(lit.Value); err == nil {
							constVal[nm.Name] = v
						}
					}
				}
			}
		}
	})

	add := func(v, site string) {
		if v != "" {
			codes = append(codes, emittedCode{v, site})
		}
	}

	forEachGoFile(t, root, dirs, func(rel string, f *ast.File, fset *token.FileSet) {
		// Track the parameter names of the function we are inside. A
		// `Code: <param>` is the DEFINITION BODY of a code-carrying helper, not
		// an emission site: the actual codes arrive at its call sites and are
		// picked up by form 3. Reporting those bodies as "unresolved" would
		// force a dozen meaningless exemptions and train readers to ignore the
		// unresolved list — the one signal that is supposed to stay short.
		//
		// External review R1: that exemption applied to EVERY function, so any
		// ordinary handler forwarding its own parameter into a code-carrying
		// helper was skipped too — the most likely shape of a real dynamic code,
		// and the scanner reported zero. The exemption now requires the enclosing
		// function to BE a code-carrying helper, which is the only case the
		// "covered at its call sites" argument actually holds for.
		params := map[string]bool{}
		inCodeHelper := false
		var walk func(n ast.Node) bool
		walk = func(n ast.Node) bool {
			if fd, ok := n.(*ast.FuncDecl); ok {
				params = map[string]bool{}
				_, inCodeHelper = codeCarryingHelpers[fd.Name.Name]
				if fd.Type.Params != nil {
					for _, fl := range fd.Type.Params.List {
						for _, nm := range fl.Names {
							params[nm.Name] = true
						}
					}
				}
				if fd.Body != nil {
					ast.Inspect(fd.Body, walk)
				}
				params = map[string]bool{}
				inCodeHelper = false
				return false
			}
			switch node := n.(type) {
			// Forms 1, 2, 7, 8: `Code:` / `Reason:` in a composite literal.
			case *ast.KeyValueExpr:
				id, ok := node.Key.(*ast.Ident)
				if !ok || (id.Name != "Code" && id.Name != "Reason") {
					return true
				}
				if v, ok := node.Value.(*ast.Ident); ok && inCodeHelper && params[v.Name] {
					return true // helper definition body; covered at its call sites
				}
				site := rel + ":" + itoa(fset.Position(node.Pos()).Line)
				switch v := node.Value.(type) {
				case *ast.BinaryExpr:
					// Form 9, see the CallExpr arm below.
					if c, ok := literalCodePrefix(v); ok {
						add(c, site)
					} else {
						unresolved[site] = rel
					}
				case *ast.BasicLit:
					if v.Kind == token.STRING {
						if s, err := strconv.Unquote(v.Value); err == nil {
							add(s, site)
						}
					}
				case *ast.Ident:
					if s, ok := constVal[v.Name]; ok {
						add(s, site)
					} else {
						unresolved[site] = rel
					}
				case *ast.SelectorExpr:
					if s, ok := constVal[v.Sel.Name]; ok {
						add(s, site)
					} else {
						unresolved[site] = rel
					}
				default:
					unresolved[site] = rel
				}

			// Form 3: an argument flowing into a known code-carrying helper.
			case *ast.CallExpr:
				var name string
				switch fn := node.Fun.(type) {
				case *ast.Ident:
					name = fn.Name
				case *ast.SelectorExpr:
					name = fn.Sel.Name
				default:
					return true
				}
				idx, ok := codeCarryingHelpers[name]
				if !ok || idx >= len(node.Args) {
					return true
				}
				site := rel + ":" + itoa(fset.Position(node.Pos()).Line)
				// External review B3: this switch used to have no default and no
				// else on the Ident/Selector arms, so a code arriving through a
				// local variable or a function result fell out of BOTH results —
				// not classified, not reported. The gate's own spec (plan D1
				// segment 3) requires an unresolvable value to hard-fail or carry
				// an explicit exemption; silently vanishing is the one outcome it
				// must never produce, because it makes "0 unclassified codes" mean
				// "we could not see any".
				unres := func() { unresolved[site] = rel }
				switch a := node.Args[idx].(type) {
				case *ast.BinaryExpr:
					// Form 9: `"code: " + err.Error()`. These reached the gate as
					// "unresolvable" and, before external review R1 narrowed the
					// parameter exemption, were not reported at all — seven of them
					// in internal/broker/run.go alone. But the code IS statically
					// present: it is the literal prefix before the colon, which is
					// exactly what cmd/tether's runFailureMessage splits back off
					// (batch-A A1 step 4). Reading it is strictly better than
					// exempting it, since the code then enters the classification
					// check like any other.
					if c, ok := literalCodePrefix(a); ok {
						add(c, site)
					} else {
						unres()
					}
				case *ast.BasicLit:
					if a.Kind == token.STRING {
						if s, err := strconv.Unquote(a.Value); err == nil {
							add(s, site)
						} else {
							unres()
						}
					} else {
						unres()
					}
				case *ast.Ident:
					if s, ok := constVal[a.Name]; ok {
						add(s, site)
					} else if !inCodeHelper || !params[a.Name] {
						// A parameter is exempt ONLY inside a code-carrying helper's
						// own definition, where the real codes arrive at its call
						// sites. Anywhere else, a parameter is exactly the dynamic
						// value this gate exists to report (external review R1).
						unres()
					}
				case *ast.SelectorExpr:
					if s, ok := constVal[a.Sel.Name]; ok {
						add(s, site)
					} else {
						unres()
					}
				default:
					unres()
				}
			}
			return true
		}
		ast.Inspect(f, walk)
	})
	return codes, unresolved
}

func forEachGoFile(t *testing.T, root string, dirs []string, fn func(rel string, f *ast.File, fset *token.FileSet)) {
	t.Helper()
	fset := token.NewFileSet()
	for _, d := range dirs {
		abs := filepath.Join(root, d)
		err := filepath.Walk(abs, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, p, nil, 0)
			if perr != nil {
				return nil
			}
			rel, _ := filepath.Rel(root, p)
			fn(filepath.ToSlash(rel), f, fset)
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", abs, err)
		}
	}
}

func itoa(i int) string { return strconv.Itoa(i) }

// scannedTrees is the set of packages that emit ctl-facing reply codes.
var scannedTrees = []string{"internal/broker", "internal/agent", "internal/proto", "internal/spawnsafe"}

// TestErrorCodeCoverage is the gate: every statically resolvable emitted code
// must either have an exit class or be on the allowlist with a reason.
func TestErrorCodeCoverage(t *testing.T) {
	root := repoRoot(t)
	codes, unresolved := scanTree(t, root, scannedTrees)

	if len(codes) == 0 {
		t.Fatal("scanner found no emitted codes at all — it has degenerated; see TestErrorCodeCoverageSelfCheck")
	}

	firstSite := map[string]string{}
	for _, c := range codes {
		if _, seen := firstSite[c.code]; !seen {
			firstSite[c.code] = c.site
		}
	}

	var missing []string
	for code, site := range firstSite {
		if _, classified := brokerCodeExitClasses[code]; classified {
			continue
		}
		if _, allowed := unclassifiedCodeAllowlist[code]; allowed {
			continue
		}
		missing = append(missing, code+"  (first emitted at "+site+")")
	}
	sort.Strings(missing)

	if len(missing) > 0 {
		t.Errorf("%d emitted reply code(s) have no exit class and no allowlist entry.\n"+
			"Each one currently exits 70, which docs/usage.md §9.13 tells automation to RETRY.\n"+
			"Classify it in brokerCodeExitClasses, or add it to unclassifiedCodeAllowlist WITH A REASON:\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}

	// Unresolved sites must be declared, so "the scanner cannot see it" is a
	// recorded decision rather than an accident.
	// Keyed by SITE, not by file: a file-keyed map kept only the first dynamic
	// site in each file, so a second one in the same file vanished — the same
	// "one entry blankets the rest" defect the file-wide exemptions had, moved
	// into the reporting side (external review R1). File-wide exemption entries
	// are gone with it; an exemption now names one line and covers one line.
	var undeclared []string
	for site := range unresolved {
		if _, ok := unresolvedCodeSites[site]; ok {
			continue // exempted at this exact line
		}
		undeclared = append(undeclared, site)
	}
	sort.Strings(undeclared)
	if len(undeclared) > 0 {
		t.Errorf("%d site(s) hold a Code:/Reason: the scanner cannot resolve statically and that are not "+
			"declared in unresolvedCodeSites. Add an entry explaining why the code is covered elsewhere "+
			"(or make the value a constant):\n  %s", len(undeclared), strings.Join(undeclared, "\n  "))
	}
}

// TestAllowlistEntriesStillHaveEmitters keeps the allowlist from rotting into a
// graveyard of codes nobody emits any more.
func TestAllowlistEntriesStillHaveEmitters(t *testing.T) {
	root := repoRoot(t)
	codes, unresolved := scanTree(t, root, scannedTrees)
	emitted := map[string]bool{}
	for _, c := range codes {
		emitted[c.code] = true
	}
	// bucket_create_failed is emitted via a returned literal (form 5), which the
	// scanner deliberately does not cover; grep the tree for its literal instead.
	raw := map[string]bool{}
	forEachGoFile(t, root, scannedTrees, func(rel string, f *ast.File, fset *token.FileSet) {
		ast.Inspect(f, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if s, err := strconv.Unquote(lit.Value); err == nil {
					raw[s] = true
				}
			}
			return true
		})
	})
	for code, reason := range unclassifiedCodeAllowlist {
		if reason == "" {
			t.Errorf("allowlist entry %q has an empty reason — every exemption must justify itself", code)
		}
		if !emitted[code] && !raw[code] {
			t.Errorf("allowlist entry %q is no longer emitted anywhere; delete it", code)
		}
	}
	for key, reason := range unresolvedCodeSites {
		if reason == "" {
			t.Errorf("unresolvedCodeSites entry %q has an empty reason", key)
		}
		i := strings.LastIndexByte(key, ':')
		if i < 0 {
			t.Errorf("unresolvedCodeSites entry %q is file-wide; every exemption must name one site", key)
			continue
		}
		if _, err := strconv.Atoi(key[i+1:]); err != nil {
			t.Errorf("unresolvedCodeSites entry %q is not keyed by file:line", key)
			continue
		}
		if _, ok := unresolved[key]; !ok {
			t.Errorf("unresolvedCodeSites entry %q no longer identifies an unresolved site; "+
				"the code moved, became resolvable, or was deleted — re-check and remove/update it", key)
		}
	}
}

// TestCodeCarryingHelperListIsComplete re-derives the helper list from source:
// any function that assigns one of its own parameters to a Code: field is
// code-carrying. A new helper that is not registered would make the coverage
// gate blind to every call site that uses it.
func TestCodeCarryingHelperListIsComplete(t *testing.T) {
	root := repoRoot(t)
	found := map[string]int{}
	forEachGoFile(t, root, scannedTrees, func(rel string, f *ast.File, fset *token.FileSet) {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil || fd.Type.Params == nil {
				continue
			}
			paramIdx := map[string]int{}
			idx := 0
			for _, fl := range fd.Type.Params.List {
				for _, nm := range fl.Names {
					paramIdx[nm.Name] = idx
					idx++
				}
				if len(fl.Names) == 0 {
					idx++
				}
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				kv, ok := n.(*ast.KeyValueExpr)
				if !ok {
					return true
				}
				k, ok := kv.Key.(*ast.Ident)
				if !ok || (k.Name != "Code" && k.Name != "Reason") {
					return true
				}
				if v, ok := kv.Value.(*ast.Ident); ok {
					if pi, isParam := paramIdx[v.Name]; isParam {
						found[fd.Name.Name] = pi
					}
				}
				return true
			})
		}
	})

	for name, idx := range found {
		got, ok := codeCarryingHelpers[name]
		if !ok {
			t.Errorf("helper %q takes a reply code at arg #%d but is NOT in codeCarryingHelpers — "+
				"every call site that uses it is invisible to TestErrorCodeCoverage. Register it.", name, idx)
			continue
		}
		if got != idx {
			t.Errorf("helper %q carries its code at arg #%d, but codeCarryingHelpers says #%d", name, idx, got)
		}
	}
	for name := range codeCarryingHelpers {
		if _, ok := found[name]; !ok {
			t.Errorf("codeCarryingHelpers lists %q but no such code-carrying helper exists any more; delete it", name)
		}
	}
}

// TestErrorCodeCoverageSelfCheck is the reason this gate can be trusted.
//
// A scanner that silently stops matching reports "0 unclassified codes" — a
// perfect green that means nothing. So: synthesise one source sample per
// SUPPORTED form and assert the scanner finds each. If someone simplifies the
// scanner and breaks a form, this fails instead of the gate going quietly blind.
func TestErrorCodeCoverageSelfCheck(t *testing.T) {
	samples := []struct {
		form string
		src  string
		want string
	}{
		{
			form: "1: Code: string literal",
			src: `package x
type R struct{ Code string }
func f() R { return R{Code: "selfcheck_literal"} }`,
			want: "selfcheck_literal",
		},
		{
			form: "2: Code: named constant",
			src: `package x
const codeSelfcheckConst = "selfcheck_const"
type R struct{ Code string }
func f() R { return R{Code: codeSelfcheckConst} }`,
			want: "selfcheck_const",
		},
		{
			form: "3: code-carrying helper argument",
			src: `package x
func replyExposeErr(msg any, code string, detail string) {}
func f() { replyExposeErr(nil, "selfcheck_helper", "") }`,
			want: "selfcheck_helper",
		},
		{
			form: "8: Reason: field (a different field name)",
			src: `package x
type C struct{ Reason string }
func f() C { return C{Reason: "selfcheck_reason"} }`,
			want: "selfcheck_reason",
		},
	}

	for _, s := range samples {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "sample.go"), []byte(s.src), 0o600); err != nil {
			t.Fatalf("%s: write sample: %v", s.form, err)
		}
		if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module selfcheck\n\ngo 1.25\n"), 0o600); err != nil {
			t.Fatalf("%s: write go.mod: %v", s.form, err)
		}
		codes, _ := scanTree(t, dir, []string{"."})
		var got []string
		for _, c := range codes {
			got = append(got, c.code)
		}
		if !contains(got, s.want) {
			t.Errorf("SELF-CHECK FAILED for form %q: the scanner did not find %q (found %v).\n"+
				"The coverage gate is therefore blind to this emission form and its green result is meaningless.",
				s.form, s.want, got)
		}
	}
}

// TestErrorCodeScannerDeclaresItsLimits pins the scanner's honesty: the forms it
// does NOT cover must stay documented. A gate that overstates its reach is the
// failure mode this whole file exists to avoid.
func TestErrorCodeScannerDeclaresItsLimits(t *testing.T) {
	root := repoRoot(t)
	src, err := os.ReadFile(filepath.Join(root, "cmd", "tether", "error_code_coverage_test.go"))
	if err != nil {
		t.Fatalf("read own source: %v", err)
	}
	for _, must := range []string{
		"undecidable without full data-flow analysis",
		"return \"too_many_in_flight\"",
		"It does not pretend to be complete",
	} {
		if !strings.Contains(string(src), must) {
			t.Errorf("the scanner's documented limits no longer mention %q — if coverage really was "+
				"extended, update this test; if not, restore the caveat.", must)
		}
	}
}

func contains(hay []string, needle string) bool {
	for _, h := range hay {
		if h == needle {
			return true
		}
	}
	return false
}
