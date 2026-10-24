package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// origin: hot-update S4 plan §9.2 / N1-REFUSAL (drill 36.L).
// Read the actual emitters, rather than copied expected strings: reverting any
// one site to a bare agent_upgrading must break the N-1 classification proof.
// The existing agent gate tests separately exercise these replies over NATS
// and prove refusal precedes parsing/spawn/effects, including pending rollback.
func TestLegacyUpgradeRefusalsRemainTransientOnNMinusOneClients(t *testing.T) {
	root := repoRoot(t)
	for _, tc := range []struct {
		file, shape string
		count       int
	}{
		{"internal/agent/exec.go", "ExecChunk", 1},
		{"internal/agent/run.go", "RunChunk", 2},
		{"internal/agent/expose.go", "ExposeForwardedResp", 1},
		// hot-update S5 AGENT-SLIM moved the transfer handlers out of internal/agent.
		{"internal/filetransfer/handler.go", "PushPrepareResp", 1},
		{"internal/filetransfer/handler.go", "TransferCommitResp", 1},
		{"internal/filetransfer/handler.go", "PullPrepareResp", 1},
	} {
		t.Run(tc.shape, func(t *testing.T) {
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, filepath.Join(root, tc.file), nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			ast.Inspect(f, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok {
					return true
				}
				typ, ok := lit.Type.(*ast.SelectorExpr)
				if !ok || typ.Sel.Name != tc.shape {
					return true
				}
				fields := map[string]string{}
				for _, elt := range lit.Elts {
					kv, ok := elt.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					key, keyOK := kv.Key.(*ast.Ident)
					value, valueOK := kv.Value.(*ast.BasicLit)
					if keyOK && valueOK && value.Kind == token.STRING {
						fields[key.Name], err = strconv.Unquote(value.Value)
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				if !strings.Contains(fields["Reason"]+fields["Error"]+fields["Code"], "agent_upgrading") {
					return true
				}
				count++
				var current error
				code, text := fields["Code"], fields["Error"]
				switch tc.shape {
				case "RunChunk":
					text = fields["Reason"]
					current = runFailureMessage(text)
					if fields["Kind"] != "failed" {
						t.Error("run admission refusal must be a failed chunk")
					}
				case "ExecChunk":
					current = execFailureMessage(text)
					if fields["Kind"] != "error" {
						t.Error("exec admission refusal must be an error chunk")
					}
				case "ExposeForwardedResp":
					// Both v0.6.0 and current brokers wrap expose's agent
					// code; transfer replies go straight to the ctl inbox.
					code = "agent_rejected:" + code
					current = brokerErrorMessage("expose", code, text)
				default:
					current = transferRefusalErr(code, "transfer refused: code=%s %s", code, text)
				}
				if got := v060LegacyRefusalExitClass(tc.shape, code, text); got != 75 {
					t.Errorf("%s: v0.6.0 exit=%d, want 75: code=%q text=%q", fset.Position(lit.Pos()), got, code, text)
				}
				if got := classifyExit(current); got != 75 {
					t.Errorf("%s: current exit=%d, want 75: %v", fset.Position(lit.Pos()), got, current)
				}
				for _, marker := range []string{"try_again", "agent_upgrading", "image switch", "retry"} {
					if !strings.Contains(current.Error(), marker) {
						t.Errorf("current client lost %q: %v", marker, current)
					}
				}
				if strings.Contains(current.Error(), "storage hiccup") {
					t.Errorf("agent upgrade was diagnosed as broker storage trouble: %v", current)
				}
				return true
			})
			if count != tc.count {
				t.Fatalf("found %d upgrade refusals, want %d; a refusal lost its machine-visible reason or the surface changed", count, tc.count)
			}
		})
	}

	// Negative controls pin the old unknown-code default and keep detail-based
	// hints from turning a storage failure into an upgrade diagnosis.
	for _, shape := range []string{"RunChunk", "ExecChunk", "ExposeForwardedResp", "PushPrepareResp", "TransferCommitResp", "PullPrepareResp"} {
		if got := v060LegacyRefusalExitClass(shape, "agent_upgrading", "agent_upgrading: image switch in progress; retry"); got != 70 {
			t.Errorf("%s: frozen v0.6.0 unknown-code default=%d, want 70", shape, got)
		}
	}
	for _, detail := range []string{"storage busy", "not agent_upgrading", "agent_upgrading_other: busy", "agent_no_responders: agent_upgrading"} {
		for _, current := range []error{
			brokerErrorMessage("expose", "try_again", detail),
			runFailureMessage("try_again: " + detail),
			execFailureMessage("try_again: " + detail),
		} {
			if classifyExit(current) != 75 || !strings.Contains(current.Error(), "storage hiccup") {
				t.Errorf("unrelated detail acquired the upgrade hint: %v", current)
			}
		}
	}
	wrapped := brokerErrorMessage("expose", "agent_rejected:try_again", "agent_upgrading: image switch in progress; retry")
	if classifyExit(wrapped) != 75 || wireCodeOf(wrapped) != "try_again" || !strings.Contains(wrapped.Error(), "retry in a few seconds") || !strings.Contains(wrapped.Error(), "agent_upgrading") {
		t.Errorf("wrapped legacy refusal lost classification or upgrade hint: %v", wrapped)
	}
}

// origin: git tag v0.6.0:cmd/tether/error_hints.go, runFailureMessage,
// execFailureMessage, brokerErrorMessage, stripPrefix and brokerCodeExitClass; cmd/tether/transfer.go,
// transferRefusalErr; cmd/tether/expose.go's brokerErrorMessage call.
// The extraction branches below are copied from that tag. The frozen table
// excerpt contains the only mapped code used by this refusal contract:
// brokerCodeExitClasses["try_again"] = exitTransient (75). agent_upgrading
// has no entry; the copied lookup default is exitInternal (70). Do not route
// this through today's table: that would let a new client hide an N-1 break.
func v060LegacyRefusalExitClass(shape, code, text string) int {
	switch shape {
	case "RunChunk":
		code = text
		if i := strings.IndexByte(text, ':'); i >= 0 {
			code = strings.TrimSpace(text[:i])
		}
	case "ExecChunk":
		code = text
		if i := strings.IndexByte(text, ':'); i >= 0 {
			code = text[:i]
		}
		code = strings.TrimSpace(code)
	case "ExposeForwardedResp":
		// The wrapper has no hint entry in v0.6.0, so brokerErrorMessage
		// reaches stripPrefix before looking up its underlying exit class.
		code = strings.TrimPrefix(code, "agent_rejected:")
	}
	classes := map[string]int{"try_again": 75}
	if c, ok := classes[code]; ok {
		return c
	}
	return 70
}
