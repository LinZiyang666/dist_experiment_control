package main

import (
	"bytes"
	"context"
	"debug/elf"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/infraroot"
	"github.com/LinZiyang666/tether/internal/testharness"
)

func repository(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func supervised(t *testing.T, cmd *exec.Cmd) ([]byte, error) {
	t.Helper()
	return testharness.CombinedOutput(cmd)
}

func commandContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute) // ctx-root: bounded compiler/fixture process owner.
	t.Cleanup(cancel)
	return ctx
}

func buildHelper(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "infra-helper-under-test")
	cmd := exec.CommandContext(commandContext(t), "go", "build", "-o", path, "./cmd/tether-infra-helper")
	cmd.Dir = repository(t)
	// Keep GOFLAGS, including the production mutation overlay, in the entire
	// build graph. Static release verification intentionally uses CGO_ENABLED=0.
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := supervised(t, cmd); err != nil {
		t.Fatalf("build helper: %v\n%s", err, output)
	}
	return path
}

// origin: hot-update S9 plan §6.1 / T92 / M36
func TestInfraRootExecutableHasNoBusinessBootstrap(t *testing.T) {
	cmd := exec.CommandContext(commandContext(t), "go", "list", "-deps", "-f", "{{.ImportPath}}", "./cmd/tether-infra-helper")
	cmd.Dir = repository(t)
	output, err := supervised(t, cmd)
	if err != nil {
		t.Fatalf("actual helper dependency closure: %v\n%s", err, output)
	}
	seen := make(map[string]bool)
	for _, dependency := range strings.Fields(string(output)) {
		seen[dependency] = true
		for _, forbidden := range []string{"/internal/agent", "/internal/broker", "/internal/infraupgrade", "/internal/proto", "/internal/infracontract/contractstub", "github.com/nats-io/nats", "github.com/hashicorp/raft", "/internal/tunnel"} {
			if strings.HasPrefix(dependency, "github.com/LinZiyang666/tether"+forbidden) || strings.HasPrefix(dependency, forbidden) {
				t.Fatalf("business bootstrap entered the actual privileged dependency closure: %s", dependency)
			}
		}
	}
	for _, required := range []string{"infraroot", "infrapriv", "infrahost", "infralaunch", "ingressswitch"} {
		if !seen["github.com/LinZiyang666/tether/internal/"+required] {
			t.Fatal("real executable omitted a required root leaf", required)
		}
	}
	// The source check complements go list: calling another entry's bootstrap,
	// interpreting a business handoff variable, or replacing assembly with a
	// success stub is forbidden even when the import is hidden behind a wrapper.
	fset := token.NewFileSet()
	source, err := parser.ParseFile(fset, filepath.Join(repository(t), "cmd/tether-infra-helper/main.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var linked, activated, bootstrap bool
	ast.Inspect(source, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
			if ident, ok := selector.X.(*ast.Ident); ok && ident.Name == "infraroot" && selector.Sel.Name == "OpenInstalled" {
				linked = true
			}
			if selector.Sel.Name == "Bootstrap" {
				bootstrap = true
			}
			if selector.Sel.Name == "ServeConn" {
				activated = true
			}
		}
		return true
	})
	// OpenInstalled is passed as a function value into the same command runner,
	// rather than called before command/platform/UID admission has completed.
	ast.Inspect(source, func(n ast.Node) bool {
		selector, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if ident, ok := selector.X.(*ast.Ident); ok && ident.Name == "infraroot" && selector.Sel.Name == "OpenInstalled" {
			linked = true
		}
		return true
	})
	if !linked || !activated || !bootstrap {
		t.Fatal("real command does not reach root assembly, bootstrap, and the authenticated server")
	}
	var entrySanitizes, clearsEnvironment, closesDescriptors bool
	for _, declaration := range source.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(function.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if ident, ok := call.Fun.(*ast.Ident); ok && function.Name.Name == "main" && ident.Name == "sanitizeInherited" {
				entrySanitizes = true
			}
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok && function.Name.Name == "sanitizeInherited" {
				if ident, ok := selector.X.(*ast.Ident); ok && ident.Name == "os" && selector.Sel.Name == "Clearenv" {
					clearsEnvironment = true
				}
				if selector.Sel.Name == "Close" {
					closesDescriptors = true
				}
			}
			return true
		})
	}
	if !entrySanitizes || !clearsEnvironment || !closesDescriptors {
		t.Fatal("real root entry omitted its inherited environment/FD boundary")
	}
	data, err := os.ReadFile(filepath.Join(repository(t), "cmd/tether-infra-helper/main.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"agentboot", "_TETHER_HANDOFF", "_TETHER_BROKER", "contractstub", "exec.Command(\"sh\""} {
		if bytes.Contains(data, []byte(forbidden)) {
			t.Fatal("root dispatch contains a business or shell entry", forbidden)
		}
	}
	binary := buildHelper(t)
	if runtime.GOOS == "linux" {
		file, err := elf.Open(binary)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = file.Close() }()
		for _, segment := range file.Progs {
			if segment.Type == elf.PT_INTERP {
				t.Fatal("CGO_ENABLED=0 helper has a dynamic interpreter")
			}
		}
		libraries, err := file.ImportedLibraries()
		if err != nil || len(libraries) != 0 {
			t.Fatal("helper links shared libraries", libraries, err)
		}
	}
	cmd = exec.CommandContext(commandContext(t), binary, "version")
	output, err = supervised(t, cmd)
	if err != nil || string(output) != "tether-infra-helper protocol 1\n" {
		t.Fatalf("real helper version: %q %v", output, err)
	}
}

// origin: hot-update S9 plan §6.1 / T92 / M36
func TestInfraRootCommandRefusesUntrustedInputs(t *testing.T) {
	for _, args := range [][]string{nil, {"worker", "/tmp/other.sock"}, {"bootstrap", "--force"}, {"/bin/sh"}, {"worker;id"}, {"--component=nats"}} {
		var output, diagnostic bytes.Buffer
		called := false
		rc := runWith(args, &output, &diagnostic, func() (*infraroot.Root, error) { called = true; return nil, c.ErrContractStub })
		if rc != 64 || called || diagnostic.String() != "infra_config_invalid\n" || output.Len() != 0 {
			t.Fatalf("untrusted arguments admitted: %q rc=%d", args, rc)
		}
	}
	if os.Geteuid() != 0 || runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		for _, name := range []string{"worker", "bootstrap"} {
			var output, diagnostic bytes.Buffer
			called := false
			rc := runWith([]string{name}, &output, &diagnostic, func() (*infraroot.Root, error) { called = true; return nil, c.ErrContractStub })
			want := 77
			if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
				want = 64
			}
			if rc != want || called || output.Len() != 0 {
				t.Fatal("platform/UID refusal happened after root initialization", name, rc)
			}
		}
	}
	for _, test := range []struct {
		err  error
		code string
		rc   int
	}{
		{c.ErrUnsupported, "infra_unsupported", 64}, {infrapriv.ErrPermission, "infra_permission_denied", 77},
		{c.ErrIncomplete, "infra_inventory_incomplete", 75}, {infrapriv.ErrNotEnabled, "infra_not_enabled", 75},
		{infrapriv.ErrBusy, "infra_busy", 75}, {infrapriv.ErrPolicyStale, "infra_policy_stale", 77},
		{errors.New("private-key-canary"), "infra_internal", 70},
	} {
		if code, rc := publicError(test.err); code != test.code || rc != test.rc {
			t.Fatal("private diagnostic crossed the closed error boundary", code, rc)
		}
	}
}

// origin: hot-update S9 plan §12 P40 lifecycle
func TestInfraRootLifecycle(t *testing.T) {
	binary := buildHelper(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRoot(t, binary)
	}
	testharness.AssertNoGoroutineLeak(t, "root executable supervised workers", beforeG)
	testharness.AssertNoFDLeak(t, "root executable supervised workers", beforeFD, 0)
}

func exerciseRoot(t *testing.T, binary string) {
	t.Helper()
	for _, test := range []struct {
		args []string
		want string
		rc   int
	}{
		{[]string{"version"}, "tether-infra-helper protocol 1\n", 0},
		{[]string{"worker", "--config=/tmp/untrusted"}, "infra_config_invalid\n", 64},
	} {
		cmd := exec.CommandContext(commandContext(t), binary, test.args...)
		// A hostile inherited environment must not select a business bootstrap.
		// Only version/argument refusal executes; no privileged host action runs.
		cmd.Env = []string{"PATH=/usr/bin:/bin", "_TETHER_HANDOFF=untrusted", "_TETHER_BROKER_IMAGE=untrusted"}
		output, err := supervised(t, cmd)
		rc := 0
		if err != nil {
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Fatal(err)
			}
			rc = exit.ExitCode()
		}
		if rc != test.rc || string(output) != test.want {
			t.Fatalf("real root command: rc=%d output=%q err=%v", rc, output, err)
		}
	}
}
