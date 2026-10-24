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

	"gopkg.in/yaml.v3"
)

type infraRelease struct {
	Builds []struct {
		ID, Main, Binary  string
		Env, Goos, Goarch []string
	}
	Archives []struct {
		ID     string
		Builds []string
	}
}

// The release archive must really contain both independent entry points.
// Comments, extra-files and an installer string do not create a compiled binary.
// A default archive includes all builds; an explicit list must include both.
// origin: hot-update S9 plan §6.5 / §10.2 / T83
func checkInfraReleaseArtifacts(source string) error {
	var release infraRelease
	if err := yaml.Unmarshal([]byte(source), &release); err != nil {
		return err
	}
	ids := map[string]string{}
	for _, name := range []string{"tether-infra-helper", "tether-infra-launch"} {
		count := 0
		for _, build := range release.Builds {
			if build.Binary != name {
				continue
			}
			count++
			if build.Main != "./cmd/"+name || build.ID == "" || !infraContains(build.Env, "CGO_ENABLED=0") || !infraContains(build.Goos, "linux") || !infraContains(build.Goarch, "amd64") {
				return fmt.Errorf("%s: release loses static linux/amd64 independent entry", name)
			}
			ids[name] = build.ID
		}
		if count != 1 {
			return fmt.Errorf("%s: missing/duplicate compiled release artifact", name)
		}
	}
	for _, archive := range release.Archives {
		if len(archive.Builds) == 0 || infraContains(archive.Builds, ids["tether-infra-helper"]) && infraContains(archive.Builds, ids["tether-infra-launch"]) {
			return nil
		}
	}
	return fmt.Errorf("support binaries have no common checksummed release archive")
}

func infraContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// This reader deliberately accepts only the installer's literal unit templates,
// never executes shell, and fails closed if the rendering grammar changes. P37
// owns the full shell/heredoc audit; this gate compares the emitted unit fields
// with the actual Go renderer and client entry contracts on every OS.
// origin: hot-update S9 plan §6.4 / §10.2 / T83 / M31
func checkInfraInstallOutput(script string, sources map[string]string) error {
	start := strings.Index(script, "\nwrite_infra_files() {\n")
	if start < 0 {
		return fmt.Errorf("missing installer unit renderer")
	}
	body, _, closed := strings.Cut(script[start+1:], "\n}\n")
	if !closed {
		return fmt.Errorf("unclosed installer unit renderer")
	}
	var templates []string
	lines := strings.Split(body, "\n")
	for i := 0; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) != `cat > "$CONFIG_DEST" <<EOF` {
			continue
		}
		var output []string
		for i++; i < len(lines) && lines[i] != "EOF"; i++ {
			output = append(output, lines[i])
		}
		if i == len(lines) {
			return fmt.Errorf("unclosed installer output")
		}
		templates = append(templates, strings.Join(output, "\n")+"\n")
	}
	if len(templates) != 3 {
		return fmt.Errorf("installer must emit socket, worker and component templates, got %d", len(templates))
	}
	values := map[string]string{"LIBEXEC_DIR": "/usr/local/libexec", "INFRA_RUN_DIR": "/run/tether-infra"}
	for name, suffix := range map[string]string{"LIBEXEC_DIR": "/usr/local/libexec", "INFRA_RUN_DIR": "/run/tether-infra"} {
		if !strings.Contains(script, "\n    "+name+`="${TETHER_INSTALL_ROOT:-}`+suffix+`"`+"\n") {
			return fmt.Errorf("installer changed redirected %s path", name)
		}
	}
	for name, want := range map[string]string{"INFRA_WORKER_LIFETIME_S": "15", "INFRA_WORKER_STOP_S": "5", "INFRA_MAX_CONNECTIONS": "4"} {
		matches := regexp.MustCompile(`(?m)^`+name+`=([^\n]+)$`).FindAllStringSubmatch(script, -1)
		if len(matches) != 1 || matches[0][1] != want {
			return fmt.Errorf("installer %s must have one bounded definition", name)
		}
		values[name] = want
	}
	all, err := transferDeclarations(sources)
	if err != nil {
		return err
	}
	decl, ok := all["internal/infraunit/effective_linux.go:ManagedUnitDropIn"]
	if !ok {
		return fmt.Errorf("missing actual Go unit renderer")
	}
	fn, ok := decl.node.(*ast.FuncDecl)
	if !ok || fn.Body == nil {
		return fmt.Errorf("empty Go unit renderer")
	}
	for _, component := range []string{"nats", "caddy"} {
		values["_wif_component"], values["name"] = component, component
		values["_wif_caps"], values["capabilities"] = "", ""
		if component == "caddy" {
			values["_wif_caps"], values["capabilities"] = "CAP_NET_BIND_SERVICE", "CAP_NET_BIND_SERVICE"
		}
		var rendered []string
		for _, template := range templates {
			if strings.ContainsAny(template, "`\\") || strings.Contains(template, "$(") {
				return fmt.Errorf("installer output contains active shell syntax")
			}
			var unknown string
			rendered = append(rendered, os.Expand(template, func(name string) string {
				value, found := values[name]
				if !found {
					unknown = name
				}
				return value
			}))
			if unknown != "" {
				return fmt.Errorf("installer output has unknown expansion %s", unknown)
			}
		}
		for i, required := range [][]string{
			{"[Socket]", "Accept=yes", "SocketUser=root", "SocketGroup=tether", "SocketMode=0660", "MaxConnections=4", "Service=tether-infra-helper@.service"},
			{"[Service]", "User=root", "Group=root", "ExecStart=/usr/local/libexec/tether-infra-helper worker", "StandardInput=socket", "StandardOutput=socket", "Restart=no", "RuntimeMaxSec=15s", "KillMode=control-group"},
			{"[Service]", "User=tether", "Group=tether", "NoNewPrivileges=yes", "ExecStart=", "ExecStart=/usr/local/libexec/tether-infra-launch --component=" + component},
		} {
			for _, line := range required {
				if !strings.Contains("\n"+rendered[i], "\n"+line+"\n") {
					return fmt.Errorf("installer output %d missing %s", i, line)
				}
				key, _, directive := strings.Cut(line, "=")
				count := 1
				if i == 2 && key == "ExecStart" {
					count = 2 // The first assignment clears the accumulated command.
				}
				if directive && strings.Count("\n"+rendered[i], "\n"+key+"=") != count {
					return fmt.Errorf("installer output %d duplicates %s", i, key)
				}
			}
		}
		// Compare the returned expression, not a comment containing the template.
		var goOutput string
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			ret, ok := node.(*ast.ReturnStmt)
			if !ok || len(ret.Results) != 2 {
				return true
			}
			call, ok := ret.Results[0].(*ast.CallExpr)
			if ok && len(call.Args) == 1 {
				if value, valid := infraRenderedString(call.Args[0], values); valid {
					goOutput = value
				}
			}
			return true
		})
		if goOutput == "" || goOutput != rendered[2] {
			return fmt.Errorf("%s installer output differs from actual Go renderer", component)
		}
		file, err := brokerParse("ipc.go", sources["internal/infrapriv/ipc.go"])
		if err != nil {
			return err
		}
		var socket string
		ast.Inspect(file, func(node ast.Node) bool {
			if spec, ok := node.(*ast.ValueSpec); ok {
				for i, name := range spec.Names {
					if name.Name == "DefaultSocketPath" && i < len(spec.Values) {
						socket, _ = infraRenderedString(spec.Values[i], nil)
					}
				}
			}
			return true
		})
		if socket == "" || strings.Count("\n"+rendered[0], "\nListenStream=") != 1 || !strings.Contains("\n"+rendered[0], "\nListenStream="+socket+"\n") {
			return fmt.Errorf("installer socket differs from actual IPC client default")
		}
	}
	return nil
}

func infraRenderedString(expr ast.Expr, values map[string]string) (string, bool) {
	switch value := expr.(type) {
	case *ast.BasicLit:
		if value.Kind == token.STRING {
			text, err := strconv.Unquote(value.Value)
			return text, err == nil
		}
	case *ast.Ident:
		text, ok := values[value.Name]
		return text, ok
	case *ast.BinaryExpr:
		left, lOK := infraRenderedString(value.X, values)
		right, rOK := infraRenderedString(value.Y, values)
		return left + right, value.Op == token.ADD && lOK && rOK
	}
	return "", false
}

// origin: hot-update S9 plan §6.4 / §10.2 / T83
// gate-control: TestInfraSupplyControls
func TestInfraInstallerOutputMatchesRuntime(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "scripts/install.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkInfraInstallOutput(string(raw), brokerGateSources(t, root)); err != nil {
		t.Fatal(err)
	}
}

// No direct exec.Cmd start may bypass the shared process supervisor in an infra
// fixture. Match the command object's construction, so HTTP server Start and
// product lifecycle methods do not become unrelated process exceptions.
// origin: hot-update S9 plan lead L22 / §9.5 / T83
func checkInfraFixtureSpawns(sources map[string]string) error {
	for _, path := range brokerSortedSourcePaths(sources) {
		file, err := brokerParse(path, sources[path])
		if err != nil {
			return err
		}
		imports := brokerImports(file)
		commands := map[string]bool{}
		// Commands may be package variables, parameters or aliases. Discover
		// ownership before checking references so a method value is visible too.
		// origin: hot-update S9 plan §9.5 / T83 / M31
		commandExpr := func(expr ast.Expr) bool {
			if id, ok := expr.(*ast.Ident); ok {
				return commands[id.Name]
			}
			call, ok := expr.(*ast.CallExpr)
			if !ok {
				return false
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return false
			}
			alias, ok := selector.X.(*ast.Ident)
			return ok && imports[alias.Name] == "os/exec" && (selector.Sel.Name == "Command" || selector.Sel.Name == "CommandContext")
		}
		for changed := true; changed; {
			changed = false
			mark := func(name string, expr ast.Expr) {
				if name != "_" && commandExpr(expr) && !commands[name] {
					commands[name] = true
					changed = true
				}
			}
			ast.Inspect(file, func(node ast.Node) bool {
				switch value := node.(type) {
				case *ast.AssignStmt:
					for i, lhs := range value.Lhs {
						if id, ok := lhs.(*ast.Ident); ok && i < len(value.Rhs) {
							mark(id.Name, value.Rhs[i])
						}
					}
				case *ast.ValueSpec:
					for i, id := range value.Names {
						if i < len(value.Values) {
							mark(id.Name, value.Values[i])
						}
					}
				case *ast.Field:
					if ptr, ok := value.Type.(*ast.StarExpr); ok {
						if sel, ok := ptr.X.(*ast.SelectorExpr); ok {
							if id, ok := sel.X.(*ast.Ident); ok && imports[id.Name] == "os/exec" && sel.Sel.Name == "Cmd" {
								for _, name := range value.Names {
									if !commands[name.Name] {
										commands[name.Name] = true
										changed = true
									}
								}
							}
						}
					}
				}
				return true
			})
		}
		var failure error
		ast.Inspect(file, func(node ast.Node) bool {
			if selector, ok := node.(*ast.SelectorExpr); ok && infraContains([]string{"Start", "Run", "Output", "CombinedOutput"}, selector.Sel.Name) && commandExpr(selector.X) {
				failure = fmt.Errorf("%s: exec.Cmd method value/alias bypasses shared supervisor", path)
			}
			if assign, ok := node.(*ast.AssignStmt); ok {
				for i, rhs := range assign.Rhs {
					call, ok := rhs.(*ast.CallExpr)
					if !ok || i >= len(assign.Lhs) {
						continue
					}
					selector, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						continue
					}
					alias, ok := selector.X.(*ast.Ident)
					name, named := assign.Lhs[i].(*ast.Ident)
					if ok && named && imports[alias.Name] == "os/exec" && (selector.Sel.Name == "Command" || selector.Sel.Name == "CommandContext") {
						commands[name.Name] = true
					}
				}
			}
			if call, ok := node.(*ast.CallExpr); ok {
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok && infraContains([]string{"Start", "Run", "Output", "CombinedOutput"}, selector.Sel.Name) {
					if id, ok := selector.X.(*ast.Ident); ok && commands[id.Name] {
						failure = fmt.Errorf("%s: infra fixture starts exec.Cmd outside shared supervisor", path)
					}
					if inner, ok := selector.X.(*ast.CallExpr); ok {
						if constructor, ok := inner.Fun.(*ast.SelectorExpr); ok {
							if alias, ok := constructor.X.(*ast.Ident); ok && imports[alias.Name] == "os/exec" {
								failure = fmt.Errorf("%s: inline exec.Cmd bypasses shared supervisor", path)
							}
						}
					}
				}
			}
			return true
		})
		if failure != nil {
			return failure
		}
	}
	return nil
}

// origin: hot-update S9 plan §6.5 / §9.5 / T83
// gate-control: TestInfraSupplyControls
func TestInfraSupplyUsesCompiledArtifactsAndSharedSupervisor(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "build/goreleaser.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkInfraReleaseArtifacts(string(raw)); err != nil {
		t.Errorf("NOT_RUN pending P26: %v", err)
	}
	raw, err = os.ReadFile(filepath.Join(root, "test/infraharness/process.go"))
	if err != nil {
		t.Fatal(err)
	}
	sources := map[string]string{"test/infraharness/process.go": string(raw)}
	if err := checkInfraRuntimeRoots(sources, []infraRootRule{{"test/infraharness/process.go:StartProcess", "internal/testharness:StartProcess", "T57"}}); err != nil {
		t.Error(err)
	}
	for _, home := range infraIntegrationHomes {
		entries, err := os.ReadDir(filepath.Join(root, home))
		if err != nil {
			t.Errorf("NOT_RUN pending integration owner: %s: %v", home, err)
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
				continue
			}
			path := home + "/" + entry.Name()
			raw, err := os.ReadFile(filepath.Join(root, path))
			if err != nil {
				t.Fatal(err)
			}
			sources[path] = string(raw)
		}
	}
	if err := checkInfraFixtureSpawns(sources); err != nil {
		t.Error(err)
	}
}

// origin: hot-update S9 plan §11 / T83 / M31
func TestInfraSupplyControls(t *testing.T) {
	infraInstallOutputControls(t)
	root := transferFixtureTree(t, map[string]string{})
	good := "builds:\n"
	for _, name := range []string{"tether-infra-helper", "tether-infra-launch"} {
		good += "  - id: " + name + "\n    main: ./cmd/" + name + "\n    binary: " + name + "\n    env: [CGO_ENABLED=0]\n    goos: [linux, darwin]\n    goarch: [amd64, arm64]\n"
	}
	good += "archives:\n  - id: tether\n"
	check := func(source string) error {
		transferWriteFixture(t, root, "build/goreleaser.yaml", source)
		raw, err := os.ReadFile(filepath.Join(root, "build/goreleaser.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		return checkInfraReleaseArtifacts(string(raw))
	}
	if err := check(good); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		strings.Replace(good, "binary: tether-infra-helper", "binary: dormant-helper", 1),
		strings.Replace(good, "./cmd/tether-infra-launch", "./cmd/tether", 1),
		strings.Replace(good, "CGO_ENABLED=0", "CGO_ENABLED=1", 1),
		strings.Replace(good, "[amd64, arm64]", "[arm64]", 1),
		good + "    builds: [tether]\n",
		"# tether-infra-helper tether-infra-launch\nbuilds: []\narchives: []\n",
	} {
		if check(bad) == nil {
			t.Fatal("on-disk support release mutation escaped")
		}
	}
	path := "test/infranats/fixture_test.go"
	for _, body := range []string{`cmd:=e.Command("fixture");cmd.Start()`, `e.Command("fixture").Run()`, `cmd:=e.CommandContext(ctx,"fixture");cmd.CombinedOutput()`, `cmd:=e.Command("fixture");alias:=cmd;start:=alias.Start;start()`, `var cmd=e.Command("fixture");cmd.Run()`} {
		transferWriteFixture(t, root, path, "package fixture;import e \"os/exec\";func Run(){"+body+"}")
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if checkInfraFixtureSpawns(map[string]string{path: string(raw)}) == nil {
			t.Fatal("on-disk fixture supervision bypass escaped")
		}
	}
	goodFixture := `package fixture;import e "os/exec";import s "github.com/LinZiyang666/tether/test/infraharness";func Run(){cmd:=e.Command("fixture");s.StartProcess(cmd)}`
	if err := checkInfraFixtureSpawns(map[string]string{path: goodFixture}); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S9 plan §6.4 / §11 / T83 / M31
func infraInstallOutputControls(t *testing.T) {
	root := transferFixtureTree(t, map[string]string{})
	socket := "[Socket]\nListenStream=$INFRA_RUN_DIR/host.sock\nAccept=yes\nSocketUser=root\nSocketGroup=tether\nSocketMode=0660\nMaxConnections=$INFRA_MAX_CONNECTIONS\nService=tether-infra-helper@.service\n"
	worker := "[Service]\nUser=root\nGroup=root\nExecStart=$LIBEXEC_DIR/tether-infra-helper worker\nStandardInput=socket\nStandardOutput=socket\nRestart=no\nRuntimeMaxSec=${INFRA_WORKER_LIFETIME_S}s\nKillMode=control-group\nTimeoutStopSec=${INFRA_WORKER_STOP_S}s\n"
	unit := "[Service]\nUser=tether\nGroup=tether\nNoNewPrivileges=yes\nExecStart=\nExecStart=$LIBEXEC_DIR/tether-infra-launch --component=$_wif_component\n"
	script := "#!/bin/sh\nINFRA_WORKER_LIFETIME_S=15\nINFRA_WORKER_STOP_S=5\nINFRA_MAX_CONNECTIONS=4\n    LIBEXEC_DIR=\"${TETHER_INSTALL_ROOT:-}/usr/local/libexec\"\n    INFRA_RUN_DIR=\"${TETHER_INSTALL_ROOT:-}/run/tether-infra\"\nwrite_infra_files() {\n"
	for _, template := range []string{socket, worker, unit} {
		script += "    cat > \"$CONFIG_DEST\" <<EOF\n" + template + "EOF\n"
	}
	script += "}\n"
	goPrefix := strings.Replace(unit, "$LIBEXEC_DIR", "/usr/local/libexec", 1)
	goPrefix = strings.TrimSuffix(goPrefix, "$_wif_component\n")
	goRenderer := "package infraunit;func ManagedUnitDropIn(){return []byte(" + strconv.Quote(goPrefix) + "+name+\"\\n\"),nil}"
	ipc := `package infrapriv;const DefaultSocketPath="/run/tether-infra/host.sock"`
	check := func(shell, renderer, client string) error {
		transferWriteFixture(t, root, "scripts/install.sh", shell)
		transferWriteFixture(t, root, "internal/infraunit/effective_linux.go", renderer)
		transferWriteFixture(t, root, "internal/infrapriv/ipc.go", client)
		raw, err := os.ReadFile(filepath.Join(root, "scripts/install.sh"))
		if err != nil {
			t.Fatal(err)
		}
		return checkInfraInstallOutput(string(raw), brokerGateSources(t, root))
	}
	if err := check(script, goRenderer, ipc); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ old, new string }{
		{"cat >", "# cat >"}, {"Accept=yes", "Accept=no"}, {"User=tether", "User=root"},
		{"SocketMode=0660", "SocketMode=0666"}, {"\nUser=root\n", "\nUser=root\nUser=tether\n"},
		{"--component=", "--component "}, {"INFRA_WORKER_LIFETIME_S=15", "INFRA_WORKER_LIFETIME_S=0"},
		{"KillMode=control-group", "KillMode=process"}, {"/host.sock", "/control.sock"},
		{"Accept=yes", "Accept=yes\n# $(forbidden)"}, {"$LIBEXEC_DIR/tether-infra-helper", "$UNKNOWN/tether-infra-helper"},
	} {
		if check(strings.Replace(script, tc.old, tc.new, 1), goRenderer, ipc) == nil {
			t.Fatalf("on-disk installer output mutation escaped: %s", tc.old)
		}
	}
	for _, renderer := range []string{"package infraunit", strings.Replace(goRenderer, "NoNewPrivileges=yes", "NoNewPrivileges=no", 1)} {
		if check(script, renderer, ipc) == nil {
			t.Fatal("on-disk Go output mutation escaped")
		}
	}
	if check(script, goRenderer, strings.Replace(ipc, "host.sock", "control.sock", 1)) == nil {
		t.Fatal("on-disk IPC client socket mismatch escaped")
	}
}
