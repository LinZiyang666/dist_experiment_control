package infracontract_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infracontract/contractstub"
)

var (
	_ func() c.Dependencies               = contractstub.NewDependencies
	_ c.DriverFactory                     = contractstub.NewDriver
	_ c.ServiceFactory                    = contractstub.NewService
	_ func(c.HostRequest) ([]byte, error) = c.EncodeRequest
	_ func([]byte) (c.HostRequest, error) = c.DecodeRequest
	_ func(c.HostRequest) ([]byte, error) = c.SigningBytes
	_ func(c.HostRequest, []byte) error   = c.VerifyRequest
	_ c.Deadline                          = controlbudget.Deadline(1)
	_ c.Stop                              = (<-chan struct{})(nil)
)

// Freeze both producer and consumer sides. A production import anywhere in the
// source tree creates a possible transitive stub path, even behind a build tag.
// origin: hot-update S9 plan §2.1 / §2.2 / §2.3 / T01 / M02
func TestInfraContractFactories(t *testing.T) {
	t.Run("signatures", checkInfraSignatures)
	t.Run("records", checkInfraRecords)
	t.Run("closed_families", checkInfraFamilies)
	t.Run("production_imports", func(t *testing.T) {
		_, source, _, ok := runtime.Caller(0)
		if !ok {
			t.Fatal("cannot find contract source")
		}
		root := filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
			t.Fatal(err)
		}
		count, violations, err := scanInfraImports(root)
		if err != nil {
			t.Fatal(err)
		}
		if count < 100 {
			t.Fatalf("production scan was vacuous: %d files", count)
		}
		if len(violations) != 0 {
			t.Fatalf("production contract imports: %v", violations)
		}
		checkEnumDeclarations(t, root)
		checkImportControls(t)
	})
}

// Enumerate declarations, not only constants already mentioned by this test.
// Otherwise an added, unregistered enum member would silently evade the golden.
func checkEnumDeclarations(t *testing.T, root string) {
	t.Helper()
	want := map[string][]string{
		"Component":      {"NATS", "Caddy", "HostSupport"},
		"Phase":          {"Prepared", "Staged", "Ready", "Moving", "Cutting", "Verifying", "Returning", "Retiring", "Completed", "Deferred", "Halted"},
		"Action":         {"Upgrade", "Rollback", "Reload", "Recover"},
		"ArtifactKind":   {"NATSBinary", "CaddyBinary", "HelperBinary", "LauncherBinary"},
		"RoutePurpose":   {"RoutePrepare", "RouteBind"},
		"RouteDirection": {"RouteOutbound", "RouteReturn"},
		"HostOp":         {"Inspect", "Stage", "FenceStart", "Promote", "SignalLDM", "SignalStop", "SignalGraceful", "StartWarm", "SwitchIngress", "FinalizeStorage", "ReleaseStart", "RetireWarm", "RecoverHost", "RotatePolicy", "UpdateSupport", "LaunchClaim", "LaunchAck", "VerifyIngress"},
	}
	got := map[string][]string{}
	files, err := filepath.Glob(filepath.Join(root, "internal/infracontract/*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			group, ok := declaration.(*ast.GenDecl)
			if !ok || group.Tok != token.CONST {
				continue
			}
			previousType := ""
			for _, spec := range group.Specs {
				value := spec.(*ast.ValueSpec)
				typ := previousType
				if value.Type != nil {
					name, ok := value.Type.(*ast.Ident)
					if !ok {
						typ = ""
					} else {
						typ = name.Name
					}
				} else if len(value.Values) != 0 {
					typ = ""
				}
				previousType = typ
				if _, known := want[typ]; !known {
					continue
				}
				for _, name := range value.Names {
					got[typ] = append(got[typ], name.Name)
				}
			}
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("enum declaration membership changed: %v", got)
	}
}

func checkInfraSignatures(t *testing.T) {
	t.Helper()
	for _, row := range interfaceGoldens {
		typ := row.typ
		methods := strings.Split(row.shape, ";")
		if typ.NumMethod() != len(methods) {
			t.Fatalf("%s method count changed", typ)
		}
		for _, entry := range methods {
			name, want, _ := strings.Cut(entry, "|")
			method, ok := typ.MethodByName(name)
			if !ok || method.Type.String() != want {
				t.Errorf("%s.%s = %v, want %s", typ, name, method.Type, want)
			}
		}
	}
	for _, row := range []struct {
		typ  reflect.Type
		want string
	}{
		{reflect.TypeFor[c.HostFactory](), "func(infracontract.HostBindings) (infracontract.Host, error)"},
		{reflect.TypeFor[c.DriverFactory](), "func(infracontract.Dependencies) (infracontract.Driver, error)"},
		{reflect.TypeFor[c.ServiceFactory](), "func(infracontract.Dependencies, infracontract.Driver, infracontract.Driver) (infracontract.Service, error)"},
	} {
		unnamed := reflect.FuncOf(functionTypes(row.typ, true), functionTypes(row.typ, false), false)
		if unnamed.String() != row.want {
			t.Errorf("%s = %s, want %s", row.typ, unnamed, row.want)
		}
	}
	if reflect.TypeFor[c.HostBindings]().Len() != 19 || reflect.TypeFor[c.HostBindings]().Elem() != reflect.TypeFor[c.Host]() {
		t.Fatal("host binding slots changed")
	}
	if reflect.TypeFor[c.Deadline]() != reflect.TypeFor[controlbudget.Deadline]() || reflect.TypeFor[c.Stop]().ChanDir() != reflect.RecvDir || reflect.TypeFor[c.Stop]().Elem() != reflect.TypeFor[struct{}]() {
		t.Fatal("deadline alias or stop channel changed")
	}
}

func functionTypes(typ reflect.Type, inputs bool) []reflect.Type {
	var types []reflect.Type
	if inputs {
		for i := 0; i < typ.NumIn(); i++ {
			types = append(types, typ.In(i))
		}
	} else {
		for i := 0; i < typ.NumOut(); i++ {
			types = append(types, typ.Out(i))
		}
	}
	return types
}

func checkInfraRecords(t *testing.T) {
	t.Helper()
	descriptors := c.CodecDescriptors()
	if len(descriptors) != len(recordGoldens)-1 {
		t.Fatal("codec descriptor membership changed")
	}
	for i, row := range recordGoldens {
		expected := strings.Split(row.shape, ";")
		if row.typ.NumField() != len(expected) {
			t.Fatalf("%s field count changed", row.typ)
		}
		for j, field := range expected {
			name, typ, _ := strings.Cut(field, "|")
			actual := row.typ.Field(j)
			if actual.Name != name || actual.Type.String() != typ || actual.Tag != "" || actual.Anonymous || !actual.IsExported() {
				t.Errorf("%s field %d = %v; want %s", row.typ, j, actual, field)
			}
		}
		if row.typ == reflect.TypeFor[c.Dependencies]() {
			continue
		}
		descriptor := descriptors[i]
		if descriptor.Name != row.typ.Name() || descriptor.Version != 1 || descriptor.MaxBytes != 65536 || len(descriptor.Fields) != len(expected) {
			t.Fatalf("bad descriptor for %s", row.typ)
		}
		for j, field := range descriptor.Fields {
			if field.Name+"|"+field.Type != expected[j] {
				t.Errorf("descriptor drift for %s field %d", row.typ, j)
			}
		}
	}
	// Descriptor storage is not a mutable global authorization surface.
	descriptors[0].Fields[0].Name = "corrupted"
	if c.CodecDescriptors()[0].Fields[0].Name == "corrupted" {
		t.Fatal("descriptor aliases global state")
	}
	if reflect.TypeFor[c.ID]().Kind() != reflect.Array || reflect.TypeFor[c.ID]().Len() != 16 || reflect.TypeFor[c.ID]().Elem().Kind() != reflect.Uint8 || reflect.TypeFor[c.Digest]().Len() != 32 || reflect.TypeFor[c.Digest]().Elem().Kind() != reflect.Uint8 {
		t.Fatal("identity/digest width changed")
	}
}

func checkInfraFamilies(t *testing.T) {
	t.Helper()
	families := []struct {
		typ    reflect.Type
		values []uint8
		max    int
	}{
		{reflect.TypeFor[c.Component](), []uint8{uint8(c.NATS), uint8(c.Caddy), uint8(c.HostSupport)}, 3},
		{reflect.TypeFor[c.Phase](), []uint8{uint8(c.Prepared), uint8(c.Staged), uint8(c.Ready), uint8(c.Moving), uint8(c.Cutting), uint8(c.Verifying), uint8(c.Returning), uint8(c.Retiring), uint8(c.Completed), uint8(c.Deferred), uint8(c.Halted)}, 11},
		{reflect.TypeFor[c.Action](), []uint8{uint8(c.Upgrade), uint8(c.Rollback), uint8(c.Reload), uint8(c.Recover)}, 4},
		{reflect.TypeFor[c.ArtifactKind](), []uint8{uint8(c.NATSBinary), uint8(c.CaddyBinary), uint8(c.HelperBinary), uint8(c.LauncherBinary)}, 4},
		{reflect.TypeFor[c.RoutePurpose](), []uint8{uint8(c.RoutePrepare), uint8(c.RouteBind)}, 2},
		{reflect.TypeFor[c.RouteDirection](), []uint8{uint8(c.RouteOutbound), uint8(c.RouteReturn)}, 2},
		{reflect.TypeFor[c.HostOp](), []uint8{uint8(c.Inspect), uint8(c.Stage), uint8(c.FenceStart), uint8(c.Promote), uint8(c.SignalLDM), uint8(c.SignalStop), uint8(c.SignalGraceful), uint8(c.StartWarm), uint8(c.SwitchIngress), uint8(c.FinalizeStorage), uint8(c.ReleaseStart), uint8(c.RetireWarm), uint8(c.RecoverHost), uint8(c.RotatePolicy), uint8(c.UpdateSupport), uint8(c.LaunchClaim), uint8(c.LaunchAck), uint8(c.VerifyIngress)}, 18},
	}
	for _, family := range families {
		if family.typ.Kind() != reflect.Uint8 || len(family.values) != family.max {
			t.Fatalf("%s enum shape changed", family.typ)
		}
		for i, value := range family.values {
			if value != uint8(i+1) {
				t.Errorf("%s member %d = %d", family.typ, i, value)
			}
		}
	}
	scalars := c.ScalarDescriptors()
	wantScalars := []c.ScalarDescriptor{
		{Name: "ID", Kind: "array", Length: 16}, {Name: "Digest", Kind: "array", Length: 32},
		{Name: "Component", Kind: "uint8", Values: []uint64{1, 2, 3}},
		{Name: "Phase", Kind: "uint8", Values: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}},
		{Name: "Action", Kind: "uint8", Values: []uint64{1, 2, 3, 4}},
		{Name: "ArtifactKind", Kind: "uint8", Values: []uint64{1, 2, 3, 4}},
		{Name: "RoutePurpose", Kind: "uint8", Values: []uint64{1, 2}},
		{Name: "RouteDirection", Kind: "uint8", Values: []uint64{1, 2}},
		{Name: "HostOp", Kind: "uint8", Values: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}},
	}
	if !reflect.DeepEqual(scalars, wantScalars) {
		t.Fatal("scalar descriptors changed")
	}
	scalars[2].Values[0] = 255
	if c.ScalarDescriptors()[2].Values[0] != 1 {
		t.Fatal("scalar descriptor aliases global state")
	}
	if c.ErrContractStub.Error() != "infra_contract_stub" || c.ErrUnsupported.Error() != "infra_unsupported" || c.ErrIncomplete.Error() != "infra_inventory_incomplete" {
		t.Fatal("sentinel vocabulary changed")
	}
	if c.RequestVersion != 1 || c.MaxRequestBytes != 65536 || c.MaxCensusPages != 64 || c.MaxCensusPageBytes != 65536 || c.MaxCensusBytes != 4194304 || c.RequestSigningDomain != "tether-infra-host-v1\n" {
		t.Fatal("private framing/size contract changed")
	}
	want := strings.Split("artifact_verified,asset_sealed,successor_installed,attachment_changed,auth_usable,business_ready,signal_applied,old_exit,storage_finalized,startup_released,return_verified,owner_retired,completed,halted", ",")
	if !slices.Equal(c.ObservationKinds(), want) {
		t.Fatal("observation vocabulary changed")
	}
}

func scanInfraImports(root string) (int, []string, error) {
	count := 0
	var violations []string
	for _, tree := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, tree), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			count++
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			for _, spec := range file.Imports {
				imported, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					return err
				}
				if imported == "github.com/LinZiyang666/tether/internal/infracontract/contractstub" {
					violations = append(violations, relative+": production stub import")
				}
				if filepath.Dir(relative) == filepath.Join("internal", "infracontract") && imported != "github.com/LinZiyang666/tether/internal/controlbudget" && strings.Contains(strings.Split(imported, "/")[0], ".") {
					violations = append(violations, relative+": non-neutral import "+imported)
				}
			}
			return nil
		})
		if err != nil {
			return count, nil, err
		}
	}
	return count, violations, nil
}

func checkImportControls(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	for _, directory := range []string{"cmd/main", "internal/bridge", "internal/infracontract"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0700); err != nil {
			t.Fatal(err)
		}
	}
	writes := map[string]string{
		"cmd/main/main.go":               "package main\nimport _ \"github.com/LinZiyang666/tether/internal/bridge\"\n",
		"internal/bridge/bridge.go":      "package bridge\n",
		"internal/infracontract/leaf.go": "package infracontract\nimport _ \"fmt\"\n",
		"internal/bridge/bridge_test.go": "package bridge\nimport _ \"github.com/LinZiyang666/tether/internal/infracontract/contractstub\"\n",
	}
	for path, data := range writes {
		if err := os.WriteFile(filepath.Join(root, path), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	count, violations, err := scanInfraImports(root)
	if err != nil || count != 3 || len(violations) != 0 {
		t.Fatalf("positive scan: %d %v %v", count, violations, err)
	}
	for _, importPath := range []string{"github.com/LinZiyang666/tether/internal/infracontract/contractstub", "github.com/nats-io/nats.go"} {
		path := "internal/bridge/bridge.go"
		if strings.Contains(importPath, "nats.go") {
			path = "internal/infracontract/leaf.go"
		}
		// Build tags do not make a forbidden production dependency acceptable.
		data := fmt.Sprintf("//go:build integration_only\n\npackage fixture\nimport _ %q\n", importPath)
		if err := os.WriteFile(filepath.Join(root, path), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		_, violations, err := scanInfraImports(root)
		if err != nil || len(violations) == 0 {
			t.Fatalf("scanner accepted %s: %v", importPath, err)
		}
		if err := os.WriteFile(filepath.Join(root, path), []byte(writes[path]), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

type shapeGolden struct {
	typ   reflect.Type
	shape string
}

// Literal snapshots of the frozen plan, independent of production descriptors.
var recordGoldens = []shapeGolden{
	{reflect.TypeFor[c.Process](), "BootID|string;PID|int32;StartTicks|uint64;InvocationID|string;ExeDevice|uint64;ExeInode|uint64;ExeSHA256|infracontract.Digest;Cgroup|string"},
	{reflect.TypeFor[c.Socket](), "NetNS|uint64;Device|uint64;Inode|uint64;Cookie|uint64;Local|string;Remote|string;Owner|infracontract.Process"},
	{reflect.TypeFor[c.Attachment](), "Participant|infracontract.ID;Generation|uint64;ServerID|string;CID|uint64;URL|string;Raw|infracontract.Socket;Ingress|infracontract.Process;HealthGeneration|uint64"},
	{reflect.TypeFor[c.Store](), "Device|uint64;Inode|uint64;MountID|uint64;LockDevice|uint64;LockInode|uint64"},
	{reflect.TypeFor[c.Artifact](), "Kind|infracontract.ArtifactKind;Version|string;GOOS|string;GOARCH|string;ArchiveSHA256|infracontract.Digest;BinarySHA256|infracontract.Digest;CatalogSHA256|infracontract.Digest"},
	{reflect.TypeFor[c.Plan](), "Tx|infracontract.ID;Step|uint32;Domain|infracontract.ID;Component|infracontract.Component;Action|infracontract.Action;Candidate|infracontract.Artifact;Previous|infracontract.Artifact;Before|infracontract.Process;ConfigSHA256|infracontract.Digest;Store|infracontract.Store;PolicyRevision|uint64"},
	{reflect.TypeFor[c.Census](), "Complete|bool;Generation|uint64;Processes|[]infracontract.Process;Sockets|[]infracontract.Socket;LiveBusiness|uint32;OldOwnerPending|uint32;Digest|infracontract.Digest"},
	{reflect.TypeFor[c.Status](), "Tx|infracontract.ID;Step|uint32;Phase|infracontract.Phase;Revision|uint64;Code|string;Detail|string;Before|infracontract.Process;Current|infracontract.Process;Census|infracontract.Census;Proof|infracontract.Digest"},
	{reflect.TypeFor[c.AssetSeal](), "Revision|uint64;Accounts|infracontract.Digest;Assets|infracontract.Digest;Participants|infracontract.Digest;Complete|bool"},
	{reflect.TypeFor[c.Readiness](), "Topology|infracontract.Digest;Seal|infracontract.AssetSeal;Auth|infracontract.Digest;WSS|infracontract.Digest;Running|infracontract.Process;MetaLeader|string;Groups|infracontract.Digest;Complete|bool"},
	{reflect.TypeFor[c.RouteTarget](), "URL|string;NodeID|string;Component|infracontract.Component;Before|infracontract.Process;Candidate|infracontract.Artifact;Ingress|infracontract.Process"},
	{reflect.TypeFor[c.MoveResult](), "Census|infracontract.Census;Attachments|[]infracontract.Attachment"},
	{reflect.TypeFor[c.RouteProof](), "Purpose|infracontract.RoutePurpose;Direction|infracontract.RouteDirection;Target|infracontract.RouteTarget;Version|uint16;Tx|infracontract.ID;Participant|infracontract.ID;Old|infracontract.Attachment;New|infracontract.Attachment;HomeDigest|infracontract.Digest;AuthorityRevision|uint64;PolicyRevision|uint64;RemainingNS|uint64;Nonce|infracontract.ID;KeyID|string;Signature|[]uint8"},
	{reflect.TypeFor[c.TransferSeal](), "Version|uint16;SourceSHA256|infracontract.Digest;SchemaSHA256|infracontract.Digest;InvocationSet|infracontract.Digest;ReceiptSet|infracontract.Digest;ConsumerSet|infracontract.Digest;Complete|bool"},
	{reflect.TypeFor[c.Checkpoint](), "Cohort|infracontract.ID;Assets|infracontract.AssetSeal;Transfers|infracontract.TransferSeal;Proof|infracontract.Digest"},
	{reflect.TypeFor[c.Observation](), "Version|uint16;Tx|infracontract.ID;Step|uint32;Kind|string;BootID|string;BootNS|int64;Subject|infracontract.ID;Generation|uint64;Evidence|infracontract.Digest"},
	{reflect.TypeFor[c.HostRequest](), "Version|uint16;Op|infracontract.HostOp;Component|infracontract.Component;Tx|infracontract.ID;Step|uint32;Domain|infracontract.ID;Sequence|uint64;PolicyRevision|uint64;KeyID|string;HostBootID|string;Nonce|infracontract.ID;Expected|infracontract.Process;Candidate|infracontract.Artifact;Store|infracontract.Store;ExpectedJournal|infracontract.Digest;Config|infracontract.Digest;BeforeCensus|infracontract.Digest;WarmCensus|infracontract.Digest;ReturnProof|infracontract.Digest;Payload|infracontract.Digest;RemainingNS|uint64;Signature|[]uint8"},
	{reflect.TypeFor[c.HostReceipt](), "Tx|infracontract.ID;Sequence|uint64;Revision|uint64;Op|infracontract.HostOp;Applied|bool;RequestDigest|infracontract.Digest;Journal|infracontract.Digest;Current|infracontract.Process;Census|infracontract.Census;Code|string"},
	{reflect.TypeFor[c.Peer](), "PID|int32;UID|uint32;GID|uint32;BootID|string;StartTicks|uint64"},
	{reflect.TypeFor[c.Dependencies](), "Host|infracontract.Host;Artifacts|infracontract.Artifacts;Ready|infracontract.ReadyProbe;Cohort|infracontract.Cohort;Routes|infracontract.Routes;Transfers|infracontract.Transfers;Recovery|infracontract.Recovery;Tx|infracontract.Transaction;Observe|infracontract.Observer"},
}

var interfaceGoldens = []shapeGolden{
	{reflect.TypeFor[c.Host](), "Do|func(infracontract.HostRequest, controlbudget.Deadline, infracontract.Stop) (infracontract.HostReceipt, error);Inspect|func(infracontract.ID, controlbudget.Deadline) (infracontract.Census, error)"},
	{reflect.TypeFor[c.Artifacts](), "Resolve|func(infracontract.ArtifactKind, string, string, string) (infracontract.Artifact, error);Verify|func(infracontract.Artifact, controlbudget.Deadline, infracontract.Stop) error;Compatible|func(infracontract.Artifact, infracontract.Artifact, infracontract.Digest, infracontract.Store, controlbudget.Deadline) error"},
	{reflect.TypeFor[c.ReadyProbe](), "Read|func(infracontract.Plan, controlbudget.Deadline, infracontract.Stop) (infracontract.Readiness, error)"},
	{reflect.TypeFor[c.Cohort](), "Seal|func(infracontract.Plan, controlbudget.Deadline) (infracontract.Checkpoint, error);Install|func(infracontract.Checkpoint, infracontract.RouteProof, controlbudget.Deadline) error;Move|func(infracontract.Checkpoint, controlbudget.Deadline, infracontract.Stop) (infracontract.MoveResult, error);Return|func(infracontract.Checkpoint, controlbudget.Deadline, infracontract.Stop) (infracontract.MoveResult, error);Bind|func(infracontract.Checkpoint, []infracontract.RouteProof, controlbudget.Deadline) (infracontract.Census, error);Release|func(infracontract.Checkpoint, controlbudget.Deadline) error"},
	{reflect.TypeFor[c.Routes](), "Prepare|func(infracontract.Plan, infracontract.Checkpoint, infracontract.RouteDirection, controlbudget.Deadline) ([]infracontract.RouteProof, error);Bind|func(infracontract.Plan, infracontract.MoveResult, infracontract.RouteDirection, controlbudget.Deadline) ([]infracontract.RouteProof, error)"},
	{reflect.TypeFor[c.Transfers](), "Fence|func(infracontract.ID, controlbudget.Deadline) (infracontract.TransferSeal, error);Rebind|func(infracontract.TransferSeal, controlbudget.Deadline) error;Release|func(infracontract.TransferSeal, controlbudget.Deadline) error"},
	{reflect.TypeFor[c.Recovery](), "Seal|func(infracontract.Plan, controlbudget.Deadline) (infracontract.Checkpoint, error);VerifyBeforeReplay|func(infracontract.Checkpoint, controlbudget.Deadline, infracontract.Stop) error;Release|func(infracontract.Checkpoint, controlbudget.Deadline) error"},
	{reflect.TypeFor[c.Transaction](), "Acquire|func(infracontract.Plan, controlbudget.Deadline) (infracontract.Status, error);CompareAndSwap|func(infracontract.Status, infracontract.Phase, infracontract.Digest, controlbudget.Deadline) (infracontract.Status, error);Load|func(infracontract.ID, controlbudget.Deadline) (infracontract.Status, error)"},
	{reflect.TypeFor[c.Observer](), "Record|func(infracontract.Observation) error"},
	{reflect.TypeFor[c.Driver](), "Advance|func(infracontract.Plan, controlbudget.Deadline, infracontract.Stop) (infracontract.Status, error);Recover|func(infracontract.Plan, controlbudget.Deadline, infracontract.Stop) (infracontract.Status, error)"},
	{reflect.TypeFor[c.Service](), "Start|func(infracontract.Plan, controlbudget.Deadline, infracontract.Stop) (infracontract.Status, error);Resume|func(infracontract.ID, controlbudget.Deadline, infracontract.Stop) (infracontract.Status, error);Status|func(infracontract.ID, controlbudget.Deadline) (infracontract.Status, error)"},
}
