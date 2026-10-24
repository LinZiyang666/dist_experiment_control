package contractstub

import (
	"context"
	"database/sql"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusterio"
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/execclient"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/httplisten"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/proxydial"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/test/clusterharness"
	"github.com/nats-io/nats.go"
)

type compileLock = cluster.DataDirLock

// Compile probes compare both owner aliases and the exact function shapes.
// The private tunnel runtime hook has no public callable entry; its alias
// pins the signature here and its owner tests exercise the concrete function.
// origin: hot-update S6 plan §2.1 / §12.1 P00
var (
	_ brokerimage.FreezeFunc                                                                                                                = brokerimage.Freeze
	_ func(brokerimage.Config) (brokerimage.Cut, error)                                                                                     = brokerimage.Freeze
	_ brokerimage.ImportFunc                                                                                                                = brokerimage.Import
	_ func(brokerimage.Config, brokerimage.Bundle, tunnel.FileSource) (brokerimage.Imported, tunnel.Salvage, error)                         = brokerimage.Import
	_ brokerimage.OpenSingleStoreFunc                                                                                                       = brokerimage.OpenSingleStore
	_ func(brokerimage.SingleStoreConfig) (brokerimage.StoreOwner, *sql.DB, error)                                                          = brokerimage.OpenSingleStore
	_ brokerimage.PreflightFunc                                                                                                             = brokerimage.Preflight
	_ func(brokerimage.PreflightRequest) (brokerimage.ReaderProof, brokerimage.ReaderProof, error)                                          = brokerimage.Preflight
	_ storage.OpenCurrentFunc                                                                                                               = storage.OpenCurrent
	_ func(storage.OpenImageConfig) (*sql.DB, error)                                                                                        = storage.OpenCurrent
	_ listenerimage.AdoptFunc                                                                                                               = listenerimage.Adopt
	_ func(listenerimage.Descriptor, *os.File) (listenerimage.Keeper, error)                                                                = listenerimage.Adopt
	_ listenerimage.CaptureFunc                                                                                                             = listenerimage.Capture
	_ func(net.Listener, string) (listenerimage.Keeper, error)                                                                              = listenerimage.Capture
	_ tunnel.FreezeServerRuntimeFunc                                                                                                        = tunnel.FreezeServerRuntimeFunc(nil)
	_ func(*tunnel.Server, controlbudget.Deadline) (tunnel.ServerRuntimeCut, error)                                                         = tunnel.FreezeServerRuntimeFunc(nil)
	_ tunnel.FreezeServerFunc                                                                                                               = (*tunnel.Server).FreezeServer
	_ func(*tunnel.Server, controlbudget.Deadline) (tunnel.ServerFrozen, error)                                                             = (*tunnel.Server).FreezeServer
	_ tunnel.ImportServerFunc                                                                                                               = tunnel.ImportServer
	_ func(tunnel.ServerImportConfig, tunnel.ServerImage, tunnel.FileSource) (tunnel.ServerImported, tunnel.Salvage, error)                 = tunnel.ImportServer
	_ cli.DoWithinFunc                                                                                                                      = cli.DoWithin
	_ func(cli.RetryConfig, func(time.Duration) error) error                                                                                = cli.DoWithin
	_ cli.ConnectWithinFunc                                                                                                                 = cli.ConnectWithin
	_ func(string, *cli.Identity, controlbudget.Deadline, controlbudget.Observer, ...nats.Option) (*nats.Conn, error)                       = cli.ConnectWithin
	_ cli.InboxConnectWithinFunc                                                                                                            = natsinbox.ConnectWithin
	_ func(string, string, []nats.Option, controlbudget.Deadline, controlbudget.Observer) (*nats.Conn, bool, error)                         = natsinbox.ConnectWithin
	_ cli.OptionsWithinFunc                                                                                                                 = proxydial.OptionsWithin
	_ func(proxydial.Env, controlbudget.Deadline, controlbudget.Observer) ([]nats.Option, error)                                            = proxydial.OptionsWithin
	_ brokerimage.NewForImageFunc                                                                                                           = cluster.NewForImage
	_ func(cluster.Config, storage.StoreStamp) (*cluster.Node, error)                                                                       = cluster.NewForImage
	_ brokerimage.NewProductionForImageFunc                                                                                                 = cluster.NewProductionForImage
	_ func(cluster.ProductionConfig, storage.StoreStamp) (*cluster.Node, error)                                                             = cluster.NewProductionForImage
	_ brokerimage.PrepareImageShutdownFunc                                                                                                  = cluster.PrepareImageShutdown
	_ func(*cluster.Node, controlbudget.Deadline) error                                                                                     = cluster.PrepareImageShutdown
	_ brokerimage.ClosedImageStoreFunc                                                                                                      = cluster.ClosedImageStore
	_ func(*cluster.Node) (storage.StoreSeal, error)                                                                                        = cluster.ClosedImageStore
	_ brokerimage.ReadImageRoleFunc                                                                                                         = cluster.ReadImageRole
	_ func(*cluster.Node) (storage.RoleReceipt, error)                                                                                      = cluster.ReadImageRole
	_ brokerimage.AcquireImageDataDirLockFunc[compileLock]                                                                                  = cluster.AcquireImageDataDirLock
	_ func(string) (*compileLock, error)                                                                                                    = cluster.AcquireImageDataDirLock
	_ brokerimage.AdoptDataDirLockFunc[compileLock]                                                                                         = cluster.AdoptDataDirLock
	_ func(string, *os.File) (*compileLock, error)                                                                                          = cluster.AdoptDataDirLock
	_ brokerimage.DataDirLockExportFunc[compileLock]                                                                                        = (*cluster.DataDirLock).Export
	_ func(*compileLock) (handoff.Resource, error)                                                                                          = (*cluster.DataDirLock).Export
	_ brokerimage.DataDirLockDetachFunc[compileLock]                                                                                        = (*cluster.DataDirLock).Detach
	_ func(*compileLock) (*os.File, error)                                                                                                  = (*cluster.DataDirLock).Detach
	_ brokerimage.DataDirLockReleaseFunc[compileLock]                                                                                       = (*cluster.DataDirLock).Release
	_ func(*compileLock) error                                                                                                              = (*cluster.DataDirLock).Release
	_ brokerimage.TakeRoleFunc                                                                                                              = agentboot.TakeRole
	_ func(string) (*handoff.Restored, error)                                                                                               = agentboot.TakeRole
	_ execclient.ProbeWithinFunc                                                                                                            = execclient.ProbeWithin
	_ func(context.Context, *nats.Conn, string, proto.ProcessResumeReq, execclient.AdmissionConfig) (proto.ProcessResumeReply, error)       = execclient.ProbeWithin
	_ execclient.CreateWithinFunc                                                                                                           = execclient.CreateWithin
	_ func(context.Context, *nats.Conn, string, proto.ProcessResumeReq, execclient.AdmissionConfig) (proto.ProcessResumeReply, error)       = execclient.CreateWithin
	_ execclient.RunWithinFunc                                                                                                              = execclient.RunWithin
	_ func(context.Context, *nats.Conn, string, proto.ExecReq, io.Writer, io.Writer, execclient.AdmissionConfig) (execclient.Result, error) = execclient.RunWithin
	_ clusterharness.StartBrokerImageFixtureFunc                                                                                            = clusterharness.StartBrokerImageFixture
	_ func(*testing.T, clusterharness.BrokerImageFixtureConfig) (*clusterharness.BrokerImageFixture, error)                                 = clusterharness.StartBrokerImageFixture
	_ clusterharness.BrokerImageCheckpointFunc                                                                                              = (*clusterharness.BrokerImageFixture).Checkpoint
	_ func(*clusterharness.BrokerImageFixture, string) (clusterharness.BrokerImageReceipt, error)                                           = (*clusterharness.BrokerImageFixture).Checkpoint
	_ clusterharness.BrokerImageArmFaultFunc                                                                                                = (*clusterharness.BrokerImageFixture).ArmFault
	_ func(*clusterharness.BrokerImageFixture, clusterharness.BrokerImageFault) error                                                       = (*clusterharness.BrokerImageFixture).ArmFault
	_ clusterharness.BrokerImageCloseFunc                                                                                                   = (*clusterharness.BrokerImageFixture).Close
	_ func(*clusterharness.BrokerImageFixture) error                                                                                        = (*clusterharness.BrokerImageFixture).Close
	_ httplisten.StartImageFunc                                                                                                             = httplisten.StartImage
	_ func(listenerimage.Keeper, http.Handler, string, controlbudget.Deadline) (httplisten.ImageOwner, error)                               = httplisten.StartImage
	_ adminsock.StartInheritedFunc                                                                                                          = (*adminsock.Server).StartInherited
	_ func(*adminsock.Server, listenerimage.Keeper, controlbudget.Deadline) error                                                           = (*adminsock.Server).StartInherited
	_ adminsock.FenceImageFunc                                                                                                              = (*adminsock.Server).FenceImage
	_ func(*adminsock.Server, controlbudget.Deadline) error                                                                                 = (*adminsock.Server).FenceImage
	_ adminsock.JoinImageFunc                                                                                                               = (*adminsock.Server).JoinImage
	_ func(*adminsock.Server, controlbudget.Deadline) error                                                                                 = (*adminsock.Server).JoinImage
	_ adminsock.DetachImageFunc                                                                                                             = (*adminsock.Server).DetachImage
	_ func(*adminsock.Server) (listenerimage.Keeper, error)                                                                                 = (*adminsock.Server).DetachImage
	_ port.PlanReassignHomeGuardedFunc                                                                                                      = port.PlanReassignHomeGuarded
	_ func(*sql.DB, port.MutationGuard, string, time.Time) (int64, *cluster.Command, error)                                                 = port.PlanReassignHomeGuarded
	_ port.PlanAllocateGuardedFunc                                                                                                          = port.PlanAllocateGuarded
	_ func(*sql.DB, port.MutationGuard, port.AllocationIntent) (*port.Allocation, *cluster.Command, error)                                  = port.PlanAllocateGuarded
	_ port.PlanRotateAllocationGuardedFunc                                                                                                  = port.PlanRotateAllocationGuarded
	_ func(*sql.DB, port.MutationGuard, port.AllocationIntent, time.Time) (*port.Allocation, *cluster.Command, error)                       = port.PlanRotateAllocationGuarded
	_ port.PlanFreeAllocationGuardedFunc                                                                                                    = port.PlanFreeAllocationGuarded
	_ func(*sql.DB, port.MutationGuard, time.Time) (*cluster.Command, error)                                                                = port.PlanFreeAllocationGuarded
)

// Retired compile adapters must stay absent; every frozen owner must have a real body.
// Compile probes above now bind those owners directly, never resource-free substitutes.
// origin: hot-update S6 plan §2.1 / §12.1 P00
func TestBrokerContractStubOwners(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerContractStubOwners(t)
	}
	testharness.AssertNoGoroutineLeak(t, "retired contract adapters remain absent", beforeG)
	testharness.AssertNoFDLeak(t, "retired contract adapters remain absent", beforeFD, 0)
}

func exerciseBrokerContractStubOwners(t *testing.T) {
	t.Helper()
	cases := []struct {
		name                string
		owner, path, symbol string
	}{
		{"Freeze", "P01", "internal/brokerimage/cut.go", "Freeze"},
		{"Import", "P02", "internal/brokerimage/import.go", "Import"},
		{"OpenSingleStore", "P11", "internal/brokerimage/single_store.go", "OpenSingleStore"},
		{"Preflight", "P02", "internal/brokerimage/preflight.go", "Preflight"},
		{"OpenCurrent", "P03", "internal/storage/current.go", "OpenCurrent"},
		{"Adopt", "P08", "internal/listenerimage/listener_linux.go", "Adopt"},
		{"Capture", "P08", "internal/listenerimage/keeper.go", "Capture"},
		{"FreezeServerRuntime", "P06", "internal/tunnel/server_pause.go", "freezeServerRuntime"},
		{"FreezeServer", "P07", "internal/tunnel/server_image.go", "Server.FreezeServer"},
		{"ImportServer", "P07", "internal/tunnel/server_import.go", "ImportServer"},
		{"DoWithin", "P18", "internal/cli/retry.go", "DoWithin"},
		{"ConnectWithin", "P18", "internal/cli/retry.go", "ConnectWithin"},
		{"InboxConnectWithin", "P20", "internal/natsinbox/natsinbox.go", "ConnectWithin"},
		{"OptionsWithin", "P19", "internal/proxydial/proxydial.go", "OptionsWithin"},
		{"NewForImage", "P04", "internal/cluster/node.go", "NewForImage"},
		{"NewProductionForImage", "P04", "internal/cluster/production.go", "NewProductionForImage"},
		{"PrepareImageShutdown", "P04", "internal/cluster/node.go", "PrepareImageShutdown"},
		{"ClosedImageStore", "P04", "internal/cluster/node.go", "ClosedImageStore"},
		{"ReadImageRole", "P04", "internal/cluster/node.go", "ReadImageRole"},
		{"AcquireImageDataDirLock", "P04", "internal/cluster/datadirlock.go", "AcquireImageDataDirLock"},
		{"AdoptDataDirLock", "P04", "internal/cluster/datadirlock.go", "AdoptDataDirLock"},
		{"DataDirLockExport", "P04", "internal/cluster/storelock/lock.go", "DataDirLock.Export"},
		{"DataDirLockDetach", "P04", "internal/cluster/storelock/lock.go", "DataDirLock.Detach"},
		{"DataDirLockRelease", "P04", "internal/cluster/storelock/lock.go", "DataDirLock.Release"},
		{"TakeRole", "P22", "internal/agentboot/image.go", "TakeRole"},
		{"ProbeWithin", "P21", "internal/execclient/admission.go", "ProbeWithin"},
		{"CreateWithin", "P21", "internal/execclient/admission.go", "CreateWithin"},
		{"RunWithin", "P21", "internal/execclient/admission.go", "RunWithin"},
		{"StartBrokerImageFixture", "P26", "test/clusterharness/broker_image.go", "StartBrokerImageFixture"},
		{"BrokerImageCheckpoint", "P26", "test/clusterharness/broker_image.go", "BrokerImageFixture.Checkpoint"},
		{"BrokerImageArmFault", "P26", "test/clusterharness/broker_image.go", "BrokerImageFixture.ArmFault"},
		{"BrokerImageClose", "P26", "test/clusterharness/broker_image.go", "BrokerImageFixture.Close"},
		{"StartImage", "P09", "internal/httplisten/image.go", "StartImage"},
		{"StartInherited", "P10", "internal/adminsock/image.go", "Server.StartInherited"},
		{"FenceImage", "P10", "internal/adminsock/image.go", "Server.FenceImage"},
		{"JoinImage", "P10", "internal/adminsock/image.go", "Server.JoinImage"},
		{"DetachImage", "P10", "internal/adminsock/image.go", "Server.DetachImage"},
		{"PlanReassignHomeGuarded", "P14", "internal/port/plan.go", "PlanReassignHomeGuarded"},
		{"PlanAllocateGuarded", "P14", "internal/port/plan.go", "PlanAllocateGuarded"},
		{"PlanRotateAllocationGuarded", "P14", "internal/port/plan.go", "PlanRotateAllocationGuarded"},
		{"PlanFreeAllocationGuarded", "P14", "internal/port/plan.go", "PlanFreeAllocationGuarded"},
	}
	root := contractRoot(t)
	seen := make(map[string]bool)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if seen[tc.name] {
				t.Fatalf("duplicate stub %s", tc.name)
			}
			seen[tc.name] = true
			if path := findContractImplementation(t, filepath.Join(root, filepath.Dir(tc.path)), tc.symbol); path == "" {
				t.Fatalf("%s has no concrete %s implementation (%s)", tc.name, tc.owner, tc.symbol)
			}
		})
	}
	// All packages have landed. Reintroducing any callable adapter would let a
	// compile-only substitute survive integration again.
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, "internal/brokerimage/contractstub/stub.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok {
			t.Errorf("retired stub %s remains callable", fn.Name.Name)
		}
	}
}

func contractRoot(t *testing.T) string {
	t.Helper()
	_, here, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate contract sources")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(here), "../../.."))
}

func findContractImplementation(t *testing.T, dir, symbol string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			name := fn.Name.Name
			if fn.Recv != nil {
				recv := fn.Recv.List[0].Type
				if ptr, ok := recv.(*ast.StarExpr); ok {
					recv = ptr.X
				}
				id, ok := recv.(*ast.Ident)
				if !ok {
					continue
				}
				name = id.Name + "." + name
			}
			if name == symbol {
				return path
			}
		}
	}
	return ""
}

// Source scans include platform-tagged files: dormant production imports are unsafe.
// origin: hot-update S6 plan §2.1 / §12.1 P00
func TestBrokerProductionCannotImportContractStub(t *testing.T) {
	if hits := productionStubImports(t, contractRoot(t)); len(hits) != 0 {
		t.Fatalf("production imports contractstub: %v", hits)
	}
	// On-disk controls prove that both a normal and a tagged import are visible.
	root := t.TempDir()
	for _, tag := range []string{"", "//go:build contract_stub_control\n\n"} {
		path := filepath.Join(root, "owner.go")
		data := tag + "package owner\nimport _ \"github.com/LinZiyang666/tether/internal/brokerimage/contractstub\"\n"
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if hits := productionStubImports(t, root); len(hits) != 1 {
			t.Fatalf("import control missed: %v", hits)
		}
	}
	if err := os.Rename(filepath.Join(root, "owner.go"), filepath.Join(root, "owner_test.go")); err != nil {
		t.Fatal(err)
	}
	if hits := productionStubImports(t, root); len(hits) != 0 {
		t.Fatalf("test import falsely blocked: %v", hits)
	}
}

func productionStubImports(t *testing.T, root string) []string {
	t.Helper()
	var hits []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "contractstub" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range file.Imports {
			value, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			if value == "github.com/LinZiyang666/tether/internal/brokerimage/contractstub" {
				hits = append(hits, path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return hits
}

// Image and signed-body fields retain explicit zero values and declaration order.
// origin: hot-update S6 plan §2.1 / §9.1 / §12.1 P00
// S7 appends exactly one trailing field to each of Config and Bundle: the
// transfer owner and its optional public reference (`transfers,omitempty`,
// nil keeps the S6 bytes). Every S6 field keeps its position, type and tag.
// xfercontract.ImageRef is declared in the NATS-free xfervalue package, so
// reflection names its host package.
// origin: hot-update S7 plan §8.2 / P24 additive image domain
func TestBrokerContractFieldTypesAndTags(t *testing.T) {
	snapshot, ok := reflect.TypeFor[tunnel.ServerRuntimeCut]().MethodByName("snapshot")
	if !ok {
		t.Fatal("private runtime snapshot method missing")
	}
	cases := []struct {
		typ    reflect.Type
		fields string
	}{
		{reflect.TypeFor[brokerimage.Config](), "Mode|brokerimage.Mode|;Store|brokerimage.StoreOwner|;Control|brokerimage.ControlOwner|;Server|*tunnel.Server|;Listeners|[]listenerimage.Keeper|;FreezeServer|func(*tunnel.Server, controlbudget.Deadline) (tunnel.ServerFrozen, error)|;ImportServer|func(tunnel.ServerImportConfig, tunnel.ServerImage, tunnel.FileSource) (tunnel.ServerImported, tunnel.Salvage, error)|;Observe|controlbudget.Observer|;Deadline|controlbudget.Deadline|;NewStore|func(storage.StoreStamp, *os.File, controlbudget.Deadline) (brokerimage.StoreOwner, error)|;NewControl|func(brokerimage.StoreOwner, controlbudget.Deadline) (brokerimage.ControlOwner, error)|;TunnelConfig|tunnel.ServerImportConfig|;Transfers|brokerimage.TransferOwner|"},
		{reflect.TypeFor[brokerimage.SingleStoreConfig](), "DBPath|string|;DataDir|string|;Registry|storage.DriverRegistry|"},
		{reflect.TypeFor[brokerimage.PreflightRequest](), "CandidatePath|string|;PreviousPath|string|;Config|[]uint8|;AllowedArgs|[]string|;Stamp|storage.StoreStamp|;RequiredDomains|[]string|;Deadline|controlbudget.Deadline|"},
		{reflect.TypeFor[storage.OpenImageConfig](), "Stamp|storage.StoreStamp|;Registry|storage.DriverRegistry|"},
		{reflect.TypeFor[cli.RetryConfig](), "Deadline|controlbudget.Deadline|;Family|cli.RetryFamily|;LogicalID|string|;HasAuthReceipt|bool|;Observer|controlbudget.Observer|"},
		{reflect.TypeFor[execclient.AdmissionConfig](), "Deadline|controlbudget.Deadline|;LogicalID|string|;LegacyConfirmed|bool|;Observer|controlbudget.Observer|"},
		// Preserve all seven S6 fields and pin S8's additional incarnation fence.
		// origin: hot-update S8 plan §6.2 / P35; S7 internal review GATE-CONVERGE-layering F5
		{reflect.TypeFor[port.MutationGuard](), "Expected|*port.Allocation|;ExpectAbsent|bool|;FromHome|string|;ToHome|string|;ExpectedMaintenance|map[string]string|;Proofs|map[string]brokermaint.LocalLease|;NowBootNS|int64|;Authority|upgradeauthority.Guard|"},
		{reflect.TypeFor[port.AllocationIntent](), "SID|string|;NID|string|;Name|string|;LocalPort|int|;DesiredPort|int|;CreatedByFP|string|;HomeBroker|string|;RebuildOff|bool|;Config|*port.Config|"},
		{reflect.TypeFor[clusterharness.BrokerImageFixtureConfig](), "Topology|string|;CandidatePath|string|;PreviousPath|string|;FaultArtifactPath|string|"},
		{reflect.TypeFor[clusterharness.BrokerImageFixture](), "api|clusterharness.BrokerImageFixtureAPI|"},
		{reflect.TypeFor[brokerimage.PrivateFieldDescriptor](), "Name|string|;Type|string|;Encoding|string|;MaxBytes|int|;Secret|bool|;CompatibleReader|string|"},
		{reflect.TypeFor[brokerimage.PrivateRecordDescriptor](), "Kind|string|;Version|uint32|;StateRecord|string|;Owner|string|;CompatibleReader|string|;Fields|[]brokerimage.PrivateFieldDescriptor|"},
		{reflect.TypeFor[tunnel.ServerImportConfig](), "PublicHost|string|;Lookup|tunnel.TokenLookup|;Certificate|*tls.Certificate|;Flow|tunnel.FlowConfig|;Logger|*slog.Logger|"},
		{reflect.TypeFor[tunnel.Salvage](), "Resources|[]handoff.Resource|"},
		{snapshot.Type.Out(0), "Records|[]sealedstate.Record|;Resources|[]handoff.Resource|"},

		{reflect.TypeFor[controlbudget.Attempt](), "LogicalID|string|logical_id;Ordinal|uint32|ordinal;Phase|controlbudget.AttemptPhase|phase;StartedMonoNS|int64|started_mono_ns;FinishedMonoNS|int64|finished_mono_ns;Sent|bool|sent;Class|string|class;BackoffNS|int64|backoff_ns"},
		{reflect.TypeFor[storage.DBIdentity](), "Path|string|path;DataDir|string|data_dir;LockPath|string|lock_path;Device|uint64|device;Inode|uint64|inode"},
		{reflect.TypeFor[storage.SchemaStamp](), "Applied|[]storage.MigrationDigest|applied;Supported|[]storage.MigrationDigest|supported;Digest|string|digest;ObjectDigest|string|object_digest"},
		{reflect.TypeFor[storage.MigrationDigest](), "Name|string|name;SHA256|string|sha256"},
		{reflect.TypeFor[storage.StoreStamp](), "Identity|storage.DBIdentity|identity;Schema|storage.SchemaStamp|schema;Journal|string|journal;Synchronous|int|synchronous;AppliedIndex|uint64|applied_index;AppliedTerm|uint64|applied_term;RaftLastIndex|uint64|raft_last_index;RaftConfigIndex|uint64|raft_config_index;RaftConfigDigest|string|raft_config_digest"},
		{reflect.TypeFor[storage.StoreSeal](), "Stamp|storage.StoreStamp|stamp;Writer|storage.ClosePhase|writer;ReadOnly|storage.ClosePhase|read_only;Bolt|storage.ClosePhase|bolt;Transport|storage.ClosePhase|transport;Raft|storage.ClosePhase|raft;Role|storage.RoleReceipt|role;DriverWriters|uint32|driver_writers;DriverReaders|uint32|driver_readers;ActiveRPCs|uint32|active_rpcs;ActiveBackups|uint32|active_backups;CheckpointBusy|int|checkpoint_busy;CheckpointLog|int|checkpoint_log;CheckpointDone|int|checkpoint_done"},
		{reflect.TypeFor[storage.RoleReceipt](), "EdgeSeq|uint64|edge_seq;Term|uint64|term;ConfigIndex|uint64|config_index;VoterDigest|string|voter_digest;LastRunningIsLeader|bool|last_running_is_leader;ObserverClosed|bool|observer_closed"},
		{reflect.TypeFor[listenerimage.Descriptor](), "ID|string|id;Network|string|network;Address|string|address;Path|string|path;Device|uint64|device;Inode|uint64|inode;Cookie|uint64|cookie;UnixPathInode|uint64|unix_path_inode;Family|int|family;SockType|int|sock_type;Nonblock|bool|nonblock;CloseOnExec|bool|close_on_exec"},
		{reflect.TypeFor[brokermaint.Record](), "CutID|string|cut_id;Holder|string|holder;Home|string|home;InstanceID|string|instance_id;IssuerBootID|string|issuer_boot_id;Mode|string|mode;IngressServerID|string|ingress_server_id;Generation|uint64|generation;Phase|brokermaint.Phase|phase;IssuedBootNS|int64|issued_boot_ns;ExpiresBootNS|int64|expires_boot_ns;Term|uint64|term;ConfigIndex|uint64|config_index;VoterDigest|string|voter_digest;BaselineDigest|string|baseline_digest;RequiredCapability|string|required_capability"},
		{reflect.TypeFor[brokermaint.Proof](), "CutID|string|cut_id;Nonce|string|nonce;IssuerBootID|string|issuer_boot_id;ResponseBootNS|int64|response_boot_ns;ExpiresBootNS|int64|expires_boot_ns;RecordDigest|string|record_digest;AppliedIndex|uint64|applied_index;Sig|string|sig"},
		{reflect.TypeFor[brokermaint.LocalLease](), "Record|brokermaint.Record|record;ExpiresLocalBootNS|int64|expires_local_boot_ns;Proof|brokermaint.Proof|proof"},
		{reflect.TypeFor[tunnel.ServerImage](), "Schema|string|schema;SchemaVersion|uint32|schema_version;Generation|uint64|generation;IssuerBootID|string|issuer_boot_id;InstanceID|string|instance_id;OwnerGeneration|uint64|owner_generation;ReplayLimit|int64|replay_limit;ReplayUsed|int64|replay_used;Listeners|[]listenerimage.Descriptor|listeners;StateResource|string|state_resource;Endpoints|[]tunnel.EndpointImage|endpoints;Flows|flow.Image|flows"},
		{reflect.TypeFor[brokerimage.Bundle](), "Schema|string|schema;SchemaVersion|uint32|schema_version;Role|string|role;Mode|brokerimage.Mode|mode;Generation|uint64|generation;ExporterSHA256|string|exporter_sha256;CandidateSHA256|string|candidate_sha256;PreviousSHA256|string|previous_sha256;InstanceID|string|instance_id;BootID|string|boot_id;Args|[]string|args;ConfigDigest|string|config_digest;RequiredDomains|[]string|required_domains;StateResource|string|state_resource;Store|storage.StoreStamp|store;Seal|storage.StoreSeal|seal;LockResource|string|lock_resource;Listeners|[]listenerimage.Descriptor|listeners;Tunnel|tunnel.ServerImage|tunnel;Maintenance|[]brokermaint.LocalLease|maintenance;Transfers|*xfervalue.ImageRef|transfers,omitempty"},
		{reflect.TypeFor[brokerimage.ReaderProof](), "SHA256|string|sha256;ABI|uint32|abi;Domains|[]string|domains;MigrationDigest|string|migration_digest"},
		{reflect.TypeFor[clusterio.SnapshotTicket](), "RPCID|uint64|rpc_id;Index|uint64|index;Term|uint64|term;ConfigIndex|uint64|config_index;ConfigDigest|string|config_digest;PayloadBytes|int64|payload_bytes"},
		{reflect.TypeFor[clusterharness.BrokerImageReceipt](), "Label|string|label;JSON|[]uint8|json;Digest|string|digest"},
		{reflect.TypeFor[clusterharness.BrokerImageFault](), "Point|string|point;Ordinal|uint32|ordinal;Once|bool|once"},
		{reflect.TypeFor[proto.BrokerImageStatus](), "RunningSHA256|string|running_sha256,omitempty;Generation|uint64|generation,omitempty;Phase|string|phase,omitempty;CutID|string|cut_id,omitempty;Mode|string|mode,omitempty"},
		{reflect.TypeFor[proto.BrokerMaintenanceReq](), "Record|brokermaint.Record|record;Proof|brokermaint.Proof|proof;NID|string|nid;ConnectedBrokerID|string|connected_broker_id;IngressServerID|string|ingress_server_id;RouteDigest|string|route_digest;Sig|string|sig"},
		{reflect.TypeFor[proto.BrokerMaintenanceAck](), "CutID|string|cut_id;Nonce|string|nonce;NID|string|nid;ConnectedBrokerID|string|connected_broker_id;IngressServerID|string|ingress_server_id;InstanceID|string|instance_id;RouteDigest|string|route_digest;ProofDigest|string|proof_digest;Accepted|bool|accepted;RemainingNS|int64|remaining_ns;Sig|string|sig"},
	}
	for _, tc := range cases {
		t.Run(tc.typ.String(), func(t *testing.T) {
			want := strings.Split(tc.fields, ";")
			if tc.typ.NumField() != len(want) {
				t.Fatalf("field count %d, want %d", tc.typ.NumField(), len(want))
			}
			for i, row := range want {
				parts := strings.Split(row, "|")
				field := tc.typ.Field(i)
				if field.Name != parts[0] || field.Type.String() != parts[1] || field.Tag.Get("json") != parts[2] {
					t.Errorf("field %d: got %s|%s|%s, want %s", i, field.Name, field.Type, field.Tag.Get("json"), row)
				}
			}
		})
	}
}

// String wire fields stay strings; constants only pin the closed allowed values.
// origin: hot-update S6 plan §2.1 / §8.3 / §9.1 / §12.1 P00
func TestBrokerContractClosedFamilies(t *testing.T) {
	if got := []string{proto.BrokerImagePhaseServing, proto.BrokerImagePhasePreparing, proto.BrokerImagePhaseFrozen, proto.BrokerImagePhaseImporting, proto.BrokerImagePhaseAuthUsable, proto.BrokerImagePhaseControlReady, proto.BrokerImagePhaseRollingBack, proto.BrokerImagePhaseFailed}; !reflect.DeepEqual(got, []string{"serving", "preparing", "frozen", "importing", "auth_usable", "control_ready", "rolling_back", "failed"}) {
		t.Fatalf("wire phases: %v", got)
	}
	if got := []string{proto.BrokerImageModeSingle, proto.BrokerImageModeSoleVoter, proto.BrokerImageModeQuorum}; !reflect.DeepEqual(got, []string{"single", "sole-voter", "quorum"}) {
		t.Fatalf("wire modes: %v", got)
	}
	if got := []string{adminsock.CodeUpgradeDeferred, brokerimage.DetailMaintenanceObserverUnsupported, brokerimage.DetailMaintenanceExpired, brokerimage.DetailStoreCloseUnknown, brokerimage.DetailControlResultUnknown}; !reflect.DeepEqual(got, []string{"upgrade_deferred", "maintenance_observer_unsupported", "maintenance_expired", "store_close_unknown", "control_result_unknown"}) {
		t.Fatalf("error vocabulary: %v", got)
	}
	if reflect.TypeFor[controlbudget.Deadline]().Kind() != reflect.Int64 || reflect.TypeFor[controlbudget.AttemptPhase]().Kind() != reflect.String || reflect.TypeFor[storage.ClosePhase]().Kind() != reflect.Uint8 || reflect.TypeFor[brokerimage.Mode]().Kind() != reflect.Uint8 || reflect.TypeFor[brokermaint.Phase]().Kind() != reflect.Uint8 || reflect.TypeFor[cli.RetryFamily]().Kind() != reflect.Uint8 || reflect.TypeFor[clusterupgrade.PlanScope]().Kind() != reflect.String {
		t.Fatal("enum underlying type changed")
	}

	if got := []storage.ClosePhase{storage.CloseOpen, storage.CloseStopped, storage.CloseClosed, storage.CloseUnknown}; !reflect.DeepEqual(got, []storage.ClosePhase{0, 1, 2, 3}) {
		t.Fatalf("close phases: %v", got)
	}
	if got := []brokerimage.Mode{brokerimage.ModeSingle, brokerimage.ModeSoleVoter, brokerimage.ModeQuorum}; !reflect.DeepEqual(got, []brokerimage.Mode{1, 2, 3}) {
		t.Fatalf("modes: %v", got)
	}
	if got := []brokermaint.Phase{brokermaint.MaintPrepared, brokermaint.MaintActive, brokermaint.MaintEnded}; !reflect.DeepEqual(got, []brokermaint.Phase{1, 2, 3}) {
		t.Fatalf("maintenance phases: %v", got)
	}
	if got := []cli.RetryFamily{cli.RetryReadOnly, cli.RetryStableProcess, cli.RetryUnsafe}; !reflect.DeepEqual(got, []cli.RetryFamily{1, 2, 3}) {
		t.Fatalf("retry families: %v", got)
	}
	if clusterupgrade.ScopeWholeHost != "whole-host" || clusterupgrade.ScopeBrokerDaemon != "broker-daemon" {
		t.Fatal("scope vocabulary changed")
	}
	if brokerimage.Domain != "broker-image-v1" || brokerimage.ImageRole != "broker" || brokerimage.BundleVersion != 1 || brokerimage.MaxBundleBytes != 16<<20 || brokermaint.Capability != "broker-maint-v1" {
		t.Fatal("image or maintenance contract changed")
	}
	got := []controlbudget.AttemptPhase{controlbudget.PhaseDNS, controlbudget.PhaseProxy, controlbudget.PhaseTLS, controlbudget.PhaseWS, controlbudget.PhaseConnect, controlbudget.PhaseInboxProbe, controlbudget.PhaseInboxFallback, controlbudget.PhaseDiscovery, controlbudget.PhaseBanner, controlbudget.PhaseProbe, controlbudget.PhaseCreate, controlbudget.PhaseRequest, controlbudget.PhaseBackoff, controlbudget.PhaseCancel, controlbudget.PhaseResult, controlbudget.PhaseFreeze, controlbudget.PhaseExec, controlbudget.PhaseImport, controlbudget.PhaseAuth, controlbudget.PhaseReady, controlbudget.PhaseMaintenance}
	want := []controlbudget.AttemptPhase{"dns", "proxy", "tls", "ws", "connect", "inbox_probe", "inbox_fallback", "discovery", "banner", "probe", "create", "request", "backoff", "cancel", "result", "freeze", "exec", "import", "auth", "ready", "maintenance"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("attempt phases: %v", got)
	}
}

// The broker kind partition must never leak into the global S5 reader vocabulary.
// origin: hot-update S6 plan §2.2 / §12.1 P00
func TestBrokerPrivateDescriptorSkeleton(t *testing.T) {
	want := []string{
		"broker-runtime|broker-state-v1|P02", "store|broker-state-v1|P03", "control|broker-state-v1|P02",
		"server|broker-tunnel-state-v1|P07", "endpoint|broker-tunnel-state-v1|P07",
		"permit|broker-tunnel-state-v1|P07", "receipt|broker-tunnel-state-v1|P07",
		"fences|broker-tunnel-state-v1|P07", "resources|broker-state-v1|P02",
	}
	if brokerimage.PrivateStateRecord != "broker-state-v1" || brokerimage.TunnelStateRecord != "broker-tunnel-state-v1" || brokerimage.PrivateDescriptorVersion != 1 {
		t.Fatal("private namespace changed")
	}
	got := make([]string, 0, len(brokerimage.PrivateRecordDescriptors))
	for _, row := range brokerimage.PrivateRecordDescriptors {
		if row.Version != 1 || row.CompatibleReader != "broker-image-v1" {
			t.Fatalf("bad private reader/version for %s", row.Kind)
		}
		got = append(got, row.Kind+"|"+row.StateRecord+"|"+row.Owner)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("private skeleton %v, want %v", got, want)
	}
	for _, row := range sealedstate.RecordDescriptors {
		if row.Kind == "broker-runtime" || row.Kind == "store" || row.Kind == "control" || row.Kind == "server" || row.Kind == "fences" || row.Kind == "resources" || row.Kind == "broker-state-v1" || row.Kind == "broker-tunnel-state-v1" {
			t.Fatalf("broker-only kind %s polluted global reader", row.Kind)
		}
	}
}

// origin: hot-update S6 plan §2.1 / §12.1 P00
func TestBrokerContractAdapterMethodSets(t *testing.T) {
	cases := []struct {
		typ     reflect.Type
		methods map[string]string
	}{
		{reflect.TypeFor[controlbudget.Observer](), map[string]string{"Begin": "func(string, controlbudget.AttemptPhase) (controlbudget.Attempt, error)", "Record": "func(controlbudget.Attempt) error", "Close": "func() error"}},
		{reflect.TypeFor[storage.DriverRegistry](), map[string]string{"Connections": "func(storage.DBIdentity) (uint32, uint32)", "AssertExclusive": "func(storage.DBIdentity) error"}},
		{reflect.TypeFor[listenerimage.Keeper](), map[string]string{"Descriptor": "func() listenerimage.Descriptor", "View": "func() net.Listener", "Pause": "func(controlbudget.Deadline) error", "Export": "func() (listenerimage.Descriptor, handoff.Resource, error)", "Resume": "func() error", "Detach": "func() (*os.File, error)", "Close": "func() error"}},
		{reflect.TypeFor[tunnel.FileSource](), map[string]string{"TakeFile": "func(string) (*os.File, error)"}},
		{reflect.TypeFor[tunnel.ServerFrozen](), map[string]string{"Export": "func() (tunnel.ServerImage, []handoff.Resource, error)", "Thaw": "func(controlbudget.Deadline) error", "Discard": "func() error"}},
		{reflect.TypeFor[tunnel.ServerImported](), map[string]string{"Server": "func() *tunnel.Server", "Activate": "func(controlbudget.Deadline) error", "DetachPristine": "func() (tunnel.Salvage, error)"}},
		{reflect.TypeFor[tunnel.ServerRuntimeCut](), map[string]string{"snapshot": "func() (tunnel.serverStateSnapshot, error)", "flowExport": "func() (flow.Image, *sealedstate.Snapshot, error)", "thaw": "func(controlbudget.Deadline) error", "discard": "func() error"}},
		{reflect.TypeFor[brokerimage.StoreOwner](), map[string]string{"Quiesce": "func(controlbudget.Deadline) error", "CloseForImage": "func(controlbudget.Deadline) (storage.StoreSeal, error)", "ReopenCurrent": "func(storage.StoreStamp, controlbudget.Deadline) (brokerimage.StoreOwner, error)", "Stamp": "func() (storage.StoreStamp, error)", "LockResource": "func() (handoff.Resource, error)"}},
		{reflect.TypeFor[brokerimage.ControlOwner](), map[string]string{"Fence": "func(controlbudget.Deadline) error", "JoinStoreUsers": "func(controlbudget.Deadline) error", "StopTransport": "func(controlbudget.Deadline) error", "StartAuth": "func(controlbudget.Deadline) error", "StartBusiness": "func(controlbudget.Deadline) error", "StopCurrent": "func(controlbudget.Deadline) error", "FenceTransfers": "func(controlbudget.Deadline) (uint32, error)", "ResumeTransfers": "func() error"}},
		{reflect.TypeFor[brokerimage.Cut](), map[string]string{"Export": "func(storage.StoreSeal) (brokerimage.Bundle, []handoff.Resource, error)", "Prepare": "func(brokerimage.Bundle, []handoff.Resource) (*handoff.Prepared, error)", "Thaw": "func(controlbudget.Deadline) error", "Discard": "func() error"}},
		{reflect.TypeFor[brokerimage.Imported](), map[string]string{"Activate": "func(brokerimage.RuntimeSlot, controlbudget.Deadline) error", "DetachPristine": "func() (tunnel.Salvage, error)"}},
		{reflect.TypeFor[brokerimage.RuntimeSlot](), map[string]string{"Install": "func(brokerimage.StoreOwner, brokerimage.ControlOwner, tunnel.ServerImported) error", "StopCurrent": "func(controlbudget.Deadline) error"}},
		{reflect.TypeFor[clusterio.Tracker](), map[string]string{"Track": "func(net.Conn) (net.Conn, error)", "Fence": "func(controlbudget.Deadline) error", "Join": "func(controlbudget.Deadline) error", "Counts": "func() (uint32, uint32)"}},
		{reflect.TypeFor[httplisten.ImageOwner](), map[string]string{"Fence": "func(controlbudget.Deadline) error", "Join": "func(controlbudget.Deadline) error", "Resume": "func() error", "Stop": "func(controlbudget.Deadline) error"}},
		{reflect.TypeFor[clusterharness.BrokerImageFixtureAPI](), map[string]string{"Checkpoint": "func(string) (clusterharness.BrokerImageReceipt, error)", "ArmFault": "func(clusterharness.BrokerImageFault) error", "Close": "func() error"}},
	}
	for _, tc := range cases {
		t.Run(tc.typ.String(), func(t *testing.T) {
			if tc.typ.NumMethod() != len(tc.methods) {
				t.Fatalf("method count %d, want %d", tc.typ.NumMethod(), len(tc.methods))
			}
			for name, shape := range tc.methods {
				method, ok := tc.typ.MethodByName(name)
				if !ok || method.Type.String() != shape {
					t.Errorf("method %s got %v, want %s", name, method.Type, shape)
				}
			}
		})
	}
}
