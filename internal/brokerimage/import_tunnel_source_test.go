//go:build linux

// import_tunnel_source_test.go — the broker importer's FileSource must resolve
// the exact references the real tunnel.ImportServer asks for.
package brokerimage

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

// recordingSource sits exactly where restoredSource sits in production and
// records which inherited files the importer actually asked the envelope for.
type recordingSource struct {
	inner *flatFaultSource
	refs  []string
}

func (s *recordingSource) TakeFile(ref string) (*os.File, error) {
	s.refs = append(s.refs, ref)
	return s.inner.TakeFile(ref)
}

// The tunnel's sealed state travels as an ordinary inherited resource whose ID
// is ServerImage.StateResource and whose file is "state" (sealedstate.New).
// The real tunnel importer must be able to Take it through the broker importer;
// otherwise every broker that runs a tunnel server falls back to pristine
// redispatch before a single listener or raw FD can be adopted.
// origin: hot-update S6 internal review rv-fd
func TestBrokerImportResolvesTunnelAdapterTakes(t *testing.T) {
	b := fixtureBundle(t)
	tunnelRef := "broker-tunnel-state." + strings.Repeat("ab", 16)
	b.Tunnel = tunnel.ServerImage{Schema: "broker-tunnel-state-v1", SchemaVersion: 1, StateResource: tunnelRef}
	lockPath := filepath.Join(t.TempDir(), "tether.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	tunnelState, err := sealedstate.New(tunnelRef, []byte("tunnel private cut bytes"))
	if err != nil {
		t.Fatal(err)
	}
	resources := []handoff.Resource{{ID: b.LockResource, Kind: "store-lock-v1", Files: map[string]*os.File{"lock": lock}}}
	resources = append(resources, tunnelState.Resources...)
	state := fixturePrivate(b)
	for _, res := range resources {
		for name, file := range res.Files {
			identity, err := handoff.ResourceFileIdentity(file)
			if err != nil {
				t.Fatal(err)
			}
			purpose, owner := "tunnel-state", "server"
			if res.ID == b.LockResource {
				purpose, owner = "store-lock", "store"
			}
			state.Resources = append(state.Resources, ResourceOwnership{ID: res.ID, Kind: res.Kind, Name: name, Purpose: purpose, Identity: identity, Owner: owner, CutID: state.Runtime.CutID})
		}
	}
	sort.Slice(state.Resources, func(i, j int) bool {
		return state.Resources[i].ID+"/"+state.Resources[i].Name < state.Resources[j].ID+"/"+state.Resources[j].Name
	})
	private, err := EncodePrivateState(state)
	if err != nil {
		t.Fatal(err)
	}
	brokerState, err := sealedstate.New(b.StateResource, private)
	if err != nil {
		t.Fatal(err)
	}
	resources = append(resources, brokerState.Resources...)
	t.Cleanup(func() {
		for _, res := range resources {
			for _, file := range res.Files {
				_ = file.Close()
			}
		}
	})
	source := &recordingSource{inner: &flatFaultSource{source: handofftest.DupResources(t, resources)}}
	cfg := Config{Mode: b.Mode, Deadline: controlbudget.Deadline(1 << 62), ImportServer: tunnel.ImportServer,
		NewStore: func(storage.StoreStamp, *os.File, controlbudget.Deadline) (StoreOwner, error) {
			t.Fatal("pristine import constructed a store")
			return nil, nil
		},
		NewControl: func(StoreOwner, controlbudget.Deadline) (ControlOwner, error) {
			t.Fatal("pristine import constructed control")
			return nil, nil
		},
		TunnelConfig: tunnel.ServerImportConfig{PublicHost: "127.0.0.1", Flow: tunnel.FlowConfig{
			Budget: &workload.ReplayBudget{}, BootID: "boot", InstanceID: "instance", OwnerGen: 1,
			TokenLookup: func(tunnel.AuthRequest) error { return nil },
			Authorize:   func(req tunnel.AuthRequest) (tunnel.Authority, error) { return tunnel.Authority{}, nil },
		}}}
	_, salvage, _ := Import(cfg, b, source)
	defer closeSalvage(salvage)
	reached := false
	for _, ref := range source.refs {
		id, _, _ := strings.Cut(ref, "/")
		reached = reached || id == tunnelRef
	}
	if !reached {
		t.Fatalf("the real tunnel importer never reached its inherited sealed state %q; envelope saw only %q",
			tunnelRef, source.refs)
	}
}

// tunnel.ImportServer's refusal returns every taken file in rows stamped
// {Kind: "sealed-state-v1", Data: nil} (serverImageSource → appendClientSalvage),
// whatever the inherited resource really was. Pristine Repack merges the
// importer's salvage with the untaken remainder exactly like BootImage.Repack;
// that merge must still describe each inherited OFD once.
// origin: hot-update S6 internal review rv-fd
func TestBrokerImportRefusedTunnelSalvageStaysRepackable(t *testing.T) {
	f := newImportFixture(t)
	f.config.ImportServer = func(_ tunnel.ServerImportConfig, _ tunnel.ServerImage, source tunnel.FileSource) (tunnel.ServerImported, tunnel.Salvage, error) {
		file, err := source.TakeFile("raw.fixture")
		if err != nil {
			return nil, tunnel.Salvage{}, err
		}
		// Row shape copied from tunnel.ImportServer's fail(): src.taken.
		return nil, tunnel.Salvage{Resources: []handoff.Resource{{ID: "raw.fixture", Kind: "sealed-state-v1", Files: map[string]*os.File{"raw": file}}}},
			sealedstate.ErrInvalid
	}
	image, salvage, err := Import(f.config, f.bundle, &flatFaultSource{source: f.source})
	defer closeSalvage(salvage)
	if err == nil || image != nil {
		t.Fatalf("refused tunnel import accepted: %T %v", image, err)
	}
	merged, err := handoff.MergeResources(f.source.Borrow(), salvage.Resources)
	if err != nil {
		t.Fatalf("pristine Repack impossible after a refused tunnel import: %v", err)
	}
	if fileCount(merged) != fileCount(f.resources) {
		t.Fatalf("repack table has %d files, inherited %d", fileCount(merged), fileCount(f.resources))
	}
	if err = handoff.ValidateResourceOwners(merged); err != nil {
		t.Fatalf("repack table aliases one OFD: %v", err)
	}
}

// The real tunnel adapter and the real Restored source traverse two same-PID
// execs. A pristine import must detach and repack its exact listener/raw/private
// closure before the previous reader can claim it again; no constructor runs.
// origin: hot-update S6 plan §4.2 / T04 / T56; internal review rv-fd-2 / rv-fd-5
func TestBrokerTunnelPristineRepackRoundTrip(t *testing.T) {
	phase := os.Getenv("BROKER_TUNNEL_REPACK_PHASE")
	if phase != "" {
		exerciseBrokerTunnelRepackHelper(t, phase)
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerTunnelRepackProcess(t)
	}
	testharness.AssertNoGoroutineLeak(t, "tunnel pristine repack", beforeG)
	testharness.AssertNoFDLeak(t, "tunnel pristine repack", beforeFD, 0)
}

func exerciseBrokerTunnelRepackProcess(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second) // ctx-root: parent owns and waits for the exec helper.
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBrokerTunnelPristineRepackRoundTrip$", "-test.count=1")
	cmd.Env = replaceTestEnv(handoff.CleanEnvironment(os.Environ()), "BROKER_TUNNEL_REPACK_PHASE", "before")
	output, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "tunnel pristine repack receipt") {
		t.Fatalf("tunnel pristine repack failed: %v\n%s", err, output)
	}
}

func exerciseBrokerTunnelRepackHelper(t *testing.T, phase string) {
	t.Helper()
	flowConfig := tunnel.FlowConfig{Budget: &workload.ReplayBudget{}, BootID: "repack-boot", InstanceID: "repack-instance", OwnerGen: 7,
		TokenLookup: func(tunnel.AuthRequest) error { return nil },
		Authorize: func(req tunnel.AuthRequest) (tunnel.Authority, error) {
			return tunnel.Authority{Enabled: true, Allocation: req.Allocation}, nil
		}}
	if phase == "before" {
		controlReservation, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := controlReservation.Addr().String()
		defer func() { _ = controlReservation.Close() }()
		publicReservation, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := publicReservation.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("close publicReservation: %v", err)
			}
		}()
		port := publicReservation.Addr().(*net.TCPAddr).Port
		flowConfig.TokenLookup = func(tunnel.AuthRequest) error { return publicReservation.Close() }
		// Keep this leaf test independent of the process fixture, which imports
		// brokerimage itself. Trust only a fresh fixture certificate.
		pub, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
		der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
		if err != nil {
			t.Fatal(err)
		}
		certificate := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		pool := x509.NewCertPool()
		pool.AddCert(cert)

		ctx, cancel := context.WithCancel(context.Background()) // ctx-root: before-exec control fixture.
		defer cancel()
		var server *tunnel.Server
		// Keep the public port reserved while selecting the control port. The
		// first occupied attempt deterministically exercises the TOCTOU retry;
		// a free-port probe cannot promise that Start will acquire the address.
		// origin: hot-update S6 plan §4.2 / T56 / TESTFIX
		for attempt := 0; attempt < 10; attempt++ {
			if attempt != 0 {
				controlReservation, err = net.Listen("tcp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				address = controlReservation.Addr().String()
				_ = controlReservation.Close()
			}
			server = tunnel.NewServer(address, "127.0.0.1", nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			server.SetCertificate(&certificate)
			if err = server.SetFlowConfig(flowConfig); err != nil {
				t.Fatal(err)
			}
			err = server.Start(ctx)
			_ = controlReservation.Close()
			if err == nil {
				break
			}
			server.Close()
			if !errors.Is(err, unix.EADDRINUSE) || attempt == 9 {
				t.Fatal(err)
			}
		}
		defer server.Close()
		// Trust only this fixture CA and its loopback SAN, as on the cold path.
		control, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp4", address, &tls.Config{RootCAs: pool, NextProtos: []string{tunnel.FlowALPN}, MinVersion: tls.VersionTLS12})
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := control.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("close control: %v", err)
			}
		}()
		_ = control.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err = fmt.Fprintf(control, "REGISTER session agent %d token 1\n", port); err != nil {
			t.Fatal(err)
		}
		line, err := bufio.NewReader(control).ReadString('\n')
		if err != nil || line != "OK\n" {
			t.Fatalf("REGISTER: %q %v", line, err)
		}
		peer, err := net.DialTimeout("tcp4", publicReservation.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = peer.Write([]byte("original-pending")); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := peer.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("close peer: %v", err)
			}
		}()
		// Freeze joins the accept worker; retry only when the first cut did not
		// yet contain a pending raw. The peer is already connected to this OFD.
		var frozen tunnel.ServerFrozen
		var image tunnel.ServerImage
		var resources []handoff.Resource
		until := time.Now().Add(3 * time.Second)
		for {
			deadline, err := NewStoreCutDeadline()
			if err != nil {
				t.Fatal(err)
			}
			frozen, err = server.FreezeServer(deadline)
			if err != nil {
				t.Fatal(err)
			}
			image, resources, err = frozen.Export()
			if err != nil {
				t.Fatal(err)
			}
			pending := false
			for _, res := range resources {
				pending = pending || res.Files["raw"] != nil
			}
			if pending {
				break
			}
			if err = frozen.Thaw(deadline); err != nil {
				t.Fatal(err)
			}
			if time.Now().After(until) {
				t.Fatal("freeze never owned the accepted raw")
			}
		}
		b := fixtureBundle(t)
		b.InstanceID, b.Tunnel = flowConfig.InstanceID, image
		b.RequiredDomains = []string{Domain, "flow-v1"}
		b.Args = []string{os.Args[0], "-test.run=^TestBrokerTunnelPristineRepackRoundTrip$", "-test.count=1"}
		lock, err := os.OpenFile(filepath.Join(t.TempDir(), "tether.lock"), os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := lock.Close(); err != nil {
				t.Errorf("close lock: %v", err)
			}
		}()
		if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			t.Fatal(err)
		}
		resources = append(resources, handoff.Resource{ID: b.LockResource, Kind: "store-lock-v1", Files: map[string]*os.File{"lock": lock}})
		private := fixturePrivate(b)
		for _, res := range resources {
			for name, file := range res.Files {
				identity, err := handoff.ResourceFileIdentity(file)
				if err != nil {
					t.Fatal(err)
				}
				purpose, owner := name, "server"
				if res.ID == b.LockResource {
					purpose, owner = "store-lock", "store"
				}
				private.Resources = append(private.Resources, ResourceOwnership{ID: res.ID, Kind: res.Kind, Data: res.Data, Name: name, Identity: identity, Purpose: purpose, Owner: owner, CutID: private.Runtime.CutID})
			}
		}
		sort.Slice(private.Resources, func(i, j int) bool {
			return private.Resources[i].ID+"/"+private.Resources[i].Name < private.Resources[j].ID+"/"+private.Resources[j].Name
		})
		data, err := EncodePrivateState(private)
		if err != nil {
			t.Fatal(err)
		}
		state, err := sealedstate.New(b.StateResource, data)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := state.Close(); err != nil {
				t.Errorf("close state: %v", err)
			}
		}()
		resources = append(resources, state.Resources...)
		bundle, err := Encode(b)
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := handoff.Prepare(handoff.State{Role: ImageRole, Instance: b.InstanceID, Generation: b.Generation, Data: bundle}, resources)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := prepared.Close(); err != nil {
				t.Errorf("close prepared: %v", err)
			}
		}()
		env := replaceTestEnv(handoff.CleanEnvironment(os.Environ()), "BROKER_TUNNEL_REPACK_PHASE", "imported")
		env = replaceTestEnv(env, "BROKER_TUNNEL_REPACK_PID", strconv.Itoa(os.Getpid()))
		if err = prepared.Exec("/proc/self/exe", b.Args, env); err != nil {
			t.Fatal(err)
		}
		t.Fatal("exec returned without error")
	}
	restored, err := handoff.Restore()
	if err != nil || restored == nil {
		t.Fatalf("Restore: %v", err)
	}
	defer func() {
		if err := restored.Close(); err != nil {
			t.Errorf("close restored: %v", err)
		}
	}()
	if again, err := handoff.Restore(); err != nil || again != nil {
		t.Fatal("Restore consumed the envelope twice")
	}
	boot, err := Boot(restored)
	if err != nil {
		t.Fatal(err)
	}
	b := boot.Bundle()
	pid, err := strconv.Atoi(os.Getenv("BROKER_TUNNEL_REPACK_PID"))
	if err != nil || pid != os.Getpid() || b.Generation != 8 || len(b.Tunnel.Listeners) != 2 {
		t.Fatal("exec changed PID/generation/listener closure")
	}
	cfg := Config{Mode: b.Mode, Deadline: controlbudget.Deadline(1 << 62), ImportServer: tunnel.ImportServer,
		TunnelConfig: tunnel.ServerImportConfig{PublicHost: "127.0.0.1", Flow: flowConfig},
		NewStore: func(storage.StoreStamp, *os.File, controlbudget.Deadline) (StoreOwner, error) {
			t.Fatal("pristine store constructor")
			return nil, ErrImage
		},
		NewControl: func(StoreOwner, controlbudget.Deadline) (ControlOwner, error) {
			t.Fatal("pristine control constructor")
			return nil, ErrImage
		}}
	imported, err := boot.Import(cfg)
	if err != nil {
		t.Fatal(err)
	}
	resources, err := ImportedResources(imported)
	if err != nil {
		t.Fatal(err)
	}
	if restoredCount := fileCount(resources.Resources); restoredCount != 7 {
		t.Fatalf("inherited closure has %d files, want 7", restoredCount)
	}
	if phase == "imported" {
		prepared, err := boot.Repack()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := prepared.Close(); err != nil {
				t.Errorf("close prepared: %v", err)
			}
		}()
		env := replaceTestEnv(handoff.CleanEnvironment(os.Environ()), "BROKER_TUNNEL_REPACK_PHASE", "repacked")
		if err = prepared.Exec("/proc/self/exe", b.Args, env); err != nil {
			t.Fatal(err)
		}
		t.Fatal("repack exec returned without error")
	}
	if err = boot.Close(); err != nil {
		t.Fatal(err)
	}
	for _, res := range resources.Resources {
		for _, file := range res.Files {
			if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatal("terminal pristine cleanup left a taken file open")
			}
		}
	}
	fmt.Println("tunnel pristine repack receipt")
}

// Decode-only broker metadata must end before the first store side effect,
// including a failing opener. Waiting for successful activation or process exit
// pins one inherited memfd for every image generation.
// origin: hot-update S6 plan §4.2 / T52; internal review rv-fd-8
func TestBrokerActivationReleasesInheritedPrivateState(t *testing.T) {
	for _, failOpen := range []bool{false, true} {
		t.Run(strconv.FormatBool(failOpen), func(t *testing.T) {
			f := newImportFixture(t)
			var state *os.File
			for _, res := range f.source.Borrow() {
				if res.ID == f.bundle.StateResource {
					state = res.Files["state"]
				}
			}
			if state == nil {
				t.Fatal("fixture lacks inherited broker state")
			}
			opener := f.config.NewStore
			f.config.NewStore = func(stamp storage.StoreStamp, lock *os.File, deadline controlbudget.Deadline) (StoreOwner, error) {
				if _, err := state.Stat(); !errors.Is(err, os.ErrClosed) {
					t.Fatal("store constructor started while inherited private state remained open")
				}
				if failOpen {
					return nil, ErrImage
				}
				return opener(stamp, lock, deadline)
			}
			image, _, err := Import(f.config, f.bundle, &flatFaultSource{source: f.source})
			if err != nil {
				t.Fatal(err)
			}
			err = image.Activate(&testSlot{events: &f.events}, f.config.Deadline)
			if (err != nil) != failOpen {
				t.Fatalf("activation failure=%v, want %v", err, failOpen)
			}
			resources, err := ImportedResources(image)
			if err != nil {
				t.Fatal(err)
			}
			defer closeSalvage(resources)
		})
	}
}
