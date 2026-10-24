//go:build linux && amd64

// Package infraprivilege tests the nonroot authorization/syscall seam. Actual
// root UID, systemd capability and nft receipts remain the responsibility of 86.K.
package infraprivilege

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/caddyconfig"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// The tagged supply adds the actual installed-entry checks to these same Tests.
// Default runs exercise real codecs, kernel peers, Unix admin and unit policy;
// they do not advertise a multi-UID or installed-helper acceptance receipt.
var installedAuthorityCheck func(*testing.T)
var installedRotationCheck func(*testing.T)

type authorityFixture struct {
	policy infrapriv.Policy
	keys   map[infrapriv.Scope]ed25519.PrivateKey
	peer   c.Peer
}

func newAuthorityFixture(t *testing.T) authorityFixture {
	t.Helper()
	keys := make(map[infrapriv.Scope]ed25519.PrivateKey)
	for _, scope := range []infrapriv.Scope{infrapriv.AccountAuthority, infrapriv.LocalAuthority, infrapriv.LaunchAuthority, infrapriv.RootAuthority} {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		keys[scope] = key
		t.Cleanup(func() { clear(key) })
	}
	// The UID table is an explicit syscall seam, not a claim that this process
	// changed host credentials. kernelPeer below separately verifies SO_PEERCRED.
	p := infrapriv.Policy{Version: 1, HostBootID: "authority-boot", Domain: c.ID{31}, Revision: 7,
		AccountPublicKey: keys[infrapriv.AccountAuthority].Public().(ed25519.PublicKey), TrustSHA256: digest("trust"), ServiceUID: 12341, ServiceGID: 12342}
	for _, scope := range []infrapriv.Scope{infrapriv.AccountAuthority, infrapriv.LocalAuthority, infrapriv.LaunchAuthority, infrapriv.RootAuthority} {
		uid, gid := p.ServiceUID, p.ServiceGID
		if scope == infrapriv.RootAuthority {
			uid, gid = 0, 0
		}
		p.Keys = append(p.Keys, infrapriv.PolicyKey{KeyID: fmt.Sprintf("key-%d", scope), PublicKey: keys[scope].Public().(ed25519.PublicKey), UID: uid, GID: gid, Scope: scope})
	}
	return authorityFixture{p, keys, c.Peer{PID: int32(os.Getpid()), UID: p.ServiceUID, GID: p.ServiceGID, BootID: p.HostBootID, StartTicks: 31}}
}

func digest(s string) c.Digest { return sha256.Sum256([]byte(s)) }

func deadline(t *testing.T) c.Deadline {
	t.Helper()
	d, err := controlbudget.After(infrapriv.WorkerBudget)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func authorityRequest(f authorityFixture, op c.HostOp) c.HostRequest {
	r := c.HostRequest{Version: 1, Op: op, Component: c.HostSupport, Tx: c.ID{1}, Step: 1, Domain: f.policy.Domain,
		Sequence: 1, PolicyRevision: f.policy.Revision, HostBootID: f.policy.HostBootID, Nonce: c.ID{2}, RemainingNS: uint64(infrapriv.ActionBudget)}
	if op != c.Inspect {
		r.ExpectedJournal, r.Payload = digest("journal"), digest("approved-record")
	}
	if op == c.Stage || op == c.UpdateSupport {
		r.Candidate = c.Artifact{Kind: c.HelperBinary, Version: "next", GOOS: "linux", GOARCH: "amd64",
			ArchiveSHA256: digest("archive"), BinarySHA256: digest("binary"), CatalogSHA256: digest("catalog")}
	}
	if op == c.UpdateSupport || op == c.RecoverHost {
		r.Expected = c.Process{BootID: r.HostBootID, PID: 31, StartTicks: 32, InvocationID: "invocation", ExeDevice: 33, ExeInode: 34, ExeSHA256: digest("old-worker"), Cgroup: "/sandbox/worker"}
	}
	return r
}

func signRequest(t *testing.T, f authorityFixture, r c.HostRequest, scope infrapriv.Scope) c.HostRequest {
	t.Helper()
	r.KeyID = fmt.Sprintf("key-%d", scope)
	data, err := c.SigningBytes(r)
	if err != nil {
		t.Fatal(err)
	}
	r.Signature = ed25519.Sign(f.keys[scope], data)
	return r
}

// origin: hot-update S9 plan §5.1 / §5.3 / §6.1 / T72 / M05–M11
func TestInfraPrivilegeCanaryBoundary(t *testing.T) {
	f := newAuthorityFixture(t)
	store, err := infrapriv.NewPolicyStore(f.policy)
	if err != nil {
		t.Fatal(err)
	}
	r := signRequest(t, f, authorityRequest(f, c.Stage), infrapriv.LocalAuthority)
	if err := store.Authorize(r, f.peer); err != nil {
		t.Fatalf("service authority positive control: %v", err)
	}
	for _, row := range []struct {
		name string
		peer c.Peer
		r    c.HostRequest
	}{
		{"ordinary-member", c.Peer{PID: 41, UID: 12343, GID: 12344, BootID: f.policy.HostBootID, StartTicks: 42}, r},
		{"ordinary-uid-same-group", c.Peer{PID: 41, UID: 12343, GID: f.policy.ServiceGID, BootID: f.policy.HostBootID, StartTicks: 42}, r},
		{"service-uid-wrong-group", c.Peer{PID: 41, UID: f.policy.ServiceUID, GID: 12344, BootID: f.policy.HostBootID, StartTicks: 42}, r},
		{"root-with-service-key", c.Peer{PID: 41, BootID: f.policy.HostBootID, StartTicks: 42}, r},
		{"same-uid-launch-key", f.peer, signRequest(t, f, authorityRequest(f, c.Stage), infrapriv.LaunchAuthority)},
		{"same-uid-policy-rotation", f.peer, signRequest(t, f, authorityRequest(f, c.RotatePolicy), infrapriv.LocalAuthority)},
		{"same-uid-support-update", f.peer, signRequest(t, f, authorityRequest(f, c.UpdateSupport), infrapriv.LocalAuthority)},
	} {
		t.Run(row.name, func(t *testing.T) {
			if err := store.Authorize(row.r, row.peer); !errors.Is(err, infrapriv.ErrPermission) {
				t.Fatalf("unauthorized authority reached effect boundary: %v", err)
			}
		})
	}
	root := f.peer
	root.UID, root.GID = 0, 0
	if err := store.Authorize(signRequest(t, f, authorityRequest(f, c.UpdateSupport), infrapriv.RootAuthority), root); err != nil {
		t.Fatalf("root management positive control: %v", err)
	}
	// The signed body must include every field even if another operand guard
	// would also refuse it. We mutate a canonical copy to avoid signature aliasing.
	bound := authorityRequest(f, c.RecoverHost)
	bound.Candidate = authorityRequest(f, c.Stage).Candidate
	bound.Store = c.Store{Device: 1, Inode: 2, MountID: 3, LockDevice: 4, LockInode: 5}
	bound.Config, bound.BeforeCensus, bound.WarmCensus, bound.ReturnProof = digest("config"), digest("before"), digest("warm"), digest("return")
	bound = signRequest(t, f, bound, infrapriv.LocalAuthority)
	if err := store.Authorize(bound, f.peer); err != nil {
		t.Fatal(err)
	}
	unsigned := bound
	unsigned.Signature = nil
	canonical, err := json.Marshal(unsigned)
	if err != nil {
		t.Fatal(err)
	}
	signing, err := c.SigningBytes(bound)
	if err != nil || !bytes.Equal(signing, append([]byte("tether-infra-host-v1\n"), canonical...)) {
		t.Fatal("signing bytes omitted or normalized a request operand")
	}
	for i := 0; i < reflect.TypeFor[c.HostRequest]().NumField(); i++ {
		name := reflect.TypeFor[c.HostRequest]().Field(i).Name
		t.Run("signed-"+name, func(t *testing.T) {
			data, err := c.EncodeRequest(bound)
			if err != nil {
				t.Fatal(err)
			}
			bad, err := c.DecodeRequest(data)
			if err != nil {
				t.Fatal(err)
			}
			changeValue(reflect.ValueOf(&bad).Elem().Field(i))
			if store.Authorize(bad, f.peer) == nil {
				t.Fatalf("unsigned field %s accepted", name)
			}
		})
	}
	for _, field := range []string{"Domain", "PolicyRevision", "HostBootID", "KeyID"} {
		t.Run("signature-independent-"+field, func(t *testing.T) {
			bad := bound
			if field == "HostBootID" {
				bad = signRequest(t, f, authorityRequest(f, c.Inspect), infrapriv.LocalAuthority)
			}
			changeValue(reflect.ValueOf(&bad).Elem().FieldByName(field))
			matching := f.policy
			matching.Keys = append([]infrapriv.PolicyKey(nil), f.policy.Keys...)
			matching.Domain, matching.Revision, matching.HostBootID = bad.Domain, bad.PolicyRevision, bad.HostBootID
			matching.Keys[1].KeyID = bad.KeyID
			peer := f.peer
			peer.BootID = bad.HostBootID
			verifier, err := infrapriv.NewPolicyStore(matching)
			if err != nil {
				t.Fatal(err)
			}
			if err := verifier.Authorize(bad, peer); !errors.Is(err, infrapriv.ErrPermission) {
				t.Fatalf("policy matching substituted for %s signature binding: %v", field, err)
			}
		})
	}
	t.Run("effective-unit", checkEffectiveUnitBoundary)
	t.Run("admin-permissions", checkAdminPermissions)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePrivilegeLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "privilege canary", g)
	testharness.AssertNoFDLeak(t, "privilege canary", fd, 0)
	if installedAuthorityCheck != nil {
		installedAuthorityCheck(t)
	}
}

// origin: hot-update S9 plan §5.1 / T72 / M10
func checkAdminPermissions(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "permissions.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	expected := c.Process{BootID: "boot", PID: int32(os.Getpid()), StartTicks: 1, InvocationID: "invocation", ExeDevice: 1, ExeInode: 1, ExeSHA256: digest("exe"), Cgroup: "/private"}
	for _, mode := range []os.FileMode{0666, 0660, 0640} {
		if err := os.Chmod(socket, mode); err != nil {
			t.Fatal(err)
		}
		called := false
		admin, err := caddyconfig.OpenAdmin(t.Context(), socket, expected, uint32(os.Geteuid()), func(*net.UnixConn, c.Process, uint32) error { called = true; return caddyconfig.ErrAdmin })
		if admin != nil {
			_ = admin.Close()
		}
		if called || !errors.Is(err, caddyconfig.ErrAdmin) {
			t.Fatalf("socket mode %04o reached peer/config access", mode)
		}
	}
	if err := os.Chmod(socket, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	called := false
	admin, err := caddyconfig.OpenAdmin(t.Context(), socket, expected, uint32(os.Geteuid()), func(*net.UnixConn, c.Process, uint32) error { called = true; return caddyconfig.ErrAdmin })
	if admin != nil {
		_ = admin.Close()
	}
	if called || !errors.Is(err, caddyconfig.ErrAdmin) {
		t.Fatal("public parent directory reached admin verification")
	}
}

func changeValue(v reflect.Value) {
	switch v.Kind() {
	case reflect.String:
		v.SetString(v.String() + "changed")
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(v.Uint() + 1)
	case reflect.Int32:
		v.SetInt(v.Int() + 1)
	case reflect.Array:
		changeValue(v.Index(0))
	case reflect.Struct:
		if v.Type() == reflect.TypeFor[c.Process]() {
			changeValue(v.FieldByName("PID"))
		} else {
			changeValue(v.Field(0))
		}
	case reflect.Slice:
		v.Index(0).SetUint(v.Index(0).Uint() ^ 1)
	default:
		panic("unsupported signed field")
	}
}

func checkEffectiveUnitBoundary(t *testing.T) {
	u := infrahost.EffectiveUnit{Name: "caddy.service", LoadState: "loaded", UnitFileState: "enabled", ActiveState: "active", SubState: "running",
		FragmentPath: "/etc/systemd/system/caddy.service", NeedDaemonReload: "no", User: "tether", Group: "tether", DynamicUser: "no", Type: "simple",
		Restart: "always", RestartUSec: "2s", RuntimeMaxUSec: "infinity", NoNewPrivileges: "yes", AmbientCapabilities: "cap_net_bind_service", CapabilityBoundingSet: "cap_net_bind_service",
		Exec: [7]string{2: "{ path=/usr/local/libexec/tether-infra-launch ; argv[]=/usr/local/libexec/tether-infra-launch --component caddy ; flags= }"}}
	if err := infrahost.ValidateEffectiveUnit(u, infrahost.UnitPolicy{Target: infrahost.UnitCaddy, Expected: u}); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		name string
		edit func(*infrahost.EffectiveUnit)
	}{
		{"ambient-addition", func(u *infrahost.EffectiveUnit) { u.AmbientCapabilities += " cap_dac_override" }},
		{"bounding-addition", func(u *infrahost.EffectiveUnit) { u.CapabilityBoundingSet += " cap_sys_admin" }},
		{"privileged-prefix", func(u *infrahost.EffectiveUnit) {
			u.Exec[2] = strings.Replace(u.Exec[2], "flags=", "flags=fully-privileged", 1)
		}},
		{"environment-file", func(u *infrahost.EffectiveUnit) { u.EnvironmentFiles = "/operator/canary" }},
		{"environment", func(u *infrahost.EffectiveUnit) { u.Environment = "CANARY=private" }},
		{"extra-group", func(u *infrahost.EffectiveUnit) { u.SupplementaryGroups = "root" }},
		{"root-uid", func(u *infrahost.EffectiveUnit) { u.User = "root" }},
		{"warm-deadline", func(u *infrahost.EffectiveUnit) { u.RuntimeMaxUSec = "2min" }},
	} {
		t.Run(row.name, func(t *testing.T) {
			bad := u
			row.edit(&bad)
			// Even an attacker-adjusted expected template cannot authorize these
			// intrinsically unsafe effective properties.
			if err := infrahost.ValidateEffectiveUnit(bad, infrahost.UnitPolicy{Target: infrahost.UnitCaddy, Expected: bad}); !errors.Is(err, infrahost.ErrUnitInvalid) {
				t.Fatalf("unsafe effective unit accepted: %v", err)
			}
		})
	}
	for _, index := range []int{0, 1, 3, 4, 5, 6} {
		bad := u
		bad.Exec[index] = "{ path=/bin/sh ; argv[]=/bin/sh -c read-canary ; flags= }"
		if infrahost.ValidateEffectiveUnit(bad, infrahost.UnitPolicy{Target: infrahost.UnitCaddy, Expected: bad}) == nil {
			t.Fatalf("extra Exec hook %d accepted", index)
		}
	}
	dropin, err := infrahost.ManagedUnitDropIn(c.Caddy)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"AmbientCapabilities", "CapabilityBoundingSet"} {
		clearAt := bytes.Index(dropin, []byte("\n"+field+"=\n"))
		setAt := bytes.Index(dropin, []byte("\n"+field+"=CAP_NET_BIND_SERVICE\n"))
		if clearAt < 0 || setAt <= clearAt || bytes.Count(dropin, []byte("\n"+field+"=")) != 2 {
			t.Fatalf("%s does not clear accumulated capabilities", field)
		}
	}
}

// origin: hot-update S9 plan §12 P31 / §9.5
func TestInfraPrivilegeLifecycle(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePrivilegeLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infra privilege", g)
	testharness.AssertNoFDLeak(t, "infra privilege", fd, 0)
}

func exercisePrivilegeLifecycle(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "admin.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	if err := os.Chmod(socket, 0600); err != nil {
		t.Fatal(err)
	}
	joined := make(chan error, 1)
	go func() {
		conn, err := listener.AcceptUnix()
		if err != nil {
			joined <- err
			return
		}
		defer func() { _ = conn.Close() }()
		// No request is allowed through the failed invocation/UID verification.
		_ = conn.SetReadDeadline(time.Now().Add(infrapriv.ReadBudget))
		var b [1]byte
		n, err := conn.Read(b[:])
		if n != 0 || !errors.Is(err, io.EOF) {
			joined <- errors.New("unauthorized admin sent request bytes")
			return
		}
		joined <- nil
	}()
	canary := make([]byte, 32)
	if _, err := rand.Read(canary); err != nil {
		t.Fatal(err)
	}
	defer clear(canary)
	expected := c.Process{BootID: "boot", PID: int32(os.Getpid()), StartTicks: 1, InvocationID: "invocation", ExeDevice: 1, ExeInode: 1, ExeSHA256: digest("exe"), Cgroup: "/private"}
	uid := uint32(os.Geteuid())
	if uid == 0 {
		t.Fatal("nonroot privilege suite must not run as host root")
	}
	verified := false
	admin, err := caddyconfig.OpenAdmin(t.Context(), socket, expected, uid, func(conn *net.UnixConn, _ c.Process, _ uint32) error {
		peer, err := infrapriv.PeerCredentials(conn)
		if err != nil {
			return err
		}
		if peer.PID != int32(os.Getpid()) || peer.UID != uid || peer.GID != uint32(os.Getegid()) || peer.StartTicks == 0 || peer.BootID == "" {
			return errors.New("kernel peer mismatch")
		}
		verified = true
		// A hostile verifier error is deliberately secret-bearing. OpenAdmin
		// must replace it at the public error boundary, not wrap its message.
		return fmt.Errorf("private verifier: %s", canary)
	})
	if admin != nil {
		_ = admin.Close()
		t.Fatal("unauthorized invocation opened admin")
	}
	if !verified || !errors.Is(err, caddyconfig.ErrAdmin) {
		t.Fatalf("admin did not use peer authorization: %v", err)
	}
	assertNoCanary(t, fmt.Sprintf("%v %#v", err, err), canary)
	if err := <-joined; err != nil {
		t.Fatal(err)
	}
	// Public formatters must redact private paths regardless of their field names.
	options := caddyconfig.Options{Host: string(canary), AdminSocket: string(canary), Certificate: string(canary), Key: string(canary)}
	assertNoCanary(t, fmt.Sprintf("%v %+v %#v %s", options, options, options, options), canary)
}

func assertNoCanary(t *testing.T, output string, canary []byte) {
	t.Helper()
	for _, encoded := range []string{string(canary), hex.EncodeToString(canary), strings.ToUpper(hex.EncodeToString(canary)), base64.StdEncoding.EncodeToString(canary), base64.RawStdEncoding.EncodeToString(canary)} {
		if strings.Contains(output, encoded) {
			t.Fatal("private canary escaped into public diagnostics")
		}
	}
}

// Inotify observes actual opens/accesses independently of a helper's claimed
// denial. A root load of a canary can fail later and still be a permission bug.
// origin: hot-update S9 plan §5.1 / §6.3 / T72
func watchBusinessCanaries(t *testing.T, root string) ([]byte, func(), func()) {
	t.Helper()
	private := filepath.Join(root, "private-business-secrets")
	if err := os.Mkdir(private, 0700); err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	canary := []byte(hex.EncodeToString(raw))
	clear(raw)
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"account.nk", "broker.nk", "tunnel.key"} {
		path := filepath.Join(private, name)
		if err := os.WriteFile(path, canary, 0600); err != nil {
			_ = unix.Close(fd)
			t.Fatal(err)
		}
		if _, err := unix.InotifyAddWatch(fd, path, unix.IN_OPEN|unix.IN_ACCESS); err != nil {
			_ = unix.Close(fd)
			t.Fatal(err)
		}
	}
	return canary, func() {
		var events [4096]byte
		n, err := unix.Read(fd, events[:])
		if n > 0 || (err != nil && !errors.Is(err, unix.EAGAIN)) {
			t.Fatal("supporting helper opened a business-secret canary")
		}
	}, func() { _ = unix.Close(fd); clear(canary) }
}
