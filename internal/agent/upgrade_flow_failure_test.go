//go:build linux

package agent

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

// origin: hot-update S5 plan §5.4 / §8 TestFlowSSExecFailureThawsBothDirections.
// The exec seam invokes the real syscall on a non-executable image. Original
// public/target connections must survive, and SS must await fresh authority.
func TestFlowSSExecFailureThawsBothDirections(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseFlowSSExecFailure(t)
		})
	}
	testharness.AssertNoFDLeak(t, "flow/SS real exec failure x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "flow/SS real exec failure x5", beforeG)
}

func exerciseFlowSSExecFailure(t *testing.T) {
	t.Helper()
	a := cutTestAgent(t)
	budget := &workload.ReplayBudget{}
	ss := ssproxy.New(a.cfg.Logger)
	keys := []ssproxy.Key{{KeyID: "fixture", Secret: "exec-failure-fixture"}}
	ssPort, err := ss.Start(0, keys)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ss.Stop)
	a.proxy.srv = ss
	a.proxy.localPort = ssPort
	if err := ss.Authorize(ssproxy.Grant{Enabled: true, Generation: 1, Epoch: 1, AppliedRevision: 1, Keys: keys}); err != nil {
		t.Fatalf("initial SS grant (pending P8): %v", err)
	}
	// Directly installing the real SS owner must also install the matching
	// agent authority receipt and allocation token, as the live apply path does.
	// origin: hot-update S5 internal review PRIVATE-STATE-2
	a.proxy.gen, a.proxy.epoch, a.proxy.token = 1, 1, "fixture"
	a.proxy.keysetDigest, err = ssproxy.GrantDigest(ssproxy.Grant{Enabled: true, Generation: 1, Epoch: 1, Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	a.proxy.digestApplied = true

	plainTarget := flowFailureListener(t)
	ssTarget := flowFailureListener(t)
	control := flowFailureListener(t)
	public := flowFailureListener(t)
	ssPublic := flowFailureListener(t)
	controlAddr := control.Addr().String()
	plainPort := public.Addr().(*net.TCPAddr).Port
	ssPublicPort := ssPublic.Addr().(*net.TCPAddr).Port
	a.proxy.publicPort = ssPublicPort
	_ = control.Close()
	_ = public.Close()
	_ = ssPublic.Close()
	grantRequested, grantRelease := make(chan struct{}), make(chan struct{})
	var grantOnce, releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(grantRelease) }) })
	var holdGrant atomic.Bool
	authorize := func(req tunnel.AuthRequest) (tunnel.Authority, error) {
		if holdGrant.Load() {
			grantOnce.Do(func() { close(grantRequested) })
			timer := time.NewTimer(time.Until(req.Deadline))
			defer timer.Stop()
			select {
			case <-grantRelease:
			case <-req.Stop:
				return tunnel.Authority{}, errors.New("fixture stopped")
			case <-timer.C:
				return tunnel.Authority{}, os.ErrDeadlineExceeded
			}
		}
		return tunnel.Authority{Enabled: true, Allocation: req.Allocation, Generation: 1, ProxyEpoch: 1, AppliedRevision: 2, Keys: []tunnel.AuthorizedKey{{ID: "fixture", Secret: []byte(keys[0].Secret)}}}, nil
	}
	server := tunnel.NewServer(controlAddr, "127.0.0.1", func(string, string, int, string, int64) error { return nil }, a.cfg.Logger)
	if err := server.SetFlowConfig(tunnel.FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "fixture-broker", BootID: "fixture-boot", OwnerGen: 1, TokenLookup: func(tunnel.AuthRequest) error { return nil }, Authorize: authorize}); err != nil {
		t.Fatalf("flow broker configuration (pending P1): %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() { cancel(); server.Close() })
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	adapter := NewTunnelExposeAdapter(controlAddr, a.cfg.SID, a.cfg.NID, a.cfg.Logger)
	a.cfg.ExposeAdapter = adapter
	a.imageGeneration = 1
	if err := adapter.client.SetFlowConfig(tunnel.FlowConfig{Budget: budget, InstanceID: a.instanceID, OwnerGen: 1, BootID: "fixture", BeforeLocalConnect: func(id flow.ID, tuple rawsocket.Tuple) error {
		if int(tuple.Remote.Port()) == ssPort {
			return ss.ExpectFlow([16]byte(id), tuple)
		}
		return nil
	}}); err != nil {
		t.Fatalf("flow client configuration (pending P1/P14): %v", err)
	}
	if err := ss.SetFlowRevoke(func(id [16]byte, _ string) { _ = adapter.client.RevokeFlow(flow.ID(id), flow.ReasonRevoked) }); err != nil {
		t.Fatal(err)
	}
	adapter.Start(ctx)
	t.Cleanup(func() { adapter.client.Close(plainPort); adapter.client.Close(ssPublicPort) })
	for _, pair := range [][2]int{{plainPort, plainTarget.Addr().(*net.TCPAddr).Port}, {ssPublicPort, ssPort}} {
		if err := adapter.AddProxy(t.Context(), PortToken{Name: "fixture", Port: pair[0], LocalPort: pair[1], Token: "fixture"}); err != nil {
			t.Fatal(err)
		}
	}
	plain := flowFailureDial(t, plainPort)
	plainPeer, err := plainTarget.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plainPeer.Close() })
	ssWire := flowFailureDial(t, ssPublicPort)
	ssClient := &flowFailureSSClient{conn: ssWire, password: keys[0].Secret}
	addr := ssTarget.Addr().(*net.TCPAddr)
	header := []byte{1, 127, 0, 0, 1, byte(addr.Port >> 8), byte(addr.Port)}
	ssClient.send(t, append(header, []byte("before")...))
	ssPeer, err := ssTarget.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ssPeer.Close() })
	flowFailureExchange(t, plain, plainPeer, []byte("plain-before"))
	flowFailureRead(t, ssPeer, []byte("before"))
	if _, err := ssPeer.Write([]byte("before")); err != nil {
		t.Fatal(err)
	}
	ssClient.receive(t, []byte("before"))
	plainID, ssID := flowFailureIdentity(t, plainPeer), flowFailureIdentity(t, ssPeer)
	if c := adapter.client.FlowCensus(); c.Unsupported || c.Owned == 0 {
		t.Fatal("flow cut has no real v1 owner (pending P1/P14)")
	}
	a.gate.close()
	cut, err := freezeForUpgrade(a, time.Now().Add(5*time.Second))
	if err != nil {
		t.Fatalf("complete flow/SS cut (pending P7/P10/P14/P15/P17): %v", err)
	}
	cut.readers = []agentimage.Capabilities{agentimage.CurrentCapabilities(mustSHA(t, a.cfg.UpgradeExecutablePath))}
	holdGrant.Store(true)
	execReturned := make(chan error, 1)
	a.cfg.UpgradeNoExit = false
	a.cfg.UpgradeExecFn = func(path string) error {
		// ENOEXEC is a real failed replacement, so this process and its
		// original descriptors continue owning the business image.
		err := syscall.Exec(path, []string{path}, []string{})
		execReturned <- err
		return err
	}
	a.reExecInPlace(a.cfg.UpgradeExecutablePath, cut, false)
	select {
	case err := <-execReturned:
		if !errors.Is(err, syscall.ENOEXEC) {
			t.Fatalf("real exec refusal=%v", err)
		}
	case <-time.After(cutTestWait):
		t.Fatal("exec failure seam not reached")
	}
	waitExposeAdmission(t, grantRequested, "fresh SS authority requested after exec failure")
	ssClient.send(t, []byte("during-pending"))
	if err := ssPeer.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if n, err := ssPeer.Read(b[:]); n != 0 || err == nil {
		t.Fatal("SS wrote target bytes before the fresh grant")
	} else if e, ok := err.(net.Error); !ok || !e.Timeout() {
		t.Fatalf("original SS target was closed: %v", err)
	}
	_ = ssPeer.SetReadDeadline(time.Now().Add(cutTestWait))
	if flowFailureIdentity(t, plainPeer) != plainID || flowFailureIdentity(t, ssPeer) != ssID {
		t.Fatal("exec failure replaced an original business FD")
	}
	releaseOnce.Do(func() { close(grantRelease) })
	cutWait(t, "exec failure thaw and fresh grant", func() bool { a.gate.mu.Lock(); defer a.gate.mu.Unlock(); return !a.gate.closed })
	flowFailureRead(t, ssPeer, []byte("during-pending"))
	if _, err := ssPeer.Write([]byte("after-grant")); err != nil {
		t.Fatal(err)
	}
	ssClient.receive(t, []byte("after-grant"))
	flowFailureExchange(t, plain, plainPeer, []byte("plain-after"))
	// A second accept would expose a hidden re-Dial that byte equality alone
	// cannot detect. Both original peers remain alive for this assertion.
	for _, ln := range []*net.TCPListener{plainTarget, ssTarget} {
		_ = ln.SetDeadline(time.Now().Add(20 * time.Millisecond))
		if c, err := ln.Accept(); err == nil {
			_ = c.Close()
			t.Fatal("exec failure re-dialed a business target")
		}
	}
}

func flowFailureListener(t *testing.T) *net.TCPListener {
	t.Helper()
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	_ = ln.SetDeadline(time.Now().Add(cutTestWait))
	return ln
}
func flowFailureDial(t *testing.T, port int) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), cutTestWait)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(cutTestWait))
	return c
}
func flowFailureRead(t *testing.T, r io.Reader, want []byte) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(r, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("original business stream lost or duplicated bytes")
	}
}
func flowFailureExchange(t *testing.T, c net.Conn, peer net.Conn, data []byte) {
	t.Helper()
	if _, err := c.Write(data); err != nil {
		t.Fatal(err)
	}
	flowFailureRead(t, peer, data)
	if _, err := peer.Write(data); err != nil {
		t.Fatal(err)
	}
	flowFailureRead(t, c, data)
}
func flowFailureIdentity(t *testing.T, c *net.TCPConn) [2]uint64 {
	t.Helper()
	raw, err := c.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var st syscall.Stat_t
	var statErr error
	if err := raw.Control(func(fd uintptr) { statErr = syscall.Fstat(int(fd), &st) }); err != nil {
		t.Fatal(err)
	}
	if statErr != nil {
		t.Fatal(statErr)
	}
	return [2]uint64{uint64(st.Dev), st.Ino}
}

// Independent SS framing uses only the published AEAD protocol. This is an
// interoperability fixture; no production parser/writer participates in it.
type flowFailureSSClient struct {
	conn             net.Conn
	password         string
	tx, rx           cipher.AEAD
	txNonce, rxNonce [12]byte
}

func flowFailureCipher(t *testing.T, password string, salt []byte) cipher.AEAD {
	t.Helper()
	var master, prev []byte
	for len(master) < 32 {
		h := md5.New()
		_, _ = h.Write(prev)
		_, _ = h.Write([]byte(password))
		prev = h.Sum(nil)
		master = append(master, prev...)
	}
	key := make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha1.New, master[:32], salt, []byte("ss-subkey")), key); err != nil {
		t.Fatal(err)
	}
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		t.Fatal(err)
	}
	return aead
}
func flowFailureNonce(n *[12]byte) {
	for i := range n {
		n[i]++
		if n[i] != 0 {
			return
		}
	}
	panic("fixture nonce exhausted")
}
func (c *flowFailureSSClient) send(t *testing.T, data []byte) {
	t.Helper()
	if c.tx == nil {
		salt := make([]byte, 32)
		if _, err := rand.Read(salt); err != nil {
			t.Fatal(err)
		}
		c.tx = flowFailureCipher(t, c.password, salt)
		if _, err := c.conn.Write(salt); err != nil {
			t.Fatal(err)
		}
	}
	var size [2]byte
	binary.BigEndian.PutUint16(size[:], uint16(len(data)))
	wire := c.tx.Seal(nil, c.txNonce[:], size[:], nil)
	flowFailureNonce(&c.txNonce)
	wire = append(wire, c.tx.Seal(nil, c.txNonce[:], data, nil)...)
	flowFailureNonce(&c.txNonce)
	if _, err := c.conn.Write(wire); err != nil {
		t.Fatal(err)
	}
}
func (c *flowFailureSSClient) receive(t *testing.T, want []byte) {
	t.Helper()
	if c.rx == nil {
		salt := make([]byte, 32)
		if _, err := io.ReadFull(c.conn, salt); err != nil {
			t.Fatal(err)
		}
		c.rx = flowFailureCipher(t, c.password, salt)
	}
	var got []byte
	for len(got) < len(want) {
		length := make([]byte, 18)
		if _, err := io.ReadFull(c.conn, length); err != nil {
			t.Fatal(err)
		}
		plain, err := c.rx.Open(nil, c.rxNonce[:], length, nil)
		if err != nil {
			t.Fatal("response length authentication failed")
		}
		flowFailureNonce(&c.rxNonce)
		n := int(binary.BigEndian.Uint16(plain))
		if n == 0 || n > 0x3fff {
			t.Fatal("invalid response chunk length")
		}
		wire := make([]byte, n+16)
		if _, err := io.ReadFull(c.conn, wire); err != nil {
			t.Fatal(err)
		}
		plain, err = c.rx.Open(nil, c.rxNonce[:], wire, nil)
		if err != nil {
			t.Fatal("response payload authentication failed")
		}
		flowFailureNonce(&c.rxNonce)
		got = append(got, plain...)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("SS response lost or duplicated bytes")
	}
}
