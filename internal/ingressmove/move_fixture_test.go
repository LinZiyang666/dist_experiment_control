package ingressmove

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"golang.org/x/sys/unix"
)

type sandbox struct {
	t           *testing.T
	servers     []*server.Server
	process     c.Process
	connections []*sandboxConnection
	key         ed25519.PrivateKey
}
type sandboxConnection struct {
	mu          sync.Mutex
	sandbox     *sandbox
	id          c.ID
	generation  uint64
	nc          *nats.Conn
	raw         net.Conn
	attachCalls int
	lostReply   bool
}
type captureDialer struct {
	raw     net.Conn
	timeout time.Duration
}

func (d *captureDialer) Dial(network, address string) (net.Conn, error) {
	conn, err := net.DialTimeout(network, address, d.timeout)
	if err == nil {
		d.raw = conn
	}
	return conn, err
}

func newSandbox(t *testing.T) *sandbox {
	t.Helper()
	s := &sandbox{t: t, process: currentProcess(t), key: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{11}, ed25519.SeedSize))}
	for i := 0; i < 2; i++ {
		ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
		if err != nil {
			s.close()
			t.Fatal(err)
		}
		s.servers = append(s.servers, ns)
		go ns.Start()
		if !ns.ReadyForConnections(2 * time.Second) {
			s.close()
			t.Fatal("sandbox NATS not ready")
		}
	}
	return s
}
func (s *sandbox) close() {
	for _, conn := range s.connections {
		conn.close()
	}
	for _, ns := range s.servers {
		ns.Shutdown()
		ns.WaitForShutdown()
	}
}
func (s *sandbox) sign(t *testing.T, proof *c.RouteProof) {
	t.Helper()
	proof.Signature = nil
	data, err := c.EncodeRouteProof(*proof)
	if err != nil {
		t.Fatal(err)
	}
	proof.Signature = ed25519.Sign(s.key, data)
}

func (p *sandboxConnection) connect(url string, d c.Deadline) error {
	left, err := controlbudget.Remaining(d)
	if err != nil {
		return err
	}
	dialer := &captureDialer{timeout: left}
	nc, err := nats.Connect(url, nats.NoReconnect(), nats.IgnoreDiscoveredServers(), nats.Timeout(left), nats.SetCustomDialer(dialer))
	if err != nil {
		if dialer.raw != nil {
			_ = dialer.raw.Close()
		}
		return err
	}
	generation := p.generation + 1
	id, owner, socket := p.id, p.sandbox.process, dialer.raw
	// This endpoint is installed by the NC owner on that same NC. A signed
	// Prepare/Bind request and its digest ACK really cross the NATS connection;
	// a different control connection cannot confirm this independent dataNC.
	_, err = nc.Subscribe(p.subject(), func(msg *nats.Msg) {
		proof, decodeErr := c.DecodeRouteProof(msg.Data)
		if decodeErr != nil || proof.Participant != id {
			return
		}
		unsigned := cloneProof(proof)
		unsigned.Signature = nil
		data, encodeErr := c.EncodeRouteProof(unsigned)
		if encodeErr != nil || !ed25519.Verify(p.sandbox.key.Public().(ed25519.PublicKey), data, proof.Signature) {
			return
		}
		cid, cidErr := nc.GetClientID()
		raw, rawErr := socketIdentity(socket, owner)
		if cidErr != nil || rawErr != nil {
			return
		}
		attachment := c.Attachment{Participant: id, Generation: generation, ServerID: nc.ConnectedServerId(), CID: cid, URL: nc.ConnectedUrl(), Raw: raw, Ingress: owner, HealthGeneration: 1}
		if proof.Purpose == c.RoutePrepare && proof.Old != attachment || proof.Purpose == c.RouteBind && proof.New != attachment {
			return
		}
		encoded, encodeErr := json.Marshal(attachment)
		if encodeErr != nil {
			return
		}
		ack := sha256.Sum256(encoded)
		_ = msg.Respond(ack[:])
	})
	if err != nil {
		nc.Close()
		_ = dialer.raw.Close()
		return err
	}
	if err = nc.FlushTimeout(left); err != nil {
		nc.Close()
		_ = dialer.raw.Close()
		return err
	}
	p.nc, p.raw, p.generation = nc, dialer.raw, generation
	return nil
}
func (p *sandboxConnection) snapshot(d c.Deadline) (c.Attachment, error) {
	if p.nc == nil || p.nc.IsClosed() {
		return c.Attachment{}, io.ErrClosedPipe
	}
	left, err := controlbudget.Remaining(d)
	if err != nil {
		return c.Attachment{}, err
	}
	if err = p.nc.FlushTimeout(left); err != nil {
		return c.Attachment{}, err
	}
	cid, err := p.nc.GetClientID()
	if err != nil {
		return c.Attachment{}, err
	}
	raw, err := socketIdentity(p.raw, p.sandbox.process)
	if err != nil {
		return c.Attachment{}, err
	}
	return c.Attachment{Participant: p.id, Generation: p.generation, ServerID: p.nc.ConnectedServerId(), CID: cid, URL: p.nc.ConnectedUrl(), Raw: raw, Ingress: p.sandbox.process, HealthGeneration: 1}, nil
}
func (p *sandboxConnection) Observe(d c.Deadline) (c.Attachment, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshot(d)
}
func (p *sandboxConnection) Install(proof c.RouteProof, d c.Deadline) (c.Attachment, error) {
	return p.acknowledge(proof, d)
}
func (p *sandboxConnection) Bind(proof c.RouteProof, d c.Deadline) (c.Attachment, error) {
	return p.acknowledge(proof, d)
}

func (p *sandboxConnection) subject() string {
	return "sandbox.nc." + strconv.Itoa(int(p.id[0])) + ".ack"
}
func (p *sandboxConnection) acknowledge(proof c.RouteProof, d c.Deadline) (c.Attachment, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, err := c.EncodeRouteProof(proof)
	if err != nil {
		return c.Attachment{}, err
	}
	left, err := controlbudget.Remaining(d)
	if err != nil {
		return c.Attachment{}, err
	}
	msg, err := p.nc.Request(p.subject(), data, left)
	if err != nil {
		return c.Attachment{}, err
	}
	a, err := p.snapshot(d)
	if err != nil {
		return c.Attachment{}, err
	}
	encoded, err := json.Marshal(a)
	if err != nil {
		return c.Attachment{}, err
	}
	want := sha256.Sum256(encoded)
	if !bytes.Equal(msg.Data, want[:]) {
		return c.Attachment{}, ErrIdentity
	}
	return a, nil
}
func (p *sandboxConnection) Attach(target c.RouteTarget, d c.Deadline, stop c.Stop) (c.Attachment, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := check(d, stop); err != nil {
		return c.Attachment{}, err
	}
	p.nc.Close()
	p.attachCalls++
	if err := p.connect(target.URL, d); err != nil {
		return c.Attachment{}, err
	}
	a, err := p.snapshot(d)
	if p.lostReply {
		p.lostReply = false
		return c.Attachment{}, errors.New("actual new CONNECT/PONG received; attachment reply cut")
	}
	return a, err
}
func (p *sandboxConnection) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.nc != nil {
		p.nc.Close()
	}
	if p.raw != nil {
		_ = p.raw.Close()
	}
}

// Independently match server-side CID/peer address to the client's real raw
// tuple. Both embedded server endpoints belong to this test process; this is
// direct-NATS evidence and deliberately does not stand in for Caddy's PID map.
func (s *sandbox) Verify(a c.Attachment, target c.RouteTarget, d c.Deadline) error {
	if err := check(d, nil); err != nil {
		return err
	}
	if a.Ingress != s.process || a.Raw.Owner != s.process || !onTarget(a, target) {
		return ErrIdentity
	}
	for _, ns := range s.servers {
		if ns.ID() != a.ServerID || ns.ClientURL() != a.URL {
			continue
		}
		z, err := ns.Connz(&server.ConnzOptions{CID: a.CID})
		if err != nil {
			return err
		}
		for _, conn := range z.Conns {
			if conn.Cid == a.CID && net.JoinHostPort(conn.IP, strconv.Itoa(conn.Port)) == a.Raw.Local {
				return nil
			}
		}
	}
	return ErrIdentity
}
func (s *sandbox) Census(_ c.Plan, attachments []c.Attachment, d c.Deadline) (c.Census, error) {
	for {
		if err := check(d, nil); err != nil {
			return c.Census{}, err
		}
		var sockets []c.Socket
		count := 0
		for _, ns := range s.servers {
			z, err := ns.Connz(&server.ConnzOptions{Limit: 100})
			if err != nil {
				return c.Census{}, err
			}
			count += z.Total
			for _, conn := range z.Conns {
				found := false
				for _, a := range attachments {
					if a.ServerID == ns.ID() && a.CID == conn.Cid {
						found = true
						sockets = append(sockets, a.Raw)
					}
				}
				if !found {
					count += 1000
				} // A still-live old or unknown NC cannot be hidden.
			}
		}
		if count == len(attachments) && len(sockets) == len(attachments) {
			for _, a := range attachments {
				if err := s.Verify(a, c.RouteTarget{}, d); err != nil {
					return c.Census{}, err
				}
			}
			data, err := json.Marshal(sockets)
			if err != nil {
				return c.Census{}, err
			}
			return c.Census{Complete: true, Generation: 1, LiveBusiness: uint32(count), Processes: []c.Process{s.process}, Sockets: sockets, Digest: sha256.Sum256(data)}, nil
		}
		// Only the sandbox producer waits for NATS's asynchronous accepted-socket
		// close, within the caller's deadline. The production producer is P02.
		time.Sleep(time.Millisecond)
	}
}

func currentProcess(t *testing.T) c.Process {
	t.Helper()
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(stat)[strings.LastIndexByte(string(stat), ')')+2:])
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	exe, err := os.Open("/proc/self/exe")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exe.Close() }()
	info, err := exe.Stat()
	if err != nil {
		t.Fatal(err)
	}
	identity := info.Sys().(*syscall.Stat_t)
	hash := sha256.New()
	if _, err = io.Copy(hash, exe); err != nil {
		t.Fatal(err)
	}
	var sha c.Digest
	copy(sha[:], hash.Sum(nil))
	cgroup, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		t.Fatal(err)
	}
	return c.Process{BootID: strings.TrimSpace(string(boot)), PID: int32(os.Getpid()), StartTicks: ticks, InvocationID: "embedded-nats-test", ExeDevice: uint64(identity.Dev), ExeInode: identity.Ino, ExeSHA256: sha, Cgroup: strings.TrimSpace(string(cgroup))}
}
func socketIdentity(conn net.Conn, owner c.Process) (c.Socket, error) {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return c.Socket{}, ErrIdentity
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return c.Socket{}, err
	}
	var stat unix.Stat_t
	var cookie uint64
	var operation error
	err = raw.Control(func(fd uintptr) {
		operation = unix.Fstat(int(fd), &stat)
		if operation != nil {
			return
		}
		// Linux fixture ABI: SOL_SOCKET=1, SO_COOKIE=57. This runtime-selected
		// sandbox helper stays compilable on Darwin; no production kernel code
		// or platform support claim is introduced by this isolated exercise.
		cookie, operation = unix.GetsockoptUint64(int(fd), 1, 57)
	})
	if err != nil || operation != nil {
		return c.Socket{}, errors.Join(err, operation)
	}
	ns, err := os.Stat("/proc/self/ns/net")
	if err != nil {
		return c.Socket{}, err
	}
	return c.Socket{NetNS: ns.Sys().(*syscall.Stat_t).Ino, Device: uint64(stat.Dev), Inode: stat.Ino, Cookie: cookie, Local: conn.LocalAddr().String(), Remote: conn.RemoteAddr().String(), Owner: owner}, nil
}
