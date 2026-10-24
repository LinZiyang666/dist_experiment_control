//go:build linux && amd64

package infralaunch

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"golang.org/x/sys/unix"
)

func fixtureBackend(t *testing.T, r infracontract.HostRequest, s Selection, census *infracontract.Census, observed *infracontract.Process, authorize *error) *Backend {
	t.Helper()
	b, err := NewBackend(Bindings{
		Authorize: func(infracontract.HostRequest, infracontract.Deadline, infracontract.Stop) (Selection, error) {
			return s, *authorize
		},
		StoreCensus: func(infracontract.Store, infracontract.Deadline, infracontract.Stop) (infracontract.Census, error) {
			return *census, nil
		},
		Process: func(infracontract.Process, infracontract.Deadline) (infracontract.Process, error) {
			return *observed, nil
		},
		Inspect: func(infracontract.ID, infracontract.Deadline) (infracontract.Census, error) { return *census, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func emptyCensus() infracontract.Census {
	return infracontract.Census{Complete: true, Generation: 1, Digest: infracontract.Digest{7}}
}

// origin: hot-update S9 plan §4.1 / T15
func TestLegacyStoreOwnerBlocksSecondLaunch(t *testing.T) {
	for _, component := range []infracontract.Component{infracontract.NATS, infracontract.Caddy} {
		t.Run(componentName(component), func(t *testing.T) {
			for _, lane := range []string{"service", "cli", "recover", "bootstrap", "reinstall"} {
				t.Run(lane, func(t *testing.T) {
					r, s, _ := launchFixture(t, stockBinary(t))
					r.Component = component
					if component == infracontract.Caddy {
						r.Candidate.Kind = infracontract.CaddyBinary
					}
					census := emptyCensus()
					observed := r.Expected
					var denied error
					b := fixtureBackend(t, r, s, &census, &observed, &denied)
					census.Processes = []infracontract.Process{r.Expected}
					if receipt, err := b.Do(r, launchDeadline(t), nil); !errors.Is(err, ErrStoreBusy) || receipt.Applied {
						t.Fatalf("live legacy process bypassed %s guard: %+v %v", lane, receipt, err)
					}
					census.Processes = nil
					census.Complete = false
					if _, err := b.Do(r, launchDeadline(t), nil); !errors.Is(err, infracontract.ErrIncomplete) {
						t.Fatalf("unknown legacy census admitted: %v", err)
					}
					census = emptyCensus()
					census.OldOwnerPending = 1
					if _, err := b.Do(r, launchDeadline(t), nil); !errors.Is(err, ErrStoreBusy) {
						t.Fatalf("old store responsibility admitted: %v", err)
					}
					census = emptyCensus()
					denied = ErrLaunchRejected
					if _, err := b.Do(r, launchDeadline(t), nil); !errors.Is(err, ErrLaunchRejected) {
						t.Fatalf("closed startup fence admitted: %v", err)
					}
					denied = nil
					if receipt, err := b.Do(r, launchDeadline(t), nil); err != nil || !receipt.Applied {
						t.Fatalf("proved legacy exit failed to hand off: %+v %v", receipt, err)
					}
					transfer, err := b.Transfer(r)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = transfer.Close() }()
					assertStoreBusy(t, s.Directory, s.Locks)
				})
			}
		})
	}
	// Paths and symlinks are only ways to open a descriptor, never lock keys.
	t.Run("alias", func(t *testing.T) {
		_, s, guard := launchFixture(t, stockBinary(t))
		alias := filepath.Join(t.TempDir(), "alias")
		target, err := os.Readlink("/proc/self/fd/" + fdText(s.Directory))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, alias); err != nil {
			t.Fatal(err)
		}
		dir, err := os.Open(alias)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = dir.Close() }()
		assertStoreBusy(t, dir, s.Locks)
		dup, err := guard.duplicate()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = dup.Close() }()
		for _, f := range []*os.File{guard.file, dup.file} {
			flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
			if err != nil || flags&unix.FD_CLOEXEC == 0 {
				t.Fatalf("unrelated pre-exec child could inherit store lock: %d %v", flags, err)
			}
		}
		if err := guard.Close(); err != nil {
			t.Fatal(err)
		}
		assertStoreBusy(t, dir, s.Locks)
	})
	t.Run("replaced-lock", func(t *testing.T) {
		r, s, guard := launchFixture(t, stockBinary(t))
		if err := unix.Renameat(int(s.Locks.Fd()), lockName(r.Store), int(s.Locks.Fd()), "displaced.lock"); err != nil {
			t.Fatal(err)
		}
		fd, err := unix.Openat(int(s.Locks.Fd()), lockName(r.Store), unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC, 0640)
		if err != nil {
			t.Fatal(err)
		}
		_ = unix.Close(fd)
		if err := guard.verify(); !errors.Is(err, ErrLaunchRejected) {
			t.Fatalf("replaced permanent inode accepted: %v", err)
		}
		if p, err := prepare(r, s); err == nil {
			_ = p.Close()
			t.Fatal("launch accepted a displaced lock inode")
		}
	})
	t.Run("symlink-lock", func(t *testing.T) {
		_, s, guard := launchFixture(t, stockBinary(t))
		name := guard.name
		if err := guard.Close(); err != nil {
			t.Fatal(err)
		}
		if err := unix.Renameat(int(s.Locks.Fd()), name, int(s.Locks.Fd()), "real.lock"); err != nil {
			t.Fatal(err)
		}
		if err := unix.Symlinkat("real.lock", int(s.Locks.Fd()), name); err != nil {
			t.Fatal(err)
		}
		if l, err := AcquireStore(s.Directory, s.Locks); err == nil {
			_ = l.Close()
			t.Fatal("symlink substituted for permanent lock")
		}
	})
}

func componentName(c infracontract.Component) string {
	if c == infracontract.NATS {
		return "nats"
	}
	return "caddy"
}

func fdText(f *os.File) string { return strconv.FormatUint(uint64(f.Fd()), 10) }

// origin: hot-update S9 plan §4.1 / §6.2 / T14 / T15
func TestInfraLaunchAckRequiresObservedStockExec(t *testing.T) {
	r, s, _ := launchFixture(t, stockBinary(t))
	census, observed := emptyCensus(), r.Expected
	var denied error
	b := fixtureBackend(t, r, s, &census, &observed, &denied)
	claimed, err := b.Do(r, launchDeadline(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := b.Do(r, launchDeadline(t), nil); err != nil || !reflect.DeepEqual(again, claimed) {
		t.Fatalf("claim replay changed receipt: %v", err)
	}
	conflict := r
	conflict.Sequence++
	if _, err := b.Do(conflict, launchDeadline(t), nil); !errors.Is(err, ErrLaunchRejected) {
		t.Fatalf("second claim replaced pending custody: %v", err)
	}
	ack := r
	ack.Op = infracontract.LaunchAck
	ack.Sequence++
	ack.BeforeCensus = census.Digest
	if _, err := b.Do(ack, launchDeadline(t), nil); !errors.Is(err, ErrLaunchRejected) {
		t.Fatalf("launcher acknowledged its own future exec: %v", err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(s.Executable.Fd()), &st); err != nil {
		t.Fatal(err)
	}
	observed.ExeDevice, observed.ExeInode, observed.ExeSHA256 = uint64(st.Dev), st.Ino, r.Candidate.BinarySHA256
	ack.Expected = observed
	stockObserved := observed
	for _, mutate := range []func(*infracontract.HostRequest){
		func(r *infracontract.HostRequest) { r.Expected.PID++ }, func(r *infracontract.HostRequest) { r.Expected.StartTicks++ },
		func(r *infracontract.HostRequest) { r.Expected.InvocationID = "other" }, func(r *infracontract.HostRequest) { r.Expected.ExeInode++ },
		func(r *infracontract.HostRequest) { r.Expected.ExeSHA256 = infracontract.Digest{99} }, func(r *infracontract.HostRequest) { r.BeforeCensus = infracontract.Digest{99} },
		func(r *infracontract.HostRequest) { r.Domain = infracontract.ID{99} }, func(r *infracontract.HostRequest) { r.Payload = infracontract.Digest{99} },
	} {
		bad := ack
		mutate(&bad)
		observed = bad.Expected
		if _, err := b.Do(bad, launchDeadline(t), nil); err == nil {
			t.Fatalf("changed stock observation/permit accepted: %+v", bad)
		}
		observed = stockObserved
	}
	receiver, err := b.Transfer(r)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = receiver.Close() }()
	if receipt, err := b.Do(ack, launchDeadline(t), nil); err != nil || !receipt.Applied {
		t.Fatalf("observed stock exec not acknowledged: %+v %v", receipt, err)
	}
	if _, err := b.Transfer(r); !errors.Is(err, ErrLaunchRejected) {
		t.Fatalf("acknowledged claim could be taken twice: %v", err)
	}
	assertStoreBusy(t, s.Directory, s.Locks)
}

// origin: hot-update S9 plan §6.1 / T15
func TestInfraLaunchRejectsChangedResourcesAndAdmission(t *testing.T) {
	for _, which := range []string{"executable-hash", "config-hash", "writable-config", "wrong-store", "wrong-store-descriptor", "wrong-lock", "unknown-component", "directory-permissions", "wrong-platform"} {
		t.Run(which, func(t *testing.T) {
			r, s, _ := launchFixture(t, stockBinary(t))
			switch which {
			case "executable-hash":
				r.Candidate.BinarySHA256 = infracontract.Digest{99}
			case "config-hash":
				r.Config = infracontract.Digest{99}
			case "writable-config":
				path, err := os.Readlink("/proc/self/fd/" + fdText(s.Config))
				if err != nil {
					t.Fatal(err)
				}
				f, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = f.Close() }()
				s.Config = f
			case "wrong-store":
				r.Store.Inode++
			case "wrong-store-descriptor":
				f, err := os.Open(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = f.Close() }()
				s.Directory = f
			case "wrong-lock":
				r.Store.LockInode++
			case "unknown-component":
				r.Component = infracontract.HostSupport
			case "directory-permissions":
				if err := s.Locks.Chmod(0777); err != nil {
					t.Fatal(err)
				}
			case "wrong-platform":
				r.Candidate.GOOS = "darwin"
			}
			if p, err := prepare(r, s); err == nil {
				_ = p.Close()
				t.Fatalf("unsafe %s accepted", which)
			}
		})
	}
	r, s, _ := launchFixture(t, stockBinary(t))
	census, observed := emptyCensus(), r.Expected
	var denied error
	b := fixtureBackend(t, r, s, &census, &observed, &denied)
	stop := make(chan struct{})
	close(stop)
	if _, err := b.Do(r, launchDeadline(t), stop); !errors.Is(err, ErrLaunchRejected) {
		t.Fatalf("cancelled claim admitted: %v", err)
	}
	if _, err := b.Do(r, 1, nil); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("expired claim admitted: %v", err)
	}
	if _, err := NewBackend(Bindings{}); !errors.Is(err, infracontract.ErrIncomplete) {
		t.Fatalf("missing root bindings accepted: %v", err)
	}
}

// origin: hot-update S9 plan §6.1 / T14 / T15
func TestInfraLaunchTransportRejectsForeignPeerAndPartialReply(t *testing.T) {
	for round := 0; round < 5; round++ {
		t.Run("partial", func(t *testing.T) { exerciseTransport(t) })
	}
}

func exerciseTransport(t *testing.T) {
	t.Helper()
	// A valid reply from an ordinary peer must be rejected before reading it.
	r, s, _ := launchFixture(t, stockBinary(t))
	prepared, err := prepare(r, s)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	prepared.deadline = launchDeadline(t)
	ordinary, writer := transportPair(t)
	if err := Send(writer, prepared, launchDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	if p, err := Receive(ordinary, infracontract.NATS, launchDeadline(t), nil); err == nil {
		_ = p.Close()
		t.Fatal("ordinary UID supplied root permit")
	}
	_ = ordinary.Close()
	_ = writer.Close()
	client, server := transportPair(t)
	stop := make(chan struct{})
	timer := time.AfterFunc(20*time.Millisecond, func() { close(stop) })
	defer timer.Stop()
	started := time.Now()
	if p, err := receiveFrom(client, infracontract.NATS, launchDeadline(t), stop, uint32(os.Geteuid())); err == nil {
		_ = p.Close()
		t.Fatal("partial reply was admitted")
	}
	if time.Since(started) > time.Second {
		t.Fatal("cancelled partial reply was not joined promptly")
	}
	_ = client.Close()
	_ = server.Close()
	for _, which := range []string{"version", "expired", "length", "missing-fd", "extra-fd", "wrong-ofd", "changed-process", "changed-component", "trailing-request"} {
		t.Run(which, func(t *testing.T) {
			client, server := transportPair(t)
			request := r
			if which == "changed-process" {
				request.Expected.PID++
			}
			if which == "changed-component" {
				request.Component = infracontract.Caddy
				request.Candidate.Kind = infracontract.CaddyBinary
			}
			data, err := infracontract.EncodeRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			if which == "trailing-request" {
				data = append(data, ' ')
			}
			packet := make([]byte, 16+len(data))
			binary.BigEndian.PutUint32(packet, 1)
			binary.BigEndian.PutUint32(packet[4:], uint32(len(data)))
			binary.BigEndian.PutUint64(packet[8:], uint64(prepared.deadline))
			copy(packet[16:], data)
			fds := []int{int(prepared.executable.Fd()), int(prepared.config.Fd()), int(prepared.directory.Fd()), int(prepared.lock.directory.Fd()), int(prepared.lock.file.Fd())}
			switch which {
			case "version":
				binary.BigEndian.PutUint32(packet, 2)
			case "expired":
				binary.BigEndian.PutUint64(packet[8:], 1)
			case "length":
				binary.BigEndian.PutUint32(packet[4:], infracontract.MaxRequestBytes+1)
			case "missing-fd":
				fds = fds[:4]
			case "extra-fd":
				fds = append(fds, fds[4], fds[4])
			case "wrong-ofd":
				fd, err := unix.Openat(int(prepared.lock.directory.Fd()), prepared.lock.name, unix.O_RDONLY|unix.O_CLOEXEC, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = unix.Close(fd) }()
				fds[4] = fd
			}
			if n, _, err := server.WriteMsgUnix(packet, unix.UnixRights(fds...), nil); err != nil || n != len(packet) {
				t.Fatalf("malformed reply supply: %d %v", n, err)
			}
			if p, err := receiveFrom(client, infracontract.NATS, launchDeadline(t), nil, uint32(os.Geteuid())); err == nil {
				_ = p.Close()
				t.Fatalf("unsafe %s reply admitted", which)
			}
		})
	}
}

func transportPair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(t.TempDir(), "reply.sock"), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	client, err := net.DialUnix("unix", nil, listener.Addr().(*net.UnixAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	server, err := listener.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	return client, server
}

// origin: hot-update S9 plan §4.1 / §6.2 / T14 / T15
func TestInfraLaunchRecoveryKeepsConsumedPermit(t *testing.T) {
	r, s, guard := launchFixture(t, stockBinary(t))
	census, observed := emptyCensus(), r.Expected
	var denied error
	first := fixtureBackend(t, r, s, &census, &observed, &denied)
	receipt, err := first.Do(r, launchDeadline(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	server, err := first.Transfer(r)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}
	adopted, err := AdoptStoreLock(s.Directory, s.Locks, server.lock.file)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adopted.Close() }()
	s.Guard = adopted
	var st unix.Stat_t
	if err := unix.Fstat(int(s.Executable.Fd()), &st); err != nil {
		t.Fatal(err)
	}
	observed.ExeDevice, observed.ExeInode, observed.ExeSHA256 = uint64(st.Dev), st.Ino, r.Candidate.BinarySHA256
	recovery := r
	recovery.Op = infracontract.RecoverHost
	recovery.Sequence += 2
	recovery.Expected = observed
	b := fixtureBackend(t, r, s, &census, &observed, &denied)
	if err := RestoreClaim(b, recovery, r, receipt, launchDeadline(t), nil); !errors.Is(err, ErrStoreBusy) {
		t.Fatalf("empty recovery census treated as current stock owner: %v", err)
	}
	census.Processes = []infracontract.Process{observed, observed}
	if err := RestoreClaim(b, recovery, r, receipt, launchDeadline(t), nil); !errors.Is(err, ErrStoreBusy) {
		t.Fatalf("ambiguous recovery owners admitted: %v", err)
	}
	census.Processes = census.Processes[:1]
	bad := receipt
	bad.RequestDigest = infracontract.Digest{99}
	if err := RestoreClaim(b, recovery, r, bad, launchDeadline(t), nil); err == nil {
		t.Fatal("altered durable claim restored")
	}
	changed := r
	changed.PolicyRevision++
	data, err := infracontract.EncodeRequest(changed)
	if err != nil {
		t.Fatal(err)
	}
	bad.RequestDigest = infracontract.Digest(sha256.Sum256(data))
	// The journal binding is Authorize's responsibility; a valid request digest
	// still must not bypass explicit reauthorization of a different epoch.
	if err := RestoreClaim(b, recovery, changed, bad, launchDeadline(t), nil); err == nil {
		t.Fatal("different authority epoch restored")
	}
	if err := RestoreClaim(b, recovery, r, receipt, launchDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	if transfer, err := b.Transfer(r); err == nil {
		_ = transfer.Close()
		t.Fatal("recovered consumed permit issued new exec rights")
	}
	ack := r
	ack.Op = infracontract.LaunchAck
	ack.Sequence += 3
	ack.BeforeCensus = receipt.Census.Digest
	ack.Expected = observed
	if _, err := b.Do(ack, launchDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	assertStoreBusy(t, s.Directory, s.Locks)
	other, err := unix.Openat(int(s.Locks.Fd()), lockName(r.Store), unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	f := os.NewFile(uintptr(other), "wrong-recovery-ofd")
	defer func() { _ = f.Close() }()
	if l, err := AdoptStoreLock(s.Directory, s.Locks, f); err == nil {
		_ = l.Close()
		t.Fatal("recovery accepted a different unlocked OFD")
	}
}
