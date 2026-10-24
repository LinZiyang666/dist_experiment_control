//go:build linux && amd64

// Real Unix sockets, SO_PEERCRED/pidfd, shared flocks and cancellation workers.
// The Host records effects; no systemd/nft/process mutation is substituted here.
// Real root/multiple UIDs and process-parent death supervision belong to P31/P27.
package infrapriv

import (
	"crypto/sha256"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"golang.org/x/sys/unix"
)

func exerciseIPCClient(t *testing.T) {
	t.Helper()
	for _, scenario := range []string{"success", "server-uid", "denied-policy", "stale-policy", "lost-effect-reply", "refusal-tx", "refusal-sequence", "refusal-digest", "receipt-tx", "receipt-sequence", "receipt-op", "receipt-digest", "receipt-purpose", "stop"} {
		t.Run(scenario, func(t *testing.T) {
			server, request, limit, host := ipcFixture(t)
			defer closeWorkerLimit(t, limit)
			dir, err := os.MkdirTemp("", "infra-ipc-")
			if err != nil {
				t.Fatal(err)
			}
			// MkdirTemp is outside t.TempDir: this defer is its only remover.
			defer func() {
				if err := os.RemoveAll(dir); err != nil {
					t.Errorf("remove socket directory: %v", err)
				}
			}()
			path := filepath.Join(dir, "control.sock")
			listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = listener.Close() }()
			client := &Client{path: path, serverUID: uint32(os.Geteuid())}
			if scenario == "server-uid" {
				client.serverUID++
			}
			if scenario == "denied-policy" {
				server.policy.current.Keys[1].UID++
			}
			if scenario == "stale-policy" {
				server.policy.current.Revision++
			}
			if scenario == "lost-effect-reply" {
				host.onDo = func(c.HostRequest, c.Deadline, c.Stop) (c.HostReceipt, error) { return c.HostReceipt{}, ErrBudget }
			}
			dl := testDeadline(t, time.Second)
			stop := make(chan struct{})
			if scenario == "stop" {
				close(stop)
			}
			result := make(chan error, 1)
			if scenario != "stop" {
				go func() {
					accepted, err := listener.AcceptUnix()
					if err != nil {
						result <- err
						return
					}
					if scenario == "success" || scenario == "server-uid" || scenario == "denied-policy" || scenario == "stale-policy" || scenario == "lost-effect-reply" {
						result <- server.ServeConn(accepted, dl, nil)
						return
					}
					defer func() { _ = accepted.Close() }()
					if err := setReadDeadline(accepted, dl); err != nil {
						result <- err
						return
					}
					data, err := readFrame(accepted, RequestPurpose)
					if err != nil {
						result <- err
						return
					}
					if strings.HasPrefix(scenario, "refusal-") {
						refusal := Refusal{1, request.Tx, request.Sequence, sha256.Sum256(data), "infra_permission_denied"}
						switch scenario {
						case "refusal-tx":
							refusal.Tx[0]++
						case "refusal-sequence":
							refusal.Sequence++
						case "refusal-digest":
							refusal.RequestDigest[0]++
						}
						encoded, err := encodeRefusal(refusal)
						if err != nil {
							result <- err
							return
						}
						if err := setWriteDeadline(accepted, dl); err != nil {
							result <- err
							return
						}
						err = writeFrame(accepted, RefusalPurpose, encoded)
						if err == nil {
							err = accepted.CloseWrite()
						}
						result <- err
						return
					}
					receipt := host.receipt(request)
					if receipt.RequestDigest != sha256.Sum256(data) {
						result <- ErrIPC
						return
					}
					switch scenario {
					case "receipt-tx":
						receipt.Tx[0]++
					case "receipt-sequence":
						receipt.Sequence++
					case "receipt-op":
						receipt.Op = c.Stage
					case "receipt-digest":
						receipt.RequestDigest[0]++
					}
					encoded, err := c.EncodeReceipt(receipt)
					if err != nil {
						result <- err
						return
					}
					purpose := ReceiptPurpose
					if scenario == "receipt-purpose" {
						purpose = RequestPurpose
					}
					if err := setWriteDeadline(accepted, dl); err != nil {
						result <- err
						return
					}
					err = writeFrame(accepted, purpose, encoded)
					if err == nil {
						err = accepted.CloseWrite()
					}
					result <- err
				}()
			}
			receipt, err := client.Do(request, dl, stop)
			if scenario == "success" {
				if err != nil || receipt.RequestDigest == (c.Digest{}) {
					t.Fatalf("client exchange: %v", err)
				}
			} else if err == nil {
				t.Fatal("client accepted unbound receipt/peer/cancellation")
			}
			if scenario == "denied-policy" && (!errors.Is(err, ErrPermission) || host.calls() != 0) {
				t.Fatalf("before-effect denial lost its permission class: %v", err)
			}
			if scenario == "stale-policy" && (!errors.Is(err, ErrPolicyStale) || host.calls() != 0) {
				t.Fatalf("revoked policy lost its permission class: %v", err)
			}
			if (scenario == "lost-effect-reply" || strings.HasPrefix(scenario, "refusal-") || strings.HasPrefix(scenario, "receipt-")) && !errors.Is(err, ErrResultUnknown) {
				t.Fatalf("after-send uncertainty misclassified: %v", err)
			}
			if scenario == "lost-effect-reply" && host.calls() != 1 {
				t.Fatal("unknown result lacks real host invocation")
			}
			if scenario != "stop" {
				_ = awaitServe(t, result)
			}
		})
	}
}

type testFileExchange struct {
	accept func(c.HostRequest, c.Peer, []*os.File, c.Deadline, c.Stop) error
	reply  func(c.HostRequest, c.HostReceipt, c.Peer, c.Deadline, c.Stop) ([]*os.File, error)
}

func (x testFileExchange) Accept(r c.HostRequest, p c.Peer, files []*os.File, d c.Deadline, s c.Stop) error {
	return x.accept(r, p, files, d, s)
}
func (x testFileExchange) Reply(r c.HostRequest, receipt c.HostReceipt, p c.Peer, d c.Deadline, s c.Stop) ([]*os.File, error) {
	return x.reply(r, receipt, p, d, s)
}

// origin: hot-update S9 plan §3.1 / §4.1 / §6.2 / T08
func exerciseFileExchange(t *testing.T) {
	t.Helper()
	if rules := FileRules(); len(rules) != 2 || rules[0] != (FileRule{c.Stage, 1, 0}) || rules[1] != (FileRule{c.LaunchClaim, 0, 5}) {
		t.Fatal("FD operation whitelist changed")
	}
	for _, op := range []c.HostOp{c.Inspect, c.RotatePolicy, c.LaunchAck, c.SignalStop} {
		if rule := fileRule(op); rule.MaxInput != 0 || rule.Output != 0 {
			t.Fatal("unapproved FD operation admitted")
		}
	}
	for _, op := range []c.HostOp{c.Stage, c.LaunchClaim} {
		server, _, limit, host := ipcFixture(t)
		public, private := testKey(t)
		scope, keyID := LocalAuthority, "local"
		if op == c.LaunchClaim {
			scope, keyID = LaunchAuthority, "launch"
		}
		for i := range server.policy.current.Keys {
			if server.policy.current.Keys[i].Scope == scope {
				server.policy.current.Keys[i].PublicKey = public
			}
		}
		request := requestFixture(op)
		request.HostBootID = server.policy.current.HostBootID
		request.Expected.BootID = request.HostBootID
		request = signedRequest(t, request, keyID, private)
		directory, err := os.MkdirTemp("", "infra-fd-")
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(directory, "control.sock")
		listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		source, err := os.Create(filepath.Join(directory, "source"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := source.Write([]byte("fixed input bytes")); err != nil {
			t.Fatal(err)
		}
		permit, err := os.Open(source.Name())
		if err != nil {
			t.Fatal(err)
		}
		if err := validateReceivedFile(source, true); err == nil {
			t.Fatal("writable startup permit admitted")
		}
		if err := unix.Flock(int(source.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			t.Fatal(err)
		}
		store, err := os.Open(directory)
		if err != nil {
			t.Fatal(err)
		}
		var receivedInput *os.File
		server.files = testFileExchange{accept: func(r c.HostRequest, peer c.Peer, files []*os.File, _ c.Deadline, _ c.Stop) error {
			if peer.PID != int32(os.Getpid()) || peer.UID != uint32(os.Geteuid()) {
				return ErrPermission
			}
			if r.Op == c.Stage {
				if len(files) != 1 {
					return ErrPermission
				}
				receivedInput = files[0]
				flags, err := unix.FcntlInt(files[0].Fd(), unix.F_GETFD, 0)
				if err != nil || flags&unix.FD_CLOEXEC == 0 {
					return ErrPermission
				}
				data := make([]byte, len("fixed input bytes"))
				if _, err := files[0].ReadAt(data, 0); err != nil || string(data) != "fixed input bytes" {
					return ErrPermission
				}
			}
			return nil
		}, reply: func(c.HostRequest, c.HostReceipt, c.Peer, c.Deadline, c.Stop) ([]*os.File, error) {
			return []*os.File{permit, permit, store, store, source}, nil
		}}
		dl := testDeadline(t, time.Second)
		result := make(chan error, 1)
		go func() {
			accepted, err := listener.AcceptUnix()
			if err != nil {
				result <- err
				return
			}
			result <- server.ServeConn(accepted, dl, nil)
		}()
		client := &Client{path: path, serverUID: uint32(os.Geteuid())}
		var inputs []*os.File
		if op == c.Stage {
			inputs = []*os.File{source}
		}
		_, outputs, exchangeErr := client.Exchange(request, inputs, dl, nil)
		serverErr := awaitServe(t, result)
		if exchangeErr != nil || serverErr != nil || host.calls() != 1 {
			closeFiles(outputs)
			t.Fatalf("approved FD exchange failed: client=%v server=%v", exchangeErr, serverErr)
		}
		if op == c.Stage {
			if _, err := receivedInput.Stat(); err == nil {
				t.Fatal("server retained borrowed input after reply")
			}
			if len(outputs) != 0 {
				t.Fatal("Stage returned unapproved descriptors")
			}
		}
		if op == c.LaunchClaim {
			if len(outputs) != 5 {
				t.Fatal("claim did not return executable, config, directories and lock")
			}
			for _, file := range outputs {
				flags, err := unix.FcntlInt(file.Fd(), unix.F_GETFD, 0)
				if err != nil || flags&unix.FD_CLOEXEC == 0 {
					t.Fatal("received loan is not CLOEXEC")
				}
			}
			_ = source.Close()
			contender, err := os.OpenFile(filepath.Join(directory, "source"), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB); !errors.Is(err, unix.EWOULDBLOCK) {
				t.Fatalf("sender death lost shared lock OFD: %v", err)
			}
			closeFiles(outputs)
			if err := unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
				t.Fatalf("last loan reference retained lock: %v", err)
			}
			_ = contender.Close()
		}
		_ = source.Close()
		_ = permit.Close()
		_ = store.Close()
		_ = listener.Close()
		_ = limit.Close()
		_ = os.RemoveAll(directory)
	}
}
