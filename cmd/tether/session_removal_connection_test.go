package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// This signs the real process/CONNECT boundary separately from the production
// settlement/history test: the rm responder below witnesses handler reach, not
// a substitute terminal-history implementation.
// origin: S7 internal review rv-n1compat F4
func TestSessionRemovalBinaryReconnectsWithRestrictedScope(t *testing.T) {
	// origin: hot-update S10 plan L17 / §11.1 / POLISH-B
	if runIsolatedBinaryTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	binary := filepath.Join(t.TempDir(), "tether-removal-test")
	build := exec.CommandContext(t.Context(), "go", "build", "-race", "-o", binary, ".")
	var output bytes.Buffer
	build.Stdout, build.Stderr = &output, &output
	if err := testharness.RunProcess(build); err != nil {
		t.Fatalf("build: %v %s", err, output.String())
	}
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseSessionRemovalReconnect(t, binary) })
	}
	testharness.AssertNoGoroutineLeak(t, "session removal reconnect", g)
	testharness.AssertNoFDLeak(t, "session removal reconnect", fd, 0)
}

func exerciseSessionRemovalReconnect(t *testing.T, binary string) {
	for _, legacy := range []bool{false, true} {
		name := "current"
		if legacy {
			name = "legacy_subscription_surface"
		}
		t.Run(name, func(t *testing.T) {
			db := testharness.OpenDB(t)
			home := t.TempDir()
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state) VALUES('lab','lab',?,'unused','ACTIVE')`, id.Fingerprint); err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(`INSERT INTO members(sid,pubkey_fp,role,via) VALUES('lab',?,'owner','create')`, id.Fingerprint); err != nil {
				t.Fatal(err)
			}
			account, _ := nkeys.CreateAccount()
			defer account.Wipe()
			issuer, _ := account.PublicKey()
			service, _ := nkeys.CreateUser()
			defer service.Wipe()
			servicePub, _ := service.PublicKey()
			signer, _ := nkeys.CreateServer()
			defer signer.Wipe()
			s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, AlwaysEnableNonce: true,
				Nkeys: []*server.NkeyUser{{Nkey: servicePub}}, AuthCallout: &server.AuthCallout{Issuer: issuer, Account: "$G", AuthUsers: []string{servicePub}}})
			if err != nil {
				t.Fatal(err)
			}
			s.Start()
			defer func() { s.Shutdown(); s.WaitForShutdown() }()
			if !s.ReadyForConnections(time.Second) {
				t.Fatal("NATS not ready")
			}
			nc, err := nats.Connect(s.ClientURL(), nats.Nkey(servicePub, service.Sign))
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			h := &authcallout.Handler{DB: db, AccountKp: account, Now: time.Now, Logger: testharness.SilentLog(), JWTTTL: time.Hour}
			var connections, requests atomic.Int32
			_, err = nc.Subscribe("$SYS.REQ.USER.AUTH", func(m *nats.Msg) {
				request, err := jwt.DecodeAuthorizationRequestClaims(string(m.Data))
				if err != nil {
					t.Error(err)
					return
				}
				connections.Add(1)
				wire := string(m.Data)
				if legacy {
					// v0.6.0 ignores Token for an already joined member. Strip only the
					// new intent before exercising the unchanged ordinary-member branch.
					request.ConnectOptions.Token = ""
					wire, err = request.Encode(signer)
					if err != nil {
						t.Error(err)
						return
					}
				}
				reply, err := h.Handle(wire)
				if err != nil {
					t.Error(err)
					return
				}
				decoded, err := jwt.DecodeAuthorizationResponseClaims(reply)
				if err != nil {
					t.Error(err)
					return
				}
				if !legacy && decoded.Error == "" {
					claims, err := jwt.DecodeUserClaims(decoded.Jwt)
					if err != nil {
						t.Error(err)
						return
					}
					got, _ := json.Marshal(claims.Permissions)
					want, _ := json.Marshal(auth.PermissionsForSessionRemoval(id.PublicKey, "lab"))
					if !bytes.Equal(got, want) {
						t.Errorf("command regained business permissions: %s", got)
					}
				}
				_ = m.Respond([]byte(reply))
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = nc.Subscribe(proto.SubjCtrlSessionRm(id.PublicKey, "lab"), func(m *nats.Msg) {
				requests.Add(1)
				var req proto.SessionRmReq
				if err := json.Unmarshal(m.Data, &req); err != nil {
					t.Error(err)
					return
				}
				response := proto.SessionRmResp{Code: "session_not_found_or_deleting", Error: "pending transfer original (commit_reserved)"}
				if legacy {
					response = proto.SessionRmResp{OK: true}
					if requests.Load() > 1 {
						response = proto.SessionRmResp{Code: "already_deleting", Error: "already deleting"}
					}
				} else {
					if _, err := db.Exec(`UPDATE sessions SET state='DELETING' WHERE sid='lab'`); err != nil {
						t.Error(err)
						return
					}
					if req.AbandonUnsettledTransfers {
						response = proto.SessionRmResp{OK: true}
					}
				}
				body, _ := json.Marshal(response)
				_ = m.Respond(body)
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = nc.Flush(); err != nil {
				t.Fatal(err)
			}
			run := func(want int, extra ...string) {
				args := append([]string{"session", "rm", "lab", "--home", home, "--nats-url", s.ClientURL(), "--ack-alerts"}, extra...)
				command := exec.CommandContext(t.Context(), binary, args...)
				// All user state is private to this test. Inherited developer bypasses
				// must not make the real authentication counterexample vacuous.
				for _, v := range os.Environ() {
					if !strings.HasPrefix(v, cli.DevNoAuthEnv+"=") {
						command.Env = append(command.Env, v)
					}
				}
				var b bytes.Buffer
				command.Stdout, command.Stderr = &b, &b
				err := testharness.RunProcess(command)
				rc := 0
				if err != nil {
					var exit *exec.ExitError
					if !errors.As(err, &exit) {
						t.Fatal(err)
					}
					rc = exit.ExitCode()
				}
				if rc != want {
					t.Fatalf("rm process exit=%d want=%d: %s", rc, want, b.String())
				}
			}
			if legacy {
				run(0)
				run(64)
			} else {
				run(75)
				run(75)
				run(0, "--abandon-unsettled-transfers")
			}
			want := int32(3)
			if legacy {
				want = 2
			}
			if requests.Load() != want || connections.Load() < want {
				t.Fatalf("fresh processes failed to reach removal: requests=%d connects=%d", requests.Load(), connections.Load())
			}
		})
	}
}
