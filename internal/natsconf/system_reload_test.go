package natsconf

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// origin: hot-update S9 plan §3.3 / T27
func TestSystemPrincipalReloadPreservesGlobalAccount(t *testing.T) {
	exerciseSystemPrincipal(t)
}

// origin: hot-update S9 plan §12 P12 / T27/T28
func TestSystemPrincipalLifecycle(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSystemPrincipal(t)
	}
	testharness.AssertNoGoroutineLeak(t, "system principal reload", goroutines)
	// A fully joined embedded server has no retained listener, client or file-store FD.
	testharness.AssertNoFDLeak(t, "system principal reload", fds, 0)
}

func systemTestKey(t *testing.T) (nkeys.KeyPair, string) {
	t.Helper()
	key, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(key.Wipe)
	pub, err := key.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	return key, pub
}

func systemTestWrite(t *testing.T, path, text string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

func startSystemTestServer(t *testing.T, path string) *natsserver.Server {
	t.Helper()
	opts, err := natsserver.ProcessConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	opts.NoLog, opts.NoSigs = true, true
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded system-principal server did not start")
	}
	return ns
}

func systemTestConnect(t *testing.T, ns *natsserver.Server, key nkeys.KeyPair, prefix string, handler nats.ErrHandler) *nats.Conn {
	t.Helper()
	pub, err := key.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	opts := []nats.Option{nats.Nkey(pub, key.Sign), nats.NoReconnect(), nats.Timeout(time.Second)}
	if prefix != "" {
		opts = append(opts, nats.CustomInboxPrefix(prefix))
	}
	if handler != nil {
		opts = append(opts, nats.ErrorHandler(handler))
	}
	nc, err := nats.Connect(ns.ClientURL(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func exerciseSystemPrincipal(t *testing.T) {
	t.Helper()
	// Per-round cleanup runs before the leak assertion, even on a fatal test failure.
	t.Run("reload", func(t *testing.T) {
		dir := t.TempDir()
		brokerKey, brokerPub := systemTestKey(t)
		readKey, readPub := systemTestKey(t)
		_, stepPub := systemTestKey(t)
		accountKey, err := nkeys.CreateAccount()
		if err != nil {
			t.Fatal(err)
		}
		issuer, err := accountKey.PublicKey()
		t.Cleanup(accountKey.Wipe)
		if err != nil {
			t.Fatal(err)
		}
		peer := Broker{ServerName: "principal-single", NkeyPub: brokerPub, RouteURL: "nats://127.0.0.1:6222"}
		cfg := Config{Local: peer, Peers: []Broker{peer}, AccountIssuer: issuer,
			JSStoreDir: filepath.Join(dir, "store"), ClientListen: "127.0.0.1:-1", Standalone: true}
		old, err := Render(cfg)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "nats.conf")
		systemTestWrite(t, path, old)
		ns := startSystemTestServer(t, path)
		global, err := ns.LookupAccount("$G")
		if err != nil {
			t.Fatal(err)
		}
		system := ns.SystemAccount()
		if system == nil || system.Name != "$SYS" {
			t.Fatal("legacy fixture must have the existing default system account")
		}
		before, err := ns.Varz(nil)
		if err != nil {
			t.Fatal(err)
		}
		nc := systemTestConnect(t, ns, brokerKey, "", nil)
		// Use the real product decision engine on its normal queue, so an absent
		// responder cannot masquerade as a SYS migration defect. Its SYS seam reads
		// the managed users from the same config file the server reloads: before
		// provisioning the key is an ordinary (here unknown) identity, afterwards it
		// is admitted into $SYS with exactly the rendered descriptor authority.
		// origin: hot-update S9 plan §3.3 / P12 — normal-queue SYS admission
		handler := &authcallout.Handler{AccountKp: accountKey, Now: time.Now,
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), JWTTTL: 24 * time.Hour,
			SystemPrincipals: func() []authcallout.SystemPrincipal { return loadedSystemPrincipals(t, path) }}
		var callouts atomic.Int32
		if _, err := nc.QueueSubscribe("$SYS.REQ.USER.AUTH", "tether-authcallout", func(msg *nats.Msg) {
			reply, err := handler.Handle(string(msg.Data))
			if err != nil {
				t.Errorf("normal auth decision failed: %v", err)
				return
			}
			// Count the completed decision before releasing its reply; otherwise a fast
			// CONNECT/PONG could outrun the callback's counter on another goroutine.
			callouts.Add(1)
			if err := msg.Respond([]byte(reply)); err != nil {
				t.Errorf("normal auth response failed: %v", err)
				return
			}
		}); err != nil {
			t.Fatal(err)
		}
		if err := nc.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		ordinaryKey, ordinaryPub := systemTestKey(t)
		assertOrdinarySystemTestAuth(t, ns, ordinaryKey, ordinaryPub)
		if callouts.Load() != 1 {
			t.Fatal("normal auth queue did not authenticate the ordinary identity")
		}
		cid, err := nc.GetClientID()
		if err != nil {
			t.Fatal(err)
		}
		js, err := nc.JetStream()
		if err != nil {
			t.Fatal(err)
		}
		stream, err := js.AddStream(&nats.StreamConfig{Name: "ORIGINAL", Subjects: []string{proto.SubjectPrefix + ".s.original.audit.event"}, Storage: nats.FileStorage})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := js.Publish(proto.SubjectPrefix+".s.original.audit.event", []byte("before")); err != nil {
			t.Fatal(err)
		}
		sub, err := nc.SubscribeSync(proto.SubjClusterPrefix + ".business")
		if err != nil {
			t.Fatal(err)
		}
		if err := nc.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		own, err := Preflight(path)
		if err != nil {
			t.Fatal(err)
		}
		principals := SystemPrincipals{ReadNkey: readPub, StepNkey: stepPub}
		provisioned, err := WithSystemPrincipals(own, principals)
		if err != nil {
			t.Fatal(err)
		}
		if _, present := own.Parsed["accounts"]; present {
			t.Fatal("provisioning mutated the legacy ownership view")
		}
		merged, err := BuildMergedConf(provisioned, cfg)
		if err != nil {
			t.Fatal(err)
		}
		// The only addition to the old single config is SYS authority. No monitor, account
		// option, store path, top-level authorization or restart-bearing topology change.
		block, err := SystemPrincipalBlock(principals)
		if err != nil {
			t.Fatal(err)
		}
		if merged != old+"\n"+block {
			t.Fatal("SYS provisioning changed the original global-account configuration")
		}
		if err := Apply(path, merged); err != nil {
			t.Fatal(err)
		}
		if err := ns.Reload(); err != nil {
			t.Fatalf("adding users to the existing SYS account must reload: %v", err)
		}
		after, err := ns.Varz(nil)
		if err != nil {
			t.Fatal(err)
		}
		currentGlobal, err := ns.LookupAccount("$G")
		if err != nil {
			t.Fatal(err)
		}
		currentCID, err := nc.GetClientID()
		if err != nil {
			t.Fatal(err)
		}
		if ns.ID() != before.ID || !after.Start.Equal(before.Start) || currentCID != cid || nc.IsClosed() || nc.Stats().Reconnects != 0 {
			t.Fatal("reload restarted the server or replaced the original business connection")
		}
		if global != currentGlobal || ns.SystemAccount() != system || after.HTTPPort != 0 {
			t.Fatal("reload moved the global/system account or added an HTTP monitor")
		}
		info, err := js.StreamInfo("ORIGINAL")
		if err != nil || !info.Created.Equal(stream.Created) || info.State.Msgs != 1 {
			t.Fatalf("reload lost the original nonempty global stream: info=%+v err=%v", info, err)
		}
		msg, err := js.GetMsg("ORIGINAL", 1)
		if err != nil || !bytes.Equal(msg.Data, []byte("before")) {
			t.Fatalf("reload lost the original ACK prefix: %v", err)
		}
		if _, err := js.Publish(proto.SubjectPrefix+".s.original.audit.event", []byte("after")); err != nil {
			t.Fatal(err)
		}
		if err := nc.Publish(proto.SubjClusterPrefix+".business", []byte("same socket")); err != nil {
			t.Fatal(err)
		}
		business, err := sub.NextMsg(time.Second)
		if err != nil || string(business.Data) != "same socket" {
			t.Fatalf("original subscription did not survive reload: %v", err)
		}
		assertOrdinarySystemTestAuth(t, ns, ordinaryKey, ordinaryPub)
		if callouts.Load() != 2 {
			t.Fatal("reload replaced the normal auth queue or its ordinary authority")
		}
		// A fresh exempt broker must still authenticate using the old $G identity.
		fresh := systemTestConnect(t, ns, brokerKey, "", nil)
		if err := fresh.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		in := Inputs{SelfServerName: peer.ServerName, Peers: []Broker{peer}, AccountIssuer: issuer, ConfPath: path, DesiredGen: 2}
		t.Run("roundtrip", func(t *testing.T) {
			readBack, err := Preflight(path)
			if err != nil {
				t.Fatal(err)
			}
			identity, err := SystemPrincipalIdentity(readBack)
			if err != nil || identity != principals {
				t.Fatalf("parsed SYS identity changed: %+v %v", identity, err)
			}
			desired, err := RenderDesired(in, readBack, RenderOverride{Intent: IntentPreserve})
			if err != nil || desired != merged {
				t.Fatalf("reconcile render erased SYS authority: %v", err)
			}
		})
		t.Run("reconcile observation", func(t *testing.T) {
			// The current-conf branch exercises the real reload+probe seam without spawning
			// a validator; two-version DryRun/signal supervision belongs to P27/P28.
			reloads := 0
			out := ReconcileOnce(in, 1, 1, func() error { reloads++; return ns.Reload() }, nil)
			if out.Action != ActionSwappedReloadPending || out.ObservedGen != 1 || reloads != 1 {
				t.Fatalf("reload return alone must not synthesize observation: %+v reloads=%d", out, reloads)
			}
			out = ReconcileOnce(in, 2, 1, func() error { reloads++; return ns.Reload() }, func() (time.Time, error) { return ns.ConfigTime(), nil })
			if out.Action != ActionNoop || out.ObservedGen != 2 || reloads != 1 {
				t.Fatalf("real loaded config must converge without another reload: %+v reloads=%d", out, reloads)
			}
		})
		t.Run("SYS usable after reload", func(t *testing.T) {
			before := callouts.Load()
			t.Cleanup(func() {
				if callouts.Load() != before+1 {
					t.Error("SYS CONNECT did not reach the real normal auth queue")
				}
			})
			descriptors, err := SystemPrincipalDescriptors(principals)
			if err != nil {
				t.Fatal(err)
			}
			// The callout re-checks that every reply grant stays under this root;
			// the renderer and the admission leaf must agree on it exactly.
			for _, descriptor := range descriptors {
				if descriptor.ReplyPrefix != authcallout.SystemInboxRoot+"."+descriptor.Nkey {
					t.Fatalf("renderer and callout disagree on the SYS reply root: %q", descriptor.ReplyPrefix)
				}
			}
			reader := systemTestConnect(t, ns, readKey, descriptors[0].ReplyPrefix, nil)
			assertSystemReadRequests(t, reader, ns)
			reader.Close()
		})
		fresh.Close()
		nc.Close()
		ns.Shutdown()
		ns.WaitForShutdown()
		if _, err := os.Stat(filepath.Join(dir, "store", "jetstream")); err != nil {
			t.Fatalf("original store layout changed: %v", err)
		}
	})
}

func assertSystemReadRequests(t *testing.T, nc *nats.Conn, ns *natsserver.Server) {
	t.Helper()
	for _, subject := range []string{fmt.Sprintf("$SYS.REQ.SERVER.%s.JSZ", ns.ID()), fmt.Sprintf("$SYS.REQ.SERVER.%s.VARZ", ns.ID()), "$SYS.REQ.ACCOUNT.$G.INFO"} {
		msg, err := nc.Request(subject, []byte("{}"), 2*time.Second)
		if err != nil {
			t.Fatalf("managed read authority cannot request %s: %v", subject, err)
		}
		if len(msg.Data) == 0 || !bytes.Contains(msg.Data, []byte("\"data\"")) || bytes.Contains(msg.Data, []byte("\"error\"")) {
			t.Fatalf("managed SYS request returned no successful server data for %s: %s", subject, msg.Data)
		}
	}
}

// loadedSystemPrincipals is the fixture's binding of the callout seam: the managed
// $SYS users of the config file on disk, rendered by the product descriptors. A
// config without the block yields none, so SYS admission cannot precede provisioning.
func loadedSystemPrincipals(t *testing.T, path string) []authcallout.SystemPrincipal {
	descriptors, err := LoadedSystemPrincipalDescriptors(path)
	if err != nil {
		t.Errorf("SYS seam cannot read the loaded config: %v", err)
		return nil
	}
	principals := make([]authcallout.SystemPrincipal, 0, len(descriptors))
	for _, descriptor := range descriptors {
		principals = append(principals, authcallout.SystemPrincipal{Nkey: descriptor.Nkey, Publish: descriptor.Publish, Subscribe: descriptor.Subscribe})
	}
	return principals
}

func assertOrdinarySystemTestAuth(t *testing.T, ns *natsserver.Server, key nkeys.KeyPair, pub string) {
	t.Helper()
	nc, err := nats.Connect(ns.ClientURL(), nats.Nkey(pub, key.Sign), nats.Name("tether-cli"), nats.NoReconnect(), nats.Timeout(time.Second))
	if err != nil {
		t.Fatalf("normal auth handler cannot authenticate an ordinary client: %v", err)
	}
	defer nc.Close()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
}
