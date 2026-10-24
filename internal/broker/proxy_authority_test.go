package broker

// Real: SQLite WAL snapshots, concurrent committed revocation and NATS pushes.
// Instrumented: a driver wrapper inserts a commit/error at an exact read boundary.
// Not covered: agent digest-conflict handling (P15), or tunnel authorization (P12).

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"modernc.org/sqlite"
)

// origin: hot-update S5 plan §6.4 / T28. The writer commits while the reader
// still owns its cut; a split transaction must produce a mixed grant and fail.
func TestProxyAuthoritySnapshotNeverMixesKeysAndEpoch(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	before := runtime.NumGoroutine()
	beforeFD := testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			exerciseProxyAuthoritySnapshot(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "proxy authority snapshots", before)
	testharness.AssertNoFDLeak(t, "proxy authority snapshots", beforeFD, 2)
}

func exerciseProxyAuthoritySnapshot(t *testing.T) {
	t.Helper()
	for _, boundary := range []string{"SELECT proxy_epoch", "FROM port_allocations", "FROM proxy_subscribers"} {
		for _, change := range []string{"revoke", "disable", "free", "generation"} {
			t.Run(boundary+"/"+change, func(t *testing.T) {
				f := newProxyAuthorityFixture(t)
				before, err := proxyAuthoritySnapshot(f.b, "lab", "node")
				if err != nil || !before.Enabled || before.Allocation == nil || len(before.Keys) != 2 {
					t.Fatal("snapshot premise missing")
				}
				f.hook.arm(boundary, func() error {
					tx, err := f.writer.Begin()
					if err != nil {
						return err
					}
					defer func() { _ = tx.Rollback() }()
					var query string
					switch change {
					case "revoke":
						query = `UPDATE proxy_subscribers SET state='REVOKED' WHERE name='alice'`
					case "disable":
						query = `UPDATE sessions SET proxy_enabled=0 WHERE sid='lab'`
					case "free":
						query = `UPDATE port_allocations SET state='FREED' WHERE nid='node'`
					case "generation":
						query = `UPDATE proxy_meta SET generation=generation+1 WHERE id=1`
					default:
						return errors.New("unknown fixture change")
					}
					if _, err := tx.Exec(query); err != nil {
						return err
					}
					if _, err := tx.Exec(`UPDATE sessions SET proxy_epoch=proxy_epoch+1 WHERE sid='lab'`); err != nil {
						return err
					}
					return tx.Commit()
				})
				// Execute the real register keep-token path, including its builder.
				d := f.b.proxyDirectiveForRegister("lab", "node", f.registerReq())
				if !f.hook.fired() {
					t.Fatal("commit boundary was not exercised")
				}
				if d == nil || !d.Enabled || d.Epoch != before.Epoch || d.Generation != before.Generation ||
					d.PublicPort != before.Allocation.Port || !reflect.DeepEqual(d.Keys, before.Keys) {
					t.Fatal("register mixed authority across a committed mutation")
				}
				after, err := proxyAuthoritySnapshot(f.b, "lab", "node")
				if err != nil || after.Epoch != before.Epoch+1 {
					t.Fatal("writer commit was not visible to the next snapshot")
				}
				switch change {
				case "revoke":
					if len(after.Keys) != 1 || after.Keys[0].SubID == before.Keys[0].SubID {
						t.Fatal("revoked key survived in fresh authority")
					}
				case "disable":
					if after.Enabled {
						t.Fatal("fresh snapshot missed OFF")
					}
				case "free":
					if after.Allocation != nil {
						t.Fatal("fresh snapshot retained freed allocation")
					}
				case "generation":
					if after.Generation != before.Generation+1 {
						t.Fatal("fresh snapshot missed durable generation")
					}
				}
			})
		}
	}

	// Every SQL read is individually fallible, including the epoch read whose
	// error the old implementation ignored. Failed register must never mint.
	for _, query := range []string{"SELECT proxy_enabled", "SELECT proxy_capable", "SELECT proxy_epoch", "SELECT generation", "SELECT value", "FROM port_allocations", "FROM proxy_subscribers"} {
		t.Run("read-error/"+query, func(t *testing.T) {
			f := newProxyAuthorityFixture(t)
			f.hook.arm(query, func() error { return errors.New("injected SQL read failure") })
			if f.b.proxyDirectiveForRegister("lab", "node", f.registerReq()) != nil || !f.hook.fired() {
				t.Fatal("register did not reject the injected authority read error")
			}
			f.hook.arm(query, func() error { return errors.New("injected SQL read failure") })
			if d, err := proxyDirectiveForPush(f.b, "lab", "node", nil); err == nil || d != nil || !f.hook.fired() {
				t.Fatal("push did not reject the injected authority read error")
			}
		})
	}
	for _, query := range []string{
		`UPDATE sessions SET proxy_epoch=-1`,
		`UPDATE sessions SET proxy_epoch='9223372036854775808'`,
		`UPDATE proxy_meta SET generation=-1`,
		`UPDATE proxy_meta SET generation=4611686018427387904`,
		`UPDATE port_allocations SET epoch=-1`,
		`INSERT INTO cluster_meta(key,value) VALUES('applied_index','-1')`,
		`UPDATE sessions SET state='DELETING'`,
	} {
		t.Run("invalid/"+query, func(t *testing.T) {
			f := newProxyAuthorityFixture(t)
			if _, err := f.writer.Exec(query); err != nil {
				t.Fatal(err)
			}
			if a, err := proxyAuthoritySnapshot(f.b, "lab", "node"); err == nil || a.Allocation != nil || len(a.Keys) != 0 {
				t.Fatal("invalid authority was not rejected as a whole")
			}
		})
	}
	t.Run("same-pair-push", func(t *testing.T) { exerciseProxyAuthorityPush(t) })
	t.Run("home-pins-share-keyset-cut", func(t *testing.T) {
		f := newProxyAuthorityFixture(t)
		if _, err := f.writer.Exec(`INSERT INTO cluster_nodes
			(node_id,name,node_ident_pub,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at)
			VALUES('home','home','pub','raft','route','host:7777','host','old-pin','VOTER',?)`, time.Now()); err != nil {
			t.Fatal(err)
		}
		if _, err := f.writer.Exec(`UPDATE port_allocations SET home_broker='home',epoch=7 WHERE nid='node'`); err != nil {
			t.Fatal(err)
		}
		f.b.selfID = "home"
		before := f.b.proxyDirectiveForRegister("lab", "node", f.registerReq())
		if before == nil || before.Home == nil || before.Home.CertPins.Current != "old-pin" {
			t.Fatal("home snapshot premise missing")
		}
		f.hook.arm("FROM cluster_nodes", func() error {
			tx, err := f.writer.Begin()
			if err != nil {
				return err
			}
			defer func() { _ = tx.Rollback() }()
			for _, query := range []string{
				`UPDATE cluster_nodes SET cert_fp='new-pin' WHERE node_id='home'`,
				`UPDATE proxy_subscribers SET state='REVOKED' WHERE name='alice'`,
				`UPDATE sessions SET proxy_epoch=proxy_epoch+1 WHERE sid='lab'`,
			} {
				if _, err := tx.Exec(query); err != nil {
					return err
				}
			}
			return tx.Commit()
		})
		d := f.b.proxyDirectiveForRegister("lab", "node", f.registerReq())
		if !f.hook.fired() || d == nil || !reflect.DeepEqual(d, before) {
			t.Fatal("home pins mixed with another allocation/keyset cut")
		}
		d = f.b.proxyDirectiveForRegister("lab", "node", f.registerReq())
		if d == nil || d.Home == nil || d.Home.CertPins.Current != "new-pin" || d.Epoch != before.Epoch+1 || len(d.Keys) != 1 {
			t.Fatal("fresh home/keyset snapshot missed committed rotation")
		}
		f.hook.arm("FROM cluster_nodes", func() error { return errors.New("injected home read error") })
		if f.b.proxyDirectiveForRegister("lab", "node", f.registerReq()) != nil || !f.hook.fired() {
			t.Fatal("home read error returned a partial directive")
		}
		for _, query := range []string{
			`UPDATE cluster_nodes SET phase='DRAINING' WHERE node_id='home'`,
			`UPDATE cluster_nodes SET phase='VOTER',cert_fp='' WHERE node_id='home'`,
		} {
			if _, err := f.writer.Exec(query); err != nil {
				t.Fatal(err)
			}
			if f.b.proxyDirectiveForRegister("lab", "node", f.registerReq()) != nil {
				t.Fatal("undeliverable home returned an enabled directive")
			}
		}
	})
	t.Run("cluster-revision-and-unavailable-runtime", func(t *testing.T) {
		f := newProxyAuthorityFixture(t)
		if _, err := f.writer.Exec(`INSERT INTO cluster_meta(key,value) VALUES('applied_index','42')`); err != nil {
			t.Fatal(err)
		}
		tx, err := f.b.cfg.DB.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		a, readErr := readProxyAuthorityTx(t.Context(), tx, "lab", "node", true)
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if readErr != nil || !a.Enabled || a.Generation != 0 || a.AppliedRevision != 42 || len(a.Keys) != 2 {
			t.Fatal("cluster snapshot lost epoch-only ordering or SQL command revision")
		}
		f.b.clusterMode = true
		if a, err := proxyAuthoritySnapshot(f.b, "lab", "node"); err == nil || a.Allocation != nil || len(a.Keys) != 0 {
			t.Fatal("positive cluster rows bypassed unavailable-runtime refusal")
		}
	})
	t.Run("mint-rechecks-authority", func(t *testing.T) {
		f := newProxyAuthorityFixture(t)
		// The first pre-allocation snapshot is valid. Fail its successor at
		// the epoch read, proving minted tokens cannot reuse that first cut.
		f.hook.arm("INSERT INTO port_allocations", func() error {
			f.hook.arm("SELECT proxy_epoch", func() error { return errors.New("post-mint epoch failure") })
			return nil
		})
		if f.b.proxyDirectiveForRegister("lab", "node", proto.NodeRegisterReq{Capabilities: []string{proto.CapProxyV1}}) != nil {
			t.Fatal("mint reused pre-allocation authority after an epoch failure")
		}
		if !f.hook.fired() {
			t.Fatal("post-mint read was not exercised")
		}
	})
}

func exerciseProxyAuthorityPush(t *testing.T) {
	t.Helper()
	f := newProxyAuthorityFixture(t)
	d := f.b.proxyDirectiveForRegister("lab", "node", f.registerReq())
	if d == nil || len(d.Keys) != 2 {
		t.Fatal("register keyset premise missing")
	}
	pending := *d
	pending.Keys = []proto.ProxyKey{{SubID: "conflicting-sub", Secret: "conflicting-material"}}
	current, err := proxyDirectiveForPush(f.b, "lab", "node", &pending)
	if err != nil || current.Generation != d.Generation || current.Epoch != d.Epoch || !reflect.DeepEqual(current.Keys, d.Keys) {
		t.Fatal("same-pair push retained producer keys instead of full snapshot")
	}
	// Test the public delivery method too, using a real NATS subscription.
	nc, err := nats.Connect(startNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	sub, err := nc.SubscribeSync(proto.SubjCmdForwarded("lab", "node", "proxy-keys"))
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	f.b.pushProxyDirective(nc, "lab", "node", &pending)
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var delivered proto.ProxyDirective
	if json.Unmarshal(msg.Data, &delivered) != nil || !reflect.DeepEqual(delivered, *current) {
		t.Fatal("NATS push did not deliver the current complete authority")
	}
	if _, err := f.b.revokeSubAndBump("lab", "alice"); err != nil {
		t.Fatal(err)
	}
	current, err = proxyDirectiveForPush(f.b, "lab", "node", &pending)
	if err != nil || current.Epoch != pending.Epoch+1 || len(current.Keys) != 1 {
		t.Fatal("delayed push mixed old keys with the new epoch")
	}
	// A valid once-only token can be retained, but only for its own allocation.
	pending.Token = f.alloc.Token
	if current, err = proxyDirectiveForPush(f.b, "lab", "node", &pending); err != nil || current.Token != f.alloc.Token {
		t.Fatal("matching minted token was withheld")
	}
	pending.Token = "different-allocation-token"
	if current, err = proxyDirectiveForPush(f.b, "lab", "node", &pending); err == nil || current != nil {
		t.Fatal("stale raw token crossed allocation authority")
	}
	if _, err := session.SetProxyEnabledAndBumpEpoch(f.writer, "lab", false); err != nil {
		t.Fatal(err)
	}
	current, err = proxyDirectiveForPush(f.b, "lab", "node", &pending)
	if err != nil || current.Enabled || len(current.Keys) != 0 || current.Token != "" || current.PublicPort != 0 {
		t.Fatal("delayed enabled push escaped current OFF authority")
	}
	if _, err := session.SetProxyEnabledAndBumpEpoch(f.writer, "lab", true); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`UPDATE nodes SET proxy_capable=0 WHERE nid='node'`,
		`UPDATE nodes SET proxy_capable=1, status='OFFLINE' WHERE nid='node'`,
	} {
		if _, err := f.writer.Exec(query); err != nil {
			t.Fatal(err)
		}
		current, err = proxyDirectiveForPush(f.b, "lab", "node", &pending)
		if err != nil || current.Enabled || len(current.Keys) != 0 {
			t.Fatal("delayed push escaped node participation authority")
		}
	}
	if _, err := f.writer.Exec(`UPDATE nodes SET status='ONLINE' WHERE nid='node'`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.writer.Exec(`UPDATE port_allocations SET state='FREED' WHERE nid='node'`); err != nil {
		t.Fatal(err)
	}
	if current, err = proxyDirectiveForPush(f.b, "lab", "node", nil); err == nil || current != nil {
		t.Fatal("enabled push escaped missing allocation authority")
	}
}

type proxyAuthorityFixture struct {
	b      *Broker
	writer *sql.DB
	hook   *proxyAuthorityReadHook
	alloc  *port.Allocation
}

func newProxyAuthorityFixture(t *testing.T) proxyAuthorityFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "authority.db")
	seedBrokerFixtureDBFile(t, path)
	dsn := "file:" + path
	writer, err := storage.OpenWAL(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	if _, err := session.Create(writer, "lab", "lab", "owner", "pin", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := session.SetProxyEnabledAndBumpEpoch(writer, "lab", true); err != nil {
		t.Fatal(err)
	}
	b, err := New(Config{NATSURL: "nats://127.0.0.1:1", DB: writer, Logger: silentLogger()})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"alice", "bob"} {
		if _, _, err := b.createSubAndBump("lab", name, "owner"); err != nil {
			t.Fatal(err)
		}
	}
	if err := node.Register(writer, node.RegisterInput{SID: "lab", NID: "node", ProtoVersion: proto.ProtoVersion, ProxyCapable: true}, time.Now()); err != nil {
		t.Fatal(err)
	}
	alloc, err := port.AllocateProxy(writer, "lab", "node", b.cfg.PortAllocCfg())
	if err != nil {
		t.Fatal(err)
	}
	hook := &proxyAuthorityReadHook{}
	reader := sql.OpenDB(&proxyAuthorityConnector{dsn: dsn, hook: hook})
	reader.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = reader.Close() })
	b.cfg.DB = reader
	return proxyAuthorityFixture{b: b, writer: writer, hook: hook, alloc: alloc}
}

func (f proxyAuthorityFixture) registerReq() proto.NodeRegisterReq {
	return proto.NodeRegisterReq{Capabilities: []string{proto.CapProxyV1},
		LocalPorts: []proto.LocalPort{{Name: port.ProxyPortName, TokenHash: f.alloc.TokenHash}}}
}

// The hook never substitutes authority data: all rows still come from SQLite.
// Keeping it per connector avoids mutating the process-wide sqlite driver.
type proxyAuthorityReadHook struct {
	mu      sync.Mutex
	query   string
	fn      func() error
	invoked bool
}

func (h *proxyAuthorityReadHook) arm(query string, fn func() error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.query, h.fn, h.invoked = query, fn, false
}

func (h *proxyAuthorityReadHook) before(query string) error {
	h.mu.Lock()
	fn := h.fn
	if fn == nil || !strings.Contains(query, h.query) {
		h.mu.Unlock()
		return nil
	}
	h.fn, h.invoked = nil, true
	h.mu.Unlock()
	return fn()
}

func (h *proxyAuthorityReadHook) fired() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.invoked
}

type proxyAuthorityConnector struct {
	dsn  string
	hook *proxyAuthorityReadHook
}

func (c *proxyAuthorityConnector) Connect(context.Context) (driver.Conn, error) {
	base, err := c.Driver().Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &proxyAuthorityConn{Conn: base, hook: c.hook}, nil
}

func (*proxyAuthorityConnector) Driver() driver.Driver { return &sqlite.Driver{} }

type proxyAuthorityConn struct {
	driver.Conn
	hook *proxyAuthorityReadHook
}

func (c *proxyAuthorityConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func (c *proxyAuthorityConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.hook.before(query); err != nil {
		return nil, err
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *proxyAuthorityConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.hook.before(query); err != nil {
		return nil, err
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
