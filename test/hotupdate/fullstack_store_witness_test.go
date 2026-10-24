//go:build linux

// Real production-schema SQLite stores, admin session-allow/deny ACKs and the
// actual OpenCurrent RO boundary are exercised here. No service response is
// stubbed. Raft replay, same-PID product exec and sealed startup wiring remain
// the S6/P06/P11 fullstack fixture's separate acceptance obligations.
package hotupdate_test

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/storewitness"
	"github.com/LinZiyang666/tether/internal/testharness"
)

type storeWitnessSource struct{ file *os.File }

func (s *storeWitnessSource) TakeFile(_, _ string) (*os.File, error) {
	f := s.file
	s.file = nil
	return f, nil
}

// origin: hot-update S10 plan §5.3 / §5.4 / §10.2 P08
func TestFullstackStoreWitnessPrecedesReplay(t *testing.T) {
	// Warm SQLite and the permanent runtime netpoll descriptors before the
	// exact FD baseline; every measured cycle still owns its complete lifetime.
	exerciseStoreWitness(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseStoreWitness(t)
	}
	testharness.AssertNoGoroutineLeak(t, "store witness RO/replay/contender", beforeG)
	testharness.AssertNoFDLeak(t, "store witness RO/replay/contender", beforeFD, 0)
}

func exerciseStoreWitness(t *testing.T) {
	t.Helper()
	// Repeated subtest names contain '#'; production deliberately refuses
	// URI option characters in plain database paths. Keep store paths under
	// the parent test's canonical temporary directory instead.
	root := t.TempDir()
	for _, scenario := range []struct {
		name   string
		wal    bool
		cursor uint64
		erase  bool
	}{
		{name: "single-delete"}, {name: "single-wal", wal: true},
		{name: "replicated-cursor", wal: true, cursor: 17},
		{name: "erase-then-replay", wal: true, cursor: 17, erase: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			startupStarted := time.Now()
			dir := filepath.Join(root, scenario.name)
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "current.db")
			opener := storage.Open
			if scenario.wal {
				opener = storage.OpenWAL
			}
			db, err := opener(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			}()
			var nonce [12]byte
			if _, err := rand.Read(nonce[:]); err != nil {
				t.Fatal(err)
			}
			fpSum := sha256.Sum256(nonce[:])
			fp := "SHA256:" + base64.RawStdEncoding.EncodeToString(fpSum[:])
			absentSum := sha256.Sum256(append([]byte("denied:"), nonce[:]...))
			absent := "SHA256:" + base64.RawStdEncoding.EncodeToString(absentSum[:])
			note := hex.EncodeToString(nonce[:])
			server := adminsock.New(filepath.Join(dir, "admin.sock"), adminsock.Backend{DB: db})
			if err := server.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			startupNS := time.Since(startupStarted).Nanoseconds()
			ioStarted := time.Now()
			defer func() {
				if err := server.Close(); err != nil {
					t.Error(err)
				}
			}()
			client := &adminsock.Client{Path: filepath.Join(dir, "admin.sock"), Timeout: time.Second}
			for _, request := range []adminsock.Request{
				{Op: adminsock.OpSessionAllow, FP: fp, Note: note},
				{Op: adminsock.OpSessionAllow, FP: absent, Note: note},
				{Op: adminsock.OpSessionDeny, FP: absent},
			} {
				response, err := client.Call(request)
				if err != nil || response == nil || !response.OK {
					t.Fatalf("real business ACK failed: %v", err)
				}
			}
			if err := server.Close(); err != nil {
				t.Fatal(err)
			}
			ioNS := time.Since(ioStarted).Nanoseconds()
			cutStarted := time.Now()
			if scenario.cursor != 0 {
				if _, err := db.Exec("INSERT INTO cluster_meta(key,value) VALUES('applied_index',?),('applied_term','3')", scenario.cursor); err != nil {
					t.Fatal(err)
				}
			}
			id, err := storage.CanonicalStoreIdentity(path, dir)
			if err != nil {
				t.Fatal(err)
			}
			stamp, err := storage.ReadStoreStamp(db, id)
			if err != nil {
				t.Fatal(err)
			}
			if stamp.AppliedIndex != scenario.cursor || (scenario.cursor == 0 && stamp.AppliedTerm != 0) {
				t.Fatal("fixture did not preserve the single/replicated cursor distinction")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			owner, err := os.OpenFile(id.LockPath, os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			}()
			if err := syscall.Flock(int(owner.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
				t.Fatal(err)
			}
			contender, err := storewitness.StartContender(id.LockPath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = contender.Close()
				if err := contender.Join(); err != nil {
					t.Error(err)
				}
			}()
			registration := storewitness.Registration{Device: id.Device, Inode: id.Inode, Nonce: nonce,
				ScopeDigest: sha256.Sum256(append([]byte(scenario.name), nonce[:]...)), Keys: []string{fp, absent}}
			config, err := storewitness.EncodeRegistration(registration)
			if err != nil {
				t.Fatal(err)
			}
			sealed, err := sealedstate.New("store-witness", config)
			if err != nil {
				t.Fatal(err)
			}
			witness, err := storewitness.Register(&storeWitnessSource{sealed.Owned[0]}, "store-witness", "state")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := witness.Close(); err != nil {
					t.Error(err)
				}
			}()
			var events []drillobserve.Event
			old := drillobserve.Hook
			drillobserve.Hook = func(e drillobserve.Event) {
				if e.Kind != storewitness.Kind {
					t.Error("unexpected store event")
				}
				writers, readers := storage.DefaultDriverRegistry().Connections(id)
				if writers != 0 || readers != 1 {
					t.Errorf("pre-replay reader is not the sole actual RO connection: %d/%d", writers, readers)
				}
				if err := contender.Probe(); err != nil {
					t.Error("store lock became available during RO validation")
				}
				events = append(events, e)
			}
			defer func() { drillobserve.Hook = old }()
			if scenario.erase {
				lost, err := storage.OpenExisting(path)
				if err != nil {
					t.Fatal(err)
				}
				_, eraseErr := lost.Exec("DELETE FROM session_creators WHERE fp=?", fp)
				if err := errors.Join(eraseErr, lost.Close()); err != nil {
					t.Fatal(err)
				}
			}
			current, err := storage.OpenCurrent(storage.OpenImageConfig{Stamp: stamp, Registry: storage.DefaultDriverRegistry()})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := current.Close(); err != nil {
					t.Error(err)
				}
			}()
			cutNS := time.Since(cutStarted).Nanoseconds()
			settleStarted := time.Now()
			if len(events) != 1 {
				t.Fatalf("OpenCurrent emitted %d pre-replay receipts, want exactly one", len(events))
			}
			receipt, err := storewitness.Decode(events[0])
			if err != nil {
				t.Fatal(err)
			}
			if receipt.Stamp.AppliedIndex != stamp.AppliedIndex || receipt.Stamp.AppliedTerm != stamp.AppliedTerm ||
				receipt.Stamp.SchemaDigest != sha256.Sum256([]byte(stamp.Schema.Digest+stamp.Schema.ObjectDigest)) ||
				receipt.Stamp.Journal != stamp.Journal || receipt.Stamp.Synchronous != stamp.Synchronous {
				t.Fatal("receipt lost the actual RO durability/schema stamp")
			}
			expected := []storewitness.Row{{Key: fp, Present: true, Digest: sha256.Sum256([]byte(note))}, {Key: absent}}
			if scenario.erase {
				// Model the later replay effect only after OpenCurrent returned its
				// writer. Completed-state inspection sees restored ACKs, whereas
				// the product receipt must retain the earlier missing row.
				if _, err := current.Exec("INSERT INTO session_creators(fp,added_at,added_by,note) VALUES(?,?,'admin',?)", fp, time.Now().UTC(), note); err != nil {
					t.Fatal(err)
				}
				if err := storewitness.Verify(receipt, registration, expected); !errors.Is(err, storewitness.ErrWitness) {
					t.Fatal("replay concealed erased ACK in pre-replay evidence")
				}
			} else if err := storewitness.Verify(receipt, registration, expected); err != nil {
				t.Fatal("pre-replay ACK/absence differs from independent business ledger")
			}
			var finalNote string
			if err := current.QueryRow("SELECT note FROM session_creators WHERE fp=?", fp).Scan(&finalNote); err != nil || finalNote != note {
				t.Fatal("completed-state ACK was not present")
			}
			if _, err := storage.ReadStoreStamp(current, id); err != nil {
				t.Fatal(err)
			}
			if len(events) != 1 {
				t.Fatal("ordinary writer stamping emitted pre-replay evidence")
			}
			if err := current.Close(); err != nil {
				t.Fatal(err)
			}
			if writers, readers := storage.DefaultDriverRegistry().Connections(id); writers != 0 || readers != 0 {
				t.Fatal("RO/writer connections survived close")
			}
			settleNS := time.Since(settleStarted).Nanoseconds()
			joinStarted := time.Now()
			if err := contender.Close(); err != nil {
				t.Fatal(err)
			}
			if err := contender.Join(); err != nil {
				t.Fatal(err)
			}
			lockReceipt, err := contender.Result()
			if err != nil || lockReceipt.Attempts == 0 || lockReceipt.Acquired != 0 || lockReceipt.FirstAcquiredAttempt != 0 {
				t.Fatal("continuous lock evidence was missing or showed acquisition")
			}
			// Cost is evidence, never a timing tolerance. There is no child or
			// supervisor in this RO/admin/contender lifecycle.
			t.Logf("store witness costs_ns startup=%d io=%d cut=%d settle=%d observer_join=%d", startupNS, ioNS, cutNS, settleNS, time.Since(joinStarted).Nanoseconds())
		})
	}
}
