package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: S7 internal review rv-n1compat F1
func TestTransferLegacyCommitAckCannotDecideOutcome(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseLegacyCommitOutcome(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "legacy terminal wait", g)
	testharness.AssertNoFDLeak(t, "legacy terminal wait", fd, 0)
}

func exerciseLegacyCommitOutcome(t *testing.T) {
	for _, mode := range []string{"delayed_complete", "failed", "missing_ack", "missing_event", "wrong_id", "wrong_size", "changed_source"} {
		t.Run(mode, func(t *testing.T) {
			s, nc, id, stop := transferConnectionFixture(t)
			defer stop()
			cfg, closeOwner, err := TransferClientConfig(nc, s.ClientURL(), id, "lab")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := closeOwner(); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			cfg.Identity.NID, cfg.Identity.TransferID = "node", "original"
			cfg.Intent = x.Intent{Verb: "push", Path: "/tmp/target", Size: 600 * 1024}
			path := filepath.Join(t.TempDir(), "source")
			if err = os.WriteFile(path, bytes.Repeat([]byte("s"), int(cfg.Intent.Size)), 0600); err != nil {
				t.Fatal(err)
			}
			cfg.Source, err = os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := cfg.Source.Close(); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
			store, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: proto.XferBucketName("lab")})
			if err != nil {
				t.Fatal(err)
			}
			caps := answerTransferCaps(t, nc, id.PublicKey, "lab", "", "")
			defer func() {
				if err := caps.Unsubscribe(); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			var prepares, commits atomic.Int32
			prep, err := nc.Subscribe(proto.SubjCmdBy("lab", id.PublicKey, "node", "push"), func(m *nats.Msg) {
				prepares.Add(1)
				if mode == "changed_source" {
					if err := os.WriteFile(path, bytes.Repeat([]byte("changed"), int(cfg.Intent.Size)/7+1)[:cfg.Intent.Size], 0600); err != nil {
						t.Error(err)
						return
					}
				}
				_ = m.Respond([]byte(`{"ok":true,"tier":"b"}`))
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := prep.Unsubscribe(); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			committed := make(chan struct{}, 1)
			release := make(chan struct{})
			handlerDone := make(chan struct{})
			commit, err := nc.Subscribe(proto.SubjCmdBy("lab", id.PublicKey, "node", "push-commit"), func(m *nats.Msg) {
				defer close(handlerDone)
				commits.Add(1)
				if mode != "missing_ack" {
					_ = m.Respond([]byte(`{"ok":true}`))
				}
				committed <- struct{}{}
				if mode == "delayed_complete" {
					select {
					case <-release:
					case <-t.Context().Done():
						return
					}
				}
				if mode == "missing_event" {
					return
				}
				ev := proto.TransferEvent{Kind: "complete", Verb: "push", TransferID: "original", Tier: "b", Bytes: cfg.Intent.Size}
				switch mode {
				case "failed":
					ev.Kind, ev.Code, ev.Error = "failed", "dst_exists", "destination exists"
				case "wrong_id":
					ev.TransferID = "foreign"
				case "wrong_size":
					ev.Bytes--
				}
				body, _ := json.Marshal(ev)
				_ = nc.Publish(proto.SubjEvTransfer("lab", "node", "original", ev.Kind), body)
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := commit.Unsubscribe(); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			if err = nc.Flush(); err != nil {
				t.Fatal(err)
			}
			d, _ := controlbudget.After(time.Second)
			legacy, err := SelectTransferClient(&cfg, d)
			if err != nil || !legacy {
				t.Fatalf("cap0: %v %v", legacy, err)
			}
			owner, err := NewLegacyTransfer(cfg, path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := StopLegacyTransfer(owner); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			type outcome struct {
				result x.LegacyResult
				err    error
			}
			done := make(chan outcome, 1)
			go func() { r, e := owner.Run(d); done <- outcome{r, e} }()
			if mode != "changed_source" {
				<-committed
			}
			if mode == "delayed_complete" {
				select {
				case early := <-done:
					close(release)
					<-handlerDone
					t.Fatalf("commit ACK decided outcome before receiver: %+v", early)
				case <-time.After(50 * time.Millisecond):
				}
				close(release)
			}
			got := <-done
			if mode != "changed_source" {
				<-handlerDone
			}
			want := 74
			if mode == "delayed_complete" {
				want = 0
			}
			if mode == "failed" || mode == "changed_source" {
				want = 64
			}
			if code := xferclient.ExitCode(got.err); code != want {
				t.Fatalf("exit=%d want=%d result=%+v err=%v", code, want, got.result, got.err)
			}
			if want == 0 && (got.result.Kind != "complete" || got.result.Bytes != cfg.Intent.Size) {
				t.Fatalf("wrong completion: %+v", got.result)
			}
			metadata, err := store.GetInfo(t.Context(), "original", jetstream.GetObjectInfoShowDeleted())
			if err != nil {
				t.Fatal(err)
			}
			wantCommits := int32(1)
			if mode == "changed_source" {
				wantCommits = 0
				if !metadata.Deleted {
					t.Fatal("changed source was not tombstoned before commit")
				}
			}
			againDeadline, _ := controlbudget.After(time.Second)
			again, againErr := owner.Run(againDeadline)
			after, err := store.GetInfo(t.Context(), "original", jetstream.GetObjectInfoShowDeleted())
			if err != nil {
				t.Fatal(err)
			}
			if again != got.result || againErr != got.err || metadata.NUID != after.NUID || prepares.Load() != 1 || commits.Load() != wantCommits {
				t.Fatal("terminal wait reentered the original transfer or replaced its result")
			}
		})
	}
}
