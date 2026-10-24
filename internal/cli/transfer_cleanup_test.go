package cli

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: S7 internal review rv-jsnats F3 / s7-p9jsn2 item 1
func TestTransferAttachmentRetainsConsumerCleanup(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseTransferConsumerCleanup(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer consumer cleanup", g)
	testharness.AssertNoFDLeak(t, "transfer consumer cleanup", fd, 0)
}

func exerciseTransferConsumerCleanup(t *testing.T) {
	for _, mode := range []string{"close", "replace", "retain_on_error"} {
		t.Run(mode, func(t *testing.T) {
			s, control, id, stop := transferConnectionFixture(t)
			defer stop()
			cfg, closeOwner, err := TransferClientConfig(control, s.ClientURL(), id, "lab")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := closeOwner(); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			}()
			identity := transferAttachmentIdentity()
			transport := cfg.Transport.(*TransferTransport)
			deadline, _ := controlbudget.After(5 * time.Second)
			dataJS, err := transport.OpenData(identity, deadline)
			if err != nil {
				t.Fatal(err)
			}
			adminJS, err := jetstream.New(control)
			if err != nil {
				t.Fatal(err)
			}
			bucket := proto.XferBucketName(identity.SID)
			store, err := adminJS.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: bucket})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = store.PutBytes(t.Context(), identity.TransferID, bytes.Repeat([]byte("x"), 256<<10)); err != nil {
				t.Fatal(err)
			}
			dataStore, err := dataJS.ObjectStore(t.Context(), bucket)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := jsstream.GetTransferObject(t.Context(), dataJS, dataStore, bucket, identity.TransferID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = io.ReadFull(reader, make([]byte, 32)); err != nil {
				t.Fatal(err)
			}
			stream, err := adminJS.Stream(t.Context(), "OBJ_"+bucket)
			if err != nil {
				t.Fatal(err)
			}
			before, err := stream.Info(t.Context())
			if err != nil || before.State.Consumers != 1 {
				t.Fatalf("no live consumer: %+v %v", before, err)
			}
			old := dataJS.Conn()
			old.Close()
			_ = reader.Close() // Its closed management socket cannot delete the server consumer.
			if jsstream.ConsumerCleanupFor(old) == nil {
				t.Fatal("fixture lost unresolved cleanup before host retirement")
			}
			if mode == "retain_on_error" {
				unavailable, err := nats.Connect(s.ClientURL())
				if err != nil {
					t.Fatal(err)
				}
				unavailable.Close()
				transport.control = unavailable
				if err = transport.CloseData(identity, deadline); err == nil {
					t.Fatal("closed management binding reported cleanup success")
				}
				transport.mu.Lock()
				retained := transport.cleanup != nil
				transport.mu.Unlock()
				if !retained {
					t.Fatal("cleanup error discarded the retained handle")
				}
				transport.control = control
			}
			if mode == "replace" {
				replacement, err := transport.OpenData(identity, deadline)
				if err != nil || replacement.Conn() == old || replacement.Conn().IsClosed() {
					t.Fatalf("replacement not admitted: %v", err)
				}
			} else if err = transport.CloseData(identity, deadline); err != nil {
				t.Fatal(err)
			}
			after, err := stream.Info(t.Context())
			if err != nil || after.State.Consumers != 0 {
				t.Fatalf("host returned before deleting old consumer: %+v %v", after, err)
			}
			owner := jsstream.ConsumerCleanupFor(old)
			if owner != nil && !errors.Is(owner.Wait(deadline), nil) {
				t.Fatal("old attachment cleanup did not join")
			}
		})
	}
}
