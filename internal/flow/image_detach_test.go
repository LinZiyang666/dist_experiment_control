//go:build linux

package flow

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S5 internal review rv-image-1
func TestFlowPristineDetachPreservesOriginalClosure(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseFlowPristineDetach(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "flow pristine detach", beforeG)
	testharness.AssertNoFDLeak(t, "flow pristine detach", beforeFD, 0)
}

func closeDetachedFlowResources(resources []handoff.Resource) {
	for _, res := range resources {
		for _, file := range res.Files {
			_ = file.Close()
		}
	}
}

func exerciseFlowPristineDetach(t *testing.T) {
	original, req, peer, cleanup := flowFixture(t)
	defer cleanup()
	if _, err := original.Detach(); err == nil {
		t.Fatal("live manager allowed pristine detach")
	}
	frozen, err := original.Freeze(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(frozen.Discard)
	if _, err := original.Detach(); err == nil {
		t.Fatal("exporter allowed pristine detach")
	}
	im, snapshot, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	for _, exit := range []string{"detach", "activate", "discard", "shutdown", "refusal", "revoke"} {
		src := handofftest.DupResources(t, snapshot.Resources)
		expected := src.Borrow()
		metadata := expected[0].Files["state"]
		budget := &workload.ReplayBudget{}
		restored, salvage, err := Restore(Config{Budget: budget}, im, src)
		if err != nil {
			closeDetachedFlowResources(salvage)
			t.Fatal(err)
		}
		defer func(closeFunc func() error) { _ = closeFunc() }(restored.Close)
		if metadata == nil {
			t.Fatal("sealed metadata premise missing")
		}
		switch exit {
		case "detach":
			resources, err := restored.Detach()
			defer closeDetachedFlowResources(resources)
			if err != nil {
				t.Fatal(err)
			}
			if len(resources) != len(expected) || budget.Used() != 0 {
				t.Fatal("detach omitted metadata or retained reservation")
			}
			files := map[string]*os.File{}
			for _, res := range resources {
				for name, file := range res.Files {
					files[res.ID+"/"+name] = file
					if _, err := file.Stat(); err != nil {
						t.Fatal("detach closed business or sealed file")
					}
				}
			}
			for _, res := range expected {
				for name, file := range res.Files {
					if files[res.ID+"/"+name] != file {
						t.Fatal("detach replaced original closure")
					}
				}
			}
			if _, err := restored.Detach(); err == nil {
				t.Fatal("detach succeeded twice")
			}
		case "activate":
			if err := restored.ActivateRaw(); err != nil {
				t.Fatal(err)
			}
			if _, err := restored.Detach(); err == nil {
				t.Fatal("active import allowed pristine detach")
			}
			if _, err := metadata.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatal("activation retained pristine sealed metadata")
			}
			message := []byte("same-business-OFD")
			frame := incoming(req, KindData, 0, message)
			frame.Flags = 0 // The restored target owner sends toward the public direction.
			receive(t, restored, req, frame)
			got := make([]byte, len(message))
			if _, err := io.ReadFull(peer, got); err != nil || string(got) != string(message) {
				t.Fatal("pristine detach shut down shared business OFD")
			}
			_ = restored.Close()
		case "discard", "shutdown":
			if exit == "discard" {
				_ = (&Frozen{state: &frozenState{manager: restored}}).Discard()
			} else {
				_ = restored.Close()
			}
		case "revoke":
			if err := restored.RevokeFlow(req.ID, ReasonRevoked); err != nil {
				t.Fatal(err)
			}
			if _, err := restored.Detach(); err == nil {
				t.Fatal("changed import allowed pristine redispatch")
			}
		case "refusal":
			// A bad wrapper must be refused before any metadata/file transfer.
			r := restored.state.records[req.ID]
			_ = r.replay.Close()
			if resources, err := restored.Detach(); err == nil || len(resources) != 0 {
				t.Fatal("partial detach escaped a refused wrapper")
			}
			if _, err := metadata.Stat(); err != nil {
				t.Fatal("refused detach lost sealed metadata")
			}
			_ = restored.Close()
		}
		if exit != "detach" {
			if _, err := metadata.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatal("lifecycle exit retained pristine sealed metadata")
			}
		}
	}
}
