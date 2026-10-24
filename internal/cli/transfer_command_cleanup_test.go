package cli

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/spf13/cobra"
)

// Only the command's ownership seam is modeled; source FD IO and closure are
// real. Transfer bytes, transport negotiation and history are tested by the
// existing native engine tests, rather than certified by this invocation.
type sourceOwningInvocation struct {
	source  *os.File
	runErr  error
	stopErr error
	stops   int
}

func (i *sourceOwningInvocation) Run(x.Deadline) (x.TerminalReceipt, error) {
	if _, err := i.source.Stat(); err != nil {
		return x.TerminalReceipt{}, err
	}
	return x.TerminalReceipt{TerminalID: "terminal", Kind: "complete", CanonicalDigest: "digest", ReqID: "request", HistoryStream: "history", HistorySeq: 1}, i.runErr
}
func (*sourceOwningInvocation) Cancel(x.CancelReason, x.Deadline) (x.Status, error) {
	return x.Status{}, errors.New("unexpected cancellation")
}
func (i *sourceOwningInvocation) Stop(x.Deadline) error {
	i.stops++
	return errors.Join(i.source.Close(), i.stopErr)
}

// origin: hot-update S10 plan §0 / LINT-L2 errcheck
func TestTransferCommandRetiresSourceThroughItsCurrentOwner(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferCommandSourceCustody(t)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseTransferCommandSourceCustody(t *testing.T) {
	t.Helper()
	for _, mode := range []string{"success", "run_error", "stop_error", "factory_error"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("TETHER_SESSION", "lab")
			directory := t.TempDir()
			path := filepath.Join(directory, "source")
			if err := os.WriteFile(path, []byte("source"), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := &cobra.Command{}
			cmd.SetContext(t.Context())
			cmd.SetOut(io.Discard)
			cmd.Flags().Int64("rate-limit", 0, "Transfer rate")
			failure := errors.New("injected invocation failure")
			owner := &sourceOwningInvocation{}
			ports := TransferCommandPorts{
				Open: func(*cobra.Command, string, string, string) (xferclient.Config, func() error, error) {
					return xferclient.Config{}, func() error { return nil }, nil
				},
				Client: func(cfg xferclient.Config) (xferclient.Invocation, error) {
					owner.source = cfg.Source
					if mode == "factory_error" {
						return nil, failure
					}
					return owner, nil
				},
				Classify: func(error) (string, int) { return "fixture_failure", exitInternal },
			}
			if mode == "run_error" {
				owner.runErr = failure
			}
			if mode == "stop_error" {
				owner.stopErr = failure
			}
			err := RunTransferCommand(cmd, ports, directory, "fixture", "push", path, RemoteTransferSpec{Node: "node", Path: "/destination"}, false, time.Second)
			if mode == "success" && err != nil || mode != "success" && !errors.Is(err, failure) || errors.Is(err, os.ErrClosed) {
				t.Fatalf("command lost the original outcome or closed its source twice: %v", err)
			}
			wantStops := 1
			if mode == "factory_error" {
				wantStops = 0
			}
			if owner.stops != wantStops {
				t.Fatalf("source owner stopped %d times, want %d", owner.stops, wantStops)
			}
			if owner.source == nil {
				t.Fatal("command never pinned its source")
			}
			if _, err := owner.source.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("source descriptor survived its owner: %v", err)
			}
		})
	}
}
