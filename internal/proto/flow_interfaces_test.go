package proto_test

import (
	"io"
	"net/netip"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

// These assignments consume the exact frozen signatures, including channel
// direction and non-method constructors. A merely compatible call is weaker:
// changing a pointer/interface parameter could still compile at a nil call site.
// origin: hot-update S5 plan §2.1 / §12.1 / §12.2 P0
var (
	_ func(*os.File, rawsocket.Fingerprint) (*rawsocket.Socket, error)                                              = rawsocket.Adopt
	_ func(netip.AddrPort, time.Time, <-chan struct{}, func(rawsocket.Tuple) error) (*rawsocket.Socket, error)      = rawsocket.Connect
	_ func(*os.File, rawsocket.Fingerprint) error                                                                   = rawsocket.ValidateFile
	_ func(string, []byte) (*sealedstate.Snapshot, error)                                                           = sealedstate.New
	_ func(rawsocket.FileSource, string, string, int64) ([]byte, *os.File, error)                                   = sealedstate.Read
	_ func(io.Writer, flow.Frame) error                                                                             = flow.EncodeFrame
	_ func(io.Reader) (flow.Frame, error)                                                                           = flow.DecodeFrame
	_ func(flow.Config) (*flow.Manager, error)                                                                      = flow.New
	_ func(flow.Config, flow.Image, rawsocket.FileSource) (*flow.Manager, []handoff.Resource, error)                = flow.Restore
	_ func(tunnel.FlowConfig, tunnel.ClientImage, rawsocket.FileSource) (*tunnel.Client, []handoff.Resource, error) = tunnel.RestoreClient
	_ func(ssproxy.Image, rawsocket.FileSource) (*ssproxy.Server, []handoff.Resource, error)                        = ssproxy.Restore

	_ interface {
		CompleteConnect(time.Time, <-chan struct{}) error
		Read([]byte) (int, error)
		Write([]byte) (int, error)
		SetReadDeadline(time.Time) error
		SetWriteDeadline(time.Time) error
		CloseWrite() error
		Shutdown() error
		Close() error
		Fingerprint() rawsocket.Fingerprint
		ExportFile() (*os.File, error)
	} = (*rawsocket.Socket)(nil)
	_ interface {
		Freeze(time.Time) (*flow.Frozen, error)
		ActivateRaw() error
		RevokeFlow(flow.ID, flow.Reason) error
		Close() error
	} = (*flow.Manager)(nil)
	_ interface {
		Export() (flow.Image, *sealedstate.Snapshot, error)
		Thaw() error
		Discard() error
		SnapshotState() (sealedstate.CutState, error)
	} = (*flow.Frozen)(nil)
	_ interface{ SetFlowConfig(tunnel.FlowConfig) error } = (*tunnel.Server)(nil)
	_ interface {
		SetFlowConfig(tunnel.FlowConfig) error
		PrepareHandoff(uint64, [16]byte, time.Time) error
		FreezeFlows(time.Time) (*tunnel.ClientFrozen, error)
		ActivateRaw() error
		ResumeCarriers(time.Time) error
		RevokeFlow(flow.ID, flow.Reason) error
		FlowCensus() tunnel.Census
	} = (*tunnel.Client)(nil)
	_ interface {
		Export() (tunnel.ClientImage, *sealedstate.Snapshot, error)
		Thaw() error
		Discard() error
		SnapshotState() (sealedstate.CutState, error)
	} = (*tunnel.ClientFrozen)(nil)
	_ interface {
		ExpectFlow([16]byte, rawsocket.Tuple) error
		SetFlowRevoke(func([16]byte, string)) error
		Freeze(time.Time) (*ssproxy.Frozen, error)
		ActivateRaw() error
		Authorize(ssproxy.Grant) error
		RevokeAll(string) error
	} = (*ssproxy.Server)(nil)
	_ interface {
		Export() (ssproxy.Image, *sealedstate.Snapshot, error)
		Thaw() error
		Discard() error
		SnapshotState() (sealedstate.CutState, error)
	} = (*ssproxy.Frozen)(nil)
	_ interface{ Close() error } = (*sealedstate.Snapshot)(nil)
)
