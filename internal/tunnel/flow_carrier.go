package tunnel

import (
	"io"
	"net"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/tunnel/flowcarrier"
	"github.com/LinZiyang666/tether/internal/tunnel/flowstate"
	"github.com/hashicorp/yamux"
)

// The runtime retains its API while the leaf owns this implementation.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
type resumeRequest = flowstate.ResumeRequest
type resumeResponse = flowstate.ResumeResponse

func encodeResumeRequest(r flowstate.ResumeRequest) []byte { return flowstate.EncodeResumeRequest(r) }
func decodeResumeRequest(body []byte) (flowstate.ResumeRequest, error) {
	return flowstate.DecodeResumeRequest(body)
}
func encodeResumeResponse(r flowstate.ResumeResponse) []byte {
	return flowstate.EncodeResumeResponse(r)
}
func decodeResumeResponse(body []byte) (flowstate.ResumeResponse, error) {
	return flowstate.DecodeResumeResponse(body)
}

type flowCarrier = flowcarrier.Carrier
type controlKey = flowcarrier.ControlKey

func newFlowCarrier(conn net.Conn, mux *yamux.Session, control net.Conn, m *flow.Manager, id flow.EndpointID, owner uint64, gen uint64, direction uint16) *flowcarrier.Carrier {
	return flowcarrier.NewFlowCarrier(conn, mux, control, m, id, owner, gen, direction)
}
func flowMuxConfig(loggerWriter io.Writer) *yamux.Config {
	return flowcarrier.FlowMuxConfig(loggerWriter)
}
