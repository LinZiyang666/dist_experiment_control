// origin: hot-update S6 plan §2.1 / §12.1 P00
package execclient

import (
	"context"
	"io"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// Admission cancellation ends at true start, leaving the long task's lifetime intact.
type AdmissionConfig struct {
	Deadline        controlbudget.Deadline
	LogicalID       string
	LegacyConfirmed bool
	Observer        controlbudget.Observer
}
type ProbeWithinFunc = func(ctx context.Context, nc *nats.Conn, subject string, req proto.ProcessResumeReq, cfg AdmissionConfig) (proto.ProcessResumeReply, error)
type CreateWithinFunc = func(ctx context.Context, nc *nats.Conn, subject string, req proto.ProcessResumeReq, cfg AdmissionConfig) (proto.ProcessResumeReply, error)
type RunWithinFunc = func(ctx context.Context, nc *nats.Conn, subject string, spec proto.ExecReq, stdout, stderr io.Writer, cfg AdmissionConfig) (Result, error)
