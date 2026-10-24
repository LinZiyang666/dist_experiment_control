// origin: hot-update S6 plan §2.1 / §12.1 P00
package cli

import (
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proxydial"
	"github.com/nats-io/nats.go"
)

// RetryFamily zero is invalid; unsafe sent requests cannot replay a mutation.
type RetryFamily uint8

const (
	RetryReadOnly RetryFamily = iota + 1
	RetryStableProcess
	RetryUnsafe
	// Transfer mutations return to their invocation for status/receipt
	// reconciliation whenever an attempt may have escaped.
	// origin: hot-update S7 plan §7 / T32
	RetryTransfer
)

type RetryConfig struct {
	Deadline       controlbudget.Deadline
	Family         RetryFamily
	LogicalID      string
	HasAuthReceipt bool
	Observer       controlbudget.Observer
}
type DoWithinFunc = func(RetryConfig, func(time.Duration) error) error
type ConnectWithinFunc = func(url string, id *Identity, deadline controlbudget.Deadline, observer controlbudget.Observer, opts ...nats.Option) (*nats.Conn, error)

// The other two neutral aliases refer to their existing argument types. Their
// actual functions remain in natsinbox and proxydial, owned by P20 and P19.
type InboxConnectWithinFunc = func(url, pubKey string, opts []nats.Option, deadline controlbudget.Deadline, observer controlbudget.Observer) (*nats.Conn, bool, error)
type OptionsWithinFunc = func(env proxydial.Env, deadline controlbudget.Deadline, observer controlbudget.Observer) ([]nats.Option, error)
