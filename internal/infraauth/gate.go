// Package infraauth owns the ordinary local auth-before-business barrier.
// origin: hot-update S9 plan §4.4 / T38
package infraauth

import (
	"errors"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats.go"
)

var (
	ErrBinding     = errors.New("infraauth: invalid binding")
	ErrFenced      = errors.New("infraauth: business admission fenced")
	ErrNormalQueue = errors.New("infraauth: local normal queue proof missing")
	ErrProbe       = errors.New("infraauth: ordinary private inbox RPC failed")
)

type Phase uint8

// A generation moves forward once; disconnect changes it only to Fenced.
// Open creates a new generation rather than changing a fenced one back to ready.
const (
	Fenced Phase = iota
	AuthBinding
	AuthUsable
	BusinessBinding
	ControlReady
)

// Transport is implemented by P14's raw transport owner. Cancel must be sticky,
// interrupt raw I/O without waiting for nc.mu, and be safe to call repeatedly.
// Join proves its workers actually left; it must not merely report cancellation.
// A fresh transport belongs to exactly one connection in one generation.
type Transport interface {
	nats.CustomDialer
	Cancel()
	Join(infracontract.Deadline) error
}

// TransportFactory only constructs an owner; network I/O starts in Dial. This
// keeps Close able to interrupt every blocking operation through Cancel.
type TransportFactory func(infracontract.Deadline, infracontract.Stop) (Transport, error)

type Binding struct {
	Subject string
	Queue   string
	Handle  nats.MsgHandler
}

type Subscription struct {
	Subject string
	Queue   string
}

// Receipts contain identities, digests and sender BOOTTIME, never request JWTs,
// credentials or RPC payloads. A returned receipt is an independent snapshot.
type Receipt struct {
	// All times are sender-local BOOTTIME. They establish barrier order within
	// this generation, and must never be compared to another host's clock.
	Generation   uint64
	Phase        Phase
	ServerID     string
	AuthCID      uint64
	BusinessCID  uint64
	AuthPONG     int64
	AuthUsable   int64
	BusinessPONG int64
	ControlReady int64
	Probes       [2]ProbeReceipt
	AuthSUBs     []Subscription
	BusinessSUBs []Subscription
}

type generation struct {
	// Construction owns the slices until built closes. Cleanup then has the
	// complete set, including failed CONNECTs whose raw dialer still needs join.
	// responses hold only hashes and public identities; signed JWTs stay in
	// the normal callback and are never copied into the public receipt. The
	// armed witness holds signed requests only until its probe disarms it.
	mu            sync.Mutex
	receipt       Receipt
	fenced        bool
	lost          chan struct{}
	built         chan struct{}
	done          chan struct{}
	transports    []Transport
	connections   []*nats.Conn
	auth          *nats.Conn
	normalQueue   *nats.Subscription
	business      *nats.Conn
	active        sync.WaitGroup
	responses     [2]ProbeReceipt
	responseReady [2]chan struct{}
	retireErr     error
	// witness is the one armed ordinary probe capture (nil between probes).
	witness *probeWitness
}

// probeWitness is one ordinary probe's deterministic view of the LOCAL
// server's real callout request. nats-server delivers a callout to ONE queue
// member chosen uniformly across local and routed members
// (server/client.go processMsgResults, selectQSub), so with N brokers a peer
// answers the probe CONNECT with probability (N-1)/N and the normal queue
// callback alone witnesses the local owner only by chance. A plain SUB on the
// same auth NC is delivered every request by subject interest, independent of
// that choice. It never responds: the server's queue choice still decides who
// answers, and a second answer would race the first inside the callout.
// origin: hot-update S9 P22 resume — design finding (callout queue selection)
type probeWitness struct {
	identity Identity
	sub      *nats.Subscription
	// captured maps a probe CID to the local server's signed request for it.
	captured map[uint64]string
	// taken holds digests of requests this generation's normal queue itself
	// received while armed; the probe never re-decides one of those.
	taken   map[infracontract.Digest]bool
	arrived chan struct{}
}

// witnessEntries bounds both witness maps. Overflow only costs a redundant
// in-process decision of a request the queue already answered, never a proof.
const witnessEntries = 4096

// Invalidate synchronously, before waiting for any NATS mutex. ReconnectHandler
// is too late: nats.go has already replayed every SUB when it calls that handler.
// No connection owned here auto-reconnects; the next Open builds a fresh gate.
// origin: hot-update S9 plan §4.4–§4.5 / T38
func fence(g *generation) {
	g.mu.Lock()
	if !g.fenced {
		g.fenced = true
		g.receipt.Phase = Fenced
		close(g.lost)
	}
	transports := append([]Transport(nil), g.transports...)
	g.mu.Unlock()
	for _, transport := range transports {
		transport.Cancel()
	}
}

func live(g *generation, deadline infracontract.Deadline) error {
	// The original admission budget shrinks through every phase. A reconnect,
	// PONG or probe response cannot create another full budget for this Open.
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fenced {
		return ErrFenced
	}
	return nil
}

func snapshot(g *generation) Receipt {
	// Callers may project receipts into status, but cannot mutate the SUB set
	// subsequently used by this owner's gate or a later diagnostic borrower.
	g.mu.Lock()
	defer g.mu.Unlock()
	r := g.receipt
	r.AuthSUBs = append([]Subscription(nil), r.AuthSUBs...)
	r.BusinessSUBs = append([]Subscription(nil), r.BusinessSUBs...)
	return r
}

func waitDone(g *generation, deadline infracontract.Deadline) error {
	// A timeout is not a join receipt. Keep the fenced generation reachable so
	// a later Close can finish the same cleanup, rather than abandon workers or
	// let Open publish another generation over dependencies still in use.
	select {
	case <-g.done:
		return g.retireErr
	default:
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return err
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-g.done:
		return g.retireErr
	case <-timer.C:
		return controlbudget.ErrExpired
	}
}

// Callback retirement is distinct from socket retirement. An auth decision may
// still borrow SQLite or a signer after its connection has closed; stop cannot
// release those dependencies until every admitted callback has returned.
// origin: hot-update S9 plan §4.4 / TestInfraAuthLifecycle
func retire(g *generation) {
	<-g.lost
	<-g.built
	// The lost notification can schedule this worker before fence reaches its
	// Cancel loop. Repeat sticky cancellation here so nc.Close can never win
	// that race and block on the very raw write we are trying to interrupt.
	// origin: hot-update S9 plan §4.4–§4.5 / TestInfraAuthLifecycle
	for _, transport := range g.transports {
		transport.Cancel()
	}
	for _, nc := range g.connections {
		nc.Close()
	}
	g.active.Wait()
	deadline, err := controlbudget.After(upgradebudget.SafePointWait)
	if err == nil {
		for _, transport := range g.transports {
			err = errors.Join(err, transport.Join(deadline))
		}
	}
	g.retireErr = err
	close(g.done)
}
