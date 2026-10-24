package brokercontrol

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// BindAdminUpgrade keeps caller contexts inside the control adapter. The
// socket supplies its own read/control stop signal and an absolute budget;
// Fence may cancel reads while an accepted cut retains its reply custody.
// origin: S7 internal review GATE-CONVERGE-layering F2
func BindAdminUpgrade(endpoint upgradeport.Endpoint) adminsock.UpgradeEndpoint {
	if endpoint == nil {
		return nil
	}
	return func(body []byte, access upgradewire.Access, deadline controlbudget.Deadline, stop <-chan struct{}) upgradewire.Response {
		remaining, err := controlbudget.Remaining(deadline)
		if err != nil {
			return upgradewire.ErrorResponse(&upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "admin upgrade deadline expired"})
		}
		// ctx-none: the admin port supplies a deadline and owner stop, not a caller context.
		ctx, cancel := context.WithTimeout(context.Background(), remaining)
		// A previously fenced read must refuse before engine admission, rather
		// than racing the cancellation watcher to an irreversible operation.
		// origin: S7 internal review GATE-CONVERGE-layering F2
		select {
		case <-stop:
			cancel()
		default:
		}
		joined := make(chan struct{})
		go func() {
			defer close(joined)
			select {
			case <-stop:
				cancel()
			case <-ctx.Done():
			}
		}()
		defer func() { cancel(); <-joined }()
		return upgradewire.Handle(ctx, endpoint, body, access)
	}
}

// brokerAuthOwner is one generation's explicit store-user barrier. The mutex
// linearizes admission against Fence; queued messages and copied verifier
// closures cannot touch SQLite after the fence. Idle is closed by the last
// real borrower, so deadline expiry does not fabricate a join receipt.
// origin: hot-update S6 plan §8.1 / T29
type BrokerAuthOwner struct {
	VerifierOwner   *atomic.Pointer[func(string, string) error]
	Emit            func(string, int64)
	Deadline        controlbudget.Deadline
	Key             nkeys.KeyPair
	Sub             *nats.Subscription
	Verifier        *func(string, string) error
	Mu              sync.Mutex
	Active          int
	Fenced          bool
	OffRecorded     bool
	LastReplyBootNS int64
	Idle            chan struct{}
	Done            chan struct{}
	FinishOnce      sync.Once
	ProbeNkey       string
	ProbeReply      bool
	HandleProbe     nats.MsgHandler
	SubFlushBootNS  int64
	ReplyBootNS     int64
	UsableBootNS    int64
}

func (o *BrokerAuthOwner) Enter() bool {
	o.Mu.Lock()
	defer o.Mu.Unlock()
	if o.Fenced {
		return false
	}
	if o.Active == 0 {
		o.Idle = make(chan struct{})
	}
	o.Active++
	return true
}

func (o *BrokerAuthOwner) Leave() {
	o.Mu.Lock()
	o.Active--
	last := o.Active == 0
	if last {
		close(o.Idle)
	}
	finish := last && o.Fenced
	o.Mu.Unlock()
	// A close event can race SetClosedHandler. The last fenced borrower also
	// owns retirement, so a lost subscription-close callback cannot retain
	// the signing key or manufacture a joined store while it is still used.
	if finish {
		o.Finish()
	}
}

func (o *BrokerAuthOwner) Fence() <-chan struct{} {
	o.Mu.Lock()
	defer o.Mu.Unlock()
	o.Fenced = true
	if o.Verifier != nil {
		o.VerifierOwner.CompareAndSwap(o.Verifier, nil)
	}
	return o.Idle
}

func (o *BrokerAuthOwner) Finish() {
	o.FinishOnce.Do(func() {
		<-o.Fence()
		if o.Key != nil {
			o.Key.Wipe()
		}
		close(o.Done)
	})
}

func BrokerAuthRemaining(deadline controlbudget.Deadline) (time.Duration, error) {
	if deadline == 0 {
		return time.Second, nil // Cold install has no image deadline.
	}
	now, err := procrequest.BootNow()
	if err != nil {
		return 0, err
	}
	if int64(deadline) <= now {
		return 0, fmt.Errorf("broker: auth image deadline expired")
	}
	return time.Duration(int64(deadline) - now), nil
}

// Fence before closing any store. Unsubscribe alone does not join a callback
// already running, nor a PIN verifier borrowed by the leader apply responder.
// This may be called again after a timeout to complete the same join.
// origin: hot-update S6 plan §8.1 / T29
func StopBrokerImageAuth(owner *BrokerAuthOwner, deadline controlbudget.Deadline) error {
	if owner == nil {
		return nil
	}
	idle := owner.Fence()
	var err error
	if owner.Sub == nil {
		owner.Finish()
	} else {
		err = owner.Sub.Unsubscribe()
	}
	if errors.Is(err, nats.ErrBadSubscription) || errors.Is(err, nats.ErrConnectionClosed) {
		err = nil
	}
	select {
	case <-idle:
		owner.Finish()
		RecordBrokerAuthOff(owner)
		return err
	default:
	}
	timeout, remainingErr := BrokerAuthRemaining(deadline)
	if remainingErr != nil {
		return errors.Join(err, remainingErr)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-idle:
		// Queued callbacks cannot enter after Fence. All actual Handle/Plan
		// borrowers have left; retirement need not wait for a close callback
		// that may have raced subscription setup.
		owner.Finish()
		RecordBrokerAuthOff(owner)
		return err
	case <-timer.C:
	}
	return errors.Join(err, fmt.Errorf("broker: auth store users did not join within image deadline"))
}

// Record the old endpoint only after every admitted response has left. The
// producer's last successful callout reply is the measurement witness; fencing
// or a log poll alone does not prove an authentication response was usable.
// origin: hot-update S6 plan §8.1 / rv-ctl-3
func RecordBrokerAuthOff(owner *BrokerAuthOwner) {
	if owner.HandleProbe == nil {
		return
	} // The apply responder is not a callout endpoint.
	owner.Mu.Lock()
	defer owner.Mu.Unlock()
	if owner.OffRecorded {
		return
	}
	owner.OffRecorded = true
	owner.Emit("auth_off", owner.LastReplyBootNS)
}

// A successful static broker connection never sets probeReply. Decode verifies
// JWT signatures and binds both response and user grant to this real server's
// ephemeral user. No raw JWT is retained in status, logs or the public image.
// origin: hot-update S6 plan §8.1 / T29
func (o *BrokerAuthOwner) ObserveProbe(request, response string) {
	o.Mu.Lock()
	wanted := o.ProbeNkey
	o.Mu.Unlock()
	if wanted == "" {
		return
	}
	req, err := jwt.DecodeAuthorizationRequestClaims(request)
	if err != nil || req.ConnectOptions.Nkey != wanted {
		return
	}
	o.Mu.Lock()
	defer o.Mu.Unlock()
	if o.ProbeNkey != wanted || o.Fenced {
		return
	}
	resp, err := jwt.DecodeAuthorizationResponseClaims(response)
	if err != nil || resp.Error != "" || resp.Subject != req.UserNkey || resp.Audience != req.Server.ID {
		return
	}
	account, err := o.Key.PublicKey()
	if err != nil || resp.Issuer != account {
		return
	}
	user, err := jwt.DecodeUserClaims(resp.Jwt)
	if err == nil && user.Issuer == account && user.Subject == req.UserNkey {
		now, clockErr := procrequest.BootNow()
		if clockErr == nil && now < int64(o.Deadline) {
			o.ProbeReply = true
			o.ReplyBootNS = now
		}
	}
}

func ProbeBrokerImageAuth(owner *BrokerAuthOwner, nc *nats.Conn, deadline controlbudget.Deadline) error {
	kp, err := nkeys.CreateUser()
	if err != nil {
		return err
	}
	defer kp.Wipe()
	pub, err := kp.PublicKey()
	if err != nil {
		return err
	}
	owner.Mu.Lock()
	owner.ProbeNkey, owner.ProbeReply = pub, false
	owner.Mu.Unlock()
	defer func() {
		owner.Mu.Lock()
		owner.ProbeNkey = ""
		owner.Mu.Unlock()
	}()
	// A shared queue may route the local server's CONNECT to a peer. During
	// this isolated, unactivated, read-only probe, a temporary local observer
	// also signs/responds to precisely this disposable nkey's real request.
	// Other clients are ignored. This proves the local handler without relying
	// on queue affinity, and a static-auth server still produces no proof.
	// origin: hot-update S6 plan §8.1 / T29
	probeSub, err := nc.Subscribe("$SYS.REQ.USER.AUTH", func(msg *nats.Msg) {
		req, err := jwt.DecodeAuthorizationRequestClaims(string(msg.Data))
		if err == nil && req.ConnectOptions.Nkey == pub {
			owner.HandleProbe(msg)
		}
	})
	if err != nil {
		return err
	}
	defer func() { _ = probeSub.Unsubscribe() }()
	timeout, err := BrokerAuthRemaining(deadline)
	if err != nil {
		return err
	}
	if err := nc.FlushTimeout(timeout); err != nil {
		return err
	}
	timeout, err = BrokerAuthRemaining(deadline)
	if err != nil {
		return err
	}
	// Use the connected IP, not another discovery/DNS/peer route. TLS uses the
	// already configured trust and server name, with a fresh user credential.
	opts := []nats.Option{nats.Nkey(pub, kp.Sign), nats.Name("tether-cli"),
		nats.Timeout(timeout), nats.MaxReconnects(0), nats.IgnoreDiscoveredServers()}
	if nc.Opts.Secure {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if nc.Opts.TLSConfig != nil {
			tlsConfig = nc.Opts.TLSConfig.Clone()
		}
		if tlsConfig.ServerName == "" {
			connectedURL, err := url.Parse(nc.ConnectedUrl())
			if err != nil {
				return err
			}
			tlsConfig.ServerName = connectedURL.Hostname()
		}
		opts = append(opts, nats.Secure(tlsConfig))
	}
	probe, err := nats.Connect("nats://"+nc.ConnectedAddr(), opts...)
	if err != nil {
		return fmt.Errorf("broker: image callout CONNECT probe: %w", err)
	}
	defer probe.Close()
	return WaitBrokerImageAuthProof(owner, deadline)
}

// A peer's queue reply can complete CONNECT before this owner's temporary
// observer finishes. Wait for the real local signed proof within the original
// deadline; an authenticated peer connection cannot replace that proof.
// origin: hot-update S6 plan §8.1 / T29 / N3 maintenance integration
func WaitBrokerImageAuthProof(owner *BrokerAuthOwner, deadline controlbudget.Deadline) error {
	for {
		owner.Mu.Lock()
		usable, fenced := owner.ProbeReply, owner.Fenced
		owner.Mu.Unlock()
		if usable && !fenced {
			return nil
		}
		left, err := BrokerAuthRemaining(deadline)
		if fenced || err != nil {
			return fmt.Errorf("broker: image CONNECT did not verify the local callout response")
		}
		time.Sleep(min(left, time.Millisecond))
	}
}
