package execclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats.go"
)

// PTYClient owns one private subscription and one Run. Probe deliberately
// precedes local raw-mode setup: neither probing nor Close can create a task.
// The mutex protects lifecycle only; the Run loop owns all protocol state.
// origin: hot-update S4 plan §2.2 / §7.2
type PTYClient struct {
	mu     sync.Mutex
	t      *transport
	handle proto.ProcessHandle
	now    func() time.Time
	phase  ptyPhase
	closed bool
	cancel context.CancelFunc
	done   chan struct{}
}

// ProbePTY verifies both the agent capability and the broker's PTY operation
// path within one absolute budget. Only the second lease can authorize Run.
func ProbePTY(ctx context.Context, nc *nats.Conn, subject string) (*PTYClient, error) {
	return probePTY(ctx, nc, subject, nil)
}

// ProbePTYWithin shares RunE admission through both capability probes and true
// start. Silence proves no compatibility; only an explicit old capability or
// unsupported reply permits legacy before create has been sent.
// origin: hot-update S6 plan §8.2 / T31 / T57Cmd
func ProbePTYWithin(ctx context.Context, nc *nats.Conn, subject string, cfg AdmissionConfig) (*PTYClient, error) {
	if _, err := admissionRemaining(ctx, cfg); err != nil {
		return nil, err
	}
	return probePTY(ctx, nc, subject, &cfg)
}

func probePTY(ctx context.Context, nc *nats.Conn, subject string, cfg *AdmissionConfig) (*PTYClient, error) {
	sid, actor, nid, verb, ok := proto.ParseCmdBy(subject)
	if !ok || verb != proto.ProcessResumeVerb || nc == nil {
		return nil, fmt.Errorf("PTY client: invalid request")
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		return nil, err
	}
	inbox := nc.NewInbox()
	if !strings.HasPrefix(inbox, auth.InboxPrefixFor(actor)+".") {
		if cfg != nil {
			return nil, fmt.Errorf("PTY client: private inbox unavailable")
		}
		return nil, ErrUnsupported
	}
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = sub.Unsubscribe()
		}
	}()
	// The consumer has one in-flight RPC. Bound delayed replies as well as
	// business buffers, so a stalled destination cannot grow the inbox forever.
	if err := sub.SetPendingLimits(8, 8*proto.ProcessRequestMaxBytes); err != nil {
		return nil, err
	}
	t := &transport{nc: nc, sub: sub, subject: subject, inbox: inbox, admission: cfg}
	lineage := proc.NewPID()
	probeBudget := 2 * time.Second
	if cfg != nil {
		probeBudget, err = admissionRemaining(ctx, *cfg)
		if err != nil {
			return nil, err
		}
	}
	probeCtx, cancel := context.WithTimeout(ctx, probeBudget)
	defer cancel()
	var lease proto.ProcessLease
	for _, op := range []string{"probe", proto.ProcessOpPTYProbe} {
		capability, err := t.call(probeCtx, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion,
			Op: op, Kind: proto.ProcessKindPTY, ClientLineage: lineage})
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if ptyUnsupported(err) || errors.Is(err, nats.ErrNoResponders) {
				return nil, ErrUnsupported
			}
			return nil, err
		}
		l := capability.Lease
		// Validate the old capability too, before interpreting a missing version
		// as permission to fall back. A malformed or foreign lease proves nothing.
		if !validCapability(capability, sid, nid, fp, lineage) || l.LeaseID == "" || l.BootID == "" ||
			l.MAC == "" || l.IssuedBootNS < 0 || l.ExpiresBootNS <= l.IssuedBootNS {
			return nil, fmt.Errorf("PTY client: invalid capability response")
		}
		if capability.PTYVersion == 0 && op == "probe" {
			return nil, ErrUnsupported
		}
		if capability.PTYVersion != proto.PTYVersion || capability.TaskKind != proto.ProcessKindPTY ||
			(op == proto.ProcessOpPTYProbe && (l.TargetInstance != lease.TargetInstance || l.BootID != lease.BootID)) {
			return nil, fmt.Errorf("PTY client: invalid PTY capability response")
		}
		lease = *l
	}
	keep = true
	return &PTYClient{t: t, handle: proto.ProcessHandle{Lease: lease, RequestID: proc.NewPID()}, now: time.Now, phase: ptyProbing}, nil
}

func ptyUnsupported(err error) bool {
	var pe *ProtocolError
	return isUnsupported(err) || (errors.As(err, &pe) &&
		(pe.Code == proto.CodeProcessUnsupportedOp || (pe.Code == "" && pe.Message == "unsupported process operation")))
}

// Close cancels and joins an active Run, or releases an unused probe. It does
// not close the caller's shared NATS connection. Concurrent calls are safe.
func (c *PTYClient) Close() error {
	c.mu.Lock()
	c.closed = true
	cancel, done := c.cancel, c.done
	if cancel != nil {
		cancel()
	}
	if done == nil && c.t != nil {
		_ = c.t.sub.Unsubscribe()
	}
	c.mu.Unlock()
	if done != nil {
		<-done
	}
	return nil
}

// Run starts the probed intent exactly once. Source/Sink cancellation is a
// required ownership contract: no arbitrary Reader is hidden behind a leaked
// goroutine, and both workers are joined before this method returns.
func (c *PTYClient) Run(ctx context.Context, spec proto.RunReq, in Source, out Sink, events <-chan ControlEvent) (Result, error) {
	if in == nil || out == nil {
		return Result{}, fmt.Errorf("PTY client: cancellable input and output are required")
	}
	c.mu.Lock()
	if c.phase != ptyProbing || c.closed || c.t == nil {
		c.mu.Unlock()
		return Result{}, fmt.Errorf("PTY client: already used or closed")
	}
	c.phase = ptyCreating
	ctx, c.cancel = context.WithCancel(ctx)
	c.done = make(chan struct{})
	c.mu.Unlock()
	workers := newPTYWorkers(ctx, in, out)
	defer func() {
		c.cancel()
		_ = in.Cancel()
		_ = out.Cancel()
		workers.wg.Wait()
		_ = c.t.sub.Unsubscribe()
		c.mu.Lock()
		c.phase = ptyDone
		c.mu.Unlock()
		close(c.done)
	}()
	// Own the intent while retries are pending; caller edits to its slices or
	// map after construction must not change the same request's meaning.
	spec.Argv = append([]string(nil), spec.Argv...)
	if spec.Env != nil {
		env := make(map[string]string, len(spec.Env))
		for k, v := range spec.Env {
			env[k] = v
		}
		spec.Env = env
	}
	r := ptyRun{client: c, phase: ptyCreating, result: Result{Handle: c.handle}, workers: workers,
		input: ptyInput{data: make([]byte, 0, upgradebudget.PTYInputBytes)}, events: events, idle: idlePollMin}
	if spec.KAIntervalMS > 0 {
		r.kaInterval = time.Duration(max(1000, min(spec.KAIntervalMS, 60000))) * time.Millisecond
		r.nextKA = c.now().Add(r.kaInterval)
	}
	return r.run(ctx, &spec)
}

type ptyRead struct {
	data []byte
	err  error
}

type ptyWritten struct {
	n    int
	done bool
	err  error
}

type ptyWorkers struct {
	wg      sync.WaitGroup
	read    chan int
	reads   chan ptyRead
	write   chan []byte
	written chan ptyWritten
}

func newPTYWorkers(ctx context.Context, in Source, out Sink) *ptyWorkers {
	w := &ptyWorkers{read: make(chan int, 1), reads: make(chan ptyRead, 1),
		write: make(chan []byte, 1), written: make(chan ptyWritten, 1)}
	w.wg.Add(2)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case size := <-w.read:
				buf := make([]byte, size)
				n, err := in.Read(buf)
				if n < 0 || n > len(buf) {
					n, err = 0, fmt.Errorf("PTY client: invalid source read count")
				}
				select {
				case w.reads <- ptyRead{buf[:n], err}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case data := <-w.write:
				for len(data) > 0 {
					n, err := out.Write(data)
					if n < 0 || n > len(data) {
						n, err = 0, fmt.Errorf("PTY client: invalid destination write count")
					}
					if n == 0 && err == nil {
						err = io.ErrShortWrite
					}
					data = data[n:]
					select {
					case w.written <- ptyWritten{n, len(data) == 0 || err != nil, err}:
					case <-ctx.Done():
						return
					}
					if err != nil {
						break
					}
				}
			}
		}
	}()
	return w
}

// data retains [applied, readEnd), including bytes the server accepted but has
// not written. A read reservation counts against the same 256 KiB allowance.
// origin: hot-update S4 plan §3.2 / T40
type ptyInput struct {
	data                       []byte
	applied, accepted, sentEnd uint64
	window                     uint64
	reading, eof, fin, ended   bool
	finSent                    bool
	abort                      string
	pending                    *proto.ProcessResumeReq
}

// A/Q must be monotone and bounded by bytes this owner actually sent. Those
// checks precede subtraction and slicing: an invalid receipt must neither
// underflow the ring nor silently discard the client's byte responsibility.
// An ended input fixes both final counters, even when A remains below Q.
// origin: hot-update S4 plan §3.2 / T40 / T41
func (i *ptyInput) accept(reply proto.ProcessResumeReply) error {
	if reply.InputACK < i.applied || reply.InputAccepted < i.accepted || reply.InputACK > reply.InputAccepted ||
		reply.InputAccepted > i.sentEnd || reply.InputAccepted-reply.InputACK > upgradebudget.PTYInputBytes ||
		reply.InputWindow > upgradebudget.PTYInputBytes-(reply.InputAccepted-reply.InputACK) ||
		(reply.InputError != "" && !reply.InputEnded) || (i.ended && (!reply.InputEnded || reply.InputACK != i.applied || reply.InputAccepted != i.accepted)) ||
		(i.fin && !reply.InputFIN) || (reply.InputFIN && (!i.finSent || reply.InputAccepted != i.sentEnd)) ||
		(reply.InputEnded && reply.InputError == "" && (!reply.InputFIN || reply.InputACK != reply.InputAccepted)) {
		return fmt.Errorf("PTY client: invalid input receipt")
	}
	if i.abort != "" && reply.InputError != i.abort {
		return fmt.Errorf("PTY client: input abort changed")
	}
	// Accepted is credit, never delivery. Only this actual-write ACK releases
	// the retained prefix, including during a partial-write abort settlement.
	release := reply.InputACK - i.applied
	if release > uint64(len(i.data)) {
		return fmt.Errorf("PTY client: input ACK exceeds retained bytes")
	}
	copy(i.data, i.data[release:])
	i.data = i.data[:uint64(len(i.data))-release]
	i.applied, i.accepted, i.window = reply.InputACK, reply.InputAccepted, reply.InputWindow
	i.fin, i.ended, i.abort = reply.InputFIN, reply.InputEnded, reply.InputError
	if i.pending != nil && ((reply.InputAccepted >= i.pending.InputSeq+uint64(len(i.pending.Input)) &&
		(!i.pending.InputFIN || reply.InputFIN)) || reply.InputEnded) {
		i.pending = nil
	}
	return nil
}

type ptyRun struct {
	client      *PTYClient
	phase       ptyPhase
	result      Result
	workers     *ptyWorkers
	input       ptyInput
	output      receiver
	writing     *proto.ProcessOutput
	terminal    *proto.ProcessResumeReply
	control     *proto.ProcessResumeReq
	controlSeq  uint64
	events      <-chan ControlEvent
	kaInterval  time.Duration
	nextKA      time.Time
	recoveryEnd time.Time
	lane        int
	idle        time.Duration
}

func (r *ptyRun) base(op string) proto.ProcessResumeReq {
	return proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, Kind: proto.ProcessKindPTY, Op: op,
		Handle: &r.result.Handle, AttachEpoch: 1, StdoutACK: r.output.offset, StdoutFIN: r.output.fin, StderrFIN: true}
}

func (r *ptyRun) run(ctx context.Context, spec *proto.RunReq) (Result, error) {
	for {
		if err := ctx.Err(); err != nil {
			return r.snapshot(), err
		}
		if !r.recoveryEnd.IsZero() && !r.client.now().Before(r.recoveryEnd) {
			return r.snapshot(), fmt.Errorf("PTY client: recovery deadline exceeded; task may have executed")
		}
		if err := r.progress(ctx); err != nil {
			// A local write failure still owes its actual prefix. This is a
			// bounded best effort, not permission to wait forever on a bad sink.
			ack := r.base("ack")
			_, _ = r.client.t.attempt(ctx, ack)
			return r.snapshot(), err
		}
		req, err := r.next(spec)
		if err != nil {
			return r.snapshot(), err
		}
		attemptCtx := ctx
		cancel := func() {}
		if !r.recoveryEnd.IsZero() {
			attemptCtx, cancel = context.WithTimeout(ctx, max(0, r.recoveryEnd.Sub(r.client.now())))
		}
		admission := r.client.t.admission
		if admission != nil {
			remaining, err := admissionRemaining(attemptCtx, *admission)
			if err != nil {
				cancel()
				return r.snapshot(), err
			}
			outerCancel := cancel
			var admissionCancel context.CancelFunc
			attemptCtx, admissionCancel = context.WithTimeout(attemptCtx, remaining)
			cancel = func() { admissionCancel(); outerCancel() }
		}
		var reply proto.ProcessResumeReply
		if admission != nil {
			reply, err = admissionCall(attemptCtx, r.client.t, req, *admission)
		} else {
			reply, err = r.client.t.attempt(attemptCtx, req)
		}
		cancel()
		r.result.CreateSent = r.client.t.created
		if ctx.Err() != nil {
			return r.snapshot(), ctx.Err()
		}
		if !r.recoveryEnd.IsZero() && !r.client.now().Before(r.recoveryEnd) {
			return r.snapshot(), fmt.Errorf("PTY client: recovery deadline exceeded; task may have executed")
		}
		if err != nil {
			if r.phase == ptySettling && isExpiredSettlement(err) {
				return r.finish()
			}
			// origin: hot-update S4 plan §2.2 / T38
			// A first create's explicit busy receipt precedes Reserve. It is
			// an admission refusal. Recovery-pending has its own code and keeps
			// the original recovery window.
			var refusal *ProtocolError
			if r.phase == ptyCreating && r.recoveryEnd.IsZero() && errors.As(err, &refusal) && refusal.Code == proto.CodeProcessBusy {
				copy := *refusal
				copy.AfterCreate = false
				return r.snapshot(), &copy
			}
			if !r.result.CreateSent || !ptyRetryable(err) {
				// No post-create error, including unsupported, can authorize a
				// second command through the legacy path.
				return r.snapshot(), err
			}
			// origin: hot-update S4 plan §7.2 / T41
			if r.recoveryEnd.IsZero() {
				r.recoveryEnd = r.client.now().Add(upgradebudget.PTYRecovery)
			}
		} else {
			if err := r.accept(req, reply); err != nil {
				return r.snapshot(), err
			}
			// A validated starting receipt keeps the original admission deadline;
			// long-running PTY IO returns to the user's context at true start.
			// origin: hot-update S6 plan §8.2 / T57Cmd
			if r.client.t.admission != nil && reply.Phase != "starting" {
				r.client.t.admission = nil
			}
			// Only a fully validated status from the original runtime ends an
			// outage. A refusal or an unrelated delayed reply never restamps it.
			r.recoveryEnd = time.Time{}
			if r.phase == ptyDone {
				return r.finish()
			}
		}
		// Bounded fairness: even an immediate busy/offline refusal yields.
		// Workers wake this wait when bytes move; a quiet terminal backs off.
		if err := r.wait(ctx); err != nil {
			return r.snapshot(), err
		}
	}
}

func ptyRetryable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrNoResponders) || errors.Is(err, nats.ErrConnectionReconnecting) {
		return true
	}
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		return false
	}
	code := pe.Code
	if code == "" {
		code, _, _ = strings.Cut(pe.Message, ":")
	}
	// origin: hot-update S4 CLOSE admission contract / plan §7.2
	// Capacity and legacy busy refusals stay final even during an outage.
	return code == "node_offline" || code == "agent_upgrading" || code == proto.CodeProcessRecovering
}

func (r *ptyRun) next(spec *proto.RunReq) (proto.ProcessResumeReq, error) {
	if r.phase == ptyCreating {
		req := r.base("create")
		req.Run, req.StderrFIN = spec, false // create has no output acknowledgments
		return req, nil
	}
	if r.terminal != nil && r.output.ended() && r.input.ended && r.writing == nil {
		r.phase = ptySettling
		req := r.base("terminal-ack")
		req.TerminalID = r.terminal.TerminalID
		return req, nil
	}
	// Three rotating lanes bound priority in both directions: a stuck control
	// cannot starve output, and input retries cannot hide a due keepalive.
	for tries := 0; tries < 3; tries++ {
		lane := r.lane
		r.lane = (r.lane + 1) % 3
		switch lane {
		case 0:
			if r.terminal != nil {
				continue
			}
			if r.control == nil {
				var event ControlEvent
				if r.kaInterval > 0 && !r.client.now().Before(r.nextKA) {
					event.Op = proto.ProcessOpKeepalive
				} else {
					select {
					case e, ok := <-r.events:
						if !ok {
							r.events = nil
						}
						event = e
					default:
					}
				}
				if event.Op != "" {
					control, err := r.newControl(event)
					if err != nil {
						return proto.ProcessResumeReq{}, err
					}
					r.control = &control
				}
			}
			if r.control != nil {
				req := *r.control
				req.StdoutACK, req.StdoutFIN = r.output.offset, r.output.fin
				return req, nil
			}
		case 1:
			if r.input.ended {
				continue
			}
			if r.input.pending == nil {
				seq := r.input.accepted
				end := r.input.applied + uint64(len(r.input.data))
				size := min(end-seq, r.input.window, upgradebudget.PTYBatchBytes)
				fin := r.input.eof && seq+size == end && !r.input.fin
				if size > 0 || fin {
					req := r.base(proto.ProcessOpInput)
					req.InputSeq, req.InputFIN = seq, fin
					req.Input = append([]byte(nil), r.input.data[seq-r.input.applied:seq-r.input.applied+size]...)
					r.input.pending = &req
					r.input.sentEnd = max(r.input.sentEnd, seq+size)
					r.input.finSent = r.input.finSent || fin
				}
			}
			if r.input.pending != nil {
				req := *r.input.pending
				req.StdoutACK, req.StdoutFIN = r.output.offset, r.output.fin
				return req, nil
			}
		case 2:
			return r.base("ack"), nil
		}
	}
	return r.base("ack"), nil
}

func (r *ptyRun) newControl(event ControlEvent) (proto.ProcessResumeReq, error) {
	req := r.base(event.Op)
	switch event.Op {
	case proto.ProcessOpResize:
		if event.Cols == 0 || event.Rows == 0 || event.Signal != 0 {
			return req, fmt.Errorf("PTY client: invalid resize")
		}
		req.Resize = &proto.PtyResizeEvent{Cols: int(event.Cols), Rows: int(event.Rows)}
	case proto.ProcessOpSignal:
		if (event.Signal != 1 && event.Signal != 2 && event.Signal != 15) || event.Cols != 0 || event.Rows != 0 {
			return req, fmt.Errorf("PTY client: invalid signal")
		}
		req.Signal = event.Signal
	case proto.ProcessOpKeepalive, proto.ProcessOpHangup:
		if event.Signal != 0 || event.Cols != 0 || event.Rows != 0 {
			return req, fmt.Errorf("PTY client: invalid control payload")
		}
	default:
		return req, fmt.Errorf("PTY client: unsupported local control")
	}
	if r.controlSeq == ^uint64(0) {
		return req, fmt.Errorf("PTY client: control sequence exhausted")
	}
	r.controlSeq++
	req.ControlSeq = r.controlSeq
	return req, nil
}

func (r *ptyRun) accept(req proto.ProcessResumeReq, reply proto.ProcessResumeReply) error {
	if err := validateReply(&r.result, req, reply); err != nil {
		return err
	}
	if reply.TaskKind != proto.ProcessKindPTY || reply.Stderr == nil || reply.Stderr.Offset != 0 ||
		len(reply.Stderr.Data) != 0 || !reply.Stderr.FIN || reply.Stderr.Error != "" {
		return fmt.Errorf("PTY client: invalid merged output status")
	}
	// origin: hot-update S4 plan §3.3 / T36 / T37
	// Retirement releases input/output resources. A repeated terminal ACK
	// returns the registry's compact settlement receipt, not another input
	// snapshot. Only the exact terminal we already drained can discharge it;
	// retain our validated final A/Q and abort rather than accepting zeroes.
	if r.phase == ptySettling && reply.Phase == "settled" && reply.Stdout == nil && !reply.InputEnded {
		if r.terminal == nil || reply.TerminalID != req.TerminalID || reply.TerminalID != r.terminal.TerminalID ||
			reply.ExitCode != r.terminal.ExitCode || reply.Signal != r.terminal.Signal || reply.Error != r.terminal.Error ||
			reply.InputACK != 0 || reply.InputAccepted != 0 || reply.InputWindow != 0 || reply.InputFIN || reply.InputError != "" ||
			reply.ControlACK != 0 || reply.ControlError != "" {
			return fmt.Errorf("PTY client: invalid compact settlement")
		}
		r.phase = ptyDone
		return nil
	}
	if err := r.input.accept(reply); err != nil {
		return err
	}
	// There can be only one unconfirmed control. Anything beyond that seq, or
	// behind its immediate predecessor, cannot be this attachment's receipt.
	// origin: hot-update S4 plan §3.4
	if reply.ControlACK > r.controlSeq ||
		(r.control == nil && reply.ControlACK != r.controlSeq) ||
		(r.control != nil && reply.ControlACK+1 < r.controlSeq) {
		return fmt.Errorf("PTY client: invalid control receipt")
	}
	if r.control != nil && reply.ControlACK == r.control.ControlSeq {
		if reply.ControlError != "" {
			return fmt.Errorf("%s", reply.ControlError)
		}
		if r.control.Op == proto.ProcessOpKeepalive {
			r.nextKA = r.client.now().Add(r.kaInterval)
		}
		r.control = nil
	}
	if reply.Phase == "running" || reply.Signal != 0 || (reply.TerminalID != "" && reply.ExitCode >= 0) {
		r.result.Executed = true
	}
	if reply.TerminalID != "" {
		if (reply.Phase != "terminal" && reply.Phase != "settled") ||
			(r.terminal != nil && (r.terminal.TerminalID != reply.TerminalID || r.terminal.ExitCode != reply.ExitCode ||
				r.terminal.Signal != reply.Signal || r.terminal.Error != reply.Error)) {
			return fmt.Errorf("PTY client: terminal changed")
		}
		// Do not retain an entire output batch through this metadata pointer.
		terminal := reply
		terminal.Stdout, terminal.Stderr = nil, nil
		r.terminal = &terminal
	}
	if err := r.acceptOutput(reply.Stdout); err != nil {
		return err
	}
	if r.phase == ptySettling {
		if reply.Phase != "settled" || reply.TerminalID != req.TerminalID {
			return fmt.Errorf("PTY client: terminal was not settled")
		}
		r.phase = ptyDone
	} else if reply.Phase == "settled" {
		return fmt.Errorf("PTY client: premature settlement")
	} else {
		r.phase = ptyAttached
	}
	return nil
}

// A reply never grants an additional output worker. Validate the wire bound
// before copying, and fetch any replay suffix only after actual sink progress.
// origin: hot-update S4 plan §7.2 / T40
func (r *ptyRun) acceptOutput(batch *proto.ProcessOutput) error {
	if batch == nil {
		return nil
	}
	if len(batch.Data) > upgradebudget.PTYBatchBytes || batch.Offset > r.output.offset ||
		uint64(len(batch.Data)) > ^uint64(0)-batch.Offset || (batch.FIN && batch.Error != "") {
		return fmt.Errorf("PTY client: invalid output batch")
	}
	end := batch.Offset + uint64(len(batch.Data))
	if r.output.ended() && end > r.output.offset {
		return fmt.Errorf("PTY client: bytes after output end")
	}
	if r.writing != nil {
		// The server may replay the batch while the sink is blocked. Retain
		// exactly the original worker batch; the next ACK will fetch its suffix.
		return nil
	}
	if end <= r.output.offset {
		return r.output.accept(batch) // no Write: this validates end markers only
	}
	pending := *batch
	pending.Data = append([]byte(nil), batch.Data[r.output.offset-batch.Offset:]...)
	pending.Offset = r.output.offset
	r.writing = &pending
	r.workers.write <- pending.Data
	return nil
}

func (r *ptyRun) readResult(read ptyRead) error {
	r.input.reading = false
	if r.input.ended {
		return nil // a read already in flight may race the server's final abort
	}
	if len(read.data) > upgradebudget.PTYInputBytes-len(r.input.data) ||
		uint64(len(read.data)) > ^uint64(0)-r.input.applied-uint64(len(r.input.data)) {
		return fmt.Errorf("PTY client: input buffer exhausted")
	}
	r.input.data = append(r.input.data, read.data...)
	if errors.Is(read.err, io.EOF) {
		r.input.eof = true
	} else if read.err != nil {
		return read.err
	}
	return nil
}

func (r *ptyRun) writeResult(written ptyWritten) error {
	r.output.offset += uint64(written.n)
	if written.n > 0 {
		r.result.Executed = true
		r.idle = idlePollMin
	}
	if written.err != nil {
		return written.err
	}
	if written.done {
		r.output.fin, r.output.abort = r.writing.FIN, r.writing.Error
		r.writing = nil
	}
	return nil
}

func (r *ptyRun) progress(ctx context.Context) error {
	for {
		select {
		case read := <-r.workers.reads:
			if err := r.readResult(read); err != nil {
				return err
			}
		case written := <-r.workers.written:
			if err := r.writeResult(written); err != nil {
				return err
			}
		default:
			// Read only on server credit, after a valid status, with a bounded
			// reservation. A partition permits at most the already granted read.
			// origin: hot-update S4 plan §7.2 / T40
			if r.phase == ptyAttached && r.recoveryEnd.IsZero() && !r.input.reading && !r.input.eof && !r.input.ended &&
				r.input.pending == nil && r.input.accepted == r.input.applied+uint64(len(r.input.data)) {
				size := min(r.input.window, uint64(upgradebudget.PTYInputBytes-len(r.input.data)), upgradebudget.PTYBatchBytes)
				if size > 0 {
					r.input.reading = true
					select {
					case r.workers.read <- int(size):
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			return nil
		}
	}
}

func (r *ptyRun) wait(ctx context.Context) error {
	delay := r.idle
	if r.kaInterval > 0 && r.control == nil {
		delay = min(delay, max(idlePollMin, r.nextKA.Sub(r.client.now())))
	}
	if !r.recoveryEnd.IsZero() {
		delay = min(delay, max(0, r.recoveryEnd.Sub(r.client.now())))
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case read := <-r.workers.reads:
		return r.readResult(read)
	case written := <-r.workers.written:
		if err := r.writeResult(written); err != nil {
			_, _ = r.client.t.attempt(ctx, r.base("ack"))
			return err
		}
		return nil
	case <-timer.C:
		r.idle = min(r.idle*2, idlePollMax)
		return nil
	}
}

func (r *ptyRun) snapshot() Result {
	r.result.InputACK, r.result.InputAccepted, r.result.InputAbort = r.input.applied, r.input.accepted, r.input.abort
	r.result.StdoutBytes, r.result.StdoutAbort = r.output.offset, r.output.abort
	if r.terminal != nil {
		r.result.ExitCode, r.result.Signal, r.result.TerminalError = r.terminal.ExitCode, r.terminal.Signal, r.terminal.Error
	}
	return r.result
}

func (r *ptyRun) finish() (Result, error) {
	result := r.snapshot()
	// origin: hot-update S4 plan §3.2–§3.3 / T35 / T37
	// A last-slave end closes the input pump even when the local terminal is
	// still open. Preserve the wire diagnostic, but do not replace a real
	// remote exit when every byte this client sent/read was actually applied.
	// Hangup, write failures and any outstanding prefix remain aborts.
	if result.InputAbort == "pty input aborted: source_ended" &&
		r.input.applied == r.input.accepted && r.input.accepted == r.input.sentEnd && len(r.input.data) == 0 {
		result.InputAbort = ""
	}
	if result.TerminalError != "" || result.StdoutAbort != "" || result.InputAbort != "" {
		return result, &SettledError{Result: result}
	}
	return result, nil
}
