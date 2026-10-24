//go:build linux

package drillobserve

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve/infrastructure"

	"golang.org/x/sys/unix"
)

const custodySeals = unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL

type pending struct {
	event *Event
	flush chan struct{}
	exit  bool
}
type custody struct {
	fd, channel   int
	identity      [32]byte
	incarnation   [16]byte
	queue         chan pending
	broken        atomic.Bool
	failure       atomic.Uint64
	sequence      uint64
	samples       chan runtimeRequest
	exitDone      chan struct{}
	holdNative    bool
	exitReady     bool
	drainNonce    [16]byte
	drainSequence uint64
}

var active *custody

// Only fixed numeric causes leave private custody. Preserve the first failure
// so a later failed poison write cannot disguise a bounded-queue overflow.
// origin: hot-update S5 plan §6.6 / §9.4 / DRILL-RUN 75.C
func poisonCustody(c *custody, cause uint64) {
	c.failure.CompareAndSwap(0, cause)
	c.broken.Store(true)
}

func init() {
	enable, descriptor := os.Getenv(EnableEnv), os.Getenv(CustodyEnv)
	// No spawned workload may activate observation, including startup failures.
	_ = os.Unsetenv(EnableEnv)
	_ = os.Unsetenv(CustodyEnv)
	if enable != "v1" {
		return
	}
	fd, err := strconv.Atoi(descriptor)
	if err != nil || fd < 3 {
		return
	}
	c := inheritedCustody(fd)
	if c == nil {
		return
	}
	active = c
	Hook = c.record
	go c.writeLoop()
}

// A descriptor number alone is never authority. Require an immutable memfd,
// exact versioned content, and its inherited UNIX seqpacket custody endpoint.
// The observer creates neither a socket nor an ordinary file.
// origin: hot-update S5 plan §6.6 / T26
func inheritedCustody(fd int) *custody {
	seals, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
	if err != nil || seals&custodySeals != custodySeals {
		return nil
	}
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Size != 32 {
		return nil
	}
	b := make([]byte, 32)
	if n, err := unix.Pread(fd, b, 0); err != nil || n != len(b) || (string(b[:8]) != "TDCFG1\x00\x00" && string(b[:8]) != "TDCFG2\x00\x00") {
		return nil
	}
	socketFD := int(binary.LittleEndian.Uint64(b[8:16]))
	if socketFD < 3 || socketFD == fd {
		return nil
	}
	family, err := unix.GetsockoptInt(socketFD, unix.SOL_SOCKET, unix.SO_DOMAIN)
	if err != nil || family != unix.AF_UNIX {
		return nil
	}
	typ, err := unix.GetsockoptInt(socketFD, unix.SOL_SOCKET, unix.SO_TYPE)
	if err != nil || typ != unix.SOCK_SEQPACKET {
		return nil
	}
	if _, err := unix.Getpeername(socketFD); err != nil {
		return nil
	}
	c := &custody{fd: fd, channel: socketFD, identity: sha256.Sum256(b), queue: make(chan pending, queueDepth), samples: make(chan runtimeRequest, maxBatch), exitDone: make(chan struct{}), holdNative: string(b[:8]) == "TDCFG2\x00\x00"}
	if _, err := rand.Read(c.incarnation[:]); err != nil {
		return nil
	}
	unix.CloseOnExec(fd)
	unix.CloseOnExec(socketFD)
	return c
}

// A dropped or unrepresentable event destroys the ledger's completeness; it
// poisons custody instead of making product IO fail or waiting for the keeper.
// origin: hot-update S5 plan §6.2 / §6.6 / T26 / DRILL-OBSERVER
func (c *custody) record(e Event) {
	if c.broken.Load() {
		return
	}
	if len(e.Kind) == 0 || len(e.Kind) > 63 || len(e.Domain) > 31 || len(e.Data) > MaxData || !validCustodyVocabulary(e.Kind, e.Domain) {
		poisonCustody(c, 4)
		return
	}
	// A malformed runtime receipt is sticky poison even when no judge retains
	// event objects. Do not accept a missing nonce or foreign domain as a tick.
	// origin: hot-update S10 plan §5.3 / P06
	if e.Kind == RuntimeSampleKind {
		if _, err := DecodeRuntimeSample(e); err != nil {
			poisonCustody(c, 16)
			return
		}
	}
	var now unix.Timespec
	if unix.ClockGettime(unix.CLOCK_BOOTTIME, &now) != nil {
		poisonCustody(c, 5)
		return
	}
	if e.Kind != RuntimeSampleKind {
		e.At = uint64(now.Nano())
	}
	// Copy before returning to the actor: its next primitive may reuse buffers.
	e.Data = append([]byte(nil), e.Data...)
	select {
	case c.queue <- pending{event: &e}:
	default:
		poisonCustody(c, 2)
	}
}
func (c *custody) writeLoop() {
	if c.exitDone != nil {
		defer close(c.exitDone)
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	batch := make([]Event, 0, maxBatch)
	requests := make(map[[12]byte]runtimeRequest)
	for {
		select {
		case p := <-c.queue:
			writeCustodyPending(c, p, &batch, requests)
		case <-ticker.C:
			var request [48]byte
			n, _, flags, _, err := unix.Recvmsg(c.channel, request[:], nil, unix.MSG_DONTWAIT)
			// origin: hot-update S7 plan §10.4 / T41
			if err == nil && (n == 0 || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0) {
				poisonCustody(c, 12)
				return
			}
			// This remains the only socket reader. The sampler consumes a bounded
			// mailbox, so a second Recv cannot steal a drain or fresh challenge.
			// origin: hot-update S10 plan §5.3 / P06
			if err == nil && n >= 4 && string(request[:4]) == "TDS1" {
				value, parseErr := decodeRuntimeRequest(request[:n])
				var now unix.Timespec
				_, duplicate := requests[value.nonce]
				if parseErr != nil || duplicate || len(requests) == maxBatch || unix.ClockGettime(unix.CLOCK_BOOTTIME, &now) != nil {
					poisonCustody(c, 17)
				} else {
					value.at = uint64(now.Nano())
					select {
					case c.samples <- value:
						requests[value.nonce] = value
					default:
						poisonCustody(c, 17)
					}
				}
			} else if err == nil && n == 28 && string(request[:4]) == "TDR2" {
				// Release consumes the exact final drain, after its sealed batch
				// and ACK crossed custody. EOF can never satisfy this predicate.
				// origin: S7 internal review rv-drill F1
				if c.exitReady && !c.broken.Load() && c.drainSequence != 0 &&
					string(request[4:20]) == string(c.drainNonce[:]) &&
					binary.LittleEndian.Uint64(request[20:28]) == c.drainSequence && c.sequence == c.drainSequence {
					return
				}
				poisonCustody(c, 19)
			} else if err == nil && (n != 4 || string(request[:n]) != "TDP1") && !transferDrainRequest(request[:n]) && !infrastructureRequest(c, request[:n], &batch, requests) {
				poisonCustody(c, 12)
				// Malformed controls end custody just like a truncated packet; no
				// subsequent drain can restore its completeness.
				// origin: S7 internal review rv-drill F1
				// A malformed transfer drain destroys the only reader's custody;
				// terminate it instead of retaining a poisoned writer indefinitely.
				// origin: S7 internal review INTEG-M F2 / T41
				return
			} else if err != nil && err != unix.EAGAIN && err != unix.EINTR {
				poisonCustody(c, 12)
				return
			}
			if err == nil && (n == 4 && string(request[:n]) == "TDP1" || transferDrainRequest(request[:n])) {
				// A custody judge needs a current barrier, including any earlier
				// queue overflow whose poison packet could not yet be delivered.
				for i := 0; i < queueDepth; i++ {
					select {
					case p := <-c.queue:
						writeCustodyPending(c, p, &batch, requests)
					default:
						i = queueDepth
					}
				}
				c.send(batch)
				clear(batch)
				batch = batch[:0]
				reply := "TDOK"
				if c.broken.Load() {
					reply = "TDBR"
				}
				// origin: hot-update S7 plan §10.4 / T41
				packet := []byte(reply)
				if transferDrainRequest(request[:n]) {
					packet = transferDrainReply(c, request[:n])
					copy(c.drainNonce[:], request[4:20])
					c.drainSequence = c.sequence
				}
				if err := unix.Sendmsg(c.channel, packet, nil, nil, unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL); err != nil {
					poisonCustody(c, 20)
				}
			}
			if len(batch) > 0 || c.broken.Load() {
				c.send(batch)
				clear(batch)
				batch = batch[:0]
			}
		}
	}
}

// EAGAIN leaves ownership here: no descriptor or sequence has crossed the
// channel yet. Retain this one sealed batch while the private writer waits for
// writable capacity. Record stays nonblocking; its original bounded queue and
// Flush's original deadline can poison this wait and force descriptor release.
// origin: hot-update S5 plan §6.6 / §9.1 / DRILL-RUN 75.C
func sendCustodyDescriptor(c *custody, fd int) error {
	rights := unix.UnixRights(fd)
	for {
		err := unix.Sendmsg(c.channel, []byte("TDC1"), rights, nil, unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL)
		if err == unix.EINTR {
			continue
		}
		if err != unix.EAGAIN {
			return err
		}
		if c.broken.Load() {
			return unix.ECANCELED
		}
		_, err = unix.Poll([]unix.PollFd{{Fd: int32(c.channel), Events: unix.POLLOUT}}, 20)
		if err != nil && err != unix.EINTR {
			return err
		}
		if c.broken.Load() {
			return unix.ECANCELED
		}
	}
}

func (c *custody) send(batch []Event) {
	c.sequence++
	header := append([]byte("TDRILL1\x00"), c.identity[:]...)
	header = append(header, c.incarnation[:]...)
	header = binary.LittleEndian.AppendUint64(header, c.sequence)
	var broken uint64
	if c.broken.Load() {
		broken = max(c.failure.Load(), 1)
	}
	header = binary.LittleEndian.AppendUint64(header, broken)
	header = append(header, encodeEvents(batch)...)
	fd, err := unix.MemfdCreate("tether-drill-custody", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		poisonCustody(c, 6)
		return
	}
	defer func() {
		if err := unix.Close(fd); err != nil {
			poisonCustody(c, 9)
		}
	}()
	if n, err := unix.Write(fd, header); err != nil || n != len(header) {
		poisonCustody(c, 7)
		return
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, custodySeals); err != nil {
		poisonCustody(c, 8)
		return
	}
	if err := sendCustodyDescriptor(c, fd); err != nil {
		poisonCustody(c, 3)
	}
}

// Flush seals queued attempts at a judge or exec barrier. A broken or stalled
// keeper never fails product IO; it loses the right to sign a GREEN receipt.
// This is called only outside product locks, never for each read/write.
func Flush() {
	if active == nil {
		return
	}
	done := make(chan struct{})
	select {
	case active.queue <- pending{flush: done}:
	default:
		poisonCustody(active, 10)
		return
	}
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		poisonCustody(active, 11)
	}
}

// ExecEnvironment restores the two startup enablers only for same-PID exec,
// after the image gate has drained child admission. All workload paths strip
// them. CLOEXEC is reinstated by the successor's startup validation.
func ExecEnvironment(env []string) []string {
	env = StripEnvironment(env)
	if active == nil {
		return env
	}
	Flush()
	if active.broken.Load() {
		return env
	}
	_, _ = unix.FcntlInt(uintptr(active.fd), unix.F_SETFD, 0)
	_, _ = unix.FcntlInt(uintptr(active.channel), unix.F_SETFD, 0)
	return append(env, EnableEnv+"=v1", CustodyEnv+"="+strconv.Itoa(active.fd))
}

// A legacy drain can dequeue a runtime sample too. Both paths must seal its
// event before replying, or the fresh challenge would disappear inside Flush.
// origin: hot-update S10 plan §5.3 / P06
func writeCustodyPending(c *custody, p pending, batch *[]Event, requests map[[12]byte]runtimeRequest) {
	if p.event != nil {
		*batch = append(*batch, *p.event)
	}
	var sample *RuntimeCounters
	if p.event != nil && p.event.Kind == RuntimeSampleKind {
		if request, ok := requests[p.event.Nonce]; ok {
			value, err := DecodeRuntimeSample(*p.event)
			if err != nil || value.ScopeDigest != hex.EncodeToString(request.scope[:]) || value.AtBootNS < request.at {
				poisonCustody(c, 16)
			} else {
				sample = &value
			}
			delete(requests, p.event.Nonce)
		}
	}
	if len(*batch) == maxBatch || p.flush != nil || sample != nil || p.exit {
		c.send(*batch)
		clear(*batch)
		*batch = (*batch)[:0]
	}
	if sample != nil {
		reply := runtimeSampleReply(c, *sample)
		if err := unix.Sendmsg(c.channel, reply, nil, nil, unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL); err != nil {
			poisonCustody(c, 18)
		}
	}
	if p.exit {
		c.exitReady = true
	}
	if p.flush != nil {
		close(p.flush)
	}
}

func runtimeSampleReply(c *custody, sample RuntimeCounters) []byte {
	scope, _ := hex.DecodeString(sample.ScopeDigest) // Already validated before enqueue.
	reply := append(append([]byte("TDSA"), sample.Nonce[:]...), scope...)
	reply = binary.LittleEndian.AppendUint64(reply, sample.Sequence)
	reply = binary.LittleEndian.AppendUint64(reply, c.sequence)
	failure := uint64(0)
	if c.broken.Load() {
		failure = max(c.failure.Load(), 1)
	}
	return binary.LittleEndian.AppendUint64(reply, failure)
}

func takeRuntimeRequest() (runtimeRequest, bool) {
	if active != nil {
		select {
		case request := <-active.samples:
			return request, true
		default:
		}
	}
	return runtimeRequest{}, false
}

func poisonRuntimeSamples() {
	if active != nil {
		poisonCustody(active, 16)
	}
}

// /proc/self/exe describes executing bytes even after a binary path is flipped.
// Cache only immutable process/image identity; each tick measures its own G/FD.
// origin: hot-update S10 plan §5.3 / P06
var runtimeIdentityCache struct {
	sync.Once
	c   RuntimeCounters
	err error
}

func runtimeSampleIdentity(b RuntimeBinding) (RuntimeCounters, error) {
	runtimeIdentityCache.Do(func() {
		c := &runtimeIdentityCache.c
		c.PID = uint64(os.Getpid())
		boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
		if err != nil {
			runtimeIdentityCache.err = ErrRuntimeSample
			return
		}
		c.BootID = strings.TrimSpace(string(boot))
		c.PIDNS, err = os.Readlink("/proc/self/ns/pid")
		if err != nil {
			runtimeIdentityCache.err = ErrRuntimeSample
			return
		}
		stat, err := os.ReadFile("/proc/self/stat")
		end := strings.LastIndexByte(string(stat), ')')
		if err != nil || end < 0 {
			runtimeIdentityCache.err = ErrRuntimeSample
			return
		}
		fields := strings.Fields(string(stat)[end+1:])
		if len(fields) < 20 {
			runtimeIdentityCache.err = ErrRuntimeSample
			return
		}
		c.StartTicks, err = strconv.ParseUint(fields[19], 10, 64)
		if err != nil || c.StartTicks == 0 {
			runtimeIdentityCache.err = ErrRuntimeSample
			return
		}
		exe, err := os.Open("/proc/self/exe")
		if err != nil {
			runtimeIdentityCache.err = ErrRuntimeSample
			return
		}
		hash := sha256.New()
		_, readErr := io.Copy(hash, exe)
		closeErr := exe.Close()
		if readErr != nil || closeErr != nil {
			runtimeIdentityCache.err = ErrRuntimeSample
			return
		}
		copy(c.ExeSHA[:], hash.Sum(nil))
	})
	c := runtimeIdentityCache.c
	c.Binding, c.Unknown = b, RuntimeUnknownCounters
	if runtimeIdentityCache.err != nil || b.Domain != "agent" || !runtimeToken(b.InstanceID, false) {
		return RuntimeCounters{}, ErrRuntimeSample
	}
	return c, nil
}

func fillRuntimeSample(c *RuntimeCounters) error {
	var now unix.Timespec
	if unix.ClockGettime(unix.CLOCK_BOOTTIME, &now) != nil {
		return ErrRuntimeSample
	}
	c.AtBootNS = uint64(now.Nano())
	c.Goroutines = uint64(runtime.NumGoroutine())
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil || len(entries) < 2 {
		return ErrRuntimeSample
	}
	// ReadDir's temporary directory FD appears in its census but is now closed.
	c.FDs = uint64(len(entries) - 1)
	return nil
}

// FinishNativeExit preserves the command's actual result while custody remains
// available for the judge's later barriers. Only startup-authenticated custody
// can delay exit; ordinary invocations return immediately. A failed observer
// never changes the native code or invents the final acknowledgement.
// origin: S7 internal review rv-drill F1 / observer exit-release handoff
func finishNativeExit(code int) {
	c := active
	// The sealed v2 configuration explicitly opts into S7 release. Legacy
	// S5/S6 observers have no TDR2 sender and retain their normal native exit.
	// origin: S7 internal review rv-drill F1 / custody compatibility
	if c == nil || !c.holdNative || c.exitDone == nil || c.broken.Load() {
		return
	}
	identity, err := TransferProcessIdentity(1, "native-exit")
	if err != nil {
		poisonCustody(c, 21)
		return
	}
	value := struct {
		PID          int    `json:"pid"`
		ProcessStart uint64 `json:"process_start"`
		BootID       string `json:"boot_id"`
		NativeRC     int    `json:"native_rc"`
	}{identity.PID, identity.ProcessStart, identity.BootID, code}
	body, _ := json.Marshal(value)
	var now unix.Timespec
	if unix.ClockGettime(unix.CLOCK_BOOTTIME, &now) != nil {
		poisonCustody(c, 21)
		return
	}
	event := Event{Kind: "native-exit", Domain: "exec", At: uint64(now.Nano()), Data: body}
	select {
	case c.queue <- pending{event: &event, exit: true}:
	case <-c.exitDone:
		return
	case <-time.After(100 * time.Millisecond):
		poisonCustody(c, 10)
		return
	}
	<-c.exitDone
}

// origin: S10 internal review MAKETEST-GATES F8
func infrastructurePlatformSupported() bool { return runtime.GOARCH == "amd64" }

func infrastructureRequest(c *custody, request []byte, batch *[]Event, requests map[[12]byte]runtimeRequest) bool {
	return infrastructure.Request(request, SampleInfrastructure[int64], transferNow, func() (uint64, uint64) {
		// Sample calls only enqueue. Seal those rows before acknowledging the
		// challenge, using the same single reader and sequence as every other drain.
		for i := 0; i < queueDepth; i++ {
			select {
			case p := <-c.queue:
				writeCustodyPending(c, p, batch, requests)
			default:
				i = queueDepth
			}
		}
		c.send(*batch)
		clear(*batch)
		*batch = (*batch)[:0]
		if c.broken.Load() {
			return c.sequence, max(c.failure.Load(), 1)
		}
		return c.sequence, 0
	}, func(reply []byte) {
		if unix.Sendmsg(c.channel, reply, nil, nil, unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL) != nil {
			poisonCustody(c, 20)
		}
	})
}
