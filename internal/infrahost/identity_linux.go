//go:build linux

// Package infrahost observes kernel identities independently of product status.
// origin: hot-update S9 plan §2.3 / T05
package infrahost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"golang.org/x/sys/unix"
)

// ProcessPin owns a CLOEXEC pidfd until Close. A PID alone may address a successor;
// callers retain this pin across their journal intent and actual signal.
type ProcessPin struct {
	fd       int
	expected infracontract.Process
}

// origin: hot-update S9 plan §2.1 / T05
func identityBudget(deadline infracontract.Deadline, stop infracontract.Stop) error {
	if runtime.GOARCH != "amd64" {
		return infracontract.ErrUnsupported
	}
	select {
	case <-stop:
		return context.Canceled
	default:
	}
	_, err := controlbudget.Remaining(deadline)
	return err
}

// origin: hot-update S9 plan §2.3 / T05
func validProcess(p infracontract.Process) bool {
	// Invocation IDs are systemd's exact lowercase 128-bit spelling. PID/start
	// ticks distinguish process births, while invocation, inode and SHA prevent
	// unit reuse and same-PID exec from borrowing a predecessor's authority.
	invocation, err := hex.DecodeString(p.InvocationID)
	return err == nil && len(invocation) == 16 && strings.ToLower(p.InvocationID) == p.InvocationID &&
		p.BootID != "" && p.PID > 0 && p.StartTicks > 0 && p.ExeDevice > 0 && p.ExeInode > 0 &&
		p.ExeSHA256 != (infracontract.Digest{}) && strings.HasPrefix(p.Cgroup, "/")
}

// A process can be moved outside a unit's cgroup. Enumerating only the configured
// paths would then falsely prove exit while the original image remains alive.
// Pin every predecessor independently, including when its old cgroup disappeared.
// A dead pidfd or a proven recycled PID is absence; unreadable live identity is
// uncertainty. Cross-boot census reuse never carries retirement authority.
// origin: hot-update S9 plan §2.3 / T05
func verifyBeforeCoverage(before, processes []infracontract.Process, deadline infracontract.Deadline) error {
	if len(processes) == 0 || len(before) == 0 {
		return infracontract.ErrIncomplete
	}
	for _, old := range before {
		if !validProcess(old) || old.BootID != processes[0].BootID {
			return infracontract.ErrIncomplete
		}
		if err := identityBudget(deadline, nil); err != nil {
			return err
		}
		fd, err := unix.PidfdOpen(int(old.PID), 0)
		if errors.Is(err, unix.ESRCH) {
			continue
		}
		if err != nil {
			return identityError(err)
		}
		actual, readErr := readPinnedProcess(old.PID, fd, deadline)
		poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		_, pollErr := unix.Poll(poll, 0)
		_ = unix.Close(fd)
		if pollErr != nil || poll[0].Revents&(unix.POLLERR|unix.POLLNVAL) != 0 {
			return infracontract.ErrIncomplete
		}
		if poll[0].Revents&unix.POLLIN != 0 {
			continue
		}
		if readErr != nil {
			return readErr
		}
		if actual.StartTicks != old.StartTicks {
			continue
		}
		if !slices.Contains(processes, actual) {
			return infracontract.ErrIncomplete
		}
	}
	return nil
}

// ReadProcess hashes the open /proc exe inode, including a deleted image. Hashing
// the installation pathname would certify disk staging while the old server runs.
// Missing systemd invocation provenance is incomplete, never an anonymous owner.
// origin: hot-update S9 plan §2.1–§2.3 / T05
func ReadProcess(pid int32, deadline infracontract.Deadline) (infracontract.Process, error) {
	if err := identityBudget(deadline, nil); err != nil {
		return infracontract.Process{}, err
	}
	if pid <= 0 {
		return infracontract.Process{}, infracontract.ErrIncomplete
	}
	fd, err := unix.PidfdOpen(int(pid), 0)
	if err != nil {
		return infracontract.Process{}, identityError(err)
	}
	defer func() { _ = unix.Close(fd) }()
	return readPinnedProcess(pid, fd, deadline)
}

// origin: hot-update S9 plan §2.3 / T05
func identityError(err error) error {
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) {
		return infracontract.ErrUnsupported
	}
	// Do not include proc contents, executable paths or environment in errors.
	return infracontract.ErrIncomplete
}

// origin: hot-update S9 plan §2.3 / T05
func readPinnedProcess(pid int32, fd int, deadline infracontract.Deadline) (infracontract.Process, error) {
	root := filepath.Join("/proc", strconv.Itoa(int(pid)))
	before, err := processTicks(root)
	if err != nil {
		return infracontract.Process{}, err
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return infracontract.Process{}, identityError(err)
	}
	env, err := os.ReadFile(root + "/environ")
	if err != nil {
		return infracontract.Process{}, identityError(err)
	}
	var invocation string
	invocationFound := false
	for _, value := range strings.Split(string(env), "\x00") {
		if strings.HasPrefix(value, "INVOCATION_ID=") {
			// An empty first occurrence still makes a later spelling ambiguous.
			if invocationFound {
				return infracontract.Process{}, infracontract.ErrIncomplete
			}
			invocationFound = true
			invocation = strings.TrimPrefix(value, "INVOCATION_ID=")
		}
	}
	cgroup, err := os.ReadFile(root + "/cgroup")
	if err != nil {
		return infracontract.Process{}, identityError(err)
	}
	lines := strings.Split(strings.TrimSpace(string(cgroup)), "\n")
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "0::/") {
		return infracontract.Process{}, infracontract.ErrUnsupported
	}
	exe, err := os.Open(root + "/exe")
	if err != nil {
		return infracontract.Process{}, identityError(err)
	}
	defer func() { _ = exe.Close() }()
	var stat unix.Stat_t
	if err = unix.Fstat(int(exe.Fd()), &stat); err != nil {
		return infracontract.Process{}, identityError(err)
	}
	hash := sha256.New()
	buffer := make([]byte, 64<<10)
	for {
		if err = identityBudget(deadline, nil); err != nil {
			return infracontract.Process{}, err
		}
		n, readErr := exe.Read(buffer)
		_, _ = hash.Write(buffer[:n])
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return infracontract.Process{}, identityError(readErr)
		}
	}
	result := infracontract.Process{BootID: strings.TrimSpace(string(boot)), PID: pid, StartTicks: before,
		InvocationID: invocation, ExeDevice: uint64(stat.Dev), ExeInode: stat.Ino, Cgroup: strings.TrimPrefix(lines[0], "0::")}
	copy(result.ExeSHA256[:], hash.Sum(nil))
	// Exec can preserve PID/start ticks. Reopen exe after hashing and check its
	// inode as well, so a mixed observation cannot become the expected identity.
	after, err := processTicks(root)
	var final unix.Stat_t
	if err != nil || after != before || unix.Stat(root+"/exe", &final) != nil || final.Dev != stat.Dev || final.Ino != stat.Ino || !validProcess(result) {
		return infracontract.Process{}, infracontract.ErrIncomplete
	}
	poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	if _, err = unix.Poll(poll, 0); err != nil || poll[0].Revents != 0 {
		return infracontract.Process{}, infracontract.ErrIncomplete
	}
	return result, nil
}

// origin: hot-update S9 plan §2.3 / T05
func processTicks(root string) (uint64, error) {
	// comm may itself contain spaces and parentheses, so split only after its
	// final closing delimiter. Fields are relative to state (stat field 3);
	// starttime is then index 19. Zombies have exited but cannot supply a complete
	// running image; callers must prove death through their independently held fd.
	body, err := os.ReadFile(root + "/stat")
	if err != nil {
		return 0, identityError(err)
	}
	end := strings.LastIndexByte(string(body), ')')
	if end < 0 {
		return 0, infracontract.ErrIncomplete
	}
	fields := strings.Fields(string(body[end+1:]))
	if len(fields) < 20 || fields[0] == "Z" || fields[0] == "X" {
		return 0, infracontract.ErrIncomplete
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || ticks == 0 {
		return 0, infracontract.ErrIncomplete
	}
	return ticks, nil
}

// PinProcess must run before recording any signal intent. Every field, including
// invocation and running image, is compared again through the held pidfd.
// origin: hot-update S9 plan §2.1 / T05
func PinProcess(expected infracontract.Process, deadline infracontract.Deadline) (*ProcessPin, error) {
	if err := identityBudget(deadline, nil); err != nil {
		return nil, err
	}
	if !validProcess(expected) {
		return nil, infracontract.ErrIncomplete
	}
	fd, err := unix.PidfdOpen(int(expected.PID), 0)
	if err != nil {
		return nil, identityError(err)
	}
	actual, err := readPinnedProcess(expected.PID, fd, deadline)
	if err != nil || actual != expected {
		_ = unix.Close(fd)
		return nil, infracontract.ErrIncomplete
	}
	return &ProcessPin{fd: fd, expected: expected}, nil
}

// Close releases only the observer's pidfd; it never changes the server lifetime.
// origin: hot-update S9 plan §12 P02 / T05
func (p *ProcessPin) Close() error {
	if p == nil || p.fd < 0 {
		return nil
	}
	fd := p.fd
	p.fd = -1
	return unix.Close(fd)
}

// Signal uses the frozen semantic operations rather than a caller-selected signal.
// NATS stop is SIGKILL; Caddy graceful stop is SIGTERM; LDM is SIGUSR2.
// A same-PID exec after Pin is rejected before it can receive the predecessor's signal.
// origin: hot-update S9 plan §4.2 / §5.3 / T05
func (p *ProcessPin) Signal(op infracontract.HostOp, deadline infracontract.Deadline, stop infracontract.Stop) error {
	if err := identityBudget(deadline, stop); err != nil {
		return err
	}
	if p == nil || p.fd < 0 {
		return infracontract.ErrIncomplete
	}
	var signal unix.Signal
	switch op {
	case infracontract.SignalLDM:
		signal = unix.SIGUSR2
	case infracontract.SignalStop:
		signal = unix.SIGKILL
	case infracontract.SignalGraceful:
		signal = unix.SIGTERM
	case infracontract.Inspect, infracontract.Stage, infracontract.FenceStart,
		infracontract.Promote, infracontract.StartWarm, infracontract.SwitchIngress,
		infracontract.FinalizeStorage, infracontract.ReleaseStart, infracontract.RetireWarm,
		infracontract.RecoverHost, infracontract.RotatePolicy, infracontract.UpdateSupport,
		infracontract.LaunchClaim, infracontract.LaunchAck, infracontract.VerifyIngress:
		// origin: hot-update S9 plan §10.2 / HOSTOP-SWITCHES
		return infracontract.ErrUnsupported
	default:
		return infracontract.ErrUnsupported
	}
	actual, err := readPinnedProcess(p.expected.PID, p.fd, deadline)
	if err != nil || actual != p.expected {
		return infracontract.ErrIncomplete
	}
	if err = identityBudget(deadline, stop); err != nil {
		return err
	}
	return unix.PidfdSendSignal(p.fd, signal, nil, 0)
}

// Exited tests the pinned incarnation, even after its numeric PID is recycled.
// origin: hot-update S9 plan §2.3 / T05
func (p *ProcessPin) Exited(deadline infracontract.Deadline) (bool, error) {
	if err := identityBudget(deadline, nil); err != nil {
		return false, err
	}
	if p == nil || p.fd < 0 {
		return false, infracontract.ErrIncomplete
	}
	poll := []unix.PollFd{{Fd: int32(p.fd), Events: unix.POLLIN}}
	if _, err := unix.Poll(poll, 0); err != nil {
		return false, identityError(err)
	}
	if poll[0].Revents&(unix.POLLERR|unix.POLLNVAL) != 0 {
		return false, infracontract.ErrIncomplete
	}
	return poll[0].Revents&unix.POLLIN != 0, nil
}

// VerifyRetired compares this step's before set, never a global old binary SHA.
// Only launcher/support exec may retire an image in place; external servers must
// actually exit. Live transferred business is allowed after old responsibility is zero.
// origin: hot-update S9 plan §2.3 / T05
func VerifyRetired(before []infracontract.Process, census infracontract.Census, allowExec bool) error {
	if runtime.GOARCH != "amd64" {
		return infracontract.ErrUnsupported
	}
	if len(before) == 0 || validateCensus(census) != nil || census.OldOwnerPending != 0 {
		return infracontract.ErrIncomplete
	}
	for _, old := range before {
		if !validProcess(old) || old.BootID != census.Processes[0].BootID {
			return infracontract.ErrIncomplete
		}
		for _, current := range census.Processes {
			if old.BootID == current.BootID && old.PID == current.PID && old.StartTicks == current.StartTicks {
				if !allowExec || old.InvocationID != current.InvocationID ||
					(old.ExeDevice == current.ExeDevice && old.ExeInode == current.ExeInode && old.ExeSHA256 == current.ExeSHA256) {
					return infracontract.ErrIncomplete
				}
			}
		}
	}
	return nil
}

// identityBackend is an action adapter, not an authority boundary. P04 journals
// the effect and P03 authenticates requests before dispatch; neither is bypassed
// by binding this backend to Inspect and the three fixed signal operations.
type identityBackend struct {
	scope        CensusScope
	mu           sync.Mutex
	observations CensusSequence
	read         func(CensusScope, infracontract.Deadline) (infracontract.Census, error)
}

// CensusSequence belongs to a trusted physical producer, not its consumers.
// The source serializes sampling with Observe. Only complete observations enter
// this sequence; equal observations retain their digest so a later signal can
// revalidate the exact signed census. A changed observation gets a boot-clock
// generation, which cannot roll back when a helper is reconstructed this boot.
// It contains no process/socket data and is not a substitute for kernel sampling.
// origin: S9 internal review rv-infra F06
type CensusSequence struct {
	generation uint64
	content    infracontract.Digest
}

// Observe finalizes an independently collected, complete census. The unit owner
// must keep this object across requests and serialize all observations with the
// underlying sample. Callers cannot nominate the published generation.
func (s *CensusSequence) Observe(census infracontract.Census) (infracontract.Census, error) {
	if runtime.GOARCH != "amd64" {
		return infracontract.Census{}, infracontract.ErrUnsupported
	}
	if err := validateCensus(census); err != nil {
		return infracontract.Census{}, err
	}
	census.Generation = 1
	content := censusDigest(census)
	if s.generation == 0 || s.content != content {
		now, err := controlbudget.Now()
		if err != nil || now <= 0 || s.generation == ^uint64(0) {
			return infracontract.Census{}, infracontract.ErrIncomplete
		}
		s.generation, s.content = max(uint64(now), s.generation+1), content
	}
	census.Generation = s.generation
	census.Digest = censusDigest(census)
	return census, nil
}

var _ infracontract.Host = (*identityBackend)(nil)

// NewIdentityBackend copies the domain scope so caller slice mutation cannot
// widen a live backend to another unit or predecessor incarnation.
// origin: hot-update S9 plan §2.1 / T05
func NewIdentityBackend(scope CensusScope) (infracontract.Host, error) {
	if runtime.GOARCH != "amd64" {
		return nil, infracontract.ErrUnsupported
	}
	if err := validateScope(scope); err != nil {
		return nil, err
	}
	scope.Cgroups = append([]string(nil), scope.Cgroups...)
	scope.Before = append([]infracontract.Process(nil), scope.Before...)
	scope.Addresses = append([]string(nil), scope.Addresses...)
	return &identityBackend{scope: scope, read: ReadCensus}, nil
}

// origin: hot-update S9 plan §2.1 / T05
func (h *identityBackend) Inspect(domain infracontract.ID, deadline infracontract.Deadline) (infracontract.Census, error) {
	// Sampling and publication share one lock: a slow earlier dump must not be
	// stamped after a later observation and appear newer to the driver.
	h.mu.Lock()
	defer h.mu.Unlock()
	if domain != h.scope.Domain {
		return infracontract.Census{}, infracontract.ErrIncomplete
	}
	census, err := h.read(h.scope, deadline)
	if err != nil {
		return infracontract.Census{}, err
	}
	return h.observations.Observe(census)
}

// origin: hot-update S9 plan §2.1 / T05
func (h *identityBackend) Do(request infracontract.HostRequest, deadline infracontract.Deadline, stop infracontract.Stop) (infracontract.HostReceipt, error) {
	if err := identityBudget(deadline, stop); err != nil {
		return infracontract.HostReceipt{}, err
	}
	if request.Domain != h.scope.Domain {
		return infracontract.HostReceipt{}, infracontract.ErrIncomplete
	}
	encoded, err := infracontract.EncodeRequest(request)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	receipt := infracontract.HostReceipt{Tx: request.Tx, Sequence: request.Sequence, Op: request.Op, RequestDigest: sha256.Sum256(encoded)}
	if request.Op == infracontract.Inspect {
		receipt.Census, err = h.Inspect(request.Domain, deadline)
		return receipt, err
	}
	if request.Op != infracontract.SignalLDM && request.Op != infracontract.SignalStop && request.Op != infracontract.SignalGraceful {
		return infracontract.HostReceipt{}, infracontract.ErrUnsupported
	}
	found := false
	for _, before := range h.scope.Before {
		if before == request.Expected {
			found = true
		}
	}
	if !found || request.HostBootID != request.Expected.BootID {
		return infracontract.HostReceipt{}, infracontract.ErrIncomplete
	}
	// Pin first and keep it through the full pre-effect census. Root's startup
	// fence must prevent exec/restart while this expected operation is in flight.
	pin, err := PinProcess(request.Expected, deadline)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	defer func() { _ = pin.Close() }()
	census, err := h.Inspect(request.Domain, deadline)
	if err != nil {
		return infracontract.HostReceipt{}, err
	}
	if err = signalCensusReady(request, census); err != nil {
		return infracontract.HostReceipt{}, err
	}
	if err = pin.Signal(request.Op, deadline, stop); err != nil {
		return infracontract.HostReceipt{}, err
	}
	receipt.Applied, receipt.Current, receipt.Census = true, request.Expected, census
	return receipt, nil
}

// The signed census reference must still describe this scope at effect time.
// A past zero count cannot authorize a signal after a late accept or unit change.
// P04 separately resolves the journal/payload and holds the startup/ingress fence.
// origin: hot-update S9 plan §2.1 / §5.2 / T05/T06
func signalCensusReady(request infracontract.HostRequest, census infracontract.Census) error {
	if validateCensus(census) != nil || request.BeforeCensus != census.Digest || !slices.Contains(census.Processes, request.Expected) {
		return infracontract.ErrIncomplete
	}
	switch request.Op {
	case infracontract.SignalLDM:
		return nil
	case infracontract.SignalStop, infracontract.SignalGraceful:
		if census.OldOwnerPending == 0 {
			return nil
		}
	case infracontract.Inspect, infracontract.Stage, infracontract.FenceStart,
		infracontract.Promote, infracontract.StartWarm, infracontract.SwitchIngress,
		infracontract.FinalizeStorage, infracontract.ReleaseStart, infracontract.RetireWarm,
		infracontract.RecoverHost, infracontract.RotatePolicy, infracontract.UpdateSupport,
		infracontract.LaunchClaim, infracontract.LaunchAck, infracontract.VerifyIngress:
		// origin: hot-update S9 plan §10.2 / HOSTOP-SWITCHES
		return infracontract.ErrUnsupported
	default:
		return infracontract.ErrUnsupported
	}
	return infracontract.ErrIncomplete
}

// ReadSocket records a participant's live raw fd, not nats.go's current URL.
// The supplied owner must independently match the process holding this fd.
// origin: hot-update S9 plan §2.3 / T06
func ReadSocket(fd int, owner infracontract.Process, deadline infracontract.Deadline) (infracontract.Socket, error) {
	// The network owner must hold its RawConn.Control pin throughout this read.
	// Otherwise fd reuse between fstat/cookie/tuple calls could join evidence from
	// two sockets. No dup is retained here: the business owner keeps its original
	// descriptor, and this observation never changes connection lifetime.
	actual, err := ReadProcess(int32(os.Getpid()), deadline)
	if err != nil {
		return infracontract.Socket{}, err
	}
	if actual != owner {
		return infracontract.Socket{}, infracontract.ErrIncomplete
	}
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFSOCK {
		return infracontract.Socket{}, infracontract.ErrIncomplete
	}
	cookie, err := unix.GetsockoptUint64(fd, unix.SOL_SOCKET, unix.SO_COOKIE)
	if err != nil {
		return infracontract.Socket{}, identityError(err)
	}
	local, e1 := unix.Getsockname(fd)
	remote, e2 := unix.Getpeername(fd)
	ns, e3 := namespaceInode(os.Getpid())
	if e1 != nil || e2 != nil || e3 != nil || cookie == 0 {
		return infracontract.Socket{}, infracontract.ErrIncomplete
	}
	l, r := sockaddrEndpoint(local), sockaddrEndpoint(remote)
	if l == "" || r == "" {
		return infracontract.Socket{}, infracontract.ErrUnsupported
	}
	return infracontract.Socket{NetNS: ns, Device: uint64(stat.Dev), Inode: stat.Ino, Cookie: cookie, Local: l, Remote: r, Owner: owner}, nil
}

// origin: hot-update S9 plan §5.2 / T06
func sockaddrEndpoint(address unix.Sockaddr) string {
	switch a := address.(type) {
	case *unix.SockaddrInet4:
		return netip.AddrPortFrom(netip.AddrFrom4(a.Addr), uint16(a.Port)).String()
	case *unix.SockaddrInet6:
		if a.ZoneId != 0 {
			return ""
		}
		return netip.AddrPortFrom(netip.AddrFrom16(a.Addr), uint16(a.Port)).String()
	default:
		return ""
	}
}

// VerifyIngressProof requires the independently enumerated accepted socket in
// the expected main process. Certificates, HTTP 200 and upstream serverID cannot
// distinguish a healthy shadow from a broken main. NAT translation, if needed,
// must first be proven by the ingress owner; this direct tuple verifier refuses it.
// origin: hot-update S9 plan §5.2 / T06
func VerifyIngressProof(raw, accepted infracontract.Socket, expected infracontract.Process, census infracontract.Census) error {
	if runtime.GOARCH != "amd64" {
		return infracontract.ErrUnsupported
	}
	if validateCensus(census) != nil || !validProcess(expected) || accepted.Owner != expected ||
		!slices.Contains(census.Sockets, accepted) || raw.Inode == 0 || raw.Cookie == 0 || raw.Device == 0 || raw.NetNS == 0 ||
		!validProcess(raw.Owner) || !equivalentEndpoint(raw.Local, accepted.Remote) || !equivalentEndpoint(raw.Remote, accepted.Local) {
		return infracontract.ErrIncomplete
	}
	return nil
}

// Dual-stack listeners report IPv4 peers as IPv4-mapped IPv6 addresses. Compare
// wire endpoints after unmapping, while retaining the original socket's family,
// inode/cookie and owner in the census. Port translation still requires proof.
// origin: hot-update S9 plan §5.2 / T06
func equivalentEndpoint(a, b string) bool {
	x, e1 := netip.ParseAddrPort(a)
	y, e2 := netip.ParseAddrPort(b)
	return e1 == nil && e2 == nil && x.Port() == y.Port() && x.Addr().Unmap() == y.Addr().Unmap()
}

// VerifyAttachmentChange binds migration to a new raw socket and NATS session;
// changing URL metadata on the original transport is never a physical move.
// origin: hot-update S9 plan §2.3 / T06
func VerifyAttachmentChange(before, after infracontract.Attachment, accepted infracontract.Socket, census infracontract.Census) error {
	if runtime.GOARCH != "amd64" {
		return infracontract.ErrUnsupported
	}
	if before.Participant == (infracontract.ID{}) || before.Participant != after.Participant || after.Generation <= before.Generation ||
		after.HealthGeneration == 0 || after.ServerID == "" || after.CID == 0 || after.URL == "" ||
		(before.ServerID == after.ServerID && before.CID == after.CID) ||
		(before.Raw.NetNS == after.Raw.NetNS && before.Raw.Cookie == after.Raw.Cookie) ||
		(before.Raw.NetNS == after.Raw.NetNS && before.Raw.Device == after.Raw.Device && before.Raw.Inode == after.Raw.Inode) {
		return infracontract.ErrIncomplete
	}
	return VerifyIngressProof(after.Raw, accepted, after.Ingress, census)
}
