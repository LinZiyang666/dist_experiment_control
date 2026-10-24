//go:build linux

package infrahost

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/LinZiyang666/tether/internal/infracontract"
	"golang.org/x/sys/unix"
)

// CensusScope is supplied by the trusted unit/domain owner, never a caller path.
// Cgroups includes old, main and temporary units and all their descendant groups.
// Addresses are the retiring physical listener endpoints (before DNAT), including
// both address families. Before is this step's incarnation set, not a SHA filter.
type CensusScope struct {
	Domain     infracontract.ID
	Generation uint64
	Cgroups    []string
	Before     []infracontract.Process
	Addresses  []string
}

type diagSocket struct {
	socket infracontract.Socket
	state  uint8
	queue  uint32
}

type trackedFlow struct {
	local    string
	remote   string
	protocol uint8
	state    uint8
}

// ReadCensus is a full kernel producer. There is deliberately no unprivileged
// fallback that claims Complete from only the known NC registry or /proc/net/tcp.
// The trusted ingress/startup fences remain necessary: a census is an observation,
// not a barrier that prevents new accepts or unit launches after it was sampled.
// origin: hot-update S9 plan §2.2 / §5.2 / T05/T06
func ReadCensus(scope CensusScope, deadline infracontract.Deadline) (infracontract.Census, error) {
	if err := identityBudget(deadline, nil); err != nil {
		return infracontract.Census{}, err
	}
	if err := validateScope(scope); err != nil {
		return infracontract.Census{}, err
	}
	pids, err := scopePIDs(scope.Cgroups, deadline)
	if err != nil || len(pids) == 0 {
		return infracontract.Census{}, infracontract.ErrIncomplete
	}
	var processes []infracontract.Process
	owners := make(map[uint64][]infracontract.Process)
	ns, err := namespaceInode(os.Getpid())
	if err != nil {
		return infracontract.Census{}, err
	}
	for _, pid := range pids {
		p, e := ReadProcess(pid, deadline)
		if e != nil {
			return infracontract.Census{}, e
		}
		if !inScope(p.Cgroup, scope.Cgroups) {
			return infracontract.Census{}, infracontract.ErrIncomplete
		}
		processes = append(processes, p)
		otherNS, e := namespaceInode(int(pid))
		if e != nil || otherNS != ns {
			return infracontract.Census{}, infracontract.ErrUnsupported
		}
		inodes, e := processSockets(pid)
		if e != nil {
			return infracontract.Census{}, e
		}
		for _, inode := range inodes {
			owners[inode] = append(owners[inode], p)
		}
	}
	diag, err := readSocketDiag(deadline)
	if err != nil {
		return infracontract.Census{}, err
	}
	flows, err := readConntrack(deadline)
	if err != nil {
		return infracontract.Census{}, err
	}
	census, err := assembleCensus(scope, processes, owners, diag, flows)
	if err != nil {
		return infracontract.Census{}, err
	}
	// Both membership and fd ownership must remain unchanged during the dump. A
	// disappearing fd is uncertainty; dropping it would create a false zero count.
	after, err := scopePIDs(scope.Cgroups, deadline)
	if err != nil || !slices.Equal(pids, after) {
		return infracontract.Census{}, infracontract.ErrIncomplete
	}
	for _, p := range processes {
		current, e := ReadProcess(p.PID, deadline)
		inodes, socketErr := processSockets(p.PID)
		if e != nil || current != p || socketErr != nil {
			return infracontract.Census{}, infracontract.ErrIncomplete
		}
		var prior []uint64
		for inode, ps := range owners {
			if slices.Contains(ps, p) {
				prior = append(prior, inode)
			}
		}
		slices.Sort(prior)
		if !slices.Equal(prior, inodes) {
			return infracontract.Census{}, infracontract.ErrIncomplete
		}
	}
	if err = verifyBeforeCoverage(scope.Before, processes, deadline); err != nil {
		return infracontract.Census{}, err
	}
	return census, nil
}

// origin: hot-update S9 plan §2.3 / T05
func validateScope(s CensusScope) error {
	// The scope is a unit-owner input, not a discovery hint. An empty address
	// list would skip pending accepts/conntrack, and an empty before set would
	// turn every observed process into an apparent successor. Refuse either.
	if s.Domain == (infracontract.ID{}) || s.Generation == 0 || len(s.Cgroups) == 0 || len(s.Before) == 0 || len(s.Addresses) == 0 {
		return infracontract.ErrIncomplete
	}
	for i, group := range s.Cgroups {
		// Never permit a caller to expand the domain to the host root or walk out
		// through symlinks. systemd owns this fixed cgroup-v2 hierarchy.
		if group == "/" || !strings.HasPrefix(group, "/") || filepath.Clean(group) != group {
			return infracontract.ErrIncomplete
		}
		if slices.Contains(s.Cgroups[:i], group) {
			return infracontract.ErrIncomplete
		}
	}
	for _, p := range s.Before {
		if !validProcess(p) || !inScope(p.Cgroup, s.Cgroups) {
			return infracontract.ErrIncomplete
		}
	}
	for i, value := range s.Addresses {
		addr, err := netip.ParseAddrPort(value)
		if err != nil || addr.Port() == 0 || addr.String() != value || slices.Contains(s.Addresses[:i], value) {
			return infracontract.ErrIncomplete
		}
	}
	return nil
}

func inScope(group string, roots []string) bool {
	for _, root := range roots {
		if group == root || strings.HasPrefix(group, root+"/") {
			return true
		}
	}
	return false
}

// origin: hot-update S9 plan §2.3 / T05
func scopePIDs(groups []string, deadline infracontract.Deadline) ([]int32, error) {
	// Keep the root dirfd while walking: a check followed by a pathname reopen
	// would let an ancestor replacement change which hierarchy was certified.
	// A set of supplied PIDs is insufficient because units can have children;
	// recursively include every descendant before returning the sorted union.
	root, err := unix.Open("/sys/fs/cgroup", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, infracontract.ErrUnsupported
	}
	defer func() { _ = unix.Close(root) }()
	seen := make(map[int32]bool)
	for _, group := range groups {
		fd, e := unix.Openat2(root, strings.TrimPrefix(group, "/"), &unix.OpenHow{
			Flags: unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC, Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS})
		// Kernel removal requires an empty cgroup. Retired transient units may
		// disappear, but a live predecessor moved elsewhere is still rejected by
		// independent pidfd coverage; absence of a path alone never signs exit.
		if errors.Is(e, unix.ENOENT) {
			continue
		}
		if e != nil {
			return nil, identityError(e)
		}
		file := os.NewFile(uintptr(fd), "infra-cgroup")
		e = walkCgroup(file, seen, deadline)
		_ = file.Close()
		if e != nil {
			return nil, e
		}
	}
	var pids []int32
	for pid := range seen {
		pids = append(pids, pid)
	}
	slices.Sort(pids)
	return pids, nil
}

// origin: hot-update S9 plan §2.2 / T05
func walkCgroup(dir *os.File, pids map[int32]bool, deadline infracontract.Deadline) error {
	// Bound each membership read as well as the total PID set. Cgroup contents
	// can change while read; the caller repeats membership and full fd identities
	// before accepting the assembled observation. No failed read means empty.
	if err := identityBudget(deadline, nil); err != nil {
		return err
	}
	fd, err := unix.Openat(int(dir.Fd()), "cgroup.procs", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return identityError(err)
	}
	file := os.NewFile(uintptr(fd), "infra-cgroup-procs")
	body, err := readLimited(file, 64<<10)
	_ = file.Close()
	if err != nil {
		return err
	}
	for _, word := range strings.Fields(string(body)) {
		pid, e := strconv.ParseInt(word, 10, 32)
		if e != nil || pid <= 0 || len(pids) >= 4096 {
			return infracontract.ErrIncomplete
		}
		pids[int32(pid)] = true
	}
	entries, err := dir.ReadDir(-1)
	if err != nil {
		return identityError(err)
	}
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 {
			return infracontract.ErrIncomplete
		}
		if !entry.IsDir() {
			continue
		}
		childFD, e := unix.Openat(int(dir.Fd()), entry.Name(), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if e != nil {
			return identityError(e)
		}
		child := os.NewFile(uintptr(childFD), "infra-cgroup-child")
		e = walkCgroup(child, pids, deadline)
		_ = child.Close()
		if e != nil {
			return e
		}
	}
	return nil
}

// origin: hot-update S9 plan §2.2 / T05
func readLimited(file *os.File, limit int) ([]byte, error) {
	// Read one byte past the ceiling to distinguish an exact-size record from
	// truncation. A partial prefix is never evidence of a complete process set.
	body := make([]byte, limit+1)
	n := 0
	for n < len(body) {
		count, err := file.Read(body[n:])
		n += count
		if err != nil {
			if errors.Is(err, io.EOF) && n <= limit {
				return body[:n], nil
			}
			return nil, infracontract.ErrIncomplete
		}
	}
	return nil, infracontract.ErrIncomplete
}

// origin: hot-update S9 plan §2.3 / T06
func namespaceInode(pid int) (uint64, error) {
	// Socket inode/cookie pairs are meaningful within a network namespace.
	// Refuse a different namespace rather than silently consulting the helper's
	// own sock_diag table for a target it cannot observe. P31 supplies same-host
	// systemd units; this leaf does not enter caller-selected namespaces.
	var stat unix.Stat_t
	if err := unix.Stat("/proc/"+strconv.Itoa(pid)+"/ns/net", &stat); err != nil {
		return 0, identityError(err)
	}
	return stat.Ino, nil
}

// origin: hot-update S9 plan §2.3 / T06
func processSockets(pid int32) ([]uint64, error) {
	// Count identities, not fd numbers: dup aliases can share one socket, while
	// close/open can reuse the same fd for another inode. FD-count equality would
	// hide both cases. Readlink failure, even for a concurrently closed fd, makes
	// this observation uncertain and forces a fresh census under the same budget.
	root := "/proc/" + strconv.Itoa(int(pid)) + "/fd"
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, identityError(err)
	}
	seen := make(map[uint64]bool)
	for _, entry := range entries {
		link, e := os.Readlink(root + "/" + entry.Name())
		if e != nil {
			return nil, identityError(e)
		}
		if !strings.HasPrefix(link, "socket:[") {
			continue
		}
		inode, e := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(link, "socket:["), "]"), 10, 64)
		if e != nil || inode == 0 {
			return nil, infracontract.ErrIncomplete
		}
		seen[inode] = true
	}
	var result []uint64
	for inode := range seen {
		result = append(result, inode)
	}
	slices.Sort(result)
	return result, nil
}

// origin: hot-update S9 plan §5.2 / T06
func endpointMatches(endpoint string, addresses []string) bool {
	// Wildcard listeners cover their family, never the other family by accident.
	// A textual port match alone would charge IPv4 shadow traffic to an IPv6
	// predecessor, or allow a different destination to escape responsibility.
	value, err := netip.ParseAddrPort(endpoint)
	if err != nil {
		return false
	}
	for _, address := range addresses {
		target, e := netip.ParseAddrPort(address)
		if e == nil && target.Port() == value.Port() && target.Addr().Is4() == value.Addr().Is4() &&
			(target.Addr().IsUnspecified() || target.Addr() == value.Addr()) {
			return true
		}
	}
	return false
}

// origin: hot-update S9 plan §5.2 / T06
func assembleCensus(scope CensusScope, processes []infracontract.Process, owners map[uint64][]infracontract.Process, diag []diagSocket, flows []trackedFlow) (infracontract.Census, error) {
	// This reducer never trusts registry counts. Every observed owner inode must
	// resolve in the independent kernel dump, and every owned record retains its
	// complete Process identity. Shared OFDs produce one row per owning process;
	// there is no arbitrary winner that could hide the old holder.
	//
	// LISTEN itself may remain until graceful stop. Its receive queue and any
	// accepted socket still block retirement. All accepted TCP is counted equally:
	// the kernel cannot distinguish WSS, TLS setup, idle HTTP or late Upgrade.
	// Conntrack may duplicate those counts; conservative overcounting is safe,
	// whereas dropping an unaccepted or unconfirmed flow could break continuity.
	c := infracontract.Census{Generation: scope.Generation, Processes: slices.Clone(processes)}
	oldPresent := false
	for _, p := range processes {
		if slices.Contains(scope.Before, p) {
			oldPresent = true
		}
	}
	seen := make(map[uint64]bool)
	for _, d := range diag {
		ps := owners[d.socket.Inode]
		if len(ps) == 0 {
			// SYN_RECV has no accepted fd/inode. Other unowned live sockets at a
			// retiring endpoint cannot be silently attributed to an empty old unit.
			if endpointMatches(d.socket.Local, scope.Addresses) && d.state != 6 && d.state != 7 {
				c.OldOwnerPending++
			}
			continue
		}
		if seen[d.socket.Inode] || d.socket.Cookie == 0 || d.socket.Cookie == ^uint64(0) {
			return infracontract.Census{}, infracontract.ErrIncomplete
		}
		seen[d.socket.Inode] = true
		for _, p := range ps {
			socket := d.socket
			socket.Owner = p
			c.Sockets = append(c.Sockets, socket)
			if d.state != 10 && d.state != 7 {
				c.LiveBusiness++
			}
			if slices.Contains(scope.Before, p) && (d.state != 10 || d.queue > 0) {
				c.OldOwnerPending++
			}
		}
	}
	if len(seen) != len(owners) {
		return infracontract.Census{}, infracontract.ErrIncomplete
	}
	for _, flow := range flows {
		// The reply source is the physical destination after DNAT. Counting only
		// the original canonical tuple would mistake shadow traffic for old-main.
		// Conntrack does not identify the serving PID: once the old incarnation is
		// independently absent, traffic on a reused endpoint belongs to its successor.
		if oldPresent && endpointMatches(flow.local, scope.Addresses) && (flow.protocol != unix.IPPROTO_TCP || (flow.state != 7 && flow.state != 8)) {
			c.OldOwnerPending++
		}
	}
	slices.SortFunc(c.Processes, func(a, b infracontract.Process) int { return int(a.PID - b.PID) })
	slices.SortFunc(c.Sockets, func(a, b infracontract.Socket) int {
		if a.Inode < b.Inode {
			return -1
		}
		if a.Inode > b.Inode {
			return 1
		}
		return int(a.Owner.PID - b.Owner.PID)
	})
	c.Complete = true
	c.Digest = censusDigest(c)
	if err := validateCensus(c); err != nil {
		return infracontract.Census{}, err
	}
	return c, nil
}

func censusDigest(c infracontract.Census) infracontract.Digest {
	// Exclude only the digest's own field. Processes, sockets, generation,
	// completeness and both responsibility counts all participate. Root's journal
	// binds this private digest to Tx/step/domain; it is not a public status token.
	c.Digest = infracontract.Digest{}
	body, _ := json.Marshal(c)
	return sha256.Sum256(body)
}

// origin: hot-update S9 plan §2.3 / T05
func validateCensus(c infracontract.Census) error {
	// Complete is a necessary claim, not sufficient evidence. Empty, unsorted,
	// malformed or digest-mismatched sets cannot become an old-exit receipt.
	// Requiring a live successor also separates host inspection failure from a
	// legitimate empty retiring unit inside a nonempty complete domain census.
	if !c.Complete || c.Generation == 0 || len(c.Processes) == 0 || c.Digest == (infracontract.Digest{}) || c.Digest != censusDigest(c) {
		return infracontract.ErrIncomplete
	}
	for i, p := range c.Processes {
		if !validProcess(p) || (i > 0 && c.Processes[i-1].PID >= p.PID) {
			return infracontract.ErrIncomplete
		}
	}
	for _, s := range c.Sockets {
		if s.NetNS == 0 || s.Device == 0 || s.Inode == 0 || s.Cookie == 0 || s.Local == "" || s.Remote == "" || !slices.Contains(c.Processes, s.Owner) {
			return infracontract.ErrIncomplete
		}
	}
	body, _ := json.Marshal(c)
	if len(body) > infracontract.MaxCensusBytes {
		return infracontract.ErrIncomplete
	}
	return nil
}

// netlinkDump accepts only kernel datagrams for this sequence. Missing DONE,
// interrupted dumps, truncation, malformed messages and resource ceilings are
// uncertainty, never a shortened successful census. All waits share one deadline.
// origin: hot-update S9 plan §2.2 / T05/T06
func netlinkDump(protocol int, kind uint16, payload []byte, deadline infracontract.Deadline) ([][]byte, error) {
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, protocol)
	if err != nil {
		return nil, identityError(err)
	}
	defer func() { _ = unix.Close(fd) }()
	if err = unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return nil, identityError(err)
	}
	request := make([]byte, 16+len(payload))
	binary.NativeEndian.PutUint32(request, uint32(len(request)))
	binary.NativeEndian.PutUint16(request[4:], kind)
	binary.NativeEndian.PutUint16(request[6:], unix.NLM_F_REQUEST|unix.NLM_F_DUMP)
	binary.NativeEndian.PutUint32(request[8:], 1)
	copy(request[16:], payload)
	if err = unix.Sendto(fd, request, 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return nil, identityError(err)
	}
	var result [][]byte
	for page := 0; page < infracontract.MaxCensusPages; page++ {
		if err = identityBudget(deadline, nil); err != nil {
			return nil, err
		}
		poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		if _, err = unix.Poll(poll, 10); err != nil {
			if errors.Is(err, unix.EINTR) {
				page--
				continue
			}
			return nil, identityError(err)
		}
		if poll[0].Revents == 0 {
			page--
			continue
		}
		buffer := make([]byte, infracontract.MaxCensusPageBytes)
		n, _, flags, from, e := unix.Recvmsg(fd, buffer, nil, 0)
		peer, ok := from.(*unix.SockaddrNetlink)
		if e != nil || !ok || peer.Pid != 0 || flags&unix.MSG_TRUNC != 0 {
			return nil, infracontract.ErrIncomplete
		}
		items, done, e := decodeNetlinkPage(buffer[:n], flags, peer.Pid, protocol, kind)
		if e != nil {
			return nil, e
		}
		result = append(result, items...)
		if done {
			return result, nil
		}
	}
	return nil, infracontract.ErrIncomplete
}

// decodeNetlinkPage is also the sandbox test boundary. Require exact framing,
// not ParseNetlinkMessage's permissive trailing short-header behavior. An ACK
// without a successful DONE cannot certify that all kernel pages were received.
// origin: hot-update S9 plan §2.2 / T05/T06
func decodeNetlinkPage(body []byte, flags int, peerPID uint32, protocol int, kind uint16) ([][]byte, bool, error) {
	if len(body) == 0 || len(body) > infracontract.MaxCensusPageBytes || flags&unix.MSG_TRUNC != 0 || peerPID != 0 {
		return nil, false, infracontract.ErrIncomplete
	}
	var result [][]byte
	for len(body) > 0 {
		if len(body) < 16 {
			return nil, false, infracontract.ErrIncomplete
		}
		n := int(binary.NativeEndian.Uint32(body))
		aligned := (n + 3) &^ 3
		if n < 16 || aligned > len(body) {
			return nil, false, infracontract.ErrIncomplete
		}
		typeID := binary.NativeEndian.Uint16(body[4:])
		messageFlags := binary.NativeEndian.Uint16(body[6:])
		if binary.NativeEndian.Uint32(body[8:]) != 1 || messageFlags&unix.NLM_F_DUMP_INTR != 0 {
			return nil, false, infracontract.ErrIncomplete
		}
		data := body[16:n]
		switch typeID {
		case unix.NLMSG_DONE:
			if aligned != len(body) || (len(data) != 0 && (len(data) != 4 || binary.NativeEndian.Uint32(data) != 0)) {
				return nil, false, infracontract.ErrIncomplete
			}
			return result, true, nil
		case unix.NLMSG_ERROR:
			return nil, false, infracontract.ErrIncomplete
		default:
			// Conntrack dumps (requested as CT_GET 257 / CT_GET_UNCONFIRMED 263)
			// answer with IPCTNL_MSG_CT_NEW: NFNL_SUBSYS_CTNETLINK<<8 | 0 == 256.
			conntrackEntry := protocol == unix.NETLINK_NETFILTER && typeID == 256
			if typeID != kind && !conntrackEntry {
				return nil, false, infracontract.ErrIncomplete
			}
			result = append(result, slices.Clone(data))
		}
		body = body[aligned:]
	}
	return result, false, nil
}

// origin: hot-update S9 plan §5.2 / T06
func readSocketDiag(deadline infracontract.Deadline) ([]diagSocket, error) {
	// Use the actual Linux UAPI layouts (inet_diag_req_v2/inet_diag_msg and
	// unix_diag_req/unix_diag_msg). Scalar fields are host endian; inet addresses
	// and ports are network endian. Mixing them can produce a plausible but wrong
	// accepted tuple, which must never prove main readiness.
	//
	// Dump both families and both TCP/UDP protocols. TCP-only observation could
	// silently certify a Caddy with unobserved QUIC responsibility. Unknown owner
	// protocols fail the subsequent inode coverage check instead of disappearing.
	// Unix sockets include the protected admin listener and accepted admin calls.
	ns, err := namespaceInode(os.Getpid())
	if err != nil {
		return nil, err
	}
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, identityError(err)
	}
	var stat unix.Stat_t
	err = unix.Fstat(fd, &stat)
	// All sockets in the observed namespace use sockfs. Obtain its device from
	// a real socket rather than inventing one; target inode and cookie are then
	// independently supplied by sock_diag and matched to /proc fd ownership.
	_ = unix.Close(fd)
	if err != nil {
		return nil, identityError(err)
	}
	var result []diagSocket
	for _, family := range []uint8{unix.AF_INET, unix.AF_INET6} {
		for _, protocol := range []uint8{unix.IPPROTO_TCP, unix.IPPROTO_UDP} {
			request := make([]byte, 56)
			request[0], request[1] = family, protocol
			binary.NativeEndian.PutUint32(request[4:], ^uint32(0))
			pages, e := netlinkDump(unix.NETLINK_INET_DIAG, unix.SOCK_DIAG_BY_FAMILY, request, deadline)
			if e != nil {
				return nil, e
			}
			for _, body := range pages {
				if len(body) < 72 || body[0] != family {
					return nil, infracontract.ErrIncomplete
				}
				local, e1 := diagEndpoint(family, body[8:24], binary.BigEndian.Uint16(body[4:]))
				remote, e2 := diagEndpoint(family, body[24:40], binary.BigEndian.Uint16(body[6:]))
				if e1 != nil || e2 != nil {
					return nil, infracontract.ErrIncomplete
				}
				result = append(result, diagSocket{socket: infracontract.Socket{NetNS: ns, Device: uint64(stat.Dev), Inode: uint64(binary.NativeEndian.Uint32(body[68:])),
					Cookie: uint64(binary.NativeEndian.Uint32(body[44:])) | uint64(binary.NativeEndian.Uint32(body[48:]))<<32, Local: local, Remote: remote}, state: body[1], queue: binary.NativeEndian.Uint32(body[56:])})
			}
		}
	}
	request := make([]byte, 24)
	request[0] = unix.AF_UNIX
	binary.NativeEndian.PutUint32(request[4:], ^uint32(0))
	binary.NativeEndian.PutUint32(request[12:], 1|4|16)
	pages, err := netlinkDump(unix.NETLINK_INET_DIAG, unix.SOCK_DIAG_BY_FAMILY, request, deadline)
	if err != nil {
		return nil, err
	}
	for _, body := range pages {
		if len(body) < 16 || body[0] != unix.AF_UNIX {
			return nil, infracontract.ErrIncomplete
		}
		attrs, e := netlinkAttrs(body[16:])
		if e != nil {
			return nil, e
		}
		inode := uint64(binary.NativeEndian.Uint32(body[4:]))
		socket := infracontract.Socket{NetNS: ns, Device: uint64(stat.Dev), Inode: inode, Cookie: uint64(binary.NativeEndian.Uint32(body[8:])) | uint64(binary.NativeEndian.Uint32(body[12:]))<<32,
			Local: "unix:" + strconv.FormatUint(inode, 10), Remote: "unix:0"}
		if name := attrs[0]; len(name) > 0 {
			socket.Local = "unix:" + hexName(name)
		}
		if peer := attrs[2]; len(peer) == 4 {
			socket.Remote = "unix:" + strconv.FormatUint(uint64(binary.NativeEndian.Uint32(peer)), 10)
		}
		var queue uint32
		if q := attrs[4]; len(q) == 8 {
			queue = binary.NativeEndian.Uint32(q)
		}
		result = append(result, diagSocket{socket: socket, state: body[2], queue: queue})
	}
	return result, nil
}

func hexName(name []byte) string {
	// Unix names can contain NULs (abstract namespace) and non-UTF8 bytes. A
	// reversible encoding avoids JSON replacement characters aliasing distinct
	// names. These private names are evidence, never paths to reopen or log.
	const digits = "0123456789abcdef"
	b := make([]byte, len(name)*2)
	for i, v := range name {
		b[2*i], b[2*i+1] = digits[v>>4], digits[v&15]
	}
	return string(b)
}

// origin: hot-update S9 plan §5.2 / T06
func diagEndpoint(family uint8, body []byte, port uint16) (string, error) {
	// Canonical AddrPort spelling makes address equality unambiguous, including
	// IPv6 brackets. The family fixes the byte width; an IPv4-mapped IPv6 socket
	// retains its actual AF_INET6 identity instead of borrowing another listener.
	if family == unix.AF_INET && len(body) >= 4 {
		return netip.AddrPortFrom(netip.AddrFrom4([4]byte(body[:4])), port).String(), nil
	}
	if family == unix.AF_INET6 && len(body) >= 16 {
		return netip.AddrPortFrom(netip.AddrFrom16([16]byte(body[:16])), port).String(), nil
	}
	return "", infracontract.ErrIncomplete
}

// origin: hot-update S9 plan §2.2 / T06
func netlinkAttrs(body []byte) (map[uint16][]byte, error) {
	// Each attribute length includes its header but excludes alignment padding.
	// Strip only the kernel's nested/network-order flags from the type; nested
	// content is parsed at the corresponding typed boundary. Duplicate attributes
	// are ambiguous, even if their bytes happen to agree in this observation.
	result := make(map[uint16][]byte)
	for len(body) != 0 {
		if len(body) < 4 {
			return nil, infracontract.ErrIncomplete
		}
		n := int(binary.NativeEndian.Uint16(body))
		kind := binary.NativeEndian.Uint16(body[2:]) & 0x3fff
		aligned := (n + 3) &^ 3
		if n < 4 || aligned > len(body) {
			return nil, infracontract.ErrIncomplete
		}
		if _, exists := result[kind]; exists {
			return nil, infracontract.ErrIncomplete
		}
		result[kind] = body[4:n]
		body = body[aligned:]
	}
	return result, nil
}

func readConntrack(deadline infracontract.Deadline) ([]trackedFlow, error) {
	return readConntrackUsing(netlinkDump, deadline)
}

func readConntrackUsing(dump func(int, uint16, []byte, infracontract.Deadline) ([][]byte, error), deadline infracontract.Deadline) ([]trackedFlow, error) {
	var result []trackedFlow
	// Include unconfirmed entries: an in-flight SYN must not disappear merely
	// because it has not yet entered the ordinary conntrack hash table. Read
	// unconfirmed first: promotion between the two dumps must be counted in the
	// later confirmed dump, rather than falling into the gap between both tables.
	// Duplicate sightings are conservative; product ingress fencing prevents new
	// retiring-endpoint admissions during the observation (the dump is no fence).
	// origin: hot-update S9 plan §5.2 / T06
	for _, message := range []uint16{263, 257} {
		pages, err := dump(unix.NETLINK_NETFILTER, message, []byte{unix.AF_UNSPEC, 0, 0, 0}, deadline)
		if err != nil {
			return nil, err
		}
		for _, body := range pages {
			flow, e := parseConntrack(body)
			if e != nil {
				return nil, e
			}
			if flow.protocol == unix.IPPROTO_TCP || flow.protocol == unix.IPPROTO_UDP {
				result = append(result, flow)
			}
		}
	}
	return result, nil
}

// origin: hot-update S9 plan §5.2 / T06
func parseConntrack(body []byte) (trackedFlow, error) {
	// Inspect reply tuples because DNAT rewrites the physical destination. The
	// original canonical tuple alone cannot distinguish main from warm ingress.
	// TCP state is nf_conntrack_tcp's vocabulary, not inet_diag's socket state:
	// LAST_ACK=6 still owns responsibility; TIME_WAIT=7 and CLOSE=8 are terminal.
	// Missing protocol/state/IP fields are uncertainty rather than a zero flow.
	if len(body) < 4 {
		return trackedFlow{}, infracontract.ErrIncomplete
	}
	attrs, err := netlinkAttrs(body[4:])
	if err != nil {
		return trackedFlow{}, err
	}
	tuple, err := netlinkAttrs(attrs[2])
	if err != nil {
		return trackedFlow{}, err
	}
	ip, err := netlinkAttrs(tuple[1])
	if err != nil {
		return trackedFlow{}, err
	}
	proto, err := netlinkAttrs(tuple[2])
	if err != nil || len(proto[1]) != 1 {
		return trackedFlow{}, infracontract.ErrIncomplete
	}
	f := trackedFlow{protocol: proto[1][0]}
	if f.protocol != unix.IPPROTO_TCP && f.protocol != unix.IPPROTO_UDP {
		return f, nil
	}
	if len(proto[2]) != 2 || len(proto[3]) != 2 {
		return trackedFlow{}, infracontract.ErrIncomplete
	}
	src, dst := ip[1], ip[2]
	if body[0] == unix.AF_INET6 {
		src, dst = ip[3], ip[4]
	}
	if (body[0] != unix.AF_INET && body[0] != unix.AF_INET6) || len(src) != len(dst) || (body[0] == unix.AF_INET && len(src) != 4) || (body[0] == unix.AF_INET6 && len(src) != 16) {
		return trackedFlow{}, infracontract.ErrIncomplete
	}
	f.local, err = diagEndpoint(body[0], src, binary.BigEndian.Uint16(proto[2]))
	if err != nil {
		return trackedFlow{}, err
	}
	f.remote, err = diagEndpoint(body[0], dst, binary.BigEndian.Uint16(proto[3]))
	if err != nil {
		return trackedFlow{}, err
	}
	if f.protocol == unix.IPPROTO_TCP {
		info, e := netlinkAttrs(attrs[4])
		if e != nil {
			return trackedFlow{}, e
		}
		tcp, e := netlinkAttrs(info[1])
		if e != nil || len(tcp[1]) != 1 || tcp[1][0] == 0 || tcp[1][0] > 9 {
			return trackedFlow{}, infracontract.ErrIncomplete
		}
		f.state = tcp[1][0]
	}
	return f, nil
}
