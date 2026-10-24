//go:build linux

package infrahost

import (
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"runtime"
	"slices"
	"testing"

	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

func observedSocketCensus(t *testing.T, receipt socketReceipt) (infracontract.Census, []diagSocket) {
	t.Helper()
	diag, err := readSocketDiag(identityDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	inodes, err := processSockets(receipt.Process.PID)
	if err != nil {
		t.Fatal(err)
	}
	owners := make(map[uint64][]infracontract.Process)
	for _, inode := range inodes {
		owners[inode] = []infracontract.Process{receipt.Process}
	}
	// This is the assembler's sandbox boundary, not a privileged cgroup/conntrack
	// receipt. The independent kernel dump and /proc owners are real; full host
	// cgroup membership/confirmed+unconfirmed conntrack remain P31 integration.
	scope := CensusScope{Generation: 1, Before: []infracontract.Process{receipt.Process}, Addresses: []string{receipt.Address}}
	c, err := assembleCensus(scope, []infracontract.Process{receipt.Process}, owners, diag, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c, diag
}

// origin: hot-update S9 plan §2.3 / §5.2 / T06
func TestIngressProofRequiresAcceptedSocket(t *testing.T) {
	if runtime.GOARCH != "amd64" {
		return
	}
	mainChild, main := launchSocketChild(t, "socket")
	shadowChild, shadow := launchSocketChild(t, "socket")
	dualChild, dual := launchSocketChild(t, "dual-socket")
	dualCensus, _ := observedSocketCensus(t, dual)
	if dual.Raw.Local == dual.Accepted.Remote {
		t.Fatal("dual-stack fixture did not expose IPv4-mapped IPv6 accepted tuple")
	}
	if err := VerifyIngressProof(dual.Raw, dual.Accepted, dual.Process, dualCensus); err != nil {
		t.Fatal("real dual-stack accepted proof rejected")
	}
	census, diag := observedSocketCensus(t, main)
	if !slices.Contains(census.Sockets, main.Accepted) || !slices.Contains(census.Sockets, main.Raw) {
		t.Fatal("kernel socket identity disagrees with raw fd identity")
	}
	if err := VerifyIngressProof(main.Raw, main.Accepted, main.Process, census); err != nil {
		t.Fatalf("real accepted socket: %v", err)
	}
	if census.LiveBusiness < 2 || census.OldOwnerPending < 2 {
		t.Fatal("idle raw and accepted TCP responsibilities vanished")
	}
	for name, mutate := range map[string]func(*infracontract.Socket, *infracontract.Socket, *infracontract.Process, *infracontract.Census){
		"healthy_shadow_is_not_main": func(raw, accepted *infracontract.Socket, expected *infracontract.Process, c *infracontract.Census) {
			*raw = shadow.Raw
			*accepted = shadow.Accepted
		},
		"forged_inode": func(raw, accepted *infracontract.Socket, expected *infracontract.Process, c *infracontract.Census) {
			accepted.Inode++
		},
		"forged_cookie": func(raw, accepted *infracontract.Socket, expected *infracontract.Process, c *infracontract.Census) {
			accepted.Cookie++
		},
		"forged_netns": func(raw, accepted *infracontract.Socket, expected *infracontract.Process, c *infracontract.Census) {
			accepted.NetNS++
		},
		"wrong_pid": func(raw, accepted *infracontract.Socket, expected *infracontract.Process, c *infracontract.Census) {
			expected.PID++
		},
		"wrong_exe": func(raw, accepted *infracontract.Socket, expected *infracontract.Process, c *infracontract.Census) {
			expected.ExeSHA256[0]++
		},
		"wrong_remote": func(raw, accepted *infracontract.Socket, expected *infracontract.Process, c *infracontract.Census) {
			raw.Remote = "127.0.0.1:1"
		},
		"missing_socket": func(raw, accepted *infracontract.Socket, expected *infracontract.Process, c *infracontract.Census) {
			c.Sockets = nil
			c.Digest = testCensusDigest(*c)
		},
		"incomplete": func(raw, accepted *infracontract.Socket, expected *infracontract.Process, c *infracontract.Census) {
			c.Complete = false
			c.Digest = testCensusDigest(*c)
		},
		"unknown_raw": func(raw, accepted *infracontract.Socket, expected *infracontract.Process, c *infracontract.Census) {
			raw.Cookie = 0
		},
	} {
		t.Run(name, func(t *testing.T) {
			raw, accepted, expected, c := main.Raw, main.Accepted, main.Process, census
			mutate(&raw, &accepted, &expected, &c)
			if err := VerifyIngressProof(raw, accepted, expected, c); err == nil {
				t.Fatal("unproven ingress certified main readiness")
			}
		})
	}
	before := infracontract.Attachment{Participant: infracontract.ID{1}, Generation: 1, ServerID: "upstream", CID: 11, URL: "wss://home", Raw: shadow.Raw, Ingress: shadow.Process, HealthGeneration: 1}
	after := infracontract.Attachment{Participant: before.Participant, Generation: 2, ServerID: before.ServerID, CID: 12, URL: "wss://home", Raw: main.Raw, Ingress: main.Process, HealthGeneration: 2}
	if err := VerifyAttachmentChange(before, after, main.Accepted, census); err != nil {
		t.Fatalf("real raw migration: %v", err)
	}
	writeIdentityReceipt(t, identityReceipt{Name: "accepted-socket-ingress", Scope: "real independent sock_diag/proc/FD observations of supervised child sockets; assembler sandbox excludes privileged cgroup/conntrack; NATS session values are validator inputs", Before: []infracontract.Process{shadow.Process}, After: []infracontract.Process{main.Process}, Old: before, New: after, Accepted: main.Accepted, Census: census})
	for name, mutate := range map[string]func(*infracontract.Attachment){
		"url_only":    func(a *infracontract.Attachment) { a.Raw = before.Raw; a.URL = "wss://pretend-new" },
		"same_cookie": func(a *infracontract.Attachment) { a.Raw.Cookie = before.Raw.Cookie; a.Raw.NetNS = before.Raw.NetNS },
		"same_inode": func(a *infracontract.Attachment) {
			a.Raw.Inode = before.Raw.Inode
			a.Raw.Device = before.Raw.Device
			a.Raw.NetNS = before.Raw.NetNS
		},
		"same_session":     func(a *infracontract.Attachment) { a.CID = before.CID },
		"stale_generation": func(a *infracontract.Attachment) { a.Generation = before.Generation },
		"unhealthy":        func(a *infracontract.Attachment) { a.HealthGeneration = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := after
			mutate(&bad)
			if err := VerifyAttachmentChange(before, bad, main.Accepted, census); err == nil {
				t.Fatal("metadata-only or unready move was certified")
			}
		})
	}
	// A census must cover every actual fd, including unknown protocols. It cannot
	// drop an owner that was not found in sock_diag and still declare completeness.
	owners := map[uint64][]infracontract.Process{^uint64(0): {main.Process}}
	if _, err := assembleCensus(CensusScope{Generation: 1}, []infracontract.Process{main.Process}, owners, diag, nil); !errors.Is(err, infracontract.ErrIncomplete) {
		t.Fatal("unobserved socket was discarded")
	}
	mainChild.finish(t)
	shadowChild.finish(t)
	dualChild.finish(t)
}

// origin: hot-update S9 plan §5.2 / T06 (full TCP closure, not just WSS)
func TestInfraCensusIncludesPendingKernelResponsibilities(t *testing.T) {
	if runtime.GOARCH != "amd64" {
		return
	}
	child, receipt := launchSocketChild(t, "backlog")
	census, diag := observedSocketCensus(t, receipt)
	queueObserved := false
	for _, d := range diag {
		if d.socket.Local == receipt.Address && d.state == 10 && d.queue > 0 {
			queueObserved = true
		}
	}
	if !queueObserved || census.OldOwnerPending == 0 {
		t.Fatal("unaccepted listener backlog did not block old stop")
	}
	for _, d := range diag {
		if d.socket.Local == receipt.Address && d.state == 10 && d.queue > 0 {
			isolated, err := assembleCensus(CensusScope{Generation: 1, Before: []infracontract.Process{receipt.Process}}, []infracontract.Process{receipt.Process}, map[uint64][]infracontract.Process{d.socket.Inode: {receipt.Process}}, []diagSocket{d}, nil)
			if err != nil || isolated.OldOwnerPending != 1 {
				t.Fatal("listener queue was not counted independently of accepted clients")
			}
		}
	}
	// SYN_RECV has no accepted owner yet. Confirmed DNAT flows use the reply's
	// physical destination, and terminal TIME_WAIT/CLOSE no longer own service.
	processes := []infracontract.Process{receipt.Process}
	scope := CensusScope{Generation: 1, Before: processes, Addresses: []string{receipt.Address}}
	syn := diagSocket{socket: infracontract.Socket{Local: receipt.Address}, state: 3}
	c, err := assembleCensus(scope, processes, nil, []diagSocket{syn}, []trackedFlow{{local: receipt.Address, protocol: 6, state: 3}})
	if err != nil || c.OldOwnerPending != 2 {
		t.Fatalf("SYN/conntrack responsibility missing: %+v %v", c, err)
	}
	c, err = assembleCensus(scope, processes, nil, nil, []trackedFlow{{local: "127.0.0.1:1", protocol: 6, state: 1}, {local: receipt.Address, protocol: 6, state: 7}, {local: receipt.Address, protocol: 6, state: 8}})
	if err != nil || c.OldOwnerPending != 0 {
		t.Fatal("shadow/terminal flows counted against old physical endpoint")
	}
	c, err = assembleCensus(scope, processes, nil, nil, []trackedFlow{{local: receipt.Address, protocol: 6, state: 6}})
	if err != nil || c.OldOwnerPending != 1 {
		t.Fatal("conntrack LAST_ACK was mistaken for socket TIME_WAIT")
	}
	newOwner := receipt.Process
	newOwner.PID++
	c, err = assembleCensus(scope, []infracontract.Process{newOwner}, nil, nil, []trackedFlow{{local: receipt.Address, protocol: 6, state: 3}})
	if err != nil || c.OldOwnerPending != 0 {
		t.Fatal("successor endpoint traffic was attributed to an absent predecessor")
	}
	if _, err = ReadCensus(CensusScope{}, identityDeadline(t)); !errors.Is(err, infracontract.ErrIncomplete) {
		t.Fatal("empty scope became a complete census")
	}
	child.finish(t)
}

func nla(kind uint16, body ...byte) []byte {
	n := 4 + len(body)
	result := make([]byte, (n+3)&^3)
	binary.NativeEndian.PutUint16(result, uint16(n))
	binary.NativeEndian.PutUint16(result[2:], kind)
	copy(result[4:], body)
	return result
}

func conntrackMessage(family uint8, state uint8) []byte {
	src, dst := []byte{127, 0, 0, 1}, []byte{127, 0, 0, 2}
	srcKind, dstKind := uint16(1), uint16(2)
	if family == unix.AF_INET6 {
		src = make([]byte, 16)
		dst = make([]byte, 16)
		src[15] = 1
		dst[15] = 2
		srcKind, dstKind = 3, 4
	}
	ip := append(nla(srcKind, src...), nla(dstKind, dst...)...)
	proto := append(nla(1, 6), nla(2, 0x20, 0xfb)...)
	proto = append(proto, nla(3, 0x30, 0x39)...)
	tuple := append(nla(1|0x8000, ip...), nla(2|0x8000, proto...)...)
	tcp := nla(1, state)
	result := append([]byte{family, 0, 0, 0}, nla(2|0x8000, tuple...)...)
	return append(result, nla(4|0x8000, nla(1|0x8000, tcp...)...)...)
}

// origin: hot-update S9 plan §2.2 / T05/T06
func TestInfraCensusRejectsIncompleteKernelRecords(t *testing.T) {
	// The sandbox kernel promotes a flow immediately before the unconfirmed
	// read. Reading confirmed first would miss it in both dumps and certify zero.
	promoted := false
	flows, err := readConntrackUsing(func(protocol int, kind uint16, payload []byte, deadline infracontract.Deadline) ([][]byte, error) {
		if kind == 263 {
			promoted = true
			return nil, nil
		}
		if kind == 257 && promoted {
			return [][]byte{conntrackMessage(unix.AF_INET, 3)}, nil
		}
		return nil, nil
	}, identityDeadline(t))
	if err != nil || len(flows) != 1 {
		t.Fatal("promotion between conntrack dumps escaped the full responsibility set")
	}
	for _, family := range []uint8{unix.AF_INET, unix.AF_INET6} {
		message := conntrackMessage(family, 3)
		flow, err := parseConntrack(message)
		if err != nil || flow.protocol != 6 || flow.state != 3 || !endpointMatches(flow.local, []string{flow.local}) {
			t.Fatalf("conntrack reply tuple lost: %+v %v", flow, err)
		}
		for n := 0; n < len(message); n++ {
			if _, err = parseConntrack(message[:n]); err == nil {
				t.Fatalf("truncated conntrack accepted at %d", n)
			}
		}
		if _, err = parseConntrack(append(slices.Clone(message), message[4:]...)); err == nil {
			t.Fatal("conflicting duplicate conntrack attributes accepted")
		}
		for _, state := range []uint8{0, 10, 255} {
			if _, err = parseConntrack(conntrackMessage(family, state)); err == nil {
				t.Fatal("unknown TCP state accepted")
			}
		}
	}
	for _, body := range [][]byte{{1}, {0, 0, 1, 0}, {3, 0, 1, 0}, {8, 0, 1, 0}, {4, 0, 1, 0, 4, 0, 1, 0}} {
		if _, err := netlinkAttrs(body); err == nil {
			t.Fatal("malformed/duplicate netlink attributes accepted")
		}
	}
	if endpointMatches("127.0.0.1:443", []string{"[::]:443"}) || endpointMatches("127.0.0.1:443", []string{"0.0.0.0:444"}) || !endpointMatches("127.0.0.1:443", []string{"0.0.0.0:443"}) {
		t.Fatal("dual-stack/wildcard physical match is incorrect")
	}
	// Boundary refuses arbitrary filesystem traversal and whole-host expansion
	// before opening any cgroup or netlink descriptor.
	_, receipt := launchSocketChild(t, "socket")
	scope := CensusScope{Domain: infracontract.ID{1}, Generation: 1, Cgroups: []string{receipt.Process.Cgroup}, Before: []infracontract.Process{receipt.Process}, Addresses: []string{receipt.Address}}
	for _, group := range []string{"/", "/../", "relative", "/unit/../other"} {
		bad := scope
		bad.Cgroups = []string{group}
		if _, err := NewIdentityBackend(bad); err == nil {
			t.Fatal("unsafe cgroup scope accepted")
		}
	}
	if _, err := ReadProcess(int32(os.Getpid()), 1); err == nil {
		t.Fatal("expired kernel observation started")
	}
	// These descriptors are DTOs, not new JSON/wire contracts. Freeze the P02
	// resource shapes so a field cannot silently fall out of a downstream ledger.
	if reflect.TypeFor[CensusScope]().NumField() != 5 || reflect.TypeFor[diagSocket]().NumField() != 3 || reflect.TypeFor[trackedFlow]().NumField() != 4 {
		t.Fatal("P02 observation resource shape changed")
	}
}

func netlinkMessage(kind, flags uint16, sequence uint32, data []byte) []byte {
	n := 16 + len(data)
	result := make([]byte, (n+3)&^3)
	binary.NativeEndian.PutUint32(result, uint32(n))
	binary.NativeEndian.PutUint16(result[4:], kind)
	binary.NativeEndian.PutUint16(result[6:], flags)
	binary.NativeEndian.PutUint32(result[8:], sequence)
	copy(result[16:], data)
	return result
}

// origin: hot-update S9 plan §2.2 / T05/T06 (sandbox kernel framing)
func TestInfraCensusRequiresCompleteNetlinkDump(t *testing.T) {
	kind := uint16(unix.SOCK_DIAG_BY_FAMILY)
	data := netlinkMessage(kind, unix.NLM_F_MULTI, 1, []byte{1, 2, 3, 4})
	done := netlinkMessage(unix.NLMSG_DONE, unix.NLM_F_MULTI, 1, []byte{0, 0, 0, 0})
	items, complete, err := decodeNetlinkPage(append(slices.Clone(data), done...), 0, 0, unix.NETLINK_INET_DIAG, kind)
	if err != nil || !complete || len(items) != 1 || !slices.Equal(items[0], []byte{1, 2, 3, 4}) {
		t.Fatal("full multipart kernel page lost")
	}
	if _, complete, err = decodeNetlinkPage(data, 0, 0, unix.NETLINK_INET_DIAG, kind); err != nil || complete {
		t.Fatal("data without DONE certified completeness")
	}
	for name, bad := range map[string][]byte{
		"empty":                   nil,
		"short_header":            data[:15],
		"trailing_partial_header": append(slices.Clone(data), 0),
		"wrong_sequence":          netlinkMessage(kind, unix.NLM_F_MULTI, 2, []byte{1}),
		"interrupted":             netlinkMessage(kind, unix.NLM_F_DUMP_INTR, 1, []byte{1}),
		"error":                   netlinkMessage(unix.NLMSG_ERROR, 0, 1, []byte{1, 0, 0, 0}),
		"ack_without_done":        netlinkMessage(unix.NLMSG_ERROR, 0, 1, []byte{0, 0, 0, 0}),
		"wrong_type":              netlinkMessage(999, 0, 1, []byte{1}),
		"done_with_error":         netlinkMessage(unix.NLMSG_DONE, 0, 1, []byte{1, 0, 0, 0}),
		"done_short_status":       netlinkMessage(unix.NLMSG_DONE, 0, 1, []byte{0}),
		"after_done":              append(slices.Clone(done), data...),
		"oversize":                netlinkMessage(kind, unix.NLM_F_MULTI, 1, make([]byte, 64<<10)),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := decodeNetlinkPage(bad, 0, 0, unix.NETLINK_INET_DIAG, kind); err == nil {
				t.Fatal("incomplete kernel page accepted")
			}
		})
	}
	bad := slices.Clone(data)
	binary.NativeEndian.PutUint32(bad, 15)
	if _, _, err = decodeNetlinkPage(bad, 0, 0, unix.NETLINK_INET_DIAG, kind); err == nil {
		t.Fatal("undersize message header accepted")
	}
	if _, _, err = decodeNetlinkPage(data, unix.MSG_TRUNC, 0, unix.NETLINK_INET_DIAG, kind); err == nil {
		t.Fatal("truncated datagram accepted")
	}
	if _, _, err = decodeNetlinkPage(data, 0, 123, unix.NETLINK_INET_DIAG, kind); err == nil {
		t.Fatal("userspace producer impersonated kernel")
	}
	if _, err = netlinkDump(unix.NETLINK_INET_DIAG, kind, nil, 1); err == nil {
		t.Fatal("expired dump accepted")
	}
}

// origin: hot-update S9 plan §2.2 / T05/T06
func FuzzInfraKernelCensus(f *testing.F) {
	f.Add(conntrackMessage(unix.AF_INET, 3))
	f.Add(conntrackMessage(unix.AF_INET6, 7))
	f.Add(netlinkMessage(unix.NLMSG_DONE, 0, 1, []byte{0, 0, 0, 0}))
	f.Add([]byte{0})
	f.Fuzz(func(t *testing.T, body []byte) {
		_, _ = parseConntrack(body)
		_, _ = netlinkAttrs(body)
		_, _, _ = decodeNetlinkPage(body, 0, 0, unix.NETLINK_INET_DIAG, unix.SOCK_DIAG_BY_FAMILY)
	})
}

// origin: hot-update S9 plan §12 P02 / T05/T06
func TestInfraCensusLifecycle(t *testing.T) {
	if runtime.GOARCH != "amd64" {
		return
	}
	exerciseCensus(t) // Warm the shared supervisor's pipe poller before the baseline.
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCensus(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infra kernel census x5", beforeG)
	testharness.AssertNoFDLeak(t, "infra kernel census x5", beforeFD, 0)
}

// origin: hot-update S9 plan §2.1 / §5.2 / T05/T06
func TestInfraSignalRequiresCurrentCensus(t *testing.T) {
	if runtime.GOARCH != "amd64" {
		return
	}
	child, receipt := launchSocketChild(t, "socket")
	census, _ := observedSocketCensus(t, receipt)
	if err := verifyBeforeCoverage([]infracontract.Process{receipt.Process}, census.Processes, identityDeadline(t)); err != nil {
		t.Fatal(err)
	}
	request := infracontract.HostRequest{Op: infracontract.SignalLDM, Expected: receipt.Process, BeforeCensus: census.Digest}
	if err := signalCensusReady(request, census); err != nil {
		t.Fatal("current LDM census rejected")
	}
	request.BeforeCensus[0]++
	if err := signalCensusReady(request, census); err == nil {
		t.Fatal("stale signed census reference accepted")
	}
	request.BeforeCensus = census.Digest
	request.Expected.PID++
	if err := signalCensusReady(request, census); err == nil {
		t.Fatal("expected owner outside census accepted")
	}
	request.Expected = receipt.Process
	for _, op := range []infracontract.HostOp{infracontract.SignalStop, infracontract.SignalGraceful} {
		request.Op = op
		if err := signalCensusReady(request, census); err == nil {
			t.Fatal("stop with idle live TCP responsibility accepted")
		}
	}
	request.Op = infracontract.Stage
	if err := signalCensusReady(request, census); !errors.Is(err, infracontract.ErrUnsupported) {
		t.Fatal("non-signal operation admitted")
	}
	zero := testCensus(receipt.Process)
	zero.LiveBusiness = 0
	zero.Digest = testCensusDigest(zero)
	request.BeforeCensus = zero.Digest
	request.Op = infracontract.SignalGraceful
	if err := signalCensusReady(request, zero); err != nil {
		t.Fatal("sealed zero-responsibility stop rejected")
	}
	child.finish(t)
}

// origin: hot-update S9 plan §2.3 / T05
func TestInfraCensusCannotHideLivePredecessorOutsideScope(t *testing.T) {
	if runtime.GOARCH != "amd64" {
		return
	}
	oldChild, old := launchSocketChild(t, "socket")
	newChild, current := launchSocketChild(t, "socket")
	if err := verifyBeforeCoverage([]infracontract.Process{old.Process}, []infracontract.Process{current.Process}, identityDeadline(t)); err == nil {
		t.Fatal("live predecessor omitted from cgroup census signed exit")
	}
	if err := verifyBeforeCoverage([]infracontract.Process{old.Process}, []infracontract.Process{old.Process, current.Process}, identityDeadline(t)); err != nil {
		t.Fatal("fully covered predecessor rejected")
	}
	wrongBoot := old.Process
	wrongBoot.BootID += "wrong"
	if err := verifyBeforeCoverage([]infracontract.Process{wrongBoot}, []infracontract.Process{old.Process, current.Process}, identityDeadline(t)); err == nil {
		t.Fatal("cross-boot before set accepted")
	}
	oldChild.finish(t)
	if err := verifyBeforeCoverage([]infracontract.Process{old.Process}, []infracontract.Process{current.Process}, identityDeadline(t)); err != nil {
		t.Fatal("independently dead predecessor rejected")
	}
	newChild.finish(t)
}

func exerciseCensus(t *testing.T) {
	t.Helper()
	child, receipt := launchSocketChild(t, "socket")
	census, _ := observedSocketCensus(t, receipt)
	if err := verifyBeforeCoverage([]infracontract.Process{receipt.Process}, census.Processes, identityDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := VerifyIngressProof(receipt.Raw, receipt.Accepted, receipt.Process, census); err != nil {
		t.Fatal(err)
	}
	if _, err := netlinkDump(unix.NETLINK_INET_DIAG, unix.SOCK_DIAG_BY_FAMILY, nil, 1); err == nil {
		t.Fatal("expired partial kernel dump succeeded")
	}
	child.finish(t)
}
