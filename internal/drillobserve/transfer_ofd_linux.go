//go:build linux

package drillobserve

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

// TransferOFDWitness owns one CLOEXEC duplicate, never a pathname reopen. Send
// briefly owns a second (sealed metadata) FD; the keeper receives two FDs and
// closes metadata after validation. These peaks count toward the cut budget.
// origin: hot-update S7 plan §8.3 / §10.4 / T47
type TransferOFDWitness struct {
	mu      sync.Mutex
	file    *os.File
	receipt TransferOFDReceipt
}

func CaptureTransferOFD(file *os.File, value any) (*TransferOFDWitness, error) {
	var ref transferResource
	if copyTransferValue(reflect.ValueOf(&ref).Elem(), reflect.ValueOf(value)) != nil {
		return nil, ErrTransferCustody
	}
	if file == nil || !validTransferResource(ref, true) {
		return nil, ErrTransferCustody
	}
	conn, err := file.SyscallConn()
	if err != nil {
		return nil, ErrTransferCustody
	}
	fd := -1
	var duplicateErr error
	if err := conn.Control(func(original uintptr) {
		fd, duplicateErr = unix.FcntlInt(original, unix.F_DUPFD_CLOEXEC, 3)
	}); err != nil || duplicateErr != nil {
		return nil, ErrTransferCustody
	}
	w := &TransferOFDWitness{file: os.NewFile(uintptr(fd), "transfer-ofd-witness")}
	fail := func() (*TransferOFDWitness, error) { _ = w.Close(); return nil, ErrTransferCustody }
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || (stat.Mode&unix.S_IFMT != unix.S_IFREG && stat.Mode&unix.S_IFMT != unix.S_IFDIR) {
		return fail()
	}
	process, err := TransferProcessIdentity(0, "")
	if err != nil {
		return fail()
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return fail()
	}
	digest := sha256.Sum256(nonce[:])
	w.receipt = TransferOFDReceipt{Schema: TransferSchemaVersion, ProcessStart: process.ProcessStart, BootID: process.BootID, PID: process.PID, Resource: ref, WitnessDigest: hex.EncodeToString(digest[:]), OwnedFDs: 1, PeakFDs: 2}
	return w, nil
}

func (w *TransferOFDWitness) Receipt() TransferOFDReceipt {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.receipt
}

// Compare uses the real kernel OFD comparator. /proc stat/fdinfo, equal inode,
// flags, size and restored offset are useful context, never identity evidence.
// PID/start checks on both sides fence PID reuse around the syscall. ENOSYS or
// EPERM is SETUP2; neither permits fallback to fstat or mutation of an offset.
// origin: hot-update S7 plan §10.4 / T47 / M09
func (w *TransferOFDWitness) Compare(pid int, processStart uint64, fd int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil || pid <= 0 || fd < 0 || processStart == 0 {
		return ErrTransferCustody
	}
	before, err := transferProcessStart(pid)
	if err != nil || before != processStart {
		return ErrTransferCustody
	}
	result, _, errno := unix.Syscall6(unix.SYS_KCMP, uintptr(os.Getpid()), uintptr(pid), 0 /* KCMP_FILE */, w.file.Fd(), uintptr(fd), 0)
	after, err := transferProcessStart(pid)
	if errno != 0 || result != 0 || err != nil || after != processStart {
		return ErrTransferCustody
	}
	return nil
}

func (w *TransferOFDWitness) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	file := w.file
	w.file = nil
	if file.Close() != nil {
		return ErrTransferCustody
	}
	return nil
}

// Send runs only at a fixture/cut barrier outside product locks. It never
// retries under backpressure and never takes ownership of the caller's socket.
// Metadata and the duplicate original travel together in one seqpacket.
// origin: hot-update S7 plan §10.4 / T47
func (w *TransferOFDWitness) Send(channel int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil || !transferSeqpacket(channel) {
		return ErrTransferCustody
	}
	data, err := json.Marshal(w.receipt)
	if err == nil {
		var fields map[string]json.RawMessage
		err = json.Unmarshal(data, &fields)
		if err == nil {
			var resource map[string]any
			resource, err = transferObject(reflect.ValueOf(w.receipt.Resource))
			if err == nil {
				fields["resource"], err = json.Marshal(resource)
			}
			if err == nil {
				data, err = json.Marshal(fields)
			}
		}
	}
	if err != nil {
		return ErrTransferCustody
	}
	fd, err := unix.MemfdCreate("tether-transfer-ofd", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return ErrTransferCustody
	}
	defer func() { _ = unix.Close(fd) }()
	if n, err := unix.Write(fd, data); err != nil || n != len(data) {
		return ErrTransferCustody
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, custodySeals); err != nil {
		return ErrTransferCustody
	}
	if unix.Sendmsg(channel, []byte("TDO1"), unix.UnixRights(fd, int(w.file.Fd())), nil, unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL) != nil {
		return ErrTransferCustody
	}
	return nil
}

// ReceiveTransferOFD validates both descriptors before handing one to the
// keeper. Every rejected packet closes its entire received FD closure.
// origin: hot-update S7 plan §10.4 / T47
func ReceiveTransferOFD(channel int) (*TransferOFDWitness, error) {
	data, fds, err := receiveTransferPacket(channel)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, fd := range fds {
			_ = unix.Close(fd)
		}
	}()
	if string(data) != "TDO1" || len(fds) != 2 {
		return nil, ErrTransferCustody
	}
	metadata, err := readTransferSealed(fds[0], 4096)
	if err != nil {
		return nil, err
	}
	var r TransferOFDReceipt
	fields, err := transferRawObject(metadata)
	if err != nil || len(fields) != 8 {
		return nil, ErrTransferCustody
	}
	for _, name := range []string{"schema", "process_start", "boot_id", "pid", "resource", "witness_digest", "owned_fds", "peak_fds"} {
		if fields[name] == nil {
			return nil, ErrTransferCustody
		}
	}
	resourceFields, err := transferRawObject(fields["resource"])
	if err != nil || decodeTransferObject(resourceFields, reflect.ValueOf(&r.Resource).Elem()) != nil {
		return nil, ErrTransferCustody
	}
	delete(fields, "resource")
	metadata, err = json.Marshal(fields)
	if err != nil {
		return nil, ErrTransferCustody
	}
	d := json.NewDecoder(bytes.NewReader(metadata))
	d.DisallowUnknownFields()
	if d.Decode(&r) != nil || r.Schema != TransferSchemaVersion || r.ProcessStart == 0 || r.PID <= 0 || !transferToken(r.BootID, true) || !validTransferResource(r.Resource, true) || !transferDigest(r.WitnessDigest) || r.OwnedFDs != 1 || r.PeakFDs != 2 {
		return nil, ErrTransferCustody
	}
	var stat unix.Stat_t
	if unix.Fstat(fds[1], &stat) != nil || (stat.Mode&unix.S_IFMT != unix.S_IFREG && stat.Mode&unix.S_IFMT != unix.S_IFDIR) {
		return nil, ErrTransferCustody
	}
	file := os.NewFile(uintptr(fds[1]), "transfer-ofd-keeper")
	fds = fds[:1]
	return &TransferOFDWitness{file: file, receipt: r}, nil
}

func TransferProcessIdentity(generation uint64, cutID string) (TransferProcess, error) {
	pid := os.Getpid()
	start, err := transferProcessStart(pid)
	if err != nil {
		return TransferProcess{}, err
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || !transferToken(strings.TrimSpace(string(boot)), true) || !transferToken(cutID, false) {
		return TransferProcess{}, ErrTransferCustody
	}
	return TransferProcess{ProcessStart: start, BootID: strings.TrimSpace(string(boot)), PID: pid, Generation: generation, CutID: cutID}, nil
}

func transferProcessStart(pid int) (uint64, error) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, ErrTransferCustody
	}
	// comm may contain spaces and parentheses. Field 22 follows the LAST ')'.
	end := bytes.LastIndexByte(data, ')')
	if end < 0 {
		return 0, ErrTransferCustody
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 20 {
		return 0, ErrTransferCustody
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || start == 0 {
		return 0, ErrTransferCustody
	}
	return start, nil
}

func transferSeqpacket(fd int) bool {
	family, e1 := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_DOMAIN)
	kind, e2 := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_TYPE)
	_, e3 := unix.Getpeername(fd)
	return e1 == nil && e2 == nil && e3 == nil && family == unix.AF_UNIX && kind == unix.SOCK_SEQPACKET
}

func receiveTransferPacket(channel int) ([]byte, []int, error) {
	if !transferSeqpacket(channel) {
		return nil, nil, ErrTransferCustody
	}
	data := make([]byte, 64)
	rights := make([]byte, unix.CmsgSpace(16*4))
	n, oobn, flags, _, err := unix.Recvmsg(channel, data, rights, unix.MSG_DONTWAIT|unix.MSG_CMSG_CLOEXEC)
	if err != nil {
		return nil, nil, ErrTransferCustody
	}
	messages, err := unix.ParseSocketControlMessage(rights[:oobn])
	var fds []int
	for _, message := range messages {
		got, parseErr := unix.ParseUnixRights(&message)
		if parseErr != nil {
			err = parseErr
		} else {
			fds = append(fds, got...)
		}
	}
	if err != nil || n == 0 || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		for _, fd := range fds {
			_ = unix.Close(fd)
		}
		return nil, nil, ErrTransferCustody
	}
	return data[:n], fds, nil
}

func readTransferSealed(fd, limit int) ([]byte, error) {
	seals, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
	var stat unix.Stat_t
	if err != nil || seals&custodySeals != custodySeals || unix.Fstat(fd, &stat) != nil || stat.Size <= 0 || stat.Size > int64(limit) {
		return nil, ErrTransferCustody
	}
	data := make([]byte, int(stat.Size))
	if n, err := unix.Pread(fd, data, 0); err != nil || n != len(data) {
		return nil, ErrTransferCustody
	}
	return data, nil
}

func transferDrainRequest(data []byte) bool {
	return len(data) == 20 && string(data[:4]) == "TDP2" && !bytes.Equal(data[4:], make([]byte, 16))
}

// Called by the existing custody writer AFTER its queue and batch drain. No
// new reader races that writer for requests on the inherited seqpacket socket.
// origin: hot-update S7 plan §10.4 / T41
func transferDrainReply(c *custody, request []byte) []byte {
	reply := append([]byte("TDA2"), request[4:20]...)
	reply = binary.LittleEndian.AppendUint64(reply, c.sequence)
	failure := uint64(0)
	if c.broken.Load() {
		failure = max(c.failure.Load(), 1)
	}
	return binary.LittleEndian.AppendUint64(reply, failure)
}

func poisonTransferCustody() {
	if active != nil {
		poisonCustody(active, 13)
	}
}

// PublishTransferOFD uses only the immutable startup channel. Primitive owners
// call it outside their locks after capturing a witness, before exposing its
// digest as an observation. Failure poisons custody and never repairs product IO.
// origin: hot-update S7 plan §10.4 / T47
func PublishTransferOFD(w *TransferOFDWitness) error {
	if w == nil || active == nil || active.broken.Load() {
		return ErrTransferCustody
	}
	if err := w.Send(active.channel); err != nil {
		poisonCustody(active, 14)
		return ErrTransferCustody
	}
	return nil
}

func transferNow() (int64, error) {
	var now unix.Timespec
	if unix.ClockGettime(unix.CLOCK_BOOTTIME, &now) != nil {
		return 0, ErrTransferCustody
	}
	return now.Nano(), nil
}

var transferReaderIdentity struct {
	sync.Once
	digest string
	err    error
}

// Package state is replaced by exec. Hash the original mapped executable once
// per incarnation, independently of any renamed on-disk candidate pathname.
// origin: S7 internal review INTEG-M F1 / hot-update S7 plan §10.4
func transferRunningReader() (string, error) {
	transferReaderIdentity.Do(func() {
		file, err := os.Open("/proc/self/exe")
		if err != nil {
			transferReaderIdentity.err = ErrTransferCustody
			return
		}
		hash := sha256.New()
		_, err = io.Copy(hash, file)
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			transferReaderIdentity.err = ErrTransferCustody
			return
		}
		transferReaderIdentity.digest = hex.EncodeToString(hash.Sum(nil))
	})
	return transferReaderIdentity.digest, transferReaderIdentity.err
}

// TransferExecDescriptors identifies only the immutable startup custody lease.
// An exec adapter adds these originals to its strict inheritance allowlist and
// restores their CLOEXEC flags under the same ForkLock on every failed exec.
// Revalidate sealed authority so a closed/reused descriptor cannot authorize
// an unrelated inheritable FD merely because its number matches the old lease.
// origin: S7 internal review INTEG-M F4 / hot-update S7 plan §10.4
func TransferExecDescriptors() ([]int, error) {
	if active == nil || active.broken.Load() {
		return nil, nil
	}
	data, err := readTransferSealed(active.fd, 32)
	if err != nil || len(data) != 32 || string(data[:8]) != "TDCFG1\x00\x00" || sha256.Sum256(data) != active.identity ||
		active.fd < 3 || active.channel < 3 || active.fd == active.channel || binary.LittleEndian.Uint64(data[8:16]) != uint64(active.channel) || !transferSeqpacket(active.channel) {
		return nil, ErrTransferCustody
	}
	return []int{active.fd, active.channel}, nil
}
