package infrapriv

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"reflect"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// The stream has exactly one purpose-tagged frame and a write half-close. EOF
// makes appended frames/bytes observable before any effect. JSON's own strict
// codec then binds every operand, including optional zero fields, to the signature.
// origin: hot-update S9 plan §2.2 / §6.1 / T08
const (
	ReadBudget         = upgradebudget.InfraReadBudget
	ActionBudget       = upgradebudget.InfraActionBudget
	WorkerBudget       = upgradebudget.InfraWorkerBudget
	MaxWorkers         = upgradebudget.InfraMaxWorkers
	MaxFileDescriptors = 2
	// Launch replies alone carry the frozen executable/config/store/locks/OFD set.
	LaunchFileDescriptors = 5
	RequestPurpose        = "THQ1"
	ReceiptPurpose        = "THA1"
	RefusalPurpose        = "THF1"
	DefaultSocketPath     = "/run/tether-infra/control.sock"
)

type IPCDescriptor struct {
	Version        uint16
	RequestPurpose string
	ReceiptPurpose string
	RefusalPurpose string
	HeaderBytes    uint32
	MaxBytes       uint32
	MaxWorkers     uint32
	ReadNS         uint64
	ActionNS       uint64
	WorkerNS       uint64
}

// IPCDescriptorForRuntime is also consumed by the frame reader and admission,
// so the inventory describes actual limits rather than an unrelated declaration.
func IPCDescriptorForRuntime() IPCDescriptor {
	return IPCDescriptor{1, RequestPurpose, ReceiptPurpose, RefusalPurpose, 8, c.MaxRequestBytes, MaxWorkers, uint64(ReadBudget), uint64(ActionBudget), uint64(WorkerBudget)}
}

// Refusal is exclusively a before-effect reply. It cannot be mistaken for a
// journal receipt, and does not invent a Journal/Revision to encode a denial in
// HostReceipt. A backend error after Do starts always remains result-unknown.
// origin: hot-update S9 plan §2.2 / §7.3 / T07 / T08
type Refusal struct {
	Version       uint16
	Tx            c.ID
	Sequence      uint64
	RequestDigest c.Digest
	Code          string
}

// IPCRecordDescriptors supplies the private inventory roots alongside the
// request/receipt roots owned by P00. Refusal's exact order is enforced below;
// the limits record is consumed by every real frame reader/writer.
func IPCRecordDescriptors() []c.RecordDescriptor {
	return []c.RecordDescriptor{
		{Name: "Refusal", Version: 1, MaxBytes: c.MaxRequestBytes, Fields: []c.FieldDescriptor{
			{Name: "Version", Type: "uint16"}, {Name: "Tx", Type: "infracontract.ID"}, {Name: "Sequence", Type: "uint64"},
			{Name: "RequestDigest", Type: "infracontract.Digest"}, {Name: "Code", Type: "string"},
		}},
		{Name: "IPCDescriptor", Version: 1, MaxBytes: c.MaxRequestBytes, Fields: []c.FieldDescriptor{
			{Name: "Version", Type: "uint16"}, {Name: "RequestPurpose", Type: "string"}, {Name: "ReceiptPurpose", Type: "string"},
			{Name: "RefusalPurpose", Type: "string"}, {Name: "HeaderBytes", Type: "uint32"}, {Name: "MaxBytes", Type: "uint32"},
			{Name: "MaxWorkers", Type: "uint32"}, {Name: "ReadNS", Type: "uint64"}, {Name: "ActionNS", Type: "uint64"}, {Name: "WorkerNS", Type: "uint64"},
		}},
		{Name: "FileRule", Version: 1, MaxBytes: c.MaxRequestBytes, Fields: []c.FieldDescriptor{
			{Name: "Op", Type: "infracontract.HostOp"}, {Name: "MaxInput", Type: "uint8"}, {Name: "Output", Type: "uint8"},
		}},
	}
}

func encodeRefusal(r Refusal) ([]byte, error) {
	typ := reflect.TypeFor[Refusal]()
	fields := IPCRecordDescriptors()[0].Fields
	if typ.NumField() != len(fields) {
		return nil, ErrIPC
	}
	for i, field := range fields {
		actual := typ.Field(i)
		if actual.Name != field.Name || actual.Type.String() != field.Type || actual.Tag != "" {
			return nil, ErrIPC
		}
	}
	if r.Version != 1 || r.Tx == (c.ID{}) || r.Sequence == 0 || r.RequestDigest == (c.Digest{}) || refusalError(r.Code) == nil {
		return nil, ErrIPC
	}
	return json.Marshal(r)
}

func decodeRefusal(data []byte) (Refusal, error) {
	var r Refusal
	if len(data) == 0 || len(data) > c.MaxRequestBytes || !utf8.Valid(data) || json.Unmarshal(data, &r) != nil {
		return Refusal{}, ErrIPC
	}
	canonical, err := encodeRefusal(r)
	if err != nil || !bytes.Equal(data, canonical) {
		return Refusal{}, ErrIPC
	}
	return r, nil
}

func refusalError(code string) error {
	switch code {
	case "infra_permission_denied":
		return ErrPermission
	case "infra_policy_stale":
		return ErrPolicyStale
	case "infra_busy":
		return ErrBusy
	case "infra_config_invalid":
		return ErrPolicy
	case "infra_budget_exceeded":
		return ErrBudget
	case "infra_unsupported":
		return c.ErrUnsupported
	default:
		return nil
	}
}

func refusalCode(err error) string {
	for _, code := range []string{"infra_permission_denied", "infra_policy_stale", "infra_busy", "infra_config_invalid", "infra_budget_exceeded", "infra_unsupported"} {
		if errors.Is(err, refusalError(code)) {
			return code
		}
	}
	if controlbudget.IsExpired(err) {
		return "infra_budget_exceeded"
	}
	return ""
}

// WorkerLimit holds the trusted directory for four permanent lock inodes.
// Every Accept=yes worker opens its own OFD and takes a nonblocking flock;
// process death releases the slot, and a client cannot monopolize a waiting queue.
// Close waits for no operation: the server owns joining workers before closing it.
type WorkerLimit struct {
	mu     sync.Mutex
	dir    *os.File
	owner  uint32
	closed bool
}

// FileExchange is a root-owned binding, not a wire extension. Accept may only
// validate/register borrowed inputs and the kernel peer; P04's Host.Do copies
// the input after durable intent and hashes the private final inode. Reply lends
// P06's already verified permit/lock OFDs; P06 retains its references until ACK.
// The server closes every received input when this request ends. No callback
// may bypass the shared domain CAS or perform an effect in Accept.
// origin: hot-update S9 plan §3.1 / §4.1 / §6.2 / T07 / T08
type FileExchange interface {
	Accept(c.HostRequest, c.Peer, []*os.File, c.Deadline, c.Stop) error
	Reply(c.HostRequest, c.HostReceipt, c.Peer, c.Deadline, c.Stop) ([]*os.File, error)
}

type FileRule struct {
	Op       c.HostOp
	MaxInput uint8
	Output   uint8
}

func FileRules() []FileRule {
	return []FileRule{{c.Stage, 1, 0}, {c.LaunchClaim, 0, LaunchFileDescriptors}}
}

func fileRule(op c.HostOp) FileRule {
	for _, rule := range FileRules() {
		if rule.Op == op {
			return rule
		}
	}
	return FileRule{Op: op}
}

func closeFiles(files []*os.File) {
	for _, file := range files {
		_ = file.Close()
	}
}

func (l *WorkerLimit) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	return l.dir.Close()
}

type Server struct {
	policy *PolicyStore
	host   c.Host
	limit  *WorkerLimit
	slots  chan struct{}
	files  FileExchange
}

// Client addresses only the fixed product socket. Nonroot orchestration signs
// Inspect through Do too; an unsigned convenience Inspect would bypass policy.
type Client struct {
	path      string
	serverUID uint32
	gid       uint32
	checkPath bool
}

func NewServer(policy *PolicyStore, host c.Host, limit *WorkerLimit, files ...FileExchange) (*Server, error) {
	if err := supportedPlatform(); err != nil {
		return nil, err
	}
	if policy == nil || host == nil || limit == nil {
		return nil, ErrIPC
	}
	if len(files) > 1 {
		return nil, ErrIPC
	}
	s := &Server{policy: policy, host: host, limit: limit, slots: make(chan struct{}, MaxWorkers)}
	if len(files) == 1 {
		s.files = files[0]
	}
	return s, nil
}

// ServeConn owns the accepted descriptor, including every early refusal. Its
// synchronous Host.Do must honor deadline/stop and join its work (P04 contract);
// abandoning a goroutine cannot turn an unbounded backend into a bounded worker.
// The domain journal lock must never be held while reading or writing this socket.
// origin: hot-update S9 plan §6.1 / T07 / T08
func (s *Server) ServeConn(conn *net.UnixConn, deadline c.Deadline, stop c.Stop) (resultErr error) {
	if conn == nil {
		return ErrIPC
	}
	// Delivery is decided by the checked CloseWrite (receipt) or the deferred
	// refusal write; interruptConn may already have closed this descriptor. A
	// close error must not rewrite a reply the peer has already been handed.
	defer func() { _ = conn.Close() }()
	entered, err := controlbudget.Now()
	if err != nil {
		return err
	}
	if err := checkBudget(deadline, stop); err != nil {
		return err
	}
	select {
	case s.slots <- struct{}{}:
	default:
		return ErrBusy
	}
	defer func() { <-s.slots }()
	releaseSlot, err := acquireWorkerSlot(s.limit)
	if err != nil {
		return err
	}
	defer releaseSlot()
	workerDeadline := earlier(deadline, c.Deadline(entered+int64(WorkerBudget)))
	workStop, join := interruptConn(conn, workerDeadline, stop)
	defer join()
	peer, err := PeerCredentials(conn)
	if err != nil {
		return err
	}
	readDeadline := earlier(workerDeadline, c.Deadline(entered+int64(ReadBudget)))
	if err := setReadDeadline(conn, readDeadline); err != nil {
		return err
	}
	purpose, data, inputs, err := readFileFrame(conn, RequestPurpose)
	if err != nil {
		return err
	}
	defer closeFiles(inputs)
	if purpose == LaunchQueryPurpose {
		if len(inputs) != 0 {
			return ErrPermission
		}
		current, err := PeerCredentials(conn)
		if err != nil || current != peer {
			return ErrPermission
		}
		return s.serveLaunchQuery(conn, data, peer, entered, workerDeadline, workStop)
	}
	r, err := c.DecodeRequest(data)
	if err != nil {
		return ErrIPC
	}
	effectStarted := false
	defer func() {
		code := refusalCode(resultErr)
		if effectStarted || code == "" {
			return
		}
		encoded, err := encodeRefusal(Refusal{1, r.Tx, r.Sequence, sha256.Sum256(data), code})
		if err != nil || setWriteDeadline(conn, workerDeadline) != nil {
			return
		}
		if writeFrame(conn, RefusalPurpose, encoded) == nil {
			_ = conn.CloseWrite()
		}
	}()
	// Deduct the ENTIRE admission/read interval. Receiving the last byte does not
	// grant a fresh RemainingNS window, nor can a repeated request renew P04's CAS.
	now, err := controlbudget.Now()
	if err != nil {
		return err
	}
	remaining := int64(r.RemainingNS) - (now - entered)
	if remaining <= 0 {
		return controlbudget.ErrExpired
	}
	actionDeadline := earlier(workerDeadline, c.Deadline(now+min(remaining, int64(ActionBudget))))
	if err := checkBudget(actionDeadline, workStop); err != nil {
		return err
	}
	// Re-read the kernel identity after a slow read; PID reuse or a dead original
	// sender cannot be authorized as the new process currently using that number.
	current, err := PeerCredentials(conn)
	if err != nil || current != peer {
		return ErrPermission
	}
	releaseAuthority, err := s.policy.begin(r, peer)
	if err != nil {
		return err
	}
	defer releaseAuthority()
	rule := fileRule(r.Op)
	if len(inputs) > int(rule.MaxInput) || (len(inputs) != 0 && s.files == nil) {
		return ErrPermission
	}
	if rule.Output != 0 && s.files == nil {
		return c.ErrContractStub
	}
	for _, input := range inputs {
		if err := validateReceivedFile(input, false); err != nil {
			return err
		}
	}
	if s.files != nil {
		if err := s.files.Accept(r, peer, inputs, actionDeadline, workStop); err != nil {
			return err
		}
	}
	if err := checkBudget(actionDeadline, workStop); err != nil {
		return err
	}
	effectStarted = true
	receipt, err := s.host.Do(r, actionDeadline, workStop)
	if err != nil {
		return err
	}
	if err := checkBudget(workerDeadline, workStop); err != nil {
		return err
	}
	if receipt.Tx != r.Tx || receipt.Sequence != r.Sequence || receipt.Op != r.Op || receipt.RequestDigest != sha256.Sum256(data) {
		return ErrIPC
	}
	encoded, err := c.EncodeReceipt(receipt)
	if err != nil {
		return ErrIPC
	}
	if err := setWriteDeadline(conn, workerDeadline); err != nil {
		return err
	}
	var outputs []*os.File
	if rule.Output != 0 {
		outputs, err = s.files.Reply(r, receipt, peer, actionDeadline, workStop)
		if err != nil || len(outputs) != int(rule.Output) {
			return ErrIPC
		}
		if err := validateLaunchFiles(outputs); err != nil {
			return err
		}
	}
	if err := writeFileFrame(conn, ReceiptPurpose, encoded, outputs); err != nil {
		return err
	}
	return conn.CloseWrite()
}

// interruptConn is a joined control worker. It closes the actual descriptor on
// cancellation and passes the same stop to the host; no reader/writer is orphaned.
func interruptConn(conn *net.UnixConn, deadline c.Deadline, stop c.Stop) (c.Stop, func()) {
	done := make(chan struct{})
	joined := make(chan struct{})
	cancel := make(chan struct{})
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		remaining = 0
	}
	timer := time.NewTimer(remaining)
	go func() {
		defer close(joined)
		select {
		case <-done:
			return
		case <-stop:
		case <-timer.C:
		}
		close(cancel)
		_ = conn.Close()
	}()
	return cancel, func() { close(done); timer.Stop(); <-joined }
}

func readFrame(conn *net.UnixConn, purpose string) ([]byte, error) {
	_, data, err := readPurposeFrame(conn, purpose)
	return data, err
}

func readPurposeFrame(conn *net.UnixConn, purpose string) (string, []byte, error) {
	actual, data, files, err := readFileFrame(conn, purpose)
	if len(files) != 0 {
		closeFiles(files)
		return "", nil, ErrIPC
	}
	return actual, data, err
}

func readFileFrame(conn *net.UnixConn, purpose string) (actual string, data []byte, files []*os.File, err error) {
	return readFileFrameLimit(conn, purpose, MaxFileDescriptors)
}

// Only an already selected LaunchClaim client may receive the five-role set.
// Requests and every other response retain the smaller admission limit.
// origin: S9 internal review rv-infra F03
func readFileFrameLimit(conn *net.UnixConn, purpose string, limit int) (actual string, data []byte, files []*os.File, err error) {
	defer func() {
		if err != nil {
			closeFiles(files)
			files = nil
		}
	}()
	d := IPCDescriptorForRuntime()
	header := make([]byte, d.HeaderBytes)
	if err := readFileExact(conn, header, &files, limit); err != nil {
		return "", nil, files, ErrIPC
	}
	size := binary.BigEndian.Uint32(header[4:])
	actualPurpose := string(header[:4])
	if (purpose != "" && actualPurpose != purpose && (purpose != RequestPurpose || actualPurpose != LaunchQueryPurpose)) || (purpose == "" && actualPurpose != ReceiptPurpose && actualPurpose != RefusalPurpose) || size == 0 || size > d.MaxBytes {
		return "", nil, files, ErrIPC
	}
	data = make([]byte, size)
	if err := readFileExact(conn, data, &files, limit); err != nil {
		return "", nil, files, ErrIPC
	}
	var tail [1]byte
	n, tailFiles, err := receiveFilesLimit(conn, tail[:], limit)
	files = append(files, tailFiles...)
	if n != 0 || len(tailFiles) != 0 || !errors.Is(err, io.EOF) {
		return "", nil, files, ErrIPC
	}
	return actualPurpose, data, files, nil
}

func readFileExact(conn *net.UnixConn, dst []byte, files *[]*os.File, limit int) error {
	for len(dst) > 0 {
		n, received, err := receiveFilesLimit(conn, dst, limit)
		*files = append(*files, received...)
		if err != nil || len(*files) > limit {
			return ErrIPC
		}
		if n <= 0 {
			return io.ErrNoProgress
		}
		dst = dst[n:]
	}
	return nil
}

func writeFrame(conn *net.UnixConn, purpose string, data []byte) error {
	return writeFileFrame(conn, purpose, data, nil)
}

func writeFileFrame(conn *net.UnixConn, purpose string, data []byte, files []*os.File) error {
	if len(files) > MaxFileDescriptors {
		receipt, err := c.DecodeReceipt(data)
		if purpose != ReceiptPurpose || err != nil || receipt.Op != c.LaunchClaim || len(files) != LaunchFileDescriptors {
			return ErrIPC
		}
	}
	d := IPCDescriptorForRuntime()
	if len(purpose) != 4 || len(data) == 0 || len(data) > int(d.MaxBytes) {
		return ErrIPC
	}
	frame := make([]byte, int(d.HeaderBytes)+len(data))
	copy(frame, purpose)
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(data)))
	copy(frame[8:], data)
	if len(files) != 0 {
		n, err := sendFiles(conn, frame, files)
		if err != nil || n <= 0 {
			return ErrIPC
		}
		frame = frame[n:]
	}
	for len(frame) > 0 {
		n, err := conn.Write(frame)
		if err != nil {
			return ErrIPC
		}
		if n == 0 {
			return io.ErrNoProgress
		}
		frame = frame[n:]
	}
	return nil
}

func checkBudget(deadline c.Deadline, stop c.Stop) error {
	select {
	case <-stop:
		return controlbudget.ErrExpired
	default:
	}
	_, err := controlbudget.Remaining(deadline)
	return err
}

func earlier(a, b c.Deadline) c.Deadline { return min(a, b) }

func setReadDeadline(conn *net.UnixConn, deadline c.Deadline) error {
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return err
	}
	return conn.SetReadDeadline(time.Now().Add(remaining))
}

func setWriteDeadline(conn *net.UnixConn, deadline c.Deadline) error {
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return err
	}
	return conn.SetWriteDeadline(time.Now().Add(remaining))
}
