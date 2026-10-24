//go:build linux

package ingressswitch

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strings"
	"syscall"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"golang.org/x/sys/unix"
)

const tableName = "tether_ingress"
const lockPath = "/run/tether/infra-ingress.lock"

// The adapter accepts no caller command, pathname, table or expression. nft's
// JSON frontend applies the entire file as a single kernel transaction. The
// permanent lock serializes all product writers; rootjournal owns domain CAS.
type linuxBackend struct {
	run  func(bool, []byte, infracontract.Deadline, infracontract.Stop) ([]byte, error)
	lock func() (func(), error)
}

// NewLinux checks the frozen platform before it opens a lock or changes rules.
// Bootstrap and SwitchIngress share this backend; root must install the baseline
// guard before advertising management readiness. No business service is started.
// origin: hot-update S9 plan lead LQ1 / §5.2 / §6.1 / T16
func NewLinux() (Backend, error) {
	if runtime.GOARCH != "amd64" || os.Geteuid() != 0 {
		return nil, infracontract.ErrUnsupported
	}
	for _, path := range []string{"/run/systemd/system", "/sys/fs/cgroup/cgroup.controllers"} {
		if _, err := os.Stat(path); err != nil {
			return nil, infracontract.ErrUnsupported
		}
	}
	for _, path := range []string{"/proc/sys/net/ipv6/conf/all/disable_ipv6", "/proc/sys/net/ipv6/conf/default/disable_ipv6"} {
		data, err := os.ReadFile(path)
		if err != nil || strings.TrimSpace(string(data)) != "0" {
			return nil, infracontract.ErrUnsupported
		}
	}
	data, err := os.ReadFile("/proc/net/if_inet6")
	if err != nil || len(bytes.TrimSpace(data)) == 0 {
		return nil, infracontract.ErrUnsupported
	}
	pidfd, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		return nil, infracontract.ErrUnsupported
	}
	_ = unix.Close(pidfd)
	var binary string
	for _, path := range []string{"/usr/sbin/nft", "/usr/bin/nft"} {
		info, err := os.Stat(path)
		if err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0022 == 0 && info.Mode().Perm()&0111 != 0 {
			if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Uid == 0 {
				binary = path
				break
			}
		}
	}
	if binary == "" {
		return nil, infracontract.ErrUnsupported
	}
	return &linuxBackend{run: nftRunner(binary), lock: ingressLock}, nil
}

func ingressLock() (func(), error) {
	// The containing directory is root-private and cannot be replaced by a
	// caller. Never unlink the lock: concurrent workers must share one inode.
	info, err := os.Lstat("/run/tether")
	if err != nil || !info.IsDir() || info.Mode().Perm()&0022 != 0 {
		return nil, infracontract.ErrUnsupported
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 {
		return nil, infracontract.ErrUnsupported
	}
	fd, err := unix.Open(lockPath, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0600)
	if err != nil {
		return nil, infracontract.ErrUnsupported
	}
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0077 != 0 || st.Uid != 0 || st.Nlink != 1 {
		_ = unix.Close(fd)
		return nil, infracontract.ErrUnsupported
	}
	if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = unix.Close(fd)
		return nil, ErrConflict
	}
	return func() { _ = unix.Close(fd) }, nil
}

func (b *linuxBackend) Read(deadline infracontract.Deadline, stop infracontract.Stop) (Snapshot, error) {
	if err := active(deadline, stop); err != nil {
		return Snapshot{}, err
	}
	unlock, err := b.lock()
	if err != nil {
		return Snapshot{}, err
	}
	defer unlock()
	return b.read(deadline, stop)
}

func (b *linuxBackend) read(deadline infracontract.Deadline, stop infracontract.Stop) (Snapshot, error) {
	data, err := b.run(false, nil, deadline, stop)
	if err != nil {
		return Snapshot{}, err
	}
	return inspectRules(data)
}

// origin: hot-update S9 plan §5.2 / §6.2 / T16
func (b *linuxBackend) CompareAndSwap(before, after Snapshot, deadline infracontract.Deadline, stop infracontract.Stop) (Snapshot, error) {
	if err := active(deadline, stop); err != nil {
		return Snapshot{}, err
	}
	if !validSnapshot(before) || !validSnapshot(after) || before.Generation == math.MaxUint64 || after.Generation != before.Generation+1 {
		return Snapshot{}, infracontract.ErrIncomplete
	}
	unlock, err := b.lock()
	if err != nil {
		return Snapshot{}, err
	}
	defer unlock()
	current, err := b.read(deadline, stop)
	if err != nil {
		return Snapshot{}, err
	}
	// The persisted kernel marker resolves a lost reply with the same digest.
	if current == after {
		return current, nil
	}
	if current != before {
		return Snapshot{}, ErrConflict
	}
	batch := ruleBatch(before, after)
	// Check kernel hooks, expression/version support and foreign rules before
	// effect. Replacing only our owned table preserves every conntrack entry.
	if _, err = b.run(true, batch, deadline, stop); err != nil {
		return Snapshot{}, err
	}
	if err = active(deadline, stop); err != nil {
		return Snapshot{}, err
	}
	_, applyErr := b.run(false, batch, deadline, stop)
	// Even an error may follow commit (reply lost). Query under the same lock;
	// if the budget expired, leave an unknown result for the next root worker.
	current, err = b.read(deadline, stop)
	if err != nil {
		return Snapshot{}, ErrResultUnknown
	}
	if current == after {
		return current, nil
	}
	if applyErr != nil {
		if current == before {
			return Snapshot{}, applyErr
		}
		return Snapshot{}, ErrResultUnknown
	}
	return Snapshot{}, ErrConflict
}

func validSnapshot(s Snapshot) bool {
	return s.Version == Version && (s.Generation > 0 && s.Digest != (infracontract.Digest{}) ||
		s.Generation == 0 && s.Digest == (infracontract.Digest{}) && !s.Redirect)
}

type ruleMarker struct {
	Version    uint16 `json:"v"`
	Generation uint64 `json:"g"`
	Digest     string `json:"d"`
	Redirect   bool   `json:"r"`
}

func marker(s Snapshot) string {
	data, _ := json.Marshal(ruleMarker{s.Version, s.Generation, hex.EncodeToString(s.Digest[:]), s.Redirect})
	return string(data)
}

func parseMarker(value string) (Snapshot, error) {
	var m ruleMarker
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&m); err != nil {
		return Snapshot{}, infracontract.ErrIncomplete
	}
	canonical, _ := json.Marshal(m)
	if string(canonical) != value {
		return Snapshot{}, infracontract.ErrIncomplete
	}
	data, err := hex.DecodeString(m.Digest)
	if err != nil || len(data) != 32 {
		return Snapshot{}, infracontract.ErrIncomplete
	}
	s := Snapshot{Version: m.Version, Generation: m.Generation, Redirect: m.Redirect}
	copy(s.Digest[:], data)
	if !validSnapshot(s) || s.Generation == 0 {
		return Snapshot{}, infracontract.ErrIncomplete
	}
	return s, nil
}

// nft JSON values are tool-schema objects, never root IPC DTOs. All expressions
// are product literals; the only variable text is our closed generation marker.
type nftObject map[string]json.RawMessage
type nftDocument struct {
	Objects []nftObject `json:"nftables"`
}

func object(kind string, body any) nftObject {
	data, _ := json.Marshal(body)
	return nftObject{kind: data}
}

func fixedObject(kind, body string) nftObject { return nftObject{kind: json.RawMessage(body)} }

func ruleObjects(s Snapshot) []nftObject {
	objects := []nftObject{fixedObject("table", `{"family":"inet","name":"tether_ingress"}`)}
	for _, chain := range []string{
		`{"family":"inet","table":"tether_ingress","name":"generation"}`,
		`{"family":"inet","table":"tether_ingress","name":"prerouting","type":"nat","hook":"prerouting","prio":-101,"policy":"accept"}`,
		`{"family":"inet","table":"tether_ingress","name":"output","type":"nat","hook":"output","prio":-101,"policy":"accept"}`,
		`{"family":"inet","table":"tether_ingress","name":"admin_guard","type":"filter","hook":"output","prio":-1,"policy":"accept"}`,
	} {
		objects = append(objects, fixedObject("chain", chain))
	}
	objects = append(objects, object("rule", struct {
		Family  string          `json:"family"`
		Table   string          `json:"table"`
		Chain   string          `json:"chain"`
		Expr    json.RawMessage `json:"expr"`
		Comment string          `json:"comment"`
	}{"inet", tableName, "generation", json.RawMessage(`[{"accept":null}]`), marker(s)}))
	// Isolate the stock root loopback admin from every non-root local UID.
	// This rule persists through both redirect directions, including bootstrap.
	objects = append(objects, fixedObject("rule", `{"family":"inet","table":"tether_ingress","chain":"admin_guard","expr":[{"match":{"op":"==","left":{"fib":{"result":"type","flags":["daddr"]}},"right":"local"}},{"match":{"op":"==","left":{"payload":{"protocol":"tcp","field":"dport"}},"right":2019}},{"match":{"op":"!=","left":{"meta":{"key":"skuid"}},"right":0}},{"reject":{"type":"tcp reset"}}]}`))
	if s.Redirect {
		for _, chain := range []string{"prerouting", "output"} {
			for _, family := range []string{"ipv4", "ipv6"} {
				body := fmt.Sprintf(`{"family":"inet","table":"tether_ingress","chain":%q,"expr":[{"match":{"op":"==","left":{"meta":{"key":"nfproto"}},"right":%q}},{"match":{"op":"==","left":{"fib":{"result":"type","flags":["daddr"]}},"right":"local"}},{"match":{"op":"==","left":{"payload":{"protocol":"tcp","field":"dport"}},"right":443}},{"match":{"op":"!=","left":{"meta":{"key":"mark"}},"right":%d}},{"match":{"op":"==","left":{"ct":{"key":"state"}},"right":"new"}},{"redirect":{"port":8443}}]}`, chain, family, PrimaryMark)
				objects = append(objects, fixedObject("rule", body))
			}
		}
	}
	return objects
}

func ruleBatch(before, after Snapshot) []byte {
	var commands []nftObject
	if before.Generation > 0 {
		commands = append(commands, object("delete", fixedObject("table", `{"family":"inet","name":"tether_ingress"}`)))
	}
	for _, item := range ruleObjects(after) {
		commands = append(commands, object("create", item))
	}
	data, _ := json.Marshal(nftDocument{commands})
	return data
}

// inspectRules verifies actual expressions, hooks and complete object union,
// not just a marker an altered rule could leave behind. Unknown own objects or
// any foreign drop/reject/NAT/flowtable are unsupported until independently
// audited; a ruleset with silent competing ingress behavior cannot sign ready.
// origin: hot-update S9 plan §5.2 / T16
func inspectRules(data []byte) (Snapshot, error) {
	if len(data) == 0 || len(data) > infracontract.MaxCensusBytes {
		return Snapshot{}, infracontract.ErrIncomplete
	}
	var doc nftDocument
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&doc); err != nil {
		return Snapshot{}, infracontract.ErrIncomplete
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) || doc.Objects == nil {
		return Snapshot{}, infracontract.ErrIncomplete
	}
	var own []nftObject
	var result Snapshot
	for _, item := range doc.Objects {
		if len(item) != 1 {
			return Snapshot{}, infracontract.ErrIncomplete
		}
		for kind, raw := range item {
			if kind == "metainfo" {
				continue
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(raw, &fields); err != nil {
				return Snapshot{}, infracontract.ErrIncomplete
			}
			var family, table, name string
			_ = json.Unmarshal(fields["family"], &family)
			_ = json.Unmarshal(fields["table"], &table)
			_ = json.Unmarshal(fields["name"], &name)
			owned := family == "inet" && (table == tableName || kind == "table" && name == tableName)
			if !owned {
				if family == "inet" && name == tableName || unsafeForeign(fields, kind) {
					return Snapshot{}, infracontract.ErrUnsupported
				}
				continue
			}
			delete(fields, "handle")
			delete(fields, "index")
			normalized, _ := json.Marshal(fields)
			own = append(own, nftObject{kind: normalized})
			if kind == "rule" && string(fields["chain"]) == `"generation"` {
				if result.Generation != 0 {
					return Snapshot{}, infracontract.ErrIncomplete
				}
				var value string
				if err := json.Unmarshal(fields["comment"], &value); err != nil {
					return Snapshot{}, infracontract.ErrIncomplete
				}
				var err error
				result, err = parseMarker(value)
				if err != nil {
					return Snapshot{}, err
				}
			}
		}
	}
	if len(own) == 0 {
		return Snapshot{Version: Version}, nil
	}
	if result.Generation == 0 || !sameObjects(own, ruleObjects(result)) {
		return Snapshot{}, infracontract.ErrIncomplete
	}
	return result, nil
}

func sameObjects(a, b []nftObject) bool {
	// Object keys are unordered, but rule order inside each chain is material.
	// nft dumps chains before rules, so compare that order independent of our
	// declaration interleaving. Never sort rules by expression or comment.
	group := func(rows []nftObject) (map[string][]any, bool) {
		out := make(map[string][]any)
		for _, row := range rows {
			for kind, raw := range row {
				if kind != "table" && kind != "chain" && kind != "rule" {
					return nil, false
				}
				var fields map[string]any
				if json.Unmarshal(raw, &fields) != nil {
					return nil, false
				}
				key := kind
				if kind == "rule" {
					key += ":" + fmt.Sprint(fields["chain"])
				}
				if kind == "chain" {
					key += ":" + fmt.Sprint(fields["name"])
				}
				out[key] = append(out[key], fields)
			}
		}
		return out, true
	}
	left, lok := group(a)
	right, rok := group(b)
	return lok && rok && reflect.DeepEqual(left, right)
}

func unsafeForeign(fields map[string]json.RawMessage, kind string) bool {
	if kind == "flowtable" || string(fields["policy"]) == `"drop"` {
		return true
	}
	// Verdict maps can hide drop and redirect inside a set element, so walk
	// the full tool object instead of only scanning top-level rule statements.
	var value any
	data, _ := json.Marshal(fields)
	_ = json.Unmarshal(data, &value)
	return unsafeValue(value)
}

func unsafeValue(value any) bool {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if key == "drop" || key == "reject" || key == "dnat" || key == "redirect" || key == "tproxy" || key == "notrack" {
				return true
			}
			if unsafeValue(child) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if unsafeValue(child) {
				return true
			}
		}
	}
	return false
}

func operation(deadline infracontract.Deadline, stop infracontract.Stop) (context.Context, func(), error) {
	if err := active(deadline, stop); err != nil {
		return nil, nil, err
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), remaining) // ctx-root: bounded root tool/dial operation, driven by Deadline/Stop.
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, func() { cancel(); <-joined }, nil
}

type boundedOutput struct{ bytes.Buffer }

func (b *boundedOutput) Write(data []byte) (int, error) {
	if b.Len()+len(data) > infracontract.MaxCensusBytes {
		return 0, infracontract.ErrIncomplete
	}
	return b.Buffer.Write(data)
}

func nftRunner(binary string) func(bool, []byte, infracontract.Deadline, infracontract.Stop) ([]byte, error) {
	return func(check bool, batch []byte, deadline infracontract.Deadline, stop infracontract.Stop) ([]byte, error) {
		ctx, closeOperation, err := operation(deadline, stop)
		if err != nil {
			return nil, err
		}
		defer closeOperation()
		args := []string{"--json"}
		if batch == nil {
			args = append(args, "list", "ruleset")
		} else {
			if check {
				args = append(args, "--check")
			}
			args = append(args, "--file", "-")
		}
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Env = []string{"LANG=C", "LC_ALL=C"}
		cmd.Stdin = bytes.NewReader(batch)
		var output boundedOutput
		cmd.Stdout = &output
		cmd.Stderr = io.Discard
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
		cmd.Cancel = func() error {
			err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			if errors.Is(err, syscall.ESRCH) {
				return os.ErrProcessDone
			}
			return err
		}
		cmd.WaitDelay, _ = controlbudget.Remaining(deadline)
		if err = cmd.Run(); err != nil {
			if ctx.Err() != nil {
				return nil, errors.Join(ErrBudgetExceeded, ctx.Err())
			}
			return nil, infracontract.ErrUnsupported
		}
		return output.Bytes(), nil
	}
}

// TLSAttempt keeps every first attempt, including failures. Callers retain a
// sticky failure ledger; an ensuing WSS retry never erases this raw TLS result.
type TLSAttempt struct {
	StartedNS  int64
	FinishedNS int64
	Success    bool
	Mark       uint32
}

func DialPrimary(address string, config *tls.Config, deadline infracontract.Deadline, stop infracontract.Stop) (*tls.Conn, TLSAttempt, error) {
	return dialTLS(address, config, true, deadline, stop)
}

func DialCanonical(address string, config *tls.Config, deadline infracontract.Deadline, stop infracontract.Stop) (*tls.Conn, TLSAttempt, error) {
	return dialTLS(address, config, false, deadline, stop)
}

func dialTLS(address string, config *tls.Config, bypass bool, deadline infracontract.Deadline, stop infracontract.Stop) (*tls.Conn, TLSAttempt, error) {
	var attempt TLSAttempt
	host, port, err := net.SplitHostPort(address)
	ip, parseErr := netip.ParseAddr(host)
	if err != nil || parseErr != nil || !ip.IsValid() || ip.IsUnspecified() || ip.IsMulticast() || port != "443" ||
		config == nil || config.InsecureSkipVerify || config.ServerName == "" {
		return nil, attempt, ErrProbe
	}
	if runtime.GOARCH != "amd64" || bypass && os.Geteuid() != 0 {
		return nil, attempt, infracontract.ErrUnsupported
	}
	ctx, closeOperation, err := operation(deadline, stop)
	if err != nil {
		return nil, attempt, err
	}
	defer closeOperation()
	clone := config.Clone()
	clone.ClientSessionCache = nil
	if clone.MinVersion < tls.VersionTLS12 {
		clone.MinVersion = tls.VersionTLS12
	}
	dialer := &net.Dialer{}
	if bypass {
		attempt.Mark = PrimaryMark
		dialer.Control = primaryControl
	}
	attempt.StartedNS, err = controlbudget.Now()
	if err != nil {
		return nil, attempt, err
	}
	raw, err := dialer.DialContext(ctx, "tcp", address)
	if err == nil {
		conn := tls.Client(raw, clone)
		err = conn.HandshakeContext(ctx)
		attempt.FinishedNS, _ = controlbudget.Now()
		if err == nil {
			attempt.Success = true
			return conn, attempt, nil
		}
		_ = conn.Close()
	}
	attempt.FinishedNS, _ = controlbudget.Now()
	return nil, attempt, ErrProbe
}

// Setting the mark in Control happens before connect sends the first SYN.
// Ignoring setsockopt failure would let the primary probe reach healthy shadow.
// origin: hot-update S9 plan §5.2 / T17
func primaryControl(_, _ string, raw syscall.RawConn) error {
	return markControl(raw, unix.SetsockoptInt)
}

func markControl(raw syscall.RawConn, set func(int, int, int, int) error) error {
	var markErr error
	if err := raw.Control(func(fd uintptr) { markErr = set(int(fd), unix.SOL_SOCKET, unix.SO_MARK, PrimaryMark) }); err != nil {
		return err
	}
	return markErr
}
