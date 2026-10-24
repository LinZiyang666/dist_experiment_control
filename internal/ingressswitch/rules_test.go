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
	"net"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

func init() { ruleChecks = exerciseRuleTransactions; markChecks = exerciseMarkControl }

// This independent stock-shaped dump does not call ruleObjects/marker. Chain
// grouping reflects nft output rather than the command's declaration order.
func ruleFixture(s Snapshot) []byte {
	comment := fmt.Sprintf(`{"v":1,"g":%d,"d":%q,"r":%t}`, s.Generation, hex.EncodeToString(s.Digest[:]), s.Redirect)
	var rows []json.RawMessage
	rows = append(rows, json.RawMessage(`{"metainfo":{"json_schema_version":1}}`), json.RawMessage(`{"table":{"family":"inet","name":"tether_ingress","handle":1}}`))
	for _, chain := range []string{
		`{"chain":{"family":"inet","table":"tether_ingress","name":"generation","handle":2}}`,
		`{"chain":{"family":"inet","table":"tether_ingress","name":"prerouting","type":"nat","hook":"prerouting","prio":-101,"policy":"accept","handle":3}}`,
		`{"chain":{"family":"inet","table":"tether_ingress","name":"output","type":"nat","hook":"output","prio":-101,"policy":"accept","handle":4}}`,
		`{"chain":{"family":"inet","table":"tether_ingress","name":"admin_guard","type":"filter","hook":"output","prio":-1,"policy":"accept","handle":5}}`,
	} {
		rows = append(rows, json.RawMessage(chain))
	}
	rows = append(rows, json.RawMessage(fmt.Sprintf(`{"rule":{"family":"inet","table":"tether_ingress","chain":"generation","expr":[{"accept":null}],"comment":%q,"handle":6}}`, comment)))
	if s.Redirect {
		for _, chain := range []string{"prerouting", "output"} {
			for _, family := range []string{"ipv4", "ipv6"} {
				rows = append(rows, json.RawMessage(fmt.Sprintf(`{"rule":{"family":"inet","table":"tether_ingress","chain":%q,"expr":[{"match":{"op":"==","left":{"meta":{"key":"nfproto"}},"right":%q}},{"match":{"op":"==","left":{"fib":{"result":"type","flags":["daddr"]}},"right":"local"}},{"match":{"op":"==","left":{"payload":{"protocol":"tcp","field":"dport"}},"right":443}},{"match":{"op":"!=","left":{"meta":{"key":"mark"}},"right":1414091848}},{"match":{"op":"==","left":{"ct":{"key":"state"}},"right":"new"}},{"redirect":{"port":8443}}],"handle":7}}`, chain, family)))
			}
		}
	}
	rows = append(rows, json.RawMessage(`{"rule":{"family":"inet","table":"tether_ingress","chain":"admin_guard","expr":[{"match":{"op":"==","left":{"fib":{"result":"type","flags":["daddr"]}},"right":"local"}},{"match":{"op":"==","left":{"payload":{"protocol":"tcp","field":"dport"}},"right":2019}},{"match":{"op":"!=","left":{"meta":{"key":"skuid"}},"right":0}},{"reject":{"type":"tcp reset"}}],"handle":8}}`))
	data, _ := json.Marshal(struct {
		Rows []json.RawMessage `json:"nftables"`
	}{rows})
	return data
}

type nftMemory struct {
	current      Snapshot
	checks       int
	applies      int
	lockHeld     bool
	lostReply    bool
	noEffect     bool
	checkErr     bool
	readErr      bool
	readAfterErr bool
	reads        int
	foreign      bool
	batch        []byte
}

func (m *nftMemory) backend(t *testing.T) *linuxBackend {
	t.Helper()
	return &linuxBackend{lock: func() (func(), error) {
		if m.lockHeld {
			return nil, ErrConflict
		}
		m.lockHeld = true
		return func() { m.lockHeld = false }, nil
	}, run: func(check bool, batch []byte, _ infracontract.Deadline, _ infracontract.Stop) ([]byte, error) {
		if !m.lockHeld {
			t.Fatal("rule operation outside permanent lock")
		}
		if batch == nil {
			m.reads++
			if m.readAfterErr && m.reads > 1 {
				return nil, errors.New("query lost")
			}
			if m.readErr {
				return nil, errors.New("read failed")
			}
			if m.current.Generation == 0 {
				return []byte(`{"nftables":[]}`), nil
			}
			data := ruleFixture(m.current)
			if m.foreign {
				data = bytes.Replace(data, []byte(`"nftables":[`), []byte(`"nftables":[{"chain":{"family":"inet","table":"operator","name":"input","policy":"drop"}},`), 1)
			}
			return data, nil
		}
		if check {
			m.checks++
			if m.checkErr {
				return nil, infracontract.ErrUnsupported
			}
			return nil, nil
		}
		m.applies++
		m.batch = append([]byte(nil), batch...)
		if !m.noEffect {
			var doc nftDocument
			if err := json.Unmarshal(batch, &doc); err != nil {
				t.Fatal(err)
			}
			var rows []nftObject
			for _, cmd := range doc.Objects {
				if raw, ok := cmd["create"]; ok {
					var row nftObject
					if err := json.Unmarshal(raw, &row); err != nil {
						t.Fatal(err)
					}
					rows = append(rows, row)
				}
			}
			data, _ := json.Marshal(nftDocument{rows})
			after, err := inspectRules(data)
			if err != nil {
				t.Fatalf("invalid atomic apply: %v", err)
			}
			m.current = after
		}
		if m.lostReply {
			return nil, errors.New("lost reply")
		}
		return nil, nil
	}}
}

// origin: hot-update S9 plan §5.2 / T16
func exerciseRuleTransactions(t *testing.T) {
	before, after := changeFixture(false).Before, changeFixture(false).After
	t.Run("single-batch-all-hooks-and-families", func(t *testing.T) {
		m := &nftMemory{current: before}
		b := m.backend(t)
		got, err := b.CompareAndSwap(before, after, deadline(t), nil)
		if err != nil || got != after || m.checks != 1 || m.applies != 1 || m.lockHeld {
			t.Fatalf("atomic CAS: %v", err)
		}
		if bytes.Contains(m.batch, []byte(`"flush"`)) || bytes.Contains(m.batch, []byte(`"ruleset"`)) {
			t.Fatal("host-wide mutation")
		}
		var doc nftDocument
		if err = json.Unmarshal(m.batch, &doc); err != nil {
			t.Fatal(err)
		}
		if len(doc.Objects) != 12 || len(doc.Objects[0]["delete"]) == 0 {
			t.Fatalf("split/incomplete transaction: %s", m.batch)
		}
		data := ruleFixture(after)
		observed, err := inspectRules(data)
		if err != nil || observed != after {
			t.Fatalf("stock-shaped dump rejected: %v", err)
		}
		// Explicit external, local OUTPUT and container-hairpin paths. The
		// predicate includes local FIB rather than relying on a physical iface.
		for _, chain := range []string{"prerouting", "output"} {
			for _, family := range []string{"ipv4", "ipv6"} {
				needle := fmt.Sprintf(`"chain":%q`, chain)
				if bytes.Count(m.batch, []byte(needle)) != 2 || bytes.Count(m.batch, []byte(`"right":"`+family+`"`)) != 2 {
					t.Fatalf("missing dual-stack/hairpin path: %s/%s", chain, family)
				}
			}
		}
		if bytes.Count(m.batch, []byte(`"result":"type","flags":["daddr"]`)) != 5 ||
			bytes.Count(m.batch, []byte(`"key":"mark"`)) != 4 || bytes.Count(m.batch, []byte(`"key":"state"`)) != 4 ||
			bytes.Count(m.batch, []byte(`"port":8443`)) != 4 {
			t.Fatal("new-flow/bypass/target predicate missing")
		}
	})
	t.Run("bootstrap-and-return-keep-admin-quarantine", func(t *testing.T) {
		for _, start := range []Snapshot{{Version: 1}, after} {
			next := Snapshot{Version: 1, Generation: start.Generation + 1, Digest: digest(12)}
			m := &nftMemory{current: start}
			b := m.backend(t)
			if _, err := b.CompareAndSwap(start, next, deadline(t), nil); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(m.batch, []byte(`"redirect"`)) || bytes.Count(m.batch, []byte(`"key":"skuid"`)) != 1 ||
				bytes.Count(m.batch, []byte(`"right":2019`)) != 1 {
				t.Fatal("return removed admin guard or retained redirect")
			}
		}
	})
	t.Run("CAS-lost-reply-and-preflight", func(t *testing.T) {
		for _, mode := range []string{"stale", "lost", "idempotent", "check-fail", "no-effect", "foreign", "read-error", "query-lost", "busy", "invalid"} {
			t.Run(mode, func(t *testing.T) {
				m := &nftMemory{current: before}
				b := m.backend(t)
				candidate := after
				wantErr := true
				expectedApplies := 0
				switch mode {
				case "stale":
					m.current.Generation++
				case "lost":
					m.lostReply = true
					wantErr = false
					expectedApplies = 1
				case "idempotent":
					m.current = after
					wantErr = false
				case "check-fail":
					m.checkErr = true
				case "no-effect":
					m.noEffect = true
					expectedApplies = 1
				case "foreign":
					m.foreign = true
				case "read-error":
					m.readErr = true
				case "query-lost":
					m.readAfterErr = true
					expectedApplies = 1
				case "busy":
					m.lockHeld = true
				case "invalid":
					candidate.Version++
				}
				_, err := b.CompareAndSwap(before, candidate, deadline(t), nil)
				if (err != nil) != wantErr || m.applies != expectedApplies {
					t.Fatalf("%s: err=%v applies=%d", mode, err, m.applies)
				}
				if mode == "query-lost" && !errors.Is(err, ErrResultUnknown) {
					t.Fatalf("sent effect misreported as safe refusal: %v", err)
				}
			})
		}
	})
	t.Run("altered-kernel-rules-fail-closed", func(t *testing.T) {
		good := string(ruleFixture(after))
		cases := []struct {
			name string
			from string
			to   string
		}{
			{"IPv6", `"right":"ipv6"`, `"right":"ipv4"`}, {"OUTPUT", `"hook":"output"`, `"hook":"input"`},
			{"hairpin", `"right":"local"`, `"right":"unicast"`}, {"bypass", `"right":1414091848`, `"right":0`},
			{"established-remap", `"right":"new"`, `"right":"established"`}, {"wrong-target", `"port":8443`, `"port":8444`},
			{"wrong-priority", `"prio":-101`, `"prio":100`}, {"admin-UID", `"right":0`, `"right":1000`},
			{"unknown-object", `"nftables":[`, `"nftables":[{"set":{"family":"inet","table":"tether_ingress","name":"extra"}},`},
			{"foreign-drop", `"nftables":[`, `"nftables":[{"rule":{"family":"inet","table":"operator","expr":[{"drop":null}]}},`},
			{"foreign-reject", `"nftables":[`, `"nftables":[{"rule":{"family":"inet","table":"operator","expr":[{"reject":null}]}},`},
			{"foreign-verdict-map", `"nftables":[`, `"nftables":[{"set":{"family":"inet","table":"operator","elem":[{"drop":null}]}},`},
			{"foreign-NAT", `"nftables":[`, `"nftables":[{"rule":{"family":"inet","table":"operator","expr":[{"dnat":{"port":8443}}]}},`},
			{"foreign-flowtable", `"nftables":[`, `"nftables":[{"flowtable":{"family":"inet","table":"operator"}},`},
			{"no-marker", `"chain":"generation"`, `"chain":"other"`}, {"wrong-union", `"family":"inet"`, `"family":"ip"`},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				bad := strings.Replace(good, tc.from, tc.to, 1)
				if bad == good {
					t.Fatal("nonempty mutation prerequisite missing")
				}
				if _, err := inspectRules([]byte(bad)); err == nil {
					t.Fatal("altered ingress signed ready")
				}
			})
		}
		for _, bad := range []string{"", `{}`, `{"nftables":null}`, good + `{}`, `{"nftables":[],"extra":1}`, `{"nftables":[{"table":{},"chain":{}}]}`} {
			if _, err := inspectRules([]byte(bad)); err == nil {
				t.Fatalf("invalid dump accepted: %q", bad)
			}
		}
	})
	t.Run("marker-canonical-binding", func(t *testing.T) {
		good := marker(after)
		for _, bad := range []string{good + " ", good + "{}", strings.Replace(good, `"v":1`, `"v":2`, 1),
			strings.Replace(good, `"g":5`, `"g":0`, 1), strings.Replace(good, `"r":true`, `"r":true,"r":false`, 1),
			strings.Replace(good, `"d":"`, `"d":"ff`, 1), strings.Replace(good, `"v":1`, `"v":1,"extra":1`, 1)} {
			if _, err := parseMarker(bad); err == nil {
				t.Fatalf("ambiguous marker accepted: %s", bad)
			}
		}
		if len(marker(Snapshot{Version: 1, Generation: ^uint64(0), Digest: digest(1), Redirect: true})) > 128 {
			t.Fatal("kernel comment limit exceeded")
		}
	})
}

type rawControl struct {
	err      error
	controls int
}

func (r *rawControl) Control(fn func(uintptr)) error {
	r.controls++
	if r.err != nil {
		return r.err
	}
	fn(123)
	return nil
}
func (*rawControl) Read(func(uintptr) bool) error  { return errors.New("unexpected read") }
func (*rawControl) Write(func(uintptr) bool) error { return errors.New("unexpected write") }

// origin: hot-update S9 plan §5.2 / T17
func exerciseMarkControl(t *testing.T) {
	raw := &rawControl{}
	calls := 0
	err := markControl(raw, func(fd, level, key, value int) error {
		calls++
		if fd != 123 || level != unix.SOL_SOCKET || key != unix.SO_MARK || value != 0x54495448 {
			t.Fatal("wrong mark syscall")
		}
		return nil
	})
	if err != nil || calls != 1 || raw.controls != 1 {
		t.Fatalf("mark not applied before connect: %v", err)
	}
	injected := errors.New("no capability")
	if err = markControl(&rawControl{}, func(int, int, int, int) error { return injected }); !errors.Is(err, injected) {
		t.Fatal("setsockopt error swallowed")
	}
	if err = markControl(&rawControl{err: injected}, func(int, int, int, int) error { t.Fatal("called after raw failure"); return nil }); !errors.Is(err, injected) {
		t.Fatal("raw Control error swallowed")
	}
	for _, address := range []string{"127.0.0.1:8443", "localhost:443", "0.0.0.0:443", "[ff02::1]:443", "[::]:443"} {
		if conn, attempt, err := DialPrimary(address, &tls.Config{ServerName: "example.test"}, deadline(t), nil); conn != nil || attempt.Success || err == nil {
			t.Fatal("unapproved probe address accepted")
		}
	}
	for _, config := range []*tls.Config{nil, {InsecureSkipVerify: true, ServerName: "example.test"}, {}} {
		if conn, _, err := DialCanonical("127.0.0.1:443", config, deadline(t), nil); conn != nil || err == nil {
			t.Fatal("unverified TLS accepted")
		}
	}
}

// origin: hot-update S9 plan §12 P07 / TestIngressSwitchLifecycle
func TestIngressSwitchLifecycle(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseIngress(t)
	}
	testharness.AssertNoGoroutineLeak(t, "ingress switch", beforeG)
	testharness.AssertNoFDLeak(t, "ingress switch", beforeFD, 0)
}

func exerciseIngress(t *testing.T) {
	t.Helper()
	stop := make(chan struct{})
	ctx, join, err := operation(deadline(t), stop)
	if err != nil {
		t.Fatal(err)
	}
	close(stop)
	<-ctx.Done()
	join()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("stop did not cancel root operation")
	}
	ctx, join, err = operation(deadline(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	join()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("close did not join root operation")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	joined := make(chan error, 1)
	accepted := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		close(accepted)
		if err == nil {
			defer func() { _ = conn.Close() }() // the verdict is the byte read below
			buf := make([]byte, 1)
			_, err = io.ReadFull(conn, buf)
			if err == nil && buf[0] != 42 {
				err = errors.New("wrong byte")
			}
		}
		joined <- err
	}()
	conn, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		<-joined
		t.Fatal(err)
	}
	<-accepted
	_, err = conn.Write([]byte{42})
	_ = conn.Close()
	_ = listener.Close()
	acceptErr := <-joined
	if err != nil || acceptErr != nil {
		t.Fatalf("raw probe lifecycle: write=%v accept=%v", err, acceptErr)
	}
	s, _, _, _, req := controllerFixture(t, false)
	if _, err = s.Do(req, deadline(t), nil); err != nil {
		t.Fatal(err)
	}
	if receipt, err := s.Do(req, deadline(t), nil); err != nil || !receipt.Applied {
		t.Fatalf("committed effect replay did not resolve: %v", err)
	}
}
