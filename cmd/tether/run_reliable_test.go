package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/execclient"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/terminalio"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

// origin: hot-update S4 plan §7.3 / T39; PRODUCT-R4 stderr audit
// Optional watchdog setup can warn synchronously. Pin its call before the
// legacy raw-mode boundary, so a setup refusal cannot pollute remote bytes.
func TestRunLegacyWatchdogDiagnosticPrecedesRawMode(t *testing.T) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "run.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var watchdog, raw []token.Pos
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "newRunCmd" {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			name, ok := call.Fun.(*ast.Ident)
			if !ok {
				return true
			}
			switch name.Name {
			case "startRunWatchdog":
				watchdog = append(watchdog, call.Pos())
			case "enterRawMode":
				raw = append(raw, call.Pos())
			}
			return true
		})
	}
	if len(watchdog) != 1 || len(raw) != 1 || watchdog[0] >= raw[0] {
		t.Fatalf("watchdog warning crossed raw-mode boundary: watchdog=%v raw=%v", watchdog, raw)
	}
}

// The injected failures test the CLI boundary, not a replacement protocol or
// terminal implementation. Real bytes/termios and endpoint skew belong to P13.
// origin: hot-update S4 plan §8 T49 / §12 P12
func TestRunReliableErrorExitClasses(t *testing.T) {
	handle := proto.ProcessHandle{RequestID: "original-task"}
	created := execclient.Result{Handle: handle, CreateSent: true}
	inputAbort := execclient.Result{Handle: handle, CreateSent: true, Executed: true,
		InputAbort: "write_error", InputACK: 7, InputAccepted: 19}
	for _, tc := range []struct {
		name       string
		result     execclient.Result
		err        error
		class      int
		code, text string
	}{
		{"configuration", execclient.Result{}, &execclient.ProtocolError{Code: "bad_request", Message: "bad_request"}, 64, "bad_request", "bad_request"},
		{"capacity", execclient.Result{}, &execclient.ProtocolError{Code: proto.CodeProcessCapacity, Message: "full"}, 75, proto.CodeProcessCapacity, "full"},
		{"recovering_probe", execclient.Result{}, &execclient.ProtocolError{Code: proto.CodeProcessRecovering, Message: "recovering"}, 75, proto.CodeProcessRecovering, "recovering"},
		{"recovering_created", created, &execclient.ProtocolError{Code: proto.CodeProcessRecovering, Message: "recovering", AfterCreate: true}, 74, proto.CodeProcessRecovering, "result is unknown"},
		{"busy", execclient.Result{}, &execclient.ProtocolError{Code: proto.CodeProcessBusy, Message: "busy"}, 75, proto.CodeProcessBusy, "busy"},
		{"busy_before_admission", created, &execclient.ProtocolError{Code: proto.CodeProcessBusy, Message: "busy"}, 75, proto.CodeProcessBusy, "busy"},
		{"permission_probe", execclient.Result{}, &execclient.ProtocolError{Code: "not_a_member", Message: "denied"}, 77, "not_a_member", "denied"},
		{"revoked_probe", execclient.Result{}, &execclient.ProtocolError{Code: proto.CodeProcessRevoked, Message: "revoked"}, 77, proto.CodeProcessRevoked, "revoked"},
		{"revoked_created", created, &execclient.ProtocolError{Code: proto.CodeProcessRevoked, Message: "revoked", AfterCreate: true}, 77, proto.CodeProcessRevoked, "revoked"},
		{"permission_over_input_abort", inputAbort, &execclient.ProtocolError{Code: "not_a_member", Message: "denied", AfterCreate: true}, 77, "not_a_member", "denied"},
		{"permission_created", created, &execclient.ProtocolError{Code: "not_a_member", Message: "denied", AfterCreate: true}, 77, "not_a_member", "denied"},
		{"unknown_probe", execclient.Result{}, &execclient.ProtocolError{Code: "future_pty_refusal", Message: "future refusal"}, 74, "future_pty_refusal", "future refusal"},
		{"unknown_created", created, &execclient.ProtocolError{Code: "future_pty_refusal", Message: "future refusal"}, 74, "future_pty_refusal", "result is unknown"},
		{"deadline_probe", execclient.Result{}, context.DeadlineExceeded, 75, "", "before the command was accepted"},
		{"lost_create_reply", created, context.DeadlineExceeded, 74, "", "recovery window expired after 3m0s"},
		{"cancel_created", created, context.Canceled, 74, "", "interrupted"},
		{"unsupported_created", created, execclient.ErrUnsupported, 74, "", "result is unknown"},
		{"input_abort_rc0", inputAbort, nil, 74, "", "applied=7 accepted=19"},
		{"input_abort_settled", created, &execclient.SettledError{Result: inputAbort}, 74, "", "write_error"},
		{"stdout_abort_rc0", execclient.Result{CreateSent: true, Executed: true, StdoutAbort: "replay_error", StdoutBytes: 31}, nil, 74, "", "stdout aborted after 31 byte(s)"},
		{"setup_never_ran", execclient.Result{}, &execclient.SettledError{Result: execclient.Result{TerminalError: "argv_required", ExitCode: -1}}, 64, "", "argv_required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := classifyReliableRunFailure("node1", tc.result, tc.err)
			if got := classifyExit(err); got != tc.class {
				t.Fatalf("exit=%d want=%d: %v", got, tc.class, err)
			}
			if wireCodeOf(err) != tc.code || !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("code=%q error=%v; want code=%q text=%q", wireCodeOf(err), err, tc.code, tc.text)
			}
			if tc.result.InputAbort != "" && tc.class == 74 && !strings.Contains(err.Error(), "remote exit=0") {
				t.Fatalf("input abort lost the actual remote exit: %v", err)
			}
		})
	}

	for _, tc := range []struct {
		name                                                    string
		probeErr, openErr, rawErr, runErr, restoreErr, closeErr error
		result                                                  execclient.Result
		legacy                                                  bool
		class                                                   int
		trace                                                   []string
		warning                                                 string
	}{
		{"remote23", nil, nil, nil, nil, nil, nil, execclient.Result{CreateSent: true, Executed: true, ExitCode: 23}, false, 0,
			[]string{"probe", "open", "raw", "events", "run", "join", "restore", "close", "client-close", "exit:23"}, ""},
		{"old_endpoint", execclient.ErrUnsupported, nil, nil, nil, nil, nil, execclient.Result{}, true, 0,
			[]string{"probe"}, ""},
		{"denied_probe", &execclient.ProtocolError{Code: "not_a_member", Message: "denied"}, nil, nil, nil, nil, nil, execclient.Result{}, false, 77,
			[]string{"probe"}, ""},
		{"socket_stdio", nil, fmt.Errorf("open: %w", &terminalio.ErrUnsupportedStdio{Stream: "stdout", Kind: "socket"}), nil, nil, nil, nil, execclient.Result{}, true, 0,
			[]string{"probe", "open", "client-close"}, "tether: stdout is a socket; using legacy run (no hot-update continuity)\n"},
		{"open_fault", nil, errors.New("open failed"), nil, nil, nil, nil, execclient.Result{}, false, 74,
			[]string{"probe", "open", "client-close"}, ""},
		{"raw_fault", nil, nil, errors.New("raw failed"), nil, nil, nil, execclient.Result{}, false, 74,
			[]string{"probe", "open", "raw", "restore", "close", "client-close"}, ""},
		{"denied_created", nil, nil, nil, &execclient.ProtocolError{Code: "not_a_member", Message: "denied", AfterCreate: true}, nil, nil, created, false, 77,
			[]string{"probe", "open", "raw", "events", "run", "join", "restore", "close", "client-close"}, ""},
		// origin: hot-update S4 plan §2.2 / T38 / T49
		// An explicit pre-Reserve refusal still owns raw-terminal cleanup after
		// create was sent. It must neither invoke legacy nor bypass worker join.
		{"busy_created_cleanup", nil, nil, nil, &execclient.ProtocolError{Code: proto.CodeProcessBusy, Message: "busy"}, nil, nil, created, false, 75,
			[]string{"probe", "open", "raw", "events", "run", "join", "restore", "close", "client-close"}, ""},
		{"unsupported_created_no_fallback", nil, nil, nil, execclient.ErrUnsupported, nil, nil, created, false, 74,
			[]string{"probe", "open", "raw", "events", "run", "join", "restore", "close", "client-close"}, ""},
		{"input_abort_no_exit0", nil, nil, nil, nil, nil, nil, inputAbort, false, 74,
			[]string{"probe", "open", "raw", "events", "run", "join", "restore", "close", "client-close"}, ""},
		{"restore_fault_no_exit0", nil, nil, nil, nil, errors.New("restore failed"), nil, created, false, 74,
			[]string{"probe", "open", "raw", "events", "run", "join", "restore", "close", "client-close"}, ""},
		{"close_fault_no_exit0", nil, nil, nil, nil, nil, errors.New("close failed"), created, false, 74,
			[]string{"probe", "open", "raw", "events", "run", "join", "restore", "close", "client-close"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var trace []string
			var warning bytes.Buffer
			terminal := &runTestTerminal{trace: &trace, rawErr: tc.rawErr, restoreErr: tc.restoreErr, closeErr: tc.closeErr}
			spec := proto.RunReq{Argv: []string{"bash", "-c", "exit 23"}, Cwd: "/tmp", Safe: true,
				Env: map[string]string{"TERM": "xterm", "LANG": "C"}, Cols: 91, Rows: 37, KAIntervalMS: 5000}
			client := &runTestClient{trace: &trace, result: tc.result, err: tc.runErr,
				check: func(ctx context.Context, got proto.RunReq, in execclient.Source, out execclient.Sink, events <-chan execclient.ControlEvent) {
					if ctx != t.Context() || !reflect.DeepEqual(got, spec) || in != terminal || out != terminal || events == nil || !terminal.raw {
						t.Fatal("create did not receive the caller context, intact spec, raw terminal and cancellable I/O")
					}
				}}
			deps := reliableRunDeps{
				probe: func(context.Context) (runPTYClient, error) {
					trace = append(trace, "probe")
					return client, tc.probeErr
				},
				open: func() (runTerminal, error) { trace = append(trace, "open"); return terminal, tc.openErr },
				events: func(context.Context) (<-chan execclient.ControlEvent, func()) {
					trace = append(trace, "events")
					return make(chan execclient.ControlEvent), func() { trace = append(trace, "join") }
				},
				exit: func(result execclient.Result) {
					if terminal.raw {
						t.Fatal("exit invoked before restoring original terminal")
					}
					trace = append(trace, fmt.Sprintf("exit:%d", result.ExitCode))
				},
			}
			legacy, err := runReliableBranch(t.Context(), "node1", spec, &warning, deps)
			if legacy != tc.legacy || classifyExit(err) != tc.class || !reflect.DeepEqual(trace, tc.trace) || warning.String() != tc.warning {
				t.Fatalf("legacy=%v exit=%d trace=%v warning=%q err=%v; want legacy=%v exit=%d trace=%v warning=%q",
					legacy, classifyExit(err), trace, warning.String(), err, tc.legacy, tc.class, tc.trace, tc.warning)
			}
		})
	}

	t.Run("resize_preserves_original_stdout_flags", func(t *testing.T) {
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		original := os.Stdout
		os.Stdout = writer
		defer func() { os.Stdout = original; _ = reader.Close(); _ = writer.Close() }()
		flags := func() int {
			raw, err := writer.SyscallConn()
			if err != nil {
				t.Fatal(err)
			}
			var value int
			var flagErr error
			if err := raw.Control(func(fd uintptr) { value, flagErr = unix.FcntlInt(fd, unix.F_GETFL, 0) }); err != nil {
				t.Fatal(err)
			}
			if flagErr != nil {
				t.Fatal(flagErr)
			}
			return value
		}
		before := flags()
		if before&unix.O_NONBLOCK == 0 {
			t.Fatal("fixture is not a pollable nonblocking descriptor")
		}
		cols, rows := terminalSize()
		if cols != 80 || rows != 24 || flags() != before {
			t.Fatalf("resize changed inherited OFD flags or non-TTY defaults: cols=%d rows=%d flags=%x want=%x", cols, rows, flags(), before)
		}
	})

	t.Run("stdio_diagnostic_names_actual_descriptor", func(t *testing.T) {
		null, err := os.Open(os.DevNull)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = null.Close() }()
		listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(t.TempDir(), "stdio.sock"), Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = listener.Close() }()
		socket, err := listener.File()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = socket.Close() }()
		// The notice must name the descriptor terminalio actually refused, so this
		// drives the real terminalio.Open rather than a second classifier.
		for _, tc := range []struct {
			in, out *os.File
			stream  string
		}{
			{socket, null, "stdin"}, {null, socket, "stdout"},
		} {
			var trace []string
			var notice bytes.Buffer
			_, legacy, err := runReliablePTY(t.Context(), proto.RunReq{}, &notice, reliableRunDeps{
				probe: func(context.Context) (runPTYClient, error) { return &runTestClient{trace: &trace}, nil },
				open:  func() (runTerminal, error) { return terminalio.Open(tc.in, tc.out) },
			})
			want := "tether: " + tc.stream + " is a socket; using legacy run (no hot-update continuity)\n"
			if err != nil || !legacy || notice.String() != want {
				t.Fatalf("socket %s: legacy=%t err=%v notice=%q, want %q", tc.stream, legacy, err, notice.String(), want)
			}
		}
	})

	// The resize adapter owns a goroutine and a signal registration. Cancellation
	// must join it even if no signal was delivered and the protocol is already done.
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for range 5 {
		ctx, cancel := context.WithCancel(t.Context())
		events, stop := runResizeEvents(ctx)
		cancel()
		stop()
		if _, ok := <-events; ok {
			t.Fatal("resize events remained open after join")
		}
	}
	testharness.AssertNoGoroutineLeak(t, "reliable run resize adapter", beforeG)
	testharness.AssertNoFDLeak(t, "reliable run resize adapter", beforeFD, 0)

	// The shared exec classifier must also keep input responsibility when the
	// running phase was lost; a terminal setup hint must not disguise that loss.
	inputOnly := inputAbort
	inputOnly.Executed = false
	inputOnly.TerminalError = "argv_required"
	if err := classifyReliableFailure("node1", 0, created, &execclient.SettledError{Result: inputOnly}); classifyExit(err) != 74 {
		t.Fatalf("shared classifier lost accepted input responsibility: %v", err)
	}

	for _, reason := range []string{"pty_input_overflow", "pty_input_overflow: buffer full"} {
		err := runFailureMessage(reason)
		if classifyExit(err) != 74 || !strings.Contains(err.Error(), "task may have executed") || !strings.Contains(err.Error(), reason) {
			t.Fatalf("overflow must preserve execution uncertainty and exit 74: %v", err)
		}
	}
}

type runTestTerminal struct {
	trace                        *[]string
	raw                          bool
	rawErr, restoreErr, closeErr error
}

func (t *runTestTerminal) EnterRaw() error {
	*t.trace = append(*t.trace, "raw")
	t.raw = true
	return t.rawErr
}
func (t *runTestTerminal) Restore() error {
	*t.trace = append(*t.trace, "restore")
	t.raw = false
	return t.restoreErr
}
func (t *runTestTerminal) Close() error              { *t.trace = append(*t.trace, "close"); return t.closeErr }
func (t *runTestTerminal) Source() terminalio.Source { return t }
func (t *runTestTerminal) Sink() terminalio.Sink     { return t }
func (*runTestTerminal) Read([]byte) (int, error)    { return 0, io.EOF }
func (*runTestTerminal) Write(b []byte) (int, error) { return len(b), nil }
func (*runTestTerminal) Cancel() error               { return nil }

type runTestClient struct {
	trace  *[]string
	result execclient.Result
	err    error
	check  func(context.Context, proto.RunReq, execclient.Source, execclient.Sink, <-chan execclient.ControlEvent)
}

func (c *runTestClient) Run(ctx context.Context, spec proto.RunReq, in execclient.Source, out execclient.Sink, events <-chan execclient.ControlEvent) (execclient.Result, error) {
	*c.trace = append(*c.trace, "run")
	c.check(ctx, spec, in, out, events)
	return c.result, c.err
}
func (c *runTestClient) Close() error { *c.trace = append(*c.trace, "client-close"); return nil }
