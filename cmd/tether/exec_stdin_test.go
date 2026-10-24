package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/procforward"
	"github.com/LinZiyang666/tether/internal/proto"
)

// exec_stdin_test.go — `tether exec --stdin`: the local read is bounded, an
// over-limit body is a usage error (64) decided before any connection, and a
// body at the limit still fits the reliable create envelope under the 1 MiB
// request bound the broker (procforward.MaxRequestBytes), the registry's
// intent hash and NATS' default max_payload all enforce.
//
// origin: hot-update S1 plan §4.2 promised this coverage under another name
// (over-limit → 64 + zero publish; limit + envelope < 1<<20); the tree only
// covered the 48 KiB happy path in test/cli_e2e. Written by the S1 internal
// review, ctl lane (C5); the two functions below are the delivered names.

// countingReader records how many bytes were pulled from it, so the test can
// prove the limit stops the read instead of slurping an unbounded stdin.
type countingReader struct {
	r    io.Reader
	read int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.read += n
	return n, err
}

func TestExecStdinReadIsBoundedAndOverLimitIsUsageError(t *testing.T) {
	t.Run("at_limit_passes_through_byte_for_byte", func(t *testing.T) {
		body := bytes.Repeat([]byte("q"), execStdinLimit)
		got, err := readExecStdin(bytes.NewReader(body))
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("a body exactly at the limit must pass: err=%v len=%d", err, len(got))
		}
	})
	t.Run("over_limit_is_64_and_stops_reading", func(t *testing.T) {
		src := &countingReader{r: bytes.NewReader(bytes.Repeat([]byte("q"), 4*execStdinLimit))}
		got, err := readExecStdin(src)
		var xe *ExitError
		if !errors.As(err, &xe) || xe.Class != exitUsage || got != nil {
			t.Fatalf("over-limit stdin must be a usage error with no body: err=%v class=%d len=%d", err, classifyExit(err), len(got))
		}
		if !strings.Contains(err.Error(), "--stdin exceeds") {
			t.Fatalf("message does not name the flag: %q", err.Error())
		}
		// LimitReader stops at limit+1; io.ReadAll may over-request but the
		// source can only have handed out what LimitReader let through.
		if src.read > execStdinLimit+1 {
			t.Fatalf("read %d bytes past the limit: the read is not bounded", src.read-execStdinLimit-1)
		}
	})
	t.Run("read_error_is_not_a_usage_error", func(t *testing.T) {
		boom := errors.New("tty went away")
		_, err := readExecStdin(io.MultiReader(strings.NewReader("abc"), errReader{boom}))
		if err == nil || !errors.Is(err, boom) || classifyExit(err) == exitUsage {
			t.Fatalf("a read failure must surface as itself, not as 64: %v", err)
		}
	})
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// TestExecStdinLimitFitsCreateEnvelope pins the arithmetic behind
// execStdinLimit: base64(512 KiB) plus a full-size lease/handle and a
// generous argv/env/cwd still marshal under every 1 MiB bound on the path.
// It also records how much argv headroom is left, because argv is the one
// unbounded input the operator controls — beyond it nats.go refuses the
// publish (ErrMaxPayload, exit 70) before the broker ever sees the body.
func TestExecStdinLimitFitsCreateEnvelope(t *testing.T) {
	maxID := strings.Repeat("z", 32)          // sid/nid are [a-z0-9-]{1,32}
	ulid := strings.Repeat("0", 26)           // pids / lineage / instance ids
	fp := "SHA256:" + strings.Repeat("A", 43) // auth fingerprint shape
	lease := proto.ProcessLease{Version: proto.ProcessResumeVersion, LeaseID: ulid, TargetInstance: ulid,
		BootID: "00000000-0000-0000-0000-000000000000", SID: maxID, NID: maxID, ActorFP: fp, ClientLineage: ulid,
		IssuedBootNS: 1<<63 - 1, ExpiresBootNS: 1<<63 - 1, MAC: strings.Repeat("M", 64)}
	handle := proto.ProcessHandle{Lease: lease, RequestID: ulid}
	argv := []string{"/usr/local/bin/some-long-program-name", strings.Repeat("--flag=value ", 4096)} // ≈52 KiB
	env := map[string]string{}
	for i := range 64 {
		env[strings.Repeat("K", 32)+string(rune('a'+i%26))+string(rune('a'+i/26))] = strings.Repeat("v", 256)
	}
	spec := proto.ExecReq{Argv: argv, Env: env, Cwd: strings.Repeat("/d", 2048), Safe: true, ActorFP: fp,
		Stdin: bytes.Repeat([]byte{0xff}, execStdinLimit)}
	req := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 1<<64 - 1, Op: "create",
		Handle: &handle, AttachEpoch: 1, Exec: &spec, ActorFP: fp}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	const natsDefaultMaxPayload = 1 << 20
	if len(body) > procforward.MaxRequestBytes || len(body) > natsDefaultMaxPayload {
		t.Fatalf("a create at the --stdin limit is %d bytes: over the 1 MiB request bound", len(body))
	}
	// The broker's stamp (a re-encode with the canonical actor_fp in both
	// layers) must not push it over either.
	env2, err := procforward.Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	stamped, err := env2.Stamp(fp)
	if err != nil {
		t.Fatal(err)
	}
	if len(stamped) > procforward.MaxRequestBytes {
		t.Fatalf("stamped create is %d bytes: over the broker's bound", len(stamped))
	}
	headroom := natsDefaultMaxPayload - len(body)
	t.Logf("create at the --stdin limit: %d bytes on the wire (%d stamped); argv/env headroom left ≈ %d KiB", len(body), len(stamped), headroom>>10)
	if headroom < 128<<10 {
		t.Fatalf("only %d bytes of argv headroom at the --stdin limit: the limit is too close to the request bound", headroom)
	}
}
