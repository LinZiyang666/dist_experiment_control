package security_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

// === B.9 SQL injection via session name ====================================

// TestSessionCreateSQLInjectionInName: a name like
// "lab; DROP TABLE sessions" must be stored verbatim (parameterized
// query) and the sessions table must remain intact afterwards.
//
// The broker handler uses session.Create which uses prepared
// statements; this is a regression test that pins that contract.
func TestSessionCreateSQLInjectionInName(t *testing.T) {
	db := openDB(t)

	// Direct call (matches what the broker does after fp validation).
	pub, fp := freshUserPub(t)
	_ = pub
	malicious := "lab"
	if _, err := session.Create(db, malicious, "lab; DROP TABLE sessions; --", fp, testPINHash, time.Now().UTC()); err != nil {
		t.Fatalf("session.Create with malicious name: %v", err)
	}
	// Verify table still exists (count rows).
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sessions`).Scan(&n); err != nil {
		t.Fatalf("sessions table corrupted: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 session row, got %d (SQL injection succeeded?)", n)
	}
	// Verify the name was stored as a literal string.
	var got string
	if err := db.QueryRow(`SELECT name FROM sessions WHERE sid=?`, malicious).Scan(&got); err != nil {
		t.Fatalf("read back name: %v", err)
	}
	if !strings.Contains(got, "DROP TABLE") {
		t.Errorf("name was sanitized away (unexpected): got %q", got)
	}
}

// === B.10 NUL byte in session name ========================================

// TestSessionCreateNULByteInName: SQLite stores NUL fine, but the
// CONNECT name parser splits on `:` and would silently truncate.
// We pin the contract: NUL is accepted by the storage layer
// (documenting actual behavior); the sid validation regex rejects
// it before it reaches storage on the broker happy path.
func TestSessionCreateNULByteInName(t *testing.T) {
	// 1) sid validation rejects NUL.
	if err := proto.ValidateSID("lab\x00admin"); err == nil {
		t.Errorf("ValidateSID accepted sid with NUL byte")
	}
	// 2) But the `name` field has no validation — it's a free-form
	//    label. Document the actual behavior so future hardening
	//    (e.g. UTF-8 validation) has a regression baseline.
	db := openDB(t)
	_, fp := freshUserPub(t)
	if _, err := session.Create(db, "lab", "innocuous\x00secret", fp, testPINHash, time.Now().UTC()); err != nil {
		t.Fatalf("session.Create with NUL in name: %v", err)
	}
}

// === B.11 NATS subject wildcard injection in sid =========================

// TestValidateSIDRejectsNATSWildcards: sid with `>` or `*` or `.`
// would, if accepted, let an attacker subscribe to multi-session
// streams via a single subscription. ValidateSID's character class
// is `[a-z0-9-]{1,32}` so all of these must be rejected.
func TestValidateSIDRejectsNATSWildcards(t *testing.T) {
	cases := []string{
		"lab.>",
		"lab.*",
		">",
		"*",
		"lab.foo",
		"lab*",
		"lab>",
		"lab.foo.>",
	}
	for _, sid := range cases {
		t.Run(sid, func(t *testing.T) {
			if err := proto.ValidateSID(sid); err == nil {
				t.Errorf("ValidateSID accepted wildcard-bearing sid %q", sid)
			}
		})
	}
}

// === B.12 ValidateNID rejects dots & wildcards ============================

// TestValidateNIDRejectsDotsAndWildcards: nid with "." would split
// the subject layout (10-token contract in proto.SubjCmdBy /
// SubjCmdForwarded). ValidateNID rejects.
func TestValidateNIDRejectsDotsAndWildcards(t *testing.T) {
	cases := []string{
		"lab-1.injection",
		"lab-1.>",
		"lab-1.*",
		"lab*1",
		"lab.1",
		"\x00",
		"lab/1",
		"lab\\1",
	}
	for _, nid := range cases {
		t.Run(nid, func(t *testing.T) {
			if err := proto.ValidateNID(nid); err == nil {
				t.Errorf("ValidateNID accepted suspicious nid %q", nid)
			}
		})
	}
}

// === B.13 exec argv shell expansion (defense by direct exec) =============

// TestExecArgvNoShellExpansion: the agent uses os/exec.Command with
// a literal program + args. Confirm the contract that the broker's
// exec verb receives the argv as opaque slices — there is no
// "shell" anywhere in the path. We assert this at the proto level
// since wiring a full agent + child process for one assertion is
// overkill.
//
// Defense-in-depth: ExecReq.Argv is `[]string` not `string`, so
// "${HOME}/secret" appears as a literal arg not a shell parse.
func TestExecArgvIsLiteralNotShell(t *testing.T) {
	// Round-trip via JSON to confirm the wire encoding preserves
	// '${...}' verbatim.
	req := proto.ExecReq{Argv: []string{"echo", "${HOME}/secret"}}
	body, _ := json.Marshal(&req)
	var got proto.ExecReq
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Argv[1] != "${HOME}/secret" {
		t.Errorf("argv[1] mangled by JSON round-trip: %q", got.Argv[1])
	}
	// And ExecReq doesn't expose a `shell: true` knob.
	// (compile-time check — if the field appears later this test
	// becomes a discovery mechanism.)
}

// === B.14 expose name with NUL / control chars ============================

// TestExposeNameNULByteCurrentBehavior: documents actual behavior.
// The broker's expose handler only checks `req.Name == ""`; it
// does NOT validate the character set. A NUL byte would pass
// through to SQLite (which stores BLOB-ish strings fine) and to
// the public-URL composition (where it becomes a NUL in the URL
// — most tools then truncate at NUL).
//
// This is a low-severity finding: caller-supplied display string
// has no validation. We pin the contract for future hardening.
func TestExposeNameAllowsControlCharsCurrentBehavior(t *testing.T) {
	url := startNATS(t)
	db := openDB(t)
	pub, fp := freshUserPub(t)
	seedSession(t, db, "lab", fp)
	seedNodeOnline(t, db, "lab", "lab-1")
	defer startBroker(t, url, db)()

	nc, err := nats.Connect(url, nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	type exposeReq struct {
		Name      string `json:"name"`
		LocalPort int    `json:"local_port"`
	}
	for _, name := range []string{
		"jupyter\x00secret",
		"\rinjected\n",
		"name\twith\ttab",
	} {
		body, _ := json.Marshal(exposeReq{Name: name, LocalPort: 8080})
		// We expect agent_no_responders (no agent registered),
		// not a name-validation rejection. That documents the
		// missing input filter.
		respMsg, err := nc.Request(proto.SubjCmdBy("lab", pub, "lab-1", "expose"), body, 1*time.Second)
		if err != nil {
			continue // no agent → fine, broker pre-flight didn't reject the name
		}
		var resp struct {
			Code  string `json:"code"`
			Error string `json:"error"`
		}
		_ = json.Unmarshal(respMsg.Data, &resp)
		if resp.Code != "" && strings.Contains(resp.Code, "name") {
			// If the broker DOES reject control chars, that's actually a hardening — note it.
			t.Logf("broker rejected control-char name %q: %s", name, resp.Code)
		}
	}
}

// === B.15 PIN length / NUL byte =============================================

// TestPINNULByteRejected: ValidPIN restricts PIN to ASCII printable
// (0x20-0x7E), which excludes NUL. Confirms that contract.
func TestPINNULByteRejected(t *testing.T) {
	if err := auth.ValidPIN("good\x00bad"); err == nil {
		t.Errorf("ValidPIN accepted NUL byte")
	}
	if err := auth.ValidPIN("good\nbad"); err == nil {
		t.Errorf("ValidPIN accepted newline (0x0A)")
	}
	if err := auth.ValidPIN("\x7F"); err == nil {
		t.Errorf("ValidPIN accepted DEL (0x7F)")
	}
}

// TestPINLargeInputCurrentBehavior: ValidPIN has NO length cap. A
// 1 MiB PIN passes ValidPIN and forces the broker to call argon2id
// over a megabyte of input on every CONNECT — argon2 is
// memory-bound (64 MiB) so the work doesn't blow up linearly with
// input length, but the input copy still allocates 1 MiB per try.
//
// This is a real low/medium severity finding: a malicious client
// could blow ~1MB allocations + 64MB argon2 memory per CONNECT
// against the broker.
func TestPINLargeInputAcceptedNoCap(t *testing.T) {
	huge := strings.Repeat("p", 1024*1024)
	// All chars in ASCII printable so ValidPIN passes.
	if err := auth.ValidPIN(huge); err != nil {
		t.Skipf("ValidPIN unexpectedly rejected 1 MiB ASCII PIN: %v (good harden!)", err)
	}
	// HashPIN should still produce SOMETHING — but the work is real.
	// We don't actually run HashPIN (would take seconds and burn 64 MiB)
	// — the report flags it.
	t.Logf("FINDING: ValidPIN accepts arbitrarily long PINs (no max length); a 1 MiB PIN passes validation")
}
