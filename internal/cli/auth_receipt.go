package cli

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// auth_receipt.go — "this identity has authenticated for this session before".
//
// The ctl's connection-level retry (cmd/tether ctl_connect.go) treats a CONNECT
// failure during a broker or nats-server restart as transient and retries it
// within a small budget — but only for an identity that has ALREADY been
// accepted for that session. Without that gate a wrong PIN or a revoked key
// would spend the whole budget retrying an error that will never clear. The
// receipt is that gate: a file per session under <home>/auth_receipts/ whose
// first line is the identity fingerprint, written after any successful
// activated CONNECT (so every existing home self-heals on its first command
// after the upgrade) and never read for anything else.
// origin: docs/reviews/hot-update-reliable-exec-plan.md §1.5.

// AuthReceiptPath is the receipt file for one session.
func AuthReceiptPath(home, sid string) string {
	return filepath.Join(home, "auth_receipts", sid)
}

// HasAuthReceipt reports whether fingerprint authenticated for sid before. A
// receipt written by another key (the operator rotated keys) does not count:
// the retry budget must not be spent on a key the broker never accepted.
func HasAuthReceipt(home, sid, fingerprint string) bool {
	if home == "" || sid == "" || fingerprint == "" {
		return false
	}
	body, err := os.ReadFile(AuthReceiptPath(home, sid))
	if err != nil {
		return false
	}
	first, _, _ := strings.Cut(string(body), "\n")
	return strings.TrimSpace(first) == fingerprint
}

// WriteAuthReceipt records a successful activated CONNECT. Idempotent; a
// failure is not fatal to the command that just succeeded, so callers may
// ignore the error.
func WriteAuthReceipt(home, sid, fingerprint string) error {
	if home == "" || sid == "" || fingerprint == "" {
		return errors.New("cli: auth receipt needs home, session and fingerprint")
	}
	if HasAuthReceipt(home, sid, fingerprint) {
		return nil
	}
	dir := filepath.Dir(AuthReceiptPath(home, sid))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	// A private temp name: two first commands racing on one home would
	// otherwise truncate each other's fixed `<sid>.tmp` (S1 review C9).
	tmp, err := os.CreateTemp(dir, sid+".*.tmp")
	if err != nil {
		return err
	}
	if _, err := tmp.WriteString(fingerprint + "\n"); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), AuthReceiptPath(home, sid)); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	return nil
}

// SessionFromCtlName is the inverse of CtlNameForSession: the session an
// activated connection name carries, or ok=false for the unactivated name.
func SessionFromCtlName(name string) (sid string, ok bool) {
	rest, found := strings.CutPrefix(name, CtlNameUnactivated+":")
	if !found || rest == "" {
		return "", false
	}
	return rest, true
}

// ConnectRetryEligible binds bounded authentication retry to the matching receipt.
// origin: hot-update S8 internal review FIX-AGENTSLIM.
// connectRetryEligible is the whole eligibility predicate of the bounded
// connect retry, as one pure function so it can be tabled
// (TestConnectRetryEligibility). Three conjuncts, each load-bearing:
//   - not a login verb: login.go passes "login: activation for session <sid>"
//     for the --pin activation (and plain "login" for the pure-auth form),
//     and the S1 first cut compared the verb to the literal "login", so the
//     activation verb slipped through — a wrong --pin from a home that held a
//     receipt for the session (an evicted member, or a session re-created
//     under its old name) retried for the whole budget, and every retry was
//     a real Argon2 refusal that ate the per-IP PIN budget (S1 review C3).
//     A wrong PIN must fail on the first attempt.
//   - an activated name: only a session-bound CONNECT can have a receipt;
//   - a receipt written by THIS key for THIS session on THIS home.
func ConnectRetryEligible(verb, home, sid string, activated bool, fingerprint string) bool {
	if verb == "login" || strings.HasPrefix(verb, "login:") || strings.HasPrefix(verb, "login ") {
		return false
	}
	return activated && HasAuthReceipt(home, sid, fingerprint)
}
