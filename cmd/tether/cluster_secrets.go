package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/nats-io/nkeys"
)

// cluster_secrets.go (B3 item 1) — read REAL public identities so init/sign-join print
// copy-paste-ready next-commands instead of <placeholder> templates.
//
// PRIVATE seeds are NEVER returned/printed — only derived PUBLIC nkeys. Fail-closed: any
// read/parse/KIND error OR a secrets-file-vs-nats.conf mismatch leaves that value "" (the caller
// keeps the <placeholder> + a loud note), never a confidently-wrong substitution.

// clusterPublicIdentities holds the substituted real values; Note is non-empty when any value
// could not be safely derived (printed as a comment so the operator reconciles before running).
type clusterPublicIdentities struct {
	AccountIssuer string // A… (public account key), or "" when unresolved/mismatched
	BrokerNkey    string // U… (public broker key), or "" when unresolved/mismatched
	Note          string // non-empty when any value is unresolved / disagrees with nats.conf

	// IssuerSkew / BrokerSkew are the STRUCTURED skew signals (R11 P6/#54 facet 2): true when the
	// on-disk seed derives a DIFFERENT public key than the one rendered into the live nats.conf
	// auth_callout block — a rotated account.nk/broker.nk whose new identity has NOT been re-rendered
	// into the conf (e.g. the seed was swapped but the broker not yet restarted). They are set ONLY on
	// a genuine DISAGREE (both sides resolvable and different), never on an unreadable seed (that is the
	// separate "unreadable/invalid" note, which the secrets check already reports as FATAL). `cluster
	// doctor` and `reconcile nats` consume these to fail-closed instead of printing a false all-clear.
	IssuerSkew bool
	BrokerSkew bool

	// IssuerUnverified (external review N-3) is true when account.nk was readable and SUBSTITUTED but
	// could NOT be cross-checked against the live nats.conf auth_callout issuer — the conf was unreadable
	// (Preflight error) OR carries no auth_callout issuer yet (a stock pre-cutover conf, which install.sh
	// writes with no authorization{} block). This is the ABSENCE of evidence, not evidence of a skew
	// (IssuerSkew is a positive DISAGREE), so it is surfaced LOUD-but-advisory, never a hard refuse: a
	// refuse would break `cluster init` on a stock conf and the restore fresh-box printer, both of which
	// legitimately hit this state. The security fix is that "no skew detected" is no longer conflated with
	// "skew CHECK ran and passed" — a caller that only reads IssuerSkew used to print a false all-clear.
	IssuerUnverified       bool
	IssuerUnverifiedReason string
}

// readClusterPublicIdentities derives the account-issuer + broker-nkey PUBLIC keys from the secrets
// dir and cross-checks them against the live nats.conf auth_callout identity (the AUTHORITATIVE
// source — takeover takes identity from the conf). A skew (a key rotated without re-rendering the
// conf) would print a confidently-wrong command, so a mismatch refuses to substitute that value.
func readClusterPublicIdentities(secretsDir, confPath string) clusterPublicIdentities {
	var out clusterPublicIdentities
	acct := derivePublicKey(filepath.Join(secretsDir, "account.nk"), nkeys.IsValidPublicAccountKey)
	brk := derivePublicKey(filepath.Join(secretsDir, "broker.nk"), nkeys.IsValidPublicUserKey)

	confIssuer, confBroker := "", ""
	var confErr error
	if own, err := natsconf.Preflight(confPath); err == nil {
		confIssuer, confBroker = own.AuthIdentity()
	} else {
		confErr = err
	}

	var notes []string
	switch {
	case acct == "":
		notes = append(notes, "account.nk unreadable/invalid")
	case confIssuer != "" && confIssuer != acct:
		notes = append(notes, "account.nk disagrees with nats.conf auth_callout issuer")
		out.IssuerSkew = true
	default:
		// account.nk is readable → substitute it. But if we could NOT read an issuer out of the conf
		// (N-3), the skew CHECK did not run — record that loudly instead of silently implying a clean
		// verification. The value is still substituted (it IS this host's own seed, needed by the
		// init/restore printers precisely when no conf exists), just flagged UNVERIFIED.
		out.AccountIssuer = acct
		if confIssuer == "" {
			out.IssuerUnverified = true
			if confErr != nil {
				out.IssuerUnverifiedReason = fmt.Sprintf("nats.conf %s not readable/parsable (%v)", confPath, confErr)
			} else {
				out.IssuerUnverifiedReason = "nats.conf has no auth_callout issuer rendered yet (pre-cutover conf)"
			}
		}
	}
	// NOTE: AuthIdentity() returns confBroker=="" unless the conf's authorization{users} block has
	// exactly ONE user (a single-user install.sh conf). For a grown multi-user conf the cross-check
	// is skipped and broker.nk is substituted unverified — which is correct (it's this broker's own
	// bus key), but means broker-nkey verification only applies to single-user confs.
	switch {
	case brk == "":
		notes = append(notes, "broker.nk unreadable/invalid")
	case confBroker != "" && confBroker != brk:
		notes = append(notes, "broker.nk disagrees with nats.conf auth_callout broker nkey")
		out.BrokerSkew = true
	default:
		out.BrokerNkey = brk
	}
	if len(notes) > 0 {
		out.Note = "# NOTE: " + strings.Join(notes, "; ") +
			" — NOT auto-substituted; reconcile (rotated a key without re-rendering nats.conf?) before running takeover."
	}
	// N-3: the unverified case SUBSTITUTES the value, so it must NOT share the "NOT auto-substituted"
	// wording above — append a separate sentence (a fresh box with no conf hits this on its mainline).
	if out.IssuerUnverified {
		unv := "# NOTE: account-issuer substituted from secrets but UNVERIFIED — " + out.IssuerUnverifiedReason +
			"; skew detection did not run (this is not proof of a match). Verify with `tether cluster doctor` once the conf is rendered."
		if out.Note == "" {
			out.Note = unv
		} else {
			out.Note += "\n     " + unv
		}
	}
	return out
}

// Shared detail text for the two auth_callout identity skews (#54 facet 1/2). A skew means a seed
// on disk (account.nk / broker.nk) was rotated but the live nats.conf auth_callout block has NOT been
// re-rendered to match — the running cluster still authorizes against the OLD identity. The seed swap
// is INERT until the broker restarts and re-renders nats.conf from the new seed (the automatic
// topology reconciler renders from the PROCESS-STARTUP seed, not from re-reading the file), so the
// remedy is always "restart, then confirm" — never "reconcile alone".
const (
	authIssuerSkewDetail = "on-disk account.nk derives a DIFFERENT auth_callout issuer than the one rendered into " +
		"nats.conf: the running cluster still authorizes against the OLD issuer while the on-disk seed is NEW. " +
		"A rotated account.nk is INERT until the broker restarts and re-renders nats.conf from the new seed. " +
		"Restart the broker, then `tether cluster reconcile nats --all --wait` to confirm convergence"
	authBrokerNkeySkewDetail = "on-disk broker.nk derives a DIFFERENT bus nkey than the one rendered into nats.conf " +
		"auth_callout: rotated but not yet re-rendered. Restart the broker to re-render nats.conf from the new seed"
)

// clusterAuthIssuerSkewChecks cross-checks the on-disk account.nk / broker.nk seeds against the
// auth_callout identity in the live nats.conf, returning one FATAL DoctorCheck per skew (#54 facet 2:
// `cluster doctor` used to be blind to this — it validated nats.conf's syntax and the secrets' perms,
// but never that the on-disk issuer STILL MATCHES the rendered one). Empty when there is no skew (or
// when a seed is simply unreadable — that is the separate "secrets" FATAL, not a skew).
func clusterAuthIssuerSkewChecks(secretsDir, confPath string) []clusteroffline.DoctorCheck {
	ids := readClusterPublicIdentities(secretsDir, confPath)
	var out []clusteroffline.DoctorCheck
	if ids.IssuerSkew {
		out = append(out, clusteroffline.DoctorCheck{Name: "auth-issuer-skew", Status: clusteroffline.DoctorFatal, Detail: authIssuerSkewDetail})
	}
	if ids.BrokerSkew {
		out = append(out, clusteroffline.DoctorCheck{Name: "auth-broker-nkey-skew", Status: clusteroffline.DoctorFatal, Detail: authBrokerNkeySkewDetail})
	}
	// N-3: an ADVISORY (not FATAL) when account.nk was substituted but the conf cross-check could not
	// run. FATAL would fail doctor on a stock pre-init box on its mainline path; ADVISORY makes the
	// skipped verification VISIBLE without breaking the fresh-box / init flow. Deliberately asymmetric
	// with clusterAuthIssuerSkewError (which stays skew-only, a hard error): a KNOWN mismatch must
	// fail-closed, an ABSENT check must be loud-but-non-fatal. There is deliberately no broker-nkey
	// "unverified" — a multi-user conf normally yields confBroker=="", so it would be a permanent false alarm.
	if ids.IssuerUnverified {
		out = append(out, clusteroffline.DoctorCheck{Name: "auth-issuer-unverified", Status: clusteroffline.DoctorAdvisory,
			Detail: "auth_callout issuer cross-check SKIPPED: " + ids.IssuerUnverifiedReason + " — skew detection did not run; this is not proof of a match"})
	}
	return out
}

// clusterIssuerUnverifiedReason returns a non-empty reason string when the account issuer was
// substituted UNVERIFIED (N-3) — i.e. the skew CHECK could not run — so `reconcile nats` can WARN that
// its convergence report does not confirm the rendered issuer, instead of a silent false all-clear. It
// is separate from clusterAuthIssuerSkewError (kept skew-only so its callers/tests are unchanged).
func clusterIssuerUnverifiedReason(secretsDir, confPath string) string {
	ids := readClusterPublicIdentities(secretsDir, confPath)
	if ids.IssuerUnverified {
		return ids.IssuerUnverifiedReason
	}
	return ""
}

// clusterAuthIssuerSkewError returns a non-nil error naming every auth_callout identity skew, or nil
// when there is none (#54 facet 1). `reconcile nats --all --wait` used to report "all voters
// converged" purely on topology-generation convergence, blind to a rotated-but-not-re-rendered issuer
// — a false all-clear on the exact command the rotation runbook re-renders with. This makes it
// fail-closed instead.
func clusterAuthIssuerSkewError(secretsDir, confPath string) error {
	ids := readClusterPublicIdentities(secretsDir, confPath)
	var skews []string
	if ids.IssuerSkew {
		skews = append(skews, authIssuerSkewDetail)
	}
	if ids.BrokerSkew {
		skews = append(skews, authBrokerNkeySkewDetail)
	}
	if len(skews) == 0 {
		return nil
	}
	return fmt.Errorf("reconcile nats: auth_callout identity skew — NOT converged: %s", strings.Join(skews, "; "))
}

// derivePublicKey reads a seed file and returns its derived PUBLIC key, or "" on any read/parse
// error or when the derived key fails the KIND check. The private seed bytes are never returned.
func derivePublicKey(seedPath string, validKind func(string) bool) string {
	seed, err := os.ReadFile(seedPath)
	if err != nil {
		return ""
	}
	pub, err := auth.PublicKeyFromSeed([]byte(strings.TrimSpace(string(seed))))
	if err != nil || !validKind(pub) {
		return ""
	}
	return pub
}

// orPlaceholder returns v, or the <placeholder> token when v is empty — so a printed command line
// never silently drops a value (the operator sees exactly which <…> to fill).
func orPlaceholder(v, placeholder string) string {
	if v == "" {
		return placeholder
	}
	return v
}
