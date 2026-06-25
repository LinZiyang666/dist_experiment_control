package main

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/LinZiyang666/tether/internal/auth"
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
	if own, err := natsconf.Preflight(confPath); err == nil {
		confIssuer, confBroker = own.AuthIdentity()
	}

	var notes []string
	switch {
	case acct == "":
		notes = append(notes, "account.nk unreadable/invalid")
	case confIssuer != "" && confIssuer != acct:
		notes = append(notes, "account.nk disagrees with nats.conf auth_callout issuer")
	default:
		out.AccountIssuer = acct
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
	default:
		out.BrokerNkey = brk
	}
	if len(notes) > 0 {
		out.Note = "# NOTE: " + strings.Join(notes, "; ") +
			" — NOT auto-substituted; reconcile (rotated a key without re-rendering nats.conf?) before running takeover."
	}
	return out
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
