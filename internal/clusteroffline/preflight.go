package clusteroffline

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// preflight.go — the D9 §15 secrets/FDE preflight (the `cluster doctor` gate). It is
// SPLIT (d9-plan §5 OQ-4): a secrets file that is MISSING / UNREADABLE / world-or-group
// readable (a private key) is a HARD FATAL refusal (a broker that silently fell back to an
// ephemeral cert would un-pin every agent); an UN-encrypted volume is only an ADVISORY
// (psk_at_rest_unprotected) — security-pragmatic under the single-home, all-trusted-team
// threat model (proxy/psk are out of v1 HA, so the at-rest justification is weak).

type requiredSecret struct {
	name      string
	isPrivate bool
}

// SecretsPreflightOptions tunes which optional deployment groups are checked.
type SecretsPreflightOptions struct {
	// RequireAuthSeeds checks broker.nk/account.nk in secretsDir. Operator-facing
	// doctor/init use true. Daemon startup can use false when an explicit
	// --auth-callout-seeds-dir was already loaded successfully from another dir.
	RequireAuthSeeds bool
}

// requiredSecrets are the §15 files a cluster-mode broker must have. The keys (private
// material) must be 0600; the certs/CA may be group/world-readable.
var requiredSecrets = []requiredSecret{
	{"cluster-ca.pem", false},
	{"route-cert.pem", false},
	{"route-key.pem", true},
	{"tunnel-cert.pem", false},
	{"tunnel-key.pem", true},
	{"node-ident.nk", true},
}

var requiredAuthSeeds = []requiredSecret{
	{"broker.nk", true},
	{"account.nk", true},
}

// SecretsPreflight checks the §15 secrets dir. It returns a list of non-fatal ADVISORIES
// (e.g. FDE not confirmed → the caller surfaces psk_at_rest_unprotected) and a FATAL error
// (last, per ST1008) if any required file is missing/unreadable or any private key is
// too-permissive. nil error means the secrets are usable; advisories may still be present.
func SecretsPreflight(secretsDir string) (advisories []string, fatal error) {
	return SecretsPreflightWithOptions(secretsDir, SecretsPreflightOptions{RequireAuthSeeds: true})
}

// SecretsPreflightWithOptions checks the §15 secrets dir with an explicit auth-seed
// requirement. It is otherwise identical to SecretsPreflight.
func SecretsPreflightWithOptions(secretsDir string, opts SecretsPreflightOptions) (advisories []string, fatal error) {
	if secretsDir == "" {
		return nil, errors.New("clusteroffline: secrets_dir is empty (cluster mode requires §15 secrets)")
	}
	required := append([]requiredSecret{}, requiredSecrets...)
	if opts.RequireAuthSeeds {
		required = append(required, requiredAuthSeeds...)
	}
	var missing, unreadable, tooOpen, nonRegular []string
	for _, s := range required {
		p := filepath.Join(secretsDir, s.name)
		info, err := os.Lstat(p)
		if err != nil {
			missing = append(missing, s.name)
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			nonRegular = append(nonRegular, s.name)
			continue
		}
		// Readability: actually attempt to open (a 0600 file owned by another user is
		// present but unreadable — Stat alone wouldn't catch it).
		f, oerr := os.Open(p)
		if oerr != nil {
			unreadable = append(unreadable, s.name)
		} else {
			_ = f.Close()
		}
		// Private keys must not be group/world readable (0077 bits clear).
		if s.isPrivate && info.Mode().Perm()&0o077 != 0 {
			tooOpen = append(tooOpen, fmt.Sprintf("%s (mode %04o)", s.name, info.Mode().Perm()))
		}
	}
	var problems []string
	if len(missing) > 0 {
		sort.Strings(missing)
		problems = append(problems, "missing: "+strings.Join(missing, ", "))
	}
	if len(unreadable) > 0 {
		sort.Strings(unreadable)
		problems = append(problems, "unreadable: "+strings.Join(unreadable, ", "))
	}
	if len(nonRegular) > 0 {
		sort.Strings(nonRegular)
		problems = append(problems, "not regular file(s): "+strings.Join(nonRegular, ", "))
	}
	if len(tooOpen) > 0 {
		sort.Strings(tooOpen)
		problems = append(problems, "too-permissive private key(s): "+strings.Join(tooOpen, ", "))
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("clusteroffline: secrets preflight FAILED in %q — %s", secretsDir, strings.Join(problems, "; "))
	}
	// FDE is an ADVISORY only (security-pragmatic): we cannot portably confirm the volume
	// is LUKS/dm-crypt/FileVault-encrypted from a static binary, so we always advise the
	// operator to ensure full-disk encryption — never refuse on it.
	advisories = append(advisories, "psk_at_rest_unprotected: confirm the secrets volume is "+
		"full-disk-encrypted (LUKS/dm-crypt/FileVault); the shared psk + account key live here in plaintext")
	return advisories, nil
}
