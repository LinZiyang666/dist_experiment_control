// Package cli holds helpers shared by the cobra subcommands in
// cmd/tether/. Pulled out so we don't duplicate logic across serve / agent
// / session / login.
package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/LinZiyang666/tether/internal/auth"
)

// Identity bundles a loaded user nkey with derived public-key + fingerprint.
type Identity struct {
	Seed        []byte // base32 "S..." form (the bytes nkeys.FromSeed accepts)
	PublicKey   string // base32 "U..." actor token
	Fingerprint string // "SHA256:<base64-no-pad>" identity per requirements §3
}

// EnsureIdentity loads the default user nkey from `<home>/keys/default.nk`,
// generating + persisting it if absent. Mode 0700 directory / 0600 file.
//
// One identity per CLI is sufficient for v1: identity is the user (a human),
// not the session. Joining additional sessions reuses the same nkey.
func EnsureIdentity(home string) (*Identity, error) {
	keyPath := filepath.Join(home, "keys", "default.nk")
	seed, err := os.ReadFile(keyPath)
	if errors.Is(err, os.ErrNotExist) {
		if seed, err = generateAndPersist(keyPath); err != nil {
			return nil, fmt.Errorf("identity: generate: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("identity: read: %w", err)
	}

	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		return nil, fmt.Errorf("identity: derive pub: %w", err)
	}
	fp, err := auth.FingerprintFromActor(pub)
	if err != nil {
		return nil, fmt.Errorf("identity: derive fp: %w", err)
	}
	return &Identity{Seed: seed, PublicKey: pub, Fingerprint: fp}, nil
}

// EnsureAgentIdentity loads (or generates + persists) the per-session agent
// nkey at `<home>/agent/<sid>/keys/agent.nk`. Path layout per architecture
// K.1; one nkey per `(machine, sid)` because a single host can run multiple
// `tether agent --session <sid>` instances side-by-side.
//
// Caller (cmd/tether/agent.go) presents this nkey on CONNECT plus a
// `tether-agent:<sid>:<nid>` Name; the broker auth_callout binds the nkey
// to (sid, nid) on first PIN-bootstrap and re-validates thereafter.
func EnsureAgentIdentity(home, sid string) (*Identity, error) {
	keyPath := filepath.Join(home, "agent", sid, "keys", "agent.nk")
	seed, err := os.ReadFile(keyPath)
	if errors.Is(err, os.ErrNotExist) {
		if seed, err = generateAndPersist(keyPath); err != nil {
			return nil, fmt.Errorf("agent identity: generate: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("agent identity: read: %w", err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		return nil, fmt.Errorf("agent identity: derive pub: %w", err)
	}
	fp, err := auth.FingerprintFromActor(pub)
	if err != nil {
		return nil, fmt.Errorf("agent identity: derive fp: %w", err)
	}
	return &Identity{Seed: seed, PublicKey: pub, Fingerprint: fp}, nil
}

func generateAndPersist(keyPath string) ([]byte, error) {
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(keyPath), 0o700); err != nil {
		return nil, err
	}
	// Atomic-ish write: tmp + fsync + rename.
	tmp := keyPath + ".tmp"
	if err := os.WriteFile(tmp, seed, 0o600); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, keyPath); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	return seed, nil
}

// DefaultHome returns ~/.tether unless $TETHER_HOME overrides.
func DefaultHome() string {
	if h := os.Getenv("TETHER_HOME"); h != "" {
		return h
	}
	h, _ := os.UserHomeDir()
	return filepath.Join(h, ".tether")
}

// CurrentSessionFile is the persistent store for the active session
// (lower-priority than $TETHER_SESSION env).
func CurrentSessionFile(home string) string {
	return filepath.Join(home, "current_session")
}

// ReadCurrentSession honors $TETHER_SESSION first, then falls back to the
// file. Empty string means no session active.
func ReadCurrentSession(home string) string {
	if v := os.Getenv("TETHER_SESSION"); v != "" {
		return v
	}
	b, err := os.ReadFile(CurrentSessionFile(home))
	if err != nil {
		return ""
	}
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return string(b)
}

// WriteCurrentSession persists the active session to disk (always writes,
// regardless of the env var). Empty sid clears the file.
func WriteCurrentSession(home, sid string) error {
	path := CurrentSessionFile(home)
	if sid == "" {
		err := os.Remove(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	if err := os.MkdirAll(home, 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(sid+"\n"), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// DefaultBrokerURLEnv lets the operator set the broker URL once per
// shell instead of repeating --nats-url on every command.
const DefaultBrokerURLEnv = "TETHER_NATS_URL"

// BrokerURLFile is where `tether login --broker <URL>` (or any future
// `tether ctx --broker <URL>` wrapper) persists the broker URL the
// rest of the CLI should default to. Symmetric with current_session.
func BrokerURLFile(home string) string {
	return filepath.Join(home, "broker_url")
}

// ReadDefaultBrokerURL returns the broker URL the CLI should default to
// when --nats-url is not explicitly set. Precedence (high → low):
//  1. $TETHER_NATS_URL
//  2. ~/.tether/broker_url file
//  3. "" (caller falls back to the cobra default, typically nats://127.0.0.1:4222)
func ReadDefaultBrokerURL(home string) string {
	if v := os.Getenv(DefaultBrokerURLEnv); v != "" {
		return v
	}
	b, err := os.ReadFile(BrokerURLFile(home))
	if err != nil {
		return ""
	}
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return string(b)
}

// WriteDefaultBrokerURL persists the broker URL. Empty url clears.
func WriteDefaultBrokerURL(home, url string) error {
	path := BrokerURLFile(home)
	if url == "" {
		err := os.Remove(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	if err := os.MkdirAll(home, 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(url+"\n"), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
