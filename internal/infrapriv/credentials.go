package infrapriv

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"sync"

	"github.com/nats-io/nkeys"
)

type CredentialKind uint8

const (
	SYSRead     CredentialKind = 1
	SYSStep     CredentialKind = 2
	LocalInfra  CredentialKind = 3
	LocalLaunch CredentialKind = 4
)

type CredentialDescriptor struct {
	Kind     CredentialKind
	Name     string
	OwnerUID uint32
	Mode     uint32
	Encoding string
	Reader   string
}

// These four and ONLY these four credentials are readable by the new loader.
// Existing account/broker/tunnel seeds are deliberately absent. SYS users remain
// separate even though the nonroot SYS adapter needs both signatures.
// origin: hot-update S9 plan §3.3 / §6.3 / T07
func CredentialDescriptors() []CredentialDescriptor {
	return []CredentialDescriptor{
		{SYSRead, "infra-sys-read.nk", 0, 0640, "nkey-user", "sys-read-adapter"},
		{SYSStep, "infra-sys-step.nk", 0, 0640, "nkey-user", "sys-step-adapter"},
		{LocalInfra, "infra-local.nk", 0, 0640, "nkey-user", "local-infra-authority"},
		{LocalLaunch, "infra-launch.nk", 0, 0640, "nkey-user", "fixed-launcher"},
	}
}

// Credential exposes signatures/public identity, not secret text suitable for
// argv/env/JSON. Close wipes the retained raw seed. Formatting is redacted for
// every fmt verb, including %#v and nested error/struct diagnostics.
type Credential struct {
	secret *credentialSecret
}

type credentialSecret struct {
	mu     sync.Mutex
	seed   []byte
	public []byte
	nkey   string
	closed bool
}

func (Credential) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("infrapriv.Credential[redacted]"))
}

func (k *Credential) Sign(message []byte) ([]byte, error) {
	if k == nil || k.secret == nil {
		return nil, ErrCredential
	}
	s := k.secret
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrCredential
	}
	private := ed25519.NewKeyFromSeed(s.seed)
	defer clear(private)
	return ed25519.Sign(private, message), nil
}

func (k *Credential) PublicKey() []byte {
	if k == nil || k.secret == nil {
		return nil
	}
	s := k.secret
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.public...)
}

func (k *Credential) NKey() string {
	if k == nil || k.secret == nil {
		return ""
	}
	s := k.secret
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nkey
}

func (k *Credential) Close() error {
	if k == nil || k.secret == nil {
		return nil
	}
	s := k.secret
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.seed)
	s.seed = nil
	s.closed = true
	return nil
}

func credentialRow(kind CredentialKind) (CredentialDescriptor, error) {
	for _, row := range CredentialDescriptors() {
		if row.Kind == kind {
			return row, nil
		}
	}
	return CredentialDescriptor{}, ErrCredential
}

func decodeCredential(data []byte) (*Credential, error) {
	defer clear(data)
	prefix, seed, err := nkeys.DecodeSeed(data)
	if err != nil {
		return nil, ErrCredential
	}
	defer clear(seed)
	if prefix != nkeys.PrefixByteUser || len(seed) != ed25519.SeedSize {
		return nil, ErrCredential
	}
	// DecodeSeed accepts some alternate spellings. Only the canonical seed is
	// accepted; a trailing newline or credentials/JWT wrapper is not our format.
	canonical, err := nkeys.EncodeSeed(prefix, seed)
	if err != nil {
		return nil, ErrCredential
	}
	defer clear(canonical)
	if !bytes.Equal(canonical, data) {
		return nil, ErrCredential
	}
	private := ed25519.NewKeyFromSeed(seed)
	defer clear(private)
	public := append([]byte(nil), private[ed25519.SeedSize:]...)
	nkey, err := nkeys.Encode(nkeys.PrefixByteUser, public)
	if err != nil {
		return nil, ErrCredential
	}
	return &Credential{secret: &credentialSecret{seed: append([]byte(nil), seed...), public: public, nkey: string(nkey)}}, nil
}

func generateCredential() ([]byte, error) {
	seed := make([]byte, ed25519.SeedSize)
	defer clear(seed)
	if _, err := rand.Read(seed); err != nil {
		return nil, ErrCredential
	}
	data, err := nkeys.EncodeSeed(nkeys.PrefixByteUser, seed)
	if err != nil {
		return nil, ErrCredential
	}
	return data, nil
}
