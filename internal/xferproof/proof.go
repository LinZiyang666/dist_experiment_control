// Package xferproof binds transfer messages to the original agent incarnation.
// An ACL proves the node subtree, not which incarnation sent a payload.
// origin: hot-update S7 plan §6.3 / T39
package xferproof

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"

	"github.com/nats-io/nkeys"
)

const KeyHeader = "Tether-Xfer-Agent-Key"
const SignatureHeader = "Tether-Xfer-Agent-Signature"

// InstanceKey is stable across exec of the original agent lineage, using the
// agent's existing signer. The derived seed never leaves memory or the signer;
// a public node key or an instance string alone cannot recreate it.
func InstanceKey(instance string, sign func([]byte) ([]byte, error)) (nkeys.KeyPair, error) {
	if instance == "" || sign == nil {
		return nil, errors.New("transfer instance signer unavailable")
	}
	signature, err := sign([]byte("tether-transfer-instance-v1\x00" + instance))
	if err != nil {
		return nil, err
	}
	seed := sha256.Sum256(signature)
	clear(signature)
	defer clear(seed[:])
	return nkeys.FromRawSeed(nkeys.PrefixByteUser, seed[:])
}

func Binding(callID, sid, nid, instance, receiver string) []byte {
	data, _ := json.Marshal([]string{"tether-transfer-binding-v1", callID, sid, nid, instance, receiver})
	return data
}

func Message(subject, reply string, body []byte) []byte {
	h := sha256.Sum256(body)
	data, _ := json.Marshal([]any{"tether-transfer-message-v1", subject, reply, h})
	return data
}

func Sign(sign func([]byte) ([]byte, error), data []byte) (string, error) {
	sig, err := sign(data)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(sig), nil
}

func Verify(pub, signature string, data []byte) error {
	key, err := nkeys.FromPublicKey(pub)
	if err != nil {
		return err
	}
	sig, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil {
		return err
	}
	return key.Verify(data, sig)
}
