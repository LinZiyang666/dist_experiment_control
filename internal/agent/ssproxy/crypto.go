// Package ssproxy is a minimal, self-contained, pure-Go Shadowsocks AEAD
// server with multi-key trial decryption — the P13 embedded proxy.
//
// Design (architecture P13 plan §4): the agent runs ONE Server bound to
// 127.0.0.1, reachable from the public internet ONLY through the broker's
// yamux tunnel (broker:14xxx -> tunnel stream -> agent dials 127.0.0.1).
// Every active subscriber is one Shadowsocks PSK living in the same
// CipherList; a new connection is matched to its subscriber by trial-
// decrypting the first AEAD chunk against each PSK (the classic Outline
// multi-user mechanism, pre-SS2022). This needs ONLY golang.org/x/crypto
// (chacha20poly1305 + HKDF-SHA1 + MD5 key derivation), so the binary stays
// CGO_ENABLED=0 with no blake3 / SS2022 dependency, and the rendered Clash
// node uses the classic cipher string that Clash for Windows accepts.
//
// Only the cipher chacha20-ietf-poly1305 is supported in v1 (locked).
package ssproxy

import (
	"crypto/cipher"
	"crypto/md5"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

// Cipher is the single locked AEAD method string (matches the Clash config
// `cipher:` field and the SQLite proxy_subscribers.cipher column).
const Cipher = "chacha20-ietf-poly1305"

const (
	keySize  = chacha20poly1305.KeySize // 32
	saltSize = 32                       // == keySize for this cipher
	tagSize  = 16                       // poly1305 tag
	maxChunk = 0x3FFF                   // SS AEAD payload-length field cap
)

var (
	errBadChunk = errors.New("ssproxy: bad AEAD chunk length")
	errBadAddr  = errors.New("ssproxy: bad target address")
)

// evpBytesToKey derives the Shadowsocks master key from a password using
// OpenSSL's EVP_BytesToKey with MD5 and no salt (the SS convention).
func evpBytesToKey(password string, keyLen int) []byte {
	var b, prev []byte
	for len(b) < keyLen {
		h := md5.New()
		_, _ = h.Write(prev)
		_, _ = h.Write([]byte(password))
		prev = h.Sum(nil)
		b = append(b, prev...)
	}
	return b[:keyLen]
}

// sessionSubkey derives the per-connection subkey via
// HKDF-SHA1(masterKey, salt, "ss-subkey"), per the SS AEAD spec.
func sessionSubkey(masterKey, salt []byte) ([]byte, error) {
	out := make([]byte, keySize)
	r := hkdf.New(sha1.New, masterKey, salt, []byte("ss-subkey"))
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, err
	}
	return out, nil
}

// incNonce increments a little-endian nonce by one in place.
func incNonce(b []byte) {
	for i := range b {
		b[i]++
		if b[i] != 0 {
			return
		}
	}
}

// aeadReader decrypts a Shadowsocks AEAD stream (length chunk + payload
// chunk framing, monotonic nonce). It is seeded mid-stream by the server
// after the handshake has consumed the salt and first length chunk.
type aeadReader struct {
	r     io.Reader
	aead  cipher.AEAD
	nonce []byte
	buf   []byte // decrypted-but-unread plaintext
	off   int
}

func newAEADReader(r io.Reader, aead cipher.AEAD) *aeadReader {
	return &aeadReader{r: r, aead: aead, nonce: make([]byte, aead.NonceSize())}
}

func (ar *aeadReader) readChunk() error {
	lenBuf := make([]byte, 2+tagSize)
	if _, err := io.ReadFull(ar.r, lenBuf); err != nil {
		return err
	}
	lenPlain, err := ar.aead.Open(lenBuf[:0], ar.nonce, lenBuf, nil)
	if err != nil {
		return err
	}
	incNonce(ar.nonce)
	n := int(binary.BigEndian.Uint16(lenPlain))
	if n == 0 || n > maxChunk {
		return errBadChunk
	}
	payBuf := make([]byte, n+tagSize)
	if _, err := io.ReadFull(ar.r, payBuf); err != nil {
		return err
	}
	plain, err := ar.aead.Open(payBuf[:0], ar.nonce, payBuf, nil)
	if err != nil {
		return err
	}
	incNonce(ar.nonce)
	ar.buf = plain
	ar.off = 0
	return nil
}

func (ar *aeadReader) Read(p []byte) (int, error) {
	if ar.off >= len(ar.buf) {
		if err := ar.readChunk(); err != nil {
			return 0, err
		}
	}
	n := copy(p, ar.buf[ar.off:])
	ar.off += n
	return n, nil
}

// aeadWriter encrypts a Shadowsocks AEAD stream. The caller writes the salt
// to the underlying conn before the first Write.
type aeadWriter struct {
	w     io.Writer
	aead  cipher.AEAD
	nonce []byte
}

func newAEADWriter(w io.Writer, aead cipher.AEAD) *aeadWriter {
	return &aeadWriter{w: w, aead: aead, nonce: make([]byte, aead.NonceSize())}
}

func (aw *aeadWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n := len(p)
		if n > maxChunk {
			n = maxChunk
		}
		lenBuf := make([]byte, 2, 2+tagSize)
		binary.BigEndian.PutUint16(lenBuf, uint16(n))
		lenBuf = aw.aead.Seal(lenBuf[:0], aw.nonce, lenBuf[:2], nil)
		incNonce(aw.nonce)
		if _, err := aw.w.Write(lenBuf); err != nil {
			return total, err
		}
		buf := aw.aead.Seal(nil, aw.nonce, p[:n], nil)
		incNonce(aw.nonce)
		if _, err := aw.w.Write(buf); err != nil {
			return total, err
		}
		total += n
		p = p[n:]
	}
	return total, nil
}
