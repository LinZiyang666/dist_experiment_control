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
	"net"
	"strconv"

	"github.com/LinZiyang666/tether/internal/drillobserve"
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
	errBadChunk       = errors.New("ssproxy: bad AEAD chunk length")
	errBadAddr        = errors.New("ssproxy: bad target address")
	errNonceExhausted = errors.New("ssproxy: nonce_exhausted")
)

// evpBytesToKey derives the Shadowsocks master key from a password using
// OpenSSL's EVP_BytesToKey with MD5 and no salt (the SS convention).
func evpBytesToKey(password string) []byte {
	const keyLen = keySize
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

// advanceNonce preserves the final usable value and latches exhaustion. A wrap
// would reuse a Poly1305 one-time key, even if the plaintext happened to match.
// origin: hot-update S5 plan §6.2 / T18
func advanceNonce(nonce []byte, exhausted *bool) {
	last := true
	for _, b := range nonce {
		last = last && b == 255
	}
	if last {
		*exhausted = true
		return
	}
	incNonce(nonce)
}

func openChunk(a cipher.AEAD, nonce []byte, exhausted *bool, ciphertext []byte) ([]byte, error) {
	if *exhausted {
		return nil, errNonceExhausted
	}
	plain, err := a.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	advanceNonce(nonce, exhausted)
	return plain, nil
}

func sealChunk(a cipher.AEAD, nonce []byte, exhausted *bool, plain []byte) ([]byte, error) {
	if *exhausted {
		return nil, errNonceExhausted
	}
	// origin: hot-update S5 plan §6.2 / T26. Record even a failed later write.
	if drillobserve.Hook != nil {
		observeSeal(a, nonce, len(plain))
	}
	ciphertext := a.Seal(nil, nonce, plain, nil)
	advanceNonce(nonce, exhausted)
	return ciphertext, nil
}

// aeadReader keeps incomplete frames across interrupted reads. The actor uses
// the same primitives with its cursors in private records; this stream adapter
// also serves legacy clients and the existing protocol fixtures.
type aeadReader struct {
	r             io.Reader
	aead          cipher.AEAD
	nonce         []byte
	exhausted     bool
	buf           []byte
	off           int
	length        [2 + tagSize]byte
	lengthCursor  int
	payload       []byte
	payloadCursor int
}

func newAEADReader(r io.Reader, aead cipher.AEAD) *aeadReader {
	return &aeadReader{r: r, aead: aead, nonce: make([]byte, aead.NonceSize())}
}

func (ar *aeadReader) readChunk() error {
	if ar.payload == nil {
		n, err := io.ReadFull(ar.r, ar.length[ar.lengthCursor:])
		ar.lengthCursor += n
		if err != nil {
			return err
		}
		plain, err := openChunk(ar.aead, ar.nonce, &ar.exhausted, ar.length[:])
		if err != nil {
			return err
		}
		size := int(binary.BigEndian.Uint16(plain))
		if size == 0 || size > maxChunk {
			return errBadChunk
		}
		ar.payload = make([]byte, size+tagSize)
	}
	n, err := io.ReadFull(ar.r, ar.payload[ar.payloadCursor:])
	ar.payloadCursor += n
	if err != nil {
		return err
	}
	plain, err := openChunk(ar.aead, ar.nonce, &ar.exhausted, ar.payload)
	if err != nil {
		return err
	}
	ar.buf, ar.off = plain, 0
	ar.lengthCursor, ar.payloadCursor, ar.payload = 0, 0, nil
	return nil
}

func (ar *aeadReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if ar.off >= len(ar.buf) {
		if err := ar.readChunk(); err != nil {
			return 0, err
		}
	}
	n := copy(p, ar.buf[ar.off:])
	ar.off += n
	return n, nil
}

// aeadWriter owns sealed bytes until the actual writer count consumes them.
// Retrying a short write flushes the exact ciphertext, never another Seal.
// origin: hot-update S5 plan §6.1 / §6.2 / T18
type aeadWriter struct {
	w         io.Writer
	aead      cipher.AEAD
	nonce     []byte
	exhausted bool
	pending   []byte
	cursor    int
}

func newAEADWriter(w io.Writer, aead cipher.AEAD) *aeadWriter {
	return &aeadWriter{w: w, aead: aead, nonce: make([]byte, aead.NonceSize())}
}

func (aw *aeadWriter) flush() error {
	for aw.cursor < len(aw.pending) {
		attempt := aw.pending[aw.cursor:]
		n, err := aw.w.Write(attempt)
		// origin: hot-update S5 plan §6.1 / T26. Preserve the actual n+error.
		if drillobserve.Hook != nil && n >= 0 && n <= len(attempt) {
			e := drillobserve.Event{Kind: "ss-write", Domain: "ss", Values: [16]uint64{uint64(len(attempt)), uint64(n), uint64(len(attempt) - n), 0, 1}, Data: attempt[n:]}
			if err != nil {
				e.Values[3] = 1
			}
			if a, ok := aw.aead.(*observedCipher); ok {
				e.Nonce = observedPayloadNonce(aw.nonce, aw.exhausted)
				e.Values[5] = uint64(aw.cursor)
				e.Values[6] = uint64(len(aw.pending))
				e.Actor = a.identity.Actor
				e.Digest = a.identity.Digest
			}
			drillobserve.Hook(e)
		}
		if n < 0 || n > len(aw.pending)-aw.cursor {
			return io.ErrShortWrite
		}
		aw.cursor += n
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	aw.pending, aw.cursor = nil, 0
	return nil
}

func (aw *aeadWriter) Write(p []byte) (int, error) {
	if err := aw.flush(); err != nil {
		return 0, err
	}
	total := 0
	for len(p) > 0 {
		n := min(len(p), maxChunk)
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(n))
		header, err := sealChunk(aw.aead, aw.nonce, &aw.exhausted, length[:])
		if err != nil {
			return total, err
		}
		body, err := sealChunk(aw.aead, aw.nonce, &aw.exhausted, p[:n])
		if err != nil {
			return total, err
		}
		header = append(header, body...)
		aw.pending = header
		total += n
		p = p[n:]
		if err := aw.flush(); err != nil {
			return total, err
		}
	}
	return total, nil
}

// Target headers can span arbitrary AEAD chunks. Only the header consumes
// parser bytes; the remainder of the completing chunk is target plaintext.
func consumeTarget(p privateRecord, input []byte) ([]byte, error) {
	for len(input) > 0 && p.number("target_parser_phase") != 4 {
		b := input[0]
		input = input[1:]
		p.put("target_cursor", p.number("target_cursor")+1)
		switch p.number("target_parser_phase") {
		case 0:
			p.put("target_atyp", uint64(b))
			switch b {
			case 1:
				p.put("target_host_length", 4)
				p.put("target_parser_phase", 2)
			case 4:
				p.put("target_host_length", 16)
				p.put("target_parser_phase", 2)
			case 3:
				p.put("target_parser_phase", 1)
			default:
				return nil, errBadAddr
			}
		case 1:
			if b == 0 {
				return nil, errBadAddr
			}
			p.put("target_host_length", uint64(b))
			p.put("target_parser_phase", 2)
		case 2:
			p.putBytes("target_host", append(p.bytes("target_host"), b))
			p.put("target_host_cursor", p.number("target_host_cursor")+1)
			if p.number("target_atyp") != 3 {
				p.putBytes("target_address", p.bytes("target_host"))
				p.put("target_address_cursor", p.number("target_host_cursor"))
			}
			if p.number("target_host_cursor") == p.number("target_host_length") {
				p.put("target_parser_phase", 3)
			}
		case 3:
			p.putBytes("target_port", append(p.bytes("target_port"), b))
			p.put("target_port_cursor", p.number("target_port_cursor")+1)
			if p.number("target_port_cursor") == 2 {
				if binary.BigEndian.Uint16(p.bytes("target_port")) == 0 {
					return nil, errBadAddr
				}
				p.put("target_parser_phase", 4)
			}
		default:
			return nil, errBadAddr
		}
	}
	return input, nil
}

func actorTarget(p privateRecord) (string, string) {
	host := string(p.bytes("target_host"))
	if p.number("target_atyp") != 3 {
		host = net.IP(p.bytes("target_host")).String()
	}
	return host, strconv.Itoa(int(binary.BigEndian.Uint16(p.bytes("target_port"))))
}

func sealActorPending(a *ssActor) error {
	p := a.state
	plain := p.bytes("tx_plain_pending")
	if len(plain) == 0 || len(plain) > maxChunk {
		return errBadChunk
	}
	exhausted := p.flag("tx_nonce_exhausted")
	if !p.flag("tx_length_sealed") {
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(len(plain)))
		b, err := sealChunk(a.tx, p.bytes("tx_nonce"), &exhausted, length[:])
		if exhausted {
			p.put("tx_nonce_exhausted", 1)
		}
		if err != nil {
			return err
		}
		p.putBytes("tx_length_cipher", b)
		p.put("tx_length_sealed", 1)
	}
	if !p.flag("tx_payload_sealed") {
		b, err := sealChunk(a.tx, p.bytes("tx_nonce"), &exhausted, plain)
		if exhausted {
			p.put("tx_nonce_exhausted", 1)
		}
		if err != nil {
			return err
		}
		p.putBytes("tx_payload_cipher", b)
		p.put("tx_payload_sealed", 1)
	}
	p.putBytes("tx_cipher_pending", append(append([]byte(nil), p.bytes("tx_length_cipher")...), p.bytes("tx_payload_cipher")...))
	return nil
}

// consumeActorFrameLocked is also the pure parser entry for byte-cut/fuzz
// fixtures: no FD, DNS, logging, or target write can happen here.
func consumeActorFrameLocked(s *Server, a *ssActor) error {
	p := a.state
	if p.number("rx_salt_cursor") < saltSize {
		return nil
	}
	if !p.flag("rx_length_opened") {
		if p.number("rx_length_cursor") < 2+tagSize {
			p.put("phase", uint64(SSPhaseLength))
			return nil
		}
		var plain []byte
		if a.rx == nil {
			for _, k := range s.keys {
				sk, err := sessionSubkey(k.master, p.bytes("rx_salt"))
				if err != nil {
					return err
				}
				ae, err := chacha20poly1305.New(sk)
				if err != nil {
					return err
				}
				candidate, err := ae.Open(nil, p.bytes("rx_nonce"), p.bytes("rx_length_cipher"), nil)
				if err != nil {
					continue
				}
				if !bindTrialLocked(s, a, k) {
					return errors.New("ssproxy: authentication rejected")
				}
				a.rx = ae
				p.putBytes("rx_subkey", sk)
				plain = candidate
				exhausted := p.flag("rx_nonce_exhausted")
				advanceNonce(p.bytes("rx_nonce"), &exhausted)
				if exhausted {
					p.put("rx_nonce_exhausted", 1)
				}
				break
			}
			if a.rx == nil {
				return errors.New("ssproxy: authentication rejected")
			}
		} else {
			var err error
			exhausted := p.flag("rx_nonce_exhausted")
			plain, err = openChunk(a.rx, p.bytes("rx_nonce"), &exhausted, p.bytes("rx_length_cipher"))
			if err != nil {
				return err
			}
			if exhausted {
				p.put("rx_nonce_exhausted", 1)
			}
		}
		size := binary.BigEndian.Uint16(plain)
		if size == 0 || size > maxChunk {
			return errBadChunk
		}
		p.put("rx_decoded_length", uint64(size))
		p.put("rx_length_opened", 1)
		p.putBytes("rx_payload_cipher", make([]byte, int(size)+tagSize))
		p.put("phase", uint64(SSPhasePayload))
		return nil
	}
	if p.number("rx_payload_cursor") != uint64(len(p.bytes("rx_payload_cipher"))) {
		return nil
	}
	exhausted := p.flag("rx_nonce_exhausted")
	plain, err := openChunk(a.rx, p.bytes("rx_nonce"), &exhausted, p.bytes("rx_payload_cipher"))
	if err != nil {
		return err
	}
	if exhausted {
		p.put("rx_nonce_exhausted", 1)
	}
	p.put("rx_payload_opened", 1)
	if p.number("target_parser_phase") != 4 {
		p.put("phase", uint64(SSPhaseTarget))
		plain, err = consumeTarget(p, plain)
		if err != nil {
			return err
		}
		if p.number("target_parser_phase") == 4 {
			p.putBytes("first_payload_suffix", plain)
			p.put("phase", uint64(SSPhaseConnecting))
		}
	} else {
		p.put("phase", uint64(SSPhaseRelay))
	}
	p.putBytes("rx_clear_pending", plain)
	p.put("rx_clear_cursor", 0)
	p.put("rx_length_cursor", 0)
	p.put("rx_payload_cursor", 0)
	p.put("rx_length_opened", 0)
	p.put("rx_payload_opened", 0)
	p.putBytes("rx_payload_cipher", nil)
	return nil
}
