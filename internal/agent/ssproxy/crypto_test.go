package ssproxy

// crypto_test.go — the Shadowsocks AEAD framing in crypto.go, driven by fuzzing.
//
// aeadReader.readChunk is a hand-written binary parser over bytes a REMOTE PEER controls (the
// Shadowsocks client on the far side of the SS proxy). It is exactly the shape fuzzing is for: a
// length field, an allocation sized from it, hand-indexed slices. The 22 JSON fuzzers in
// internal/proto never reached anything like it.
//
// Two targets, two inputs:
//   - FuzzAEADReadChunk feeds arbitrary CIPHERTEXT: the reader must never panic, every error must be
//     one of the known classes, and — the load-bearing half — when the test's own oracle decrypts the
//     first length chunk to a value outside (0, maxChunk], the reader MUST have refused it with
//     errBadChunk (the RIGHT error class, not io.ErrUnexpectedEOF). Removing the `n > maxChunk`
//     check turns that seed into a harmless-looking io.ErrUnexpectedEOF, which is why the oracle
//     exists: "some error" is not the property. What the oracle does NOT prove is "before any
//     allocation" — the first version of this header claimed that; moving the check below the
//     `make` leaves the error class unchanged and this fuzz green. The allocation bound itself is
//     guaranteed by the uint16 length field (≤ 65535+tagSize), not by the check (internal review
//     L2-F9).
//   - FuzzAEADRoundTrip feeds arbitrary PLAINTEXT through aeadWriter and back through aeadReader:
//     byte-identical, including the multi-chunk split the writer performs above maxChunk. Writer and
//     reader are the SAME implementation, so a symmetric framing error (both sides skipping a nonce
//     increment, both reading the length little-endian) would cancel out; the independent oracle is
//     the FRAME ARITHMETIC — stream length == chunks×(2+2·tagSize)+payload and the first length
//     field (decoded by the test's own firstLength, not by the reader) == min(payload, maxChunk).
//     Interoperability with a real Shadowsocks client is outside every oracle here.
//
// origin: docs/reviews/test-system-overhaul-plan.md B2 (infra I7).

import (
	"bytes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"

	"golang.org/x/crypto/chacha20poly1305"
)

func fuzzAEAD(t testing.TB) cipher.AEAD {
	t.Helper()
	aead, err := chacha20poly1305.New(bytes.Repeat([]byte{7}, keySize))
	if err != nil {
		t.Fatal(err)
	}
	return aead
}

// encryptStream is the writer side, used to mint seeds and for the round trip.
func encryptStream(t testing.TB, aead cipher.AEAD, payload []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	w := newAEADWriter(&b, aead)
	if _, err := w.Write(payload); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// sealedLength is a single length chunk (nonce 0) claiming n bytes follow — the test's own oracle
// for "what did the first length field say", independent of readChunk.
func sealedLength(t testing.TB, aead cipher.AEAD, n uint16) []byte {
	t.Helper()
	plain := make([]byte, 2)
	binary.BigEndian.PutUint16(plain, n)
	return aead.Seal(nil, make([]byte, aead.NonceSize()), plain, nil)
}

func firstLength(aead cipher.AEAD, stream []byte) (int, bool) {
	if len(stream) < 2+tagSize {
		return 0, false
	}
	plain, err := aead.Open(nil, make([]byte, aead.NonceSize()), stream[:2+tagSize], nil)
	if err != nil {
		return 0, false
	}
	return int(binary.BigEndian.Uint16(plain)), true
}

func FuzzAEADReadChunk(f *testing.F) {
	aead := fuzzAEAD(f)
	f.Add(encryptStream(f, aead, []byte("hello")))
	f.Add(encryptStream(f, aead, bytes.Repeat([]byte{1}, maxChunk)))   // exactly the cap
	f.Add(encryptStream(f, aead, bytes.Repeat([]byte{1}, maxChunk+1))) // writer splits in two
	f.Add(sealedLength(f, aead, maxChunk+1))                           // length claims 0x4000: errBadChunk
	f.Add(sealedLength(f, aead, 0))                                    // zero length: errBadChunk
	f.Add(sealedLength(f, aead, 5))                                    // valid length, payload missing
	f.Add([]byte{})
	f.Add([]byte{1, 2, 3})
	f.Add(bytes.Repeat([]byte{0}, 2+tagSize))
	f.Fuzz(func(t *testing.T, stream []byte) {
		aead := fuzzAEAD(t)
		r := newAEADReader(bytes.NewReader(stream), aead)
		total := 0
		first := true
		for {
			err := r.readChunk()
			if err == nil {
				if len(r.buf) == 0 || len(r.buf) > maxChunk {
					t.Fatalf("readChunk accepted a %d-byte chunk (cap %d)", len(r.buf), maxChunk)
				}
				total += len(r.buf)
				if total > len(stream) {
					t.Fatalf("decrypted %d bytes out of a %d-byte stream", total, len(stream))
				}
				first = false
				continue
			}
			if first {
				if n, ok := firstLength(aead, stream); ok && (n == 0 || n > maxChunk) {
					if !errors.Is(err, errBadChunk) {
						t.Fatalf("first length chunk says %d (cap %d): want errBadChunk (the length check), got %v", n, maxChunk, err)
					}
				}
			}
			switch {
			case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, errBadChunk):
				return
			case strings.Contains(err.Error(), "message authentication failed"):
				return
			}
			t.Fatalf("readChunk returned an error outside the known classes: %v", err)
		}
	})
}

func FuzzAEADRoundTrip(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("x"))
	f.Add(bytes.Repeat([]byte{0xAB}, maxChunk))
	f.Add(bytes.Repeat([]byte{0xCD}, maxChunk+1))
	f.Add(bytes.Repeat([]byte{0xEF}, 3*maxChunk+17))
	f.Fuzz(func(t *testing.T, payload []byte) {
		aead := fuzzAEAD(t)
		stream := encryptStream(t, aead, payload)
		// Frame arithmetic, independent of the reader (internal review L2-F9).
		chunks := (len(payload) + maxChunk - 1) / maxChunk
		if want := chunks*(2+2*tagSize) + len(payload); len(stream) != want {
			t.Fatalf("framing drifted: %d-byte payload produced a %d-byte stream, want %d (%d chunks)", len(payload), len(stream), want, chunks)
		}
		if chunks > 0 {
			first := len(payload)
			if first > maxChunk {
				first = maxChunk
			}
			if n, ok := firstLength(aead, stream); !ok || n != first {
				t.Fatalf("first length field reads (%d,%v), want %d", n, ok, first)
			}
		}
		r := newAEADReader(bytes.NewReader(stream), fuzzAEAD(t))
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("round trip of %d bytes: %v", len(payload), err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("round trip of %d bytes drifted (got %d bytes)", len(payload), len(got))
		}
	})
}
