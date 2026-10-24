//go:build linux

package hotupdate_test

import (
	"bytes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"go/parser"
	"go/token"
	"io"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/crypto/chacha20poly1305"
)

// A standardized AEAD primitive is shared; all key derivation, wire parsing,
// nonce accounting and ciphertext expectations are independent of ssproxy.
// origin: hot-update S5 plan §6.2 / §8 P19
func ssReferenceSubkey(password string, salt [32]byte) [32]byte {
	first := md5.Sum([]byte(password))
	secondInput := append(append([]byte(nil), first[:]...), []byte(password)...)
	second := md5.Sum(secondInput)
	master := append(append([]byte(nil), first[:]...), second[:]...)
	extract := hmac.New(sha1.New, salt[:])
	_, _ = extract.Write(master)
	prk := extract.Sum(nil)
	var previous, expanded []byte
	for counter := byte(1); len(expanded) < 32; counter++ {
		expand := hmac.New(sha1.New, prk)
		_, _ = expand.Write(previous)
		_, _ = expand.Write([]byte("ss-subkey"))
		_, _ = expand.Write([]byte{counter})
		previous = expand.Sum(nil)
		expanded = append(expanded, previous...)
	}
	var subkey [32]byte
	copy(subkey[:], expanded)
	return subkey
}

type ssSealIdentity struct {
	SubkeyDigest [32]byte
	Nonce        [12]byte
}

// One ledger spans all actors, directions, aliases and generations. A digest
// permits accounting without exporting raw key material into a test receipt.
type ssSealLedger struct {
	mu   sync.Mutex
	seen map[ssSealIdentity]struct{}
}

func (l *ssSealLedger) Record(subkey [32]byte, nonce [12]byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.seen == nil {
		l.seen = make(map[ssSealIdentity]struct{})
	}
	id := ssSealIdentity{SubkeyDigest: sha256.Sum256(subkey[:]), Nonce: nonce}
	if _, duplicate := l.seen[id]; duplicate {
		return errors.New("SS reference: repeated AEAD primitive nonce")
	}
	l.seen[id] = struct{}{}
	return nil
}

type ssReferenceNonce struct {
	Value     [12]byte
	Exhausted bool
}

// The last nonce is usable once; advancing it latches exhaustion instead of
// wrapping. Length and payload each consume a primitive, including failures.
func (n *ssReferenceNonce) Consume() ([12]byte, error) {
	if n.Exhausted {
		return [12]byte{}, errors.New("SS reference: nonce exhausted")
	}
	used := n.Value
	for i := range n.Value {
		n.Value[i]++
		if n.Value[i] != 0 {
			return used, nil
		}
	}
	n.Exhausted = true
	return used, nil
}

func ssReferenceEncode(password string, salt [32]byte, chunks [][]byte, ledger *ssSealLedger) ([]byte, error) {
	if ledger == nil {
		return nil, errors.New("SS reference: missing Seal ledger")
	}
	key := ssReferenceSubkey(password, salt)
	aead, err := chacha20poly1305.New(key[:])
	if err != nil {
		return nil, err
	}
	wire := append([]byte(nil), salt[:]...)
	var nonce ssReferenceNonce
	for _, chunk := range chunks {
		if len(chunk) == 0 || len(chunk) > 0x3fff {
			return nil, errors.New("SS reference: invalid chunk size")
		}
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(len(chunk)))
		for _, plain := range [][]byte{length[:], chunk} {
			value, err := nonce.Consume()
			if err != nil {
				return nil, err
			}
			if err := ledger.Record(key, value); err != nil {
				return nil, err
			}
			wire = aead.Seal(wire, value[:], plain, nil)
		}
	}
	return wire, nil
}

type ssReferenceReader struct {
	Reader   io.Reader
	Password string
	aead     cipher.AEAD
	nonce    ssReferenceNonce
	sticky   error
}

func (r *ssReferenceReader) ReadChunk() ([]byte, error) {
	if r.sticky != nil {
		return nil, r.sticky
	}
	chunk, err := r.readChunk()
	if err != nil {
		r.sticky = err
	}
	return chunk, err
}

func (r *ssReferenceReader) readChunk() ([]byte, error) {
	if r.aead == nil {
		var salt [32]byte
		if _, err := io.ReadFull(r.Reader, salt[:]); err != nil {
			return nil, err
		}
		key := ssReferenceSubkey(r.Password, salt)
		aead, err := chacha20poly1305.New(key[:])
		if err != nil {
			return nil, err
		}
		r.aead = aead
	}
	lengthWire := make([]byte, 2+16)
	if _, err := io.ReadFull(r.Reader, lengthWire); err != nil {
		return nil, err
	}
	nonce, err := r.nonce.Consume()
	if err != nil {
		return nil, err
	}
	plainLength, err := r.aead.Open(nil, nonce[:], lengthWire, nil)
	if err != nil {
		return nil, errors.New("SS reference: length authentication failed")
	}
	length := int(binary.BigEndian.Uint16(plainLength))
	if length == 0 || length > 0x3fff {
		return nil, errors.New("SS reference: invalid authenticated length")
	}
	payloadWire := make([]byte, length+16)
	if _, err := io.ReadFull(r.Reader, payloadWire); err != nil {
		return nil, err
	}
	nonce, err = r.nonce.Consume()
	if err != nil {
		return nil, err
	}
	plain, err := r.aead.Open(nil, nonce[:], payloadWire, nil)
	if err != nil {
		return nil, errors.New("SS reference: payload authentication failed")
	}
	return plain, nil
}

type ssFragmentReader struct {
	Reader io.Reader
	Limit  int
}

func (r ssFragmentReader) Read(p []byte) (int, error) { return r.Reader.Read(p[:min(len(p), r.Limit)]) }

// Generated outside Go with Python hashlib/hmac + cryptography's
// ChaCha20Poly1305, using salt bytes 0..31 and nonce values 0,1,2,3. Keep the
// bytes literal: generating this expected value via either codec self-proves.
const ssReferenceGolden = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1fc587d6cf698f76b07de1e204907246d5bf7942d78a6089607a956db58d8dd531f9cbf312be815b86f3f1d04e51c8d15b7026446d53d2793f694c4d04e7f8dfbf644c8e0fade388667a42b890a0390af55e8f2e8a5bf149a8855676559195514a00bd39399aa40a3858a2e5b7996a33"

// origin: hot-update S5 plan §8 P19 / §6.2
func TestSSReferenceRejectsNonceReuse(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	// This is a source gate: mutation must be on disk. Keeping the parser file
	// free of product imports prevents a "reference" wrapping the tested codec.
	source, err := parser.ParseFile(token.NewFileSet(), "ss_reference_test.go", nil, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range source.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(path, "github.com/LinZiyang666/tether/") && path != "github.com/LinZiyang666/tether/internal/testharness" {
			t.Fatal("independent SS reference imports a product codec")
		}
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSSReference(t)
	}
	testharness.AssertNoGoroutineLeak(t, "independent SS reference", beforeG)
	testharness.AssertNoFDLeak(t, "independent SS reference", beforeFD, 0)
}

func exerciseSSReference(t *testing.T) {
	t.Helper()
	password := "reference-fixture"
	var salt [32]byte
	for i := range salt {
		salt[i] = byte(i)
	}
	chunks := [][]byte{[]byte("first reference chunk"), []byte("second reference chunk")}
	golden, err := hex.DecodeString(ssReferenceGolden)
	if err != nil {
		t.Fatal(err)
	}
	ledger := &ssSealLedger{}
	wire, err := ssReferenceEncode(password, salt, chunks, ledger)
	if err != nil || !bytes.Equal(wire, golden) {
		t.Fatal("independent SS encoder differs from fixed ciphertext golden")
	}
	if len(ledger.seen) != 4 {
		t.Fatal("Seal ledger omitted an actual primitive")
	}
	for _, fragment := range []int{1, 2, 17, 32, len(golden)} {
		r := &ssReferenceReader{Reader: ssFragmentReader{bytes.NewReader(golden), fragment}, Password: password}
		for _, expected := range chunks {
			plain, err := r.ReadChunk()
			if err != nil || !bytes.Equal(plain, expected) {
				t.Fatal("independent SS parser rejected fixed valid ciphertext")
			}
		}
		if _, err := r.ReadChunk(); err != io.EOF {
			t.Fatal("reference did not terminate at exact ciphertext end")
		}
	}
	// A second actor, alias, direction or generation with the same actual
	// material/salt repeats even identical Seal calls and must be rejected.
	if _, err := ssReferenceEncode(password, salt, chunks, ledger); err == nil {
		t.Fatal("Seal ledger accepted nonce reuse across actors/directions/aliases")
	}
	otherSalt := salt
	otherSalt[0] ^= 0xff
	if _, err := ssReferenceEncode(password, otherSalt, chunks, ledger); err != nil {
		t.Fatal("ledger rejected distinct actual subkey")
	}
	if _, err := ssReferenceEncode(password+"-rotated", salt, chunks, ledger); err != nil {
		t.Fatal("ledger rejected rotated actual material")
	}
	key := ssReferenceSubkey(password, salt)
	aead, err := chacha20poly1305.New(key[:])
	if err != nil {
		t.Fatal(err)
	}
	for _, length := range []uint16{0, 0x4000, 0xffff} {
		var plainLength [2]byte
		binary.BigEndian.PutUint16(plainLength[:], length)
		var nonce [12]byte
		invalid := aead.Seal(append([]byte(nil), salt[:]...), nonce[:], plainLength[:], nil)
		r := &ssReferenceReader{Reader: bytes.NewReader(invalid), Password: password}
		if _, err := r.ReadChunk(); err == nil || err.Error() != "SS reference: invalid authenticated length" {
			t.Fatal("reference failed to reject authenticated oversized/zero length before payload allocation")
		}
	}
	maximum := bytes.Repeat([]byte{0x7f}, 0x3fff)
	maximumWire, err := ssReferenceEncode(password, otherSalt, [][]byte{maximum}, &ssSealLedger{})
	if err != nil {
		t.Fatal(err)
	}
	maxReader := &ssReferenceReader{Reader: ssFragmentReader{bytes.NewReader(maximumWire), 17}, Password: password}
	if got, err := maxReader.ReadChunk(); err != nil || !bytes.Equal(got, maximum) {
		t.Fatal("reference rejected maximum legal chunk")
	}
	firstEnd := 32 + 18 + len(chunks[0]) + 16
	repeated := append(append([]byte(nil), golden[:firstEnd]...), golden[32:firstEnd]...)
	flipped := append([]byte(nil), golden...)
	flipped[32+18+3] ^= 1
	for _, bad := range [][]byte{repeated, flipped, golden[:31], golden[:firstEnd-1], append(append([]byte(nil), golden...), 0)} {
		r := &ssReferenceReader{Reader: bytes.NewReader(bad), Password: password}
		var rejected bool
		for i := 0; i < 3; i++ {
			plain, err := r.ReadChunk()
			if err != nil {
				rejected = err != io.EOF || i != len(chunks)
				break
			}
			if i >= len(chunks) || !bytes.Equal(plain, chunks[i]) {
				rejected = true
				break
			}
		}
		if !rejected {
			t.Fatal("reference accepted reset nonce, corruption, truncation or trailing ciphertext")
		}
		if _, err := r.ReadChunk(); err == nil {
			t.Fatal("reference lost sticky parser rejection")
		}
	}
	wrong := &ssReferenceReader{Reader: bytes.NewReader(golden), Password: password + "-wrong"}
	if _, err := wrong.ReadChunk(); err == nil {
		t.Fatal("reference authenticated ciphertext with wrong material")
	}
	var nonce ssReferenceNonce
	for i := range nonce.Value {
		nonce.Value[i] = 0xff
	}
	if _, err := nonce.Consume(); err != nil || !nonce.Exhausted {
		t.Fatal("last nonce was not consumed exactly once")
	}
	if _, err := nonce.Consume(); err == nil {
		t.Fatal("nonce counter wrapped instead of rejecting exhaustion")
	}
	// Endianness becomes observable above byte 0, independently of the golden.
	nonce = ssReferenceNonce{Value: [12]byte{0xff}}
	if _, err := nonce.Consume(); err != nil || nonce.Value != [12]byte{0, 1} {
		t.Fatal("nonce counter is not little endian")
	}
	exerciseSSProductInterop(t)
}
