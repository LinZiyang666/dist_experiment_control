package ssproxy

import (
	"bytes"
	"context"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/sealedstate"
	"golang.org/x/crypto/chacha20poly1305"
)

// Private decode and production SS trial/length/payload/target entries each have
// nonempty seeds. Authenticated lengths 0/0x4000/0xffff must fail at length
// validation, not later EOF. Targets span AEAD chunks and transport fragments.
// The finite transport never owns FDs; decoded private references cannot Take or
// Close arbitrary descriptors. The live entry runs production actor turns
// synchronously so every fragmented read commits its parser cursor.
// origin: hot-update S5 plan §6.1 / §6.2 / §8 T24
func FuzzSSFrame(f *testing.F) {
	golden := codecGolden(f)
	f.Add(uint8(0), golden, uint8(1))
	f.Add(uint8(0), golden[:len(golden)-1], uint8(1))
	f.Add(uint8(0), append(append([]byte(nil), golden...), 0), uint8(1))
	f.Add(uint8(0), []byte("TSSS\x00\x01\x00\x00\xff\xff\xff\xff"), uint8(1))
	actor := codecRecord(codecVector(), "ss-actor")
	duplicate := cloneCodecRecords([]sealedstate.Record{actor})
	duplicate[0].Fields[1] = duplicate[0].Fields[0]
	f.Add(uint8(0), referenceStateBytes(duplicate), uint8(1))
	unknownPhase := cloneCodecRecords([]sealedstate.Record{actor})
	for i := range unknownPhase[0].Fields {
		if unknownPhase[0].Fields[i].Name == "phase" {
			unknownPhase[0].Fields[i].U64 = 255
		}
	}
	f.Add(uint8(0), referenceStateBytes(unknownPhase), uint8(1))
	for i, field := range actor.Fields {
		if field.Name == "rx_nonce" {
			broken := cloneCodecRecords([]sealedstate.Record{actor})
			broken[0].Fields = append(broken[0].Fields[:i], broken[0].Fields[i+1:]...)
			f.Add(uint8(0), referenceStateBytes(broken), uint8(2))
		}
	}
	for _, target := range [][]byte{
		{1, 127, 0, 0, 1, 0, 80},
		{1, 127, 0, 0, 1, 0, 80, 't', 'a', 'i', 'l'},
		{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 187},
		{3, 3, 'a', 'b', 'c', 1, 187}, {3, 255}, {255}, {},
	} {
		f.Add(uint8(2), target, uint8(1))
		f.Add(uint8(1), ssFuzzHandshake(f, target, 1), uint8(1))
	}
	for _, length := range []uint16{0, maxChunk + 1, 0xffff} {
		aead := ssFuzzCipher(f)
		plain := []byte{byte(length >> 8), byte(length)}
		stream := aead.Seal(nil, make([]byte, 12), plain, nil)
		f.Add(uint8(1), append(bytes.Repeat([]byte{0x63}, saltSize), stream...), uint8(3))
	}
	f.Fuzz(func(t *testing.T, mode uint8, input []byte, fragment uint8) {
		// Cap exercise work; production decode has its own stricter 32 MiB bound.
		if len(input) > 64<<10 {
			return
		}
		switch mode % 3 {
		case 0:
			records, err := decodeState(input)
			if err != nil {
				if records != nil {
					t.Fatal("private decoder returned partial state on rejection")
				}
				return
			}
			// Independently repack the accepted input, retaining its original field
			// order. An empty or lossy decode cannot pass by round-tripping itself.
			if !bytes.Equal(input, referenceStateBytes(records)) {
				t.Fatal("private decoder lost fields from nonempty input")
			}
			encoded, err := encodeState(records)
			if err != nil {
				t.Fatal("private decoder accepted unencodable state")
			}
			again, err := decodeState(encoded)
			if err != nil {
				t.Fatal("private codec cannot decode its canonical state")
			}
			// Unsorted input fields are legal, while encoder output is canonical.
			second, err := encodeState(again)
			if err != nil || !bytes.Equal(encoded, second) {
				t.Fatal("private codec canonicalization is unstable")
			}
		case 1:
			exerciseSSTrialParser(t, input, int(fragment%31)+1)
		case 2:
			expected, consumed, valid := ssFuzzTargetOracle(input)
			stream := ssFuzzFrames(t, input, int(fragment%31)+1)
			reader := newAEADReader(&ssFragmentReader{r: bytes.NewReader(stream), limit: int(fragment%31) + 1}, ssFuzzCipher(t))
			got, err := readTargetAddr(reader)
			if valid {
				if err != nil || got != expected {
					t.Fatal("production target parser disagrees with independent address vector")
				}
				suffix, err := io.ReadAll(reader)
				if err != nil || !bytes.Equal(suffix, input[consumed:]) {
					t.Fatal("target parser consumed or lost first-payload suffix")
				}
			} else if err == nil {
				t.Fatal("target parser accepted truncated or unknown ATYP")
			}
		}
	})
}

// The actor sees real ciphertext and active-key trials, with an operation root
// already cancelled to prohibit external DNS/dial side effects. No listener or
// worker is needed: actorTurnLocked commits the same IO and parser decisions as
// runActor, while the finite transport keeps fuzz iterations synchronous.
// origin: hot-update S5 plan §6.1 / §8 T24
func exerciseSSTrialParser(t *testing.T, input []byte, fragment int) {
	t.Helper()
	server, actor := parserFixture(t, []Key{{KeyID: "wrong", Secret: "other-fuzz-key"}, {KeyID: "right", Secret: "state-fuzz-key"}})
	defer server.Stop()
	server.stopCtx, server.stopCancel = context.WithCancel(t.Context())
	server.stopCancel()
	server.DenyPrivateDestinations()
	transport := &ssFuzzConn{ssFragmentReader: ssFragmentReader{r: bytes.NewReader(input), limit: fragment}}
	actor.client = transport
	for turn := 0; ; turn++ {
		// Each turn consumes input or reaches EOF/a cancelled connect. A spin
		// must fail the entry instead of hanging the fuzz worker indefinitely.
		if turn > len(input)+1 {
			t.Fatal("production actor made no progress on finite transport")
		}
		server.mu.Lock()
		err := actorTurnLocked(server, actor)
		done := actor.state.flag("rx_eof")
		server.mu.Unlock()
		if err != nil || done {
			break
		}
	}
	// Independently authenticate the length before deciding how far the production
	// trial parser may read. Successful trials must attempt the first payload;
	// overflow/unknown-key trials must consume no payload bytes.
	if len(input) >= saltSize+2+tagSize {
		master := evpBytesToKey("state-fuzz-key")
		subkey, err := sessionSubkey(master, input[:saltSize])
		if err != nil {
			t.Fatal(err)
		}
		aead, err := chacha20poly1305.New(subkey)
		if err != nil {
			t.Fatal(err)
		}
		plain, err := aead.Open(nil, make([]byte, 12), input[saltSize:saltSize+2+tagSize], nil)
		if err == nil {
			n := int(binary.BigEndian.Uint16(plain))
			read := len(input) - transport.r.Len()
			if n == 0 || n > maxChunk {
				if read != saltSize+2+tagSize {
					t.Fatal("trial parser read payload after invalid authenticated length")
				}
			} else if len(input) > saltSize+2+tagSize && read <= saltSize+2+tagSize {
				t.Fatal("production trial parser did not enter first-payload phase")
			}
		}
	}
	// The same raw encrypted parser validates every length, not just trial's first.
	if len(input) >= saltSize {
		subkey, err := sessionSubkey(evpBytesToKey("state-fuzz-key"), input[:saltSize])
		if err != nil {
			t.Fatal(err)
		}
		aead, err := chacha20poly1305.New(subkey)
		if err != nil {
			t.Fatal(err)
		}
		stream := input[saltSize:]
		reader := newAEADReader(&ssFragmentReader{r: bytes.NewReader(stream), limit: fragment}, aead)
		err = reader.readChunk()
		if len(stream) >= 2+tagSize {
			plain, openErr := aead.Open(nil, make([]byte, 12), stream[:2+tagSize], nil)
			if openErr == nil {
				n := int(binary.BigEndian.Uint16(plain))
				if (n == 0 || n > maxChunk) && !errors.Is(err, errBadChunk) {
					t.Fatal("authenticated length overflow was not rejected at parser boundary")
				}
			}
		}
	}
}

func ssFuzzCipher(t testing.TB) cipher.AEAD {
	t.Helper()
	subkey, err := sessionSubkey(evpBytesToKey("state-fuzz-key"), bytes.Repeat([]byte{0x63}, saltSize))
	if err != nil {
		t.Fatal(err)
	}
	aead, err := chacha20poly1305.New(subkey)
	if err != nil {
		t.Fatal(err)
	}
	return aead
}

// Reference framing uses the cipher directly and explicit nonce arithmetic; the
// production writer and incNonce are intentionally absent from this oracle.
func ssFuzzFrames(t testing.TB, plain []byte, chunk int) []byte {
	t.Helper()
	aead := ssFuzzCipher(t)
	var out []byte
	var counter uint64
	for len(plain) > 0 {
		n := min(len(plain), chunk, maxChunk)
		length := []byte{byte(n >> 8), byte(n)}
		var nonce [12]byte
		binary.LittleEndian.PutUint64(nonce[:8], counter)
		out = aead.Seal(out, nonce[:], length, nil)
		counter++
		binary.LittleEndian.PutUint64(nonce[:8], counter)
		out = aead.Seal(out, nonce[:], plain[:n], nil)
		counter++
		plain = plain[n:]
	}
	return out
}

func ssFuzzHandshake(t testing.TB, target []byte, chunk int) []byte {
	return append(bytes.Repeat([]byte{0x63}, saltSize), ssFuzzFrames(t, target, chunk)...)
}

func ssFuzzTargetOracle(input []byte) (string, int, bool) {
	if len(input) == 0 {
		return "", 0, false
	}
	var host string
	var end int
	switch input[0] {
	case 1:
		end = 5
		if len(input) < end+2 {
			return "", 0, false
		}
		host = net.IP(input[1:end]).String()
	case 4:
		end = 17
		if len(input) < end+2 {
			return "", 0, false
		}
		host = net.IP(input[1:end]).String()
	case 3:
		if len(input) < 2 {
			return "", 0, false
		}
		end = 2 + int(input[1])
		if len(input) < end+2 {
			return "", 0, false
		}
		host = string(input[2:end])
	default:
		return "", 0, false
	}
	port := int(input[end])*256 + int(input[end+1])
	return net.JoinHostPort(host, strconv.Itoa(port)), end + 2, true
}

// Finite fragmented IO reaches io.ReadFull's partial salt/length/payload paths
// without a goroutine, socket, deadline wait or arbitrary descriptor operation.
type ssFragmentReader struct {
	r     *bytes.Reader
	limit int
}

func (r *ssFragmentReader) Read(p []byte) (int, error) { return r.r.Read(p[:min(len(p), r.limit)]) }

type ssFuzzConn struct{ ssFragmentReader }

func (c *ssFuzzConn) Write([]byte) (int, error)        { return 0, io.ErrClosedPipe }
func (c *ssFuzzConn) Close() error                     { return nil }
func (c *ssFuzzConn) CloseWrite() error                { return nil }
func (c *ssFuzzConn) LocalAddr() net.Addr              { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1} }
func (c *ssFuzzConn) RemoteAddr() net.Addr             { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2} }
func (c *ssFuzzConn) SetDeadline(time.Time) error      { return nil }
func (c *ssFuzzConn) SetReadDeadline(time.Time) error  { return nil }
func (c *ssFuzzConn) SetWriteDeadline(time.Time) error { return nil }
