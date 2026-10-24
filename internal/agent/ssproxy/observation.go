package ssproxy

import (
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/netip"
	"syscall"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/rawsocket"
)

type observedCipher struct {
	cipher.AEAD
	identity drillobserve.Event
}

func (*observedCipher) Format(w fmt.State, _ rune) {
	_, _ = io.WriteString(w, "ssproxy.observedCipher(redacted)")
}

// Inert custody keeps the original primitive and performs no subkey hashing.
// origin: hot-update S5 plan §6.6 / T26 / DRILL-OBSERVER
func observeCipher(a cipher.AEAD, subkey []byte, actor [16]byte) cipher.AEAD {
	if drillobserve.Hook == nil {
		return a
	}
	return &observedCipher{AEAD: a, identity: drillobserve.Event{Kind: "ss-seal", Domain: "ss", Actor: actor, Digest: sha256.Sum256(subkey)}}
}

// Every enabled primitive attempt is queued before Seal, including a later
// failed write. The inactive path stops before even the wrapper type check.
// origin: hot-update S5 plan §6.2 / T26 / DRILL-OBSERVER
func observeSeal(a cipher.AEAD, nonce []byte, size int) {
	if drillobserve.Hook == nil {
		return
	}
	if observed, ok := a.(*observedCipher); ok {
		e := observed.identity
		copy(e.Nonce[:], nonce)
		e.Values[0] = uint64(size)
		drillobserve.Hook(e)
	}
}
func observedActor(a *ssActor, kind string) drillobserve.Event {
	p := a.state
	e := drillobserve.Event{Kind: kind, Domain: "ss", Actor: stateID(p.bytes("id")), Flow: stateID(p.bytes("flow_id")), Digest: sha256.Sum256(p.bytes("authorized_key_id"))}
	e.Values[0] = p.number("phase")
	e.Values[1] = p.number("rx_applied")
	e.Values[2] = p.number("tx_applied")
	e.Values[3] = p.number("tx_cipher_write_cursor")
	e.Values[4] = uint64(len(p.bytes("tx_cipher_pending")))
	e.Values[5] = p.number("revoked")
	e.Values[6] = p.number("cleanup_pending")
	for _, name := range []string{"rx_nonce", "tx_nonce"} {
		b := make([]byte, 12)
		copy(b, p.bytes(name))
		e.Data = append(e.Data, b...)
	}
	for _, sock := range []actorSocket{a.client, a.upstream} {
		fp := observedSocket(sock)
		e.Data = binary.LittleEndian.AppendUint64(e.Data, fp.Dev)
		e.Data = binary.LittleEndian.AppendUint64(e.Data, fp.Ino)
	}
	if tcp, ok := a.clientConn.(*net.TCPConn); ok {
		local, le := netip.ParseAddrPort(tcp.LocalAddr().String())
		remote, re := netip.ParseAddrPort(tcp.RemoteAddr().String())
		if le == nil && re == nil {
			identity := drillobserve.SocketIdentity(local, remote)
			e.Data = append(e.Data, identity[:]...)
		}
	}
	return e
}
func observedSocket(sock actorSocket) rawsocket.Fingerprint {
	if raw, ok := sock.(*rawsocket.Socket); ok {
		return raw.Fingerprint()
	}
	var fp rawsocket.Fingerprint
	if tcp, ok := sock.(*net.TCPConn); ok {
		raw, err := tcp.SyscallConn()
		if err == nil {
			_ = raw.Control(func(fd uintptr) {
				var st syscall.Stat_t
				if syscall.Fstat(int(fd), &st) == nil {
					fp.Dev = uint64(st.Dev)
					fp.Ino = uint64(st.Ino)
					fp.Type = uint32(st.Mode) & syscall.S_IFMT
				}
			})
		}
	}
	return fp
}
func observeGrantLocked(s *Server, g Grant) {
	if drillobserve.Hook != nil {
		e := drillobserve.Event{Kind: "ss-grant", Domain: "ss", Actor: s.imageState.id, Digest: s.imageState.digest}
		e.Values[0] = uint64(g.Generation)
		e.Values[1] = uint64(g.Epoch)
		e.Values[2] = g.AppliedRevision
		if g.Enabled {
			e.Values[3] = 1
		}
		e.Values[4] = uint64(s.imageState.authority)
		drillobserve.Hook(e)
		for _, a := range s.imageState.actors {
			drillobserve.Hook(observedActor(a, "ss-state"))
		}
	}
}

// The saved cursor points to the next nonce except at exhaustion. Identify the
// payload primitive which owns a pending ciphertext across every write retry.
// origin: hot-update S5 plan §6.2 / T18 / DRILL-OBSERVER
func observedPayloadNonce(next []byte, exhausted bool) [12]byte {
	var out [12]byte
	copy(out[:], next)
	if !exhausted {
		for i := range out {
			out[i]--
			if out[i] != 255 {
				break
			}
		}
	}
	return out
}
