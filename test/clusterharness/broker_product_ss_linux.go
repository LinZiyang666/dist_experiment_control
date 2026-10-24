//go:build linux

package clusterharness

import (
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"

	"golang.org/x/crypto/chacha20poly1305"
)

// The external SS peer uses only standardized crypto primitives. Its framing,
// subkey derivation and nonces do not call the product encoder or decoder.
// origin: hot-update S6 plan §6 / T28 / hot-update S5 plan §6.2
func productSSCipher(password string, salt []byte) (cipher.AEAD, error) {
	first := md5.Sum([]byte(password))
	second := md5.Sum(append(append([]byte(nil), first[:]...), []byte(password)...))
	extract := hmac.New(sha1.New, salt)
	_, _ = extract.Write(append(first[:], second[:]...))
	prk := extract.Sum(nil)
	var previous, key []byte
	for counter := byte(1); len(key) < 32; counter++ {
		expand := hmac.New(sha1.New, prk)
		_, _ = expand.Write(previous)
		_, _ = expand.Write([]byte("ss-subkey"))
		_, _ = expand.Write([]byte{counter})
		previous = expand.Sum(nil)
		key = append(key, previous...)
	}
	return chacha20poly1305.New(key[:32])
}
func productSSAdvance(nonce []byte) {
	for i := range nonce {
		nonce[i]++
		if nonce[i] != 0 {
			return
		}
	}
}

type productSSPeer struct {
	conn                    net.Conn
	password                string
	send, receive           cipher.AEAD
	sendNonce, receiveNonce [12]byte
}

func newProductSSPeer(conn net.Conn, password string, target int) (*productSSPeer, error) {
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	aead, err := productSSCipher(password, salt)
	if err != nil {
		return nil, err
	}
	p := &productSSPeer{conn: conn, password: password, send: aead}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = conn.Write(salt); err != nil {
		return nil, err
	}
	header := []byte{1, 127, 0, 0, 1}
	header = binary.BigEndian.AppendUint16(header, uint16(target))
	if err = p.write(header); err != nil {
		return nil, err
	}
	return p, nil
}
func (p *productSSPeer) write(body []byte) error {
	length := binary.BigEndian.AppendUint16(nil, uint16(len(body)))
	frame := p.send.Seal(nil, p.sendNonce[:], length, nil)
	productSSAdvance(p.sendNonce[:])
	frame = p.send.Seal(frame, p.sendNonce[:], body, nil)
	productSSAdvance(p.sendNonce[:])
	for len(frame) > 0 {
		n, err := p.conn.Write(frame)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		frame = frame[n:]
	}
	return nil
}
func (p *productSSPeer) read() ([]byte, error) {
	if p.receive == nil {
		salt := make([]byte, 32)
		if _, err := io.ReadFull(p.conn, salt); err != nil {
			return nil, err
		}
		var err error
		p.receive, err = productSSCipher(p.password, salt)
		if err != nil {
			return nil, err
		}
	}
	length := make([]byte, 2+p.receive.Overhead())
	if _, err := io.ReadFull(p.conn, length); err != nil {
		return nil, err
	}
	decoded, err := p.receive.Open(nil, p.receiveNonce[:], length, nil)
	if err != nil {
		return nil, err
	}
	productSSAdvance(p.receiveNonce[:])
	size := int(binary.BigEndian.Uint16(decoded))
	if size > 16383 {
		return nil, errors.New("SS reference: invalid length")
	}
	body := make([]byte, size+p.receive.Overhead())
	if _, err := io.ReadFull(p.conn, body); err != nil {
		return nil, err
	}
	decoded, err = p.receive.Open(nil, p.receiveNonce[:], body, nil)
	if err != nil {
		return nil, err
	}
	productSSAdvance(p.receiveNonce[:])
	return decoded, nil
}
