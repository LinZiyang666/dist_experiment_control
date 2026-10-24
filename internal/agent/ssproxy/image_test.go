//go:build linux

package ssproxy_test

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/binaryupgrade"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

// Real: runtime Freeze/Export, sealed cut, outer Prepare manifest, durable marker,
// failed Exec/Restore, blocked-target and revoke logs. Controlled: private targets
// and high-entropy credentials. No dependency stub is accepted as successful IO.
// origin: hot-update S5 plan §6.6 / T26
// origin: hot-update S5 internal review rv-ss-4
func TestSSSecretsNeverEnterPublicCarriers(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	t.Run("observer-custody-format", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			exerciseSSObserverPublicCarriers(t)
		}
	})
	t.Run("refusal", func(t *testing.T) { exerciseSSPublicRefusal(t) })
	t.Run("blocked-target", func(t *testing.T) {
		exerciseSSBlockedTargetLog(t, "blocked-key-5147f8a092d0ec73", "blocked-psk-1369ce2470a8fb5d", nil, false)
	})
	t.Run("blocked-flow-target", func(t *testing.T) {
		exerciseSSBlockedTargetLog(t, "blocked-key-5147f8a092d0ec73", "blocked-psk-1369ce2470a8fb5d", nil, true)
	})
	t.Run("retained-image", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			exerciseSSPublicCarriers(t)
		}
	})
	testharness.AssertNoGoroutineLeak(t, "SS public carriers", g0)
	testharness.AssertNoFDLeak(t, "SS public carriers", fd0, 0)
}

func exerciseSSPublicRefusal(t *testing.T) {
	t.Helper()
	secret := []byte("private-ref-9a8c702df61b4e53")
	_, salvage, err := ssproxy.Restore(ssproxy.Image{Version: 1, StateRef: string(secret), SecretBytes: 1}, nil)
	if err == nil || len(salvage) != 0 {
		t.Fatal("invalid public reference acquired ownership")
	}
	scanSSPublic(t, "refusal error", []byte(fmt.Sprintf("%v %+v %#v %s %q %x %X", err, err, err, err, err, err, err)), [][]byte{secret})
}

func exerciseSSPublicCarriers(t *testing.T) {
	t.Helper()
	secrets := [][]byte{make([]byte, 32), make([]byte, 32)}
	for i := range secrets {
		if _, err := rand.Read(secrets[i]); err != nil {
			t.Fatal(err)
		}
	}
	// Text secrets also need encoding scans: a password/key ID can leak in
	// printable logs even when a binary salt never appears there.
	keyID, password := hex.EncodeToString(secrets[0]), base64.RawStdEncoding.EncodeToString(secrets[1])
	secrets = append(secrets, []byte(keyID), []byte(password))
	var logs bytes.Buffer
	s := ssproxy.New(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer s.Stop()
	if err := s.SetFlowRevoke(func([16]byte, string) {}); err != nil {
		t.Fatalf("NOT_RUN pending P8: SS image configuration: %v", err)
	}
	port, err := s.Start(0, []ssproxy.Key{{KeyID: keyID, Secret: password}})
	if err != nil {
		t.Fatal(err)
	}
	// Export an authenticated duplex actor paused in a
	// partial encrypted payload. Both inbound and response crypto state must
	// be present, rather than proving secrecy with an empty connection list.
	// origin: hot-update S5 internal review rv-ss-4
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(target.Close)
	accepted := make(chan net.Conn, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := target.Accept()
		if err != nil {
			return
		}
		accepted <- conn
		defer func(closeFunc func() error) { _ = closeFunc() }(conn.Close)
		buffer := make([]byte, 4096)
		for {
			n, err := conn.Read(buffer)
			if n > 0 {
				_, _ = conn.Write(buffer[:n])
			}
			if err != nil {
				return
			}
		}
	}()
	wire, _, _, subkey := ssPublicHandshake(t, password, target.Addr().String())
	raw, err := rawsocket.Connect(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port)), time.Now().Add(time.Second), nil,
		func(tuple rawsocket.Tuple) error { return s.ExpectFlow([16]byte{77}, tuple) })
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(raw.Close)
	defer func() {
		_ = target.Close()
		select {
		case conn := <-accepted:
			_ = conn.Close()
		case <-done:
		}
		<-done
	}()
	if err := raw.CompleteConnect(time.Now().Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	_ = raw.SetWriteDeadline(time.Now().Add(time.Second))
	_ = raw.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := raw.Write(wire); err != nil {
		t.Fatal(err)
	}
	cipher, err := chacha20poly1305.New(subkey)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("live-actor-secrecy-echo")
	appendFrame := func(count byte, plain []byte) []byte {
		nonce := make([]byte, 12)
		nonce[0] = count
		length := []byte{byte(len(plain) >> 8), byte(len(plain))}
		frame := cipher.Seal(nil, nonce, length, nil)
		nonce[0]++
		return cipher.Seal(frame, nonce, plain, nil)
	}
	if _, err := raw.Write(appendFrame(2, payload)); err != nil {
		t.Fatal(err)
	}
	// Read an actual encrypted response; the frozen actor must have reserved
	// its response salt and consumed both response nonces.
	response := make([]byte, 32+18+len(payload)+16)
	if _, err := io.ReadFull(raw, response); err != nil {
		t.Fatal(err)
	}
	partial := appendFrame(4, bytes.Repeat([]byte{93}, 64))
	if _, err := raw.Write(partial[:18+7]); err != nil {
		t.Fatal(err)
	}
	// Fence/export may race the last read. Retry real cuts until the observable
	// parser receipt proves that exactly seven payload cipher bytes arrived.
	var f *ssproxy.Frozen
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	for until := time.Now().Add(3 * time.Second); time.Now().Before(until); {
		f, err = s.Freeze(time.Now().Add(5 * time.Second))
		if err != nil {
			t.Fatal(err)
		}
		state, err := f.SnapshotState()
		if err != nil {
			t.Fatal(err)
		}
		if actorSecrets := ssLiveActorSecrets(t, state.Records); actorSecrets != nil {
			secrets = append(secrets, actorSecrets...)
			break
		}
		if err := f.Thaw(); err != nil {
			t.Fatal(err)
		}
		if err := s.Authorize(ssproxy.Grant{Enabled: true, Generation: 1, Epoch: 1, Keys: []ssproxy.Key{{KeyID: keyID, Secret: password}}}); err != nil {
			t.Fatal(err)
		}
		<-poll.C
	}
	if len(secrets) < 10 {
		t.Fatal("live partial crypto cursor was not exported")
	}

	if err != nil {
		t.Fatalf("NOT_RUN pending P8: real SS freeze: %v", err)
	}
	defer func() { _ = f.Discard() }()
	im, cut, err := f.Export()
	if err != nil {
		t.Fatalf("NOT_RUN pending P5/P8/P9: real SS export: %v", err)
	}
	if im.ConnectionCount != 1 || cut == nil || im.ListenerRef == "" || im.SecretBytes == 0 || len(cut.Owned) != 1 {
		t.Fatal("retained listener/key state was omitted from SS image")
	}
	data, err := json.Marshal(im)
	if err != nil {
		t.Fatal(err)
	}
	scanSSPublic(t, "image JSON", data, secrets)
	bundleData, err := json.Marshal(agentimage.Bundle{Version: agentimage.BundleVersion, Instance: "retained", SS: &im})
	if err != nil {
		t.Fatal(err)
	}
	scanSSPublic(t, "bundle JSON", bundleData, secrets)

	for _, row := range cut.Resources {
		scanSSPublic(t, "Resource.Data", row.Data, secrets)
	}
	for _, value := range []any{im, f, cut, s, ssproxy.Key{KeyID: keyID, Secret: password}, ssproxy.Grant{Enabled: true, Keys: []ssproxy.Key{{KeyID: keyID, Secret: password}}}} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X"} {
			scanSSPublic(t, "fmt", []byte(fmt.Sprintf(verb, value)), secrets)
		}
	}
	prepared, err := handoff.Prepare(handoff.State{Role: "ss-image-test", Instance: "retained", Generation: 1, Data: data}, cut.Resources)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	scanSSPreparedManifest(t, secrets)
	root := t.TempDir()
	markerPath := filepath.Join(root, binaryupgrade.MarkerName)
	marker := &binaryupgrade.Marker{State: binaryupgrade.StatePending, Generation: 1,
		ResourcePhase: binaryupgrade.PhaseStaged, CandidateImageABI: 1,
		CandidateImageDomains: []string{"exec-v1", "pty-v1", "flow-v1", "ss-v1"}}
	if err := (binaryupgrade.Store{}).WriteMarker(markerPath, marker); err != nil {
		t.Fatal(err)
	}
	markerBytes, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal(err)
	}
	scanSSPublic(t, "marker", markerBytes, secrets)
	// An actual kernel Exec refusal exercises the failure path without running
	// any tether binary. It must leave the original server available to Thaw.
	err = prepared.Exec(filepath.Join(root, "absent-image"), []string{"absent-image"}, handoff.CleanEnvironment(os.Environ()))
	if err == nil {
		t.Fatal("absent executable unexpectedly executed")
	}
	scanSSPublic(t, "Exec failure", []byte(err.Error()), secrets)
	source := handofftest.DupResources(t, cut.Resources)
	source.Drop(im.StateRef, "state")
	_, salvage, err := ssproxy.Restore(im, source)
	if err == nil || len(salvage) != 0 {
		t.Fatal("missing sealed state was accepted or business files were taken")
	}
	scanSSPublic(t, "import error", []byte(err.Error()), secrets)
	source.Close()
	// Decode real actor secrets before an adversarial FileSource failure. The
	// import boundary must redact even an underlying error containing a salt.
	actorSource := handofftest.DupResources(t, cut.Resources)
	injected := errors.New("actor file unavailable " + hex.EncodeToString(secrets[4]))
	_, actorSalvage, actorErr := ssproxy.Restore(im, ssFailingActorSource{source: actorSource, failure: injected})
	if actorErr == nil {
		t.Fatal("actor descriptor refusal was accepted")
	}
	for _, row := range actorSalvage {
		for _, file := range row.Files {
			_ = file.Close()
		}
	}
	actorSource.Close()
	scanSSPublic(t, "decoded actor import error", []byte(fmt.Sprintf("%v %+v %#v", actorErr, actorErr, actorErr)), secrets)
	if err := f.Thaw(); err != nil {
		t.Fatal(err)
	}
	// Revoke logs must also be scanned while the server is alive.
	if err := s.SetKeys(nil); err != nil {
		t.Fatal(err)
	}
	s.Stop()
	scanSSPublic(t, "revoke logs", logs.Bytes(), secrets)
	// Test the real blocked-target log separately: it has historically emitted
	// the decrypted target, so an empty image-only log is not sufficient.
	exerciseSSBlockedTargetLog(t, keyID, password, secrets, false)
}

func scanSSPublic(t *testing.T, carrier string, data []byte, secrets [][]byte) {
	t.Helper()
	for _, secret := range secrets {
		for _, representation := range [][]byte{secret, []byte(hex.EncodeToString(secret)),
			[]byte(strings.ToUpper(hex.EncodeToString(secret))), []byte(base64.StdEncoding.EncodeToString(secret)),
			[]byte(base64.RawStdEncoding.EncodeToString(secret)), []byte(base64.URLEncoding.EncodeToString(secret))} {
			if len(representation) != 0 && bytes.Contains(data, representation) {
				t.Fatalf("secret leaked into %s", carrier)
			}
		}
	}
}

func scanSSPreparedManifest(t *testing.T, secrets [][]byte) {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range entries {
		path := "/proc/self/fd/" + entry.Name()
		link, err := os.Readlink(path)
		if err != nil || !strings.Contains(link, "memfd:tether-handoff") {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		found = true
		scanSSPublic(t, "outer manifest", data, secrets)
	}
	if !found {
		t.Fatal("no actual outer handoff manifest was scanned")
	}
}

func exerciseSSBlockedTargetLog(t *testing.T, keyID, password string, secrets [][]byte, bound bool) {
	t.Helper()
	var logs bytes.Buffer
	s := ssproxy.New(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	s.DenyPrivateDestinations()
	if bound {
		if err := s.SetFlowRevoke(func([16]byte, string) {}); err != nil {
			t.Fatal(err)
		}
	}
	port, err := s.Start(0, []ssproxy.Key{{KeyID: keyID, Secret: password}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	// The wire encoder uses standard primitives independently of the SS owner,
	// so the blocked-target premise does not rely on its internal client helpers.
	target := "127.23.197.41:43987"
	wire, salt, nonce, subkey := ssPublicHandshake(t, password, target)
	secrets = append(secrets, []byte(keyID), []byte(password), []byte(target), salt, nonce, subkey)
	var conn interface {
		io.ReadWriteCloser
		SetReadDeadline(time.Time) error
		SetWriteDeadline(time.Time) error
	}
	if bound {
		// Exercise v1's approved-address path with the actual reservation and
		// original raw connector, independently of the legacy policy precheck.
		// origin: hot-update S5 plan §6.5 / §6.6 / T26
		var raw *rawsocket.Socket
		raw, err = rawsocket.Connect(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port)), time.Now().Add(3*time.Second), nil,
			func(tuple rawsocket.Tuple) error { return s.ExpectFlow([16]byte{1}, tuple) })
		if err == nil {
			conn = raw
			err = raw.CompleteConnect(time.Now().Add(3*time.Second), nil)
			if err != nil {
				_ = raw.Close()
			}
		}
	} else {
		conn, err = net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 3*time.Second)
	}
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write(wire); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	_, err = conn.Read(make([]byte, 1))
	_ = conn.Close()
	if !errors.Is(err, io.EOF) {
		t.Fatal("blocked-target path did not close its client")
	}
	s.Stop()
	if !bytes.Contains(logs.Bytes(), []byte("blocked")) {
		t.Fatal("blocked-target production log path was not exercised")
	}
	scanSSPublic(t, "blocked-target logs", logs.Bytes(), secrets)
}

func ssPublicHandshake(t *testing.T, password, target string) (wire, salt, nonce, subkey []byte) {
	t.Helper()
	var master, previous []byte
	for len(master) < 32 {
		digest := md5.New()
		_, _ = digest.Write(previous)
		_, _ = digest.Write([]byte(password))
		previous = digest.Sum(nil)
		master = append(master, previous...)
	}
	salt = make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		t.Fatal(err)
	}
	subkey = make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha1.New, master[:32], salt, []byte("ss-subkey")), subkey); err != nil {
		t.Fatal(err)
	}
	aead, err := chacha20poly1305.New(subkey)
	if err != nil {
		t.Fatal(err)
	}
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		t.Fatal(err)
	}
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	payload := append([]byte{3, byte(len(host))}, []byte(host)...)
	payload = append(payload, byte(number>>8), byte(number))
	length := make([]byte, 2)
	binary.BigEndian.PutUint16(length, uint16(len(payload)))
	nonce = make([]byte, 12)
	wire = append(wire, salt...)
	wire = aead.Seal(wire, nonce, length, nil)
	payloadNonce := make([]byte, 12)
	payloadNonce[0] = 1
	wire = aead.Seal(wire, payloadNonce, payload, nil)
	return wire, salt, nonce, subkey
}

// Collect the actual private record values, not independently guessed nonces.
// Heap-visible public representations (fmt/log/error/JSON strings) are scanned;
// the actor's necessary private byte buffers remain allowed in process memory.
func ssLiveActorSecrets(t *testing.T, records []sealedstate.Record) [][]byte {
	t.Helper()
	for _, record := range records {
		if record.Kind != sealedstate.KindSSActor {
			continue
		}
		fields := make(map[string]sealedstate.Field)
		for _, field := range record.Fields {
			fields[field.Name] = field
		}
		if fields["rx_payload_cursor"].U64 != 7 {
			return nil
		}
		var out [][]byte
		for _, name := range []string{"rx_salt", "rx_subkey", "rx_nonce", "tx_salt", "tx_subkey", "tx_nonce"} {
			value := fields[name].Bytes
			if len(value) == 0 || bytes.Equal(value, make([]byte, len(value))) {
				t.Fatalf("live crypto field %s is empty", name)
			}
			out = append(out, append([]byte(nil), value...))
		}
		out = append(out, append([]byte(nil), fields["rx_payload_cipher"].Bytes[:7]...))
		return out
	}
	t.Fatal("retained image contains zero live actors")
	return nil
}

type ssFailingActorSource struct {
	source  rawsocket.FileSource
	failure error
}

func (s ssFailingActorSource) TakeFile(id, name string) (*os.File, error) {
	if name == "loopback" {
		return nil, s.failure
	}
	return s.source.TakeFile(id, name)
}

// origin: hot-update S5 plan §6.6 / T26 / DRILL-OBSERVER
func exerciseSSObserverPublicCarriers(t *testing.T) {
	t.Helper()
	secret := []byte("observer-private-key-salt-target-nonce-760db5683e8a")
	e := drillobserve.Event{Kind: "ss-write", Domain: "ss", Data: secret}
	copy(e.Digest[:], secret)
	copy(e.Nonce[:], secret)
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%c"} {
		scanSSPublic(t, "observer formatting", []byte(fmt.Sprintf(verb, e)), [][]byte{secret, e.Digest[:], e.Nonce[:]})
	}
	public, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	scanSSPublic(t, "observer JSON", public, [][]byte{secret, e.Digest[:], e.Nonce[:]})
	env := drillobserve.StripEnvironment([]string{drillobserve.EnableEnv + "=v1", drillobserve.CustodyEnv + "=9", "TERM=xterm"})
	if len(env) != 1 || env[0] != "TERM=xterm" {
		t.Fatal("custody environment entered workload carrier")
	}
}
