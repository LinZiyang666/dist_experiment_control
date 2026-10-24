//go:build linux

package ssproxy

import (
	"bytes"
	"crypto/cipher"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/crypto/chacha20poly1305"
)

// Real: two bound actors and original upstreams, re-export, Prepare/failed Exec,
// missing second upstream Take, salvage, and Thaw/Discard. Every FD count is
// measured while the receiver and original peers remain alive, before teardown.
// This is an in-process kernel receipt, not a real-binary upgrade receipt.
// origin: hot-update S5 plan §5.3 / T27
func TestSSFiveRoundImageOwnershipFailures(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSSImageOwnership(t)
	}
	testharness.AssertNoGoroutineLeak(t, "SS image ownership", g0)
	testharness.AssertNoFDLeak(t, "SS image ownership", fd0, 0)
}

func exerciseSSImageOwnership(t *testing.T) {
	t.Helper()
	s := New(nil)
	defer s.Stop()
	if err := s.SetFlowRevoke(func([16]byte, string) {}); err != nil {
		t.Fatalf("SS actor configuration: %v", err)
	}
	port, err := s.Start(0, []Key{{KeyID: "image-owner", Secret: "image-owner-password"}})
	if err != nil {
		t.Fatal(err)
	}
	target, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = target.Close() }()
	var clients []*rawsocket.Socket
	var peers []*net.TCPConn
	var readers []io.Reader
	for actor := 1; actor <= 2; actor++ {
		var id [16]byte
		id[0] = byte(actor)
		client, err := rawsocket.Connect(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port)),
			time.Now().Add(5*time.Second), nil, func(tuple rawsocket.Tuple) error { return s.ExpectFlow(id, tuple) })
		if err != nil {
			t.Fatalf("bound loopback: %v", err)
		}
		clients = append(clients, client)
		defer func() { _ = client.Close() }()
		wire := encryptedTargetHandshake(t, "image-owner-password", target.Addr().String(), bytes.Repeat([]byte{byte(actor)}, saltSize))
		if n, err := client.Write(wire); err != nil || n != len(wire) {
			t.Fatal("original SS client did not deliver handshake")
		}
		_ = target.SetDeadline(time.Now().Add(5 * time.Second))
		peer, err := target.AcceptTCP()
		if err != nil {
			t.Fatal("original upstream was not established")
		}
		peers = append(peers, peer)
		defer func() { _ = peer.Close() }()
		// Accept can return before the connector publishes its owned raw FD.
		// Observe a real response on each original connection before asking
		// Freeze for a representable relay cut; keep this nonce cursor for Thaw.
		// origin: hot-update S5 plan §6.1 / §5.3 / T27; GATES-FINAL-2.
		_ = peer.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := peer.Write([]byte{byte(actor)}); err != nil {
			t.Fatal(err)
		}
		_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
		salt := make([]byte, saltSize)
		if _, err := io.ReadFull(client, salt); err != nil {
			t.Fatal(err)
		}
		subkey, err := sessionSubkey(evpBytesToKey("image-owner-password"), salt)
		if err != nil {
			t.Fatal(err)
		}
		reader := newAEADReader(client, mustSSImageAEAD(t, subkey))
		var got [1]byte
		if _, err := io.ReadFull(reader, got[:]); err != nil || got[0] != byte(actor) {
			t.Fatal("original response did not prove relay readiness")
		}
		readers = append(readers, reader)
	}
	baseline := ssImageFDKinds(t)
	f, err := s.Freeze(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatalf("image freeze: %v", err)
	}
	defer func() { _ = f.Discard() }()
	first, old, err := f.Export()
	if err != nil {
		t.Fatalf("image export: %v", err)
	}
	// P8's accept/listener owners are Go TCP wrappers. Their first snapshot
	// owns one nonblocking dup each (two clients plus the listener); upstreams
	// already use raw originals. Account for these three handles explicitly,
	// then require re-export to retain precisely the same socket ownership.
	// origin: hot-update S5 plan §5.1 / §5.3 / T27
	firstExportFD := ssImageFDKinds(t)
	if firstExportFD["sealed"] != baseline["sealed"]+1 || firstExportFD["socket"] != baseline["socket"]+3 {
		t.Fatal("first export did not account for the listener and two client wrapper duplicates")
	}
	im, cut, err := f.Export()
	if err != nil {
		t.Fatal(err)
	}
	if first.StateRef == im.StateRef || old == cut || im.ConnectionCount != 2 {
		t.Fatal("re-export did not replace a nonempty two-actor cut")
	}
	for _, file := range old.Owned {
		if _, err := file.Stat(); err == nil {
			t.Fatal("re-export retained the previous sealed original")
		}
	}
	afterExport := ssImageFDKinds(t)
	ssRequireFDKinds(t, afterExport, firstExportFD, "re-export retains the same business handles and one sealed cut")
	data, _ := json.Marshal(im)
	prepared, err := handoff.Prepare(handoff.State{Role: "ss-image-test", Instance: "two-actors", Generation: 1, Data: data}, cut.Resources)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Exec(t.TempDir()+"/absent-image", []string{"absent-image"}, handoff.CleanEnvironment(os.Environ())); err == nil {
		t.Fatal("kernel Exec unexpectedly succeeded")
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	ssRequireFDKinds(t, ssImageFDKinds(t), afterExport, "failed Exec/Prepared.Close")
	// Decode the real sealed state to choose the second actor's resource. No
	// private actor internals or guessed map iteration order determine the fault.
	readSource := handofftest.DupResources(t, cut.Resources)
	private, stateFile, err := sealedstate.Read(readSource, im.StateRef, "state", im.SecretBytes)
	if err != nil {
		t.Fatal(err)
	}
	_ = stateFile.Close()
	records, err := decodeState(private)
	clear(private)
	if err != nil {
		t.Fatal(err)
	}
	_, files, err := ssImageLayout(records)
	if err != nil {
		t.Fatal(err)
	}
	var upstreams []ssImageFile
	for _, file := range files {
		if file.name == "upstream" {
			upstreams = append(upstreams, file)
		}
	}
	if len(upstreams) != 2 {
		t.Fatal("failure premise lacks two original upstream descriptors")
	}
	readSource.Close()
	source := handofftest.DupResources(t, cut.Resources)
	source.Drop(upstreams[1].id, "upstream")
	imported, salvage, err := Restore(im, source)
	if err == nil || imported != nil {
		t.Fatal("missing second upstream did not reject import")
	}
	if !ssImageHasFile(salvage, upstreams[0].id, "upstream") {
		t.Fatal("failure lost the first actor's already-taken upstream")
	}
	merged, err := handoff.MergeResources(salvage, source.Borrow())
	if err != nil {
		t.Fatal(err)
	}
	if ssImageFileCount(merged) != ssImageFileCount(cut.Resources)-1 {
		t.Fatal("failure salvage lost listener, loopback, upstream or sealed responsibility")
	}
	ssCloseImageFiles(salvage)
	source.Close()
	ssRequireFDKinds(t, ssImageFDKinds(t), afterExport, "failed import live receiver")
	if err := f.Thaw(); err != nil {
		t.Fatal(err)
	}
	ssRequireFDKinds(t, ssImageFDKinds(t), baseline, "Thaw releases only sealed cut")
	if err := s.Authorize(Grant{Enabled: true, Generation: 1, Epoch: 1, AppliedRevision: 1,
		Keys: []Key{{KeyID: "image-owner", Secret: "image-owner-password"}}}); err != nil {
		t.Fatal(err)
	}
	// Original reverse byte flow must still work after Exec failed and import
	// was refused; counting descriptors alone would miss an early socket Close.
	for i, peer := range peers {
		_ = peer.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := peer.Write([]byte{byte(91 + i)}); err != nil {
			t.Fatal("Thaw closed original upstream")
		}
		_ = clients[i].SetReadDeadline(time.Now().Add(5 * time.Second))
		var got [1]byte
		if _, err := io.ReadFull(readers[i], got[:]); err != nil || got[0] != byte(91+i) {
			t.Fatal("original reverse byte flow failed after Thaw")
		}
	}
	last, err := s.Freeze(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	lastImage, lastCut, err := last.Export()
	if err != nil {
		t.Fatal(err)
	}
	identities := ssImageResourceIdentities(t, lastCut.Resources)
	successSource := handofftest.DupResources(t, lastCut.Resources)
	receiver, salvage, err := Restore(lastImage, successSource)
	if err != nil || receiver == nil || len(salvage) != 0 || successSource.Remaining() != 0 {
		t.Fatal("complete closure failed to transfer every FD to the frozen receiver")
	}
	defer receiver.Stop()
	// Restore owns frozen raw wrappers and the original consumed connect
	// receipt. Kernel connection shape alone cannot reconstruct that receipt.
	// origin: hot-update S5 plan §6.1 / T17 / T27
	for _, actor := range receiver.imageState.actors {
		for _, sock := range []actorSocket{actor.client, actor.upstream} {
			raw, ok := sock.(*rawsocket.Socket)
			if !ok {
				t.Fatal("import did not adopt the original raw descriptor")
			}
			if _, err := rawsocket.BorrowHeldFile(raw); err != nil {
				t.Fatal("import activated a raw wrapper before the frozen-owner boundary")
			}
		}
		phase, consumed, tuple, _, connectErr := rawsocket.ConnectReceipt(actor.upstream.(*rawsocket.Socket))
		wantTuple, err := parseTuple(actor.state.bytes("upstream_tuple"))
		if err != nil || phase != rawsocket.ConnectState(actor.state.number("connect_state")) || consumed != actor.state.flag("connect_so_error_consumed") || connectErr != nil || tuple != wantTuple {
			t.Fatal("import lost the original upstream connect receipt")
		}
	}
	// Callback pointers are not serialized. The restoring owner must reinstall
	// its flow-revoke adapter before activation, or cleanup remains pending for
	// the retained bound flows even after their raw sockets are shut down.
	// origin: hot-update S5 plan §6.5 / §7.1 / T27
	var revoked atomic.Uint32
	if err := receiver.SetFlowRevoke(func(id [16]byte, reason string) {
		if (id[0] != 1 && id[0] != 2) || id != ([16]byte{id[0]}) || reason != "revoked" {
			t.Error("restored owner revoked an unknown flow or reason")
		}
		revoked.Add(1)
	}); err != nil {
		t.Fatal(err)
	}
	if err := last.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := receiver.ActivateRaw(); err != nil {
		t.Fatal(err)
	}
	if err := receiver.Authorize(Grant{Enabled: true, Generation: 1, Epoch: 1, AppliedRevision: 2,
		Keys: []Key{{KeyID: "image-owner", Secret: "image-owner-password"}}}); err != nil {
		t.Fatal(err)
	}
	s.Stop()
	receiver.Stop()
	if revoked.Load() != 2 {
		t.Fatal("receiver did not finish cleanup for both retained bound flows")
	}
	successSource.Close()
	// Neither receiver exit nor closing fixture peers can clean these product
	// FDs for us. Count original identities after product owners retire.
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		info, err := os.Stat("/proc/self/fd/" + entry.Name())
		if err != nil {
			continue
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if ok && identities[[2]uint64{uint64(stat.Dev), stat.Ino}] {
			t.Fatal("live receiver retained a retired SS listener, loopback, upstream or sealed descriptor")
		}
	}
	if now := ssImageFDKinds(t); now["sealed"] != baseline["sealed"] || now["handoff"] != baseline["handoff"] {
		t.Fatal("Discard retained a sealed or prepared descriptor in the live receiver")
	}
}

// origin: hot-update S5 plan §5.3 / §5.4 / T34
// A successful frozen import can still be refused by another domain. Returning
// its exact sealed closure must preserve both business OFDs and the existing
// response nonce, while activation permanently retires pristine rollback.
func TestSSPristineDetachPreservesOriginalClosure(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSSPristineDetach(t)
	}
	testharness.AssertNoGoroutineLeak(t, "SS pristine detach", g0)
	testharness.AssertNoFDLeak(t, "SS pristine detach", fd0, 0)
}

func exerciseSSPristineDetach(t *testing.T) {
	t.Helper()
	s := New(testharness.SilentLog())
	defer s.Stop()
	if err := s.SetFlowRevoke(func([16]byte, string) {}); err != nil {
		t.Fatal(err)
	}
	keys := []Key{{KeyID: "detach-owner", Secret: "detach-owner-password"}}
	port, err := s.Start(0, keys)
	if err != nil {
		t.Fatal(err)
	}
	target, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(target.Close)
	client, err := rawsocket.Connect(netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port)), time.Now().Add(5*time.Second), nil,
		func(tuple rawsocket.Tuple) error { return s.ExpectFlow([16]byte{1}, tuple) })
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(client.Close)
	wire := encryptedTargetHandshake(t, keys[0].Secret, target.Addr().String(), bytes.Repeat([]byte{1}, saltSize))
	if n, err := client.Write(wire); err != nil || n != len(wire) {
		t.Fatal("original handshake was not delivered")
	}
	_ = target.SetDeadline(time.Now().Add(5 * time.Second))
	peer, err := target.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(peer.Close)
	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := peer.Write([]byte("before")); err != nil {
		t.Fatal(err)
	}
	salt := make([]byte, saltSize)
	if _, err := io.ReadFull(client, salt); err != nil {
		t.Fatal(err)
	}
	subkey, err := sessionSubkey(evpBytesToKey(keys[0].Secret), salt)
	if err != nil {
		t.Fatal(err)
	}
	reader := newAEADReader(client, mustSSImageAEAD(t, subkey))
	before := make([]byte, 6)
	if _, err := io.ReadFull(reader, before); err != nil || string(before) != "before" {
		t.Fatal("original response ledger did not advance")
	}
	if resources, err := s.Detach(); err == nil || resources != nil {
		t.Fatal("live server allowed pristine detach")
	}
	frozen, err := s.Freeze(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(frozen.Discard)
	im, cut, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	if resources, err := s.Detach(); err == nil || resources != nil {
		t.Fatal("exported owner allowed pristine import detach")
	}
	source := handofftest.DupResources(t, cut.Resources)
	defer source.Close()
	receiver, salvage, err := Restore(im, source)
	if err != nil || len(salvage) != 0 {
		t.Fatal("pristine import failed")
	}
	defer receiver.Stop()
	resources, err := receiver.Detach()
	if err != nil || ssImageFileCount(resources) != ssImageFileCount(cut.Resources) || !ssImageHasFile(resources, im.StateRef, "state") {
		t.Fatal("detach omitted the original sealed or business closure")
	}
	defer ssCloseImageFiles(resources)
	identities := ssImageResourceIdentities(t, resources)
	receiver.Stop()
	if again, err := receiver.Detach(); err == nil || again != nil {
		t.Fatal("detach returned a second close owner")
	}
	if err := receiver.ActivateRaw(); !errors.Is(err, ErrStopped) {
		t.Fatal("detached wrapper could reactivate")
	}
	nextSource := handofftest.DupResources(t, resources)
	defer nextSource.Close()
	next, salvage, err := Restore(im, nextSource)
	if err != nil || len(salvage) != 0 || nextSource.Remaining() != 0 {
		t.Fatal("detached closure could not restore the same image")
	}
	defer next.Stop()
	nextIdentities := ssImageResourceIdentities(t, next.imageState.importedResources)
	for identity := range identities {
		if !nextIdentities[identity] {
			t.Fatal("redispatch replaced an original descriptor identity")
		}
	}
	if err := frozen.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := next.SetFlowRevoke(func([16]byte, string) { _ = client.Shutdown() }); err != nil {
		t.Fatal(err)
	}
	stateFile := next.imageState.importedResources[0].Files["state"]
	if err := next.ActivateRaw(); err != nil {
		t.Fatal(err)
	}
	if _, err := stateFile.Stat(); err == nil {
		t.Fatal("activation retained the pristine sealed cut")
	}
	if resources, err := next.Detach(); err == nil || resources != nil {
		t.Fatal("active owner allowed pristine detach")
	}
	if err := next.Authorize(Grant{Enabled: true, Generation: 1, Epoch: 1, AppliedRevision: 2, Keys: keys}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Write([]byte("after")); err != nil {
		t.Fatal(err)
	}
	after := make([]byte, 5)
	if _, err := io.ReadFull(reader, after); err != nil || string(after) != "after" {
		t.Fatal("pristine detach closed a business OFD or reset its response nonce")
	}
}

// Real: FD transfers and Prepare duplicates. Controlled: minimal private record
// inputs for image metadata and deliberately hostile upstream error messages.
// This tests P10's boundary without substituting for any dependency's codec/IO.
// origin: hot-update S5 plan §5.1 / §6.6 / T26 / T27
func TestSSImageClosureSalvageAndRedaction(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSSImageBoundary(t)
	}
	testharness.AssertNoGoroutineLeak(t, "SS image boundary", g0)
	testharness.AssertNoFDLeak(t, "SS image boundary", fd0, 0)
}

func exerciseSSImageBoundary(t *testing.T) {
	t.Helper()
	secret := "private-image-6daf490582c73e11"
	err := ssImageError(fmt.Errorf("target %s: %w", secret, ErrCapacity))
	if err != ErrCapacity || strings.Contains(err.Error(), secret) {
		t.Fatal("typed image error retained secret upstream detail")
	}
	err = ssImageError(errors.New(secret))
	if err != ErrStateUnrepresentable || strings.Contains(err.Error(), secret) {
		t.Fatal("untyped image error leaked private upstream detail")
	}
	file, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	records := ssImageBoundaryRecords()
	im, files, err := ssImageLayout(records)
	if err != nil || len(files) != 3 || im.ConnectionCount != 1 || im.ListenerRef == "" {
		t.Fatal("nonempty private closure could not produce public metadata")
	}
	rows := []handoff.Resource{{ID: files[0].id, Kind: secret, Data: json.RawMessage(`"` + secret + `"`), Files: map[string]*os.File{"listener": file}},
		{ID: files[1].id, Kind: secret, Data: json.RawMessage(`"` + secret + `"`), Files: map[string]*os.File{"loopback": file, "upstream": file}}}
	public, err := ssImageBorrowed(rows, files)
	if err != nil {
		t.Fatal(err)
	}
	imageJSON, _ := json.Marshal(im)
	data := append([]byte(nil), imageJSON...)
	for _, row := range public {
		if len(row.Data) != 0 || strings.Contains(row.Kind, secret) || !strings.HasPrefix(row.ID, "ss-") {
			t.Fatal("public resource copied private runtime metadata")
		}
		data = append(data, []byte(fmt.Sprintf("%v %+v %#v", row, row, row))...)
	}
	if bytes.Contains(data, []byte(secret)) || bytes.Contains(data, []byte(hex.EncodeToString([]byte(secret)))) {
		t.Fatal("public image/fmt disclosed private metadata")
	}
	prepared, err := handoff.Prepare(handoff.State{Role: "ss-image-test", Instance: "boundary", Generation: 1, Data: imageJSON}, public)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	manifests := 0
	for _, entry := range entries {
		path := "/proc/self/fd/" + entry.Name()
		link, err := os.Readlink(path)
		if err != nil || !strings.Contains(link, "memfd:tether-handoff") {
			continue
		}
		manifest, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		manifests++
		if bytes.Contains(manifest, []byte(secret)) {
			t.Fatal("outer manifest disclosed private runtime metadata")
		}
	}
	if manifests == 0 {
		t.Fatal("outer manifest oracle was empty")
	}
	source := handofftest.DupResources(t, public)
	tracked := &ssImageSource{source: source}
	for _, want := range files {
		if _, err := tracked.TakeFile(want.id, want.name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tracked.TakeFile(files[2].id, "unavailable"); err == nil {
		t.Fatal("nil source result accepted")
	}
	if source.Remaining() != 0 || !ssImageHasFile(tracked.resources, files[2].id, "upstream") {
		t.Fatal("tracked closure lost a taken upstream descriptor")
	}
	salvage, err := ssImageSalvage(tracked.resources, tracked.resources[:1])
	if err != nil || ssImageFileCount(salvage) != 3 || !ssImageHasFile(salvage, files[2].id, "upstream") {
		t.Fatal("partial runtime salvage lost already-taken upstream")
	}
	ssCloseImageFiles(salvage)
	source.Close()
	empty := &ssImageSource{source: ssImageFailureSource{}}
	if _, err := empty.TakeFile(files[2].id, "upstream"); err != ErrStateUnrepresentable {
		t.Fatal("nil FileSource success accepted")
	}
	failedFile, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = failedFile.Close() }()
	failed := &ssImageSource{source: ssImageFailureSource{file: failedFile, err: errors.New(secret)}}
	if _, err := failed.TakeFile(files[2].id, "upstream"); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatal("FileSource error disclosed a private value")
	}
	stateID := "ss-state." + strings.Repeat("3", 32)
	stateTrack := &ssImageSource{source: ssImageFailureSource{file: failedFile}}
	if _, err := stateTrack.TakeFile(stateID, "state"); err != nil || !ssImageHasFile(stateTrack.resources, stateID, "state") {
		t.Fatal("tracked state FD lost salvage ownership")
	}
	if !ssImageHasFile(failed.resources, files[2].id, "upstream") {
		t.Fatal("FileSource returned file plus error without salvage ownership")
	}
}

// origin: hot-update S5 plan §5.1 / T26 / T27
func TestSSImageRejectsIncompleteFDClosure(t *testing.T) {
	for _, ref := range []string{"", "ss-state.", "secret-target", "xx-state." + strings.Repeat("0", 32), "flow-state." + strings.Repeat("0", 32),
		"ss-state." + strings.Repeat("A", 32), "ss-state." + strings.Repeat("g", 32), "ss-state." + strings.Repeat("0", 33)} {
		if ssImageRef(ref, "ss-state.") {
			t.Fatal("untyped public reference accepted")
		}
	}
	for _, im := range []Image{
		{}, {Version: 2}, {Version: 1, StateRef: "ss-state." + strings.Repeat("0", 32), SecretBytes: -1},
		{Version: 1, StateRef: "ss-state." + strings.Repeat("0", 32), SecretBytes: sealedstate.MaxStateBytes + 1},
		{Version: 1, StateRef: "ss-state." + strings.Repeat("0", 32), SecretBytes: 1, ConnectionCount: MaxActors + 1},
		{Version: 1, StateRef: "ss-state." + strings.Repeat("0", 32), SecretBytes: 1, ConnectionCount: -1},
		{Version: 1, StateRef: "ss-state." + strings.Repeat("0", 32), SecretBytes: 1, ListenerRef: "secret-target"},
	} {
		if _, salvage, err := Restore(im, ssImageFailureSource{err: errors.New("private-source")}); err != ErrStateUnrepresentable || len(salvage) != 0 {
			t.Fatal("public shape guard did not reject before sealed IO")
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func([]sealedstate.Record) []sealedstate.Record
	}{
		{"missing-server", func(r []sealedstate.Record) []sealedstate.Record { return r[1:] }},
		{"duplicate-server", func(r []sealedstate.Record) []sealedstate.Record { return append(r, r[0]) }},
		{"duplicate-actor", func(r []sealedstate.Record) []sealedstate.Record { return append(r, r[1]) }},
		{"unknown-record", func(r []sealedstate.Record) []sealedstate.Record { r[1].Kind = "unknown"; return r }},
		{"foreign-domain", func(r []sealedstate.Record) []sealedstate.Record { r[1].Kind = sealedstate.KindFlow; return r }},
		{"zero-server-id", func(r []sealedstate.Record) []sealedstate.Record {
			r[0].Fields[0].Bytes = make([]byte, 16)
			r[0].Fields[1].Bytes = []byte("ss-listener." + strings.Repeat("0", 32))
			return r
		}},
		{"short-server-id", func(r []sealedstate.Record) []sealedstate.Record {
			r[0].Fields[0].Bytes = []byte{1}
			r[0].Fields[1].Bytes = []byte("ss-listener.01")
			return r
		}},
		{"private-listener-ref", func(r []sealedstate.Record) []sealedstate.Record {
			r[0].Fields[1].Bytes = []byte("private-target")
			return r
		}},
		{"private-upstream-ref", func(r []sealedstate.Record) []sealedstate.Record {
			r[1].Fields[2].Bytes = []byte("private-target")
			return r
		}},
		{"missing-fingerprint", func(r []sealedstate.Record) []sealedstate.Record { r[1].Fields = r[1].Fields[:5]; return r }},
		{"duplicate-field", func(r []sealedstate.Record) []sealedstate.Record {
			r[0].Fields = append(r[0].Fields, r[0].Fields[1])
			return r
		}},
		{"wrong-field-type", func(r []sealedstate.Record) []sealedstate.Record {
			r[0].Fields[1].Type = sealedstate.FieldBytes
			return r
		}},
		{"zero-inode", func(r []sealedstate.Record) []sealedstate.Record { r[0].Fields[3].U64 = 0; return r }},
		{"overflow-file-type", func(r []sealedstate.Record) []sealedstate.Record { r[0].Fields[4].U64 = 1 << 32; return r }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := ssImageLayout(tc.mutate(ssImageBoundaryRecords())); err == nil {
				t.Fatal("incomplete or secret-bearing closure accepted")
			}
		})
	}
	many := ssImageBoundaryRecords()
	for actor := 1; actor <= MaxActors; actor++ {
		row := ssImageBoundaryRecords()[1]
		id := make([]byte, 16)
		id[0], id[1] = byte(actor), 1
		row.Fields[0].Bytes = id
		row.Fields[1].Bytes = []byte("ss-conn." + hex.EncodeToString(id))
		row.Fields[2].Bytes = []byte("ss-conn." + hex.EncodeToString(id))
		many = append(many, row)
	}
	if _, _, err := ssImageLayout(many); err != ErrStateUnrepresentable {
		t.Fatal("actor capacity accepted")
	}
	file, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	_, files, err := ssImageLayout(ssImageBoundaryRecords())
	if err != nil {
		t.Fatal(err)
	}
	good := []handoff.Resource{{ID: files[0].id, Files: map[string]*os.File{"listener": file}},
		{ID: files[1].id, Files: map[string]*os.File{"loopback": file, "upstream": file}}}
	for _, bad := range [][]handoff.Resource{nil, good[:1],
		append([]handoff.Resource{{ID: files[0].id, Files: map[string]*os.File{"listener": nil}}}, good...), append(append([]handoff.Resource{}, good...), good[0]),
		append(append([]handoff.Resource{}, good...), handoff.Resource{ID: "extra", Files: map[string]*os.File{"raw": file}}),
		{{ID: files[0].id, Files: map[string]*os.File{"listener": nil}}}} {
		if _, err := ssImageBorrowed(bad, files); err == nil {
			t.Fatal("unaccounted or duplicate FD accepted")
		}
	}
	other, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close() }()
	if _, err := ssImageSalvage(good, []handoff.Resource{{ID: files[1].id, Files: map[string]*os.File{"upstream": other}}}); err == nil {
		t.Fatal("runtime returned a conflicting descriptor owner")
	}
	if _, err := ssImageSalvage(good, []handoff.Resource{{ID: "foreign", Files: map[string]*os.File{"upstream": file}}}); err == nil {
		t.Fatal("runtime returned an untracked owner")
	}
}

func ssImageBoundaryRecords() []sealedstate.Record {
	serverID, connID := make([]byte, 16), make([]byte, 16)
	serverID[0], connID[0] = 1, 2
	listener := "ss-listener." + hex.EncodeToString(serverID)
	conn := "ss-conn." + hex.EncodeToString(connID)
	return []sealedstate.Record{
		{Kind: sealedstate.KindSSServer, Fields: []sealedstate.Field{
			{Name: "id", Type: sealedstate.FieldID16, Bytes: serverID},
			{Name: "listener_ref", Type: sealedstate.FieldUTF8, Bytes: []byte(listener)},
			{Name: "listener_dev", Type: sealedstate.FieldU64, U64: 1},
			{Name: "listener_ino", Type: sealedstate.FieldU64, U64: 2},
			{Name: "listener_type", Type: sealedstate.FieldU64, U64: 3},
		}},
		{Kind: sealedstate.KindSSActor, Fields: []sealedstate.Field{
			{Name: "conn_id", Type: sealedstate.FieldID16, Bytes: connID},
			{Name: "loopback_ref", Type: sealedstate.FieldUTF8, Bytes: []byte(conn)},
			{Name: "upstream_ref", Type: sealedstate.FieldUTF8, Bytes: []byte(conn)},
			{Name: "loopback_dev", Type: sealedstate.FieldU64, U64: 1},
			{Name: "loopback_ino", Type: sealedstate.FieldU64, U64: 4},
			{Name: "loopback_type", Type: sealedstate.FieldU64, U64: 3},
			{Name: "upstream_dev", Type: sealedstate.FieldU64, U64: 1},
			{Name: "upstream_ino", Type: sealedstate.FieldU64, U64: 5},
			{Name: "upstream_type", Type: sealedstate.FieldU64, U64: 3},
		}},
	}
}

func ssImageHasFile(resources []handoff.Resource, id, name string) bool {
	for _, resource := range resources {
		if resource.ID == id && resource.Files[name] != nil {
			return true
		}
	}
	return false
}

func ssImageFileCount(resources []handoff.Resource) int {
	count := 0
	for _, resource := range resources {
		count += len(resource.Files)
	}
	return count
}

func ssCloseImageFiles(resources []handoff.Resource) {
	for _, resource := range resources {
		for _, file := range resource.Files {
			_ = file.Close()
		}
	}
}

func ssImageFDKinds(t *testing.T) map[string]int {
	t.Helper()
	counts := map[string]int{"socket": 0, "sealed": 0, "handoff": 0, "replay": 0}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		link, err := os.Readlink("/proc/self/fd/" + entry.Name())
		if err != nil {
			continue
		}
		switch {
		case strings.HasPrefix(link, "socket:"):
			counts["socket"]++
		case strings.Contains(link, "memfd:tether-handoff"):
			counts["handoff"]++
		case strings.Contains(link, "memfd:tether-replay"):
			counts["replay"]++
		case strings.Contains(link, "memfd:"):
			counts["sealed"]++
		}
	}
	return counts
}

func ssRequireFDKinds(t *testing.T, got, want map[string]int, stage string) {
	t.Helper()
	for kind, count := range want {
		if got[kind] != count {
			t.Fatalf("live receiver FD class %s changed during %s: got=%d want=%d", kind, stage, got[kind], count)
		}
	}
}

func mustSSImageAEAD(t *testing.T, subkey []byte) cipher.AEAD {
	t.Helper()
	aead, err := chacha20poly1305.New(subkey)
	if err != nil {
		t.Fatal(err)
	}
	return aead
}

type ssImageFailureSource struct {
	file *os.File
	err  error
}

func (s ssImageFailureSource) TakeFile(_, _ string) (*os.File, error) { return s.file, s.err }

func ssImageResourceIdentities(t *testing.T, resources []handoff.Resource) map[[2]uint64]bool {
	t.Helper()
	identities := make(map[[2]uint64]bool)
	for _, resource := range resources {
		for _, file := range resource.Files {
			info, err := file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				t.Fatal("kernel descriptor identity unavailable")
			}
			identities[[2]uint64{uint64(stat.Dev), stat.Ino}] = true
		}
	}
	return identities
}
