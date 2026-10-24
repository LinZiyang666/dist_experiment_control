package infraattach

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S9 internal review s9-instr4 Request 1
func TestAttachmentObservationExportsOriginalConnection(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "attachment exports", goroutines)
	defer testharness.AssertNoFDLeak(t, "attachment exports", fds, 0)
	for round := 0; round < 5; round++ {
		t.Run("original", func(t *testing.T) { exerciseAttachmentExport(t) })
	}
}

// origin: hot-update S9 plan §4.5 / M17; s9-instr4 Request 1
func TestAttachmentSamplerIncludesIndependentOriginalDataNC(t *testing.T) {
	var mu sync.Mutex
	var rows []drillobserve.Event
	previous := drillobserve.Hook
	drillobserve.Hook = func(e drillobserve.Event) { mu.Lock(); defer mu.Unlock(); rows = append(rows, e) }
	defer func() { drillobserve.Hook = previous }()
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	member, _ := f.keys[1].PublicKey()
	descriptor := nats.GetDefaultOptions()
	if err := Inherit(f.nc, "data")(&descriptor); err != nil {
		t.Fatal(err)
	}
	config, ok := Configuration(descriptor.CustomDialer)
	if !ok {
		t.Fatal("original data NC lost the inherited attachment descriptor")
	}
	data, opts, err := Prepare([]nats.Option{nats.Nkey(member, f.keys[1].Sign), nats.Name("tether-cli:attach"), nats.NoReconnect()}, attachDeadline(t), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := data.Close(attachDeadline(t)); err != nil {
			t.Error(err)
		}
	}()
	nc, _, err := natsinbox.ConnectWithin(f.server.ClientURL(), member, opts, attachDeadline(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Finish(nc, attachDeadline(t)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	rows = nil
	mu.Unlock()
	if err := drillobserve.SampleInfrastructure(attachDeadline(t)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := append([]drillobserve.Event(nil), rows...)
	mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("original control plus independent data NC required, got %d", len(got))
	}
	seen := make(map[uint64]string)
	for _, e := range got {
		var row struct {
			Participant string
			CID         uint64
		}
		if err := json.Unmarshal(e.Data, &row); err != nil {
			t.Fatal(err)
		}
		seen[row.CID] = row.Participant
	}
	controlCID, _ := f.nc.GetClientID()
	dataCID, _ := nc.GetClientID()
	if controlCID == dataCID || seen[controlCID] == "" || seen[dataCID] == "" || seen[controlCID] == seen[dataCID] {
		t.Fatal("another NC signed the data attachment")
	}
	if err := data.Close(attachDeadline(t)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	rows = nil
	mu.Unlock()
	if err := drillobserve.SampleInfrastructure(attachDeadline(t)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(rows) != 1 {
		t.Fatal("retired data NC remained in the sample roster")
	}
}

// origin: hot-update S9 plan §4.4 / §4.5; s9-instr4 Request 1
func TestBrokerAttachmentSamplerUsesActualRetainedConnections(t *testing.T) {
	var mu sync.Mutex
	var rows []drillobserve.Event
	previous := drillobserve.Hook
	drillobserve.Hook = func(e drillobserve.Event) { mu.Lock(); defer mu.Unlock(); rows = append(rows, e) }
	defer func() { drillobserve.Hook = previous }()
	f := newBrokerAttachFixture(t, false)
	config := f.config(nil)
	config.Infra.Observe = func(nc *nats.Conn, raw net.Conn, id c.ID, generation uint64, d c.Deadline) (c.Attachment, error) {
		cid, err := nc.GetClientID()
		return c.Attachment{Participant: id, Generation: generation, ServerID: nc.ConnectedServerId(), CID: cid, URL: nc.ConnectedUrl(), HealthGeneration: generation,
			Raw: c.Socket{NetNS: 1, Device: 1, Inode: cid, Cookie: cid, Local: raw.LocalAddr().String(), Remote: raw.RemoteAddr().String(), Owner: attachProcess()}, Ingress: attachProcess()}, err
	}
	var err error
	f.owner, err = NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	f.nc, err = f.owner.Connect(0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		f.nc.Close()
		if err := f.owner.Close(attachDeadline(t)); err != nil {
			t.Error(err)
		}
	}()
	f.arm(t)
	read := func() map[uint64]string {
		t.Helper()
		mu.Lock()
		rows = nil
		mu.Unlock()
		if err := drillobserve.SampleInfrastructure(attachDeadline(t)); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		got := append([]drillobserve.Event(nil), rows...)
		mu.Unlock()
		if len(got) != 3 {
			t.Fatalf("long-lived business, auth and auth-owner business NCs required, got %d", len(got))
		}
		ids := make(map[uint64]string)
		for _, e := range got {
			var row struct {
				CID         uint64
				Participant string
			}
			if err := json.Unmarshal(e.Data, &row); err != nil {
				t.Fatal(err)
			}
			ids[row.CID] = row.Participant
		}
		return ids
	}
	first := read()
	for _, name := range []string{"tetherd", "tetherd-auth", "tetherd-business"} {
		if first[f.cid(t, name)] == "" {
			t.Fatalf("actual connection absent: %s", name)
		}
	}
	// A real auth reconnect recreates P17's private NCs. The logical roster
	// stays fixed while every observed CID changes; no probe dial signs it.
	oldAuth := f.cid(t, "tetherd-auth")
	if err := f.server.DisconnectClientByID(oldAuth); err != nil {
		t.Fatal(err)
	}
	if !testharness.WaitFor(t, 10*time.Second, time.Millisecond, func() bool {
		f.owner.mu.Lock()
		bound := f.owner.bound
		f.owner.mu.Unlock()
		return bound != nil && bound.receipt.AuthCID != oldAuth && f.owner.Admitted() && f.nc.IsConnected()
	}) {
		t.Fatal("ordinary broker reconnect did not recover")
	}
	second := read()
	oldSet, newSet := map[string]bool{}, map[string]bool{}
	for _, id := range first {
		oldSet[id] = true
	}
	for _, id := range second {
		newSet[id] = true
	}
	if len(oldSet) != 3 || len(newSet) != 3 {
		t.Fatal("actual connections shared a participant")
	}
	for id := range oldSet {
		if !newSet[id] {
			t.Fatal("ordinary reconnect changed the original participant roster")
		}
	}
}

// origin: hot-update S9 plan §2.3 / M04; s9-instr4 Request 1
func TestAttachmentObservationRejectsUnprovenAndChangedEvidence(t *testing.T) {
	previous := drillobserve.Hook
	var events []drillobserve.Event
	drillobserve.Hook = func(e drillobserve.Event) { events = append(events, e) }
	defer func() { drillobserve.Hook = previous }()
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	witness := f.client.config.Observe
	for _, defect := range []string{"cid", "health", "ingress", "credentials", "raw-generation"} {
		t.Run(defect, func(t *testing.T) {
			f.client.config.Observe = func(nc *nats.Conn, raw net.Conn, id c.ID, gen uint64, d c.Deadline) (c.Attachment, error) {
				a, err := witness(nc, raw, id, gen, d)
				switch defect {
				case "cid":
					a.CID++
				case "health":
					a.HealthGeneration = 0
				case "ingress":
					a.Ingress = c.Process{}
				case "credentials":
					a.URL = "nats://credential-canary@127.0.0.1:1"
				case "raw-generation":
					f.client.mu.Lock()
					old := f.client.raw
					f.client.raw = &admittedRaw{Conn: old.Conn, owned: old.owned, generation: gen + 1}
					f.client.mu.Unlock()
				}
				return a, err
			}
			events = nil
			if _, err := f.client.Observe(attachDeadline(t)); !errors.Is(err, c.ErrIncomplete) {
				t.Fatalf("unproven evidence accepted: %v", err)
			}
			if len(events) != 0 {
				t.Fatal("unproven evidence crossed custody")
			}
		})
	}
}

func exerciseAttachmentExport(t *testing.T) {
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	var events []drillobserve.Event
	previous := drillobserve.Hook
	drillobserve.Hook = func(e drillobserve.Event) { events = append(events, e) }
	defer func() { drillobserve.Hook = previous }()
	a, err := f.client.Observe(attachDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Kind != "infra-attachment" || events[0].Domain != "infra" {
		t.Fatalf("actual NC observation was not exported: %d events", len(events))
	}
	var row map[string]json.RawMessage
	if err := json.Unmarshal(events[0].Data, &row); err != nil {
		t.Fatal(err)
	}
	if len(row) != 8 || string(row["Participant"]) != `"`+hex.EncodeToString(a.Participant[:])+`"` {
		t.Fatalf("attachment schema or original participant lost: %s", events[0].Data)
	}
	var ingress struct{ ExeSHA256 string }
	if err := json.Unmarshal(row["Ingress"], &ingress); err != nil || ingress.ExeSHA256 != hex.EncodeToString(a.Ingress.ExeSHA256[:]) {
		t.Fatal("executable digest was not projected as hex")
	}
}

// origin: hot-update S9 plan §4.5; s9-instr4 Request 1
func TestAttachmentCloseJoinsAnObservationBorrower(t *testing.T) {
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	entered, release := make(chan struct{}), make(chan struct{})
	witness := f.client.config.Observe
	f.client.config.Observe = func(nc *nats.Conn, raw net.Conn, id c.ID, gen uint64, d c.Deadline) (c.Attachment, error) {
		close(entered)
		<-release
		return witness(nc, raw, id, gen, d)
	}
	observed, closed := make(chan error, 1), make(chan error, 1)
	go func() { _, err := f.client.Observe(attachDeadline(t)); observed <- err }()
	<-entered
	go func() { closed <- f.client.Close(attachDeadline(t)) }()
	var early bool
	select {
	case <-closed:
		early = true
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if !early {
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
	}
	if err := <-observed; err == nil {
		t.Fatal("retired raw observation became current")
	}
	if early {
		t.Fatal("Close released an unjoined independent witness borrower")
	}
}

// origin: hot-update S9 plan §4.5; s9-instr4 Request 1
func TestAttachmentClosedCallbackDoesNotRepeatCallerRetirement(t *testing.T) {
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	// Exercise the real installed callback at the point where caller Close
	// has claimed retirement but its transport/registry join is still pending.
	f.client.mu.Lock()
	f.client.closed = true
	f.client.mu.Unlock()
	f.nc.Opts.ClosedCB(f.nc)
	connected := f.nc.IsConnected()
	f.client.mu.Lock()
	registered := f.client.registered
	f.client.closed = false
	f.client.mu.Unlock()
	if !connected || !registered {
		t.Fatal("closed callback performed a rival transport/registry retirement")
	}
}

// origin: hot-update S9 plan §4.5; s9-instr4 Request 1
func TestAttachmentLateRegistrationCannotReviveRetiredSource(t *testing.T) {
	previous := drillobserve.Hook
	drillobserve.Hook = func(drillobserve.Event) {}
	defer func() { drillobserve.Hook = previous }()
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	entered, release := make(chan struct{}), make(chan struct{})
	rebind := f.client.config.Rebind
	f.client.config.Rebind = func(nc *nats.Conn, d c.Deadline) error {
		err := rebind(nc, d)
		close(entered)
		<-release
		return err
	}
	finished, closed := make(chan error, 1), make(chan error, 1)
	go func() { finished <- f.client.finish(f.nc, attachDeadline(t), true) }()
	<-entered
	go func() { closed <- f.client.Close(attachDeadline(t)) }()
	retiring := testharness.WaitFor(t, time.Second, time.Millisecond, func() bool { f.client.mu.Lock(); defer f.client.mu.Unlock(); return f.client.closed })
	close(release)
	finishErr, closeErr := <-finished, <-closed
	if !retiring || finishErr != nil || closeErr != nil {
		t.Fatalf("normal registration/close race failed: %t %v %v", retiring, finishErr, closeErr)
	}
	// A new original must sample successfully after the old borrower joined.
	// Any revived old source would poison this otherwise complete new roster.
	next := newAttachFixture(t)
	defer next.close(t)
	next.connect(t)
	if err := drillobserve.SampleInfrastructure(attachDeadline(t)); err != nil {
		t.Fatalf("late registration revived a retired source: %v", err)
	}
}

// origin: hot-update S9 plan §2.2 / §2.3; s9-instr4 Request 1
func TestAttachmentReceiptRefusesCredentialsAndUnknownPhysicalFacts(t *testing.T) {
	f := newAttachFixture(t)
	defer f.close(t)
	f.connect(t)
	a, err := f.client.Observe(attachDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	previous := drillobserve.Hook
	var events []drillobserve.Event
	drillobserve.Hook = func(e drillobserve.Event) { events = append(events, e) }
	defer func() { drillobserve.Hook = previous }()
	for _, defect := range []string{"credential", "query", "cookie", "health", "executable"} {
		t.Run(defect, func(t *testing.T) {
			row := a
			switch defect {
			case "credential":
				row.URL = "nats://credential-canary@127.0.0.1:4222"
			case "query":
				row.URL += "?secret=credential-canary"
			case "cookie":
				row.Raw.Cookie = 0
			case "health":
				row.HealthGeneration = 0
			case "executable":
				row.Raw.Owner.ExeSHA256 = c.Digest{}
			}
			if err := drillobserve.RecordAttachment(row); !errors.Is(err, c.ErrIncomplete) {
				t.Fatalf("unproven public receipt was admitted: %v", err)
			}
		})
	}
	if len(events) != 0 {
		t.Fatal("credentials or unknown identities entered custody")
	}
}
