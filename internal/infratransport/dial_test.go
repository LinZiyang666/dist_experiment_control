package infratransport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats.go"
)

type pipeDialer struct{ raw net.Conn }

func (d pipeDialer) Dial(string, string) (net.Conn, error) { return nil, errors.New("legacy dial") }
func (d pipeDialer) DialWithin(string, string, controlbudget.Deadline, infracontract.Stop) (net.Conn, error) {
	return d.raw, nil
}

// origin: hot-update S9 plan §4.5 / T33
func TestPlannedMoveJoinsBlockedWebSocketWrite(t *testing.T) {
	for _, scheme := range []string{"ws", "wss"} {
		t.Run(scheme, func(t *testing.T) {
			raw, peer := net.Pipe()
			defer func() { _ = peer.Close() }() // net.Pipe's Close cannot fail
			tr, err := New(1, transportDeadline(t), nil, pipeDialer{raw})
			if err != nil {
				t.Fatal(err)
			}
			defer tr.Interrupt()
			connectDone := make(chan struct{})
			var connectErr error
			go func() {
				defer close(connectDone)
				_, connectErr = tr.Connect(scheme+"://blocked.invalid:443", func() []nats.Option { return []nats.Option{nats.NoReconnect()} })
			}()
			// net.Pipe has no write buffer: read one byte proves the real TLS
			// ClientHello or HTTP req.Write entered, then stop draining the peer.
			if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			b := make([]byte, 1)
			if _, err := peer.Read(b); err != nil {
				t.Fatal(err)
			}
			if scheme == "ws" && b[0] != 'G' {
				t.Fatal("websocket upgrade request was not written")
			}
			if scheme == "wss" && b[0] != 22 {
				t.Fatal("TLS ClientHello was not written")
			}
			select {
			case <-connectDone:
				t.Fatal("handshake was not blocked in raw Write")
			case <-time.After(40 * time.Millisecond):
			}
			started := time.Now()
			if err := Move(tr, nil, transportDeadline(t), nil); err != nil {
				t.Fatal(err)
			}
			select {
			case <-connectDone:
			// Connect's deferred worker completion precedes this caller's
			// deferred channel close; wait for the caller's receipt too.
			case <-time.After(time.Second):
				t.Fatal("move returned before req.Write/handshake worker joined")
			}
			if connectErr == nil {
				t.Fatal("interrupted handshake succeeded")
			}
			if time.Since(started) >= upgradebudget.RequestLatency {
				t.Fatal("blocked Write exceeded planned move budget")
			}
			// origin: hot-update S9 plan §4.5 / T33 / M16 — natural handshake expiry cannot stand in for raw interruption.
			if time.Since(started) >= time.Second {
				t.Fatal("move waited for req.Write/TLS expiry instead of closing the raw socket")
			}
		})
	}
	t.Run("connected_deadline_is_detached", func(t *testing.T) {
		s := newTransportServer(t)
		deadline, err := controlbudget.After(200 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		tr, err := New(1, deadline, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tr.Interrupt()
		nc, err := tr.Connect(s.ClientURL(), func() []nats.Option { return []nats.Option{nats.NoReconnect()} })
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		time.Sleep(250 * time.Millisecond)
		if err := nc.Publish("after.admission", []byte("still live")); err != nil {
			t.Fatal(err)
		}
		if err := nc.FlushTimeout(time.Second); err != nil {
			t.Fatalf("admission deadline killed a live NC: %v", err)
		}
	})
	t.Run("deadline_cannot_be_cleared", func(t *testing.T) {
		raw, peer := net.Pipe()
		defer func() { _ = peer.Close() }() // net.Pipe's Close cannot fail
		deadline, err := controlbudget.After(80 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		tr, err := New(1, deadline, nil, pipeDialer{raw})
		if err != nil {
			t.Fatal(err)
		}
		defer tr.Interrupt()
		c, err := tr.Dial("tcp", "deadline.invalid:4222")
		if err != nil {
			t.Fatal(err)
		}
		for _, set := range []func(time.Time) error{c.SetDeadline, c.SetReadDeadline, c.SetWriteDeadline} {
			if err := set(time.Time{}); err != nil {
				t.Fatal(err)
			}
		}
		started := time.Now()
		watch := time.AfterFunc(250*time.Millisecond, func() { _ = peer.Close() })
		defer watch.Stop()
		_, err = c.Write([]byte("blocked"))
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatalf("cleared sticky deadline: %v", err)
		}
		if time.Since(started) > time.Second {
			t.Fatal("raw deadline was renewed")
		}
	})
	t.Run("stop_is_not_business_lifetime", func(t *testing.T) {
		stop := make(chan struct{})
		tr, err := New(1, transportDeadline(t), stop, nil)
		if err != nil {
			t.Fatal(err)
		}
		close(stop)
		tr.Interrupt()
		if _, err := tr.Dial("tcp", "127.0.0.1:1"); !errors.Is(err, context.Canceled) {
			t.Fatalf("retired dial: %v", err)
		}
	})
}
