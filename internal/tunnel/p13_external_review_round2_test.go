package tunnel

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"
)

// A proxy-off can race with a REGISTER that already passed the authoritative
// token lookup but has not inserted its serverSession yet. CloseProxy must
// invalidate that in-flight authorization; otherwise the public listener can
// appear after the kill switch has returned.
func TestExternalReviewCloseProxyInvalidatesInFlightRegister(t *testing.T) {
	controlPort := findFreePort(t)
	publicPort := findFreePort(t)

	authorized := make(chan struct{})
	releaseLookup := make(chan struct{})
	lookup := func(_, _ string, _ int, _ string, _ int64) error {
		close(authorized)
		<-releaseLookup
		return nil
	}

	srv := NewServer(
		net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
		"127.0.0.1",
		lookup,
		silentLog(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}

	cli := NewClient(
		net.JoinHostPort("127.0.0.1", strconv.Itoa(controlPort)),
		"lab",
		"lab-1",
		func(int) (int, error) { return 1, nil },
		silentLog(),
	)
	cli.Start(ctx)

	openDone := make(chan error, 1)
	go func() {
		openDone <- cli.Open(publicPort, 1, "token")
	}()

	select {
	case <-authorized:
	case <-time.After(2 * time.Second):
		t.Fatal("REGISTER never reached token authorization")
	}

	// Models proxy-off after the DB lookup authorized this token, but before
	// handleAgent installed the session in s.sessions.
	srv.CloseProxy(publicPort)
	close(releaseLookup)

	select {
	case err := <-openDone:
		if err == nil {
			conn, dialErr := net.DialTimeout(
				"tcp",
				net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort)),
				500*time.Millisecond,
			)
			if dialErr == nil {
				_ = conn.Close()
				t.Fatal("public proxy listener appeared after CloseProxy returned")
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight REGISTER did not finish")
	}
}
