package tunnel

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestExternalReviewForgetSessionInvalidatesInFlightRegister(t *testing.T) {
	controlPort := findFreePort(t)
	publicPort := findFreePort(t)

	authorized := make(chan struct{})
	releaseLookup := make(chan struct{})
	lookup := func(_, _ string, _ int, _ string) error {
		// Models a lookup that read the still-existing allocation immediately
		// before session deletion, then returns its already-authorized result.
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

	// finalizeSessionRm calls ForgetSession after deleting the DB rows. The
	// lookup above has already authorized from the pre-delete state, so DB
	// deletion alone cannot fence this in-flight installation.
	srv.ForgetSession("lab")
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
				t.Fatal("deleted session's in-flight REGISTER installed a public listener")
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight REGISTER did not finish")
	}
}
