package tunnel

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestExternalReviewCloseSessionInvalidatesInFlightRegister(t *testing.T) {
	controlPort := findFreePort(t)
	publicPort := findFreePort(t)

	authorized := make(chan struct{})
	releaseLookup := make(chan struct{})
	lookup := func(_, _ string, _ int, _ string) error {
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

	// The REGISTER has the session id and has passed the authoritative lookup,
	// but no serverSession has been installed yet. Session-level OFF must still
	// invalidate it.
	srv.CloseSession("lab")
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
				t.Fatal("public proxy listener appeared after CloseSession returned")
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight REGISTER did not finish")
	}
}

// Self-review: ForgetSession prunes the per-session kill-generation entry so
// killGenSession does not grow unbounded across session lifecycles.
func TestForgetSessionPrunesKillGen(t *testing.T) {
	srv := NewServer("127.0.0.1:0", "127.0.0.1", func(_, _ string, _ int, _ string) error { return nil }, silentLog())

	srv.CloseSession("lab")   // bumps killGenSession["lab"]
	srv.CloseSession("other") // bumps killGenSession["other"]
	srv.mu.Lock()
	_, hadLab := srv.killGenSession["lab"]
	srv.mu.Unlock()
	if !hadLab {
		t.Fatal("precondition: CloseSession should record a session kill generation")
	}

	srv.ForgetSession("lab")
	srv.mu.Lock()
	_, stillLab := srv.killGenSession["lab"]
	_, stillOther := srv.killGenSession["other"]
	srv.mu.Unlock()
	if stillLab {
		t.Fatal("ForgetSession did not prune killGenSession[lab]")
	}
	if !stillOther {
		t.Fatal("ForgetSession pruned an unrelated session's entry")
	}
}
