package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// origin: cluster_add_connect_external_review_test.go (renamed in B6)
func TestClusterAddConnectRetriesOnlyTransientAuthWindow(t *testing.T) {
	authErr := errors.New("nats: Authorization Violation")
	want := &nats.Conn{}
	attempts := 0
	got, err := retryClusterAddConnect(context.Background(), time.Second, time.Millisecond, func() (*nats.Conn, error) {
		attempts++
		if attempts < 3 {
			return nil, authErr
		}
		return want, nil
	})
	if err != nil || got != want || attempts != 3 {
		t.Fatalf("transient auth window did not recover: got=%p err=%v attempts=%d", got, err, attempts)
	}

	networkErr := errors.New("connection refused")
	attempts = 0
	_, err = retryClusterAddConnect(context.Background(), time.Second, time.Millisecond, func() (*nats.Conn, error) {
		attempts++
		return nil, networkErr
	})
	if !errors.Is(err, networkErr) || attempts != 1 {
		t.Fatalf("non-auth connect error was retried/lost: err=%v attempts=%d", err, attempts)
	}
}

func TestClusterAddConnectAuthRetryIsContextBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts := 0
	_, err := retryClusterAddConnect(ctx, time.Minute, time.Minute, func() (*nats.Conn, error) {
		attempts++
		return nil, errors.New("nats: Authorization Violation")
	})
	if !errors.Is(err, context.Canceled) || attempts != 1 {
		t.Fatalf("canceled retry did not stop immediately: err=%v attempts=%d", err, attempts)
	}
}
