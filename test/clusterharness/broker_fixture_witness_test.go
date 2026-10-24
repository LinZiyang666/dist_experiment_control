package clusterharness

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
)

type brokerWitnessReporter struct{ errors []string }

func (*brokerWitnessReporter) Helper() {}
func (r *brokerWitnessReporter) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

type brokerWitnessCloser struct {
	bytes.Buffer
	err    error
	closed int
}

func (c *brokerWitnessCloser) Close() error {
	c.closed++
	return c.err
}

// A changed key supplier or failed witness close must fail the fixture rather
// than panic during setup or certify a successful resource cleanup.
// origin: hot-update S6 plan §2.1 / §9.3 / §12.1 P26
func TestBrokerFixtureWitnessRejectsInvalidKeyAndCloseFailure(t *testing.T) {
	for _, key := range []any{nil, "not a signer", struct{}{}} {
		if _, err := brokerFixturePublicKey(key); err == nil {
			t.Fatal("non-signer certificate key accepted")
		}
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := brokerFixturePublicKey(private)
	if err != nil {
		t.Fatal(err)
	}
	key, ok := actual.(ed25519.PublicKey)
	if !ok || !bytes.Equal(key, public) {
		t.Fatal("certificate witness replaced the supplied public key")
	}
	for _, closeErr := range []error{nil, io.EOF, net.ErrClosed, os.ErrClosed, fmt.Errorf("wrapped: %w", io.EOF), fmt.Errorf("wrapped: %w", net.ErrClosed), fmt.Errorf("wrapped: %w", os.ErrClosed), errors.New("unexpected close failure")} {
		reporter := &brokerWitnessReporter{}
		witness := &brokerWitnessCloser{err: closeErr}
		closeBrokerFixtureWitness(reporter, witness)
		if witness.closed != 1 {
			t.Fatal("witness was not closed exactly once")
		}
		unexpected := closeErr != nil && !errors.Is(closeErr, io.EOF) && !errors.Is(closeErr, net.ErrClosed) && !errors.Is(closeErr, os.ErrClosed)
		if (len(reporter.errors) == 1) != unexpected {
			t.Fatalf("close error misclassified: %v: %v", closeErr, reporter.errors)
		}
	}
}
