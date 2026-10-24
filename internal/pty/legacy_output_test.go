package pty

import (
	"bytes"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
func TestLegacyOutputFlushesAfterIdleWithoutClosingSource(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLegacyOutputAfterIdle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "legacy output", beforeG)
	testharness.AssertNoFDLeak(t, "legacy output", beforeFD, 0)
}

func exerciseLegacyOutputAfterIdle(t *testing.T) {
	t.Helper()
	source, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	chunks := make(chan []byte, 4)
	done := make(chan struct{})
	go func() { defer close(done); PumpLegacyOutput(source, func(p []byte) { chunks <- p }) }()
	t.Cleanup(func() {
		_ = writer.Close()
		_ = source.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("legacy output pump did not join")
		}
	})
	// Exercise an empty timer tick before output; a one-shot timer would leave the source's next bytes queued until EOF.
	idle := time.NewTimer(3 * LegacyFlushInterval)
	<-idle.C
	want := []byte("interactive echo after idle")
	if _, err := writer.Write(want); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-chunks:
		if !bytes.Equal(got, want) {
			t.Fatalf("flush=%q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("small interactive output remained queued while the source was open")
	}
	// A later flush must own its bytes: the source read buffer is reused after each read.
	second := []byte("next echo")
	if _, err := writer.Write(second); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-chunks:
		if !bytes.Equal(got, second) {
			t.Fatalf("second flush=%q, want %q", got, second)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("later interactive output did not flush")
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("source EOF did not stop the legacy output pump")
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
}
