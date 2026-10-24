package main

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

func TestReleaseVersionDefaultNotEmpty(t *testing.T) {
	if proto.ReleaseVersion == "" {
		t.Fatal("proto.ReleaseVersion default must not be empty")
	}
}

func TestProtoVersionPositive(t *testing.T) {
	if proto.ProtoVersion < 1 {
		t.Fatalf("proto.ProtoVersion must be ≥ 1, got %d", proto.ProtoVersion)
	}
}

func TestVersionCommand(t *testing.T) {
	cmd := newRootCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"version"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}

	out := buf.String()
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	// origin: hot-update S8 plan §6.1 / §11.2 P19 — the reader-epoch line joins
	// the middle; no image-features line is advertised until the agent and broker
	// runtime exporters that would honor it exist (P13/P17 coordination).
	// origin: S8 BROKER-EXPORTER R-11 — both exporters now exist: the features
	// line joins before the final ABI line (7 lines).
	if len(lines) != 7 {
		t.Fatalf("expected exactly 7 output lines, got %d:\n%q", len(lines), out)
	}

	wantLine1 := fmt.Sprintf("tether %s (proto v%d)", proto.ReleaseVersion, proto.ProtoVersion)
	if lines[0] != wantLine1 {
		t.Errorf("line 1: got %q, want %q", lines[0], wantLine1)
	}

	wantLine2 := fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
	if lines[1] != wantLine2 {
		t.Errorf("line 2: got %q, want %q", lines[1], wantLine2)
	}

	if lines[2] != runtime.Version() {
		t.Errorf("line 3: got %q, want %q (runtime.Version())", lines[2], runtime.Version())
	}
	// origin: hot-update S4 plan §5.2 — the domain line precedes the final ABI
	// line; a candidate's reader capability is read from it before the flip.
	// origin: hot-update S5 plan §5.2 / L1.
	// Pin the external reader contract independently of the shared emitter:
	// dropping a domain from both producer and helper must still turn red.
	// origin: hot-update S7 plan §7 — transfer-v1 joins the reader domains.
	// origin: S8 BROKER-EXPORTER R-11 — and the required S8 adjunct domain.
	const wantDomains = "image-domains exec-v1 pty-v1 flow-v1 ss-v1 broker-image-v1 transfer-v1 upgrade-cut-v1"
	if want := wantDomains; lines[3] != want {
		t.Errorf("line 4: got %q, want %q", lines[3], want)
	}
	// origin: hot-update S8 plan §6.1 — the explicit reader-epoch set. A reader
	// without this line can read only its own ABI epoch, never an adjacent one.
	if want := "image-read-epochs 1"; lines[4] != want {
		t.Errorf("line 5: got %q, want %q", lines[4], want)
	}
	// origin: hot-update S8 plan §6.1 / §11.2 P17; S8 BROKER-EXPORTER R-11 — the
	// three callable S8 adapters, advertised only now that both exporters exist.
	if want := "image-features upgrade-tx-v1 authority-incarnation-v1 upgrade-cut-v1"; lines[5] != want {
		t.Errorf("line 6: got %q, want %q", lines[5], want)
	}
	// origin: hot-update S3 plan 修订 1 — the image-ABI line is last and
	// carries proto.ImageABIVersion; an upgrading agent reads it before the flip.
	if want := fmt.Sprintf("image-abi v%d", proto.ImageABIVersion); lines[6] != want {
		t.Errorf("line 7: got %q, want %q", lines[6], want)
	}
}
