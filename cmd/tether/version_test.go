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
	if len(lines) != 3 {
		t.Fatalf("expected exactly 3 output lines, got %d:\n%q", len(lines), out)
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
}
