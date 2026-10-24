package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// origin: hot-update S9 plan §2.3 / §9.1; S9 INSTRUMENTS-3.
func TestPhysicalFileObservationHashesHeldPublicBytes(t *testing.T) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return
	}
	data := []byte("original public flow bytes\x00\n")
	path := filepath.Join(t.TempDir(), "received")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	result, err := observe(request{Operation: "file", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	row, ok := result.(map[string]any)
	if !ok || row["length"] != int64(len(data)) || row["sha256"] != fmt.Sprintf("%x", sha256.Sum256(data)) {
		t.Fatalf("actual file digest differs: %v", result)
	}
}

// origin: hot-update S9 plan §2.3 / §9.4; S9 INSTRUMENTS-3.
func TestPhysicalRequestRefusesUnknownTrailingAndOversizedInputs(t *testing.T) {
	for _, raw := range []string{`{"operation":"census","private_key":"forbidden"}`, `{} {}`, `{"unit":"` + strings.Repeat("a", 65537) + `"}`} {
		if _, err := decode([]byte(raw)); err == nil {
			t.Fatal("accepted malformed observation input")
		}
	}
	if _, err := decode([]byte(`{"operation":"process","pid":1}`)); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S9 plan §2.3 / §9.1; S9 INSTRUMENTS-3.
func TestPhysicalToolRefusesUnknownOperationsWithoutSuccessOutput(t *testing.T) {
	var output bytes.Buffer
	for _, args := range [][]string{{}, {"--request", `{"operation":"repair"}`}, {"--request", `{"private_key":"forbidden"}`}} {
		if rc := run(args, &output); rc != 2 || output.Len() != 0 {
			t.Fatalf("invalid tool invocation returned rc=%d output=%q", rc, output.String())
		}
	}
}

// origin: hot-update S9 plan §2.3 / §9.4; S9 INSTRUMENTS-3.
func TestPhysicalFileObservationRefusesRelativeMissingAndNonregularInputs(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.WriteFile("public", []byte("public"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"public", filepath.Join(t.TempDir(), "absent"), t.TempDir()} {
		if _, err := observe(request{Operation: "file", Path: path}); err == nil {
			t.Fatal("accepted invalid public file")
		}
	}
}
