package binaryupgrade

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: hot-update S4 evidence audit (AGENT-SLIM-2 survivor install-running-sha-admission).
func TestInstallRejectsUnrelatedPreviousImageBeforeCandidateExecution(t *testing.T) {
	for _, previous := range []string{"running", "candidate", "unrelated"} {
		t.Run(previous, func(t *testing.T) {
			root := t.TempDir()
			dst := filepath.Join(root, "installed-image")
			receipt := filepath.Join(root, "candidate-executed")
			// The version probe is executable code, even when the later reader
			// selection rejects the install. Its external receipt distinguishes
			// early SHA admission from that redundant, later rejection.
			candidate := []byte("#!/bin/sh\nprintf 'executed' > '" + strings.ReplaceAll(receipt, "'", "'\\''") + "'\n" +
				"printf '%s\\n' '" + proto.VersionLine() + "' '" + proto.ImageABILine() + "'\n")
			running := []byte("the exporter image")
			disk := running
			switch previous {
			case "candidate":
				disk = candidate
			case "unrelated":
				disk = []byte("an image installed by an unrelated operator")
			}
			if err := os.WriteFile(dst, disk, 0o700); err != nil {
				t.Fatal(err)
			}
			inputs := InstallInputs{
				RunningSHA: installAdmissionSHA(running), Now: time.Now,
				Generation: func() uint64 { return 1 }, Logger: slog.Default(),
				CheckCandidate: func(agentimage.Capabilities) error { return nil },
			}
			version, err := (Store{}).Install(installAdmissionArchive(t, candidate), dst, inputs)
			if previous == "unrelated" {
				if !errors.Is(err, agentimage.ErrRunningImageDiffers) || version != "" {
					t.Fatalf("unrelated previous image was admitted: version=%q err=%v", version, err)
				}
				if _, err := os.Stat(receipt); !os.IsNotExist(err) {
					t.Errorf("candidate executed before rejecting an unrelated previous image: receipt stat=%v", err)
				}
				if got, err := os.ReadFile(dst); err != nil || !bytes.Equal(got, disk) {
					t.Errorf("refused install changed the installed image: bytes=%q err=%v", got, err)
				}
				for _, path := range []string{PrevPath(dst), MarkerPath(dst)} {
					if _, err := os.Stat(path); !os.IsNotExist(err) {
						t.Errorf("refused install left transaction state at %s: %v", path, err)
					}
				}
				return
			}
			// Both legal previous images must really execute the same fixture.
			// Otherwise a broken probe would make the negative receipt vacuous.
			if err != nil || !proto.SameRelease(version, proto.ReleaseVersion) {
				t.Fatalf("rollback-capable previous image refused: version=%q err=%v", version, err)
			}
			if got, err := os.ReadFile(receipt); err != nil || string(got) != "executed" {
				t.Fatalf("admitted candidate did not execute: receipt=%q err=%v", got, err)
			}
			if got, err := os.ReadFile(dst); err != nil || !bytes.Equal(got, candidate) {
				t.Fatalf("admitted candidate was not installed: bytes=%q err=%v", got, err)
			}
			if got, err := os.ReadFile(PrevPath(dst)); err != nil || !bytes.Equal(got, disk) {
				t.Fatalf("rollback slot differs from admitted previous image: bytes=%q err=%v", got, err)
			}
			marker, err := ReadMarker(MarkerPath(dst))
			if err != nil || marker == nil || marker.State != StatePending || marker.PrevSHA != installAdmissionSHA(disk) {
				t.Fatalf("admitted install lacks a matching rollback transaction: marker=%+v err=%v", marker, err)
			}
		})
	}
}

func installAdmissionSHA(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func installAdmissionArchive(t *testing.T, candidate []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "tether", Mode: 0o700, Size: int64(len(candidate))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(candidate); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
