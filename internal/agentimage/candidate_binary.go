// candidate_binary.go owns candidate extraction and the previous-binary copy
// fallback. The agent retains transaction ordering and durability hooks.
// origin: hot-update S4 plan §7.1 / §10.
package agentimage

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

// copyFile is the prev-slot fallback for filesystems that refuse hardlinks.
func CopyBinary(src, dst string, syncFile func(*os.File) error) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	_, cpErr := io.Copy(out, in)
	chmodErr := out.Chmod(0o755) // fd-chmod before the fsync (F6): OpenFile's mode is umask-clipped
	syncErr := syncFile(out)     // S13: the prev slot must survive a power cut — it IS the rollback
	closeErr := out.Close()
	if cpErr != nil {
		return cpErr
	}
	if chmodErr != nil {
		return chmodErr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// extractTetherBinary scans the gzipped tar for a single file
// entry literally named "tether" and writes its contents to
// outPath. Refuses any other path. Refuses files larger than
// upgradeMaxTarballBytes (defense in depth — the network read is
// already capped, but a maliciously-crafted small tar could
// declare a huge file size).
//
// outPath is opened with O_EXCL | O_NOFOLLOW so a local attacker
// can't race a symlink swap into the tmp dir between MkdirTemp
// and our open (audit shard 02 F2). MkdirTemp uses 0700 by
// default on Linux but a non-root local who already owns the
// containing directory could pre-plant.
func ExtractBinary(tarball []byte, outPath string, maxBytes int64, syncFile func(*os.File) error) error {
	gz, err := gzip.NewReader(bytes.NewReader(tarball))
	if err != nil {
		return fmt.Errorf("gzip open: %w", err)
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("tarball missing tether binary entry")
		}
		if err != nil {
			return fmt.Errorf("tar read: %w", err)
		}
		// Strict allowlist: the literal name "tether" and nothing
		// else. base()-ing or filepath.Clean()-ing first would
		// silently accept "./tether", "subdir/tether", or worse,
		// "../something".
		if hdr.Name != "tether" {
			continue
		}
		// archive/tar collapses the legacy TypeRegA into TypeReg
		// since Go 1.11; one check suffices.
		if hdr.Typeflag != tar.TypeReg {
			return fmt.Errorf("tether entry has wrong type: %v", hdr.Typeflag)
		}
		if hdr.Size > maxBytes {
			return fmt.Errorf("tether entry too large: %d bytes", hdr.Size)
		}
		f, err := os.OpenFile(outPath,
			os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
		if err != nil {
			return fmt.Errorf("create binary: %w", err)
		}
		_, err = io.CopyN(f, tr, hdr.Size)
		// chmod on the SAME fd, BEFORE the fsync (external review F6): a
		// chmod after the sync would leave the executable bit unpersisted —
		// after a power cut the new dst exists but cannot exec, which no
		// rollback path detects.
		chmodErr := f.Chmod(0o755)
		// fsync before the later rename onto the live binary path (internal
		// review S13): a post-rename power cut must not leave a truncated
		// dst — that is the one artifact the rollback machinery cannot fix.
		syncErr := syncFile(f)
		closeErr := f.Close()
		if err != nil {
			return fmt.Errorf("write binary: %w", err)
		}
		if chmodErr != nil {
			return fmt.Errorf("chmod binary: %w", chmodErr)
		}
		if syncErr != nil {
			return fmt.Errorf("sync binary: %w", syncErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close binary: %w", closeErr)
		}
		return nil
	}
}
