//go:build !linux

package cluster

import "errors"

// exchangedir_other.go — non-Linux build of the offline raft-store swap helper.
//
// Round-5 S5-01: `unix.Renameat2`/`unix.RENAME_EXCHANGE` exist ONLY in the Linux build of
// `golang.org/x/sys/unix`, so referencing them unconditionally broke `GOOS=darwin go build ./cmd/tether`
// outright — and `build/goreleaser.yaml` ships darwin/amd64+arm64, so the whole release pipeline could
// produce no artifact. tether is one binary for all three roles: an operator's `ctl` on macOS must build
// and run even though the offline broker surgery it guards is only ever performed on a Linux broker host.
//
// There is no atomic directory exchange here; the caller falls back to a non-atomic swap and says so.

var errExchangeUnsupported = errors.New("cluster: atomic directory exchange (RENAME_EXCHANGE) is Linux-only")

// exchangeDirs always reports unsupported off Linux, so the caller takes the documented non-atomic path.
func exchangeDirs(_, _ string) error { return errExchangeUnsupported }
