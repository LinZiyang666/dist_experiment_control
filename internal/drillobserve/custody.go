// Package drillobserve owns private drill receipts below every data-plane owner.
// It has no control-plane interface and accepts custody only at process start.
package drillobserve

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/netip"
	"strings"

	"github.com/LinZiyang666/tether/internal/drillobserve/receipt"
)

const EnableEnv = "TETHER_TEST_DRILL_OBSERVER"
const CustodyEnv = "TETHER_TEST_DRILL_CUSTODY_FD"
const MaxData = receipt.MaxData
const maxBatch = receipt.MaxBatch
const queueDepth = 256

// Hook is immutable after startup. Call sites check nil before constructing an
// event: an ordinary release executes no observation allocation or syscall.
// The installed hook copies into a bounded queue; it never writes under a
// product lock. Overflow poisons custody rather than changing product IO.
// origin: hot-update S5 plan §6.2 / §9.1 / DRILL-OBSERVER
var Hook func(Event)

// All domains share the same pure receipt envelope and redaction methods.
type Event = receipt.Event

func encodeEvents(events []Event) []byte { return receipt.EncodeEvents(events) }

// StripEnvironment also rejects user-supplied overrides of the custody keys.
// Startup unsets the variables, but explicit workload environments bypass that.
func StripEnvironment(env []string) []string {
	if env == nil {
		return nil
	}
	out := make([]string, 0, len(env))
	for _, e := range env {
		if !strings.HasPrefix(e, EnableEnv+"=") && !strings.HasPrefix(e, CustodyEnv+"=") {
			out = append(out, e)
		}
	}
	return out
}

// The private vocabulary is closed in both custody and its Python readers.
// Keep upstream transfer/store kinds alongside S5; they share this channel.
// origin: hot-update S10 plan §5.3 / P06
func validCustodyVocabulary(kind, domain string) bool {
	switch kind {
	case InfrastructureAttachmentKind:
		return domain == "infra"
	case InfrastructurePrefixKind:
		return domain == "store"
	case RuntimeSampleKind:
		return domain == "agent"
	case TransferEventKind, TransferPoisonKind:
		return domain == "transfer"
	case "native-exit":
		return domain == "exec"
	case "store-before-replay":
		return domain == "store"
	case "ss-seal", "ss-write", "ss-state", "ss-binding", "ss-revoke-mark", "ss-closed", "ss-grant",
		"flow-state", "flow-retirement", "carrier-negotiated", "carrier-prefix", "resume", "prepare",
		"final-cut", "domain-cut", "domain-file", "reaper", "register-applied", "commit-write-failed",
		"mark-ready", "watchdog-current", "domain-live", "flow-socket", "legacy-binding", "flow-capacity":
		switch domain {
		case "ss", "flow", "tunnel", "agent", "exec", "pty", "proxy":
			return true
		}
	}
	return false
}

// FinishNativeExit retains the actual result until the private observer's
// final sealed drain is released. The platform owner keeps this inert without
// authenticated startup custody.
// origin: S7 internal review rv-drill F1
func FinishNativeExit(code int) int { finishNativeExit(code); return code }

// SocketIdentity correlates independent kernel tuple observations without
// placing private addresses in public receipts. Call only behind a nil hook.
// origin: hot-update S5 plan §9.1 / DRILL-OBSERVER
func SocketIdentity(local, remote netip.AddrPort) [32]byte {
	family := "tcp"
	if local.Addr().Is6() {
		family = "tcp6"
	}
	parts := []string{family}
	for _, address := range []netip.AddrPort{local, remote} {
		raw := address.Addr().Unmap().AsSlice()
		var text strings.Builder
		for i := 0; i < len(raw); i += 4 {
			fmt.Fprintf(&text, "%08X", binary.LittleEndian.Uint32(raw[i:i+4]))
		}
		value := fmt.Sprintf("%s:%04X", text.String(), address.Port())
		parts = append(parts, fmt.Sprintf("%x", sha256.Sum256([]byte(value))))
	}
	return sha256.Sum256([]byte(strings.Join(parts, "\x00")))
}
