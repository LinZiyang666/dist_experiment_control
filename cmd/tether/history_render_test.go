package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// Real renderer and JSON decoding; no broker or JetStream fixture.
// The oversized integer is a decoding discriminator, not an admitted transfer size.
// origin: control-connection-recovery, archived tier-B audit byte-count display
func TestAuditTransferRendersExactByteCounts(t *testing.T) {
	for _, count := range []string{"0", "66549440", "9007199254740993", "9223372036854775807"} {
		t.Run(count, func(t *testing.T) {
			data := []byte(fmt.Sprintf(`{"kind":"complete","verb":"push","session":"lab","node":"agent","tier":"b","bytes":%s,"path":"/tmp/payload"}`, count))
			var out bytes.Buffer
			printAuditEntry(&out, "tether.v1.s.lab.audit.transfer", data)
			if want := "bytes=" + count + " "; !strings.Contains(out.String(), want) {
				t.Fatalf("history changed the exact integer %s: %q", count, out.String())
			}
			if !strings.Contains(out.String(), "XFER") || !strings.Contains(out.String(), "kind=complete") {
				t.Fatalf("transfer record was not rendered: %q", out.String())
			}
		})
	}
}

// A UseNumber decoder must retain Unmarshal's whole-input validation. A second
// value or trailing junk is malformed audit data, even when the first value is valid.
func TestAuditEntryRejectsTrailingJSON(t *testing.T) {
	const record = `{"kind":"complete","bytes":66549440}`
	for _, suffix := range []string{"{}", " true", " trailing-junk", "\n[]"} {
		t.Run(suffix, func(t *testing.T) {
			var out bytes.Buffer
			printAuditEntry(&out, "tether.v1.s.lab.audit.transfer", []byte(record+suffix))
			if !strings.HasPrefix(out.String(), "[malformed audit:") {
				t.Fatalf("trailing JSON was silently accepted: %q", out.String())
			}
			if strings.Contains(out.String(), "  XFER  ") {
				t.Fatalf("malformed audit was also reported as a transfer: %q", out.String())
			}
		})
	}
	var out bytes.Buffer
	printAuditEntry(&out, "tether.v1.s.lab.audit.transfer", []byte(record+" \n\t"))
	if !strings.Contains(out.String(), "  XFER  ") || strings.Contains(out.String(), "[malformed audit:") {
		t.Fatalf("ordinary trailing whitespace was rejected: %q", out.String())
	}
}
