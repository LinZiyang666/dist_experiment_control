package brokerruntime

import (
	"bufio"
	"os"
	"strconv"
)

// ResidentSetBytes returns the process resident-set size in bytes, parsed from /proc/self/statm
// (field 2 = resident pages) times the page size. Returns -1 when not measurable. This is the actual
// RSS, not a proxy for anything else.
// origin: moved verbatim from internal/broker/runtime_introspect.go (residentSetBytes) to keep the
// Broker's raw code from growing (S8 BROKER-EXPORTER R-11, P15 per-package net-zero)
func ResidentSetBytes() int64 {
	f, err := os.Open("/proc/self/statm")
	if err != nil {
		return -1
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		return -1
	}
	// Format: "size resident shared text lib data dt" (all in pages). We want field index 1.
	fields := splitFields(sc.Text())
	if len(fields) < 2 {
		return -1
	}
	residentPages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return -1
	}
	return residentPages * int64(os.Getpagesize())
}

// splitFields splits on ASCII spaces without allocating a regexp; statm is a single space-separated
// line so this is sufficient (strings.Fields would also work — kept explicit to avoid pulling a
// larger import for one call).
func splitFields(s string) []string {
	var out []string
	start := -1
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' {
			if start >= 0 {
				out = append(out, s[start:i])
				start = -1
			}
		} else if start < 0 {
			start = i
		}
	}
	if start >= 0 {
		out = append(out, s[start:])
	}
	return out
}
