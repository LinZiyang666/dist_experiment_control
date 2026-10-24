package proto

import (
	"reflect"
	"strings"
	"testing"
)

// origin: hot-update S5 plan §5.2 / L1. These lines are consumed by old
// deployed installers; an additive domain must not move the first/last seam.
// origin: hot-update S6 plan §9.1 / T46
func TestFlowSSVersionKeepsReaderDomainsAndABIEpoch(t *testing.T) {
	if ProtoVersion != 2 || ImageABIVersion != 1 {
		t.Fatal("additive flow/SS domains changed a protocol epoch")
	}
	// origin: hot-update S7 plan §6.2 / P01 — transfer-v1 is appended after
	// broker-image-v1; the first/last seam and every earlier domain stay put.
	// origin: hot-update S8 plan §6.1 / §11.2 P13 P17; S8 BROKER-EXPORTER R-11 —
	// the S8 reader (upgrade-cut-v1 domain, three adapter features) is advertised
	// once every exporter exists; the ABI line stays last.
	if got := ImageVersionLines(); !reflect.DeepEqual(got, []string{"image-domains exec-v1 pty-v1 flow-v1 ss-v1 broker-image-v1 transfer-v1 upgrade-cut-v1", "image-read-epochs 1",
		"image-features upgrade-tx-v1 authority-incarnation-v1 upgrade-cut-v1", "image-abi v1"}) {
		t.Fatalf("reader declaration/order changed: %v", got)
	}
	fields := strings.Fields(VersionLine())
	if len(fields) != 4 || fields[0] != "tether" || !SameRelease(fields[1], ReleaseVersion) || fields[2] != "(proto" || fields[3] != "v2)" {
		t.Fatal("N-1 installer release parsing seam changed")
	}
	// Release identity ignores the optional v, while this output seam still
	// preserves the spelling declared by the build. Together these checks keep
	// the original exact output assertion without comparing release strings raw.
	if strings.HasPrefix(fields[1], "v") != strings.HasPrefix(ReleaseVersion, "v") {
		t.Fatal("version output changed the declared release prefix")
	}
}
