package agentimage

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradecompat"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func readerOutput(lines ...string) string {
	return proto.VersionLine() + "\n" + strings.Join(lines, "\n") + "\n"
}

// The production parser and admission share one reader decision. Before this,
// image-read-epochs was an ignored line: a candidate that declared it could not
// read the running epoch still passed CheckDomains with ABI 1.
// origin: hot-update S8 plan §6.1 / §11.2 P17; S8 review rv-n1 F5
func TestRuntimeImageReaderDeclarationIsEnforced(t *testing.T) {
	for _, declaration := range []string{"2", "0", "1 1", "unknown", "01", "-1", "1 2 2", "18446744073709551616", ""} {
		t.Run("epochs="+declaration, func(t *testing.T) {
			// The S8 emitter order, and the reviewer's out-of-order spelling.
			for _, output := range []string{
				readerOutput("image-domains exec-v1", "image-read-epochs "+declaration, proto.ImageABILine()),
				readerOutput("image-read-epochs "+declaration, "image-domains exec-v1", proto.ImageABILine()),
			} {
				_, caps, err := ParseCapabilities(output)
				// Only "2" is well formed: it parses and is refused as a reader
				// that cannot read this epoch. Zero, repeats and non-canonical
				// numbers are malformed evidence, refused before any admission.
				if (err == nil) != (declaration == "2") {
					t.Fatalf("parse of image-read-epochs %q: %+v %v", declaration, caps, err)
				}
				if err == nil {
					err = CheckDomains(caps, []string{"exec-v1"})
				}
				if err == nil {
					t.Fatalf("production reader admission accepts image-read-epochs %q: %+v", declaration, caps)
				}
			}
		})
	}
	for name, output := range map[string]string{
		"duplicate line":     readerOutput("image-read-epochs 1", "image-read-epochs 1", proto.ImageABILine()),
		"after the ABI line": readerOutput(proto.ImageABILine(), "image-read-epochs 1"),
		"without an ABI":     readerOutput("image-read-epochs 1"),
		"glued prefix":       readerOutput("image-read-epochsx 1", proto.ImageABILine()),
		"seventeen epochs":   readerOutput("image-read-epochs 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17", proto.ImageABILine()),
		"duplicate features": readerOutput("image-features upgrade-tx-v1 upgrade-tx-v1", proto.ImageABILine()),
		"invalid feature":    readerOutput("image-features upgrade_tx", proto.ImageABILine()),
		"features after ABI": readerOutput(proto.ImageABILine(), "image-features upgrade-tx-v1"),
	} {
		if _, caps, err := ParseCapabilities(output); err == nil {
			t.Errorf("%s: malformed reader declaration parsed as %+v", name, caps)
		}
	}
	// The real emitter's adjacent-reader line is consumed, not ignored.
	_, current, err := ParseCapabilities(readerOutput(proto.ImageVersionLines()...))
	if err != nil || !reflect.DeepEqual(current.ReadEpochs, []uint64{1}) || current.ABI != proto.ImageABIVersion ||
		!reflect.DeepEqual(current, CurrentCapabilities("")) {
		t.Fatalf("emitted reader set was not parsed: %+v %v (current %+v)", current, err, CurrentCapabilities(""))
	}
}

// Both directions are checked against this running reader: the candidate must
// read our export, and we (the rollback exporter's reader) must read its
// re-export. Absent declarations never imply adjacency; ABI alone is not a cap.
// origin: hot-update S8 plan §6.1 / §11.2 P17; TestImageEpochBidirectionalReaders
func TestCandidateReaderEpochsAreCheckedBothWays(t *testing.T) {
	for _, tc := range []struct {
		name  string
		lines []string
		admit bool
	}{
		{"legacy reader at our epoch", []string{"image-domains exec-v1", "image-abi v1"}, true},
		{"explicit same-epoch reader", []string{"image-domains exec-v1", "image-read-epochs 1", "image-abi v1"}, true},
		{"declares an adjacent reader too", []string{"image-domains exec-v1", "image-read-epochs 1 2", "image-abi v1"}, true},
		{"cannot read our epoch", []string{"image-domains exec-v1", "image-read-epochs 2", "image-abi v1"}, false},
		{"next epoch reading ours, we cannot read it", []string{"image-domains exec-v1", "image-read-epochs 1 2", "image-abi v2"}, false},
		{"next epoch without a reader set", []string{"image-domains exec-v1", "image-abi v2"}, false},
		{"two epochs away", []string{"image-domains exec-v1", "image-read-epochs 1 3", "image-abi v3"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, caps, err := ParseCapabilities(readerOutput(tc.lines...))
			if err != nil {
				t.Fatalf("well-formed declaration refused at parse: %v", err)
			}
			err = CheckDomains(caps, []string{"exec-v1"})
			if (err == nil) != tc.admit {
				t.Fatalf("admit=%t err=%v caps=%+v", tc.admit, err, caps)
			}
			if err != nil && !errors.Is(err, ErrABIUnsupported) {
				t.Fatalf("epoch refusal lost the ABI class callers project: %v", err)
			}
			// The same frozen-bundle gate is used for candidate and previous.
			if bundleErr := CheckBundleCapabilities(Bundle{}, caps); (bundleErr == nil) != tc.admit {
				t.Fatalf("bundle gate disagrees: %v", bundleErr)
			}
		})
	}
}

// The parsed declarations feed the S8 three-reader admission directly; no
// caller re-derives epochs or features from a release tag or pathname.
// origin: hot-update S8 plan §6.1 / §11.2 P17
func TestCapabilitiesProjectIntoTheThreeReaderAdmission(t *testing.T) {
	full := readerOutput("image-domains exec-v1 upgrade-cut-v1", "image-features upgrade-tx-v1 authority-incarnation-v1 upgrade-cut-v1",
		"image-read-epochs 1", proto.ImageABILine())
	readers := make([]upgradecompat.Reader, 3)
	for i, digit := range []string{"a", "b", "c"} {
		release, caps, err := ParseCapabilities(full)
		if err != nil {
			t.Fatal(err)
		}
		caps.SHA256 = strings.Repeat(digit, 64)
		readers[i] = caps.Reader(release, upgradetx.ComponentAgent)
		if readers[i].Epoch != 1 || !reflect.DeepEqual(readers[i].ReadEpochs, []uint64{1}) || readers[i].Release != release ||
			!reflect.DeepEqual(readers[i].Features, []string{"upgrade-tx-v1", "authority-incarnation-v1", "upgrade-cut-v1"}) {
			t.Fatalf("projection lost a declaration: %+v", readers[i])
		}
		// A projection is a copy: admission cannot be weakened through aliasing.
		readers[i].Domains[0], readers[i].ReadEpochs[0] = "mutated", 9
		if caps.Domains[0] != "exec-v1" || caps.ReadEpochs[0] != 1 {
			t.Fatal("reader projection aliases the probed declarations")
		}
		readers[i] = caps.Reader(release, upgradetx.ComponentAgent)
	}
	if err := upgradecompat.CheckReaders(readers[0], readers[1], readers[2], []string{"exec-v1"}); err != nil {
		t.Fatalf("fully declared readers refused: %v", err)
	}
	// Today's emitter advertises the S8 adapters (every exporter exists) and is
	// admitted beside fully declared readers; an S7-era reader — transfer-v1
	// without the S8 features — must still refuse rather than have them
	// inferred from transfer-v1 or the domain line.
	// origin: S8 BROKER-EXPORTER R-11 (production advertisement)
	_, current, err := ParseCapabilities(readerOutput(proto.ImageVersionLines()...))
	if err != nil {
		t.Fatal(err)
	}
	current.SHA256 = strings.Repeat("d", 64)
	if err := upgradecompat.CheckReaders(readers[0], current.Reader("current", upgradetx.ComponentAgent), readers[2], nil); err != nil {
		t.Fatalf("today's advertised reader refused: %v", err)
	}
	_, s7, err := ParseCapabilities(readerOutput("image-domains exec-v1 pty-v1 flow-v1 ss-v1 broker-image-v1 transfer-v1", "image-read-epochs 1", proto.ImageABILine()))
	if err != nil {
		t.Fatal(err)
	}
	s7.SHA256 = strings.Repeat("e", 64)
	unadvertised := s7.Reader("s7", upgradetx.ComponentAgent)
	var refusal *upgradetx.Error
	if err := upgradecompat.CheckReaders(readers[0], unadvertised, readers[2], nil); !errors.As(err, &refusal) || refusal.Detail != "reader_capability_missing" {
		t.Fatalf("unadvertised S8 reader admitted: %v", err)
	}
}
