// verify_test.go attacks grant substitution and the persisted artifact proof.
// origin: hot-update S8 plan §5.2 / P05
package upgradeartifact

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Each field has independent authority even when the executable pair is equal.
// origin: hot-update S8 plan §5.2 / M16
func TestReceiptRequiresExactGrant(t *testing.T) {
	f := newPublicationFixture(t)
	receipt := publishFixture(t, f)
	changes := map[string]func(*upgradetx.Grant){
		"tx":             func(g *upgradetx.Grant) { g.Handle.TxID = strings.Repeat("f", 32) },
		"cut":            func(g *upgradetx.Grant) { g.Handle.CutID = strings.Repeat("f", 32) },
		"generation":     func(g *upgradetx.Grant) { g.Handle.Generation++ },
		"revision":       func(g *upgradetx.Grant) { g.Handle.Revision++ },
		"uid":            func(g *upgradetx.Grant) { g.Handle.Key.UID++ },
		"sid":            func(g *upgradetx.Grant) { g.Handle.Key.SID = "another-session" },
		"nid":            func(g *upgradetx.Grant) { g.Handle.Key.ConfiguredNID = "another-node" },
		"config":         func(g *upgradetx.Grant) { g.Handle.Key.ConfigPath += "-changed" },
		"domain":         func(g *upgradetx.Grant) { g.Handle.Key.DomainID = "another-domain" },
		"role":           func(g *upgradetx.Grant) { g.Handle.Key.Component = upgradetx.ComponentBroker },
		"boot":           func(g *upgradetx.Grant) { g.Issuer.BootID = "another-boot" },
		"pid":            func(g *upgradetx.Grant) { g.Issuer.PID++ },
		"ticks":          func(g *upgradetx.Grant) { g.Issuer.StartTicks++ },
		"instance":       func(g *upgradetx.Grant) { g.Issuer.InstanceID = "another-instance" },
		"actor":          func(g *upgradetx.Grant) { g.ActorFP = "another-actor" },
		"nonce":          func(g *upgradetx.Grant) { g.ClientNonce = "another-nonce" },
		"scope":          func(g *upgradetx.Grant) { g.ScopeDigest = strings.Repeat("f", 64) },
		"capability":     func(g *upgradetx.Grant) { g.CapabilityDigest = strings.Repeat("f", 64) },
		"source_release": func(g *upgradetx.Grant) { g.SourceRelease = "another-source" },
		"target_release": func(g *upgradetx.Grant) { g.TargetRelease = "another-target" },
		"expiry":         func(g *upgradetx.Grant) { g.ExpiresAt++ },
		"parent":         func(g *upgradetx.Grant) { g.ParentTxID, g.ParentSlot = strings.Repeat("f", 32), "host-slot" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			grant := f.grant
			change(&grant)
			requireRefusal(t, Verify(receipt, grant))
		})
	}
	// Substituting both the incoming receipt and grant still cannot rewrite the
	// separately persisted original manifest by presenting equal SHA bytes.
	forged := receipt
	forged.Grant.Handle.TxID = strings.Repeat("f", 32)
	requireRefusal(t, Verify(forged, forged.Grant))
}

// Files and parent directories must retain their exact SHA, mode and location.
// origin: hot-update S8 plan §5.2 / P05
func TestReceiptRejectsChangedFilesAndPaths(t *testing.T) {
	for _, mode := range []string{"source_bytes", "target_bytes", "source_mode", "target_symlink", "target_missing", "manifest_missing", "manifest_mode", "manifest_digest", "directory_mode", "canonical_alias", "source_path", "target_path"} {
		t.Run(mode, func(t *testing.T) {
			f := newPublicationFixture(t)
			receipt := publishFixture(t, f)
			switch mode {
			case "source_bytes", "target_bytes":
				path := receipt.SourcePath
				if mode == "target_bytes" {
					path = receipt.TargetPath
				}
				if err := os.Chmod(path, 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("wrong bytes"), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(path, 0555); err != nil {
					t.Fatal(err)
				}
			case "source_mode":
				if err := os.Chmod(receipt.SourcePath, 0755); err != nil {
					t.Fatal(err)
				}
			case "target_symlink":
				if err := os.Remove(receipt.TargetPath); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(f.location.CanonicalPath, receipt.TargetPath); err != nil {
					t.Fatal(err)
				}
			case "target_missing":
				if err := os.Remove(receipt.TargetPath); err != nil {
					t.Fatal(err)
				}
			case "manifest_missing":
				if err := os.Remove(Pins(receipt)[2]); err != nil {
					t.Fatal(err)
				}
			case "manifest_mode":
				if err := os.Chmod(Pins(receipt)[2], 0644); err != nil {
					t.Fatal(err)
				}
			case "manifest_digest":
				receipt.ManifestDigest = strings.Repeat("f", 64)
			case "directory_mode":
				if err := os.Chmod(f.location.Directory, 0777); err != nil {
					t.Fatal(err)
				}
			case "canonical_alias":
				alias := f.location.CanonicalPath + "-alias"
				if err := os.Symlink(f.location.CanonicalPath, alias); err != nil {
					t.Fatal(err)
				}
				receipt.CanonicalPath = alias
			case "source_path":
				receipt.SourcePath = f.location.CanonicalPath
			case "target_path":
				receipt.TargetPath = f.location.CanonicalPath
			}
			requireRefusal(t, Verify(receipt, f.grant))
		})
	}
}

// A correct checksum does not excuse duplicate fields, unknown fields, an
// unsupported version or an absent decoder proof in a publication envelope.
// origin: hot-update S8 plan §2.2 / §5.2 / P05
func TestPublicationManifestRejectsNoncanonicalOrIncompleteProof(t *testing.T) {
	for _, mode := range []string{"duplicate", "unknown", "trailing", "version", "source_decoder", "target_decoder", "wrong_receipt", "truncated", "oversize", "checksum"} {
		t.Run(mode, func(t *testing.T) {
			f := newPublicationFixture(t)
			receipt := publishFixture(t, f)
			data, err := os.ReadFile(Pins(receipt)[2])
			if err != nil {
				t.Fatal(err)
			}
			var manifest publicationManifest
			if err := json.Unmarshal(data, &manifest); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "duplicate":
				data = bytes.Replace(data, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
			case "unknown":
				data = bytes.Replace(data, []byte(`"version":1`), []byte(`"version":1,"unknown":true`), 1)
			case "trailing":
				data = append(data, []byte(`{}`)...)
			case "version":
				manifest.Version = 2
			case "source_decoder":
				manifest.SourceManifest = ""
			case "target_decoder":
				manifest.TargetManifest = ""
			case "wrong_receipt":
				manifest.Receipt.Grant.ParentTxID, manifest.Receipt.Grant.ParentSlot = strings.Repeat("f", 32), "other-parent-slot"
			case "checksum":
				manifest.SourceManifest = strings.Repeat("f", 64)
			case "truncated":
				data = data[:len(data)/2]
			case "oversize":
				data = append(data, bytes.Repeat([]byte(" "), maxManifestBytes)...)
			}
			if mode == "version" || mode == "source_decoder" || mode == "target_decoder" || mode == "wrong_receipt" || mode == "checksum" {
				data, err = json.Marshal(manifest)
				if err != nil {
					t.Fatal(err)
				}
			}
			if mode != "checksum" {
				receipt.ManifestDigest = digest(data)
			}
			path := filepath.Join(f.location.Directory, manifestName(receipt.ManifestDigest))
			if mode == "checksum" {
				if err := os.Chmod(path, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(path, data, 0444); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0444); err != nil {
				t.Fatal(err)
			}
			requireRefusal(t, Verify(receipt, f.grant))
		})
	}
}

// Zero, malformed and legacy authorizations do not create a publication right.
// origin: hot-update S8 plan §2.1 / §3.1 / §5.2 / P05
func TestReceiptRejectsIncompleteAuthorization(t *testing.T) {
	f := newPublicationFixture(t)
	receipt := publishFixture(t, f)
	changes := map[string]func(*upgradetx.Grant){
		"tx":             func(g *upgradetx.Grant) { g.Handle.TxID = "" },
		"cut":            func(g *upgradetx.Grant) { g.Handle.CutID = "../bad" },
		"generation":     func(g *upgradetx.Grant) { g.Handle.Generation = 0 },
		"revision":       func(g *upgradetx.Grant) { g.Handle.Revision = 0 },
		"sid":            func(g *upgradetx.Grant) { g.Handle.Key.SID = "" },
		"nid":            func(g *upgradetx.Grant) { g.Handle.Key.ConfiguredNID = "" },
		"domain":         func(g *upgradetx.Grant) { g.Handle.Key.DomainID = "" },
		"config":         func(g *upgradetx.Grant) { g.Handle.Key.ConfigPath = "relative.yaml" },
		"nats":           func(g *upgradetx.Grant) { g.Handle.Key.Component = upgradetx.ComponentNATS },
		"caddy":          func(g *upgradetx.Grant) { g.Handle.Key.Component = upgradetx.ComponentCaddy },
		"source":         func(g *upgradetx.Grant) { g.SourceSHA = strings.Repeat("A", 64) },
		"target":         func(g *upgradetx.Grant) { g.TargetSHA = "" },
		"scope":          func(g *upgradetx.Grant) { g.ScopeDigest = "" },
		"capability":     func(g *upgradetx.Grant) { g.CapabilityDigest = "" },
		"boot":           func(g *upgradetx.Grant) { g.Issuer.BootID = "" },
		"pid":            func(g *upgradetx.Grant) { g.Issuer.PID = 0 },
		"ticks":          func(g *upgradetx.Grant) { g.Issuer.StartTicks = 0 },
		"instance":       func(g *upgradetx.Grant) { g.Issuer.InstanceID = "" },
		"actor":          func(g *upgradetx.Grant) { g.ActorFP = "" },
		"nonce":          func(g *upgradetx.Grant) { g.ClientNonce = "" },
		"source_release": func(g *upgradetx.Grant) { g.SourceRelease = "" },
		"target_release": func(g *upgradetx.Grant) { g.TargetRelease = "" },
		"mode":           func(g *upgradetx.Grant) { g.Mode = upgradetx.ModeLegacyInterrupting },
		"expiry":         func(g *upgradetx.Grant) { g.ExpiresAt = 1 },
		"parent_slot":    func(g *upgradetx.Grant) { g.ParentSlot = "unregistered-slot" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			grant := f.grant
			change(&grant)
			// Isolate shape validation from the separate exact-grant comparison.
			requireRefusal(t, validateGrant(grant))
			requireRefusal(t, Verify(receipt, grant))
		})
	}
}
