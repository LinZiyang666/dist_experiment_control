package infraharness

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"testing"

	"github.com/LinZiyang666/tether/internal/infracontract"
)

// origin: hot-update S9 plan §2.3 / T58
func TestInfraObservationRejectsMissingProducer(t *testing.T) {
	o := infracontract.Observation{Version: 1, Tx: infracontract.ID{1}, Step: 1,
		Kind: "attachment_changed", BootID: "independent-reader-boot", BootNS: 123,
		Subject: infracontract.ID{2}, Generation: 9, Evidence: sha256.Sum256([]byte("raw socket incarnation"))}
	p := Producer{Tx: o.Tx, Step: o.Step, Kind: o.Kind, BootID: o.BootID,
		Subject: o.Subject, Generation: o.Generation, Evidence: o.Evidence}
	var r Observer
	if err := r.Record(o); err == nil {
		t.Fatal("state projection without producer was accepted")
	}
	if err := r.Register(p); err != nil {
		t.Fatal(err)
	}
	if err := r.Record(o); err != nil {
		t.Fatal(err)
	}
	mutations := []struct {
		name string
		edit func(*infracontract.Observation)
	}{
		{"version-empty", func(o *infracontract.Observation) { o.Version = 0 }},
		{"version-unknown", func(o *infracontract.Observation) { o.Version = 2 }},
		{"tx-empty", func(o *infracontract.Observation) { o.Tx = infracontract.ID{} }},
		{"tx-stale", func(o *infracontract.Observation) { o.Tx[0]++ }},
		{"step-empty", func(o *infracontract.Observation) { o.Step = 0 }},
		{"step-stale", func(o *infracontract.Observation) { o.Step++ }},
		{"kind-empty", func(o *infracontract.Observation) { o.Kind = "" }},
		{"kind-projection", func(o *infracontract.Observation) { o.Kind = "status" }},
		{"kind-wrong", func(o *infracontract.Observation) { o.Kind = "completed" }},
		{"boot-empty", func(o *infracontract.Observation) { o.BootID = "" }},
		{"boot-stale", func(o *infracontract.Observation) { o.BootID += "-stale" }},
		{"clock-empty", func(o *infracontract.Observation) { o.BootNS = 0 }},
		{"clock-negative", func(o *infracontract.Observation) { o.BootNS = -1 }},
		{"subject-empty", func(o *infracontract.Observation) { o.Subject = infracontract.ID{} }},
		{"subject-invented", func(o *infracontract.Observation) { o.Subject[0]++ }},
		{"generation-empty", func(o *infracontract.Observation) { o.Generation = 0 }},
		{"generation-invented", func(o *infracontract.Observation) { o.Generation++ }},
		{"evidence-empty", func(o *infracontract.Observation) { o.Evidence = infracontract.Digest{} }},
		{"evidence-invented", func(o *infracontract.Observation) { o.Evidence[0]++ }},
	}
	// This independent invalid-field table must not call validateProducer: a
	// deleted production guard would otherwise delete its own negative oracle.
	invalidProducers := map[string]bool{"tx-empty": true, "step-empty": true, "kind-empty": true,
		"kind-projection": true, "boot-empty": true, "subject-empty": true,
		"generation-empty": true, "evidence-empty": true}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			bad := o
			mutation.edit(&bad)
			if err := r.Record(bad); err == nil {
				t.Fatal("invalid observation accepted")
			}
			if len(r.Observations()) != 1 {
				t.Fatal("refused observation altered receipts")
			}
			badProducer := Producer{Tx: bad.Tx, Step: bad.Step, Kind: bad.Kind, BootID: bad.BootID,
				Subject: bad.Subject, Generation: bad.Generation, Evidence: bad.Evidence}
			// Fields structurally invalid for a producer must fail even if no
			// observation is recorded; a valid different producer may register.
			if invalidProducers[mutation.name] {
				if err := r.Register(badProducer); err == nil {
					t.Fatal("invalid producer registered")
				}
			}
		})
	}
	for _, kind := range infracontract.ObservationKinds() {
		p.Kind, o.Kind = kind, kind
		if err := r.Register(p); err != nil {
			t.Fatal(err)
		}
		if err := r.Record(o); err != nil {
			t.Fatalf("closed kind %s rejected: %v", kind, err)
		}
	}
	copy := r.Observations()
	copy[0].Subject = infracontract.ID{}
	if r.Observations()[0].Subject == (infracontract.ID{}) {
		t.Fatal("reader mutated original receipt")
	}
	before := len(r.Observations())
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Go(func() {
			for round := 0; round < 5; round++ {
				if err := r.Register(p); err != nil {
					t.Error(err)
				}
				if err := r.Record(o); err != nil {
					t.Error(err)
				}
				_ = r.Observations()
			}
		})
	}
	workers.Wait()
	if len(r.Observations()) != before+40 {
		t.Fatal("concurrent reader lost an observation")
	}
	exerciseArtifact(t)
}

func exerciseArtifact(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(self)
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "fixture")
	if err := os.WriteFile(binary, body, 0700); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("test artifact has no Go build information")
	}
	race := false
	for _, setting := range info.Settings {
		if setting.Key == "-race" && setting.Value == "true" {
			race = true
		}
	}
	if err := VerifyArtifact(root, binary, digest, false); err != nil {
		t.Fatal(err)
	}
	if err := VerifyArtifact(root, binary, digest, true); (err == nil) != race {
		t.Fatalf("inner race identity mismatch: artifact race=%t err=%v", race, err)
	}
	t.Logf("artifact binary_sha256=%x, actual inner race=%t", digest, race)
	wrong := digest
	wrong[0]++
	if err := VerifyArtifact(root, binary, wrong, false); err == nil {
		t.Fatal("wrong artifact digest accepted")
	}
	if err := VerifyArtifact(root, binary, infracontract.Digest{}, false); err == nil {
		t.Fatal("empty digest accepted")
	}
	// No claimed digest means there is no custody to inspect. A missing path
	// proves this refusal precedes all path resolution or executable reads.
	if err := VerifyArtifact(root, filepath.Join(root, "unsupplied"), infracontract.Digest{}, false); err == nil || !strings.Contains(err.Error(), "private artifact and digest required") {
		t.Fatal("empty digest did not reject before artifact inspection")
	}
	if err := VerifyArtifact("/", binary, digest, false); err == nil || !strings.Contains(err.Error(), "private artifact and digest required") {
		t.Fatal("nonprivate root did not reject before artifact inspection")
	}
	rootLink := filepath.Join(root, "hostroot")
	if err := os.Symlink("/", rootLink); err != nil {
		t.Fatal(err)
	}
	if err := VerifyArtifact(rootLink, binary, digest, false); err == nil {
		t.Fatal("root symlink escaped fixture boundary")
	}
	if err := VerifyArtifact(root, self, digest, false); err == nil {
		t.Fatal("artifact outside fixture accepted")
	}
	link := filepath.Join(root, "escape")
	if err := os.Symlink(self, link); err != nil {
		t.Fatal(err)
	}
	if err := VerifyArtifact(root, link, digest, false); err == nil {
		t.Fatal("symlink escape accepted")
	}
	if err := os.Chmod(binary, 0600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyArtifact(root, binary, digest, false); err == nil {
		t.Fatal("nonexecutable artifact accepted")
	}
	// A non-Go executable cannot satisfy a requested inner race build. This
	// check remains a negative control even when the outer test is race-built.
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(binary, 0700); err != nil {
		t.Fatal(err)
	}
	plainSHA := sha256.Sum256([]byte("#!/bin/sh\nexit 0\n"))
	if err := VerifyArtifact(root, binary, plainSHA, true); err == nil {
		t.Fatal("plain artifact advertised as race")
	}
}
