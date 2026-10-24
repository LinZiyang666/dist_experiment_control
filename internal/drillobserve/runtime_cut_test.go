package drillobserve

import (
	"strings"
	"testing"
)

// origin: S8 internal review activation-and-route-migration F4
func TestRuntimeSampleBindsOnlyItsImportedCut(t *testing.T) {
	runtimeImageCut.Lock()
	previous := runtimeImageCut.id
	runtimeImageCut.id = ""
	runtimeImageCut.Unlock()
	defer func() { runtimeImageCut.Lock(); runtimeImageCut.id = previous; runtimeImageCut.Unlock() }()
	sample := RuntimeCounters{BootID: "boot", PID: 123, StartTicks: 4, Binding: RuntimeBinding{Domain: "agent", InstanceID: "lineage", Generation: 1}}
	cold := runtimeCutIdentity(sample)
	if len(cold) != 32 || runtimeCutIdentity(sample) != cold {
		t.Fatal("cold runtime has no stable owner-bound image identity")
	}
	sample.Binding.Generation++
	if runtimeCutIdentity(sample) == cold {
		t.Fatal("distinct cold images shared a cut identity")
	}
	for _, bad := range []string{"", "../cut", strings.Repeat("A", 32)} {
		if BindRuntimeCut(bad) == nil {
			t.Fatal("invalid cut acquired sampler identity")
		}
	}
	cut := strings.Repeat("a", 32)
	if BindRuntimeCut(cut) != nil || runtimeCutIdentity(sample) != cut || BindRuntimeCut(cut) != nil {
		t.Fatal("validated imported cut did not bind the runtime sampler")
	}
	if BindRuntimeCut(strings.Repeat("b", 32)) == nil || runtimeCutIdentity(sample) != cut {
		t.Fatal("running image was relabelled as a later cut")
	}
}
