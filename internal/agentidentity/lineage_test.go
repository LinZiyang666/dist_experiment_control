package agentidentity

import (
	"reflect"
	"testing"
)

// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
func TestLeaseOfferKeepsConfiguredCredentialFamily(t *testing.T) {
	for _, tc := range []struct {
		assigned string
		want     bool
	}{
		{"gpu-02-02", true}, {"other-02", false}, {"gpu-03", false},
		{"gpu-02", false}, {"INVALID NAME", false},
	} {
		if got := AcceptableLeaseName("gpu-02", "gpu-02", tc.assigned, 0); got != tc.want {
			t.Errorf("lease %q accepted=%v, want %v", tc.assigned, got, tc.want)
		}
	}
}

// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
func TestChildEnvironmentDropsLineageAndKeepsBusinessVariables(t *testing.T) {
	env := []string{"PATH=/bin", InstanceIDEnv + "=parent", RoutingNIDEnv + "=gpu-02", InstanceIDEnv + "_BUSINESS=keep", "TERM=screen"}
	original := append([]string(nil), env...)
	want := []string{"PATH=/bin", InstanceIDEnv + "_BUSINESS=keep", "TERM=screen"}
	if got := StripInstanceEnv(env); !reflect.DeepEqual(got, want) {
		t.Fatalf("child environment=%q, want %q", got, want)
	}
	if !reflect.DeepEqual(env, original) {
		t.Fatal("filter changed the caller's environment")
	}
}
