package auth

import (
	"strings"
	"testing"
)

// The 5 P13 proxy ctrl subjects must be present in the activated-member Pub
// allow-list, each pinned to this actor + sid (no wildcard). Guards against an
// accidental drop that would silently break the feature.
func TestActivatedMemberHasProxyLiterals(t *testing.T) {
	perms := PermissionsForActivatedMember(sampleActor, "lab")
	want := []string{
		subjectPrefix + ".ctrl.by." + sampleActor + ".s.lab.proxy.set.req",
		subjectPrefix + ".ctrl.by." + sampleActor + ".s.lab.proxy.status.req",
		subjectPrefix + ".ctrl.by." + sampleActor + ".s.lab.proxy.sub.create.req",
		subjectPrefix + ".ctrl.by." + sampleActor + ".s.lab.proxy.sub.list.req",
		subjectPrefix + ".ctrl.by." + sampleActor + ".s.lab.proxy.sub.revoke.req",
	}
	for _, w := range want {
		if !contains(perms.Pub.Allow, w) {
			t.Errorf("activated member missing proxy pub literal %q", w)
		}
	}
}

// The unactivated template must NOT carry any proxy subject (proxy is a
// per-session operation; you must activate a session first).
func TestUnactivatedHasNoProxy(t *testing.T) {
	perms := PermissionsForUnactivated(sampleActor)
	for _, a := range append(append([]string{}, perms.Pub.Allow...), perms.Sub.Allow...) {
		if strings.Contains(a, ".proxy.") {
			t.Errorf("unactivated template must not reference proxy: %q", a)
		}
	}
}

// The PSK-bearing keyset push (cmd.node.<nid>.proxy-keys.req.forwarded) must
// NOT be sub-able or pub-able by any ctl template — it is broker-pub /
// agent-sub only. A ctl that could read it would harvest every subscriber PSK.
func TestCtlCannotReachProxyKeysPush(t *testing.T) {
	ctl := []struct {
		name      string
		pub, subL []string
	}{
		{"unactivated", PermissionsForUnactivated(sampleActor).Pub.Allow, PermissionsForUnactivated(sampleActor).Sub.Allow},
		{"activated", PermissionsForActivatedMember(sampleActor, "lab").Pub.Allow, PermissionsForActivatedMember(sampleActor, "lab").Sub.Allow},
	}
	for _, c := range ctl {
		for _, a := range append(append([]string{}, c.pub...), c.subL...) {
			if strings.Contains(a, "proxy-keys") {
				t.Errorf("ctl %s must not reference the proxy-keys push subject: %q", c.name, a)
			}
			// No cmd.node.* (forwarded-shape, carries directives/PSKs).
			if strings.Contains(a, ".cmd.node.") {
				t.Errorf("ctl %s reaches a cmd.node.* subject: %q", c.name, a)
			}
		}
	}
}

func contains(ss []string, v string) bool {
	for _, s := range ss {
		if s == v {
			return true
		}
	}
	return false
}
