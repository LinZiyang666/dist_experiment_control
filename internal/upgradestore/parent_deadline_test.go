package upgradestore

import (
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"testing"
	"time"
)

// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §3.1
func TestParentDeadlineUsesInitialFleetBudgetWithoutRenewal(t *testing.T) {
	for _, logical := range []string{"node@roll", "node@host"} {
		t.Run(logical, func(t *testing.T) {
			s, req := newFixture(t)
			s.options.Key.ConfiguredNID = logical
			req.Scope.Participants = []upgradetx.Key{s.options.Key}
			initial := s.now()
			req.ExpiresAt = initial.Add(upgradebudget.FullStackRollout).UnixNano()
			g, err := s.Prepare(s.options.Key, req)
			if err != nil {
				t.Fatal(err)
			}
			if g.ExpiresAt != req.ExpiresAt {
				t.Fatal("parent deadline changed at issuance")
			}
			s.now = func() time.Time { return initial.Add(time.Minute) }
			retry, err := s.Prepare(s.options.Key, req)
			if err != nil || retry != g {
				t.Fatalf("retry renewed original parent: %+v %v", retry, err)
			}
			req.ExpiresAt++
			if _, err = s.Prepare(s.options.Key, req); err == nil {
				t.Fatal("same nonce extended an issued parent")
			}
		})
	}
	s, req := newFixture(t)
	req.ExpiresAt = s.now().Add(upgradebudget.FullStackRollout).UnixNano()
	if _, err := s.Prepare(s.options.Key, req); err == nil {
		t.Fatal("standalone component inherited fleet budget")
	}
}
