package brokerupgradeport

import (
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review roll-recovery-stall F6
func TestBrokerPreflightKeepsSourceGenerationAfterCancelledCandidate(t *testing.T) {
	key := upgradetx.Key{Component: upgradetx.ComponentBroker, DomainID: strings.Repeat("d", 32), ConfigPath: "/fixture/broker.yaml"}
	previous := upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 1}
	current := upgradetx.Record{Grant: upgradetx.Grant{Handle: previous}, Forward: upgradetx.Attempt{Handle: previous, SourceGeneration: 1,
		Phase: upgradetx.PhaseCancelled, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomeCancelled}}
	journal := &fixedJournal{view: upgradetx.View{Historical: current}}
	p := &brokerRuntime{host: RuntimeHost{Key: key, Journal: journal}}
	next := upgradetx.Handle{Key: key, TxID: strings.Repeat("3", 32), CutID: strings.Repeat("4", 32), Generation: 3, Revision: 1}
	generation, custody := p.custody(next, upgradetx.View{}, false)
	if generation != 1 || custody != "source" {
		t.Fatalf("an unstarted candidate invented a running source generation: generation=%d custody=%s", generation, custody)
	}
}
