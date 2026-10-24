package agentupgradeport

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// pinJournal answers Load for one transaction (by ID or as Current) with the
// given pins; the reverse grant itself is not yet accepted.
type pinJournal struct {
	upgradetx.Store
	tx      string
	pins    []string
	current bool // the pins belong to Current, not to the grant's own TxID
}

func (j pinJournal) Load(_ upgradetx.Key, tx string) (upgradetx.View, error) {
	if tx == j.tx && !j.current || tx == "" && j.current {
		return upgradetx.View{Metadata: upgradetx.Metadata{TxID: j.tx, Pins: j.pins}}, nil
	}
	return upgradetx.View{}, &upgradetx.Error{Code: upgradetx.CodeGrantUnknown, Detail: "transaction is not recorded"}
}

func hexOf(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// origin: hot-update S8 plan §5.2 / §8.1; S8 FIX-AGENTWIRE. A reverse grant has
// no publication receipt: its target must be the journal's recorded previous
// pin (the automatic reverse's own TxID, or Current's pins for a manual one),
// verified by its bytes. A matching basename with other bytes, a pin of other
// bytes, or no recorded pin at all never stands in; and the previous reader of
// a reverse is its target, not the running source.
func TestReversePreflightResolvesOnlyTheRecordedPin(t *testing.T) {
	dir := t.TempDir()
	running, previous := []byte("running image"), []byte("recorded previous image")
	runningSHA, previousSHA := hexOf(running), hexOf(previous)
	pin := filepath.Join(dir, previousSHA)
	if err := os.WriteFile(pin, previous, 0o555); err != nil {
		t.Fatal(err)
	}
	impostor := filepath.Join(t.TempDir(), previousSHA)
	if err := os.WriteFile(impostor, []byte("other bytes under the same name"), 0o555); err != nil {
		t.Fatal(err)
	}
	owner := upgradetx.Owner{BootID: strings.Repeat("b", 32), PID: os.Getpid(), StartTicks: 7, InstanceID: strings.Repeat("9", 32)}
	key := upgradetx.Key{DomainID: strings.Repeat("d", 32), Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "node"}
	grant := upgradetx.Grant{Handle: upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 4, Revision: 9},
		Issuer: owner, Mode: upgradetx.ModeHot, SourceRelease: "new", TargetRelease: "old", SourceSHA: runningSHA, TargetSHA: previousSHA,
		ScopeDigest: strings.Repeat("5", 64)}
	reader := func(sha string) agentimage.Capabilities {
		return agentimage.Capabilities{ABI: 1, SHA256: sha, ReadEpochs: []uint64{1}, Domains: []string{"exec-v1", "upgrade-cut-v1"},
			Features: []string{"upgrade-tx-v1", "authority-incarnation-v1", "upgrade-cut-v1"}}
	}
	for _, tc := range []struct {
		name    string
		journal pinJournal
		want    string // "" refuses artifact_unavailable
	}{
		{"automatic_reverse_own_tx", pinJournal{tx: grant.Handle.TxID, pins: []string{filepath.Join(dir, runningSHA), pin}}, pin},
		{"manual_reverse_current_pins", pinJournal{tx: strings.Repeat("3", 32), pins: []string{pin}, current: true}, pin},
		{"basename_with_other_bytes", pinJournal{tx: grant.Handle.TxID, pins: []string{impostor}}, ""},
		{"other_sha_only", pinJournal{tx: grant.Handle.TxID, pins: []string{filepath.Join(dir, runningSHA)}}, ""},
		{"no_record", pinJournal{tx: strings.Repeat("4", 32)}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probed := ""
			port := NewRuntime(Host{Key: key, Journal: tc.journal,
				Owner:      func() (upgradetx.Owner, error) { return owner, nil },
				RunningSHA: func() (string, error) { return runningSHA, nil },
				Source:     func(sha string) (string, agentimage.Capabilities, error) { return "new", reader(sha), nil },
				Probe: func(path string) (string, agentimage.Capabilities, error) {
					probed = path
					return "old", reader(previousSHA), nil
				},
				Candidate: func(agentimage.Capabilities) error { return nil },
				Deferrals: func() []agentimage.DeferredError { return nil },
				Required:  func() []string { return []string{"exec-v1"} },
				Snapshot: func() (execsession.Image, agentimage.CourierState, error) {
					return execsession.Image{}, agentimage.CourierState{}, nil
				},
				Transfers:   noTransfers,
				Incarnation: func() string { return strings.Repeat("7", 64) },
			}).(*runtimePort)
			proof, err := port.Preflight(t.Context(), grant, upgradeartifact.Receipt{})
			if tc.want == "" {
				var refusal *upgradetx.Error
				if !errors.As(err, &refusal) || refusal.Code != upgradetx.CodeDeferred || refusal.Detail != ReasonArtifactUnavailable || probed != "" {
					t.Fatalf("unverified reverse target admitted: err=%v probed=%q", err, probed)
				}
				return
			}
			if err != nil || probed != tc.want || proof.Grant != grant || !proof.Census.Known {
				t.Fatalf("recorded pin not resolved: err=%v probed=%q", err, probed)
			}
			ready := port.ready[grant.Handle.TxID]
			if ready.target != tc.want || ready.previous.SHA256 != previousSHA || ready.sourceSHA != runningSHA {
				t.Fatal("a reverse's previous reader is not its recorded target")
			}
		})
	}
}
