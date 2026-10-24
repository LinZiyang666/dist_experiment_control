// Selector tests execute real sandbox images; transaction views are unit inputs.
// They do not substitute for P23's real daemon, journal and root-stage chain.
// origin: hot-update S8 plan §5.2 / §9.1 / P05
package upgradeartifact

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func selectorView(receipt Receipt, sha string) upgradetx.View {
	return upgradetx.View{Historical: upgradetx.Record{Version: upgradetx.RecordVersion, Key: receipt.Grant.Handle.Key,
		Revision: 1, Grant: receipt.Grant, EffectiveSHA: sha, RecordedPreviousSHA: receipt.SourceSHA},
		Metadata: upgradetx.Metadata{TxID: receipt.Grant.Handle.TxID, Pins: Pins(receipt), Window: upgradetx.WindowOpen}}
}

// Rollback selects the source while the canonical dispatcher still contains
// newer bytes. An ordinary boot and the next upgrade must both start there.
// origin: hot-update S8 plan §5.2 / §9.1 / P05 cold-boot mutant
func TestReadOnlyRollbackColdBootUpgrade(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseReadOnlyRollbackColdBootUpgrade(t)
	}
	testharness.AssertNoGoroutineLeak(t, "artifact cold boot", beforeG)
	testharness.AssertNoFDLeak(t, "artifact cold boot", beforeFD, 0)
}

func exerciseReadOnlyRollbackColdBootUpgrade(t *testing.T) {
	t.Helper()
	for _, role := range []upgradetx.Component{upgradetx.ComponentAgent, upgradetx.ComponentBroker} {
		f := newPublicationFixture(t)
		f.grant.Handle.Key.Component = role
		if role == upgradetx.ComponentBroker {
			f.grant.Handle.Key.ConfiguredNID = "" // Single-mode brokers have no configured cluster NID.
			f.grant.Handle.Key.SID = ""
		} else {
			f.grant.Handle.Key.ConfigPath = "" // Agent identity can come from its registered argv.
		}
		f.port.grant = f.grant
		f.port.supervise = true
		receipt := publishFixture(t, f)
		view := selectorView(receipt, receipt.TargetSHA)
		forward, err := Resolve(f.grant.Handle.Key, view)
		if err != nil || forward != receipt.TargetPath {
			t.Fatalf("forward selector: %s: %v", forward, err)
		}
		assertImageOutput(t, forward, "target\n")
		view.Historical.EffectiveSHA = receipt.SourceSHA
		cold, err := ResolveCanonical(f.location.CanonicalPath, f.grant.Handle.Key, &view)
		if err != nil || cold != receipt.SourcePath {
			t.Fatalf("cold boot selector: %s: %v", cold, err)
		}
		assertImageOutput(t, cold, "source\n")
		canonical, err := os.ReadFile(f.location.CanonicalPath)
		if err != nil || !bytes.Equal(canonical, f.target) {
			t.Fatal("rollback rewrote canonical dispatcher")
		}
		f.source, err = os.ReadFile(cold)
		if err != nil {
			t.Fatal(err)
		}
		f.grant.SourceSHA = digest(f.source)
		f.grant.Handle.TxID = strings.Repeat("f", 32)
		f.grant.Handle.CutID = strings.Repeat("c", 32)
		f.grant.Handle.Generation++
		f.target = imageBytes("second")
		f.grant.TargetSHA, f.grant.TargetRelease = digest(f.target), "second"
		f.port.grant, f.port.authorizations = f.grant, 0
		second := publishFixture(t, f)
		if second.SourceSHA != receipt.SourceSHA {
			t.Fatal("second upgrade used newest canonical bytes as source")
		}
		secondView := selectorView(second, second.TargetSHA)
		path, err := ResolveCanonical(f.location.CanonicalPath, f.grant.Handle.Key, &secondView)
		if err != nil || path != second.TargetPath {
			t.Fatalf("second upgrade: %s: %v", path, err)
		}
		assertImageOutput(t, path, "second\n")
		foreign := f.grant.Handle.Key
		foreign.UID++
		_, err = Resolve(foreign, secondView)
		requireRefusal(t, err)
	}
}

func assertImageOutput(t *testing.T, path, want string) {
	t.Helper()
	output, err := testharness.Output(exec.CommandContext(t.Context(), path))
	if err != nil || string(output) != want {
		t.Fatalf("selected image output: %q, want %q: %v", output, want, err)
	}
}

// A corrupt, cross-component, unknown or incompletely pinned journal cannot
// fall back to whatever executable happens to occupy the canonical pathname.
// origin: hot-update S8 plan §3.1 / §5.2 / P05
func TestSelectorRejectsUnknownOrUnpinnedState(t *testing.T) {
	for _, mode := range []string{"corrupt", "zero_view", "version", "key", "revision", "tx", "sha", "third_sha", "missing_manifest", "missing_image_pin", "ambiguous", "canonical", "too_many_pins", "current_tx", "current_sha"} {
		t.Run(mode, func(t *testing.T) {
			f := newPublicationFixture(t)
			receipt := publishFixture(t, f)
			view := selectorView(receipt, receipt.SourceSHA)
			canonical := f.location.CanonicalPath
			switch mode {
			case "corrupt":
				view.Code = upgradetx.CodeStateCorrupt
			case "zero_view":
				view = upgradetx.View{}
			case "version":
				view.Historical.Version = 1
			case "key":
				view.Historical.Key.ConfiguredNID = "another-node"
			case "revision":
				view.Historical.Revision = 0
			case "tx":
				view.Metadata.TxID = "another-tx"
			case "sha":
				view.Historical.EffectiveSHA = ""
			case "third_sha":
				view.Historical.EffectiveSHA = strings.Repeat("f", 64)
			case "missing_manifest":
				view.Metadata.Pins = view.Metadata.Pins[:2]
			case "missing_image_pin":
				view.Metadata.Pins = view.Metadata.Pins[1:]
			case "ambiguous":
				other := duplicatePublication(t, receipt)
				view.Metadata.Pins = append(view.Metadata.Pins, Pins(other)...)
			case "canonical":
				canonical = filepath.Join(filepath.Dir(canonical), "another-canonical")
				if err := os.WriteFile(canonical, f.target, 0555); err != nil {
					t.Fatal(err)
				}
			case "too_many_pins":
				for len(view.Metadata.Pins) <= 256 {
					view.Metadata.Pins = append(view.Metadata.Pins, receipt.SourcePath)
				}
			case "current_tx":
				view.Observation.Current.Handle = f.grant.Handle
				view.Observation.Current.Handle.TxID = strings.Repeat("f", 32)
			case "current_sha":
				view.Observation.EffectiveSHA = receipt.TargetSHA
			}
			_, err := ResolveCanonical(canonical, f.grant.Handle.Key, &view)
			requireRefusal(t, err)
		})
	}
}

func duplicatePublication(t *testing.T, receipt Receipt) Receipt {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	location := Location{CanonicalPath: receipt.CanonicalPath, Directory: directory}
	if err := os.Chmod(location.Directory, 0755); err != nil {
		t.Fatal(err)
	}
	port := &publicationTestPort{grant: receipt.Grant, location: location, lease: publicationTestLease{domain: receipt.Grant.Handle.Key.DomainID}}
	source, err := os.ReadFile(receipt.SourcePath)
	if err != nil {
		t.Fatal(err)
	}
	target, err := os.ReadFile(receipt.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	// Build conflicting immutable evidence without publishing a second grant pointer.
	copy, err := publish(WithPublicationPort(t.Context(), port), receipt.Grant, bytes.NewReader(source), bytes.NewReader(target), (*os.File).Sync)
	if err != nil {
		t.Fatal(err)
	}
	return copy
}

// A proven missing journal uses the explicit canonical registration. A nonnil
// unknown view must refuse instead of treating unknown state as an empty store.
// origin: hot-update S8 plan §5.2 / P05
func TestCanonicalBootRequiresProvenJournalAbsence(t *testing.T) {
	f := newPublicationFixture(t)
	path, err := ResolveCanonical(f.location.CanonicalPath, f.grant.Handle.Key, nil)
	if err != nil || path != f.location.CanonicalPath {
		t.Fatalf("absent journal: %s: %v", path, err)
	}
	view := upgradetx.View{Code: upgradetx.CodeGrantUnknown}
	_, err = ResolveCanonical(f.location.CanonicalPath, f.grant.Handle.Key, &view)
	requireRefusal(t, err)
	if err := os.Chmod(f.location.CanonicalPath, 0777); err != nil {
		t.Fatal(err)
	}
	_, err = ResolveCanonical(f.location.CanonicalPath, f.grant.Handle.Key, nil)
	requireRefusal(t, err)
}

// A durable selector remains valid after the inert Prepare window expires.
// Fresh Start still refuses the same expired grant.
// origin: hot-update S8 plan §3.1 / §5.2 / P05
func TestSelectorDoesNotRenewExpiredExecutionGrant(t *testing.T) {
	f := newPublicationFixture(t)
	f.grant.ExpiresAt = time.Now().Add(time.Second).UnixNano()
	f.port.grant = f.grant
	receipt := publishFixture(t, f)
	<-time.After(time.Until(time.Unix(0, f.grant.ExpiresAt)) + time.Millisecond)
	view := selectorView(receipt, receipt.SourceSHA)
	path, err := Resolve(f.grant.Handle.Key, view)
	if err != nil || path != receipt.SourcePath {
		t.Fatalf("expired accepted selector: %s: %v", path, err)
	}
	requireRefusal(t, Verify(receipt, f.grant))
}

// A manual rollback creates a new Current reverse while the old immutable
// publication remains pinned. Cold boot can read it without a second root stage;
// the original receipt still cannot authorize Start under the reverse grant.
// origin: hot-update S8 plan §4.1 / §5.2 / §8.1 / P05
func TestReverseSelectorUsesRecordedPreviousPublication(t *testing.T) {
	f := newPublicationFixture(t)
	receipt := publishFixture(t, f)
	view := reverseSelectorView(receipt)
	path, err := ResolveCanonical(f.location.CanonicalPath, f.grant.Handle.Key, &view)
	if err != nil || path != receipt.SourcePath {
		t.Fatalf("accepted reverse cold boot: %s: %v", path, err)
	}
	requireRefusal(t, Verify(receipt, view.Historical.Grant))
	for _, mode := range []string{"forward", "previous", "cut", "target"} {
		t.Run(mode, func(t *testing.T) {
			invalid := reverseSelectorView(receipt)
			switch mode {
			case "forward":
				invalid.Historical.Reverse.Direction = upgradetx.DirectionForward
			case "previous":
				invalid.Historical.RecordedPreviousSHA = receipt.TargetSHA
			case "cut":
				invalid.Historical.Reverse.Handle.CutID = strings.Repeat("f", 32)
			case "target":
				invalid.Historical.Reverse.TargetSHA = receipt.TargetSHA
			}
			_, err := Resolve(f.grant.Handle.Key, invalid)
			requireRefusal(t, err)
		})
	}
	newForward := selectorView(receipt, receipt.SourceSHA)
	newForward.Historical.Grant.Handle.TxID = strings.Repeat("f", 32)
	newForward.Metadata.TxID = newForward.Historical.Grant.Handle.TxID
	_, err = Resolve(f.grant.Handle.Key, newForward)
	requireRefusal(t, err)
}

func reverseSelectorView(receipt Receipt) upgradetx.View {
	view := selectorView(receipt, receipt.SourceSHA)
	grant := receipt.Grant
	grant.Handle.TxID, grant.Handle.CutID = strings.Repeat("f", 32), strings.Repeat("c", 32)
	grant.Handle.Generation++
	grant.SourceSHA, grant.TargetSHA = grant.TargetSHA, grant.SourceSHA
	grant.SourceRelease, grant.TargetRelease = grant.TargetRelease, grant.SourceRelease
	view.Historical.Grant, view.Metadata.TxID = grant, grant.Handle.TxID
	view.Historical.Reverse = upgradetx.Attempt{Handle: grant.Handle, Direction: upgradetx.DirectionReverse,
		SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA}
	view.Historical.Revision++
	view.Historical.Reverse.Handle.Revision = view.Historical.Revision
	return view
}
