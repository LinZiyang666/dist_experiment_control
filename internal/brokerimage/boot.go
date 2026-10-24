package brokerimage

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// BootImage receives ownership of the collector's already validated envelope.
// It never calls Restore or examines argv/hints to decide whether to read it.
// P22 redispatches using Bundle.Args, even when incoming argv is unrelated.
// origin: hot-update S6 plan §4.1 / T03
// Boot is startup-only; callers retain the failed envelope for W1 cleanup.
type BootImage struct {
	mu        sync.Mutex
	restored  *handoff.Restored
	bundle    Bundle
	imported  Imported
	salvage   tunnel.Salvage
	attempted bool
	// upgrade is the verified S8 adjunct and its domain lease or refused
	// custody; a legacy dispatch has none. origin: hot-update S8 plan §11.2 P17
	upgrade *upgradecut.Custody
}

func Boot(restored *handoff.Restored) (*BootImage, error) {
	if runtime.GOOS != "linux" {
		return nil, handoff.ErrUnsupported
	}
	if restored == nil {
		return nil, ErrImage
	}
	b, err := Decode(restored.State)
	if err != nil {
		return nil, err
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || strings.TrimSpace(string(boot)) != b.BootID {
		return nil, ErrImage
	}
	// Hash the mapped executable, not the potentially replaced on-disk path.
	running, err := hashArtifact("/proc/self/exe")
	if err != nil || (running != b.CandidateSHA256 && running != b.PreviousSHA256) {
		return nil, ErrImage
	}
	if err = handoff.ValidateResourceOwners(restored.Borrow()); err != nil {
		return nil, err
	}
	expected := map[string]bool{b.StateResource: true, b.LockResource: true}
	for _, listener := range b.Listeners {
		expected[listener.ID] = true
	}
	for id := range expected {
		found := false
		for _, res := range restored.Resources {
			found = found || res.ID == id
		}
		if !found {
			return nil, ErrImage
		}
	}
	// Validate the broker-private partition before ownership dispatch. Borrowed
	// Pread never takes or closes a descriptor, so refusal leaves the sole
	// collector's W1 census and all files intact.
	// origin: hot-update S6 plan §4.1 / T03 / T04
	borrowed := restored.Borrow()
	data, _, err := sealedstate.Read(borrowedState{borrowed}, b.StateResource, "state", sealedstate.MaxStateBytes)
	if err != nil {
		return nil, ErrImage
	}
	private, err := DecodePrivateState(data)
	if err != nil || CheckPrivateState(private, b) != nil {
		return nil, ErrImage
	}
	// origin: hot-update S8 plan §6.1 / §11.2 P17. The required S8 adjunct is
	// verified before this closure and stays outside it: the nine private kinds
	// keep delta=∅, and no private owner may claim the adjunct's one file.
	cut, err := upgradecut.Admit(borrowed, upgradetx.ComponentBroker, upgradecut.ExpectImage(b.ExporterSHA256, running))
	if err != nil {
		return nil, ErrImage
	}
	actual := make(map[string]handoff.Resource)
	fileCount := 0
	for _, resource := range borrowed {
		actual[resource.ID] = resource
		fileCount += len(resource.Files)
	}
	if fileCount != len(private.Resources)+1+upgradecut.Files(cut) {
		return nil, ErrImage
	}
	for _, owner := range private.Resources {
		resource, ok := actual[owner.ID]
		if !ok || owner.ID == b.StateResource || owner.ID == upgradecut.ResourceID || resource.Kind != owner.Kind || !bytes.Equal(resource.Data, owner.Data) {
			return nil, ErrImage
		}
		identity, err := handoff.ResourceFileIdentity(resource.Files[owner.Name])
		if err != nil || identity != owner.Identity {
			return nil, ErrImage
		}
	}
	return &BootImage{restored: restored, bundle: b, upgrade: cut}, nil
}
func (b *BootImage) Bundle() Bundle {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, _ := Encode(b.bundle)
	var value Bundle
	_ = decodeCanonical(data, &value)
	return value
}

// CheckConfig binds the exact config bytes and sealed argument vector before a
// caller constructs a store. Parsing/semantic checks belong to the pure probe.
func (b *BootImage) CheckConfig(config []byte, args []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	digest := sha256.Sum256(config)
	if hex.EncodeToString(digest[:]) != b.bundle.ConfigDigest || !reflect.DeepEqual(args, b.bundle.Args) {
		return ErrImage
	}
	return nil
}
func (b *BootImage) Import(cfg Config) (Imported, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.attempted {
		return nil, ErrImage
	}
	b.attempted = true
	b.imported, b.salvage = nil, tunnel.Salvage{}
	image, salvage, err := Import(cfg, b.bundle, restoredSource{b.restored})
	b.imported, b.salvage = image, salvage
	if err == nil {
		// origin: hot-update S8 plan §5.1 / M08 / §11.2 P17. Adopt the lease while
		// pristine, before Activate starts business or Finish closes metadata.
		err = b.upgrade.Take(b.restored.TakeFile)
	}
	return image, err
}

// UpgradeCut returns the verified adjunct for the caller's authority check
// after OpenCurrent (wired through Config.NewControl, before StartAuth), and
// transfers the adopted lease only once the import holds ACTIVE responsibility.
// origin: hot-update S8 plan §5.1 / §6.1 / §11.2 P17
func (b *BootImage) UpgradeCut() (*upgradecut.State, upgradelock.Lease) {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, _, _, active := CurrentOwners(b.imported)
	return b.upgrade.State(), b.upgrade.Release(active)
}

// Current returns the import transaction's partial owner. Run must handle an
// ACTIVE refusal through a fresh current cut before terminal slot cleanup.
// origin: hot-update S6 plan §4.2 / T49
func (b *BootImage) Current() Imported {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.imported
}

// Repack keeps the original generation. It is legal only before ACTIVE, and it
// merges complete taken salvage with the Restored's untaken files. A successful
// import must first detach its sleeping owners; no normal Close/Shutdown occurs.
// origin: hot-update S6 plan §4.2 / T04
func (b *BootImage) Repack(extra ...handoff.Resource) (*handoff.Prepared, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.imported != nil {
		salvage, err := b.imported.DetachPristine()
		if err != nil {
			return nil, err
		}
		b.imported = nil
		b.salvage = salvage
	}
	custody, err := b.upgrade.Resources() // a taken lease travels on, never unlocked
	defer upgradecut.CloseResources(custody)
	if err != nil {
		return nil, err
	}
	resources, err := handoff.MergeResources(b.restored.Borrow(), b.salvage.Resources, custody, extra)
	if err != nil {
		return nil, err
	}
	if err = handoff.ValidateResourceOwners(resources); err != nil {
		return nil, err
	}
	return handoff.Prepare(b.restored.State, resources)
}

// Close releases held/taken pristine resources after a refused dispatch. ACTIVE
// cleanup belongs to the current runtime slot, so Close must fail without touching
// its business descriptors. An importer with a failed detach remains responsible.
func (b *BootImage) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.imported != nil {
		salvage, err := b.imported.DetachPristine()
		if err != nil {
			return err
		}
		b.salvage = salvage
		b.imported = nil
	}
	var result error
	for _, res := range b.salvage.Resources {
		for _, file := range res.Files {
			if file != nil {
				result = errors.Join(result, file.Close())
			}
		}
	}
	b.salvage = tunnel.Salvage{}
	return errors.Join(result, b.upgrade.Close(), b.restored.Close())
}

// Singleton IDs address their sole file; ID/name explicitly addresses a file of
// a multi-file resource. Neither ownership nor resource metadata is fabricated.
type restoredSource struct{ restored *handoff.Restored }

func (s restoredSource) TakeFile(ref string) (*os.File, error) {
	id, name, explicit := strings.Cut(ref, "/")
	if explicit {
		return s.restored.TakeFile(id, name)
	}
	for _, res := range s.restored.Borrow() {
		if res.ID == id {
			if len(res.Files) != 1 {
				return nil, ErrImage
			}
			for name := range res.Files {
				return s.restored.TakeFile(id, name)
			}
		}
	}
	return nil, ErrImage
}

// borrowedState is exclusively a validation view. sealedstate.Read uses Pread
// and never closes the returned file; ownership remains in Restored throughout.
type borrowedState struct{ resources []handoff.Resource }

func (s borrowedState) TakeFile(id, name string) (*os.File, error) {
	for _, resource := range s.resources {
		if resource.ID == id && resource.Kind == "sealed-state-v1" && len(resource.Files) == 1 {
			if file := resource.Files[name]; file != nil {
				return file, nil
			}
		}
	}
	return nil, ErrImage
}

// CheckInvocation runs before Cobra; a held image cannot execute an unrelated
// command or acquire a cold store merely because incoming argv selects it.
// origin: hot-update S6 plan §4.1 / T03
func (b *BootImage) CheckInvocation(args []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !reflect.DeepEqual(args, b.bundle.Args) {
		return ErrImage
	}
	return nil
}

// Finish funnels pre-Cobra/config/import failures through pristine redispatch.
// ACTIVE owners belong to RuntimeSlot and may only use a new current cut; this
// method never detaches or closes their business files. The previous reader is
// bound to its sealed SHA and probed before any descriptor flags are changed.
// origin: hot-update S6 plan §4.1 / §4.2 / T03
func (b *BootImage) Finish(cause error) error {
	b.mu.Lock()
	image, bundle := b.imported, b.bundle
	b.mu.Unlock()
	if _, _, _, active := CurrentOwners(image); active {
		if cause == nil {
			return b.restored.Close() // release only untaken metadata after current-slot stop
		}
		return errors.Join(cause, ErrActive)
	}
	if cause == nil {
		return b.Close()
	}
	fail := func(err error) error { return errors.Join(cause, err, b.Close()) }
	config, err := BootConfigBytes(bundle.Args)
	if err != nil || b.CheckConfig(config, bundle.Args) != nil {
		return fail(ErrImage)
	}
	exe, err := os.Executable()
	if err != nil {
		return fail(ErrImage)
	}
	previous := strings.TrimSuffix(exe, " (deleted)") + ".prev"
	sha, err := hashArtifact(previous)
	if err != nil || sha != bundle.PreviousSHA256 {
		return fail(ErrImage)
	}
	running, err := hashArtifact("/proc/self/exe")
	// Same reader plus the same argv would repeat the same refusal forever.
	// Correcting a wrong vector is still legal when the reader SHA is equal.
	if err != nil || (running == sha && reflect.DeepEqual(os.Args, bundle.Args)) {
		return fail(ErrImage)
	}
	deadline, err := NewStoreCutDeadline()
	if err != nil {
		return fail(err)
	}
	request := ProbeRequest{Config: config, AllowedArgs: bundle.Args, Schema: bundle.Store.Schema, RequiredDomains: bundle.RequiredDomains}
	input, err := json.Marshal(request)
	if err != nil {
		return fail(ErrImage)
	}
	proof, err := probeReader(previous, input, request, int64(deadline))
	if err != nil || proof.SHA256 != sha {
		return fail(ErrImage)
	}
	prepared, err := b.Repack()
	if err != nil {
		return fail(err)
	}
	// A returned Exec leaves originals with Boot. Reclaim its aliases before
	// closing the pristine owner; successful Exec never reaches either Close.
	err = prepared.Exec(previous, bundle.Args, handoff.CleanEnvironment(os.Environ()))
	return fail(errors.Join(err, prepared.Close()))
}

// upgradeAdjunct is the exporter-side test for the one closed S8 resource that
// is neither a leaf export nor a private kind (see prepareExtras).
func upgradeAdjunct(resource handoff.Resource) bool {
	_, ok, err := upgradecut.Find([]handoff.Resource{resource})
	return ok && err == nil
}

// BootConfigBytes reads only the sealed config pathname; the parser moved to
// brokerupgrade, which derives the broker Key's ConfigPath from the same vector.
// origin: hot-update S8 plan §5.2 / §11.2 P17
func BootConfigBytes(args []string) ([]byte, error) {
	return brokerupgrade.BootConfigBytes(args, MaxBundleBytes, ErrImage)
}
