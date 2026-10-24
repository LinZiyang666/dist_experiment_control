package brokerimage

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/xfertrack"
)

// imported owns sleeping tunnel state and every taken descriptor until either
// pristine detachment or ACTIVE. The mutex serializes those mutually exclusive
// terminal choices, including a constructor that returns a partial owner/error.
// origin: hot-update S6 plan §4.2 / T04 / T49
// Constructor errors do not reset active; a retry would create a second writer.
type imported struct {
	mu       sync.Mutex
	cfg      Config
	bundle   Bundle
	private  PrivateState
	files    *trackedSource
	server   tunnel.ServerImported
	store    StoreOwner
	control  ControlOwner
	transfer TransferOwner
	active   bool
	detached bool
}

// Import validates without opening SQL, acquiring a lock, starting transport or
// activating pumps. FileSource singleton refs use the resource ID; multi-file
// refs use ID/name. Boot supplies the adapter over the one Restored envelope.
func Import(cfg Config, b Bundle, source tunnel.FileSource) (Imported, tunnel.Salvage, error) {
	if runtime.GOOS != "linux" {
		return nil, tunnel.Salvage{}, handoff.ErrUnsupported
	}
	if Validate(b) != nil || source == nil || cfg.Mode != b.Mode || cfg.ImportServer == nil || cfg.NewStore == nil || cfg.NewControl == nil || !deadlineAlive(cfg.Deadline) {
		return nil, tunnel.Salvage{}, ErrImage
	}
	tracked := &trackedSource{source: source, resources: make(map[string]handoff.Resource), spec: make(map[string]ResourceOwnership), taken: make(map[string]*os.File)}
	tracked.spec[b.StateResource] = ResourceOwnership{ID: b.StateResource, Kind: "sealed-state-v1", Name: "state"}
	tracked.resources[b.StateResource] = handoff.Resource{ID: b.StateResource, Kind: "sealed-state-v1"}
	image := &imported{cfg: cfg, bundle: b, files: tracked}
	// sealedstate.Read itself returns nonnil+error; registration happens inside
	// the source callback, before this higher layer sees that result.
	data, _, err := sealedstate.Read(privateSource{tracked}, b.StateResource, "state", sealedstate.MaxStateBytes)
	if err != nil {
		return nil, tracked.salvage(), ErrImage
	}
	image.private, err = DecodePrivateState(data)
	if err != nil || CheckPrivateState(image.private, b) != nil {
		return nil, tracked.salvage(), ErrImage
	}
	for _, owner := range image.private.Resources {
		ref := owner.ID + "/" + owner.Name
		if owner.ID == b.StateResource {
			return nil, tracked.salvage(), ErrImage
		} // Self provenance would be circular.
		tracked.spec[ref] = owner
		if existing, ok := tracked.resources[owner.ID]; ok && (existing.Kind != owner.Kind || !bytes.Equal(existing.Data, owner.Data)) {
			return nil, tracked.salvage(), ErrImage
		}
		tracked.resources[owner.ID] = handoff.Resource{ID: owner.ID, Kind: owner.Kind, Data: owner.Data}
	}
	// Install singleton shorthand only when it is unambiguous. File names are
	// never guessed from kind, and the resource metadata remains exact for Repack.
	counts := make(map[string]int)
	for _, owner := range image.private.Resources {
		counts[owner.ID]++
	}
	for _, owner := range image.private.Resources {
		if counts[owner.ID] == 1 {
			tracked.spec[owner.ID] = owner
		}
	}
	lockOwner, ok := tracked.spec[b.LockResource]
	if !ok || lockOwner.Purpose != "store-lock" {
		return nil, tracked.salvage(), ErrImage
	}
	// The lock is taken while pristine; only NewStore may adopt it after ACTIVE.
	if _, err := tracked.TakeFile(b.LockResource); err != nil {
		return nil, tracked.salvage(), ErrImage
	}
	// Sleeping entries are held before either constructor and before any eager
	// scan can mistake the imported origin for an empty tracker. Count=0 does
	// not remove retained outbox/grant/config responsibility.
	// origin: hot-update S7 plan §8.2 / P24
	if b.Transfers != nil {
		if !ownerPointer(cfg.Transfers) {
			return nil, tracked.salvage(), ErrImage
		}
		var salvage tunnel.Salvage
		image.transfer, salvage, err = cfg.Transfers.ImportImage(*b.Transfers, tracked, cfg.Deadline)
		err = errors.Join(err, tracked.include(salvage.Resources))
		if err != nil || !ownerPointer(image.transfer) {
			salvage, detachErr := image.detachLocked()
			if detachErr != nil {
				return image, salvage, errors.Join(ErrImage, err, detachErr)
			}
			return nil, salvage, errors.Join(ErrImage, err)
		}
	}
	var nestedSalvage tunnel.Salvage
	image.server, nestedSalvage, err = cfg.ImportServer(cfg.TunnelConfig, b.Tunnel, tracked)
	err = errors.Join(err, tracked.include(nestedSalvage.Resources))
	if err != nil || image.server == nil {
		salvage, detachErr := image.detachLocked()
		if detachErr != nil {
			return image, salvage, errors.Join(ErrImage, detachErr)
		}
		return nil, salvage, ErrImage
	}
	// Take and identity-check every non-tunnel file too. ImportServer can succeed
	// before this outer validation fails; failure still detaches that sleeping owner.
	for _, owner := range image.private.Resources {
		ref := owner.ID + "/" + owner.Name
		if _, seen := tracked.taken[ref]; seen {
			continue
		}
		if _, err = tracked.TakeFile(ref); err != nil {
			break
		}
	}
	if err == nil {
		for _, descriptor := range b.Listeners {
			file := tracked.file(descriptor.ID)
			if file == nil {
				err = ErrImage
				break
			}
			if err = handoff.ValidateListenerFile(file, descriptor.Device, descriptor.Inode, descriptor.Cookie, descriptor.Family, descriptor.SockType, descriptor.Nonblock); err != nil {
				break
			}
		}
	}
	if err == nil {
		err = handoff.ValidateResourceOwners(tracked.salvage().Resources)
	}
	if err == nil {
		_, err = privateFiles(tracked.salvage().Resources)
	}
	if err != nil {
		salvage, detachErr := image.detachLocked()
		if detachErr != nil {
			return image, salvage, errors.Join(ErrImage, detachErr)
		}
		return nil, salvage, ErrImage
	}
	return image, tunnel.Salvage{}, nil
}

// Activate changes responsibility BEFORE the first potentially side-effecting
// constructor. Even nil+error from an opener cannot authorize pristine Repack.
// origin: hot-update S6 plan §4.2 / T49
func (i *imported) Activate(slot RuntimeSlot, deadline controlbudget.Deadline) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.active {
		return ErrActive
	}
	if i.detached || slot == nil || !deadlineAlive(deadline) {
		return ErrImage
	}
	if i.transfer != nil {
		if err := InstallTransferOwner(slot, i.transfer); err != nil {
			return err
		}
	}
	i.active = true
	i.private.Runtime.Active = true
	// The private broker memfd is decode-only. ACTIVE forbids pristine Repack,
	// so release it now, including on a partial/failed store constructor.
	// origin: hot-update S6 plan §4.2; internal review rv-fd-8
	if state := i.files.file(i.bundle.StateResource); state != nil {
		if err := state.Close(); err != nil {
			return errors.Join(ErrActive, err)
		}
		spec := i.files.spec[i.bundle.StateResource]
		delete(i.files.taken, spec.ID+"/"+spec.Name)
	}
	if i.bundle.Transfers != nil {
		id := i.bundle.Transfers.ResourceID
		if file := i.files.file(id + "/state"); file != nil {
			if err := file.Close(); err != nil {
				return errors.Join(ErrActive, err)
			}
			delete(i.files.taken, id+"/state")
		}
	}
	lock := i.files.file(i.bundle.LockResource)
	var err error
	i.store, err = i.cfg.NewStore(i.bundle.Store, lock, deadline)
	if err != nil || i.store == nil {
		if i.store != nil {
			err = errors.Join(err, slot.Install(i.store, nil, i.server))
		}
		return errors.Join(ErrActive, err)
	}
	i.control, err = i.cfg.NewControl(i.store, deadline)
	// Install partial owners as well; the slot must own their actual stop path.
	installErr := slot.Install(i.store, i.control, i.server)
	if err != nil || installErr != nil || i.control == nil {
		return errors.Join(ErrActive, err, installErr)
	}
	if err = i.control.StartAuth(deadline); err != nil {
		return errors.Join(ErrActive, err)
	}
	if i.transfer != nil {
		if err = i.transfer.Rebind(deadline); err != nil {
			return errors.Join(ErrActive, err)
		}
	}
	if err = i.server.Activate(deadline); err != nil {
		return errors.Join(ErrActive, err)
	}
	if err = i.control.StartBusiness(deadline); err != nil {
		return errors.Join(ErrActive, err)
	}
	return nil
}
func (i *imported) DetachPristine() (tunnel.Salvage, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.detachLocked()
}
func (i *imported) detachLocked() (tunnel.Salvage, error) {
	if i.active {
		return tunnel.Salvage{}, ErrActive
	}
	if i.detached {
		return tunnel.Salvage{}, ErrImage
	}
	if i.transfer != nil {
		salvage, err := i.transfer.DetachImage()
		includeErr := i.files.include(salvage.Resources)
		if err != nil {
			return i.files.salvage(), errors.Join(err, includeErr)
		}
		i.transfer = nil
		if includeErr != nil {
			return i.files.salvage(), includeErr
		}
	}
	if i.server != nil {
		salvage, err := i.server.DetachPristine()
		// A failed detach can itself transfer partial salvage. Register it
		// before checking the error, just as for a nonnil Take result.
		// origin: hot-update S6 plan §4.2 / T04
		includeErr := i.files.include(salvage.Resources)
		if err != nil {
			return i.files.salvage(), errors.Join(err, includeErr)
		}
		i.server = nil
		if includeErr != nil {
			return i.files.salvage(), includeErr
		}
	}
	i.detached = true
	return i.files.salvage(), nil
}

// CurrentOwners exposes partial construction responsibility to the current-cut
// owner. It deliberately supplies no pristine operation once ACTIVE has begun.
func CurrentOwners(image Imported) (StoreOwner, ControlOwner, tunnel.ServerImported, bool) {
	i, ok := image.(*imported)
	if !ok {
		return nil, nil, nil, false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.store, i.control, i.server, i.active
}

// CurrentTransferOwner borrows the fourth responsibility, including sleeping
// and partially ACTIVE imports. ACTIVE failure can only freeze this current
// manager; it cannot restore a pristine checkpoint or construct an empty one.
// origin: hot-update S7 plan §8.2 / P24
func CurrentTransferOwner(image Imported) (xfertrack.Manager, bool) {
	i, ok := image.(*imported)
	if !ok || i == nil {
		return nil, false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.transfer, i.active
}

// ImportedListenerFiles borrows the validated non-tunnel listener files for
// the control constructor. Ownership stays with the image until ACTIVE; the
// current control owner adopts them without rebinding or unlinking their routes.
// origin: hot-update S6 plan §6.1 / rv-fd-7
func ImportedListenerFiles(image Imported) map[string]*os.File {
	i, ok := image.(*imported)
	if !ok {
		return nil
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	files := make(map[string]*os.File, len(i.bundle.Listeners))
	for _, descriptor := range i.bundle.Listeners {
		files[descriptor.ID] = i.files.file(descriptor.ID)
	}
	return files
}
func ImportedPrivateState(image Imported) (PrivateState, error) {
	i, ok := image.(*imported)
	if !ok {
		return PrivateState{}, ErrImage
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	data, err := EncodePrivateState(i.private)
	if err != nil {
		return PrivateState{}, err
	}
	return DecodePrivateState(data)
}

type trackedSource struct {
	source     tunnel.FileSource
	spec       map[string]ResourceOwnership
	resources  map[string]handoff.Resource
	taken      map[string]*os.File
	additional []handoff.Resource
}

func (s *trackedSource) TakeFile(ref string) (*os.File, error) {
	spec, ok := s.spec[ref]
	if !ok {
		return nil, ErrImage
	}
	key := spec.ID + "/" + spec.Name
	if _, seen := s.taken[key]; seen {
		return nil, ErrImage
	}
	file, err := s.source.TakeFile(ref)
	// origin: hot-update S6 plan §4.2 / T04. A nonnil file already belongs to
	// us, even when the source reports an error in the same return.
	if file != nil {
		s.taken[key] = file
	}
	if err != nil || file == nil {
		return file, ErrImage
	}
	if spec.Identity != "" {
		identity, identityErr := handoff.ResourceFileIdentity(file)
		if identityErr != nil || identity != spec.Identity {
			return file, ErrImage
		}
	}
	return file, nil
}
func (s *trackedSource) file(ref string) *os.File {
	spec := s.spec[ref]
	return s.taken[spec.ID+"/"+spec.Name]
}
func (s *trackedSource) salvage() tunnel.Salvage {
	resources := make(map[string]handoff.Resource)
	for key, file := range s.taken {
		id, name, _ := strings.Cut(key, "/")
		resource, seen := resources[id]
		if !seen {
			resource = s.resources[id]
			resource.Files = make(map[string]*os.File)
		}
		resource.Files[name] = file
		resources[id] = resource
	}
	out := tunnel.Salvage{Resources: make([]handoff.Resource, 0, len(resources))}
	for _, res := range resources {
		out.Resources = append(out.Resources, res)
	}
	sort.Slice(out.Resources, func(i, j int) bool { return out.Resources[i].ID < out.Resources[j].ID })
	out.Resources = append(out.Resources, s.additional...)
	return out
}

// Register every returned file even on a malformed metadata/alias receipt.
// Such a receipt cannot be repacked, but its ownership must remain reviewable
// and explicitly closable rather than disappearing behind the merge error.
func (s *trackedSource) include(resources []handoff.Resource) error {
	var result error
	for _, resource := range resources {
		original, ok := s.resources[resource.ID]
		metadataMatches := ok && resource.Kind == original.Kind && bytes.Equal(resource.Data, original.Data)
		if !metadataMatches {
			result = errors.Join(result, ErrImage)
		}
		conflicts := handoff.Resource{ID: resource.ID, Kind: resource.Kind, Data: resource.Data, Files: make(map[string]*os.File)}
		for name, file := range resource.Files {
			key := resource.ID + "/" + name
			if file == nil {
				result = errors.Join(result, ErrImage)
				continue
			}
			// The validated ownership row remains authoritative for an identical
			// taken file. Relabelled leaf salvage must not duplicate that owner
			// and prevent pristine Repack after an otherwise recoverable refusal.
			// origin: hot-update S6 plan §4.2; internal review rv-fd-5
			if previous, exists := s.taken[key]; exists && previous == file {
				continue
			}
			if !metadataMatches {
				conflicts.Files[name] = file
				continue
			}
			_, declared := s.spec[key]
			if !declared {
				spec, exists := s.spec[resource.ID]
				declared = exists && spec.Name == name
			}
			if !declared {
				conflicts.Files[name] = file
				result = errors.Join(result, ErrImage)
				continue
			}
			if previous, ok := s.taken[key]; ok && previous != file {
				conflicts.Files[name] = file
				result = errors.Join(result, ErrImage)
				continue
			}
			s.taken[key] = file
		}
		if len(conflicts.Files) != 0 {
			s.additional = append(s.additional, conflicts)
		}
	}
	return result
}

type privateSource struct{ source *trackedSource }

func (s privateSource) TakeFile(id, name string) (*os.File, error) {
	if name != "state" {
		return nil, ErrImage
	}
	return s.source.TakeFile(id)
}

// ImportedResources borrows the import's descriptors for current runtime wiring.
// The consumer must establish one terminal owner per private ownership row; it
// cannot close these borrowed files while pristine detachment is still possible.
func ImportedResources(image Imported) (tunnel.Salvage, error) {
	i, ok := image.(*imported)
	if !ok {
		return tunnel.Salvage{}, ErrImage
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.detached {
		return tunnel.Salvage{}, ErrImage
	}
	return i.files.salvage(), nil
}

// A cut borrows live lock/listener/raw/replay and leaf-owned sealed files, owns
// only the broker codec's private files and one Prepared's duplicates. Keeping
// those sets apart prevents a cut from closing a surviving business socket.
// origin: hot-update S6 plan §2.2 / §4.1 / §12.1 P01
type resourceGraph struct {
	resources []handoff.Resource
	private   map[*os.File]bool
}

// ValidateResources lets the importer apply the same original-owner alias and
// capacity rules to its complete taken-plus-untaken graph. It borrows every file;
// failure never silently closes a partial Take or a surviving original.
// origin: hot-update S6 plan §4.1 / T27
func ValidateResources(resources []handoff.Resource) error {
	return validateResourceGraph(resources)
}

// validateResourceGraph is deliberately conservative about OFDs. SameFile
// compares actual kernel identities, including socket inodes, rather than file
// names or descriptor numbers. Independent opens of the same inode are rejected
// too: without explicit provenance we cannot prove separate terminal owners.
// Prepared's intentional duplicates are held outside this original-owner graph.
// origin: hot-update S6 plan §4.1 / T25 / T27
func validateResourceGraph(resources []handoff.Resource) error {
	if len(resources) > 16384 {
		return errors.New("broker image: resource limit exceeded")
	}
	ids := make(map[string]bool, len(resources))
	var infos []os.FileInfo
	dataBytes := 0
	for _, resource := range resources {
		if !resourceName(resource.ID, 256) || !resourceName(resource.Kind, 64) || ids[resource.ID] {
			return errors.New("broker image: invalid or duplicate resource")
		}
		ids[resource.ID] = true
		dataBytes += len(resource.Data)
		if dataBytes > 64<<20 || len(resource.Data) > 0 && !json.Valid(resource.Data) {
			return errors.New("broker image: invalid resource metadata")
		}
		for name, file := range resource.Files {
			if !resourceName(name, 64) || file == nil || len(infos) >= 16384 {
				return errors.New("broker image: invalid resource file or file limit exceeded")
			}
			info, err := file.Stat()
			if err != nil {
				return errors.New("broker image: resource file unavailable")
			}
			for _, previous := range infos {
				if os.SameFile(previous, info) {
					return errors.New("broker image: resource has multiple terminal owners")
				}
			}
			infos = append(infos, info)
		}
	}
	return nil
}

// privateFiles validates sealed state capacity without transferring ownership.
// The caller owns only the broker codec extras; ServerFrozen retains ownership
// of its tunnel/flow snapshots. No secret bytes are inspected here.
func privateFiles(resources []handoff.Resource) (map[*os.File]bool, error) {
	files := make(map[*os.File]bool)
	for _, resource := range resources {
		if resource.Kind != "sealed-state-v1" && resource.Kind != "xfer-state-v1" {
			continue
		}
		for _, file := range resource.Files {
			if file == nil || files[file] {
				continue
			}
			files[file] = true // Collect the entire partial export before checking it.
		}
	}
	var size int64
	for file := range files {
		info, err := file.Stat()
		if err != nil || info.Size() <= 0 || info.Size() > sealedstate.MaxStateBytes-size {
			return files, errors.New("broker image: private state unavailable or oversized")
		}
		size += info.Size()
	}
	return files, nil
}

func closePrivate(files map[*os.File]bool) error {
	var result error
	for file := range files {
		if err := file.Close(); err != nil {
			// os.PathError contains a caller-controlled name. Resource errors
			// deliberately expose neither names nor private state.
			result = errors.Join(result, errors.New("broker image: private file close failed"))
		}
		delete(files, file)
	}
	return result
}

func copyResources(resources []handoff.Resource) []handoff.Resource {
	out := make([]handoff.Resource, len(resources))
	for i, resource := range resources {
		out[i] = handoff.Resource{ID: resource.ID, Kind: resource.Kind,
			Data: append([]byte(nil), resource.Data...), Files: make(map[string]*os.File, len(resource.Files))}
		for name, file := range resource.Files {
			out[i].Files[name] = file
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Share the codec's exact ASCII grammar while retaining each resource limit.
// origin: hot-update S7 plan §8.3 / P24
func resourceName(name string, limit int) bool { return len(name) <= limit && validID(name) }

func retainedFlow(image tunnel.ServerImage) bool {
	return len(image.Listeners) != 0 || len(image.Endpoints) != 0 || image.Flows.Version != 0 ||
		len(image.Flows.Records) != 0 || len(image.Flows.Retirement) != 0 || image.Flows.StateRef != "" ||
		image.Flows.ReservedBytes != 0 || image.ReplayUsed != 0
}

func sameProjection(bundle, current Bundle) bool {
	return bundle.Schema == current.Schema && bundle.SchemaVersion == current.SchemaVersion &&
		bundle.Role == current.Role && bundle.Mode == current.Mode && bundle.LockResource == current.LockResource &&
		reflect.DeepEqual(bundle.Store, current.Store) && reflect.DeepEqual(bundle.Seal, current.Seal) &&
		reflect.DeepEqual(bundle.Listeners, current.Listeners) && reflect.DeepEqual(bundle.Tunnel, current.Tunnel) &&
		reflect.DeepEqual(bundle.Transfers, current.Transfers)
}

func prepareExtras(bundle Bundle, supplied, exported []handoff.Resource) ([]handoff.Resource, error) {
	index := make(map[string]handoff.Resource, len(exported))
	for _, resource := range exported {
		index[resource.ID] = resource
	}
	var extra []handoff.Resource
	seen := make(map[string]bool)
	for _, resource := range supplied {
		if seen[resource.ID] {
			return nil, errors.New("broker image: duplicate prepare resource")
		}
		seen[resource.ID] = true
		if current, ok := index[resource.ID]; ok {
			if !reflect.DeepEqual(resource, current) {
				return nil, errors.New("broker image: prepare resource differs from export")
			}
			continue
		}
		// origin: hot-update S8 plan §6.1 / §11.2 P17. Besides broker state, the
		// only unexported resource is the closed S8 adjunct with its borrowed
		// lease; it is not a private kind, so the cut never claims its file.
		if !upgradeAdjunct(resource) && (resource.ID != bundle.StateResource || resource.Kind != "sealed-state-v1" || len(resource.Files) != 1 || resource.Files["state"] == nil) {
			return nil, errors.New("broker image: unowned prepare resource")
		}
		extra = append(extra, resource)
	}
	// Every exported resource exactly once, plus the unexported state and at
	// most the one adjunct (both have fixed, unique IDs).
	if len(seen) != len(index)+len(extra) || !seen[bundle.StateResource] || index[bundle.StateResource].ID != "" {
		return nil, errors.New("broker image: incomplete prepare resource graph")
	}
	return extra, nil
}

func cloneStamp(stamp storage.StoreStamp) storage.StoreStamp {
	stamp.Schema.Applied = append([]storage.MigrationDigest{}, stamp.Schema.Applied...)
	stamp.Schema.Supported = append([]storage.MigrationDigest{}, stamp.Schema.Supported...)
	return stamp
}

func cloneSeal(seal storage.StoreSeal) storage.StoreSeal {
	seal.Stamp = cloneStamp(seal.Stamp)
	return seal
}

func cloneBundle(bundle Bundle) Bundle {
	// Bundle contains only public metadata. A JSON copy also severs nested flow
	// projections; a validation callback cannot mutate the sealed exec identity.
	data, _ := json.Marshal(bundle)
	var copy Bundle
	_ = json.Unmarshal(data, &copy)
	return copy
}
