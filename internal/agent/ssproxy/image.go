package ssproxy

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"runtime"
	"sync"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
)

// The frozen runtime is deliberately opaque to the image owner (§12.1). P8
// calls closeSSImage after latching Thaw/Discard and releasing its runtime lock.
// Keeping cut ownership here avoids copying another owner's private actor state.
var ssImageOwners = struct {
	sync.Mutex
	cuts map[*Frozen]*sealedstate.Snapshot
}{cuts: make(map[*Frozen]*sealedstate.Snapshot)}

type ssImageFile struct {
	id, name, kind string
	fingerprint    rawsocket.Fingerprint
}

// Export emits only opaque references and counts. New cut ownership is installed
// only after encoding and resource-closure validation; a failed export leaves the
// previous cut available to PrepareChecked and rollback.
// origin: hot-update S5 plan §5.1 / §5.3 / T26 / T27
func (f *Frozen) Export() (Image, *sealedstate.Snapshot, error) {
	if runtime.GOOS != "linux" {
		return Image{}, nil, sealedstate.ErrUnsupported
	}
	if f == nil || f.state == nil {
		return Image{}, nil, ErrStateUnrepresentable
	}
	// Replacement and release share this lock, including the runtime's frozen
	// validation. A Thaw that races encoding cannot finish releasing the cut
	// before its new memfd is installed. Failed/terminal exports add no owner.
	ssImageOwners.Lock()
	defer ssImageOwners.Unlock()
	state, err := f.SnapshotState()
	if err != nil {
		return Image{}, nil, ssImageError(err)
	}
	im, files, err := ssImageLayout(state.Records)
	if err != nil {
		return Image{}, nil, err
	}
	resources, err := ssImageBorrowed(state.Resources, files)
	if err != nil {
		return Image{}, nil, err
	}
	data, err := encodeState(state.Records)
	if err != nil {
		return Image{}, nil, ssImageError(err)
	}
	defer clear(data)
	if len(data) == 0 || len(data) > sealedstate.MaxStateBytes {
		return Image{}, nil, ErrCapacity
	}
	var cutID [16]byte
	if _, err := rand.Read(cutID[:]); err != nil {
		return Image{}, nil, ErrStateUnrepresentable
	}
	im.StateRef = "ss-state." + hex.EncodeToString(cutID[:])
	im.SecretBytes = int64(len(data))
	snapshot, err := sealedstate.New(im.StateRef, data)
	if err != nil {
		if snapshot != nil {
			_ = snapshot.Close()
		}
		return Image{}, nil, ssImageError(err)
	}
	if snapshot == nil || len(snapshot.Owned) != 1 || len(snapshot.Resources) != 1 ||
		snapshot.Resources[0].ID != im.StateRef || snapshot.Owned[0] == nil ||
		len(snapshot.Resources[0].Files) != 1 || snapshot.Resources[0].Files["state"] != snapshot.Owned[0] {
		if snapshot != nil {
			_ = snapshot.Close()
		}
		return Image{}, nil, ErrStateUnrepresentable
	}
	// The memfd owner need not choose a public metadata schema. Canonicalize
	// its row as well so no secret can escape via Resource.Data or Kind.
	snapshot.Resources[0] = handoff.Resource{ID: im.StateRef, Kind: "sealed-state-v1",
		Files: map[string]*os.File{"state": snapshot.Owned[0]}}
	snapshot.Resources = append(snapshot.Resources, resources...)
	old := ssImageOwners.cuts[f]
	ssImageOwners.cuts[f] = snapshot
	if old != nil {
		_ = old.Close()
	}
	return im, snapshot, nil
}

// closeSSImage releases sealed originals, never borrowed listener or business
// files. The runtime must first latch its terminal frozen-owner transition, then
// call this outside runtime locks so Export's SnapshotState cannot deadlock.
// origin: hot-update S5 plan §5.3 / T27
func closeSSImage(f *Frozen) error {
	ssImageOwners.Lock()
	defer ssImageOwners.Unlock()
	snapshot := ssImageOwners.cuts[f]
	delete(ssImageOwners.cuts, f)
	if snapshot == nil {
		return nil
	}
	return ssImageError(snapshot.Close())
}

// Restore decodes and checks the complete private closure before taking any
// business FD. Every taken file is returned on failure, including files a later
// actor failure might omit; unconsumed files remain with FileSource for Repack.
// No activation, DNS, Dial or authority renewal happens here.
// origin: hot-update S5 plan §5.1 / §5.3 / §6.5 / T27
func Restore(im Image, src rawsocket.FileSource) (*Server, []handoff.Resource, error) {
	if runtime.GOOS != "linux" {
		return nil, nil, sealedstate.ErrUnsupported
	}
	if im.Version != Version || !ssImageRef(im.StateRef, "ss-state.") ||
		(im.ListenerRef != "" && !ssImageRef(im.ListenerRef, "ss-listener.")) ||
		im.ConnectionCount < 0 || im.ConnectionCount > MaxActors || im.SecretBytes <= 0 ||
		im.SecretBytes > sealedstate.MaxStateBytes || src == nil {
		return nil, nil, ErrStateUnrepresentable
	}
	tracked := &ssImageSource{source: src}
	data, file, err := sealedstate.Read(tracked, im.StateRef, "state", im.SecretBytes)
	if err != nil {
		return nil, tracked.resources, ssImageError(err)
	}
	defer clear(data)
	if file == nil || len(data) == 0 || int64(len(data)) != im.SecretBytes {
		return nil, tracked.resources, ErrStateUnrepresentable
	}
	records, err := decodeState(data)
	if err != nil {
		return nil, tracked.resources, ssImageError(err)
	}
	// A pure decoder may return slices into its input. The runtime owns these
	// copies after restore; wiping the read buffer must not erase live nonces.
	for i := range records {
		for j := range records[i].Fields {
			records[i].Fields[j].Bytes = append([]byte(nil), records[i].Fields[j].Bytes...)
		}
	}
	private, files, err := ssImageLayout(records)
	if err != nil {
		return nil, tracked.resources, err
	}
	if private.ListenerRef != im.ListenerRef || private.ConnectionCount != im.ConnectionCount {
		return nil, tracked.resources, ErrStateUnrepresentable
	}
	for _, want := range files {
		f, err := tracked.TakeFile(want.id, want.name)
		if err != nil {
			return nil, tracked.resources, ssImageError(err)
		}
		if want.name == "listener" {
			// Raw business validation rejects listening sockets. Check the
			// retained listener's identity here; restoreCutState checks its
			// private address, listening state and flags before wrapping it.
			// origin: hot-update S5 plan §5.1 / T27
			fp, err := fileFingerprint(f)
			if err != nil || fp != want.fingerprint {
				return nil, tracked.resources, ErrStateUnrepresentable
			}
		} else if err := rawsocket.ValidateFile(f, want.fingerprint); err != nil {
			return nil, tracked.resources, ssImageError(err)
		}
	}
	business := tracked.resources[1:]
	server, salvage, err := restoreCutState(sealedstate.CutState{Records: records, Resources: business})
	if err != nil {
		// P8 must relinquish any partially adopted wrappers before returning.
		// The tracked closure remains authoritative even if salvage is partial.
		if merged, mergeErr := ssImageSalvage(tracked.resources, salvage); mergeErr == nil {
			return nil, merged, ssImageError(err)
		}
		return nil, tracked.resources, ErrStateUnrepresentable
	}
	if server == nil || len(salvage) != 0 {
		return nil, tracked.resources, ErrStateUnrepresentable
	}
	server.imageState.importedResources = tracked.resources
	return server, nil, nil
}

// Detach transfers a pristine frozen import without shutting down its sockets.
// The returned sealed state is the original cut, rather than a re-export whose
// references or authority decisions could differ from the bundle being repacked.
// origin: hot-update S5 plan §5.3 / §5.4 / T34
func (s *Server) Detach() ([]handoff.Resource, error) {
	s.mu.Lock()
	st := s.imageState
	if s.closed || st == nil || !st.frozen || st.cut != nil || st.workers != 0 || st.acceptRunning || st.importedResources == nil {
		s.mu.Unlock()
		return nil, ErrStateUnrepresentable
	}
	resources := st.importedResources
	st.importedResources = nil
	s.closed = true
	ln, cancel := s.ln, s.stopCancel
	s.ln, st.listenerFile = nil, nil
	// Raw wrappers share the resource's *os.File. Drop their Close authority;
	// the salvage keeps those files reachable until the next owner takes them.
	for _, actor := range st.actors {
		actor.client, actor.upstream, actor.files = nil, nil, nil
	}
	clear(st.actors)
	clear(s.allConns)
	clear(s.keyConns)
	signalStateLocked(st)
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	// FileListener created a separate descriptor. Closing that local wrapper
	// leaves the original listener file in the transferred closure intact.
	if ln != nil {
		_ = ln.Close()
	}
	return resources, nil
}

// Activation/abandon ends pristine rollback responsibility. Business files
// stay with their raw wrappers; only the retained sealed metadata is released.
// Caller holds s.mu. Detach takes the closure instead and never calls this.
// origin: hot-update S5 plan §5.3 / T34
func releaseImportedStateLocked(s *Server) {
	st := s.imageState
	for _, resource := range st.importedResources {
		if resource.Kind == "sealed-state-v1" {
			for _, file := range resource.Files {
				_ = file.Close()
			}
		}
	}
	st.importedResources = nil
}

// FileSource errors can contain arbitrary IDs or decrypted targets. Preserve
// only known typed reasons; formatting an upstream error would disclose them.
// origin: hot-update S5 plan §6.6 / T26
func ssImageError(err error) error {
	if err == nil {
		return nil
	}
	for _, reason := range []error{ErrNotImplemented, ErrCodecNotImplemented,
		ErrStateUnrepresentable, ErrCapacity, ErrAuthorityPending, ErrStopped,
		sealedstate.ErrNotImplemented, sealedstate.ErrUnsupported, sealedstate.ErrCapacity,
		rawsocket.ErrNotImplemented, rawsocket.ErrUnsupported} {
		if errors.Is(err, reason) {
			return reason
		}
	}
	return ErrStateUnrepresentable
}

func ssImageRef(ref, prefix string) bool {
	if len(ref) != len(prefix)+32 || ref[:len(prefix)] != prefix {
		return false
	}
	for _, c := range ref[len(prefix):] {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func ssImageLayout(records []sealedstate.Record) (Image, []ssImageFile, error) {
	im := Image{Version: Version}
	var files []ssImageFile
	servers := 0
	connections := make(map[string]bool)
	for _, record := range records {
		switch record.Kind {
		case sealedstate.KindSSServer:
			servers++
			id, ok := ssImageID(record, "id")
			if !ok {
				return Image{}, nil, ErrStateUnrepresentable
			}
			ref, ok := ssImageField(record, "listener_ref", sealedstate.FieldUTF8)
			if !ok || (len(ref.Bytes) != 0 && string(ref.Bytes) != "ss-listener."+id) {
				return Image{}, nil, ErrStateUnrepresentable
			}
			im.ListenerRef = string(ref.Bytes)
			if im.ListenerRef != "" {
				file, err := ssImageDescriptor(record, "listener", im.ListenerRef, "ss-listener-v1")
				if err != nil {
					return Image{}, nil, err
				}
				files = append(files, file)
			}
		case sealedstate.KindSSActor:
			id, ok := ssImageID(record, "conn_id")
			if !ok || connections[id] {
				return Image{}, nil, ErrStateUnrepresentable
			}
			connections[id] = true
			im.ConnectionCount++
			for _, name := range []string{"loopback", "upstream"} {
				ref, ok := ssImageField(record, name+"_ref", sealedstate.FieldUTF8)
				if !ok || (len(ref.Bytes) != 0 && string(ref.Bytes) != "ss-conn."+id) {
					return Image{}, nil, ErrStateUnrepresentable
				}
				if len(ref.Bytes) == 0 {
					continue
				}
				file, err := ssImageDescriptor(record, name, string(ref.Bytes), "ss-conn-v1")
				if err != nil {
					return Image{}, nil, err
				}
				files = append(files, file)
			}
		case sealedstate.KindMaterial, sealedstate.KindSaltRing, sealedstate.KindAuthority,
			sealedstate.KindTimer, sealedstate.KindBinding, sealedstate.KindPermit, sealedstate.KindReceipt:
			// Their private fields are validated by the binary codec and runtime.
		case sealedstate.KindFlow, sealedstate.KindEndpoint, sealedstate.KindRetirement:
			// A foreign domain must never be accepted as retained SS responsibility.
			return Image{}, nil, ErrStateUnrepresentable
		default:
			return Image{}, nil, ErrStateUnrepresentable
		}
	}
	if servers != 1 || im.ConnectionCount > MaxActors {
		return Image{}, nil, ErrStateUnrepresentable
	}
	return im, files, nil
}

func ssImageID(record sealedstate.Record, name string) (string, bool) {
	field, ok := ssImageField(record, name, sealedstate.FieldID16)
	if !ok || len(field.Bytes) != 16 {
		return "", false
	}
	var nonzero byte
	for _, b := range field.Bytes {
		nonzero |= b
	}
	return hex.EncodeToString(field.Bytes), nonzero != 0
}

func ssImageField(record sealedstate.Record, name string, kind uint8) (sealedstate.Field, bool) {
	var out sealedstate.Field
	found := false
	for _, field := range record.Fields {
		if field.Name == name {
			if found || field.Type != kind {
				return sealedstate.Field{}, false
			}
			out, found = field, true
		}
	}
	return out, found
}

func ssImageDescriptor(record sealedstate.Record, name, id, kind string) (ssImageFile, error) {
	dev, d := ssImageField(record, name+"_dev", sealedstate.FieldU64)
	ino, i := ssImageField(record, name+"_ino", sealedstate.FieldU64)
	typ, m := ssImageField(record, name+"_type", sealedstate.FieldU64)
	if !d || !i || !m || ino.U64 == 0 || typ.U64 > 1<<32-1 {
		return ssImageFile{}, ErrStateUnrepresentable
	}
	return ssImageFile{id: id, name: name, kind: kind,
		fingerprint: rawsocket.Fingerprint{Dev: dev.U64, Ino: ino.U64, Type: uint32(typ.U64)}}, nil
}

// Rebuild public rows from declared refs instead of copying runtime Data/Kind.
// Only private records can carry keys, tuples, targets or pending bytes.
// origin: hot-update S5 plan §5.1 / §6.6 / T26
func ssImageBorrowed(resources []handoff.Resource, files []ssImageFile) ([]handoff.Resource, error) {
	available := make(map[string]*os.File)
	for _, resource := range resources {
		for name, file := range resource.Files {
			key := resource.ID + "/" + name
			if file == nil || available[key] != nil {
				return nil, ErrStateUnrepresentable
			}
			available[key] = file
		}
	}
	var out []handoff.Resource
	for _, want := range files {
		key := want.id + "/" + want.name
		file := available[key]
		if file == nil {
			return nil, ErrStateUnrepresentable
		}
		delete(available, key)
		out = ssImageAppend(out, want.id, want.name, want.kind, file)
	}
	if len(available) != 0 {
		return nil, ErrStateUnrepresentable
	}
	return out, nil
}

func ssImageAppend(resources []handoff.Resource, id, name, kind string, file *os.File) []handoff.Resource {
	for i := range resources {
		if resources[i].ID == id {
			resources[i].Files[name] = file
			return resources
		}
	}
	return append(resources, handoff.Resource{ID: id, Kind: kind, Files: map[string]*os.File{name: file}})
}

type ssImageSource struct {
	source    rawsocket.FileSource
	resources []handoff.Resource
}

func (s *ssImageSource) TakeFile(id, name string) (*os.File, error) {
	file, err := s.source.TakeFile(id, name)
	if file != nil {
		kind := "ss-conn-v1"
		switch name {
		case "state":
			kind = "sealed-state-v1"
		case "listener":
			kind = "ss-listener-v1"
		}
		s.resources = ssImageAppend(s.resources, id, name, kind, file)
	}
	if err != nil || file == nil {
		if err == nil {
			err = ErrStateUnrepresentable
		}
		return file, ssImageError(err)
	}
	return file, nil
}

func ssImageSalvage(taken, salvage []handoff.Resource) ([]handoff.Resource, error) {
	out := taken
	for _, resource := range salvage {
		for name, file := range resource.Files {
			found := false
			for _, existing := range out {
				if existing.ID == resource.ID && existing.Files[name] != nil {
					if existing.Files[name] != file {
						return nil, ErrStateUnrepresentable
					}
					found = true
				}
			}
			if !found {
				return nil, ErrStateUnrepresentable
			}
		}
	}
	return out, nil
}
