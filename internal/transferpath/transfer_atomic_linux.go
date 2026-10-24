//go:build linux

package transferpath

// The Linux import hook and POSIX receiver share transfer_atomic_other.go's
// unsupported boundary. On Darwin the portable path owner keeps its default
// unsupportedAtomicImport; no descriptor is taken without Linux validation.
// origin: S7 internal review GATE-CONVERGE-layering F4

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/LinZiyang666/tether/internal/handoff"
	// Transport-free contract values; see transfer_atomic_unix.go.
	xfercontract "github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
	"golang.org/x/sys/unix"
)

// ImportAtomic takes the inherited descriptions exactly once. Partial Take
// (file plus error) is salvage too; no refusal closes or repairs taken files.
// The result sleeps until the manager calls ThawAtomic, so an uninstalled
// importer cannot start writing through the original owner's shared OFD.
// origin: hot-update S7 plan §3 / §8.2 / T10
func importAtomic(state xfercontract.AtomicState, files FileSource) (AtomicReceiver, xfercontract.Salvage, error) {
	if err := validateAtomicState(state); err != nil {
		return nil, xfercontract.Salvage{}, err
	}
	r := &Atomic{state: state, written: state.DurableOffset, changed: make(chan struct{}), frozen: true, imported: true, ops: defaultAtomicOperations()}
	var salvage xfercontract.Salvage
	seen := map[*os.File]bool{}
	for _, item := range []struct {
		ref      xfercontract.ResourceRef
		identity xfercontract.FileIdentity
		target   **os.File
	}{{state.Parent, state.ParentIdentity, &r.parent}, {state.Tmp, state.TmpIdentity, &r.tmp}, {state.Source, state.SourceIdentity, &r.source}} {
		if item.ref == (xfercontract.ResourceRef{}) {
			continue
		}
		if files == nil {
			return nil, salvage, errors.New("transferpath: missing atomic file source")
		}
		file, takeErr := files.TakeFile(item.ref.ResourceID, item.ref.Name)
		if file != nil && !seen[file] {
			seen[file] = true
			salvage.Resources = append(salvage.Resources, handoff.Resource{ID: item.ref.ResourceID, Kind: item.ref.Kind, Files: map[string]*os.File{item.ref.Name: file}})
		} else if file != nil {
			return nil, salvage, errors.New("transferpath: atomic descriptors have duplicate ownership")
		}
		if takeErr != nil {
			return nil, salvage, takeErr
		}
		if file == nil {
			return nil, salvage, errors.New("transferpath: atomic file source returned no descriptor")
		}
		*item.target = file
		identity, size, err := atomicIdentity(file)
		if err != nil {
			return nil, salvage, err
		}
		if identity != item.identity || (item.ref.Kind == xfercontract.ResourceTmp && size != state.DurableOffset) {
			return nil, salvage, errors.New("transferpath: inherited atomic file identity or prefix differs")
		}
	}
	if r.parent != nil {
		fd, err := atomicDescriptor(r.parent)
		if err != nil {
			return nil, salvage, err
		}
		// Readlink is an identity check, never an open of /proc/self/fd. The
		// pinned directory remains the sole authority for leaf operations.
		r.parentPath, err = os.Readlink("/proc/self/fd/" + strconv.Itoa(fd))
		if err != nil || !filepath.IsAbs(r.parentPath) || strings.HasSuffix(r.parentPath, " (deleted)") {
			return nil, salvage, errors.New("transferpath: inherited atomic parent has no canonical path")
		}
		if r.tmp != nil {
			var tmp unix.Stat_t
			if err := unix.Fstatat(fd, state.TmpName, &tmp, unix.AT_SYMLINK_NOFOLLOW); err != nil || uint64(tmp.Dev) != state.TmpIdentity.Dev || tmp.Ino != state.TmpIdentity.Ino {
				return nil, salvage, errors.New("transferpath: inherited tmp name differs from original descriptor")
			}
		}
	}
	// A canceled, no-syscall decision must retain its closed gate after import.
	r.stopping = state.PrimitiveOutcome.Outcome == xfercontract.OutcomeNotEntered && state.PrimitiveOutcome.Code == xfercontract.CodeCanceled
	// A retained Grant already fixed the object incarnation: any later Commit
	// must present that identical Grant. Without one, the importing owner must
	// declare the binding again; until then an empty version stays refused.
	// origin: hot-update S7 plan §8.1 / INTEG-D Tier A grant regression
	if state.CommitGrant != (xfercontract.CommitGrant{}) {
		r.objectBound, r.objectVersion = true, state.CommitGrant.Identity.ObjectVersion
	}
	return r, xfercontract.Salvage{}, nil
}

// Reject malformed closures BEFORE Take. A valid fingerprint cannot make an
// impossible phase or an in-flight primitive safe to export/import.
// origin: hot-update S7 plan §3 / T10
func validateAtomicState(s xfercontract.AtomicState) error {
	if s.DurableOffset < 0 {
		return errors.New("transferpath: negative atomic prefix")
	}
	seen := map[string]bool{}
	for _, item := range []struct {
		ref      xfercontract.ResourceRef
		identity xfercontract.FileIdentity
		kind     string
		name     string
		mode     uint32
		access   int32
	}{{s.Parent, s.ParentIdentity, xfercontract.ResourceParent, xfercontract.ParentFileName, unix.S_IFDIR, unix.O_RDONLY},
		{s.Tmp, s.TmpIdentity, xfercontract.ResourceTmp, xfercontract.TmpFileName, unix.S_IFREG, unix.O_WRONLY},
		{s.Source, s.SourceIdentity, xfercontract.ResourceSource, xfercontract.SourceFileName, unix.S_IFREG, unix.O_RDONLY}} {
		if item.ref == (xfercontract.ResourceRef{}) {
			if item.identity != (xfercontract.FileIdentity{}) {
				return errors.New("transferpath: fingerprint lacks an atomic resource")
			}
			continue
		}
		if !atomicToken(item.ref.ResourceID) || item.ref.Kind != item.kind || item.ref.Name != item.name || seen[item.ref.ResourceID] ||
			item.identity.Ino == 0 || item.identity.Mode&unix.S_IFMT != item.mode || item.identity.Flags&unix.O_ACCMODE != item.access ||
			(item.kind == xfercontract.ResourceTmp && item.identity.Flags&unix.O_APPEND != 0) {
			return errors.New("transferpath: invalid atomic resource closure")
		}
		seen[item.ref.ResourceID] = true
	}
	tmp, parent := s.Tmp.ResourceID != "", s.Parent.ResourceID != ""
	if (tmp && (!parent || !atomicBasename(s.TmpName))) || (parent && !atomicBasename(s.Destination)) {
		return errors.New("transferpath: invalid atomic relative names")
	}
	return validateAtomicDecision(s)
}

func init() {
	importAtomicReceiver = importAtomic
}
