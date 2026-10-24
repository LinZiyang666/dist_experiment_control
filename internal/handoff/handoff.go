// Package handoff carries explicitly quiesced resources through a same-PID
// process image replacement. It does not stop application I/O, arbitrate task
// ownership, or make a crash durable. Those are the runtime owner's duties.
//
// origin: docs/reviews/hot-update-plan.md §4.1.
package handoff

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// ErrUnsupported means this platform cannot import or export a live image.
var ErrUnsupported = errors.New("handoff: unsupported platform")

const (
	environmentKey = "_TETHER_HANDOFF"
	formatVersion  = 1
	maxStateBytes  = 64 << 20
	maxResources   = 16384
	maxFiles       = 16384
)

// State identifies the resource owner and its next execution generation.
// Data is an application-versioned snapshot, not arbitrary Go heap state.
type State struct {
	Role       string          `json:"role"`
	Instance   string          `json:"instance"`
	Generation uint64          `json:"generation"`
	Data       json.RawMessage `json:"data,omitempty"`
}

// Resource names one quiesced object and its explicitly transferable files.
// Prepare borrows the inputs only during the call and owns independent file
// descriptor duplicates afterwards. The underlying open-file descriptions
// (offsets, status flags and socket buffers) are shared with the originals.
// Callers must keep the resource quiescent until Exec succeeds or is abandoned.
// Input descriptors must already have CLOEXEC, or their untracked originals
// would also survive exec. Prepare rejects inheritable inputs without changing
// their flags. Files must not be accessed via File.Fd: that can change shared
// O_NONBLOCK.
type Resource struct {
	ID    string
	Kind  string
	Data  json.RawMessage
	Files map[string]*os.File
}

// Restored owns the imported descriptors until TakeFile transfers each one.
// State and Resources are immutable metadata. Resource.Files is left empty;
// ownership is available only through TakeFile, preventing an accidental
// second owner when the rest of the bundle is closed.
type Restored struct {
	State     State
	Resources []Resource
	mu        sync.Mutex
	files     map[string]map[string]*os.File
	closed    bool
}

// TakeFile removes a file from the bundle and transfers its ownership to the
// caller. Unknown or already transferred names fail rather than returning a
// second reference to a resource that another owner may already be using.
func (r *Restored) TakeFile(id, name string) (*os.File, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("handoff: bundle closed")
	}
	f := r.files[id][name]
	if f == nil {
		return nil, fmt.Errorf("handoff: file %q/%q unavailable", id, name)
	}
	delete(r.files[id], name)
	return f, nil
}

// Borrow returns the files whose ownership has NOT been transferred, grouped
// by resource in the manifest's order, as Resources that Prepare accepts.
// Ownership stays with the Restored: Prepare only duplicates, so a re-dispatch
// built from a Borrow leaves this bundle usable — the same file can still be
// taken (or closed by Close) afterwards, which is what lets an exec that
// failed continue with the image it still holds (hot-update S3 §1.4 P1).
// A closed bundle borrows nothing.
func (r *Restored) Borrow() []Resource {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	var out []Resource
	for _, res := range r.Resources {
		files := r.files[res.ID]
		if len(files) == 0 {
			continue
		}
		borrowed := make(map[string]*os.File, len(files))
		for name, f := range files {
			borrowed[name] = f
		}
		out = append(out, Resource{ID: res.ID, Kind: res.Kind, Data: res.Data, Files: borrowed})
	}
	return out
}

// MergeResources joins resource groups by ID for one Prepare: the files a
// refused import took (a Salvage) and the files it never took (a Borrow)
// name the same resource, and Prepare refuses duplicate IDs. Kind and Data
// must agree, and the same file name from two groups is two owners of one
// descriptor — refused rather than silently dropped.
func MergeResources(groups ...[]Resource) ([]Resource, error) {
	var out []Resource
	index := make(map[string]int)
	for _, group := range groups {
		for _, res := range group {
			i, seen := index[res.ID]
			if !seen {
				merged := Resource{ID: res.ID, Kind: res.Kind, Data: res.Data, Files: make(map[string]*os.File, len(res.Files))}
				for name, f := range res.Files {
					merged.Files[name] = f
				}
				index[res.ID] = len(out)
				out = append(out, merged)
				continue
			}
			if out[i].Kind != res.Kind || !bytes.Equal(out[i].Data, res.Data) {
				return nil, fmt.Errorf("handoff: resource %q described twice with different kind or data", res.ID)
			}
			for name, f := range res.Files {
				if _, dup := out[i].Files[name]; dup {
					return nil, fmt.Errorf("handoff: file %q/%q has two owners", res.ID, name)
				}
				out[i].Files[name] = f
			}
		}
	}
	return out, nil
}

// Close releases only files whose ownership has not been transferred.
func (r *Restored) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	var errs []error
	for _, files := range r.files {
		for _, f := range files {
			errs = append(errs, f.Close())
		}
	}
	r.files = nil
	return errors.Join(errs...)
}

// CleanEnvironment removes private handoff dispatch from an explicit child
// environment. Nil remains nil so callers can preserve their inherit policy.
// Restore also unsets the actual process variable before inspecting its value.
func CleanEnvironment(env []string) []string {
	if env == nil {
		return nil
	}
	out := make([]string, 0, len(env))
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if key != environmentKey {
			out = append(out, entry)
		}
	}
	return out
}

func validateState(s State) error {
	if !validName(s.Role, 64) || !validName(s.Instance, 256) || s.Generation == 0 {
		return fmt.Errorf("handoff: invalid owner identity or generation")
	}
	return validateData(s.Data)
}

func validateData(data json.RawMessage) error {
	if len(data) > maxStateBytes || (len(data) > 0 && !json.Valid(data)) {
		return fmt.Errorf("handoff: invalid or oversized resource state")
	}
	return nil
}

func validName(s string, limit int) bool {
	if len(s) == 0 || len(s) > limit {
		return false
	}
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' {
			continue
		}
		return false
	}
	return true
}

// parseDispatch only parses identity; no file descriptor operations are allowed
// here. Keeping the untrusted environment parser separate permits fuzzing without
// accidentally acting on a fuzz-selected live descriptor in the test process.
func parseDispatch(value string) (int, string, error) {
	fdText, nonce, ok := strings.Cut(value, ":")
	fd, err := strconv.Atoi(fdText)
	if !ok || err != nil || fd < 3 || fdText != strconv.Itoa(fd) || len(nonce) != 64 {
		return 0, "", fmt.Errorf("handoff: invalid dispatch")
	}
	if _, err := hex.DecodeString(nonce); err != nil {
		return 0, "", fmt.Errorf("handoff: invalid snapshot identity")
	}
	return fd, nonce, nil
}
