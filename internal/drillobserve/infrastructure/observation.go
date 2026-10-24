// Infrastructure observation owns the retained source registry and public fragments.
// origin: S10 internal review MAKETEST-GATES F8
package infrastructure

import (
	"crypto/sha256"
	"errors"
	"os"
	"sort"
	"sync"

	"github.com/LinZiyang666/tether/internal/drillobserve/receipt"
)

const InfrastructureAttachmentKind = "infra-attachment"
const InfrastructurePrefixKind = "infra-prefix"
const MaxSources = 128
const MaxInfrastructureReceipt = 4 << 20

var ErrInfrastructureObservation = errors.New("drillobserve: infrastructure observation incomplete")

type Registry struct {
	sync.Mutex
	Rows   map[[16]byte]func(int64) error
	Broken bool
}

// RegisterInfrastructure keeps only retained, original NC owners. It is inert
// without authenticated startup custody. No timer, socket, file, or business
// lifetime is created by registration; the custodian requests fresh samples.
// Re-registration belongs to the same participant after ordinary reconnect.
// origin: hot-update S9 plan §2.3 / §4.5; s9-instr4 Request 1
func (r *Registry) Register(id [16]byte, sample func(int64) error, enabled bool) {
	if !enabled {
		return
	}
	r.Lock()
	defer r.Unlock()
	if id == ([16]byte{}) || sample == nil || (r.Rows[id] == nil && len(r.Rows) == MaxSources) {
		r.Broken = true
		return
	}
	r.Rows[id] = sample
}

func (r *Registry) Unregister(id [16]byte) {
	r.Lock()
	delete(r.Rows, id)
	r.Unlock()
}

// SampleInfrastructure runs sequentially under ONE absolute request budget.
// Owners retain their own close/reconnect locks; custody never holds the
// registry lock while invoking a borrower. An empty or overflowed roster
// cannot become a complete observation. Product IO never depends on this call.
func (r *Registry) Sample(d int64, supported, enabled bool, now func() (int64, error)) error {
	if !supported {
		return ErrInfrastructureObservation
	}
	if !enabled {
		return ErrInfrastructureObservation
	}
	r.Lock()
	ids := make([][16]byte, 0, len(r.Rows))
	for id := range r.Rows {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return string(ids[i][:]) < string(ids[j][:]) })
	sources := make([]func(int64) error, 0, len(ids))
	for _, id := range ids {
		sources = append(sources, r.Rows[id])
	}
	broken := r.Broken
	r.Unlock()
	if broken || len(sources) == 0 {
		return ErrInfrastructureObservation
	}
	for _, sample := range sources {
		if !receipt.BeforeInfrastructureDeadline(d, now) {
			return ErrInfrastructureObservation
		}
		if err := sample(d); err != nil {
			return ErrInfrastructureObservation
		}
	}
	if !receipt.BeforeInfrastructureDeadline(d, now) {
		return ErrInfrastructureObservation
	}
	return nil
}

// RecordInfrastructure fragments a complete public row rather than truncating
// its chunk inventory. Values bind byte offset, total and actual plan step;
// Actor is the participant or recovery transaction. Digest binds the whole
// canonical JSON row. Readers must reassemble every byte and verify the
// digest before projecting the P32 schema. Overflow retains custody's poison.
// origin: hot-update S9 plan §2.2 / §4.3; s9-instr4 Requests 1–2
func RecordInfrastructure(kind string, id [16]byte, step uint32, data []byte, hook func(receipt.Event)) error {
	if hook == nil {
		return nil
	}
	if (kind != InfrastructureAttachmentKind && kind != InfrastructurePrefixKind) || len(data) == 0 || len(data) > MaxInfrastructureReceipt {
		return ErrInfrastructureObservation
	}
	domain := "infra"
	if kind == InfrastructurePrefixKind {
		domain = "store"
	}
	digest := sha256.Sum256(data)
	for offset := 0; offset < len(data); offset += receipt.MaxData {
		end := min(offset+receipt.MaxData, len(data))
		hook(receipt.Event{Kind: kind, Domain: domain, Actor: id, Digest: digest,
			Values: [16]uint64{uint64(offset), uint64(len(data)), uint64(step)}, Data: append([]byte(nil), data[offset:end]...)})
	}
	return nil
}

// RecordAttachment projects only public physical identity. Ordinary releases
// allocate nothing here; missing or credential-bearing evidence is refused.
// origin: hot-update S9 plan §2.3; s9-instr4 Request 1
func RecordAttachment(a receipt.AttachmentProjection, hook func(receipt.Event)) error {
	if hook == nil {
		return nil
	}
	id, data, err := receipt.InfrastructureAttachment(a)
	if err != nil {
		return err
	}
	return RecordInfrastructure(InfrastructureAttachmentKind, id, 0, data, hook)
}

// ProcessFDCount returns the number of open file descriptors for this process by counting entries in
// /proc/self/fd. Returns -1 when that directory cannot be read (non-Linux, or /proc not mounted) so
// the caller reports "unavailable" rather than a fabricated 0. The directory handle is CLOSED before
// returning — a leak probe that leaked an fd on every call would be its own worst bug.
func ProcessFDCount() int {
	d, err := os.Open("/proc/self/fd")
	if err != nil {
		return -1
	}
	defer func() { _ = d.Close() }()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return -1
	}
	// Readdirnames includes the fd for the open directory handle itself; subtract it so the count is
	// the process's steady-state open fds, not "+1 while this probe runs".
	if n := len(names) - 1; n >= 0 {
		return n
	}
	return len(names)
}
