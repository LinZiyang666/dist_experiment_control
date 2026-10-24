// image.go — the runtime's versioned state ABI (image schema v1).
//
// An Image is the JSON half of a runtime cut: identities, offsets,
// capacities, phases and terminal results. The bytes themselves never enter
// it — every replay ring and pipe travels as a descriptor in the
// handoff.Resource list Export returns beside the Image, one Resource per
// task with at most six named files. The registry's HMAC secret is inside
// Image.Registry and therefore inside the image: the image may only ever be
// written into handoff's sealed anonymous snapshot, never to a log, a marker
// file or an error string (procrequest.Secret redacts every fmt rendering
// as a belt).
//
// N-1 rule for this schema (architecture §22.5): within Version 1 fields
// are only added, with omitempty and a legal zero value; unknown fields are
// refused on import (DisallowUnknownFields — an image carrying state this
// binary does not understand must not be activated half-understood); a
// Version mismatch is refused outright.
// origin: docs/reviews/hot-update-agent-image-abi-plan.md §2.
package execsession

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"sync"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

// ImageVersion is the schema epoch of Image. A bump is an epoch change: the
// previous image cannot be read, so the switch must be a fresh start.
const ImageVersion = 1

// Resource naming: one handoff.Resource per task, ID "exec-task.<pid>",
// Kind "exec-task", files named by stream and role. handoff validates names
// against [A-Za-z0-9._-], which every ULID task ID satisfies.
const (
	resourceKind     = "exec-task"
	resourcePrefix   = "exec-task."
	fileStdinReplay  = "stdin-replay"
	fileStdinTarget  = "stdin-target"
	fileStdoutReplay = "stdout-replay"
	fileStdoutSource = "stdout-source"
	fileStderrReplay = "stderr-replay"
	fileStderrSource = "stderr-source"
)

func resourceID(pid string) string { return resourcePrefix + pid }

func streamFileNames(i int) (replay, source string) {
	if i == 0 {
		return fileStdoutReplay, fileStdoutSource
	}
	return fileStderrReplay, fileStderrSource
}

// Image is the exported runtime cut. Registry carries the request table
// including its signing secret; Tasks carries every live and terminal task
// the runtime still owns; Tombstones and Revocations carry the two
// ownership decisions that outlive their tasks.
type Image struct {
	Version        int               `json:"version"`
	Instance       string            `json:"instance"`
	FrozenAtBootNS int64             `json:"frozen_at_boot_ns"`
	ReplayCapacity int64             `json:"replay_capacity"`
	BudgetLimit    int64             `json:"budget_limit,omitempty"`
	BudgetUsed     int64             `json:"budget_used"`
	Registry       procrequest.State `json:"registry"`
	Tasks          []TaskImage       `json:"tasks"`
	Tombstones     []TombstoneImage  `json:"tombstones,omitempty"`
	Revocations    []RevocationImage `json:"revocations,omitempty"`
}

// TaskImage is one task's metadata. Its phase is not exported: it is derived
// on import from the registry record (running or terminal), which closes
// the window between finish (record terminal) and conclude (done closed).
// Files maps every exported file name to its kernel identity so an import
// can prove each taken descriptor is the one that was exported.
type TaskImage struct {
	Kind         string                  `json:"kind,omitempty"`
	PTY          *ptysession.Image       `json:"pty,omitempty"`
	PID          string                  `json:"pid"`
	Handle       proto.ProcessHandle     `json:"handle"`
	Abort        [2]string               `json:"abort,omitempty"`
	Revoked      string                  `json:"revoked,omitempty"`
	LostTerminal bool                    `json:"lost_terminal,omitempty"`
	TerminalAt   int64                   `json:"terminal_at,omitempty"`
	LastTouch    int64                   `json:"last_touch"`
	Stdin        *StreamImage            `json:"stdin,omitempty"`
	Stdout       *StreamImage            `json:"stdout,omitempty"`
	Stderr       *StreamImage            `json:"stderr,omitempty"`
	Files        map[string]FileIdentity `json:"files"`
}

// StreamImage is one stream in one of three shapes: bare log (Output and
// Input nil — the pump was never built), output actor, or input actor.
type StreamImage struct {
	Replay workload.ReplayState  `json:"replay"`
	Output *workload.OutputState `json:"output,omitempty"`
	Input  *workload.InputState  `json:"input,omitempty"`
}

// FileIdentity is the kernel identity of an exported descriptor.
type FileIdentity struct {
	Dev uint64 `json:"dev"`
	Ino uint64 `json:"ino"`
}

// TombstoneImage is a discarded task's terminal and final offsets.
type TombstoneImage struct {
	Kind     string                `json:"kind,omitempty"`
	PTY      *ptysession.Tombstone `json:"pty,omitempty"`
	PID      string                `json:"pid"`
	Handle   proto.ProcessHandle   `json:"handle"`
	Terminal procrequest.Terminal  `json:"terminal"`
	FinalEnd [2]uint64             `json:"final_end"`
	FinalFIN [2]bool               `json:"final_fin"`
	AtBootNS int64                 `json:"at_boot_ns"`
}

// RevocationImage is one session revocation still within its horizon.
type RevocationImage struct {
	ClockUnknown bool   `json:"clock_unknown,omitempty"`
	SID          string `json:"sid"`
	AtBootNS     int64  `json:"at_boot_ns"`
	Reason       string `json:"reason"`
}

// Export reads a frozen runtime into an Image and the borrowed files
// handoff.Prepare duplicates. The runtime stays frozen and becomes
// "exported": from now on its own copies may only be Discarded (the switch
// happened) or Thawed (it did not). Export may run again on the same frozen
// runtime — a rollback re-exports the current state.
func (r *Runtime) Export() (Image, []handoff.Resource, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return Image{}, nil, ErrClosed
	}
	if !r.frozen {
		return Image{}, nil, ErrNotFrozen
	}
	// Other domains may already reserve the shared node budget; only this
	// runtime's capacities belong to this image's conservation equation.
	// origin: hot-update S5 plan §3.4 / T30
	img := Image{Version: ImageVersion, Instance: r.cfg.Instance, FrozenAtBootNS: r.frozenAt,
		ReplayCapacity: r.cfg.ReplayCapacity, BudgetLimit: r.sharedReplayBudget.Limit, BudgetUsed: ownedReplayBytes(r),
		Registry: r.frozenRegistry, Tasks: make([]TaskImage, 0, len(r.tasks))}
	resources := make([]handoff.Resource, 0, len(r.tasks))
	for _, t := range r.sortedTasks() {
		tf := t.frozen
		if tf == nil {
			return Image{}, nil, fmt.Errorf("%w: task %s has no frozen state", ErrImage, t.pid)
		}
		ti := TaskImage{Kind: t.kind, PTY: tf.pty, PID: t.pid, Handle: t.handle, Abort: t.abort, Revoked: t.revoked, LostTerminal: t.lostTerminal,
			TerminalAt: t.terminalAt, LastTouch: t.lastTouch, Files: make(map[string]FileIdentity, len(tf.files))}
		if ti.PTY != nil {
			st := *ti.PTY
			if t.revoked != "" {
				st.Revoked = t.revoked
				st.CleanupPending = st.CleanupPending || t.terminalAt == 0
			}
			ti.Revoked = st.Revoked
			ti.PTY = &st
		}
		if tf.have[0] {
			ti.Stdin = &StreamImage{Replay: tf.replay[0], Input: tf.input}
		}
		if tf.have[1] {
			ti.Stdout = &StreamImage{Replay: tf.replay[1], Output: tf.output[0]}
		}
		if tf.have[2] {
			ti.Stderr = &StreamImage{Replay: tf.replay[2], Output: tf.output[1]}
		}
		for name, file := range tf.files {
			id, err := fileIdentity(file)
			if err != nil {
				return Image{}, nil, fmt.Errorf("%w: identify %s of task %s: %v", ErrImage, name, t.pid, err)
			}
			ti.Files[name] = id
		}
		img.Tasks = append(img.Tasks, ti)
		files := make(map[string]*os.File, len(tf.files))
		for name, file := range tf.files {
			files[name] = file
		}
		resources = append(resources, taskResource(t.kind, t.pid, files))
	}
	for pid, ts := range r.discarded {
		img.Tombstones = append(img.Tombstones, TombstoneImage{Kind: ts.kind, PTY: ts.pty, PID: pid, Handle: ts.handle, Terminal: ts.terminal,
			FinalEnd: ts.finalEnd, FinalFIN: ts.finalFIN, AtBootNS: ts.atBootNS})
	}
	sort.Slice(img.Tombstones, func(i, j int) bool { return img.Tombstones[i].PID < img.Tombstones[j].PID })
	for sid, rev := range r.revocations {
		img.Revocations = append(img.Revocations, RevocationImage{ClockUnknown: rev.clockUnknown, SID: sid, AtBootNS: rev.atBootNS, Reason: rev.reason})
	}
	sort.Slice(img.Revocations, func(i, j int) bool { return img.Revocations[i].SID < img.Revocations[j].SID })
	r.exported = true
	return img, resources, nil
}

// fileIdentity reads a descriptor's kernel identity without File.Fd (which
// could change the shared O_NONBLOCK).
func fileIdentity(file *os.File) (FileIdentity, error) {
	raw, err := file.SyscallConn()
	if err != nil {
		return FileIdentity{}, err
	}
	var st unix.Stat_t
	var statErr error
	if err := raw.Control(func(fd uintptr) { statErr = unix.Fstat(int(fd), &st) }); err != nil {
		return FileIdentity{}, err
	}
	if statErr != nil {
		return FileIdentity{}, statErr
	}
	return FileIdentity{Dev: uint64(st.Dev), Ino: st.Ino}, nil
}

// DecodeImage parses an image strictly: unknown fields and trailing data are
// refused, so state a newer exporter added and this binary does not
// understand can never be activated half-read.
func DecodeImage(data []byte) (Image, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var img Image
	if err := dec.Decode(&img); err != nil {
		return Image{}, fmt.Errorf("%w: decode: %v", ErrImage, err)
	}
	if dec.More() {
		return Image{}, fmt.Errorf("%w: trailing data", ErrImage)
	}
	return img, nil
}

// FileSource hands out an exported file exactly once. *handoff.Restored
// satisfies it; the in-process round trip uses a duplicate-backed fake.
type FileSource interface {
	TakeFile(id, name string) (*os.File, error)
}

// Import builds a frozen, imported runtime from an Image, the process-table
// rows the same cut exported (live rows must belong to running tasks, and
// every running task must have one) and the exported files. It runs in
// three stages so that no failure leaves a half-installed runtime and no
// business descriptor is ever closed here:
//
//  1. pure validation of the image against the rows and cfg (every
//     rejection in the S2 plan §2.3 table lives here or in stage 2);
//  2. TakeFile of every expected descriptor with read-only predicates
//     (kernel identity, ring/pipe shape) — on failure the files already
//     taken are returned as Prepare-ready resources (salvage);
//  3. construction of the registry, logs and paused actors — on failure
//     (an OS-level surprise) every built actor is detached back into the
//     salvage the same way.
//
// The process table is only read (rows); Import never signals, reaps or
// forgets anything. cfg.Manager must be the table that will hold those rows
// (restored by the caller AFTER Import succeeds, so RestoreProcesses is the
// last failable step). cfg.Instance is filled from the image when empty and
// must match it otherwise: a restored runtime keeps issuing under the
// exported identity or every earlier lease dies with ErrInstance.
func Import(cfg Config, img Image, rows []workload.Record, files FileSource) (*Runtime, []handoff.Resource, error) {
	if cfg.Manager == nil || cfg.Start == nil || cfg.Started == nil || cfg.Exited == nil {
		return nil, nil, fmt.Errorf("exec session: incomplete resource adapter")
	}
	if cfg.ReplayCapacity == 0 {
		cfg.ReplayCapacity = workload.DefaultReplayCapacity
	}
	// Refuse a malformed shared bound before registry/descriptor ownership.
	// origin: hot-update S5 plan §3.4 / T30
	if cfg.ReplayCapacity < 1 || cfg.ReplayCapacity > workload.DefaultReplayCapacity || cfg.ReplayBudget < 0 || cfg.SharedReplayBudget != nil && cfg.SharedReplayBudget.Limit < 0 {
		return nil, nil, fmt.Errorf("exec session: invalid replay capacity or budget")
	}
	if cfg.Clock == nil {
		cfg.Clock = procrequest.BootNow
	}
	if cfg.Instance == "" {
		cfg.Instance = img.Instance
	}
	// Stage 1 — pure validation. RestoreWithClock is the registry's own
	// full-table validation; its only side effect is an allocation.
	registry, err := procrequest.RestoreWithClock(img.Registry, cfg.Clock)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: registry: %v", ErrImage, err)
	}
	plan, err := validateImage(cfg, img, rows)
	if err != nil {
		return nil, nil, err
	}
	// Stage 2 — take and check descriptors; never close one.
	taken := make([]handoff.Resource, 0, len(img.Tasks))
	salvage := func() []handoff.Resource { return taken }
	for _, ti := range img.Tasks {
		res := taskResource(ti.Kind, ti.PID, make(map[string]*os.File, len(ti.Files)))
		taken = append(taken, res)
		for _, name := range plan.names[ti.PID] {
			file, err := files.TakeFile(res.ID, name)
			if err != nil {
				return nil, salvage(), fmt.Errorf("%w: task %s: take %s: %v", ErrImage, ti.PID, name, err)
			}
			res.Files[name] = file
			if err := checkImageFile(ti, name, file, rows); err != nil {
				return nil, salvage(), fmt.Errorf("%w: task %s: %s: %v", ErrImage, ti.PID, name, err)
			}
		}
	}
	// Stage 3 — construction. Nothing runs yet: every actor is paused, no
	// observer exists, the registry is frozen.
	r := &Runtime{cfg: cfg, registry: registry, tasks: make(map[string]*task, len(img.Tasks)),
		discarded: make(map[string]*tombstone, len(img.Tombstones)), revocations: make(map[string]revocation, len(img.Revocations)),
		now: cfg.Clock, lastBootNS: img.FrozenAtBootNS, frozen: true, imported: true, frozenAt: img.FrozenAtBootNS, frozenRegistry: img.Registry}
	r.cond = sync.NewCond(&r.mu)
	initializeReplayBudget(r)
	for i, ti := range img.Tasks {
		var t *task
		var err error
		if ti.Kind == proto.ProcessKindPTY {
			t, err = buildPTYTaskWithRows(r, ti, plan.phase[ti.PID], taken[i].Files, rows)
		} else {
			t, err = buildTask(r, ti, plan.phase[ti.PID], taken[i].Files)
		}
		if err != nil {
			// Detach what this task built (buildTask detached its own
			// partial work into taken[i].Files already) and every earlier task.
			for _, u := range r.tasks {
				err = errors.Join(err, detachInto(u, taken))
			}
			return nil, salvage(), fmt.Errorf("%w: task %s: %v", ErrImage, ti.PID, err)
		}
		r.tasks[ti.PID] = t
	}
	for _, ts := range img.Tombstones {
		r.discarded[ts.PID] = &tombstone{kind: ts.Kind, pty: ts.PTY, scope: scopeOf(ts.Handle), handle: ts.Handle, terminal: ts.Terminal,
			finalEnd: ts.FinalEnd, finalFIN: ts.FinalFIN, atBootNS: ts.AtBootNS}
	}
	for _, rev := range img.Revocations {
		r.revocations[rev.SID] = revocation{atBootNS: rev.AtBootNS, reason: rev.Reason, clockUnknown: rev.ClockUnknown}
	}
	// The node may already hold flow reservations. Import conserves this
	// domain's capacity, while ReplayBudget guards the aggregate atomically.
	// origin: hot-update S5 plan §3.4 / T30
	if used := ownedReplayBytes(r); used != img.BudgetUsed {
		var detachErr error
		for _, u := range r.tasks {
			detachErr = errors.Join(detachErr, detachInto(u, taken))
		}
		return nil, salvage(), errors.Join(fmt.Errorf("%w: budget after import %d, exported %d", ErrImage, used, img.BudgetUsed), detachErr)
	}
	return r, nil, nil
}

// importPlan is what stage 1 derives for stage 2/3: the expected file names
// per task (sorted, so TakeFile order is deterministic), each task's phase
// from its registry record, and whether it is lost-terminal.
type importPlan struct {
	names map[string][]string
	phase map[string]string
	lost  map[string]bool
}

// imageReject formats one rejection; every text is static words plus
// identifiers, never a rendering of any state (the registry key lives there).
func imageReject(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrImage}, args...)...)
}

// validateImage is stage 1: the image against the rows and cfg, pure.
func validateImage(cfg Config, img Image, rows []workload.Record) (importPlan, error) {
	if img.Version != ImageVersion {
		return importPlan{}, imageReject("schema version %d, this binary reads %d", img.Version, ImageVersion)
	}
	if img.Instance == "" || img.Instance != img.Registry.Instance || img.Instance != cfg.Instance {
		return importPlan{}, imageReject("instance identity does not match the registry or the adapter")
	}
	if img.FrozenAtBootNS <= 0 {
		return importPlan{}, imageReject("missing freeze instant")
	}
	now, err := cfg.Clock()
	if err != nil || img.FrozenAtBootNS > now {
		return importPlan{}, imageReject("freeze instant is in the future or the clock failed")
	}
	if img.ReplayCapacity < 1 || img.ReplayCapacity > workload.DefaultReplayCapacity {
		return importPlan{}, imageReject("invalid replay capacity")
	}
	records := make(map[string]procrequest.Record, len(img.Registry.Records))
	for _, rec := range img.Registry.Records {
		records[rec.PID] = rec // PID uniqueness was proved by RestoreWithClock
	}
	plan := importPlan{names: make(map[string][]string, len(img.Tasks)), phase: make(map[string]string, len(img.Tasks)), lost: make(map[string]bool)}
	var used int64
	var fileCount, pendingCount int
	for _, ti := range img.Tasks {
		if _, dup := plan.phase[ti.PID]; dup {
			return importPlan{}, imageReject("task %s listed twice", ti.PID)
		}
		rec, ok := records[ti.PID]
		if ti.PID == "" || !ok {
			return importPlan{}, imageReject("task %q has no registry record", ti.PID)
		}
		names, capacity, err := validateTaskImage(ti, rec, img.FrozenAtBootNS)
		if err != nil {
			return importPlan{}, err
		}
		plan.names[ti.PID], plan.phase[ti.PID], plan.lost[ti.PID] = names, rec.Phase, ti.LostTerminal
		used += capacity
		fileCount += len(names)
		if ti.PTY != nil {
			pendingCount += len(ti.PTY.Pending)
		}
	}
	// Count actual kind-specific files; six-per-task rejects valid PTY cuts.
	// origin: hot-update S4 plan §5.1 / T19
	body, marshalErr := json.Marshal(img)
	if marshalErr != nil || len(body) > 16<<20 || fileCount > 16384 || pendingCount > procrequest.MaxLimit*upgradebudget.PTYBatchBytes {
		return importPlan{}, imageReject("metadata, descriptor or pending-byte bound exceeded")
	}
	if err := validateImageBudget(img, used, replayLimit(cfg)); err != nil {
		return importPlan{}, err
	}
	// Refuse a saturated node before taking business descriptors. A racing
	// import can still consume capacity later; stage 3 then detaches every
	// partial reservation into salvage instead of closing those descriptors.
	// origin: hot-update S5 plan §3.4 / T30
	if cfg.SharedReplayBudget != nil && used > replayLimit(cfg)-cfg.SharedReplayBudget.Used() {
		return importPlan{}, imageReject("replay capacity exceeds available node budget")
	}
	if err := validateImageOwners(img, records, plan, now); err != nil {
		return importPlan{}, err
	}
	if err := validateImageRows(rows, plan); err != nil {
		return importPlan{}, err
	}
	return plan, nil
}

// validateTaskImage checks one task against its record and returns the
// file names it must carry and the replay capacity it accounts for.
func validateTaskImage(ti TaskImage, rec procrequest.Record, frozenAt int64) ([]string, int64, error) {
	if !reflect.DeepEqual(rec.Handle, ti.Handle) {
		return nil, 0, imageReject("task %s handle differs from its record", ti.PID)
	}
	switch rec.Phase {
	case "running", "terminal":
	default:
		return nil, 0, imageReject("task %s record is %s", ti.PID, rec.Phase)
	}
	if ti.Kind != rec.Kind || (ti.Kind != "" && ti.Kind != proto.ProcessKindPTY) {
		return nil, 0, imageReject("task %s kind mismatch", ti.PID)
	}
	terminal := rec.Phase == "terminal"
	switch {
	case ti.LostTerminal && terminal:
		return nil, 0, imageReject("task %s is lost-terminal but its record is terminal", ti.PID)
	case !terminal && !ti.LostTerminal && ti.TerminalAt != 0:
		return nil, 0, imageReject("task %s is running but has a terminal instant", ti.PID)
	case ti.TerminalAt > frozenAt || ti.LastTouch > frozenAt || ti.TerminalAt < 0 || ti.LastTouch < 0:
		return nil, 0, imageReject("task %s instants are after the freeze", ti.PID)
	case len(ti.Revoked) > 4096 || len(ti.Abort[0]) > 4096 || len(ti.Abort[1]) > 4096:
		return nil, 0, imageReject("task %s texts exceed the bound", ti.PID)
	}
	if ti.Kind == proto.ProcessKindPTY {
		return validatePTYImage(ti, terminal)
	}
	if ti.PTY != nil {
		return nil, 0, imageReject("exec task %s carries PTY state", ti.PID)
	}
	ended := terminal || ti.LostTerminal
	// stdin: present exactly while the child may still read it.
	if (ti.Stdin != nil) == ended {
		return nil, 0, imageReject("task %s stdin presence contradicts its phase", ti.PID)
	}
	var names []string
	var capacity int64
	if ti.Stdin != nil {
		if ti.Stdin.Output != nil || ti.Stdin.Input == nil || ti.Stdin.Input.Replay != ti.Stdin.Replay ||
			!validReplayState(ti.Stdin.Replay) || ti.Stdin.Input.Version != 1 || len(ti.Stdin.Input.WriteError) > 4096 {
			return nil, 0, imageReject("task %s stdin is not a consistent input actor", ti.PID)
		}
		names = append(names, fileStdinReplay)
		if !ti.Stdin.Input.EOF {
			names = append(names, fileStdinTarget)
		}
		capacity += ti.Stdin.Replay.Capacity
	}
	for i, stream := range []*StreamImage{ti.Stdout, ti.Stderr} {
		streamNames, err := validateOutputImage(ti, i, stream, terminal)
		if err != nil {
			return nil, 0, err
		}
		if stream != nil {
			capacity += stream.Replay.Capacity
		}
		names = append(names, streamNames...)
	}
	sort.Strings(names)
	if len(ti.Files) != len(names) {
		return nil, 0, imageReject("task %s exports %d files, expects %d", ti.PID, len(ti.Files), len(names))
	}
	for _, name := range names {
		if _, ok := ti.Files[name]; !ok {
			return nil, 0, imageReject("task %s lacks file %s", ti.PID, name)
		}
	}
	return names, capacity, nil
}

// validateOutputImage checks output stream i of a task in its three shapes:
// absent (partial allocation of a terminal task), bare log (failStart
// finished it — FIN — or the post-Start setup failure aborted it — Abort,
// no FIN) or pumped.
func validateOutputImage(ti TaskImage, i int, stream *StreamImage, terminal bool) ([]string, error) {
	replayName, sourceName := streamFileNames(i)
	if stream == nil {
		if !terminal || ti.Abort[i] != "" {
			return nil, imageReject("task %s output %d is absent", ti.PID, i)
		}
		return nil, nil
	}
	if stream.Input != nil || !validReplayState(stream.Replay) {
		return nil, imageReject("task %s output %d is an input actor or has an invalid log state", ti.PID, i)
	}
	if stream.Output == nil {
		if !terminal || (ti.Abort[i] != "") == stream.Replay.FIN {
			return nil, imageReject("task %s output %d bare log contradicts its phase", ti.PID, i)
		}
		return []string{replayName}, nil
	}
	if ti.Abort[i] != "" {
		return nil, imageReject("task %s output %d is both aborted and pumped", ti.PID, i)
	}
	if stream.Output.Replay != stream.Replay || stream.Output.Version != 1 ||
		len(stream.Output.ReadError) > 4096 || len(stream.Output.Pending) > 32<<10 {
		return nil, imageReject("task %s output %d actor and log disagree or exceed their bounds", ti.PID, i)
	}
	if stream.Output.ReadError != "" && stream.Replay.FIN ||
		len(stream.Output.Pending) != 0 && stream.Output.ReadError == "" ||
		stream.Output.Discarding && stream.Output.ReadError == "" {
		return nil, imageReject("task %s output %d actor state is inconsistent", ti.PID, i)
	}
	return []string{replayName, sourceName}, nil
}

// validReplayState is the pure half of workload.RestoreReplay's rule, run in
// stage 1 so that a malformed log state is refused before any descriptor
// is taken (S2 internal review I7: the plan put these in stage 1).
func validReplayState(st workload.ReplayState) bool {
	return st.Version == 1 && st.Base <= st.End && st.Capacity > 0 && st.Capacity <= workload.DefaultReplayCapacity &&
		st.End-st.Base <= uint64(st.Capacity)
}

// validateImageBudget proves the exported budget figure is the sum of the
// exported logs and within the limit THIS binary enforces. The image's own
// budget_limit is informational: an importer that adopted it would let the
// previous image's metadata dictate the new image's memory bound (S2 plan
// §2.1; internal review I2/R6).
func validateImageBudget(img Image, used, limit int64) error {
	if used != img.BudgetUsed {
		return imageReject("budget %d does not equal the exported logs' capacity %d", img.BudgetUsed, used)
	}
	if limit == 0 {
		limit = workload.DefaultReplayBudget
	}
	if img.BudgetLimit < 0 || used > limit {
		return imageReject("budget %d exceeds this binary's limit %d", used, limit)
	}
	return nil
}

// validateImageOwners proves every registry record has exactly the owner
// its phase demands (a task, a tombstone, or none for settled), every
// tombstone is backed by a terminal record, and revocations are sane.
func validateImageOwners(img Image, records map[string]procrequest.Record, plan importPlan, now int64) error {
	tombstones := make(map[string]bool, len(img.Tombstones))
	for _, ts := range img.Tombstones {
		if ts.PID == "" || tombstones[ts.PID] {
			return imageReject("duplicate or empty tombstone")
		}
		if _, isTask := plan.phase[ts.PID]; isTask {
			return imageReject("tombstone %s is also a task", ts.PID)
		}
		rec, ok := records[ts.PID]
		if !ok || rec.Phase != "terminal" || rec.Terminal == nil || *rec.Terminal != ts.Terminal || !reflect.DeepEqual(rec.Handle, ts.Handle) {
			return imageReject("tombstone %s does not match a terminal record", ts.PID)
		}
		if ts.Kind != rec.Kind || !validPTYTombstone(ts) {
			return imageReject("tombstone %s kind or terminal state mismatch", ts.PID)
		}
		if ts.AtBootNS <= 0 || ts.AtBootNS > img.FrozenAtBootNS {
			return imageReject("tombstone %s instant is after the freeze", ts.PID)
		}
		tombstones[ts.PID] = true
	}
	for pid, rec := range records {
		if rec.Kind != "" && rec.Kind != proto.ProcessKindPTY {
			return imageReject("record %s has unknown kind", pid)
		}
		_, isTask := plan.phase[pid]
		if rec.Phase == "settled" && isTask {
			return imageReject("settled record %s still has a task", pid)
		}
		if rec.Phase != "settled" && !isTask && !tombstones[pid] {
			return imageReject("record %s has neither a task nor a tombstone", pid)
		}
	}
	sids := make(map[string]bool, len(img.Revocations))
	for _, rev := range img.Revocations {
		// A revocation is the one decision that legitimately lands UNDER the
		// fence (§22.4 item 7(b): Revoke only marks while frozen, so its
		// instant is later than frozen_at_boot_ns); it is bounded by the
		// importer's clock, not by the cut. origin: S2 internal review V1.
		if !procrequest.ValidToken(rev.SID) || sids[rev.SID] || (rev.AtBootNS <= 0 && !rev.ClockUnknown) || rev.AtBootNS < 0 || rev.AtBootNS > now || len(rev.Reason) > 4096 {
			return imageReject("invalid revocation")
		}
		sids[rev.SID] = true
	}
	return nil
}

// validateImageRows proves the process table and the task table agree:
// every row is a running task's child, every running (non-lost) task has a
// row, nothing else is there.
func validateImageRows(rows []workload.Record, plan importPlan) error {
	rowsByID := make(map[string]bool, len(rows))
	for _, row := range rows {
		if rowsByID[row.ID] {
			return imageReject("process row %s listed twice", row.ID)
		}
		rowsByID[row.ID] = true
		phase, isTask := plan.phase[row.ID]
		switch {
		case !isTask:
			return fmt.Errorf("%w: %w: process row %s belongs to no task", ErrImage, ErrForeignProcess, row.ID)
		case phase != "running":
			return imageReject("process row %s belongs to a terminal task", row.ID)
		case row.Exit == nil && row.OSPID <= 0:
			// Start ticks and boot ID are stamped by ExportProcesses and proved
			// by RestoreProcesses; an in-process cut carries the table's own
			// rows, which only know the OS pid.
			return imageReject("live process row %s lacks a kernel identity", row.ID)
		}
	}
	for pid, phase := range plan.phase {
		if phase == "running" && !plan.lost[pid] && !rowsByID[pid] {
			return imageReject("running task %s has no process row", pid)
		}
		if plan.lost[pid] && rowsByID[pid] {
			return imageReject("lost-terminal task %s still has a process row", pid)
		}
	}
	return nil
}

// ErrForeignProcess reports a process-table row that belongs to no task of
// the image: a legacy exec or a PTY child sharing the table. The image
// layer maps it to the foreign_process_rows deferral.
var ErrForeignProcess = errors.New("exec session: process row belongs to no exported task")

// checkTakenFile runs the read-only predicates on one taken descriptor.
func checkTakenFile(ti TaskImage, name string, file *os.File) error {
	id, err := fileIdentity(file)
	if err != nil {
		return err
	}
	if want := ti.Files[name]; id != want {
		return fmt.Errorf("kernel identity %d:%d, exported %d:%d", id.Dev, id.Ino, want.Dev, want.Ino)
	}
	switch name {
	case fileStdinReplay:
		return workload.CheckReplayFile(file, ti.Stdin.Replay.Capacity)
	case fileStdoutReplay:
		return workload.CheckReplayFile(file, ti.Stdout.Replay.Capacity)
	case fileStderrReplay:
		return workload.CheckReplayFile(file, ti.Stderr.Replay.Capacity)
	case fileStdinTarget:
		return workload.CheckStreamFile(file, true)
	case fileStdoutSource, fileStderrSource:
		return workload.CheckStreamFile(file, false)
	}
	return fmt.Errorf("unexpected file name %q", name)
}

// buildTask constructs one paused task from its image and taken files. On
// error it detaches everything it built back into files (the same map the
// salvage reports), so the caller's salvage is complete either way.
func buildTask(r *Runtime, ti TaskImage, phase string, files map[string]*os.File) (*task, error) {
	if ti.Kind == proto.ProcessKindPTY {
		return buildPTYTask(r, ti, phase, files)
	}
	t := &task{handle: ti.Handle, pid: ti.PID, abort: ti.Abort, revoked: ti.Revoked, lostTerminal: ti.LostTerminal,
		terminalAt: ti.TerminalAt, lastTouch: ti.LastTouch, done: make(chan struct{})}
	taken := make(map[string]*os.File, len(files))
	for name, file := range files {
		taken[name] = file
	}
	fail := func(err error) (*task, error) {
		detached, detachErr := detachTask(t)
		for name, file := range detached {
			files[name] = file
		}
		return nil, errors.Join(err, detachErr)
	}
	if ti.Stdin != nil {
		log, err := workload.RestoreReplay(r.sharedReplayBudget, ti.Stdin.Replay, files[fileStdinReplay])
		if err != nil {
			return fail(fmt.Errorf("stdin log: %w", err))
		}
		t.logs[0] = log
		delete(files, fileStdinReplay)
		target := files[fileStdinTarget]
		input, err := workload.RestoreInput(*ti.Stdin.Input, target, log)
		if err != nil {
			return fail(fmt.Errorf("stdin actor: %w", err))
		}
		t.input = input
		delete(files, fileStdinTarget)
	}
	for i, stream := range []*StreamImage{ti.Stdout, ti.Stderr} {
		if stream == nil {
			continue
		}
		replayName, sourceName := streamFileNames(i)
		log, err := workload.RestoreReplay(r.sharedReplayBudget, stream.Replay, files[replayName])
		if err != nil {
			return fail(fmt.Errorf("output %d log: %w", i, err))
		}
		t.logs[i+1] = log
		delete(files, replayName)
		if stream.Output == nil {
			continue
		}
		output, err := workload.RestoreOutput(*stream.Output, files[sourceName], log)
		if err != nil {
			return fail(fmt.Errorf("output %d actor: %w", i, err))
		}
		t.output[i] = output
		delete(files, sourceName)
	}
	if phase == "terminal" || ti.LostTerminal {
		if t.terminalAt == 0 {
			t.terminalAt = r.frozenAt // the exporter's clock failed at finish; the cut is the nearest truth
		}
		// Published: nothing will ever run for it (terminal-ack or Close
		// retires it), so it counts as owned and done is closed now.
		t.owned = true
		close(t.done)
	}
	// The imported task is a frozen task: it carries the same frozen state
	// the exporter had, so Export on an imported runtime before Thaw
	// re-exports the cut (plan §1.5 "回滚重导当前态"; S2 internal review
	// F2/P7). The files are the taken duplicates, now owned by the actors.
	t.frozen = &taskFreeze{files: taken}
	if ti.Stdin != nil {
		t.frozen.have[0], t.frozen.replay[0], t.frozen.input = true, ti.Stdin.Replay, ti.Stdin.Input
	}
	for i, stream := range []*StreamImage{ti.Stdout, ti.Stderr} {
		if stream != nil {
			t.frozen.have[i+1], t.frozen.replay[i+1], t.frozen.output[i] = true, stream.Replay, stream.Output
		}
	}
	return t, nil
}

// detachInto moves a built task's files back into the salvage entry that
// carries its resource ID.
func detachInto(t *task, salvage []handoff.Resource) error {
	files, err := detachTask(t)
	for i := range salvage {
		if salvage[i].ID == taskResource(t.kind, t.pid, nil).ID {
			for name, file := range files {
				salvage[i].Files[name] = file
			}
			return err
		}
	}
	return err
}
