// origin: hot-update S6 plan §9.3 / T53; PTY image ownership and validation.
package execsession

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
)

const (
	ptyResourceKind   = "pty-task-v1"
	ptyResourcePrefix = "pty-task."
	filePTYMaster     = "master"
	filePTYInput      = "input-replay"
	filePTYOutput     = "output-replay"
)

func taskResource(kind, pid string, files map[string]*os.File) handoff.Resource {
	if kind == proto.ProcessKindPTY {
		return handoff.Resource{ID: ptyResourcePrefix + pid, Kind: ptyResourceKind, Files: files}
	}
	return handoff.Resource{ID: resourceID(pid), Kind: resourceKind, Files: files}
}

// validatePTYImage runs before TakeFile. A closed master can retain a live
// HUP-immune child; only terminal phase requires source/cleanup completion.
// origin: hot-update S4 plan §4.2 / §5.1 / T19
func validatePTYImage(ti TaskImage, terminal bool) ([]string, int64, error) {
	st := ti.PTY
	reject := func() ([]string, int64, error) {
		return nil, 0, imageReject("task %s has inconsistent PTY state", ti.PID)
	}
	if st == nil || ti.Stdin != nil || ti.Stdout != nil || ti.Stderr != nil || ti.Abort != [2]string{} {
		return reject()
	}
	if st.Version != 1 || !validReplayState(st.Input) || !validReplayState(st.Output) || st.Input.Capacity != upgradebudget.PTYInputBytes || st.Output.Capacity != workload.DefaultReplayCapacity || st.InputFIN != st.Input.FIN {
		return reject()
	}
	if !validInputEnd(st.Input.Base, st.Input.End, st.InputFIN, st.InputEnded, st.InputError) || !validControl(st.ControlSeq, st.ControlDigest, st.ControlError) {
		return reject()
	}
	if len(st.Pending) > upgradebudget.PTYBatchBytes || len(st.OutputError) > 4096 || !utf8.ValidString(st.OutputError) ||
		st.Output.FIN && st.OutputError != "" || len(st.Pending) > 0 && st.OutputError == "" || st.Discarding && (st.OutputError == "" || st.SourceEnded) {
		return reject()
	}
	if st.MasterClosed && (!st.InputEnded || !st.Output.FIN && st.OutputError == "") {
		return reject()
	}
	if terminal && (!st.MasterClosed || st.CleanupPending) {
		return reject()
	}
	if !st.MasterClosed && (!st.Identity.Started || st.Identity.Version != 1 || st.Identity.BootID == "" || st.Identity.LeaderPID <= 0 || st.Identity.StartTimeTicks <= 0) {
		return reject()
	}
	if st.Revoked != ti.Revoked || len(st.Revoked) > 4096 || len(st.CleanupMembers) > procrequest.MaxLimit {
		return reject()
	}
	if st.KAIntervalMS < 0 || st.KAIntervalMS > 60000 || st.KAIntervalMS > 0 && st.KAIntervalMS < 1000 ||
		st.KADeadlineBootNS < 0 || st.ExpiredSinceBootNS < 0 || st.RecoveryNotBeforeBootNS < 0 || st.NextJudgeBootNS < 0 || st.AuthorityDeadlineBootNS < 0 || st.LastKASeq > st.ControlSeq {
		return reject()
	}
	if st.KAIntervalMS == 0 && (st.KADeadlineBootNS != 0 || st.ExpiredSinceBootNS != 0 || st.RecoveryNotBeforeBootNS != 0 || st.NextJudgeBootNS != 0 || st.LastKASeq != 0) || st.KAIntervalMS > 0 && st.KADeadlineBootNS <= 0 {
		return reject()
	}
	if st.ExpiredSinceBootNS == 0 && st.RecoveryNotBeforeBootNS != 0 || st.RecoveryNotBeforeBootNS != 0 && st.RecoveryNotBeforeBootNS < st.KADeadlineBootNS {
		return reject()
	}
	if len(st.CleanupMembers) > 0 && !st.CleanupPending {
		return reject()
	}
	seen := make(map[int]bool, len(st.CleanupMembers))
	for _, member := range st.CleanupMembers {
		id := member.Identity
		if id.OSPID <= 0 || id.BootID != st.Identity.BootID || id.StartTimeTicks <= 0 || member.SessionID != st.Identity.LeaderPID || seen[id.OSPID] {
			return reject()
		}
		seen[id.OSPID] = true
	}
	names := []string{filePTYInput, filePTYOutput}
	if !st.MasterClosed {
		names = append(names, filePTYMaster)
	}
	sort.Strings(names)
	if len(ti.Files) != len(names) {
		return reject()
	}
	for _, name := range names {
		if _, ok := ti.Files[name]; !ok {
			return reject()
		}
	}
	return names, st.Input.Capacity + st.Output.Capacity, nil
}

func validInputEnd(a, q uint64, fin, ended bool, diagnosis string) bool {
	if a > q || !validPTYDiagnostic(diagnosis, "pty input aborted: ") {
		return false
	}
	if diagnosis != "" && !ended {
		return false
	}
	if ended && diagnosis == "" && (!fin || a != q) {
		return false
	}
	return true
}

func validControl(seq uint64, digest, diagnosis string) bool {
	if !validPTYDiagnostic(diagnosis, "pty control failed: ") {
		return false
	}
	if seq == 0 {
		return digest == "" && diagnosis == ""
	}
	decoded, err := hex.DecodeString(digest)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == digest
}

func validPTYTombstone(ts TombstoneImage) bool {
	if ts.Kind == "" {
		return ts.PTY == nil
	}
	if ts.Kind != proto.ProcessKindPTY || ts.PTY == nil || ts.FinalEnd[1] != 0 || !ts.FinalFIN[1] {
		return false
	}
	st := ts.PTY
	return st.InputEnded && validInputEnd(st.InputACK, st.InputAccepted, st.InputFIN, st.InputEnded, st.InputError) && validControl(st.ControlSeq, st.ControlDigest, st.ControlError)
}

func checkImageFile(ti TaskImage, name string, file *os.File, rows []workload.Record) error {
	if ti.Kind != "pty" {
		return checkTakenFile(ti, name, file)
	}
	id, err := fileIdentity(file)
	if err != nil {
		return err
	}
	if id != ti.Files[name] {
		return imageReject("task %s descriptor identity differs", ti.PID)
	}
	switch name {
	case filePTYInput:
		return workload.CheckReplayFile(file, ti.PTY.Input.Capacity)
	case filePTYOutput:
		return workload.CheckReplayFile(file, ti.PTY.Output.Capacity)
	case filePTYMaster:
		for _, row := range rows {
			if row.ID == ti.PID {
				return pty.CheckMaster(file, ti.PTY.Identity, row)
			}
		}
		return imageReject("task %s master has no process row", ti.PID)
	}
	return imageReject("task %s unexpected PTY descriptor", ti.PID)
}

func freezePTYTask(ctx context.Context, t *task) (*taskFreeze, error) {
	if t.pty != nil {
		deadline, _ := ctx.Deadline()
		st, files, err := t.pty.Freeze(deadline)
		if err != nil {
			return nil, err
		}
		return &taskFreeze{pty: &st, files: files}, nil
	}
	if t.ptyState == nil {
		return nil, imageReject("task %s has no terminal actor or failed-spawn state", t.pid)
	}
	st := *t.ptyState
	files := make(map[string]*os.File, 2)
	var err error
	st.Input, files[filePTYInput], err = t.logs[0].Freeze()
	if err != nil {
		return nil, err
	}
	st.Output, files[filePTYOutput], err = t.logs[1].Freeze()
	if err != nil {
		return nil, err
	}
	return &taskFreeze{pty: &st, files: files}, nil
}

func detachPTYTask(t *task) (map[string]*os.File, error) {
	if t.pty != nil {
		files, err := t.pty.Detach()
		if err == nil {
			t.pty = nil
			t.logs = [3]*workload.Replay{}
		}
		return files, err
	}
	files := make(map[string]*os.File, 2)
	for i, name := range []string{filePTYInput, filePTYOutput} {
		if t.logs[i] == nil {
			continue
		}
		file, _, err := t.logs[i].Detach()
		if err != nil {
			return files, err
		}
		files[name] = file
		t.logs[i] = nil
	}
	return files, nil
}

func buildPTYTask(r *Runtime, ti TaskImage, phase string, files map[string]*os.File) (*task, error) {
	return buildPTYTaskWithRows(r, ti, phase, files, nil)
}

// Nothing runs here. RestoreMaster consumes its borrowed descriptor only on
// success; every later failure returns the new wrapper plus the original
// replays. Salvage is never downgraded to the exec resource kind.
func buildPTYTaskWithRows(r *Runtime, ti TaskImage, phase string, files map[string]*os.File, rows []workload.Record) (*task, error) {
	st := *ti.PTY
	t := &task{kind: ti.Kind, pid: ti.PID, handle: ti.Handle, revoked: ti.Revoked, lostTerminal: ti.LostTerminal,
		terminalAt: ti.TerminalAt, lastTouch: ti.LastTouch, done: make(chan struct{}),
		control: ptysession.ControlReceipt{Seq: st.ControlSeq, Digest: st.ControlDigest, Error: st.ControlError}}
	fail := func(cause error) (*task, error) {
		detached, err := detachPTYTask(t)
		for name, file := range detached {
			files[name] = file
		}
		return nil, errors.Join(cause, err)
	}
	owned := make(map[string]*os.File, 3)
	for i, name := range []string{filePTYInput, filePTYOutput} {
		state := st.Input
		if i == 1 {
			state = st.Output
		}
		log, err := workload.RestoreReplay(r.sharedReplayBudget, state, files[name])
		if err != nil {
			return fail(err)
		}
		t.logs[i] = log
		owned[name] = files[name]
		delete(files, name)
	}
	// Failed spawn has no started-domain identity and never acquires an actor.
	if !st.Identity.Started {
		t.ptyState = &st
	} else {
		var master pty.Master
		if !st.MasterClosed {
			var row workload.Record
			for _, candidate := range rows {
				if candidate.ID == ti.PID {
					row = candidate
					break
				}
			}
			var err error
			master, err = pty.RestoreMaster(files[filePTYMaster], st.Identity, row)
			if err != nil {
				return fail(err)
			}
			// RestoreMaster may replace the descriptor wrapper. Salvage must borrow
			// from the new owner, never return the consumed original wrapper.
			files[filePTYMaster] = master.Borrow()
		}
		actor, err := ptysession.Import(ptysession.Config{Manager: r.cfg.Manager, PID: t.pid, Master: master, Identity: st.Identity,
			Input: t.logs[0], Output: t.logs[1], Clock: r.now, KAIntervalMS: st.KAIntervalMS}, st)
		if err != nil {
			return fail(err)
		}
		t.pty = actor
	}
	if t.pty != nil {
		// Import is already frozen; Freeze is a read of that cut, not activation.
		snapshot, borrowed, err := t.pty.Freeze(time.Now().Add(time.Second))
		if err != nil {
			return fail(err)
		}
		st = snapshot
		owned = borrowed
	}

	for name := range files {
		delete(files, name)
	}
	t.frozen = &taskFreeze{pty: &st, files: owned}
	if phase == "terminal" || ti.LostTerminal {
		if t.terminalAt == 0 {
			t.terminalAt = r.frozenAt
		}
		t.owned = true
		close(t.done)
	}
	return t, nil
}

// Diagnostics are a closed vocabulary, not arbitrary payload-bearing text.
// Refuse an unknown reason before taking any descriptor from the source.
func validPTYDiagnostic(diagnosis, prefix string) bool {
	if diagnosis == "" {
		return true
	}
	for _, reason := range []string{"write_error", "replay_error", "hangup", "revoked", "authority_expired", "source_ended", "ioctl_error", "signal_error", "close_error"} {
		if diagnosis == prefix+reason {
			return true
		}
	}
	return false
}
