package ptysession

import (
	"errors"
	"fmt"
	"os"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
)

func (a *Actor) snapshotLocked() Image {
	s := a.state
	s.Input = a.cfg.Input.Snapshot()
	s.Output = a.cfg.Output.Snapshot()
	s.Pending = append([]byte(nil), s.Pending...)
	s.CleanupMembers = append([]workload.SessionMember(nil), s.CleanupMembers...)
	s.Observation.Termios = append([]byte(nil), s.Observation.Termios...)
	// A stopped reader is not necessarily a true last-slave source end.
	s.SourceEnded = a.state.SourceEnded
	s.Discarding = a.source == sourceDiscarding
	return s
}

// Import never starts I/O or renews authority. Replay and master validation
// belong to their owners; here their already-restored state must exactly agree.
func Import(cfg Config, s Image) (*Actor, error) {
	if err := validateConfig(cfg, s.MasterClosed); err != nil {
		return nil, err
	}
	if err := validateImage(cfg, s); err != nil {
		return nil, err
	}
	s.Pending = append([]byte(nil), s.Pending...)
	s.CleanupMembers = append([]workload.SessionMember(nil), s.CleanupMembers...)
	s.Observation.Termios = append([]byte(nil), s.Observation.Termios...)
	// Replay has no separate Borrow API. With exclusive ownership and no pumps,
	// a metadata-only thaw/freeze obtains the same original file without I/O or
	// renewing authority. First prove that the caller supplied frozen owners.
	in, err := borrowFrozenReplay(cfg.Input)
	if err != nil {
		return nil, err
	}
	out, err := borrowFrozenReplay(cfg.Output)
	if err != nil {
		return nil, err
	}
	a := newActor(cfg, s, phaseFrozen)
	a.files = map[string]*os.File{"input-replay": in, "output-replay": out}
	if !s.MasterClosed {
		file := cfg.Master.Borrow()
		if file == nil {
			return nil, fmt.Errorf("ptysession: imported master is unavailable")
		}
		a.files["master"] = file
	}
	return a, nil
}

func borrowFrozenReplay(log *workload.Replay) (*os.File, error) {
	if _, _, err := log.Freeze(); !errors.Is(err, workload.ErrFrozen) {
		if err == nil {
			log.Thaw()
			err = fmt.Errorf("ptysession: import requires a frozen replay")
		}
		return nil, err
	}
	log.Thaw()
	_, file, err := log.Freeze()
	return file, err
}

func validateImage(cfg Config, s Image) error {
	if s.Version != 1 || s.Identity != cfg.Identity || s.Input != cfg.Input.Snapshot() || s.Output != cfg.Output.Snapshot() ||
		s.Input.Base > s.Input.End || s.Input.End-s.Input.Base > uint64(s.Input.Capacity) ||
		s.Output.Base > s.Output.End || s.Output.End-s.Output.Base > uint64(s.Output.Capacity) ||
		s.InputFIN != s.Input.FIN || (s.InputError != "" && !s.InputEnded) ||
		(s.InputEnded && s.InputError == "" && (!s.InputFIN || s.Input.Base != s.Input.End)) ||
		(s.MasterClosed && (!s.InputEnded || (!s.Output.FIN && s.OutputError == ""))) || (s.Output.FIN && (s.OutputError != "" || !s.SourceEnded)) ||
		len(s.Pending) > upgradebudget.PTYBatchBytes || (len(s.Pending) > 0 && s.OutputError == "") ||
		(s.Discarding && (s.OutputError == "" || s.SourceEnded || s.MasterClosed)) ||
		(s.ControlSeq == 0 && (s.ControlDigest != "" || s.ControlError != "")) || (s.ControlSeq != 0 && len(s.ControlDigest) != 64) ||
		s.LastKASeq > s.ControlSeq || s.KAIntervalMS != normalizeInterval(s.KAIntervalMS) ||
		s.KADeadlineBootNS < 0 || s.ExpiredSinceBootNS < 0 || s.RecoveryNotBeforeBootNS < 0 || s.NextJudgeBootNS < 0 || s.AuthorityDeadlineBootNS < 0 ||
		(s.KAIntervalMS == 0 && s.KADeadlineBootNS != 0) || (s.KAIntervalMS > 0 && s.KADeadlineBootNS == 0) ||
		(s.RecoveryNotBeforeBootNS != 0 && s.ExpiredSinceBootNS == 0) ||
		len(s.CleanupMembers) > 4096 || (len(s.CleanupMembers) > 0 && !s.CleanupPending) {
		return fmt.Errorf("ptysession: invalid image responsibility")
	}
	return nil
}

func copyFiles(files map[string]*os.File) map[string]*os.File {
	out := make(map[string]*os.File, len(files))
	for name, file := range files {
		out[name] = file
	}
	return out
}

// Detach transfers frozen originals without closing them or restoring tty
// settings. The old wrapper becomes unreachable; only the returned owner may
// register the descriptor again. This is salvage, never an active rollback.
func (a *Actor) Detach() (map[string]*os.File, error) {
	<-a.control
	defer func() { a.control <- struct{}{} }()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.phase != phaseFrozen || a.read.running || a.write.running {
		return nil, fmt.Errorf("ptysession: detach requires frozen borrowers")
	}
	in, _, err := a.cfg.Input.Detach()
	if err != nil {
		return nil, err
	}
	files := map[string]*os.File{"input-replay": in}
	out, _, err := a.cfg.Output.Detach()
	if err != nil {
		return files, err
	}
	files["output-replay"] = out
	if !a.state.MasterClosed {
		files["master"] = a.cfg.Master.Borrow()
	}
	a.cfg.Master = nil
	a.phase = phaseClosed
	a.files = nil
	return files, nil
}
