package infrahost

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
)

// origin: hot-update S9 plan §2.2 / §6.5 / T11
// origin: S10 internal review ARCH-REDS R12
func TestInfraSupportRetiresOldWorker(t *testing.T) {
	t.Run("busy business domain allows only support staging", func(t *testing.T) {
		f := newHostFixture(t)
		f.run(t, f.request(t, infracontract.SwitchIngress, 1))
		stage := f.request(t, infracontract.Stage, 1)
		stage.Tx = infracontract.ID{9}
		stage.Nonce = infracontract.ID{9}
		stage.Component = infracontract.HostSupport
		stage.Candidate.Kind = infracontract.HelperBinary
		stage = f.sign(t, stage)
		f.run(t, stage)
		s, _, err := f.journal.Read(testDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if s.Tx != (infracontract.ID{1}) || s.Sequence != 1 || s.Slots[infracontract.HelperBinary].Pending != stage.Candidate || s.Slots[infracontract.HelperBinary].Active.BinarySHA256 != digestOf("old") {
			t.Fatal("support stage stole busy domain or active selector")
		}
		update := f.request(t, infracontract.UpdateSupport, 2)
		update.Tx = stage.Tx
		update.Nonce = infracontract.ID{10}
		update = f.sign(t, update)
		if _, err := f.worker.Do(update, testDeadline(t), nil); !errors.Is(err, ErrBusy) {
			t.Fatal("busy support cut displaced business", err)
		}
		f.run(t, f.request(t, infracontract.SwitchIngress, 2))
	})
	t.Run("selector commit requires a later worker to prove old exit", func(t *testing.T) {
		f := newHostFixture(t)
		stage := f.request(t, infracontract.Stage, 1)
		stage.Component = infracontract.HostSupport
		stage.Candidate.Kind = infracontract.HelperBinary
		stage = f.sign(t, stage)
		f.run(t, stage)
		update := f.request(t, infracontract.UpdateSupport, 2)
		before := f.run(t, update)
		if before.Code != "infra_recovery_required" {
			t.Fatal("switching worker signed its own retirement")
		}
		state, _, err := f.journal.Read(testDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if !state.SupportPending || state.SupportBefore != update.Expected || state.Slots[infracontract.HelperBinary].Active != update.Candidate {
			t.Fatalf("support intent missing: %+v", state)
		}
		next := f.request(t, infracontract.Stage, 3)
		if _, err := f.worker.Do(next, testDeadline(t), nil); !errors.Is(err, ErrBusy) {
			t.Fatal("old generation admission remained open", err)
		}
		// A replay answers the same lost reply, and cannot promote it to retirement.
		if replay := f.run(t, update); !reflect.DeepEqual(before, replay) {
			t.Fatal("support replay changed result")
		}
		seq := uint64(3)
		for _, mode := range []string{"self", "old alive", "unknown census", "wrong image", "missing proof", "new worker"} {
			t.Run(mode, func(t *testing.T) {
				r := f.request(t, infracontract.RecoverHost, seq)
				seq++
				r.Component = infracontract.HostSupport
				r.ReturnProof = digestOf("retirement")
				r.Expected.PID = 42
				r.Expected.StartTicks = 222
				r.Expected.InvocationID = "unit-2"
				r.Expected.ExeSHA256 = update.Candidate.BinarySHA256
				c := infracontract.Census{Complete: true, Generation: 2, Digest: digestOf("physical census"), Processes: []infracontract.Process{r.Expected}}
				switch mode {
				case "self":
					r.Expected = update.Expected
					c.Processes = []infracontract.Process{r.Expected}
				case "old alive":
					c.Processes = append(c.Processes, update.Expected)
				case "unknown census":
					c.Complete = false
				case "wrong image":
					r.Expected.ExeSHA256 = digestOf("wrong")
					c.Processes = []infracontract.Process{r.Expected}
				case "missing proof":
					r.ReturnProof = infracontract.Digest{}
				}
				f.effects.inspect = func(infracontract.ID, infracontract.Deadline) (infracontract.Census, error) { return c, nil }
				r = f.sign(t, r)
				receipt, err := f.worker.Do(r, testDeadline(t), nil)
				if mode != "new worker" {
					if err == nil || receipt.Applied {
						t.Fatal("unproven retirement accepted")
					}
					return
				}
				if err != nil || receipt.Code != "" {
					t.Fatal("new worker failed to retire", receipt.Code, err)
				}
			})
		}
		state, _, err = f.journal.Read(testDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if state.SupportPending || state.Retirement == (infracontract.Digest{}) {
			t.Fatal("retirement was not durable")
		}
	})
	t.Run("busy workers prevent support selection", func(t *testing.T) {
		f := newHostFixture(t)
		stage := f.request(t, infracontract.Stage, 1)
		stage.Component = infracontract.HostSupport
		stage.Candidate.Kind = infracontract.LauncherBinary
		stage = f.sign(t, stage)
		f.run(t, stage)
		other := testProcess()
		other.PID++
		f.effects.inspect = func(infracontract.ID, infracontract.Deadline) (infracontract.Census, error) {
			return infracontract.Census{Complete: true, Generation: 1, Digest: digestOf("workers"), Processes: []infracontract.Process{testProcess(), other}}, nil
		}
		r := f.request(t, infracontract.UpdateSupport, 2)
		r.Candidate = stage.Candidate
		r = f.sign(t, r)
		if _, err := f.worker.Do(r, testDeadline(t), nil); !errors.Is(err, ErrBusy) {
			t.Fatal("live worker was displaced", err)
		}
		s, _, err := f.journal.Read(testDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if s.Slots[infracontract.LauncherBinary].Active.BinarySHA256 != digestOf("old launcher") {
			t.Fatal("busy update changed selector")
		}
	})
	t.Run("launcher selection still needs later exec evidence", func(t *testing.T) {
		f := newHostFixture(t)
		stage := f.request(t, infracontract.Stage, 1)
		stage.Component = infracontract.HostSupport
		stage.Candidate.Kind = infracontract.LauncherBinary
		stage = f.sign(t, stage)
		f.run(t, stage)
		r := f.request(t, infracontract.UpdateSupport, 2)
		r.Candidate = stage.Candidate
		r = f.sign(t, r)
		f.run(t, r)
		// The next legal component launch may proceed in this same domain;
		// the unchanged helper image is not mistaken for the launcher image.
		f.run(t, f.request(t, infracontract.Stage, 3))
		f.effects.validate = func(r infracontract.HostRequest, s HostState, _ infracontract.Deadline, _ infracontract.Stop) error {
			if r.Op == infracontract.RecoverHost && s.SupportKind == infracontract.LauncherBinary && r.ReturnProof != digestOf("launcher exec proof") {
				return ErrPermission
			}
			return nil
		}
		next := testProcess()
		next.PID++
		next.StartTicks++
		f.effects.inspect = func(infracontract.ID, infracontract.Deadline) (infracontract.Census, error) {
			return infracontract.Census{Complete: true, Generation: 2, Digest: digestOf("new worker"), Processes: []infracontract.Process{next}}, nil
		}
		recover := f.request(t, infracontract.RecoverHost, 4)
		recover.Component = infracontract.HostSupport
		recover.Expected = next
		recover.ReturnProof = digestOf("only worker exit")
		recover = f.sign(t, recover)
		if _, err := f.worker.Do(recover, testDeadline(t), nil); !errors.Is(err, ErrPermission) {
			t.Fatal("worker exit masqueraded as launcher exec", err)
		}
		recover = f.request(t, infracontract.RecoverHost, 5)
		recover.Component = infracontract.HostSupport
		recover.Expected = next
		recover.ReturnProof = digestOf("launcher exec proof")
		recover = f.sign(t, recover)
		f.run(t, recover)
		s, _, err := f.journal.Read(testDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if s.SupportPending || s.Slots[infracontract.LauncherBinary].Previous.BinarySHA256 != digestOf("old launcher") {
			t.Fatal("launcher retirement lost previous generation")
		}
	})
	t.Run("current journal is strict and bounded", func(t *testing.T) {
		for _, mutate := range []func([]byte) []byte{
			func(b []byte) []byte { return append(b, ' ') },
			func(b []byte) []byte {
				return bytes.Replace(b, []byte(`"Version":1`), []byte(`"Version":1,"Version":1`), 1)
			},
			func(b []byte) []byte {
				return bytes.Replace(b, []byte(journalPurpose), []byte("tether-infra-host-v1"), 1)
			},
			func(b []byte) []byte {
				return bytes.Replace(b, []byte(`"StartupFence":false`), []byte(`"StartupFence":true`), 1)
			},
			func(b []byte) []byte { return b[:len(b)-1] },
			func([]byte) []byte { return bytes.Repeat([]byte{' '}, journalLimit+1) },
		} {
			f := newHostFixture(t)
			path := filepath.Join(f.path, "journal.json")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, mutate(data), 0600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := f.journal.Read(testDeadline(t)); !errors.Is(err, ErrJournal) {
				t.Fatal("corrupt journal accepted", err)
			}
		}
	})
	t.Run("permanent lock contention obeys deadline", func(t *testing.T) {
		f := newHostFixture(t)
		r := f.request(t, infracontract.SwitchIngress, 1)
		unlock, err := lockJournal(f.journal.directory, testDeadline(t), nil)
		if err != nil {
			t.Fatal(err)
		}
		d, err := controlbudget.After(20 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.worker.Do(r, d, nil)
		unlock()
		if err == nil {
			t.Fatal("CAS bypassed held journal lock")
		}
		if f.effects.calls.Load() != 0 {
			t.Fatal("contended request dispatched")
		}
		before, err := os.Stat(filepath.Join(f.path, "journal.lock"))
		if err != nil {
			t.Fatal(err)
		}
		f.run(t, r)
		after, err := os.Stat(filepath.Join(f.path, "journal.lock"))
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(before, after) {
			t.Fatal("permanent lock inode replaced")
		}
		next := f.request(t, infracontract.SwitchIngress, 2)
		if err := os.Remove(filepath.Join(f.path, "journal.lock")); err != nil {
			t.Fatal(err)
		}
		if _, err := f.worker.Do(next, testDeadline(t), nil); !errors.Is(err, ErrJournal) {
			t.Fatal("missing permanent lock was recreated", err)
		}
	})
	t.Run("pending deadline cannot be renewed", func(t *testing.T) {
		f := newHostFixture(t)
		r := f.request(t, infracontract.SwitchIngress, 1)
		clock := newJournalClock(t, f.journal)
		d := clock.deadline(20 * time.Millisecond)
		intent, _, err := f.journal.begin(r, d, nil)
		if err != nil {
			t.Fatal(err)
		}
		// Admission must have persisted the original deadline before it expires.
		// Expire it exactly, then replay with the same request and a fresh budget.
		if intent.Phase != EffectIntent || intent.Deadline != d {
			t.Fatal("original intent did not enter the deadline window")
		}
		clock.advance(20 * time.Millisecond)
		if _, err := f.worker.Do(r, testDeadline(t), nil); !errors.Is(err, ErrHalted) {
			t.Fatal("pending intent redispatched", err)
		}
		e, err := f.journal.unresolved(intent.RequestDigest, testDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if e.Deadline != intent.Deadline || f.effects.calls.Load() != 0 {
			t.Fatal("replay renewed deadline")
		}
	})
	// P31/P40 supply the real two-generation helper entries and P02 pidfd
	// observations. These hermetic contract tests do not sign their runtime exit.
}
