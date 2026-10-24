// fixture.go adapts the frozen Fixture port to a supervised assembly command.
// Product commands travel back through P18; evidence never supplies executable
// argv. P25 binds its real host assembly and owns the five-round lifecycle test.
// origin: hot-update S10 plan §5.1 / §10.2 P19
package fullstackharness

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/LinZiyang666/tether/test/hotupdate/acceptance"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

var ErrFixturePending = errors.New("fullstack: NOT_RUN pending P25 supervised assembly command")

// FixtureStepManifest includes host support retirement obligations alongside
// P00 steps. Support owners must retire before the next dependency is released;
// they cannot masquerade as extra tether generations in acceptance.Plan.
type FixtureStepManifest struct {
	Steps       []contract.Step
	Obligations map[string][]string
}

// BuildFixtureStepManifest consumes P10's role-qualified image output. S9
// artifacts are located by their actual release, never by a guessed SHA.
// The full seven-row plan is sealed even for an arm exercising only its prefix.
// origin: hot-update S10 plan §2.2 / §10.2 P19 / wrong mixed direction
func BuildFixtureStepManifest(p contract.Profile) (FixtureStepManifest, error) {
	rows := [][]string{
		{"agent", "broker", "nats", "caddy"}, {"caddy", "nats", "broker", "agent"},
		{"agent", "broker", "nats", "caddy"}, {"caddy", "nats", "agent", "broker"},
		{"agent", "broker", "nats", "caddy"}, {"caddy", "nats", "agent", "broker"}, {"nats", "caddy"},
	}
	tether := []string{"T0", "T1", "T0", "T2", "T3", "T4", "T5", "T5"}
	result := FixtureStepManifest{Obligations: make(map[string][]string)}
	previous := []string{}
	for row, roles := range rows {
		for _, role := range roles {
			group := []string{}
			for _, target := range p.Required {
				if target.Role != role {
					continue
				}
				step := contract.Step{ID: fmt.Sprintf("R%d/%s/%s/%s", row+1, role, target.Host, target.Unit),
					ParentID: fmt.Sprintf("R%d", row+1), Target: target, Action: "upgrade", Direction: "forward",
					Cohort: "C0", Leg: "leg2", DependsOn: slices.Clone(previous), DeadlineNS: p.EndNS}
				if row == 1 || (row == 3 || row == 5) && (role == "nats" || role == "caddy") {
					step.Action, step.Direction = "rollback", "reverse"
				}
				if row >= 3 {
					step.Cohort = "C1"
				}
				if row == 0 || row == 3 {
					step.Leg = "leg1"
				}
				for side := 0; side < 2; side++ {
					id := tether[row+side] + "-" + role
					var matches []contract.Artifact
					for _, artifact := range p.Artifacts {
						match := artifact.ID == id
						if role == "nats" || role == "caddy" {
							versions := []string{"2.14.5", "2.14.6"}
							if role == "caddy" {
								versions = []string{"2.7.5", "2.7.6"}
							}
							match = artifact.Release == versions[(row+side)%2]
						}
						if artifact.Role == role && match {
							matches = append(matches, artifact)
						}
					}
					if len(matches) != 1 {
						return FixtureStepManifest{}, errors.New("fullstack: missing or ambiguous P10/S9 artifact")
					}
					if side == 0 {
						step.From = matches[0].ID
					} else {
						step.To = matches[0].ID
					}
				}
				result.Steps = append(result.Steps, step)
				group = append(group, step.ID)
				obligations := []string{"prepare", "product-stage", "start", "control-ready", "resources-ready", "old-released"}
				if role == "nats" || role == "caddy" {
					obligations = append(obligations, "physical-cohort", "temporary-ingress", "helper", "launcher",
						"canonical-ingress", "old-worker-exit", "temporary-owner-exit")
				}
				result.Obligations[step.ID] = obligations
			}
			previous = group
		}
	}
	p.Steps = result.Steps
	if _, err := acceptance.Plan(p); err != nil {
		return FixtureStepManifest{}, err
	}
	return result, nil
}

type commandFixture struct {
	mu      sync.Mutex
	profile contract.Profile
	sealed  []byte
	cmd     *exec.Cmd
	driver  contract.Driver
	input   io.WriteCloser
	output  *bufio.Reader
	done    chan struct{}
	waitErr error
	failure error
	state   string
	seq     uint64
}

// NewFixture is inert and copies the caller's sealed input. Construction may
// validate a hermetic test profile; the drill entry separately requires deploy.
func NewFixture(profile contract.Profile) (contract.Fixture, error) {
	if !filepath.IsAbs(profile.EvidenceRoot) || profile.EvidenceRoot == string(filepath.Separator) {
		return nil, errors.New("fullstack: fixture requires an absolute private evidence directory")
	}
	if _, err := acceptance.Plan(profile); err != nil {
		return nil, err
	}
	sealed, err := contract.EncodeJSON("profile", profile)
	if err != nil {
		return nil, err
	}
	if err = contract.DecodeJSON("profile", sealed, &profile); err != nil {
		return nil, err
	}
	return &commandFixture{profile: profile, sealed: sealed, state: "new"}, nil
}

// BindFixtureCommand transfers an unstarted, sandboxed host command to Fixture.
// The command must serve the P19 line protocol; it is supplied by executable
// integration code, never decoded from raw evidence. Stderr must have an owner.
// StartProcess owns all descendants; Join preserves its native cleanup error.
func BindFixtureCommand(fixture contract.Fixture, cmd *exec.Cmd, driver contract.Driver) error {
	f, ok := fixture.(*commandFixture)
	if !ok || cmd == nil || driver == nil {
		return errors.New("fullstack: invalid fixture command binding")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state != "new" || f.cmd != nil || !filepath.IsAbs(cmd.Path) || !filepath.IsAbs(cmd.Dir) ||
		cmd.Env == nil || cmd.Stdin != nil || cmd.Stdout != nil || cmd.Stderr == nil || cmd.Process != nil {
		return errors.New("fullstack: command must be unstarted with explicit sandbox environment and stderr")
	}
	home := ""
	for _, entry := range cmd.Env {
		if strings.HasPrefix(entry, "HOME=") {
			home = strings.TrimPrefix(entry, "HOME=")
		}
	}
	if !filepath.IsAbs(home) || home == string(filepath.Separator) {
		return errors.New("fullstack: command requires an explicit sandbox HOME")
	}
	f.cmd, f.driver = cmd, driver
	return nil
}

func (f *commandFixture) Start(ctx context.Context, p contract.Profile) (contract.RawRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rawProfile, encodeErr := contract.EncodeJSON("profile", p)
	if f.state != "new" || encodeErr != nil || !bytes.Equal(rawProfile, f.sealed) {
		return contract.RawRef{}, errors.New("fullstack: fixture already started or profile changed")
	}
	if f.cmd == nil {
		return contract.RawRef{}, ErrFixturePending
	}
	if err := ctx.Err(); err != nil {
		return contract.RawRef{}, err
	}
	f.state = "starting" // A partial start is never retried with new originals.
	// P17 owns the immutable request denominator. Python receives only this
	// sealed schedule; it cannot reconstruct expected slots from successful IO.
	schedule, _, err := SealRequestSchedule(f.profile)
	if err != nil {
		return contract.RawRef{}, err
	}
	scheduleFile, err := os.OpenFile(filepath.Join(f.profile.EvidenceRoot, "fullstack-request-schedule.json"),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return contract.RawRef{}, err
	}
	_, writeErr := scheduleFile.Write(schedule)
	if err = errors.Join(writeErr, scheduleFile.Sync(), scheduleFile.Close()); err != nil {
		return contract.RawRef{}, err
	}
	input, err := f.cmd.StdinPipe()
	if err != nil {
		return contract.RawRef{}, err
	}
	output, err := f.cmd.StdoutPipe()
	if err != nil {
		return contract.RawRef{}, errors.Join(err, input.Close())
	}
	wait, err := StartProcess(f.cmd)
	if err != nil {
		return contract.RawRef{}, errors.Join(err, input.Close(), output.Close())
	}
	f.input, f.output, f.done = input, bufio.NewReader(output), make(chan struct{})
	go func() { f.waitErr = wait(); close(f.done) }()
	raw, err := f.call(ctx, "start", f.sealed)
	var ref contract.RawRef
	if err == nil {
		err = contract.DecodeJSON("raw_ref", raw, &ref)
	}
	if err == nil {
		err = f.checkRef(ref)
	}
	if err != nil {
		f.failure = errors.Join(f.failure, err)
		return ref, err
	}
	f.state = "running"
	return ref, nil
}

func (f *commandFixture) Sample(ctx context.Context, nonce string) (contract.Evidence, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var evidence contract.Evidence
	if f.state != "running" || nonce == "" || f.failure != nil {
		return evidence, errors.Join(errors.New("fullstack: sample outside healthy live fixture"), f.failure)
	}
	payload, _ := json.Marshal(map[string]string{"nonce": nonce})
	raw, err := f.call(ctx, "sample", payload)
	if err == nil {
		err = contract.DecodeJSON("evidence", raw, &evidence)
	}
	if err == nil && !reflect.DeepEqual(evidence.Profile, f.profile) {
		err = errors.New("fullstack: sampled profile changed")
	}
	f.failure = errors.Join(f.failure, err)
	return evidence, err
}

func (f *commandFixture) Drain(ctx context.Context, nonce string) (contract.RawRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ref contract.RawRef
	if f.state != "running" || nonce == "" || f.failure != nil {
		return ref, errors.Join(errors.New("fullstack: drain outside healthy live fixture"), f.failure)
	}
	payload, _ := json.Marshal(map[string]string{"nonce": nonce})
	raw, err := f.call(ctx, "drain", payload)
	if err == nil {
		err = contract.DecodeJSON("raw_ref", raw, &ref)
	}
	if err == nil {
		err = f.checkRef(ref)
	}
	f.failure = errors.Join(f.failure, err)
	if err == nil {
		f.state = "drained"
	}
	return ref, err
}

// ApplyFixtureStep advances one sealed component through the Python assembly's
// live-window/retirement gates and the bound P18 driver. A package function
// keeps the frozen Fixture interface unchanged for existing P00 consumers.
func ApplyFixtureStep(ctx context.Context, fixture contract.Fixture, stepID string) (contract.RawRef, error) {
	f, ok := fixture.(*commandFixture)
	if !ok {
		return contract.RawRef{}, errors.New("fullstack: foreign fixture")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state != "running" || f.failure != nil || !slices.ContainsFunc(f.profile.Steps, func(s contract.Step) bool { return s.ID == stepID }) {
		return contract.RawRef{}, errors.Join(errors.New("fullstack: step outside healthy sealed fixture"), f.failure)
	}
	payload, _ := json.Marshal(map[string]string{"step_id": stepID})
	raw, err := f.call(ctx, "apply", payload)
	var ref contract.RawRef
	if err == nil {
		err = contract.DecodeJSON("raw_ref", raw, &ref)
	}
	if err == nil {
		err = f.checkRef(ref)
	}
	f.failure = errors.Join(f.failure, err)
	return ref, err
}

func (f *commandFixture) Stop(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == "stopped" || f.state == "joined" {
		return f.failure
	}
	if f.input != nil {
		_, err := f.call(ctx, "stop", json.RawMessage(`{}`))
		f.failure = errors.Join(f.failure, err, f.input.Close())
	}
	f.state = "stopped"
	return f.failure
}

func (f *commandFixture) checkRef(ref contract.RawRef) error {
	name, err := filepath.Rel(f.profile.EvidenceRoot, ref.Path)
	if err != nil || !filepath.IsAbs(ref.Path) || ref.Bytes <= 0 || ref.Bytes > 16<<20 {
		return errors.New("fullstack: invalid fixture raw reference")
	}
	root, err := os.OpenRoot(f.profile.EvidenceRoot)
	if err != nil {
		return err
	}
	file, err := root.Open(name)
	if err != nil {
		return errors.Join(err, root.Close())
	}
	info, statErr := file.Stat()
	if statErr != nil || !info.Mode().IsRegular() {
		return errors.Join(errors.New("fullstack: fixture raw is not regular"), statErr, file.Close(), root.Close())
	}
	data, readErr := io.ReadAll(io.LimitReader(file, ref.Bytes+1))
	err = errors.Join(readErr, file.Close(), root.Close())
	if err != nil || int64(len(data)) != ref.Bytes || sha256.Sum256(data) != ref.SHA {
		return errors.Join(errors.New("fullstack: fixture raw digest mismatch"), err)
	}
	return nil
}

func (f *commandFixture) Join(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state != "stopped" && f.state != "joined" {
		return errors.New("fullstack: Stop required before Join")
	}
	if f.state == "joined" {
		return f.failure
	}
	if f.done != nil {
		select {
		case <-f.done:
			f.failure = errors.Join(f.failure, f.waitErr)
		case <-ctx.Done():
			// Killing only the target closes the shared lease; the shared
			// supervisor still owns descendant cleanup and its returned waiter.
			_ = f.cmd.Process.Kill()
			return errors.Join(f.failure, ctx.Err())
		}
	}
	f.state = "joined"
	return f.failure
}

// call is serialized with lifecycle operations. Cancellation closes the pipe
// and kills the target to unblock both protocol directions, then joins the IO
// worker. The shared process waiter remains available to Join after failure.
func (f *commandFixture) call(ctx context.Context, method string, payload json.RawMessage) (json.RawMessage, error) {
	f.seq++
	request := struct {
		ID      uint64          `json:"id"`
		Method  string          `json:"method"`
		Payload json.RawMessage `json:"payload"`
	}{f.seq, method, payload}
	data, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	type result struct {
		raw json.RawMessage
		err error
	}
	done := make(chan result, 1)
	go func() {
		_, writeErr := f.input.Write(append(data, '\n'))
		if writeErr != nil {
			done <- result{err: writeErr}
			return
		}
		for {
			var raw []byte
			for {
				part, prefix, readErr := f.output.ReadLine()
				raw = append(raw, part...)
				if readErr != nil || len(raw) > 16<<20 {
					done <- result{err: errors.Join(errors.New("fullstack: bounded fixture response missing"), readErr)}
					return
				}
				if !prefix {
					break
				}
			}
			if !custodyUniqueJSON(raw) {
				done <- result{err: errors.New("fullstack: duplicate fixture protocol field")}
				return
			}
			var fields map[string]json.RawMessage
			if json.Unmarshal(raw, &fields) != nil {
				done <- result{err: errors.New("fullstack: invalid fixture protocol")}
				return
			}
			if _, callback := fields["driver"]; callback && len(fields) == 2 && fields["payload"] != nil {
				var action string
				if json.Unmarshal(fields["driver"], &action) != nil {
					action = ""
				}
				view, callErr := CallFixtureDriver(ctx, f.driver, action, fields["payload"])
				message := ""
				if callErr != nil {
					message = "product driver failed; retain native P18 receipt"
				}
				reply, _ := json.Marshal(map[string]any{"view": json.RawMessage(view), "error": message})
				if _, writeErr = f.input.Write(append(reply, '\n')); writeErr == nil {
					continue
				}
				done <- result{err: writeErr}
				return
			}
			var id uint64
			var native int
			if len(fields) != 3 || fields["payload"] == nil || json.Unmarshal(fields["id"], &id) != nil ||
				id != f.seq || json.Unmarshal(fields["rc"], &native) != nil || native < 0 || native > 4 {
				done <- result{err: errors.New("fullstack: invalid fixture response identity or native status")}
				return
			}
			if native != 0 {
				done <- result{err: fmt.Errorf("fullstack: assembly native rc=%d", native)}
				return
			}
			done <- result{raw: fields["payload"]}
			return
		}
	}()
	select {
	case value := <-done:
		return value.raw, value.err
	case <-ctx.Done():
		_ = f.input.Close()
		_ = f.cmd.Process.Kill()
		value := <-done
		return value.raw, errors.Join(ctx.Err(), value.err)
	}
}

// CallFixtureDriver is the only Python-to-product bridge. P00 codecs preserve
// all View fields and P18 keeps native stdout/stderr and unsafe-send fencing.
func CallFixtureDriver(ctx context.Context, driver contract.Driver, method string, payload []byte) ([]byte, error) {
	if driver == nil {
		return nil, ErrProductCLIPending
	}
	var view contract.View
	var err error
	switch method {
	case "stage", "apply":
		var step contract.Step
		if err = contract.DecodeJSON("step", payload, &step); err != nil {
			return nil, err
		}
		if method == "stage" {
			view, err = driver.Stage(ctx, step)
		} else {
			view, err = driver.Apply(ctx, step)
		}
	case "query":
		if !custodyUniqueJSON(payload) {
			return nil, errors.New("fullstack: duplicate query field")
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(payload, &fields) != nil || len(fields) != 3 {
			return nil, errors.New("fullstack: invalid query fields")
		}
		var key contract.Key
		var tx, nonce string
		if contract.DecodeJSON("key", fields["key"], &key) != nil || json.Unmarshal(fields["tx_id"], &tx) != nil ||
			json.Unmarshal(fields["nonce"], &nonce) != nil || tx == "" || nonce == "" {
			return nil, errors.New("fullstack: invalid query")
		}
		view, err = driver.Query(ctx, key, tx, nonce)
	default:
		return nil, errors.New("fullstack: unknown product driver method")
	}
	if err != nil {
		return nil, err
	}
	return contract.EncodeJSON("view", view)
}

// FixtureProfileDigest joins upper-bound calibration to the same sealed input.
func FixtureProfileDigest(p contract.Profile) (contract.Digest, error) {
	raw, err := contract.EncodeJSON("profile", p)
	return sha256.Sum256(raw), err
}

var _ contract.FixtureFactory = NewFixture
var _ contract.Fixture = (*commandFixture)(nil)
