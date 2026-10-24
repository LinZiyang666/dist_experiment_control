// driver.go owns product CLI dispatch, never product installation or recovery.
// Raw command receipts survive refusal, lost replies and cancellation. A caller
// may query an uncertain transaction; it may not repeat its unsafe invocation.
// origin: hot-update S10 plan §2.1 / §5.1 / §10.2 P18
package fullstackharness

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
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
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

//go:embed command-bindings.json
var productCommandCatalog []byte

var (
	ErrProductCLIPending = errors.New("fullstack: NOT_RUN pending product CLI binding or native observation adapter")
	ErrProductCLISent    = errors.New("fullstack: unsafe invocation already reserved; query the original transaction")
)

type productBinding struct {
	ID            string          `json:"id"`
	Role          string          `json:"role"`
	Action        string          `json:"action"`
	Argv          []string        `json:"argv"`
	TimeoutNS     int64           `json:"timeout_ns"`
	Help          json.RawMessage `json:"help"`
	RawViewSchema string          `json:"raw_view_schema"`
}

type productBindings struct {
	Version  int              `json:"version"`
	Bindings []productBinding `json:"bindings"`
}

// ProductCLICommand binds one frozen template to explicit fixture inputs. Values
// replace entire argv elements only; there is no shell, PATH lookup, or arbitrary
// command override. Backup must be the verified product backup receipt for N3.
// Stamp reads P04/P16 evidence for EACH native stamp, including old receipts;
// S8's Unix timestamps cannot be relabeled as boot-clock timestamps.
type ProductCLICommand struct {
	BindingID    string
	Values       map[string]string
	Home         string
	BinarySHA    contract.Digest
	Backup       contract.RawRef
	VerifyBackup func(context.Context, contract.Key, contract.RawRef) error
	Stamp        func(context.Context, upgradetx.Stamp) (contract.Stamp, error)
}

type productDriver struct {
	mu       sync.Mutex
	profile  contract.Profile
	bindings map[string]productBinding
	commands map[string]ProductCLICommand
	seen     map[string]bool
	last     map[contract.Key]contract.Stamp
}

// NewDriver is inert: construction cannot start a child or write product state.
// P19 binds prepared inputs with BindProductCLICommand after one-time supply.
func NewDriver(profile contract.Profile) (contract.Driver, error) {
	if !filepath.IsAbs(profile.EvidenceRoot) || len(profile.Required) == 0 ||
		!slices.Contains([]string{"single", "quorum"}, profile.Topology) {
		return nil, errors.New("fullstack: driver requires an absolute evidence root and sealed targets")
	}
	data, err := readProductRef(profile.CommandBindings)
	if err != nil {
		return nil, err
	}
	if err = contract.ValidateJSON("command_bindings", data); err != nil {
		return nil, err
	}
	var actual, frozen productBindings
	if err = json.Unmarshal(data, &actual); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(productCommandCatalog, &frozen); err != nil {
		return nil, err
	}
	// Copy the sealed population and steps. Mutating a caller's manifest after
	// READY must not silently authorize a different target or transaction.
	sealed, err := json.Marshal(profile)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(sealed, &profile); err != nil {
		return nil, err
	}
	d := &productDriver{profile: profile, bindings: make(map[string]productBinding),
		commands: make(map[string]ProductCLICommand), seen: make(map[string]bool), last: make(map[contract.Key]contract.Stamp)}
	// Only a help receipt may change when an upstream command lands. Widening
	// argv requires an explicit reviewed binding change, never a runtime fallback.
	for _, b := range actual.Bindings {
		i := slices.IndexFunc(frozen.Bindings, func(f productBinding) bool { return f.ID == b.ID })
		if i < 0 || d.bindings[b.ID].ID != "" {
			return nil, errors.New("fullstack: unknown or duplicate CLI binding")
		}
		f := frozen.Bindings[i]
		if b.Role != f.Role || b.Action != f.Action || !slices.Equal(b.Argv, f.Argv) ||
			b.TimeoutNS != f.TimeoutNS || b.RawViewSchema != f.RawViewSchema {
			return nil, errors.New("fullstack: CLI binding differs from frozen command contract")
		}
		d.bindings[b.ID] = b
	}
	return d, nil
}

// BindProductCLICommand uses a Step ID for stage/apply/rollback, and an empty
// Step ID for query. The full Key prevents co-located runtimes sharing a route.
// Rebinding is rejected even after a failed command; inputs are frozen once.
func BindProductCLICommand(driver contract.Driver, key contract.Key, stepID, action string, command ProductCLICommand) error {
	d, ok := driver.(*productDriver)
	if !ok {
		return errors.New("fullstack: foreign product driver")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	b, ok := d.bindings[command.BindingID]
	if !ok || b.Role != key.Role || b.Action != action || !slices.Contains(d.profile.Required, key) ||
		(action == "query") != (stepID == "") || strings.Contains(b.ID, ".quorum.") && d.profile.Topology != "quorum" ||
		strings.Contains(b.ID, ".single.") && d.profile.Topology != "single" {
		return errors.New("fullstack: command does not match sealed target and action")
	}
	id := productCommandKey(key, stepID, action)
	if _, exists := d.commands[id]; exists {
		return ErrProductCLISent
	}
	command.Values = cloneProductValues(command.Values)
	d.commands[id] = command
	return nil
}

func cloneProductValues(values map[string]string) map[string]string {
	out := make(map[string]string, len(values))
	for k, v := range values {
		out[k] = v
	}
	return out
}

func productCommandKey(key contract.Key, stepID, action string) string {
	data, _ := json.Marshal(struct {
		Key          contract.Key
		Step, Action string
	}{key, stepID, action})
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func (d *productDriver) Stage(ctx context.Context, step contract.Step) (contract.View, error) {
	return d.step(ctx, step, "stage")
}

func (d *productDriver) Apply(ctx context.Context, step contract.Step) (contract.View, error) {
	action := "apply"
	if step.Direction == "reverse" {
		action = "rollback"
	}
	return d.step(ctx, step, action)
}

func (d *productDriver) step(ctx context.Context, step contract.Step, action string) (contract.View, error) {
	if !slices.ContainsFunc(d.profile.Steps, func(s contract.Step) bool { return reflect.DeepEqual(s, step) }) {
		return contract.View{}, errors.New("fullstack: command step differs from sealed plan")
	}
	// Step deadlines belong to the fixture host's clock, like P17 slots. Help
	// validation and actual dispatch consume one original budget together.
	remaining, err := controlbudget.Remaining(controlbudget.Deadline(step.DeadlineNS))
	if err != nil {
		return contract.View{}, err
	}
	call, cancel := context.WithTimeout(ctx, remaining)
	defer cancel()
	return d.call(call, step.Target, step.ID, action, "", "")
}

func (d *productDriver) Query(ctx context.Context, key contract.Key, txID, nonce string) (contract.View, error) {
	if txID == "" || nonce == "" {
		return contract.View{}, errors.New("fullstack: query requires original transaction and fresh nonce")
	}
	return d.call(ctx, key, "", "query", txID, nonce)
}

func (d *productDriver) call(ctx context.Context, key contract.Key, stepID, action, txID, nonce string) (contract.View, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return contract.View{}, err
	}
	id := productCommandKey(key, stepID, action)
	command, ok := d.commands[id]
	if !ok {
		return contract.View{}, ErrProductCLIPending
	}
	b := d.bindings[command.BindingID]
	var help contract.RawRef
	if err := contract.DecodeJSON("raw_ref", b.Help, &help); err != nil {
		return contract.View{}, err
	}
	if help.Schema != "tether-cli-help-v1" {
		return contract.View{}, ErrProductCLIPending
	}
	// Stage emits upgradeartifact.Receipt, and S9 emits infracontract.Status.
	// P00 View is exclusively an S8 view. Refuse BEFORE a side effect until the
	// respective upstream native-to-View contract exists; never return fake View.
	if b.RawViewSchema != "s8_view" || command.Stamp == nil {
		return contract.View{}, ErrProductCLIPending
	}
	values := cloneProductValues(command.Values)
	if action == "query" {
		values["{tx_id}"] = txID
		// S8 currently generates this nonce internally and exposes no frozen
		// input flag. The caller's challenge must be supported before dispatch.
		if !slices.Contains(b.Argv, "{query_nonce}") {
			return contract.View{}, ErrProductCLIPending
		}
		values["{query_nonce}"] = nonce
	}
	argv, err := productArgv(b, values, command.Backup)
	if err != nil {
		return contract.View{}, err
	}
	// The product's backup bytes and native verification are separate facts.
	// P08/P19 bind the real backup verifier; a checksummed arbitrary file alone
	// cannot authorize --backup-taken, and no driver creates a replacement.
	if slices.Contains(argv, "--backup-taken") {
		if command.VerifyBackup == nil {
			return contract.View{}, ErrProductCLIPending
		}
		if err := command.VerifyBackup(ctx, key, command.Backup); err != nil {
			return contract.View{}, err
		}
	}
	if err = checkProductExecutable(argv[0], command.BinarySHA); err != nil {
		return contract.View{}, err
	}
	if !filepath.IsAbs(command.Home) || command.Home == string(filepath.Separator) {
		return contract.View{}, errors.New("fullstack: explicit sandbox HOME required")
	}
	if flag := slices.Index(argv, "--home"); flag >= 0 && (flag+1 >= len(argv) || argv[flag+1] != command.Home) {
		return contract.View{}, errors.New("fullstack: CLI home differs from bound sandbox")
	}
	helpBytes, err := readProductRef(help)
	if err != nil {
		return contract.View{}, err
	}
	helpArgv := []string{argv[0]}
	for _, arg := range argv[1:] {
		if strings.HasPrefix(arg, "--") {
			break
		}
		helpArgv = append(helpArgv, arg)
	}
	helpArgv = append(helpArgv, "--help")
	stdout, _, err := d.invoke(ctx, id+"-help", false, helpArgv, command.Home, 5*time.Second)
	if err != nil || !bytes.Equal(stdout, helpBytes) {
		return contract.View{}, errors.Join(errors.New("fullstack: executable help differs from bound golden"), err)
	}
	stdout, raw, runErr := d.invoke(ctx, id, action != "query", argv, command.Home, time.Duration(b.TimeoutNS))
	if len(stdout) == 0 {
		return contract.View{Raw: raw}, errors.Join(errors.New("fullstack: product returned no native View"), runErr)
	}
	native, err := decodeProductView(stdout)
	if err != nil {
		return contract.View{Raw: raw}, errors.Join(err, runErr)
	}
	if txID == "" {
		txID = values["{tx_id}"]
	}
	if nonce == "" {
		nonce = native.Observation.Nonce
	}
	view, err := projectProductView(ctx, native, stdout, raw, key, txID, nonce, command.Stamp)
	if err == nil {
		stamp := view.Stamp
		old, seen := d.last[key]
		if d.seen[key.Host+"\x00"+key.Unit+"\x00"+nonce] || seen &&
			(stamp.Generation < old.Generation || stamp.Generation == old.Generation &&
				(stamp.CutID != old.CutID || stamp.TxID != old.TxID || stamp.Direction != old.Direction ||
					stamp.ScopeDigest != old.ScopeDigest || stamp.Revision < old.Revision || stamp.Sequence <= old.Sequence)) {
			err = errors.New("fullstack: replayed nonce or stale current stamp")
		} else {
			d.last[key], d.seen[key.Host+"\x00"+key.Unit+"\x00"+nonce] = stamp, true
		}
	}
	return view, errors.Join(err, runErr)
}

func productArgv(b productBinding, values map[string]string, backup contract.RawRef) ([]string, error) {
	argv := slices.Clone(b.Argv)
	for i, arg := range argv {
		if strings.HasPrefix(arg, "{") {
			value := values[arg]
			if value == "" || strings.ContainsAny(value, "\x00\r\n") || strings.HasPrefix(value, "--") {
				return nil, errors.New("fullstack: missing or invalid command input")
			}
			argv[i] = value
		}
	}
	// origin: hot-update S10 plan §2.1 / §10.2 P18 / missingbackup
	// A flag alone is not a backup receipt. Its immutable bytes must already
	// exist before reserving the unsafe send; the driver never makes a backup.
	if slices.Contains(argv, "--backup-taken") {
		if backup.Schema != "tether-cluster-backup-v1" {
			return nil, errors.New("fullstack: verified backup receipt required")
		}
		if _, err := readProductRef(backup); err != nil {
			return nil, err
		}
	}
	if slices.Contains(argv, "--rollback") && (slices.Contains(argv, "--to-version") || slices.Contains(argv, "--expect-sha256")) {
		return nil, errors.New("fullstack: rollback cannot override recorded previous image")
	}
	return argv, nil
}

func checkProductExecutable(path string, want contract.Digest) error {
	if !filepath.IsAbs(path) || want == (contract.Digest{}) {
		return errors.New("fullstack: explicit built CLI and digest required")
	}
	data, err := os.ReadFile(path)
	if err != nil || sha256.Sum256(data) != want {
		return errors.New("fullstack: CLI artifact digest mismatch")
	}
	return nil
}

func readProductRef(ref contract.RawRef) ([]byte, error) {
	if !filepath.IsAbs(ref.Path) || ref.Bytes <= 0 || ref.Bytes > 16<<20 || ref.SHA == (contract.Digest{}) {
		return nil, errors.New("fullstack: incomplete command evidence reference")
	}
	f, err := os.Open(ref.Path)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(f, ref.Bytes+1))
	err = errors.Join(readErr, f.Close())
	if err != nil || int64(len(data)) != ref.Bytes || sha256.Sum256(data) != ref.SHA {
		return nil, errors.New("fullstack: command evidence digest mismatch")
	}
	return data, nil
}

type productOutput struct{ bytes.Buffer }

func (w *productOutput) Write(data []byte) (int, error) {
	if w.Len()+len(data) > 4<<20 {
		return 0, errors.New("fullstack: native CLI output exceeds evidence ceiling")
	}
	return w.Buffer.Write(data)
}

// invoke is the only spawn site. Its durable reservation is acceptance evidence,
// not a product marker. It survives driver reconstruction and a lost response.
// Every started process is joined through P05, including context cancellation.
// origin: hot-update S10 plan §6 / §10.2 P18 / duplicate unsafe send
func (d *productDriver) invoke(ctx context.Context, id string, unsafe bool, argv []string, home string, timeout time.Duration) ([]byte, contract.RawRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, contract.RawRef{}, err
	}
	if unsafe {
		path := filepath.Join(d.profile.EvidenceRoot, "cli-intent-"+id+".json")
		intent, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, os.ErrExist) {
			return nil, contract.RawRef{}, ErrProductCLISent
		}
		if err != nil {
			return nil, contract.RawRef{}, err
		}
		data, _ := json.Marshal(argv)
		_, writeErr := intent.Write(data)
		if err = errors.Join(writeErr, intent.Sync(), intent.Close()); err != nil {
			return nil, contract.RawRef{}, err
		}
		directory, err := os.Open(d.profile.EvidenceRoot)
		if err != nil {
			return nil, contract.RawRef{}, err
		}
		if err = errors.Join(directory.Sync(), directory.Close()); err != nil {
			return nil, contract.RawRef{}, err
		}
	}
	call, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(call, argv[0], argv[1:]...)
	// Do not inherit production HOME, control configuration or secret-bearing
	// environment. Authentication paths are explicit non-secret argv inputs.
	cmd.Env = []string{"HOME=" + home, "LANG=C", "LC_ALL=C", "PATH=/usr/bin:/bin", "GOTRACEBACK=single"}
	cmd.Dir = home
	var stdout, stderr productOutput
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	started := time.Now()
	err := RunProcess(cmd)
	record := struct {
		Schema        string   `json:"schema"`
		Argv          []string `json:"argv"`
		RC            int      `json:"rc"`
		StartedUnixNS int64    `json:"started_unix_ns"`
		ElapsedNS     int64    `json:"elapsed_ns"`
		Stdout        []byte   `json:"stdout"`
		Stderr        []byte   `json:"stderr"`
	}{"fullstack-cli-command-v1", argv, ProcessExitCode(err), started.UnixNano(), time.Since(started).Nanoseconds(), stdout.Bytes(), stderr.Bytes()}
	data, marshalErr := json.Marshal(record)
	raw, archiveErr := d.archiveCommand(data)
	return stdout.Bytes(), raw, errors.Join(err, call.Err(), marshalErr, archiveErr)
}

func (d *productDriver) archiveCommand(data []byte) (contract.RawRef, error) {
	f, err := os.CreateTemp(d.profile.EvidenceRoot, "cli-result-*.json")
	if err != nil {
		return contract.RawRef{}, err
	}
	_, writeErr := f.Write(data)
	if err = errors.Join(writeErr, f.Sync(), f.Close()); err != nil {
		return contract.RawRef{}, err
	}
	return contract.RawRef{Path: f.Name(), SHA: sha256.Sum256(data), Bytes: int64(len(data)), Schema: "fullstack-cli-command-v1"}, nil
}

func decodeProductView(raw []byte) (upgradetx.View, error) {
	var view upgradetx.View
	if err := contract.ValidateJSON("s8_view", raw); err != nil {
		return view, err
	}
	if err := json.Unmarshal(raw, &view); err != nil {
		return view, err
	}
	// Compare canonical values with exact numbers: schema validation rejects
	// duplicates and aliases, and this comparison detects dropped optional data.
	encoded, err := json.Marshal(view)
	if err != nil || !sameProductJSON(raw, encoded) {
		return upgradetx.View{}, errors.New("fullstack: native View projection lost fields")
	}
	return view, nil
}

func sameProductJSON(a, b []byte) bool {
	var x, y any
	da, db := json.NewDecoder(bytes.NewReader(a)), json.NewDecoder(bytes.NewReader(b))
	da.UseNumber()
	db.UseNumber()
	return da.Decode(&x) == nil && db.Decode(&y) == nil && reflect.DeepEqual(x, y)
}

func projectProductView(ctx context.Context, native upgradetx.View, raw []byte, ref contract.RawRef, key contract.Key, txID, nonce string,
	reader func(context.Context, upgradetx.Stamp) (contract.Stamp, error)) (contract.View, error) {
	result := contract.View{Raw: ref}
	a, observation := native.Observation.Current, native.Observation
	if a.Handle.Key.DomainID != key.Domain || string(a.Handle.Key.Component) != key.Role || a.Handle.Key.UID != key.UID ||
		a.Handle.Key.SID != key.SID || a.Handle.Key.ConfiguredNID != key.NID || a.Handle.TxID != txID ||
		observation.Nonce != nonce || observation.Census.Stamp.Nonce != nonce || observation.Census.Stamp.Attempt != a.Handle ||
		!observation.OwnerKnown || !observation.Census.Known || !observation.Census.Complete || !productJointState(a) {
		return result, errors.New("fullstack: current View identity, census or joint state mismatch")
	}
	if a.Phase == upgradetx.PhaseCompleted {
		historical := native.Historical.Forward
		if a.Direction == upgradetx.DirectionReverse {
			historical = native.Historical.Reverse
		}
		if native.Historical.Key != a.Handle.Key || a.Direction == upgradetx.DirectionReverse && a.TargetSHA != native.Historical.RecordedPreviousSHA {
			return result, errors.New("fullstack: historical/current transaction mismatch")
		}
		if err := upgradetx.Completed(historical, observation); err != nil {
			return result, err
		}
		if err := upgradetx.Completed(a, observation); err != nil {
			return result, err
		}
	}
	stamp, err := productStamp(ctx, reader, observation.Census.Stamp, key, a.Direction)
	if err != nil {
		return result, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil {
		return result, err
	}
	history := struct{ Record, Metadata json.RawMessage }{fields["historical"], fields["metadata"]}
	result.Historical = append([]byte(`{"record":`), history.Record...)
	result.Historical = append(result.Historical, []byte(`,"metadata":`)...)
	result.Historical = append(result.Historical, history.Metadata...)
	result.Historical = append(result.Historical, '}')
	result.Current = bytes.Clone(fields["observation"])
	result.Stamp, result.Phase, result.Outcome = stamp, string(a.Phase), string(a.Outcome)
	result.Code, result.Detail = native.Code, native.Detail
	if a.Phase == upgradetx.PhaseDeferred || a.Phase == upgradetx.PhaseRejected {
		result.Deferred = []string{native.Code, native.Historical.Reason}
	}
	for _, pair := range []struct {
		src upgradetx.Receipt
		dst *contract.Receipt
	}{
		{a.Control, &result.Control}, {a.Resources, &result.Resources}, {a.Released, &result.Released},
	} {
		r := pair.src
		if reflect.DeepEqual(r, upgradetx.Receipt{}) {
			continue
		}
		s, stampErr := productStamp(ctx, reader, r.Stamp, key, a.Direction)
		if stampErr != nil {
			return result, stampErr
		}
		sha, decodeErr := hex.DecodeString(r.RunningSHA)
		if decodeErr != nil || len(sha) != 32 {
			return result, errors.New("fullstack: invalid receipt running SHA")
		}
		*pair.dst = contract.Receipt{Kind: string(r.Kind), Stamp: s, Known: r.Known, Complete: r.Complete, Evidence: ref}
		copy(pair.dst.RunningSHA[:], sha)
		for _, resource := range r.Census.Resources {
			pair.dst.ResourceIDs = append(pair.dst.ResourceIDs, resource.ID)
		}
	}
	return result, nil
}

func productStamp(ctx context.Context, reader func(context.Context, upgradetx.Stamp) (contract.Stamp, error), source upgradetx.Stamp,
	key contract.Key, direction upgradetx.Direction) (contract.Stamp, error) {
	k := source.Attempt.Key
	if k.DomainID != key.Domain || string(k.Component) != key.Role || k.SID != key.SID || k.ConfiguredNID != key.NID || k.UID != key.UID ||
		source.Sequence == 0 || source.At <= 0 || source.Attempt.Generation == 0 || source.Attempt.Revision == 0 {
		return contract.Stamp{}, errors.New("fullstack: native stamp is incomplete or from another target")
	}
	if reader == nil {
		return contract.Stamp{}, ErrProductCLIPending
	}
	s, err := reader(ctx, source)
	if err != nil {
		return s, err
	}
	p := s.Process
	if p.Key != key || p.BootID != source.Producer.BootID || p.PID != source.Producer.PID ||
		p.StartTicks != source.Producer.StartTicks || p.InstanceID != source.Producer.InstanceID || p.PIDNS == "" ||
		s.TxID != source.Attempt.TxID || s.CutID != source.Attempt.CutID || s.Generation != source.Attempt.Generation ||
		s.Revision != source.Attempt.Revision || s.Sequence != source.Sequence || s.QueryNonce != source.Nonce ||
		s.ScopeDigest != source.ScopeDigest || s.Direction != string(direction) || s.AtBootNS <= 0 {
		return contract.Stamp{}, errors.New("fullstack: independent stamp does not match native producer")
	}
	return s, nil
}

// Keep namespace, cut stage and outcome together. Unknown and partial ACTIVE
// states are not promoted to completion simply because the CLI returned zero.
// origin: hot-update S10 plan §5.2 / IDENT-09
func productJointState(a upgradetx.Attempt) bool {
	if a.Direction != upgradetx.DirectionForward && a.Direction != upgradetx.DirectionReverse {
		return false
	}
	rows := map[upgradetx.Phase]struct {
		cuts    string
		outcome upgradetx.Outcome
	}{
		upgradetx.PhasePrepared: {"none", upgradetx.OutcomePending}, upgradetx.PhaseStaged: {"none", upgradetx.OutcomePending},
		upgradetx.PhaseReady: {"none", upgradetx.OutcomePending}, upgradetx.PhaseFreezing: {"pristine", upgradetx.OutcomePending},
		upgradetx.PhaseActivating: {"pristine partial_active active", upgradetx.OutcomePending},
		upgradetx.PhaseServingNew: {"partial_active active", upgradetx.OutcomePending}, upgradetx.PhaseMigrating: {"active retiring", upgradetx.OutcomePending},
		upgradetx.PhaseCompleted: {"released", upgradetx.OutcomeSuccess}, upgradetx.PhaseDeferred: {"none", upgradetx.OutcomeDeferred},
		upgradetx.PhaseRejected: {"none", upgradetx.OutcomeRejected}, upgradetx.PhaseRollbackPending: {"partial_active active retiring", upgradetx.OutcomePending},
		upgradetx.PhaseRollbackBlocked: {"partial_active active retiring", upgradetx.OutcomePending}, upgradetx.PhaseRolledBack: {"released", upgradetx.OutcomeRolledBack},
		upgradetx.PhaseRecoveryRequired: {"none pristine partial_active active retiring released", upgradetx.OutcomeUnknown},
		upgradetx.PhaseCancelled:        {"none pristine", upgradetx.OutcomeCancelled}, upgradetx.PhaseAbandoned: {"none pristine partial_active active retiring released", upgradetx.OutcomeUnknown},
	}
	r, ok := rows[a.Phase]
	forward := a.Phase == upgradetx.PhaseRolledBack || a.Phase == upgradetx.PhaseRollbackPending || a.Phase == upgradetx.PhaseRollbackBlocked
	return ok && (!forward || a.Direction == upgradetx.DirectionForward) && r.outcome == a.Outcome && slices.Contains(strings.Fields(r.cuts), string(a.CutStage))
}

var _ contract.DriverFactory = NewDriver
var _ contract.Driver = (*productDriver)(nil)

// VerifyAssemblyExecutable rebuilds a fixed command from the current checkout.
// A caller-supplied SHA or a binary's self-reported build metadata cannot select
// the trusted bytes. P25's missing source entry remains pending before effects.
// origin: S10 internal review rv3-assembly F1/F2
func VerifyAssemblyExecutable(ctx context.Context, path, role string) (err error) {
	packages := map[string]string{"host": "test/hotupdate/assemblycmd", "judge": "test/hotupdate/evidencecmd", "supervisor": "test/fullstackharness/cmd/supervise"}
	pkg, ok := packages[role]
	if !ok || !filepath.IsAbs(path) {
		return errors.New("fullstack: unknown assembly command")
	}
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			return errors.New("fullstack: candidate checkout unavailable")
		}
		root = parent
	}
	if _, err := os.Stat(filepath.Join(root, pkg)); err != nil {
		return ErrFixturePending
	}
	dir, err := os.MkdirTemp("", "fullstack-source-verify-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	output := filepath.Join(dir, "command")
	cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-buildvcs=false", "-o", output, "./"+pkg)
	cmd.Dir, cmd.Env = root, os.Environ()
	cmd.Env = append(cmd.Env, "GOFLAGS=", "GOWORK=off", "GOENV=off", "CGO_ENABLED=0")
	if err := RunProcess(cmd); err != nil {
		return err
	}
	actual, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	expected, err := os.ReadFile(output)
	if err != nil || !bytes.Equal(actual, expected) {
		return errors.New("fullstack: assembly command differs from candidate source")
	}
	return nil
}

// RequireStepCompletion is stricter than Query: pending is a legal observation,
// but cannot release a dependency. Historical and fresh current DTOs retain the
// authoritative S8 completion predicate rather than a second phase-only oracle.
// origin: S10 internal review rv3-assembly F3
func RequireStepCompletion(step contract.Step, view contract.View, nonce string) error {
	var current upgradetx.Observation
	if err := contract.ValidateJSON("s8_observation", view.Current); err != nil {
		return err
	}
	if err := json.Unmarshal(view.Current, &current); err != nil {
		return err
	}
	a, s := current.Current, view.Stamp
	if view.Phase != "completed" || view.Outcome != "success" || len(view.Deferred) != 0 || nonce == "" ||
		s.QueryNonce != nonce || current.Nonce != nonce || s.Process.Key != step.Target || s.TxID != a.Handle.TxID ||
		s.CutID != a.Handle.CutID || s.Generation != a.Handle.Generation || s.Revision != a.Handle.Revision ||
		s.Direction != step.Direction || string(a.Direction) != step.Direction {
		return errors.New("fullstack: step lacks fresh current completion")
	}
	if err := upgradetx.Completed(a, current); err != nil {
		return err
	}
	var history struct {
		Record   upgradetx.Record   `json:"record"`
		Metadata upgradetx.Metadata `json:"metadata"`
	}
	if err := json.Unmarshal(view.Historical, &history); err != nil {
		return err
	}
	previous := history.Record.Forward
	if step.Direction == "reverse" {
		previous = history.Record.Reverse
	}
	return upgradetx.Completed(previous, current)
}
