// transfer.go projects the frozen S7 evidence without discarding its raw bytes.
package fullstackharness

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// TransferWorkload is predeclared supply, not a fabricated runtime BudgetSeal.
// The actual seal must come from the original capable transfer invocation.
// origin: hot-update S10 plan §3.3 / §10.2 P14
type TransferWorkload struct {
	Runtime   string `json:"runtime"`
	Cohort    string `json:"cohort"`
	Direction string `json:"direction"`
	Tier      string `json:"tier"`
	Size      int64  `json:"size"`
	RateLimit int64  `json:"rate_limit"`
	BudgetNS  int64  `json:"budget_ns"`
}

func TransferCohortPlan(cohort string) ([]TransferWorkload, error) {
	if cohort != "C0" && cohort != "C1" {
		return nil, errors.New("transfer cohort must be C0 or C1")
	}
	rows := []TransferWorkload{
		{Runtime: "a0", Direction: "push", Size: 66549440, RateLimit: 65536, BudgetNS: int64(2092 * time.Second)},
		{Runtime: "a1", Direction: "pull", Size: 66549440, RateLimit: 65536, BudgetNS: int64(2092 * time.Second)},
		{Runtime: "a2", Direction: "push", Size: 1 << 30, RateLimit: 1048576, BudgetNS: int64(2108 * time.Second)},
		{Runtime: "a3", Direction: "pull", Size: 1 << 30, RateLimit: 1048576, BudgetNS: int64(2108 * time.Second)},
	}
	for i := range rows {
		rows[i].Cohort, rows[i].Tier = cohort, "B"
	}
	return rows, nil
}

// TransferObservation keeps exact custody JSON alongside the full product DTO.
// Only counters, digests and resource references are in this public observation;
// tracker/worker private capsules must remain in the original sealed resources.
type TransferObservation struct {
	Value xfercontract.Observation
	Raw   json.RawMessage
	Ref   contract.RawRef
}

// DecodeTransferObservation uses the actual S7 strict decoder. Its complete
// typed closure is copied recursively, including nested zero-valued fields.
// No permissive JSON aliases or renamed partial DTO can authorize continuity.
// origin: hot-update S10 plan §5.1 / §5.4 / P14
func DecodeTransferObservation(raw []byte, path string) (TransferObservation, error) {
	var header struct {
		At int64 `json:"boottime_ns"`
	}
	if json.Unmarshal(raw, &header) != nil || header.At <= 0 || path == "" {
		return TransferObservation{}, errors.New("transfer observation identity is missing")
	}
	e := drillobserve.Event{Kind: drillobserve.TransferEventKind, Domain: "transfer", At: uint64(header.At),
		Digest: sha256.Sum256(raw), Data: raw}
	decoded, err := drillobserve.DecodeTransfer(e)
	if err != nil {
		return TransferObservation{}, err
	}
	var value xfercontract.Observation
	if err := copyTransferProjection(reflect.ValueOf(&value).Elem(), reflect.ValueOf(decoded)); err != nil {
		return TransferObservation{}, err
	}
	return TransferObservation{Value: value, Raw: bytes.Clone(raw), Ref: transferRawRef(raw, path, "transfer-v1")}, nil
}

func copyTransferProjection(dst, src reflect.Value) error {
	if src.Kind() != reflect.Struct || dst.Kind() != reflect.Struct || src.NumField() != dst.NumField() {
		return errors.New("transfer projection shape changed")
	}
	for i := 0; i < dst.NumField(); i++ {
		if src.Type().Field(i).Name != dst.Type().Field(i).Name {
			return errors.New("transfer projection field changed")
		}
		from, to := src.Field(i), dst.Field(i)
		if from.Kind() == reflect.Struct {
			if err := copyTransferProjection(to, from); err != nil {
				return err
			}
		} else {
			if !from.CanConvert(to.Type()) {
				return errors.New("transfer projection type changed")
			}
			to.Set(from.Convert(to.Type()))
		}
	}
	return nil
}

type TransferStatus struct {
	Value xfercontract.Status
	Raw   json.RawMessage
	Ref   contract.RawRef
}

// DecodeTransferStatus consumes the real wire DTO, retains all fields and binds
// status to the original invocation. Remaining budget may decrease; start,
// geometry, origin, entry nonce and original effective budget cannot change.
// origin: hot-update S10 plan §3.3 / §5.1 / P14
func DecodeTransferStatus(raw []byte, path string, original xfercontract.Status) (TransferStatus, error) {
	var wire proto.TransferStatusResp
	if path == "" || strictTransferJSON(raw, &wire) != nil {
		return TransferStatus{}, errors.New("transfer status JSON is incomplete")
	}
	value := xfercontract.Status{State: wire.State, Phase: wire.Phase, Identity: xfercontract.Identity(wire.Identity),
		Origin: xfercontract.Origin(wire.Origin), Budget: xfercontract.BudgetSeal(wire.Budget), DurableOffset: wire.DurableOffset,
		ChunkIndex: wire.ChunkIndex, LastConsumedStreamSeq: wire.LastConsumedStreamSeq,
		CleanupPending: wire.CleanupPending, Code: wire.Code, Error: wire.Error}
	if wire.Terminal != nil {
		terminal := xfercontract.TerminalReceipt(*wire.Terminal)
		value.Terminal = &terminal
	}
	id := original.Identity
	if id.SID == "" || id.NID == "" || id.ActorFP == "" || id.AgentInstance == "" || id.TransferID == "" ||
		id.EntryNonce == "" || id.IntentDigest == "" || id.ObjectVersion == "" || original.Budget.StartedAt.IsZero() ||
		original.Budget.EffectiveBudgetNS <= 0 || original.Budget.RemainingNS > original.Budget.EffectiveBudgetNS ||
		original.Budget.Size <= 0 || original.Origin.BrokerID == "" || original.Origin.InstanceID == "" ||
		original.Origin.BootID == "" || original.Origin.Generation == 0 {
		return TransferStatus{}, errors.New("original transfer status identity is incomplete")
	}
	if value.Identity != original.Identity || value.Origin != original.Origin ||
		!value.Budget.StartedAt.Equal(original.Budget.StartedAt) ||
		value.Budget.Size != original.Budget.Size || value.Budget.RateLimitBPS != original.Budget.RateLimitBPS ||
		value.Budget.EffectiveBudgetNS != original.Budget.EffectiveBudgetNS || !value.Budget.SourceSealed ||
		value.Budget.RemainingNS < 0 || value.Budget.RemainingNS > original.Budget.RemainingNS ||
		value.DurableOffset < original.DurableOffset || value.DurableOffset > value.Budget.Size ||
		value.ChunkIndex < original.ChunkIndex || value.LastConsumedStreamSeq < original.LastConsumedStreamSeq {
		return TransferStatus{}, errors.New("transfer status changed original identity or budget")
	}
	if value.State != xfercontract.StatusLive && value.State != xfercontract.StatusTerminal {
		return TransferStatus{}, errors.New("original transfer result is unknown")
	}
	switch value.Phase {
	case xfercontract.PhaseOpen, xfercontract.PhasePrepared, xfercontract.PhaseSending, xfercontract.PhaseReceiving,
		xfercontract.PhaseCommitReserved, xfercontract.PhaseDecided, xfercontract.PhaseTerminalPending,
		xfercontract.PhaseHistoryConfirmed, xfercontract.PhaseCleanupPending, xfercontract.PhaseSettled:
	default:
		return TransferStatus{}, errors.New("transfer status phase is unknown")
	}
	if value.State == xfercontract.StatusTerminal && (value.Terminal == nil || value.Terminal.HistorySeq == 0 ||
		value.Terminal.HistoryStream == "" || value.Terminal.TerminalID == "" || value.Terminal.Bytes != value.Budget.Size ||
		len(value.Terminal.CanonicalDigest) != 64 || value.Terminal.Kind != "complete" || value.Terminal.OutcomeUnknown) {
		return TransferStatus{}, errors.New("transfer terminal has no exact successful history receipt")
	}
	return TransferStatus{Value: value, Raw: bytes.Clone(raw), Ref: transferRawRef(raw, path, "transfer-status-v1")}, nil
}

// ValidateTransferTrackerState examines the real TrackerFrozen.Export state in
// place. P23 retains its complete sealed resource closure and owns descriptor
// teardown; this function never serializes settlement capsules into JSON.
// Pending grant/publication/cleanup entries must remain bound to the same row.
// origin: hot-update S10 plan §4.2 / §5.1 / §5.4 / P14
func ValidateTransferTrackerState(state xfercontract.TrackerState, originals []xfercontract.Status) error {
	if state.Schema != xfercontract.StateSchema || state.Generation == 0 || len(originals) == 0 || len(state.Entries) != len(originals) {
		return errors.New("transfer tracker population is incomplete")
	}
	rows := make(map[xfercontract.Identity]xfercontract.LedgerRow, len(state.Entries))
	for _, row := range state.Entries {
		if _, exists := rows[row.Identity]; exists || row.EntryNonce != row.Identity.EntryNonce || row.Revision == 0 {
			return errors.New("transfer tracker row identity is invalid")
		}
		rows[row.Identity] = row
	}
	seen := make(map[xfercontract.Identity]bool, len(originals))
	for _, original := range originals {
		row, found := rows[original.Identity]
		if !found || seen[original.Identity] || row.Origin != original.Origin ||
			!row.BudgetSeal.StartedAt.Equal(original.Budget.StartedAt) ||
			row.BudgetSeal.EffectiveBudgetNS != original.Budget.EffectiveBudgetNS ||
			row.BudgetSeal.RateLimitBPS != original.Budget.RateLimitBPS || row.BudgetSeal.Size != original.Budget.Size ||
			row.BudgetSeal.RemainingNS > original.Budget.RemainingNS {
			return errors.New("transfer tracker changed original identity or budget")
		}
		seen[original.Identity] = true
	}
	for _, records := range [][]xfercontract.CanonicalRecord{state.PendingStart, state.PendingTerminal} {
		for _, record := range records {
			row, found := rows[record.Identity]
			canonical := row.CanonicalStart
			if record.Kind != "start" {
				canonical = row.CanonicalTerminal
			}
			if !found || canonical == nil || !reflect.DeepEqual(*canonical, record) {
				return errors.New("transfer tracker publication lost its canonical row")
			}
		}
	}
	for _, slot := range state.GrantSlots {
		row, found := rows[slot.Identity]
		if !found || row.Grant == nil || *row.Grant != slot.Grant || slot.Grant.Identity != row.Identity || slot.Revision > row.Revision {
			return errors.New("transfer tracker grant lost its original row")
		}
	}
	return nil
}

func transferRawRef(raw []byte, path, schema string) contract.RawRef {
	return contract.RawRef{Path: path, SHA: contract.Digest(sha256.Sum256(raw)), Bytes: int64(len(raw)), Schema: schema}
}

// Validate the exact wire field spelling and presence recursively. Ordinary Go
// decoding accepts duplicate and case-folded keys, which can erase raw evidence.
func strictTransferJSON(raw []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	if err := transferJSONValue(d, reflect.TypeOf(target).Elem()); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("transfer JSON trailing value")
	}
	return json.Unmarshal(raw, target)
}

func transferJSONValue(d *json.Decoder, typ reflect.Type) error {
	if typ.Kind() == reflect.Pointer {
		var raw json.RawMessage
		if d.Decode(&raw) != nil {
			return errors.New("transfer JSON pointer")
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return nil
		}
		nested := json.NewDecoder(bytes.NewReader(raw))
		return transferJSONValue(nested, typ.Elem())
	}
	if typ.Kind() != reflect.Struct || typ == reflect.TypeFor[time.Time]() {
		var raw json.RawMessage
		if d.Decode(&raw) != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return errors.New("transfer JSON scalar")
		}
		return nil
	}
	fields := map[string]reflect.Type{}
	optional := map[string]bool{}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		tag := f.Tag.Get("json")
		name, options, _ := bytes.Cut([]byte(tag), []byte(","))
		if len(name) == 0 {
			name = []byte(f.Name)
		}
		fields[string(name)], optional[string(name)] = f.Type, string(options) == "omitempty"
	}
	if token, err := d.Token(); err != nil || token != json.Delim('{') {
		return errors.New("transfer JSON object")
	}
	seen := map[string]bool{}
	for d.More() {
		key, err := d.Token()
		name, ok := key.(string)
		field, present := fields[name]
		if err != nil || !ok || !present || seen[name] {
			return errors.New("transfer JSON field is duplicate or unknown")
		}
		seen[name] = true
		if err := transferJSONValue(d, field); err != nil {
			return err
		}
	}
	for name := range fields {
		if !seen[name] && !optional[name] {
			return errors.New("transfer JSON required field is missing")
		}
	}
	if token, err := d.Token(); err != nil || token != json.Delim('}') {
		return errors.New("transfer JSON object end")
	}
	return nil
}

// TransferInterval joins an observation to P00's full process/cut stamp rather
// than inventing missing transaction or authority fields from a transfer ID.
func TransferInterval(o TransferObservation, stamp contract.Stamp) (contract.Interval, error) {
	v := o.Value
	p := stamp.Process
	if p.PID != v.PID || p.StartTicks != v.ProcessStart || p.BootID != v.BootID ||
		stamp.Generation != v.Generation || stamp.CutID != v.CutID || stamp.AtBootNS != v.BoottimeNS {
		return contract.Interval{}, errors.New("transfer observation crosses process or cut identity")
	}
	return contract.Interval{Stamp: stamp, ResourceID: v.TransferID, Event: string(v.Kind), AtNS: v.BoottimeNS, Raw: o.Ref}, nil
}
