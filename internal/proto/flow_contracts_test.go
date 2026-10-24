package proto_test

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/tunnel/flowstate"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// Real: reflection over frozen contracts and strict metadata JSON. No runtime
// owner, descriptor, carrier, cipher or private binary codec is exercised here.
// This list is independent of the implementation: tags/types are wire contracts.
// origin: hot-update S5 plan §2.1 / §5.1 / §12.1
func TestFlowSSPublicContractShapes(t *testing.T) {
	// Reflection prints the defining package, while callers retain the tunnel API.
	// Require alias identity as well as the unchanged field and JSON shapes below.
	// origin: hot-update S6 plan §9.3 / HARDGATE-A
	if reflect.TypeFor[tunnel.Allocation]() != reflect.TypeFor[flowstate.Allocation]() ||
		reflect.TypeFor[tunnel.AuthorizedKey]() != reflect.TypeFor[flowstate.AuthorizedKey]() ||
		reflect.TypeFor[tunnel.Authority]() != reflect.TypeFor[flowstate.Authority]() {
		t.Fatal("tunnel aliases no longer retain the extracted flowstate types")
	}
	cases := []struct {
		typ    reflect.Type
		fields string
	}{
		{reflect.TypeFor[rawsocket.Fingerprint](), "Dev||uint64;Ino||uint64;Type||uint32"},
		{reflect.TypeFor[rawsocket.Tuple](), "Local||netip.AddrPort;Remote||netip.AddrPort"},
		{reflect.TypeFor[flow.Frame](), "Version||uint8;Kind||flow.Kind;Flags||uint16;BodyLen||uint32;FlowID||flow.ID;CarrierGen||uint64;Offset||uint64;Body||[]uint8"},
		{reflect.TypeFor[flow.Config](), "Budget||*workload.ReplayBudget;MaxFlows||int;MaxPendingFDs||int;Dial||func(flow.ID, uint16, time.Time, <-chan struct {}) (*rawsocket.Socket, error);OnTerminal||func(flow.ID, flow.Reason)"},
		{reflect.TypeFor[flow.RecordImage](), "ID|id|flow.ID;OpenSeq|open_seq|uint64;Intent|intent|[32]uint8;Phase|phase|flow.Phase;TxBase|tx_base|uint64;TxEnd|tx_end|uint64;RxApplied|rx_applied|uint64;RxReceived|rx_received|uint64;TxFIN|tx_fin|uint64;RxFIN|rx_fin|uint64;FINFlags|fin_flags|uint8;RawRef|raw_ref|string;ReplayRef|replay_ref|string"},
		{reflect.TypeFor[flow.RetirementImage](), "EndpointID|endpoint_id|flow.EndpointID;NextOpenSeq|next_open_seq|uint64;RetiredThrough|retired_through|uint64;Holes|holes|[]uint64"},
		{reflect.TypeFor[flow.Image](), "Version|version|int;Records|records|[]flow.RecordImage;Retirement|retirement|[]flow.RetirementImage;StateRef|state_ref|string;ReservedBytes|reserved_bytes|int64"},
		{reflect.TypeFor[tunnel.Allocation](), "SID||string;NID||string;Port||int;TokenHash||string;Epoch||int64"},
		{reflect.TypeFor[tunnel.AuthRequest](), "Allocation||flowstate.Allocation;Deadline||time.Time;Stop||<-chan struct {}"},
		{reflect.TypeFor[tunnel.AuthorizedKey](), "ID||string;Secret||[]uint8"},
		{reflect.TypeFor[tunnel.Authority](), "Enabled||bool;Allocation||flowstate.Allocation;Generation||int64;ProxyEpoch||int64;AppliedRevision||uint64;Keys||[]flowstate.AuthorizedKey"},
		{reflect.TypeFor[tunnel.FlowConfig](), "Budget||*workload.ReplayBudget;InstanceID||string;OwnerGen||uint64;BootID||string;MaxEndpoints||int;MaxPendingFDs||int;TokenLookup||tunnel.TokenLookupOperation;Authorize||tunnel.ResumeAuthorizer;BeforeLocalConnect||func(flow.ID, rawsocket.Tuple) error"},
		{reflect.TypeFor[tunnel.EndpointImage](), "ID|id|flow.EndpointID;OwnerGen|owner_gen|uint64;CarrierGen|carrier_gen|uint64;FlowIDs|flow_ids|[]flow.ID;StateRef|state_ref|string"},
		{reflect.TypeFor[tunnel.ClientImage](), "Version|version|int;Endpoints|endpoints|[]tunnel.EndpointImage;Flows|flows|flow.Image;StateRef|state_ref|string"},
		{reflect.TypeFor[tunnel.Census](), "Owned||int;Legacy||int;Admission||int;Rehome||int;Retained||int;Unsupported||bool"},
		{reflect.TypeFor[ssproxy.Image](), "Version|version|int;StateRef|state_ref|string;ListenerRef|listener_ref|string;ConnectionCount|connection_count|int;SecretBytes|secret_bytes|int64"},
		{reflect.TypeFor[ssproxy.Grant](), "Enabled||bool;Generation||int64;Epoch||int64;AppliedRevision||uint64;Keys||[]ssproxy.Key"},
		{reflect.TypeFor[sealedstate.Snapshot](), "Resources||[]handoff.Resource;Owned||[]*os.File"},
		{reflect.TypeFor[sealedstate.Field](), "Name||string;Type||uint8;U64||uint64;Bytes||[]uint8"},
		{reflect.TypeFor[sealedstate.Record](), "Kind||string;Fields||[]sealedstate.Field"},
		{reflect.TypeFor[sealedstate.CutState](), "Records||[]sealedstate.Record;Resources||[]handoff.Resource"},
	}
	for _, tc := range cases {
		t.Run(tc.typ.String(), func(t *testing.T) {
			fields := strings.Split(tc.fields, ";")
			if tc.typ.NumField() != len(fields) {
				t.Fatalf("fields=%d want=%d", tc.typ.NumField(), len(fields))
			}
			for i, spec := range fields {
				want := strings.Split(spec, "|")
				got := tc.typ.Field(i)
				if got.Name != want[0] || got.Tag.Get("json") != want[1] || got.Type.String() != want[2] {
					t.Errorf("field %d=%s/%s/%s want=%s", i, got.Name, got.Tag.Get("json"), got.Type, spec)
				}
			}
		})
	}
	if reflect.TypeFor[flow.ID]().Size() != 16 || reflect.TypeFor[flow.EndpointID]().Size() != 16 {
		t.Fatal("binary identities must be exactly 16 bytes")
	}
}

// Unknown fields and numeric overflow must be refused by strict image readers;
// public images retain zero fields, while only the future Bundle pointers omit nil.
// origin: hot-update S5 plan §2.1 / §5.1
func TestFlowSSPublicImagesStrictRoundTrip(t *testing.T) {
	im := flow.Image{Version: 1, Records: []flow.RecordImage{{ID: flow.ID{1}, OpenSeq: 7, Intent: [32]byte{2}, Phase: flow.FlowPhaseDraining, TxBase: 3, TxEnd: 9, RxApplied: 4, RxReceived: 8, TxFIN: 9, RxFIN: 8, FINFlags: 1, RawRef: "flow.opaque", ReplayRef: "flow.opaque"}}, Retirement: []flow.RetirementImage{{EndpointID: flow.EndpointID{3}, NextOpenSeq: 10, RetiredThrough: 6, Holes: []uint64{8}}}, StateRef: "flow-state.opaque", ReservedBytes: upgradebudget.StreamReplayBytes}
	client := tunnel.ClientImage{Version: 1, Endpoints: []tunnel.EndpointImage{{ID: flow.EndpointID{3}, OwnerGen: 4, CarrierGen: 5, FlowIDs: []flow.ID{{1}}, StateRef: "flow-endpoint.opaque"}}, Flows: im, StateRef: "flow-state.opaque"}
	ss := ssproxy.Image{Version: 1, StateRef: "ss-state.opaque", ListenerRef: "ss-listener.opaque", ConnectionCount: 2, SecretBytes: 128}
	for _, value := range []any{im, client, ss, flow.RecordImage{}, flow.RetirementImage{}, flow.Image{}, tunnel.EndpointImage{}, tunnel.ClientImage{}, ssproxy.Image{}} {
		typ := reflect.TypeOf(value)
		t.Run(typ.String(), func(t *testing.T) {
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			decoded := reflect.New(typ)
			reader := json.NewDecoder(bytes.NewReader(encoded))
			reader.DisallowUnknownFields()
			if err := reader.Decode(decoded.Interface()); err != nil {
				t.Fatal(err)
			}
			if err := reader.Decode(new(any)); err != io.EOF {
				t.Fatalf("unexpected trailing JSON: %v", err)
			}
			if !reflect.DeepEqual(value, decoded.Elem().Interface()) {
				t.Fatal("public metadata round trip lost responsibility")
			}
			var present map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &present); err != nil {
				t.Fatal(err)
			}
			if len(present) != typ.NumField() {
				t.Fatal("public image omitted fields")
			}
			unknown := append(append([]byte{}, encoded[:len(encoded)-1]...), []byte(`,"future_field":1}`)...)
			reader = json.NewDecoder(bytes.NewReader(unknown))
			reader.DisallowUnknownFields()
			if err := reader.Decode(reflect.New(typ).Interface()); err == nil || !strings.Contains(err.Error(), "unknown field") {
				t.Fatalf("unknown field accepted: %v", err)
			}
		})
	}
	for _, bad := range []string{
		`{"records":[{"tx_end":18446744073709551616}]}`, `{"records":[{"rx_applied":-1}]}`, `{"records":[{"phase":256}]}`, `{"records":[{"raw_ref":7}]}`, `{"records":[{"extra":1}]}`, `{"retirement":[{"holes":[-1]}]}`,
	} {
		reader := json.NewDecoder(strings.NewReader(bad))
		reader.DisallowUnknownFields()
		if err := reader.Decode(new(flow.Image)); err == nil {
			t.Fatalf("invalid image accepted: %s", bad)
		}
	}
	for _, bad := range []string{`{"endpoints":[{"extra":1}]}`, `{"flows":{"records":[{"future_pending":1}]}}`, `{"endpoints":[{"owner_gen":-1}]}`} {
		reader := json.NewDecoder(strings.NewReader(bad))
		reader.DisallowUnknownFields()
		if err := reader.Decode(new(tunnel.ClientImage)); err == nil {
			t.Fatalf("invalid client image accepted: %s", bad)
		}
	}
}

// Each semantic name has a fixed wire number; changing ordering or width breaks
// independently built peers even if our own encoder and decoder agree.
// origin: hot-update S5 plan §2.2 / §6.1 / §10.2
func TestFlowSSClosedEnumsKeepWireNumbers(t *testing.T) {
	cases := []struct {
		name  string
		got   []int
		first int
	}{
		{"flow.Kind", []int{int(flow.KindOpen), int(flow.KindOpenACK), int(flow.KindResume), int(flow.KindResumeOK), int(flow.KindReject), int(flow.KindData), int(flow.KindACK), int(flow.KindFIN), int(flow.KindClose)}, 1},
		{"flow.Phase", []int{int(flow.FlowPhaseOpening), int(flow.FlowPhaseConnecting), int(flow.FlowPhaseActive), int(flow.FlowPhaseDraining), int(flow.FlowPhaseTerminal)}, 1},
		{"flow.Reason", []int{int(flow.ReasonMalformed), int(flow.ReasonOffsetGap), int(flow.ReasonOffsetExpired), int(flow.ReasonIntentConflict), int(flow.ReasonUnknownFlow), int(flow.ReasonRetired), int(flow.ReasonRevoked), int(flow.ReasonStaleEpoch), int(flow.ReasonOwnerConflict), int(flow.ReasonCapacity), int(flow.ReasonUnauthorized), int(flow.ReasonDeadline), int(flow.ReasonUnsupported), int(flow.ReasonIOFailure), int(flow.ReasonNonceExhausted), int(flow.ReasonStateInvalid)}, 1},
		{"flow.ResumeScope", []int{int(flow.ResumeScopeEndpoint), int(flow.ResumeScopeFlow)}, 0},
		{"flow.ResumeMode", []int{int(flow.ResumeModeInitial), int(flow.ResumeModeReconnect), int(flow.ResumeModePrepare), int(flow.ResumeModeActivate), int(flow.ResumeModeAbort), int(flow.ResumeModeBind)}, 1},
		{"flow.ResumeVerdict", []int{int(flow.ResumeVerdictAccepted), int(flow.ResumeVerdictPending), int(flow.ResumeVerdictRejected)}, 1},
		{"flow.CloseMode", []int{int(flow.CloseModeNormal), int(flow.CloseModeReadAbort), int(flow.CloseModeWriteAbort), int(flow.CloseModeRevoke)}, 0},
		{"tunnel.EndpointPhase", []int{int(tunnel.EndpointPhaseBinding), int(tunnel.EndpointPhaseAttached), int(tunnel.EndpointPhaseDetached), int(tunnel.EndpointPhasePrepared), int(tunnel.EndpointPhaseClosing), int(tunnel.EndpointPhaseClosed)}, 1},
		{"tunnel.TicketPurpose", []int{int(tunnel.TicketPurposeCurrentOwner), int(tunnel.TicketPurposeHandoff)}, 1},
		{"ssproxy.SSPhase", []int{int(ssproxy.SSPhaseSalt), int(ssproxy.SSPhaseLength), int(ssproxy.SSPhasePayload), int(ssproxy.SSPhaseTarget), int(ssproxy.SSPhaseConnecting), int(ssproxy.SSPhaseRelay), int(ssproxy.SSPhaseDraining), int(ssproxy.SSPhaseRevoked), int(ssproxy.SSPhaseTerminal)}, 1},
		{"ssproxy.AuthorityPhase", []int{int(ssproxy.AuthorityPhasePending), int(ssproxy.AuthorityPhaseAuthorized), int(ssproxy.AuthorityPhaseRevoked)}, 1},
		{"rawsocket.ConnectState", []int{int(rawsocket.ConnectConnecting), int(rawsocket.ConnectConnected), int(rawsocket.ConnectFailed)}, 1},
		{"sealedstate.FieldType", []int{int(sealedstate.FieldU64), int(sealedstate.FieldNonnegativeI64), int(sealedstate.FieldBool), int(sealedstate.FieldBytes), int(sealedstate.FieldUTF8), int(sealedstate.FieldID16), int(sealedstate.FieldNonce12), int(sealedstate.FieldSalt32)}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for i, got := range tc.got {
				if got != i+tc.first {
					t.Errorf("member %d=%d want=%d", i, got, i+tc.first)
				}
			}
		})
	}
	if flow.Version != 1 || sealedstate.Version != 1 || ssproxy.Version != 1 || flow.HeaderBytes != 40 || flow.MaxDataBytes != 32768 || flow.MaxControlBytes != 32768 || flow.FlagDirection != 1 || flow.FlagFINReceipt != 2 || flow.FlagCloseReceipt != 4 {
		t.Fatal("frame/codec epoch, widths or flags changed")
	}
	if reflect.TypeFor[flow.Kind]().Kind() != reflect.Uint8 || reflect.TypeFor[flow.Phase]().Kind() != reflect.Uint8 || reflect.TypeFor[flow.Reason]().Kind() != reflect.Uint16 {
		t.Fatal("closed enum underlying width changed")
	}
}

// Use the real broker declarations: the shortest bad observation span starts at
// the first tick and ends at the third, not one full extra interval afterwards.
// origin: hot-update S5 plan §2.1 / §3.4 / §4.5
func TestFlowHandoffBudgetsBoundDetachedAndWatchdog(t *testing.T) {
	dwell := contractConstant(t, "../broker/proxy_reconcile.go", "proxyRehomeDwell")
	// The broker alias borrows the controller's literal, whose bound must still be checked.
	// origin: hot-update S6 plan §9.3 / HARDGATE-A
	tick := contractConstant(t, "../brokercontrol/clusterstatus.go", "ObserveTickInterval")
	contractConstantAlias(t, "../broker/observability.go", "observeTickInterval", "brokercontrol", "ObserveTickInterval")
	shortest := time.Duration(dwell-1) * time.Duration(tick) * time.Second
	if shortest != 10*time.Second || tunnel.DetachedGrace >= shortest {
		t.Fatalf("detach=%v shortest rehome span=%v", tunnel.DetachedGrace, shortest)
	}
	if tunnel.PermitTTL != upgradebudget.ComponentHandover || tunnel.ResumeAttempt > upgradebudget.LiveFlowStall || tunnel.LookupBudget >= tunnel.ResumeAttempt || tunnel.CarrierWriteBudget >= tunnel.ResumeAttempt || tunnel.ResumeAttempt >= tunnel.DetachedGrace {
		t.Fatal("operation/permit budgets violate handoff bounds")
	}
	if tunnel.FlowALPN != "tether-flow/1" || tunnel.DetachedGrace != 8*time.Second || tunnel.ResumeAttempt != 2*time.Second || tunnel.LookupBudget != time.Second || tunnel.CarrierWriteBudget != time.Second || tunnel.PermitTTL != 120*time.Second {
		t.Fatal("frozen operation budget changed")
	}
	if flow.MaxFlows != 32 || flow.MaxPendingFDs != 32 || tunnel.MaxEndpoints != 128 || ssproxy.MaxActors != 128 || ssproxy.MaxMaterialHistory != 64 || ssproxy.MaxKeys != 256 || sealedstate.MaxStateBytes != 32<<20 {
		t.Fatal("node admission/private-state caps changed")
	}
	if int64(flow.MaxFlows)*upgradebudget.StreamReplayBytes != upgradebudget.NodeReplayBytes || flow.MaxDataBytes > int(upgradebudget.StreamReplayBytes) || flow.MaxControlBytes > sealedstate.MaxStateBytes {
		t.Fatal("node/stream/pending budgets are inconsistent")
	}
}

// Reading only the moved literal would miss a broker alias that stopped using it.
// origin: hot-update S6 plan §9.3 / HARDGATE-A
func contractConstantAlias(t *testing.T, path, name, pkg, target string) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		block, ok := decl.(*ast.GenDecl)
		if !ok || block.Tok != token.CONST {
			continue
		}
		for _, spec := range block.Specs {
			value := spec.(*ast.ValueSpec)
			for i, id := range value.Names {
				if id.Name != name {
					continue
				}
				if i < len(value.Values) {
					if sel, ok := value.Values[i].(*ast.SelectorExpr); ok && sel.Sel.Name == target {
						if owner, ok := sel.X.(*ast.Ident); ok && owner.Name == pkg {
							return
						}
					}
				}
				t.Fatalf("budget alias %s must borrow %s.%s", name, pkg, target)
			}
		}
	}
	t.Fatalf("budget alias %s missing", name)
}

func contractConstant(t *testing.T, path, name string) int64 {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		block, ok := decl.(*ast.GenDecl)
		if !ok || block.Tok != token.CONST {
			continue
		}
		for _, spec := range block.Specs {
			value := spec.(*ast.ValueSpec)
			for i, id := range value.Names {
				if id.Name != name {
					continue
				}
				expression := value.Values[i]
				if product, ok := expression.(*ast.BinaryExpr); ok {
					unit, ok := product.Y.(*ast.SelectorExpr)
					if !ok || product.Op != token.MUL || unit.Sel.Name != "Second" {
						t.Fatal("budget constant no longer uses seconds")
					}
					expression = product.X
				}
				literal, ok := expression.(*ast.BasicLit)
				if !ok {
					t.Fatal("budget constant is not a bounded literal")
				}
				n, err := strconv.ParseInt(literal.Value, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				return n
			}
		}
	}
	t.Fatalf("budget declaration %s missing", name)
	return 0
}
