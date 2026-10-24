package adminsock

import (
	"encoding/json"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// These aliases keep admin's transport adaptation explicit while avoiding a
// dependency from proto to the transaction adapter.
// origin: hot-update S8 plan §2.2 / P13
// UpgradeEndpoint borrows only an operation budget and the server's stop
// signal. The control adapter owns the bounded contexts used by its engine.
// origin: S7 internal review GATE-CONVERGE-layering F2
type UpgradeEndpoint func([]byte, upgradewire.Access, controlbudget.Deadline, <-chan struct{}) upgradewire.Response
type UpgradeKey = upgradetx.Key
type UpgradeRequest = upgradewire.Request
type UpgradeResponse = upgradewire.Response

// Existing admin verbs retain their exact JSON shape. Transaction envelopes
// use the strict per-op codec, including correct omission of value structs.
// origin: hot-update S8 plan §2.2 / P13
func (r Request) MarshalJSON() ([]byte, error) {
	if r.Upgrade != nil {
		if r.Op != "" && r.Op != r.Upgrade.Op {
			return nil, upgradeAdminError()
		}
		type legacy Request
		extra := r
		extra.Op = ""
		extra.Upgrade = nil
		body, err := json.Marshal(legacy(extra))
		if err != nil || string(body) != `{"op":""}` {
			return nil, upgradeAdminError()
		}
		return upgradewire.EncodeRequest(*r.Upgrade)
	}
	type legacy Request
	return json.Marshal(legacy(r))
}
func (r Response) MarshalJSON() ([]byte, error) {
	if r.Upgrade != nil {
		return upgradewire.EncodeResponse(*r.Upgrade)
	}
	type legacy Response
	return json.Marshal(legacy(r))
}
func (r *Response) UnmarshalJSON(body []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return err
	}
	if fields["op"] == nil {
		response, err := upgradewire.DecodeResponse(body)
		*r = Response{Code: response.Code, Error: response.Error, Upgrade: &response}
		return err
	}
	type legacy Response
	return json.Unmarshal(body, (*legacy)(r))
}
func upgradeAdminError() error {
	return &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: "transaction envelope cannot contain legacy admin fields"}
}
func upgradeInconsistencyDetail(reason string) string { return upgradewire.InconsistencyDetail(reason) }
