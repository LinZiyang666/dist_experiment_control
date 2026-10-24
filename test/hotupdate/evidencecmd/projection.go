package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"slices"

	"github.com/LinZiyang666/tether/internal/upgradetx"
	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// The archive reader already validated the complete upstream schema. Compare
// canonical JSON with exact numbers so large revisions/times cannot round into
// equality. This comparison preserves every upstream field, including fields
// that the pure judge does not need to inspect.
// origin: S10 internal review rv-honesty F1
func sameJSON(a, b []byte) bool {
	decode := func(raw []byte) (any, error) {
		var value any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		err := decoder.Decode(&value)
		return value, err
	}
	x, err := decode(a)
	y, other := decode(b)
	return err == nil && other == nil && reflect.DeepEqual(x, y)
}

// Raw is archive transport metadata, assigned after the producer's bytes exist;
// including its own hash in those bytes would require a digest fixed point.
// Every observed field must still equal the strictly decoded producer record.
// origin: S10 internal review rv-honesty F1
func checkProjections(e c.Evidence, archive map[c.RawRef][]byte) error {
	bad := errors.New("fullstack projection disagrees with raw evidence")
	for _, sample := range e.Samples {
		var actual c.Sample
		if sample.Raw.Schema != "sample" || c.DecodeJSON("sample", archive[sample.Raw], &actual) != nil {
			return bad
		}
		actual.Raw = sample.Raw
		got, err := c.EncodeJSON("sample", actual)
		want, other := c.EncodeJSON("sample", sample)
		if err != nil || other != nil || !sameJSON(got, want) {
			return bad
		}
	}
	for _, attempt := range e.Attempts {
		var actual c.Attempt
		if attempt.Raw.Schema != "attempt" || c.DecodeJSON("attempt", archive[attempt.Raw], &actual) != nil {
			return bad
		}
		actual.Raw = attempt.Raw
		got, err := c.EncodeJSON("attempt", actual)
		want, other := c.EncodeJSON("attempt", attempt)
		if err != nil || other != nil || !sameJSON(got, want) {
			return bad
		}
	}
	for _, interval := range e.Intervals {
		var actual c.Interval
		if interval.Raw.Schema != "interval" || c.DecodeJSON("interval", archive[interval.Raw], &actual) != nil {
			return bad
		}
		actual.Raw = interval.Raw
		got, err := c.EncodeJSON("interval", actual)
		want, other := c.EncodeJSON("interval", interval)
		if err != nil || other != nil || !sameJSON(got, want) {
			return bad
		}
	}
	for _, view := range e.Views {
		if !checkViewProjection(view, archive) {
			return bad
		}
	}
	return nil
}

// s8_view has no lossy fallback: history and observation are joined unchanged.
// Receipt refs must point to those same historical seals, not another successful
// transaction that happens to have the desired image SHA.
// origin: S10 internal review rv-honesty F1/F3
func checkViewProjection(v c.View, archive map[c.RawRef][]byte) bool {
	var upstream upgradetx.View
	if json.Unmarshal(archive[v.Raw], &upstream) != nil {
		return false
	}
	var actual struct {
		Historical  json.RawMessage `json:"historical"`
		Metadata    json.RawMessage `json:"metadata"`
		Observation json.RawMessage `json:"observation"`
		Code        string          `json:"code"`
		Detail      string          `json:"detail"`
	}
	var history struct {
		Record   json.RawMessage `json:"record"`
		Metadata json.RawMessage `json:"metadata"`
	}
	var observation struct {
		Current struct {
			Direction string          `json:"direction"`
			Phase     string          `json:"phase"`
			Outcome   string          `json:"outcome"`
			Control   json.RawMessage `json:"control"`
			Resources json.RawMessage `json:"resources"`
			Released  json.RawMessage `json:"released"`
		} `json:"current"`
	}
	if v.Raw.Schema != "s8_view" || json.Unmarshal(archive[v.Raw], &actual) != nil || json.Unmarshal(v.Historical, &history) != nil ||
		json.Unmarshal(v.Current, &observation) != nil || !sameJSON(actual.Historical, history.Record) ||
		!sameJSON(actual.Metadata, history.Metadata) || !sameJSON(actual.Observation, v.Current) || v.Code != actual.Code || v.Detail != actual.Detail {
		return false
	}
	phase, outcome := observation.Current.Phase, observation.Current.Outcome
	if observation.Current.Direction == "reverse" && phase == "completed" && outcome == "success" {
		phase, outcome = "rolled_back", "rolled_back"
	}
	if v.Phase != phase || v.Outcome != outcome {
		return false
	}
	if !projectedStamp(upstream.Observation.Census.Stamp, v.Stamp) || string(upstream.Observation.Current.Direction) != v.Stamp.Direction {
		return false
	}
	for i, receipt := range []c.Receipt{v.Control, v.Resources, v.Released} {
		raw := []json.RawMessage{observation.Current.Control, observation.Current.Resources, observation.Current.Released}[i]
		seal := []upgradetx.Receipt{upstream.Observation.Current.Control, upstream.Observation.Current.Resources, upstream.Observation.Current.Released}[i]
		ids := make([]string, 0, len(seal.Census.Resources))
		for _, resource := range seal.Census.Resources {
			ids = append(ids, resource.ID)
		}
		slices.Sort(ids)
		projectedIDs := slices.Clone(receipt.ResourceIDs)
		slices.Sort(projectedIDs)
		if receipt.Evidence.Schema != "s8_receipt" || !sameJSON(raw, archive[receipt.Evidence]) ||
			receipt.Kind != string(seal.Kind) || receipt.Known != seal.Known || receipt.Complete != seal.Complete ||
			hex.EncodeToString(receipt.RunningSHA[:]) != seal.RunningSHA || !slices.Equal(ids, projectedIDs) ||
			receipt.Stamp.Process != v.Stamp.Process || receipt.Stamp.Direction != v.Stamp.Direction || !projectedStamp(seal.Stamp, receipt.Stamp) {
			return false
		}
	}
	return true
}

// Host/unit/PID namespace are collector axes absent from S8's DTO. They stay in
// the surrounding process binding; every axis actually supplied by S8 is joined.
// origin: S10 internal review rv-honesty F1
func projectedStamp(s upgradetx.Stamp, p c.Stamp) bool {
	k, owner := s.Attempt.Key, s.Producer
	return k.DomainID == p.Process.Key.Domain && string(k.Component) == p.Process.Key.Role && k.SID == p.Process.Key.SID &&
		k.ConfiguredNID == p.Process.Key.NID && k.UID == p.Process.Key.UID && s.Attempt.TxID == p.TxID && s.Attempt.CutID == p.CutID &&
		s.Attempt.Generation == p.Generation && s.Attempt.Revision == p.Revision && s.Nonce == p.QueryNonce && s.Sequence == p.Sequence &&
		s.At == p.AtBootNS && s.ScopeDigest == p.ScopeDigest && owner.BootID == p.Process.BootID && owner.PID == p.Process.PID &&
		owner.StartTicks == p.Process.StartTicks && owner.InstanceID == p.Process.InstanceID
}

// Bindings are checked independently of the evidence envelope. A manifest may
// not relabel another schema, duplicate an action, omit query/rollback, or hide
// an unarchived help receipt. Command bytes remain data and are never executed.
// P18 still owns exact CLI/help provenance; this boundary cannot invent an
// upstream view decoder for a component whose schema has not landed.
// origin: S10 internal review rv-honesty F1
func checkCommandBindings(p c.Profile, raw []byte) ([]c.RawRef, error) {
	bad := errors.New("invalid fullstack command bindings")
	if p.CommandBindings.Schema != "command_bindings" {
		return nil, bad
	}
	var document struct {
		Bindings []struct {
			ID     string          `json:"id"`
			Role   string          `json:"role"`
			Action string          `json:"action"`
			Help   json.RawMessage `json:"help"`
			Schema string          `json:"raw_view_schema"`
		} `json:"bindings"`
	}
	if json.Unmarshal(raw, &document) != nil {
		return nil, bad
	}
	ids, actions := map[string]bool{}, map[string]bool{}
	var refs []c.RawRef
	for _, binding := range document.Bindings {
		key := binding.Role + "/" + binding.Action
		if ids[binding.ID] || actions[key] || binding.Schema != "s8_view" || !slices.Contains([]string{"agent", "broker", "nats", "caddy"}, binding.Role) {
			return nil, bad
		}
		ids[binding.ID], actions[key] = true, true
		var ref c.RawRef
		if c.DecodeJSON("raw_ref", binding.Help, &ref) != nil {
			return nil, bad
		}
		refs = append(refs, ref)
	}
	for _, key := range p.Required {
		if !slices.Contains([]string{"agent", "broker", "nats", "caddy"}, key.Role) {
			continue
		}
		for _, action := range []string{"stage", "apply", "query", "rollback"} {
			if !actions[key.Role+"/"+action] {
				return nil, bad
			}
		}
	}
	return refs, nil
}
