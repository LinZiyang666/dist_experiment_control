//go:build linux && infrabusiness_integration

package infrabusiness_test

import (
	"errors"
	"fmt"

	i "github.com/LinZiyang666/tether/internal/infracontract"
)

// The scope is supplied by the sealed cut manifest, never inferred from the
// receipt being judged. Direction prevents an outbound ACK signing a return.
type evidenceScope struct {
	Tx                              i.ID
	Host, BootID, EvidenceDirection string
}

type ncTarget struct{ Ingress, Runtime i.Process }
type cutNC struct {
	Identity                  ncIdentity
	Before, Moving, Returning ncTarget
}
type authRequirement struct {
	Participant                      i.ID
	Case, Direction, ServerID        string
	Runtime                          i.Process
	Generation, AuthCID, BusinessCID uint64
}

func scopeFor(h hopReceipt, direction string) evidenceScope {
	return evidenceScope{Tx: h.Tx, Host: h.Host, BootID: h.BootID, EvidenceDirection: direction}
}

// origin: S9 internal review rv-business F6
func sameConnection(a, b i.Attachment) bool { return a.ServerID == b.ServerID && a.CID == b.CID }

func scopedHops(r businessReceipt) (map[uint32]hopReceipt, error) {
	hops := make(map[uint32]hopReceipt)
	for _, h := range r.Hops {
		if !knownCutHost(r, h.Host) || h.Step == 0 || h.Tx == (i.ID{}) || h.Host == "" || h.BootID == "" || h.BootID != h.Before.BootID || h.BootID != h.Current.BootID || hops[h.Step].Step != 0 {
			return nil, errors.New("cut manifest has an unknown transaction, host, boot or duplicate step")
		}
		hops[h.Step] = h
	}
	if len(hops) == 0 {
		return nil, errors.New("cut manifest is empty")
	}
	return hops, nil
}

// origin: S9 internal review rv-business F3
// Each cut seals its own NC set and physical targets. Row-local fresh sockets
// are insufficient: a previous return must be the next cut's exact old NC.
func checkAttachmentScopes(r businessReceipt) error {
	hops, err := scopedHops(r)
	if err != nil {
		return err
	}
	type key struct {
		step        uint32
		participant i.ID
	}
	targets := make(map[key]cutNC)
	for _, h := range r.Hops {
		count := 0
		for _, id := range r.RequiredNC {
			if id.HomeHost == h.Host {
				count++
			}
		}
		if count == 0 || len(h.RequiredNC) != count {
			return errors.New("cut lost its sealed NC required set")
		}
		for _, target := range h.RequiredNC {
			if target.Identity.HomeHost != h.Host {
				return errors.New("NC participant belongs to another cut host")
			}
			k := key{h.Step, target.Identity.Participant}
			if _, exists := targets[k]; exists {
				return errors.New("cut repeats an NC participant")
			}
			for _, endpoint := range []ncTarget{target.Before, target.Moving, target.Returning} {
				if !completeProcess(endpoint.Ingress) || !completeProcess(endpoint.Runtime) {
					return errors.New("cut target has no physical runtime")
				}
			}
			before, after := target.Before.Runtime, target.Returning.Runtime
			if h.Component == "caddy" {
				before, after = target.Before.Ingress, target.Returning.Ingress
			}
			if before != h.Before || after != h.Current {
				return errors.New("NC target does not belong to this component cut")
			}
			targets[k] = target
		}
	}
	rows := make(map[key]attachmentReceipt)
	for _, row := range r.Attachments {
		h, ok := hops[row.Step]
		k := key{row.Step, row.Identity.Participant}
		target, exists := targets[k]
		if !ok || !exists || row.evidenceScope != scopeFor(h, "round-trip") || target.Identity != row.Identity ||
			(ncTarget{row.Old.Ingress, row.OldRuntime}) != target.Before || (ncTarget{row.New.Ingress, row.NewRuntime}) != target.Moving ||
			(ncTarget{row.Returned.Ingress, row.ReturnedRuntime}) != target.Returning {
			return errors.New("NC proof is foreign to the sealed cut or physical target")
		}
		if _, duplicate := rows[k]; duplicate {
			return errors.New("NC proof repeats a cut participant")
		}
		rows[k] = row
	}
	previous := make(map[i.ID]attachmentReceipt)
	for _, h := range r.Hops {
		for _, target := range h.RequiredNC {
			row, ok := rows[key{h.Step, target.Identity.Participant}]
			if !ok {
				return errors.New("cut participant has no attachment proof")
			}
			if prev, exists := previous[row.Identity.Participant]; exists && (prev.Returned != row.Old || prev.ReturnedRuntime != row.OldRuntime) {
				return errors.New("NC old attachment does not continue the previous return")
			}
			previous[row.Identity.Participant] = row
		}
	}
	return nil
}

func authKey(step uint32, target authRequirement) string {
	return fmt.Sprintf("%d/%x/%s/%s/%s/%d", step, target.Participant, target.Case, target.Direction, target.ServerID, target.Generation)
}

// origin: S9 internal review rv-business F3
// Positive requirements are sealed before the cut; negative scenarios have
// independent keys and cannot fill a missing local queue on any real hop.
func checkAuthScopes(r businessReceipt) error {
	if err := checkAttachments(r); err != nil {
		return err
	}
	hops, err := scopedHops(r)
	if err != nil {
		return err
	}
	required := make(map[string]authRequirement)
	for _, h := range r.Hops {
		directions := make(map[string]bool)
		for _, target := range h.RequiredAuth {
			if target.Participant == (i.ID{}) || target.ServerID == "" || !completeProcess(target.Runtime) || target.Generation == 0 ||
				target.AuthCID == 0 || target.BusinessCID == 0 || target.AuthCID == target.BusinessCID ||
				target.Case != "cold" && target.Case != "hot" && target.Case != "ordinary-reconnect" ||
				target.Direction != "outbound" && target.Direction != "return" {
				return errors.New("cut has an incomplete auth requirement")
			}
			// The ordinary queue belongs to a broker auth NC in this cut, not to a
			// healthy remote peer whose proof happened to arrive in the same window.
			matched := false
			for _, nc := range h.RequiredNC {
				endpoint := nc.Moving
				if target.Direction == "return" {
					endpoint = nc.Returning
				}
				if nc.Identity.Participant == target.Participant && nc.Identity.Owner == "broker" && nc.Identity.Role == "auth" && endpoint.Runtime == target.Runtime {
					matched = true
				}
			}
			if matched {
				matched = false
				for _, a := range r.Attachments {
					physical := a.New
					if target.Direction == "return" {
						physical = a.Returned
					}
					if a.Step == h.Step && a.Identity.Participant == target.Participant && physical.ServerID == target.ServerID && physical.CID == target.AuthCID && physical.Generation == target.Generation {
						matched = true
					}
				}
			}
			if matched {
				matched = false
				for _, a := range r.Attachments {
					physical, runtime := a.New, a.NewRuntime
					if target.Direction == "return" {
						physical, runtime = a.Returned, a.ReturnedRuntime
					}
					if a.Step == h.Step && a.Identity.Owner == "broker" && a.Identity.Role == "control" && runtime == target.Runtime && physical.ServerID == target.ServerID && physical.CID == target.BusinessCID {
						matched = true
					}
				}
			}
			if !matched {
				return errors.New("auth server is outside the local cut NC set")
			}
			k := authKey(h.Step, target)
			if _, duplicate := required[k]; duplicate {
				return errors.New("duplicate auth requirement")
			}
			required[k], directions[target.Direction] = target, true
		}
		if !directions["outbound"] || !directions["return"] {
			return errors.New("cut lacks a required outbound or return auth server")
		}
	}
	seen, negatives := make(map[string]bool), make(map[string]bool)
	for _, row := range r.Auth {
		h, ok := hops[row.Step]
		if !ok || row.evidenceScope != scopeFor(h, row.EvidenceDirection) || row.EvidenceDirection != "outbound" && row.EvidenceDirection != "return" {
			return errors.New("auth evidence belongs to a foreign cut")
		}
		if row.Scenario != "normal" {
			if row.Scenario != row.Case || row.Admitted || negatives[row.Scenario] {
				return errors.New("negative auth scenario is missing or duplicated")
			}
			negatives[row.Scenario] = true
			continue
		}
		target := authRequirement{Participant: row.Participant, Case: row.Case, Direction: row.EvidenceDirection, ServerID: row.ServerID,
			Runtime: row.Runtime, Generation: row.Generation, AuthCID: row.AuthCID, BusinessCID: row.BusinessCID}
		k := authKey(row.Step, target)
		if required[k] != target || seen[k] || row.AuthSUBNS < h.StartNS || row.MemberRPCNS > h.EndNS || row.AgentRPCNS > h.EndNS {
			return errors.New("auth proof is replayed, missing or outside its cut window")
		}
		seen[k] = true
	}
	if len(seen) != len(required) {
		return errors.New("a cut is missing its local normal auth queue")
	}
	for _, name := range []string{"missing-normal-queue", "js503", "ack-prefix-loss", "replica-insufficient"} {
		if !negatives[name] {
			return errors.New("independent negative auth scenario is missing")
		}
	}
	return nil
}

func completeAttachment(a i.Attachment) bool {
	return a.Participant != (i.ID{}) && a.Generation > 0 && a.CID > 0 && a.ServerID != "" && a.URL != "" && a.HealthGeneration > 0 &&
		a.Raw.NetNS > 0 && a.Raw.Device > 0 && a.Raw.Inode > 0 && a.Raw.Cookie > 0 && a.Raw.Local != "" && a.Raw.Remote != "" &&
		completeProcess(a.Raw.Owner) && completeProcess(a.Ingress)
}

type ncIdentity struct {
	Participant           i.ID
	Owner, Role, HomeHost string
}

func knownCutHost(r businessReceipt, host string) bool {
	for _, h := range r.Hosts {
		if h == host {
			return host != ""
		}
	}
	return false
}
