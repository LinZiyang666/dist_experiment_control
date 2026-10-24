//go:build linux && infrabusiness_integration

package infrabusiness_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	i "github.com/LinZiyang666/tether/internal/infracontract"
)

// origin: S9 internal review rv-business F3
func TestBusinessOracleBindsEveryCutProof(t *testing.T) {
	for name, mutate := range map[string]func(*businessReceipt){
		"transaction":             func(r *businessReceipt) { r.Attachments[0].Tx[0]++ },
		"host":                    func(r *businessReceipt) { r.Attachments[0].Host = "peer" },
		"boot":                    func(r *businessReceipt) { r.Attachments[0].BootID = "another-boot" },
		"direction":               func(r *businessReceipt) { r.Attachments[0].EvidenceDirection = "return" },
		"old_generation_replayed": func(r *businessReceipt) { r.Attachments[1].Old.Raw.Cookie += 10000 },
		"old_runtime_replayed":    func(r *businessReceipt) { r.Attachments[1].OldRuntime.PID++ },
		"caddy_wrong_pid":         func(r *businessReceipt) { r.Attachments[3].New.Ingress.PID++ },
		"caddy_wrong_exe":         func(r *businessReceipt) { r.Attachments[3].New.Ingress.ExeSHA256[0]++ },
		"target_outside_step": func(r *businessReceipt) {
			r.Hops[3].RequiredNC[0].Returning.Ingress.PID++
			r.Attachments[3].Returned.Ingress = r.Hops[3].RequiredNC[0].Returning.Ingress
			r.Attachments[4].Old.Ingress = r.Attachments[3].Returned.Ingress
			r.Hops[4].RequiredNC[0].Before.Ingress = r.Attachments[4].Old.Ingress
		},
		"sealed_nc_missing": func(r *businessReceipt) { r.Hops[2].RequiredNC = r.Hops[2].RequiredNC[1:] },
	} {
		t.Run(name, func(t *testing.T) {
			r := oracleReceipt()
			if err := checkAttachments(r); err != nil {
				t.Fatal("positive attachment control: ", err)
			}
			mutate(&r)
			if checkAttachments(r) == nil {
				t.Fatal("accepted a foreign or replayed cut proof")
			}
		})
	}
	for name, mutate := range map[string]func(*businessReceipt){
		"transaction":             func(r *businessReceipt) { r.Auth[0].Tx[0]++ },
		"host":                    func(r *businessReceipt) { r.Auth[0].Host = "peer" },
		"boot":                    func(r *businessReceipt) { r.Auth[0].BootID = "another-boot" },
		"generation":              func(r *businessReceipt) { r.Auth[0].Generation++ },
		"server":                  func(r *businessReceipt) { r.Auth[0].ServerID = "peer" },
		"participant":             func(r *businessReceipt) { r.Auth[0].Participant = i.ID{99} },
		"outside_window":          func(r *businessReceipt) { r.Auth[0].AgentRPCNS = r.Hops[0].EndNS + 1 },
		"negative_in_normal_slot": func(r *businessReceipt) { r.Auth[0] = r.Auth[len(r.Auth)-1] },
		"missing_host_queue":      func(r *businessReceipt) { r.Auth = r.Auth[1:] },
		"empty_host_requiredset":  func(r *businessReceipt) { r.Hops[0].RequiredAuth = nil },
		"required_business_cid":   func(r *businessReceipt) { r.Hops[0].RequiredAuth[0].BusinessCID++; r.Auth[0].BusinessCID++ },
		"required_peer_server":    func(r *businessReceipt) { r.Hops[0].RequiredAuth[0].ServerID = "peer"; r.Auth[0].ServerID = "peer" },
	} {
		t.Run("auth_"+name, func(t *testing.T) {
			r := threeHostAuthControl()
			if err := checkAuth(r); err != nil {
				t.Fatal("positive three-host auth control: ", err)
			}
			mutate(&r)
			if checkAuth(r) == nil {
				t.Fatal("accepted missing or foreign per-host auth evidence")
			}
		})
	}
}

// Each host keeps its own original participants across all three cuts. This
// catches global case-name deduplication and absence of one host's local queue.
func threeHostAuthControl() businessReceipt {
	var r businessReceipt
	for host := 0; host < 3; host++ {
		local := oracleReceipt()
		name := fmt.Sprintf("host-%d", host)
		r.Hosts = append(r.Hosts, name)
		for n := range local.RequiredNC {
			local.RequiredNC[n].HomeHost = name
			local.RequiredNC[n].Participant[1] = byte(host + 1)
		}
		local.Attachments = nil
		for n := range local.Hops {
			h := &local.Hops[n]
			h.Step, h.Host, h.BootID = uint32(n*3+host+1), name, "boot-"+name
			h.Tx[1] = byte(host + 1)
			h.Before.BootID, h.Current.BootID = h.BootID, h.BootID
			h.StartNS, h.CutNS, h.EndNS = int64(h.Step-1)*int64(time.Second), int64(h.Step-1)*int64(time.Second)+int64(200*time.Millisecond), int64(h.Step)*int64(time.Second)
			h.RequiredNC, h.RequiredAuth = nil, nil
		}
		populateAttachmentControls(&local)
		r.RequiredNC, r.Attachments = append(r.RequiredNC, local.RequiredNC...), append(r.Attachments, local.Attachments...)
		r.Hops = append(r.Hops, local.Hops...)
	}
	// The observer orders the cut manifest, regardless of producer arrival order.
	ordered := make([]hopReceipt, len(r.Hops))
	for _, h := range r.Hops {
		ordered[h.Step-1] = h
	}
	r.Hops = ordered
	populateAuthControls(&r)
	return r
}

// origin: S9 internal review rv-business F6
func TestBusinessOracleRejectsReusedPhysicalConnections(t *testing.T) {
	for _, name := range []string{"same_server_same_cid", "url_only", "raw_socket", "generation", "accepted_pid", "accepted_exe"} {
		t.Run(name, func(t *testing.T) {
			r := oracleReceipt()
			routes := routeControls()
			mutate := func(old i.Attachment, next *i.Attachment) {
				switch name {
				case "same_server_same_cid":
					next.ServerID, next.CID = old.ServerID, old.CID
				case "url_only":
					*next = old
					next.URL = "wss://new.invalid:443"
				case "raw_socket":
					next.Raw = old.Raw
				case "generation":
					next.Generation = old.Generation
				case "accepted_pid":
					next.Ingress.PID++
				case "accepted_exe":
					next.Ingress.ExeSHA256[0]++
				}
			}
			mutate(r.Attachments[0].Old, &r.Attachments[0].New)
			mutate(routes[1].Old, &routes[1].New)
			if checkAttachments(r) == nil {
				t.Fatal("NC oracle accepted reused physical proof")
			}
			if checkRoutes(routes) == nil {
				t.Fatal("route oracle accepted reused physical proof")
			}
		})
	}
}

// Ensure every added observation field remains in strict receipt custody.
// origin: S9 internal review rv-business F3
func TestBusinessOracleRoundTripsScopedReceipt(t *testing.T) {
	r := reviewBusinessReceipt(t)
	body, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var decoded businessReceipt
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := checkMixed(decoded); err != nil {
		t.Fatal(err)
	}
	if err := checkAuth(decoded); err != nil {
		t.Fatal(err)
	}
	if err := checkTransfers(decoded); err != nil {
		t.Fatal(err)
	}
}
