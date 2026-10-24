//go:build linux && infrabusiness_integration

package infrabusiness_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	i "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/inframaint"
	"github.com/LinZiyang666/tether/internal/infratransport"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats.go"
)

type routeReceipt struct {
	Target                                                   i.Process
	Case, HomeBefore, HomeAfter, ReaperEvidence, CASEvidence string
	Old, New                                                 i.Attachment
	FirstInstallNS, LastReplyNS, ExpiryNS, ObservedNS        int64
	Protected, Reaped, Revoked, UnknownSource                bool
	PendingGeneration                                        uint64
}

type attachmentReceipt struct {
	Step uint32
	evidenceScope
	OldRuntime, NewRuntime, ReturnedRuntime  i.Process
	Identity                                 ncIdentity
	Old, New, Returned                       i.Attachment
	PrepareACK, BindACK, ReturnACK, Complete bool
	CensusDigest                             string
}

// origin: hot-update S9 plan §4.2 / §4.6 / T68 / T69 / M17 / M25
func checkAttachments(r businessReceipt) error {
	count := 0
	for _, h := range r.Hops {
		count += len(h.RequiredNC)
	}
	if len(r.RequiredNC) == 0 || len(r.Attachments) != count {
		return errors.New("sealed NC registry lost an independent connection")
	}
	required := make(map[i.ID]ncIdentity)
	roles := map[string]bool{"broker/auth": false, "broker/control": false, "agent/control": false, "agent/data": false, "ctl/control": false, "ctl/data": false}
	for _, id := range r.RequiredNC {
		if !knownCutHost(r, id.HomeHost) || id.Participant == (i.ID{}) || required[id.Participant] != (ncIdentity{}) || id.Owner != "broker" && id.Owner != "agent" && id.Owner != "ctl" ||
			id.Role != "auth" && id.Role != "control" && id.Role != "data" && id.Role != "consumer" {
			return errors.New("required NC census is incomplete")
		}
		required[id.Participant] = id
		roles[id.Owner+"/"+id.Role] = true
	}
	for _, present := range roles {
		if !present {
			return errors.New("a data/control/auth NC producer is missing")
		}
	}
	if err := checkAttachmentScopes(r); err != nil {
		return err
	}
	seen := make(map[string]bool)
	for _, row := range r.Attachments {
		key := fmt.Sprintf("%d/%x", row.Step, row.Identity.Participant)
		if row.Step == 0 || int(row.Step) > len(r.Hops) || seen[key] || required[row.Identity.Participant] != row.Identity ||
			!row.PrepareACK || !row.BindACK || !row.ReturnACK || !row.Complete || !validDigest(row.CensusDigest) ||
			!completeAttachment(row.Old) || !completeAttachment(row.New) || !completeAttachment(row.Returned) ||
			row.Old.Participant != row.Identity.Participant || row.New.Participant != row.Identity.Participant || row.Returned.Participant != row.Identity.Participant ||
			row.New.Generation <= row.Old.Generation || row.Returned.Generation <= row.New.Generation ||
			sameConnection(row.New, row.Old) || sameConnection(row.Returned, row.New) || row.New.Raw.Cookie == row.Old.Raw.Cookie || row.Returned.Raw.Cookie == row.New.Raw.Cookie {
			return errors.New("NC successor/return ACK lacks a fresh physical attachment")
		}
		seen[key] = true
	}
	return nil
}

// origin: hot-update S9 plan §4.5 / §4.6 / T69
func TestInfraRouteReaperIntegration(t *testing.T) {
	t.Run("raw_wedge", exerciseRawWedge)
	t.Run("signed_hold", exerciseSignedHold)
	t.Run("oracle_controls", func(t *testing.T) {
		rows := routeControls()
		if err := checkRoutes(rows); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"renewal", "old_cid", "missing_case", "protected_after_expiry", "home_cas", "unknown", "late_worker"} {
			t.Run(name, func(t *testing.T) {
				r := append([]routeReceipt(nil), rows...)
				switch name {
				case "renewal":
					r[0].ExpiryNS++
				case "old_cid":
					r[1].New.CID = r[1].Old.CID
				case "missing_case":
					r = r[1:]
				case "protected_after_expiry":
					r[2].Protected = true
				case "home_cas":
					r[4].Reaped = false
				case "unknown":
					r[5].Protected = true
				case "late_worker":
					r[6].Protected = true
				}
				if checkRoutes(r) == nil {
					t.Fatal("route/reaper oracle accepted a stale owner or renewed hold")
				}
			})
		}
	})
	t.Run("live", func(t *testing.T) {
		r := runLive(t, "route-reaper")
		if err := checkRoutes(r.Routes); err != nil {
			t.Fatal(err)
		}
		if err := checkMixed(r); err != nil {
			t.Fatal(err)
		}
	})
}

// Real nats.go writes HTTP/TLS into a peer that stops reading. Retirement must
// close the raw connection and join that writer, while a separate ORIGINAL
// application TCP socket keeps carrying bytes. No reconnect callback decides
// the business result, and natural five-second handshake expiry cannot pass.
// origin: hot-update S9 plan §4.5 / T69 / M16 / M25
func exerciseRawWedge(t *testing.T) {
	for _, scheme := range []string{"ws", "wss"} {
		t.Run(scheme, func(t *testing.T) {
			client, target := businessSockets(t)
			defer closeTestResource(t, client)
			defer closeTestResource(t, target)
			inode := socketInode(t, client)
			raw, peer := net.Pipe()
			defer closeTestResource(t, peer)
			tr, err := infratransport.New(1, deadline(t), nil, blockedDialer{raw: raw})
			if err != nil {
				t.Fatal(err)
			}
			defer tr.Interrupt()
			done := make(chan error, 1)
			go func() {
				_, err := tr.Connect(scheme+"://blocked.invalid:443", func() []nats.Option { return []nats.Option{nats.NoReconnect()} })
				done <- err
			}()
			if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			var first [1]byte
			if _, err := io.ReadFull(peer, first[:]); err != nil || scheme == "ws" && first[0] != 'G' || scheme == "wss" && first[0] != 22 {
				t.Fatal("real HTTP/TLS raw write was not entered")
			}
			select {
			case <-done:
				t.Fatal("raw handshake was not actually blocked")
			default:
			}
			start := time.Now()
			if err := infratransport.Move(tr, nil, deadline(t), nil); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("interrupted handshake reported success")
				}
			case <-time.After(time.Second):
				t.Fatal("raw write worker was abandoned")
			}
			if time.Since(start) >= time.Second || len(tr.RawConnections()) != 0 {
				t.Fatal("retirement waited for natural expiry or retained old raw responsibility")
			}
			if err := client.SetDeadline(time.Now().Add(upgradebudget.LiveFlowStall)); err != nil {
				t.Fatal(err)
			}
			if err := target.SetDeadline(time.Now().Add(upgradebudget.LiveFlowStall)); err != nil {
				t.Fatal(err)
			}
			if _, err := target.Write([]byte("still-original")); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len("still-original"))
			if _, err := io.ReadFull(client, got); err != nil || string(got) != "still-original" || socketInode(t, client) != inode {
				t.Fatal("planned NC cancellation killed or replaced the original business owner")
			}
		})
	}
}

type blockedDialer struct{ raw net.Conn }

func (blockedDialer) Dial(string, string) (net.Conn, error) {
	return nil, errors.New("unplanned dial is forbidden")
}
func (d blockedDialer) DialWithin(string, string, i.Deadline, i.Stop) (net.Conn, error) {
	return d.raw, nil
}

func routeAttachment(n byte) i.Attachment {
	return i.Attachment{Participant: i.ID{1}, Generation: uint64(n), ServerID: "server", CID: uint64(n), URL: "wss://localhost:443",
		Raw:     i.Socket{NetNS: 1, Device: 1, Inode: uint64(n), Cookie: uint64(n), Local: "127.0.0.1:50000", Remote: "127.0.0.1:443", Owner: oracleProcess(9)},
		Ingress: oracleProcess(n), HealthGeneration: uint64(n)}
}

// This lane runs the real P00 codec and P16 signatures/hold. Its clock is
// explicit to exercise eight/sixty-second boundaries without a long sleep;
// actual roster and home-writer effects are required separately by live.
func exerciseSignedHold(t *testing.T) {
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	domain, tx, nonce := i.ID{1}, i.ID{2}, i.ID{3}
	const t0 = int64(time.Second)
	old, next := routeAttachment(1), routeAttachment(2)
	next.URL, next.Raw.Remote = "wss://localhost:8443", "127.0.0.1:8443"
	target := i.RouteTarget{URL: next.URL, NodeID: "node", Component: i.Caddy, Ingress: next.Ingress}
	proof := i.RouteProof{Version: 1, Purpose: i.RoutePrepare, Direction: i.RouteOutbound, Target: target,
		Tx: tx, Participant: old.Participant, Old: old, HomeDigest: i.Digest{7}, AuthorityRevision: 1, PolicyRevision: 1,
		RemainingNS: uint64(upgradebudget.BrokerMaintenanceTTL), Nonce: nonce, KeyID: "route"}
	sign := func(p i.RouteProof) i.RouteProof {
		t.Helper()
		p, err := inframaint.SignProof(p, domain, func(b []byte) ([]byte, error) { return ed25519.Sign(key, b), nil })
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	proof = sign(proof)
	view := inframaint.View{Attachment: old, HomeDigest: proof.HomeDigest, AuthorityRevision: 1, PolicyRevision: 1, Known: true, Healthy: true, Registered: true}
	hold, err := inframaint.NewHold(inframaint.Challenge{Domain: domain, Tx: tx, Participant: old.Participant, Direction: i.RouteOutbound,
		Nonce: nonce, BootID: "observer", StartedNS: t0})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hold.Install(proof, "route", pub, view, "observer", t0, t0+int64(time.Second)); err != nil {
		t.Fatal(err)
	}
	pending := inframaint.Pending{Participant: old.Participant, Generation: next.Generation, Target: target}
	if err := hold.BeginDial(pending, view, "observer", t0+int64(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	bind := proof
	bind.Purpose, bind.New, bind.RemainingNS = i.RouteBind, next, uint64(3*time.Second)
	bind = sign(bind)
	view.Attachment = next
	if _, err := hold.Install(bind, "route", pub, view, "observer", t0+int64(5*time.Second), t0+int64(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	stale := view
	stale.Attachment = old
	if hold.Protects(stale, "observer", t0+int64(6*time.Second)) || !hold.Protects(view, "observer", t0+int64(6*time.Second)) {
		t.Fatal("Bind did not replace the exact old physical generation")
	}
	bind.RemainingNS = uint64(upgradebudget.BrokerMaintenanceTTL)
	bind = sign(bind)
	if _, err := hold.Install(bind, "route", pub, view, "observer", t0+int64(7*time.Second), t0+int64(7*time.Second)); err != nil {
		t.Fatal(err)
	}
	if hold.Protects(view, "observer", t0+int64(upgradebudget.BrokerMaintenanceTTL)) || hold.Protects(view, "observer", t0+int64(60*time.Second)) {
		t.Fatal("late reply renewed protection over the ordinary reaper")
	}
}

// origin: hot-update S9 plan §4.6 / T69 / M18 / M25
func checkRoutes(rows []routeReceipt) error {
	expected := map[string]bool{"before-move": false, "after-bind": false, "after-8s": false,
		"after-60s": false, "home-cas": false, "unknown-source": false, "late-worker": false, "revoked": false}
	for _, row := range rows {
		if _, ok := expected[row.Case]; !ok || expected[row.Case] || !validDigest(row.ReaperEvidence) || !validDigest(row.CASEvidence) ||
			row.FirstInstallNS <= 0 || row.ExpiryNS-row.FirstInstallNS > int64(upgradebudget.BrokerMaintenanceTTL) ||
			row.LastReplyNS < row.FirstInstallNS || row.ObservedNS < row.LastReplyNS || row.HomeBefore == "" || row.HomeAfter == "" {
			return errors.New("route/reaper evidence or original expiry is incomplete")
		}
		if row.Case == "before-move" || row.Case == "after-bind" {
			if !row.Protected || row.Reaped || row.ObservedNS >= row.ExpiryNS || row.HomeBefore != row.HomeAfter {
				return errors.New("healthy planned route caused a home reassignment")
			}
			if row.Case == "after-bind" && (!completeAttachment(row.Old) || !completeAttachment(row.New) || sameConnection(row.Old, row.New) || row.Old.Raw.Cookie == row.New.Raw.Cookie ||
				row.New.Generation <= row.Old.Generation || row.New.Participant != row.Old.Participant || row.New.Ingress != row.Target ||
				row.New.Generation != row.PendingGeneration || row.New.HealthGeneration <= row.Old.HealthGeneration) {
				return errors.New("forwarded ACK or old attachment signed the new route")
			}
		} else if row.Protected || !row.Reaped {
			return errors.New("expired/revoked/unknown route masked normal reaper or home CAS")
		}
		if row.Case == "after-8s" && row.ObservedNS < row.ExpiryNS || row.Case == "after-60s" && row.ObservedNS-row.FirstInstallNS < int64(60*time.Second) ||
			row.Case == "home-cas" && row.HomeBefore == row.HomeAfter || row.Case == "unknown-source" && !row.UnknownSource || row.Case == "revoked" && !row.Revoked {
			return errors.New("route negative window or authority change was not exercised")
		}
		expected[row.Case] = true
	}
	for _, present := range expected {
		if !present {
			return errors.New("route/reaper case is missing")
		}
	}
	return nil
}

func routeControls() []routeReceipt {
	var rows []routeReceipt
	for _, name := range []string{"before-move", "after-bind", "after-8s", "after-60s", "home-cas", "unknown-source", "late-worker", "revoked"} {
		r := routeReceipt{Case: name, Target: routeAttachment(2).Ingress, HomeBefore: "home", HomeAfter: "home", Old: routeAttachment(1), New: routeAttachment(2),
			FirstInstallNS: int64(time.Second), LastReplyNS: int64(2 * time.Second), ExpiryNS: int64(9 * time.Second), ObservedNS: int64(3 * time.Second),
			ReaperEvidence: digest([]byte("reaper")), CASEvidence: digest([]byte("cas")), PendingGeneration: 2}
		r.Protected = name == "before-move" || name == "after-bind"
		r.Reaped = !r.Protected
		if name == "after-8s" {
			r.ObservedNS = r.ExpiryNS
		}
		if name == "after-60s" {
			r.ObservedNS = int64(61 * time.Second)
		}
		if name == "home-cas" {
			r.HomeAfter = "other"
		}
		r.UnknownSource, r.Revoked = name == "unknown-source", name == "revoked"
		rows = append(rows, r)
	}
	return rows
}
