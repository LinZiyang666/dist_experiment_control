package authcallout

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferauth"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/jwt/v2"
)

type settlementCensus struct {
	row         xfercontract.LedgerRow
	unavailable bool
}

func (c *settlementCensus) Census(o xfercontract.Origin, _ controlbudget.Deadline) (xfercontract.LedgerCensus, error) {
	if c.unavailable {
		return xfercontract.LedgerCensus{}, errors.New("unreachable")
	}
	return xfercontract.LedgerCensus{Origin: o, Ready: true, Complete: true, Rows: []xfercontract.LedgerRow{c.row}}, nil
}

// origin: hot-update S7 plan §6.4 / T31
func TestTransferLifecycleNoLeaks(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseTransferLifecycle(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer settlement callout", g)
	testharness.AssertNoFDLeak(t, "transfer settlement callout", fd, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	h, _ := freshHandler(t)
	h.Now = time.Now
	client := freshUserPub(t)
	ephemeral := freshUserPub(t)
	fp, err := auth.FingerprintFromActor(client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.DB.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash,state) VALUES('lab','lab',?,'unused','DELETING')`, fp); err != nil {
		t.Fatal(err)
	}
	if _, err := h.DB.Exec(`INSERT INTO members(sid,pubkey_fp,role,via) VALUES('lab',?,'owner','create')`, fp); err != nil {
		t.Fatal(err)
	}
	c, err := xferauth.SignCapsule(h.AccountKp, xfercontract.SettlementCapsule{Identity: xfercontract.Identity{SID: "lab", NID: "node", ActorFP: fp, AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", IntentDigest: "intent"}, Origin: xfercontract.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 1}, ReceiverNkey: client, Nonce: "capsule"})
	if err != nil {
		t.Fatal(err)
	}
	owners := &settlementCensus{row: xfercontract.LedgerRow{Identity: c.Identity, Origin: c.Origin, EntryNonce: c.Identity.EntryNonce, Revision: 1, Phase: xfercontract.PhaseCommitReserved, Settlement: &c}}
	issuers, err := xferauth.NewIssuerVerifier([]string{c.Issuer})
	if err != nil {
		t.Fatal(err)
	}
	h.Settlement, err = xferauth.New(xferauth.Config{Issuers: issuers, Owners: owners})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	request := func(client, name, token string) *jwt.AuthorizationResponseClaims {
		t.Helper()
		wire, err := h.Handle(signedRequest(t, ephemeral, client, name, token))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := jwt.DecodeAuthorizationResponseClaims(wire)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	if resp := request(client, "tether-cli:lab", ""); resp.Error == "" {
		t.Fatal("ordinary activated CONNECT survived DELETING")
	}
	removal := request(client, "session-removal:lab", "")
	if removal.Error != "" {
		t.Fatalf("owner cannot retry rm after DELETING: %s", removal.Error)
	}
	removalClaims, err := jwt.DecodeUserClaims(removal.Jwt)
	if err != nil {
		t.Fatal(err)
	}
	removalActual, _ := json.Marshal(removalClaims.Permissions)
	removalWant, _ := json.Marshal(auth.PermissionsForSessionRemoval(client, "lab"))
	if string(removalActual) != string(removalWant) {
		t.Fatal("removal role restored ordinary business grants")
	}
	if request(freshUserPub(t), "session-removal:lab", "").Error == "" {
		t.Fatal("non-owner authorized removal reconnect")
	}
	if request(client, "session-removal:missing", "").Error == "" {
		t.Fatal("missing session authorized removal reconnect")
	}
	resp := request(client, "transfer-settlement:lab:node", string(payload))
	if resp.Error != "" {
		t.Fatalf("retained receiver cannot reconnect: %s", resp.Error)
	}
	// Standard base64 is also a valid NKey proof encoding in the normal
	// CONNECT path; narrowing it only during settlement strands old clients.
	proofReq, err := jwt.DecodeAuthorizationRequestClaims(signedRequest(t, ephemeral, client, "transfer-settlement:lab:node", string(payload)))
	if err != nil {
		t.Fatal(err)
	}
	proofBytes, err := base64.RawURLEncoding.DecodeString(proofReq.ConnectOptions.SignedNonce)
	if err != nil {
		t.Fatal(err)
	}
	proofReq.ConnectOptions.SignedNonce = base64.StdEncoding.EncodeToString(proofBytes)
	if _, err := xferauth.VerifyConnect(h.DB, h.Settlement, proofReq, "lab", "node"); err != nil {
		t.Fatal("standard NKey proof rejected during settlement:", err)
	}
	uc, err := jwt.DecodeUserClaims(resp.Jwt)
	if err != nil {
		t.Fatal(err)
	}
	want := auth.PermissionsForTransferSettlement(client, "lab", "node", "transfer")
	actual, _ := json.Marshal(uc.Permissions)
	expected, _ := json.Marshal(want)
	if string(actual) != string(expected) {
		t.Fatalf("settlement permissions differ: %s", actual)
	}
	for _, tc := range []struct{ name, client, role, token string }{
		{"stolen", freshUserPub(t), "transfer-settlement:lab:node", string(payload)},
		{"wrong_sid", client, "transfer-settlement:other:node", string(payload)},
		{"wrong_node", client, "transfer-settlement:lab:other", string(payload)},
		{"unknown_field", client, "transfer-settlement:lab:node", string(payload[:len(payload)-1]) + `,"unexpected":true}`},
		{"trailing", client, "transfer-settlement:lab:node", string(payload) + `{}`},
		{"wildcard", client, "transfer-settlement:lab:*", string(payload)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if request(tc.client, tc.role, tc.token).Error == "" {
				t.Fatal("invalid settlement CONNECT accepted")
			}
		})
	}
	owners.unavailable = true
	if request(client, "transfer-settlement:lab:node", string(payload)).Error == "" {
		t.Fatal("unavailable origin authorized reconnect")
	}
	owners.unavailable = false
	owners.row.Phase = xfercontract.PhaseSettled
	if request(client, "transfer-settlement:lab:node", string(payload)).Error == "" {
		t.Fatal("retired entry authorized reconnect")
	}
	owners.row.Phase = xfercontract.PhaseCommitReserved
	h.Settlement = nil
	if request(client, "transfer-settlement:lab:node", string(payload)).Error == "" {
		t.Fatal("unwired verifier authorized reconnect")
	}
}
