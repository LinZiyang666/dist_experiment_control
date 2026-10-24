package authcallout

import (
	"encoding/hex"
	"slices"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// origin: hot-update S9 plan §7.1 / T47 / WIRE-2 R14
func TestInfraManagerRequiresAccountGrantAndProvenActor(t *testing.T) {
	h, _ := freshHandler(t)
	h.Now = func() time.Time { return time.Unix(1700000000, 0).UTC() }
	actor, other := freshUserPub(t), freshUserPub(t)
	for _, scenario := range []string{"authorized", "unsigned", "foreign-account", "other-actor", "other-target", "expired", "future", "ordinary-name", "bad-connect-signature", "fenced", "noncanonical", "uninstalled"} {
		t.Run(scenario, func(t *testing.T) {
			r := proto.InfraUpgradeReq{Request: proto.InfraCommand{Version: 1, Purpose: proto.InfraRequestPurpose, Op: "probe", RemainingNS: uint64(time.Second)},
				Actor: actor, TargetNode: "b1", IssuedAt: h.Now().Format(time.RFC3339Nano), Nonce: "01000000000000000000000000000000"}
			name, client := "tether-infra-manager:b1", actor
			signer := h.AccountKp
			switch scenario {
			case "foreign-account":
				signer, _ = nkeys.CreateAccount()
				defer signer.Wipe()
			case "other-actor":
				client = other
			case "other-target":
				name = "tether-infra-manager:b2"
			case "expired":
				r.IssuedAt = h.Now().Add(-3 * time.Minute).Format(time.RFC3339Nano)
			case "future":
				r.IssuedAt = h.Now().Add(time.Second).Format(time.RFC3339Nano)
			case "ordinary-name":
				name = "tether-cli"
			case "fenced":
				h.LeaderContactStale = func(time.Time) bool { return true }
				defer func() { h.LeaderContactStale = nil }()
			}
			body, err := proto.InfraUpgradeSigningBytes(r)
			if err != nil {
				t.Fatal(err)
			}
			sig, err := signer.Sign(body)
			if err != nil {
				t.Fatal(err)
			}
			r.Sig = hex.EncodeToString(sig)
			if scenario == "unsigned" {
				r.Sig = ""
			}
			token, err := proto.EncodeInfraUpgradeRequest(r)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "noncanonical" {
				token = append(token, ' ')
			}
			request := signedRequest(t, freshUserPub(t), client, name, string(token))
			if scenario == "bad-connect-signature" {
				claims, _ := jwt.DecodeAuthorizationRequestClaims(request)
				claims.ConnectOptions.SignedNonce = "invalid"
				server, _ := nkeys.CreateServer()
				defer server.Wipe()
				request, err = claims.Encode(server)
				if err != nil {
					t.Fatal(err)
				}
			}
			answer, err := WithInfraManager(h, func(target string) bool { return target == "b1" && scenario != "uninstalled" })(request)
			if err != nil {
				t.Fatal(err)
			}
			response, err := jwt.DecodeAuthorizationResponseClaims(answer)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "authorized" {
				if response.Error != "" {
					t.Fatal("account-authorized manager refused:", response.Error)
				}
				claims, err := jwt.DecodeUserClaims(response.Jwt)
				if err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(claims.Pub.Allow, []string{proto.SubjClusterInfra("b1")}) ||
					!slices.Equal(claims.Sub.Allow, []string{auth.InboxPrefixFor(actor) + ".>"}) {
					t.Fatalf("manager grant widened: pub=%v sub=%v", claims.Pub.Allow, claims.Sub.Allow)
				}
			} else if scenario == "ordinary-name" {
				if response.Error != "" {
					t.Fatal(response.Error)
				}
				claims, _ := jwt.DecodeUserClaims(response.Jwt)
				if slices.Contains(claims.Pub.Allow, proto.SubjClusterInfra("b1")) {
					t.Fatal("ordinary name acquired manager grant")
				}
			} else if response.Error == "" {
				t.Fatalf("%s acquired infrastructure authority", scenario)
			}
		})
	}
}
