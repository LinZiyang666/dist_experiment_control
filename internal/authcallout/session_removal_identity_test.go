package authcallout

import (
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"testing"
	"time"
)

// Copy into internal/authcallout. Uses the actual signed callout and SQL path;
// the CONNECT name is exactly the one in cmd/tether/session.go's rm command.
// origin: S7 adversarial review rv-n1compat
// origin: S7 internal review rv-n1compat F4
func TestSessionRemovalCommandIdentityAfterDeleting(t *testing.T) {
	h, _ := freshHandler(t)
	h.Now = time.Now
	client, ephemeral := freshUserPub(t), freshUserPub(t)
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
	check := func(name, token string) string {
		wire, err := h.Handle(signedRequest(t, ephemeral, client, name, token))
		if err != nil {
			t.Fatal(err)
		}
		response, err := jwt.DecodeAuthorizationResponseClaims(wire)
		if err != nil {
			t.Fatal(err)
		}
		return response.Error
	}
	if err := check("session-removal:lab", ""); err != "" {
		t.Fatalf("dedicated removal fixture refused: %s", err)
	}
	options := nats.GetDefaultOptions()
	for _, option := range cli.SessionRemovalOptions("lab") {
		if err := option(&options); err != nil {
			t.Fatal(err)
		}
	}
	actualCommandName := options.Name
	if err := check(actualCommandName, options.Token); err != "" {
		t.Fatalf("session rm --abandon-unsettled-transfers cannot reconnect after first rc75: CONNECT name=%q, refusal=%q", actualCommandName, err)
	}
}
