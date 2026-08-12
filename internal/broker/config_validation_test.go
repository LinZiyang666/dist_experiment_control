// config_validation_test.go — differential pins for broker.ValidateConfig's
// NATS-pool and listen-host parsers: the preflight must reach the SAME verdict
// as nats.go's pure server-pool setup, before either path attempts a network
// dial. (Named after the unit under test, not the review round that added it.)
// origin: docs/reviews/g75-g78-deploy-defaults-external-rereview.md R1 (mixed
// transport pool) + R2 (IPv6 %zone vs DNS host).
package broker

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestValidateNATSURLRejectsMixedTransportLikeNATSClient(t *testing.T) {
	raw := "nats://127.0.0.1:4222,ws://127.0.0.1:8080"
	if _, err := nats.Connect(raw); !errors.Is(err, nats.ErrMixingWebsocketSchemes) {
		t.Fatalf("precondition: nats.go must reject the mixed pool before dialing, got %v", err)
	}
	if err := validateNATSURL(raw); err == nil {
		t.Fatal("config preflight accepted a server pool nats.go rejects before dialing")
	}
}

func TestValidateNATSURLAcceptsHomogeneousTransportPools(t *testing.T) {
	for _, raw := range []string{
		"nats://127.0.0.1:4222,tls://nats.example.com:4222",
		"ws://127.0.0.1:8080,wss://nats.example.com:443/path",
	} {
		if err := validateNATSURL(raw); err != nil {
			t.Errorf("validateNATSURL(%q) rejected a homogeneous nats.go pool: %v", raw, err)
		}
	}
}

func TestValidListenHostZonesAndAbsoluteFQDN(t *testing.T) {
	for _, host := range []string{"fe80::1%eth0", "example.com."} {
		if !validListenHost(host) {
			t.Errorf("validListenHost(%q) = false, want true", host)
		}
	}
	for _, host := range []string{"example.com%eth0", "fe80::1%", "."} {
		if validListenHost(host) {
			t.Errorf("validListenHost(%q) = true, want false", host)
		}
	}
}
