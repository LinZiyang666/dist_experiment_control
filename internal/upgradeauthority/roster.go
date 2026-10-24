// Roster authority proof encodings are pure; the caller retains its final lock.
// origin: hot-update S8 plan §6.2 / §12.1 / P14.
package upgradeauthority

import (
	"encoding/base64"
	"errors"
	"net/url"
	"time"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/proto"
)

// One codec owns every signed byte, including the nonce-only challenge leg.
// Keeping this projection here avoids a brokermaint -> proto import cycle.
// origin: hot-update S6 internal review rv-cluster-4
func MaintenanceSignedBytes(value any) ([]byte, error) {
	switch v := value.(type) {
	case proto.BrokerMaintenanceReq:
		return brokermaint.CanonicalRequestBytes(v.Record, v.Proof, v.NID, v.ConnectedBrokerID, v.IngressServerID, v.RouteDigest)
	case proto.BrokerMaintenanceAck:
		return brokermaint.CanonicalAckBytes(v.CutID, v.Nonce, v.NID, v.ConnectedBrokerID, v.IngressServerID, v.InstanceID, v.RouteDigest, v.ProofDigest, v.Accepted, v.RemainingNS)
	case brokermaint.Proof:
		return brokermaint.CanonicalProofBytes(v)
	default:
		return nil, errors.New("maintenance: invalid signed value")
	}
}

// The caller supplies signature verification so guarded auth writers can import
// this pure leaf without a reverse dependency through auth or clusterroster.
// origin: hot-update S8 plan §6.2 / §12.1 P14.
func VerifyMaintenance(pub, signature string, value any, verify func(string, []byte, []byte) error) error {
	body, err := MaintenanceSignedBytes(value)
	if err != nil {
		return err
	}
	sig, err := base64.RawStdEncoding.DecodeString(signature)
	if err != nil {
		return errors.New("maintenance: invalid signature")
	}
	return verify(pub, body, sig)
}

// RosterConsumer keeps signed-roster verification in its established owner.
// Only the identity selection below belongs to the creation-authority leaf.
type RosterConsumer struct {
	Current func(*proto.ClusterRoster, time.Time) bool
	Unique  func(*proto.ClusterRoster, string) string
	Aliases func([]string, proto.RosterBroker) []string
}

// A hostname is ingress authority only when exactly one current signed node
// owns it. N-1 replies retain the previously proven broker identity.
// origin: hot-update S8 plan §6.2 / §12.1 / P14.
func RosterAuthority(consumer RosterConsumer, roster *proto.ClusterRoster, now time.Time, connectedURL, id string, aliases []string) (string, []string) {
	cu, _ := url.Parse(connectedURL)
	if r := roster; cu != nil && consumer.Current(r, now) {
		if id == "" {
			id = consumer.Unique(r, cu.Hostname())
		}
		for _, b := range r.Brokers {
			if id != "" && b.NodeID == id {
				aliases = consumer.Aliases(aliases, b)
			}
		}
	}
	return id, aliases
}
