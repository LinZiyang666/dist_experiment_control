package cluster

import (
	"time"

	"github.com/LinZiyang666/tether/internal/cluster/state"
)

// The codec is below Raft; the facade preserves JoinBundle and its method.
// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM
type JoinBundle state.JoinBundle

func EncodeJoinBundle(b JoinBundle) (string, error) {
	return state.EncodeJoinBundle(state.JoinBundle(b))
}
func DecodeJoinBundle(s string) (*JoinBundle, error) {
	b, err := state.DecodeJoinBundle(s)
	return (*JoinBundle)(b), err
}
func VerifyBundlePoP(b *JoinBundle) error { return state.VerifyBundlePoP((*state.JoinBundle)(b)) }

// ToUpsertInput projects a verified bundle into the roster-admission input.
func (b *JoinBundle) ToUpsertInput(now time.Time) ClusterNodeUpsertInput {
	name := b.Name
	if name == "" {
		name = b.NodeID
	}
	natsSrv := b.NatsServerID
	if natsSrv == "" {
		natsSrv = b.NodeID // §6.5 SSOT: server_name == node_id; default so the node stays home-eligible (audit A)
	}
	return ClusterNodeUpsertInput{
		NodeID: b.NodeID, Name: name, NodeIdentPub: b.NodeIdentPub, NatsServerID: natsSrv,
		RaftAddr: b.RaftAddr, NatsRoute: b.NatsRoute, TunnelAddr: b.TunnelAddr, PublicHost: b.PublicHost,
		CertFP: b.CertFP, BusNkey: b.BusNkey, JoinNonce: b.JoinNonce, JoinSigHex: b.JoinSigHex, Now: now,
	}
}
