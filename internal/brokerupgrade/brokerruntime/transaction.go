package brokerruntime

import (
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokerupgradeport"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats.go"
)

// trackerCensusBudget bounds the transfer tracker's serial token for one
// census read; a tracker busy longer than this is an unknown census.
const trackerCensusBudget = 2 * time.Second

// TransactionFacts samples the owners an S8 broker cut must name, from the
// current S6 control generation: the store owner's database identity, every
// kept listener, the tunnel server's installed listeners and flow endpoints,
// the capable transfer tracker's entries and the legacy/durable transfer
// residue the S6 cut refuses. Every owner is read through its own API; one it
// cannot enumerate is an error, never an empty list. sessions is the running
// tunnel server's installed allocations (nil: no tunnel server); the Broker
// projects them, so this leaf never imports the tunnel data plane.
// origin: hot-update S8 plan §4.2 / M06 / M14 / §11.2 P15 P17; S8 BROKER-EXPORTER R-11
func TransactionFacts(store brokerimage.StoreOwner, keepers []listenerimage.Keeper, sessions func() []brokerupgradeport.TunnelSession,
	transfers brokerimage.TransferOwner, unsettled func() (int, error), status *proto.BrokerImageStatus) (brokerupgradeport.BrokerFacts, error) {
	if store == nil || unsettled == nil {
		return brokerupgradeport.BrokerFacts{}, errors.New("broker: current image owners are unavailable")
	}
	stamp, err := store.Stamp()
	if err != nil {
		return brokerupgradeport.BrokerFacts{}, err
	}
	facts := brokerupgradeport.BrokerFacts{Tunnel: sessions != nil,
		Store:   brokerupgradeport.StoreIdentity{Path: stamp.Identity.Path, Device: stamp.Identity.Device, Inode: stamp.Identity.Inode},
		Serving: status != nil && (status.Phase == proto.BrokerImagePhaseServing || status.Phase == proto.BrokerImagePhaseControlReady)}
	for _, keeper := range keepers {
		facts.Listeners = append(facts.Listeners, keeper.Descriptor().ID)
	}
	sort.Strings(facts.Listeners)
	if sessions != nil {
		facts.Sessions = sessions()
		sort.Slice(facts.Sessions, func(i, j int) bool { return facts.Sessions[i].Port < facts.Sessions[j].Port })
	}
	if transfers != nil {
		census, ok := transfers.(interface {
			Census(controlbudget.Deadline) ([]xfertrack.CensusEntry, error)
		})
		if !ok {
			return brokerupgradeport.BrokerFacts{}, errors.New("broker: transfer tracker census is unavailable")
		}
		deadline, err := controlbudget.After(trackerCensusBudget)
		if err != nil {
			return brokerupgradeport.BrokerFacts{}, err
		}
		entries, err := census.Census(deadline)
		if err != nil {
			return brokerupgradeport.BrokerFacts{}, err
		}
		facts.Transfers = true
		for _, e := range entries {
			facts.Trackers = append(facts.Trackers, brokerupgradeport.TrackerEntry{SID: e.SID, TransferID: e.TransferID, EntryNonce: e.EntryNonce, Settled: e.Settled})
		}
	}
	if facts.Unsettled, err = unsettled(); err != nil {
		return brokerupgradeport.BrokerFacts{}, err
	}
	return facts, nil
}

// ControlProbe is the broker target's real new request (§4.2): one request
// crosses the broker's own business subscriptions on its current connection
// and is answered by the handler from the current store. A single broker asks
// for the session list of a fresh, unowned actor (no session, no effect); a
// clustered broker uses the broker-only cluster cursor probe, which its
// rendered permissions allow, and must be answered by itself. A cached reply
// cannot answer: each probe has its own request inbox and actor.
// origin: hot-update S8 plan §4.2 / M05 / §11.2 P15; S8 BROKER-EXPORTER R-11
func ControlProbe(nc *nats.Conn, nonce, selfID string) error {
	if nc == nil || len(nonce) != 32 {
		return errors.New("broker: control probe has no current connection or nonce")
	}
	body, _ := json.Marshal(map[string]string{"query_nonce": nonce, "target_node": selfID})
	if selfID != "" {
		reply, err := nc.Request(proto.SubjClusterCursor, body, upgradebudget.RequestLatency)
		if err != nil {
			return err
		}
		var health proto.ClusterHealthResp
		if json.Unmarshal(reply.Data, &health) != nil || health.NodeID != selfID {
			return errors.New("broker: control probe was not answered by this broker")
		}
		return nil
	}
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		return err
	}
	actor, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		return err
	}
	reply, err := nc.Request(proto.SubjCtrlSessionList(actor), body, upgradebudget.RequestLatency)
	if err != nil {
		return err
	}
	var list proto.SessionListResp
	if json.Unmarshal(reply.Data, &list) != nil || list.Error != "" || len(list.Sessions) != 0 {
		return errors.New("broker: control probe did not traverse the session store")
	}
	return nil
}
