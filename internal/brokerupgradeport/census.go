// census.go names what a running broker holds, by identity, for the S8
// census: the store it owns, every kept listener and the tunnel server, each
// installed public listener or flow endpoint, every capable transfer tracker
// entry and every session's creation authority. A broker that cannot name one
// of them refuses (domain_census_unavailable) instead of describing a partial
// scope, and legacy transfers or unresolved durable transfer rows — which the
// S6 store cut itself refuses to carry (transfers_in_flight) — defer the same
// way. Facts are gathered by the Broker (BrokerPort.Facts); this leaf only maps
// them to rows, so neither the Broker nor this package grows a second census.
// origin: hot-update S8 plan §4.2 / M06 / M14 / §11.2 P15 P17; S8 BROKER-EXPORTER R-11
package brokerupgradeport

import (
	"database/sql"
	"sort"
	"strconv"

	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Public refusal reasons of the broker runtime. They travel as the wire
// Detail of upgrade_deferred (75) or upgrade_rejected: the request was never
// accepted.
const (
	ReasonBrokerCutUnavailable  = "broker_cut_unavailable"
	ReasonDomainCensus          = "domain_census_unavailable"
	ReasonTransfersInFlight     = "transfers_in_flight"
	ReasonIncarnationUnknown    = "authority_incarnation_unknown"
	ReasonRunningImageChanged   = "running_image_changed"
	ReasonArtifactMismatch      = "preflight_artifact_mismatch"
	ReasonArtifactUnavailable   = "artifact_unavailable"
	ReasonImageABIUnsupported   = "image_abi_unsupported"
	ReasonUpgradeInProgress     = "upgrade_in_progress"
	ReasonDomainLeaseHeld       = "domain_reservation_held"
	ReasonActivationUnavailable = "activation_unavailable"
)

// StoreIdentity is the current store owner's database file.
type StoreIdentity struct {
	Path          string
	Device, Inode uint64
}

// TunnelSession is one installed public listener or flow endpoint.
type TunnelSession struct {
	Port     int
	SID, NID string
	Epoch    int64
}

// TrackerEntry is one capable transfer the broker tracks, as the tracker's
// own registry names it: its business key and the EntryNonce of its slot.
type TrackerEntry struct {
	SID, TransferID, EntryNonce string
	Settled                     bool
}

// BrokerFacts is one read-only sample of the running broker. Unsettled counts
// legacy transfers and unresolved durable transfer rows: the S6 store cut
// refuses them, so no S8 cut can carry them either.
type BrokerFacts struct {
	Store     StoreIdentity
	Listeners []string
	Tunnel    bool
	Sessions  []TunnelSession
	// Transfers reports a capable transfer owner; Trackers are its entries.
	Transfers bool
	Trackers  []TrackerEntry
	Unsettled int
	// Authorities is the complete session authority map (SID → incarnation),
	// derived from each session's creation identity in the current store.
	Authorities []upgradecut.Authority
	// Serving is the S6 image phase: this process serves its store and graph.
	Serving bool
}

// SessionAuthorities derives every session's incarnation from the current
// store (upgradeauthority). It is the authority map an S8 cut carries and the
// census's authority rows. A session whose creation identity is unreadable
// cannot be carried: the whole map refuses rather than omitting it.
// origin: hot-update S8 plan §6.1 / §6.2 / §11.2 P17
func SessionAuthorities(db *sql.DB) ([]upgradecut.Authority, error) {
	if db == nil {
		return nil, refusal(upgradetx.CodeDeferred, ReasonIncarnationUnknown)
	}
	rows, err := db.Query(`SELECT sid FROM sessions ORDER BY sid`)
	if err != nil {
		return nil, refusal(upgradetx.CodeDeferred, ReasonIncarnationUnknown)
	}
	var sids []string
	for rows.Next() {
		var sid string
		if err := rows.Scan(&sid); err != nil {
			_ = rows.Close()
			return nil, refusal(upgradetx.CodeDeferred, ReasonIncarnationUnknown)
		}
		sids = append(sids, sid)
	}
	if err := rows.Close(); err != nil || rows.Err() != nil {
		return nil, refusal(upgradetx.CodeDeferred, ReasonIncarnationUnknown)
	}
	out := make([]upgradecut.Authority, 0, len(sids))
	for _, sid := range sids {
		creation, err := upgradeauthority.ReadCreation(db, upgradeauthority.Guard{SID: sid})
		var incarnation string
		if err == nil {
			incarnation, err = upgradeauthority.CreationIncarnation(creation)
		}
		if err != nil {
			return nil, refusal(upgradetx.CodeDeferred, ReasonIncarnationUnknown)
		}
		out = append(out, upgradecut.Authority{SID: sid, Incarnation: incarnation})
	}
	return out, nil
}

// censusRows maps one facts sample to sorted census rows. IDs are the original
// object identities and Origin binds each to the creation it belongs to, so a
// recreated session, a re-registered tunnel or a replacement tracker slot with
// the same name can never stand in for the original across the cut.
func censusRows(f BrokerFacts, owner upgradetx.Owner, generation uint64, custody string) ([]upgradetx.Resource, error) {
	if f.Unsettled != 0 {
		return nil, refusal(upgradetx.CodeDeferred, ReasonTransfersInFlight)
	}
	if f.Store.Path == "" || f.Store.Inode == 0 {
		return nil, refusal(upgradetx.CodeDeferred, ReasonDomainCensus)
	}
	var rows []upgradetx.Resource
	add := func(domain upgradetx.Domain, id, origin, state, rowCustody string) {
		rows = append(rows, upgradetx.Resource{Domain: domain, ID: id, Origin: origin, Owner: owner, Generation: generation, State: state, Custody: rowCustody})
	}
	add(upgradetx.DomainBrokerStore, "store:"+f.Store.Path, "inode:"+strconv.FormatUint(f.Store.Device, 10)+":"+strconv.FormatUint(f.Store.Inode, 10), "active", custody)
	for _, id := range f.Listeners {
		add(upgradetx.DomainBrokerRuntime, "listener:"+id, "listener:"+id, "active", custody)
	}
	if f.Tunnel {
		add(upgradetx.DomainBrokerRuntime, "tunnel:server", "tunnel:server", "active", custody)
	}
	seen := make(map[string]bool, len(f.Sessions))
	for _, s := range f.Sessions {
		origin := "allocation:" + s.SID + "/" + s.NID + "@" + strconv.FormatInt(s.Epoch, 10)
		key := strconv.Itoa(s.Port) + "\x00" + origin
		if seen[key] {
			continue // a listener and its flow endpoint name one allocation
		}
		seen[key] = true
		add(upgradetx.DomainBrokerRuntime, "tunnel:"+strconv.Itoa(s.Port), origin, "active", custody)
	}
	for _, e := range f.Trackers {
		if e.SID == "" || e.TransferID == "" || e.EntryNonce == "" {
			return nil, refusal(upgradetx.CodeDeferred, ReasonDomainCensus)
		}
		state, rowCustody := "running", custody
		if e.Settled {
			state, rowCustody = "terminal", "retained"
		}
		add(upgradetx.DomainTransferTracker, "tracker:"+e.SID+"/"+e.TransferID, "entry:"+e.EntryNonce, state, rowCustody)
	}
	for _, a := range f.Authorities {
		add(upgradetx.DomainAuthority, "session:"+a.SID, "incarnation:"+a.Incarnation, "active", custody)
	}
	sort.Slice(rows, func(i, j int) bool {
		return string(rows[i].Domain)+"\x00"+rows[i].ID < string(rows[j].Domain)+"\x00"+rows[j].ID
	})
	for i := range rows {
		if upgradetx.ValidateResource(rows[i]) != nil || i > 0 && rows[i-1].Domain == rows[i].Domain && rows[i-1].ID == rows[i].ID {
			return nil, refusal(upgradetx.CodeDeferred, ReasonDomainCensus)
		}
	}
	return rows, nil
}
