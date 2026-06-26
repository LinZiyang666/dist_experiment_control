package broker

import (
	"encoding/json"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// cluster_manifest.go (C2) — the body served at the well-known HTTP endpoint: the account-signed
// roster (reused VERBATIM from cluster_roster.go) + an account-signed SeedBundle (client-dialable
// endpoints). Both are DISCOVERY-ONLY (zero secrets). Served from a time-bucketed cache (§D-4): the
// GET hot path is a pure atomic load (no DB, no signing); a re-sign happens only when a generation
// advanced or the signature aged past rosterTTL/4 (6h) — and re-sign ATTEMPTS are rate-limited to
// ≥30s, bounding sign-rate under an unauthenticated flood.

const (
	manifestRecheckInterval = 30 * time.Second // min interval between DB generation re-checks / sign attempts
	manifestResignAfter     = rosterTTL / 4    // 6h: re-sign to keep IssuedAt/ExpiresAt fresh on a stable cluster
)

// manifestSnapshot is one cached, pre-signed manifest. rosterGen/seedGen are the RAW cluster_meta
// counters (NOT the floored wire generation) so change-detection compares like-for-like.
type manifestSnapshot struct {
	bytes       []byte
	rosterGen   uint64
	seedGen     uint64
	signedAt    time.Time
	nextCheckAt time.Time
}

// manifestBytes returns the pre-signed manifest JSON + ok. (nil,false) in single mode (selfID=="")
// or before the cluster can sign anything → the HTTP handler replies 503. Hot path = atomic load.
func (b *Broker) manifestBytes() ([]byte, bool) {
	if b.selfID == "" {
		return nil, false
	}
	now := b.cfg.Now()
	if cur := b.manifestCache.Load(); cur != nil && now.Before(cur.nextCheckAt) {
		return cur.bytes, true // hot path: no DB, no sign
	}
	b.manifestMu.Lock()
	defer b.manifestMu.Unlock()
	cur := b.manifestCache.Load()
	if cur != nil && now.Before(cur.nextCheckAt) {
		return cur.bytes, true
	}
	rg, err1 := cluster.RosterGeneration(b.cfg.DB)
	eps, bootstrap, sg, err2 := cluster.Seeds(b.cfg.DB)
	resign := cur == nil
	if cur != nil && (rg != cur.rosterGen || sg != cur.seedGen || now.Sub(cur.signedAt) >= manifestResignAfter) {
		resign = true
	}
	if !resign || err1 != nil || err2 != nil {
		if cur != nil { // push the next re-check out (cheap), keep serving the same bytes
			b.manifestCache.Store(&manifestSnapshot{
				bytes: cur.bytes, rosterGen: cur.rosterGen, seedGen: cur.seedGen,
				signedAt: cur.signedAt, nextCheckAt: now.Add(manifestRecheckInterval),
			})
			return cur.bytes, true
		}
		return nil, false
	}
	snap, ok := b.buildManifestSnapshot(now, rg, eps, bootstrap, sg)
	if !ok {
		if cur != nil {
			return cur.bytes, true // keep serving the prior good manifest
		}
		return nil, false
	}
	b.manifestCache.Store(snap)
	return snap.bytes, true
}

func (b *Broker) buildManifestSnapshot(now time.Time, rg uint64, endpoints []string, bootstrap string, sg uint64) (*manifestSnapshot, bool) {
	roster := b.buildSignedRoster()
	if roster == nil {
		return nil, false // single mode / no account seed
	}
	seeds := b.buildSeedBundle(now, endpoints, bootstrap, sg) // may be nil until `cluster seeds publish`
	m := proto.ClusterManifest{SchemaVersion: proto.ClusterManifestSchemaVersion, Roster: roster, Seeds: seeds}
	body, err := json.Marshal(m)
	if err != nil {
		return nil, false
	}
	return &manifestSnapshot{
		bytes: body, rosterGen: rg, seedGen: sg, signedAt: now, nextCheckAt: now.Add(manifestRecheckInterval),
	}, true
}

// handleSeedsShow (C2) answers OpClusterSeedsShow: a read-only RODB derive of the published seed
// config + the account pub (so the CLI can mint a full invite). Leader-agnostic.
func (b *clusterAdminBackend) handleSeedsShow(req adminsock.Request) adminsock.Response {
	eps, bootstrap, gen, err := b.admin.ReadSeeds()
	if err != nil {
		return adminsock.Response{Op: req.Op, Error: err.Error(), Code: adminsock.CodeStoreError}
	}
	ap := ""
	if b.accountPub != nil {
		ap = b.accountPub()
	}
	return adminsock.Response{Op: req.Op, OK: true, SeedEndpoints: eps, SeedBootstrap: bootstrap, SeedGeneration: gen, SeedAccountPub: ap}
}

// handleSeedsPublish (C2) answers OpClusterSeedsPublish: leader-only raft Propose of the new seed
// endpoints + bootstrap, returning the new seed_generation + account pub for the invite.
func (b *clusterAdminBackend) handleSeedsPublish(req adminsock.Request) adminsock.Response {
	gen, err := b.admin.PublishSeeds(req.Endpoints, req.Bootstrap)
	if err != nil {
		return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
	}
	ap := ""
	if b.accountPub != nil {
		ap = b.accountPub()
	}
	return adminsock.Response{Op: req.Op, OK: true, SeedGeneration: gen, SeedAccountPub: ap, SeedEndpoints: req.Endpoints, SeedBootstrap: req.Bootstrap}
}

// accountPubOrEmpty returns the cluster account public key, or "" when no account seed is available.
func (b *Broker) accountPubOrEmpty() string {
	if b.cfg.AuthCallout == nil || len(b.cfg.AuthCallout.AccountSeed) == 0 {
		return ""
	}
	ap, err := auth.PublicKeyFromSeed(b.cfg.AuthCallout.AccountSeed)
	if err != nil {
		return ""
	}
	return ap
}

// buildSeedBundle reads the replicated seed config + account-signs it. nil until `cluster seeds
// publish` has stored at least one endpoint (or when no account seed).
func (b *Broker) buildSeedBundle(now time.Time, endpoints []string, bootstrap string, gen uint64) *proto.SeedBundle {
	if b.cfg.AuthCallout == nil || len(endpoints) == 0 {
		return nil
	}
	seed := b.cfg.AuthCallout.AccountSeed
	if len(seed) == 0 {
		return nil
	}
	accountPub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		return nil
	}
	issued := now.UTC()
	expires := issued.Add(rosterTTL)
	s, err := clusterroster.BuildSeeds(seed, accountPub, gen, endpoints, bootstrap, issued.Format(time.RFC3339Nano), expires.Format(time.RFC3339Nano))
	if err != nil {
		return nil
	}
	return s
}
