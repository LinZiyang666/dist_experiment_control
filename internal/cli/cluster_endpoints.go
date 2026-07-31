package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// cluster_endpoints.go — the ctl/cli's cluster broker auto-failover discovery cache + dial resolver. Fixes
// the v0.4.5 阶段4 finding: a ctl configured with a single broker_url could not reach a surviving broker
// when its configured one died (agents fail over; the ctl did not). The ctl now hands nats.Connect a
// floor-last, account-verified, comma-separated server list — but ONLY on the persisted broker_url (File)
// path, and ONLY against an out-of-band-pinned cluster account_pub. flag/env/default stay pinned-single
// (operator override is never silently widened), and a cold/unpinned/cross-cluster ctl is byte-identical
// to today (non-cluster invariant). This package imports clusterroster+proto only — never internal/agent
// (the agent→cli import cycle is real); the WRITE/refresh path lives in package main (cmd/tether).

// NoDiscoverEnv disables broker auto-failover expansion + HTTP refresh entirely (pins single, today's
// behavior). An operator escape hatch for debugging a specific node.
const NoDiscoverEnv = "TETHER_NO_DISCOVER"

// ClusterEndpointsSchemaVersion is the on-disk schema of cluster_endpoints.json.
const ClusterEndpointsSchemaVersion = 1

// Source classifies WHERE the resolved broker URL came from. Only SourceFile (the persisted broker_url
// default) is eligible for failover expansion — an explicit flag/env override pins to exactly one endpoint.
type Source int

const (
	SourceFlag    Source = iota // --nats-url (explicit operator override)
	SourceEnv                   // $TETHER_NATS_URL (per-shell override)
	SourceFile                  // ~/.tether/broker_url (persistent default — the ONLY expandable source)
	SourceDefault               // cobra default (no broker configured)
)

// ClusterEndpointsPath is the global (session-less) discovery cache file. One cache per home = one cluster
// the user talks to.
func ClusterEndpointsPath(home string) string {
	return filepath.Join(home, "cluster_endpoints.json")
}

// ClusterEndpoints is the persisted discovery cache. PinAccountPub is set ONLY out-of-band (cluster pin /
// login --account-pub) — never HTTP/NATS-TOFU. FloorURL is the broker_url it was learned under and acts as
// the selection key (a different broker_url ⇒ cache ignored ⇒ clean re-pin). InviteSeeds are OOB-trusted,
// paste-trusted permanent floor endpoints. Roster/Seeds are account-signed, HTTP-refreshed, and re-verified
// against the pin on every read.
type ClusterEndpoints struct {
	SchemaVersion int                  `json:"schema_version"`
	PinAccountPub string               `json:"pin_account_pub"`
	FloorURL      string               `json:"floor_url"`
	BootstrapURL  string               `json:"bootstrap_url,omitempty"` // tier-2 signed HTTP manifest URL (OOB)
	InviteSeeds   []string             `json:"invite_seeds,omitempty"`
	RosterGen     uint64               `json:"roster_gen"`
	Roster        *proto.ClusterRoster `json:"roster,omitempty"`
	SeedGen       uint64               `json:"seed_gen"`
	Seeds         *proto.SeedBundle    `json:"seeds,omitempty"`
	FetchedAt     string               `json:"fetched_at"`
}

// ReadClusterEndpoints loads the discovery cache. (nil,nil) when the file does not exist OR is corrupt
// (corrupt-tolerant: a torn/garbage cache is treated as absent → the caller falls back to the floor, never
// panics). A genuine non-ENOENT read error is returned.
func ReadClusterEndpoints(home string) (*ClusterEndpoints, error) {
	b, err := os.ReadFile(ClusterEndpointsPath(home))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cli: read cluster endpoints: %w", err)
	}
	var ce ClusterEndpoints
	if err := json.Unmarshal(b, &ce); err != nil {
		// corrupt → treat as empty (floor-only), no panic
		//nolint:nilerr // This file is a CACHE. A corrupt one must degrade to "absent" so the caller
		// falls back to the floor-only path, exactly as it does when the file was never written.
		// Returning the parse error instead would turn a stale scratch file into a hard startup failure
		// for a command that has a perfectly good answer without it.
		return nil, nil
	}
	if ce.SchemaVersion > ClusterEndpointsSchemaVersion {
		// A future-schema cache may change InviteSeeds/field semantics; an old binary must NOT trust it.
		// Treat as absent → floor-only (the signed roster/seed children have their own schema gates).
		return nil, nil
	}
	return &ce, nil
}

// WriteClusterEndpoints atomically persists the discovery cache (tmp+chmod 0600+write+fsync+rename+parent
// fsync — the roster_cache.go pattern). SchemaVersion is stamped here.
func WriteClusterEndpoints(home string, ce *ClusterEndpoints) error {
	ce.SchemaVersion = ClusterEndpointsSchemaVersion
	body, err := json.MarshalIndent(ce, "", "  ")
	if err != nil {
		return fmt.Errorf("cli: marshal cluster endpoints: %w", err)
	}
	path := ClusterEndpointsPath(home)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("cli: mkdir cluster endpoints: %w", err)
	}
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("cli: open tmp cluster endpoints: %w", err)
	}
	tmp := f.Name()
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("cli: chmod cluster endpoints: %w", err)
	}
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("cli: write cluster endpoints: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("cli: fsync cluster endpoints: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("cli: close cluster endpoints: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("cli: rename cluster endpoints: %w", err)
	}
	if d, derr := os.Open(filepath.Dir(path)); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// ResolveBrokerURLSource resolves the broker URL AND its source (precedence high→low: explicit --nats-url
// flag → $TETHER_NATS_URL → ~/.tether/broker_url file → cobra default). The source split is load-bearing:
// only SourceFile expands to a failover set; everything else pins single. (ReadDefaultBrokerURL folds
// env+file, so the operator-override invariant is inexpressible without this source-aware variant.)
func ResolveBrokerURLSource(flagVal string, flagChanged bool, home string) (string, Source) {
	if flagChanged {
		return flagVal, SourceFlag
	}
	if v := os.Getenv(DefaultBrokerURLEnv); v != "" {
		return v, SourceEnv
	}
	if b, err := os.ReadFile(BrokerURLFile(home)); err == nil {
		s := strings.TrimRight(string(b), "\r\n")
		if s != "" {
			return s, SourceFile
		}
	}
	return flagVal, SourceDefault
}

// DialFor returns the comma-separated server list handed to nats.Connect for an already-resolved broker
// URL `base`. It returns `base` UNCHANGED (byte-for-byte — the non-cluster / operator-override invariant)
// unless ALL of: the URL came from the persisted broker_url default (NOT an explicit --nats-url flag nor
// $TETHER_NATS_URL) ∧ TETHER_NO_DISCOVER is unset ∧ a discovery cache exists ∧ it is OOB-pinned ∧ its
// FloorURL == base (so the cache belongs to THIS broker, not a different cluster) ∧ the cached signed
// bundles still verify against the pin at `now`. flagChanged is cmd.Flags().Changed("nats-url"); the env
// check pins the env override single too. Only the FloorURL==base match distinguishes the expandable
// persisted-default from the cobra default (whose base won't match any real cache key).
func DialFor(flagChanged bool, base, home string, now time.Time) string {
	if flagChanged || os.Getenv(DefaultBrokerURLEnv) != "" {
		return base // (A) operator override: pin single, never silently widened
	}
	if os.Getenv(NoDiscoverEnv) != "" {
		return base // R3 escape hatch
	}
	ce, err := ReadClusterEndpoints(home)
	if err != nil || ce == nil || ce.PinAccountPub == "" || ce.FloorURL != base {
		return base // (B) cold / unpinned / cross-cluster / cobra-default → byte-equivalent
	}
	var learned, seeds []string
	if ce.Roster != nil && clusterroster.VerifyAt(ce.Roster, ce.PinAccountPub, now) == nil {
		if urls, derr := clusterroster.DialURLs(ce.Roster, base); derr == nil {
			learned = urls // fail-closed: a verify failure drops this tier (never dial an unverified signed endpoint)
		}
	}
	if ce.Seeds != nil && clusterroster.VerifySeedsAt(ce.Seeds, ce.PinAccountPub, now) == nil {
		seeds = clusterroster.SeedDialURLs(ce.Seeds)
	}
	floorCSV := base
	for _, s := range ce.InviteSeeds { // OOB-trusted, paste-trusted permanent floor
		if strings.TrimSpace(s) != "" {
			floorCSV += "," + s
		}
	}
	return clusterroster.BuildDialString(learned, seeds, floorCSV)
}
