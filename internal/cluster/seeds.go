package cluster

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// seeds.go (C2 §D-5) — the replicated public client-dialable seed endpoints + bootstrap (well-known)
// URL + a DEDICATED monotone seed_generation counter, written by `cluster seeds publish`. A new agent
// has no NATS template to turn a roster into a dialable URL, so the broker serves these explicit
// endpoints in an account-signed SeedBundle. The counter is separate from roster_generation so a
// seeds-publish never pollutes the membership topology-lag signal (rostergen.go) and endpoint
// rotation gets its own anti-rollback.

const (
	metaKeySeedEndpoints  = "seed_endpoints"  // newline-joined client-dialable URLs
	metaKeySeedBootstrap  = "seed_bootstrap"  // the well-known HTTPS manifest URL
	metaKeySeedGeneration = "seed_generation" // dedicated monotone counter

	maxSeedEndpoints   = 8
	maxSeedEndpointLen = 512
)

// Seeds reads the replicated seed config (bounded-stale, no txn). Absent rows read as empty/0.
func Seeds(ro *sql.DB) (endpoints []string, bootstrap string, gen uint64, err error) {
	ep, err := readMetaTextDB(ro, metaKeySeedEndpoints)
	if err != nil {
		return nil, "", 0, err
	}
	bootstrap, err = readMetaTextDB(ro, metaKeySeedBootstrap)
	if err != nil {
		return nil, "", 0, err
	}
	gen, err = readMetaUintDB(ro, metaKeySeedGeneration)
	if err != nil {
		return nil, "", 0, err
	}
	for _, e := range strings.Split(ep, "\n") {
		if e != "" {
			endpoints = append(endpoints, e)
		}
	}
	return endpoints, bootstrap, gen, nil
}

// readMetaTextDB reads a text-valued cluster_meta KV outside any txn. Missing row → "" (the D0-empty
// convention, sibling to readMetaUintDB).
func readMetaTextDB(db *sql.DB, key string) (string, error) {
	var v string
	err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key = ?`, key).Scan(&v)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("cluster: read meta %q: %w", key, err)
	}
	return v, nil
}

// seedGenBumpStmt is the seed_generation bump appended to a seeds-publish command. Unlike the roster
// counter it is NOT change-gated — a publish is an explicit operator action that always advances the
// counter (endpoint rotation must defeat replay even when the endpoint set is unchanged). MAX-floored
// to now.UnixNano() so it is strictly monotone and never below a prior value (rolling-upgrade safe).
func seedGenBumpStmt(now time.Time) Statement {
	nowLit := MustLitText(formatUint(uint64(now.UTC().UnixNano())))
	sql := `INSERT INTO cluster_meta(key, value) VALUES(` + MustLitText(metaKeySeedGeneration) + `, ` + nowLit + `) ` +
		`ON CONFLICT(key) DO UPDATE SET value = MAX(CAST(cluster_meta.value AS INTEGER)+1, CAST(excluded.value AS INTEGER))`
	return Stmt(sql)
}

// PlanClusterSeedsPublish renders OpClusterSeedsPublish: UPSERT the endpoints + bootstrap URL and bump
// seed_generation, all in one Apply txn. Leader-side validation rejects garbage (bad scheme / empty
// host / oversized / too many / zero endpoints) BEFORE proposing; NUL/non-UTF-8 is rejected for free
// by MustLitText/LitText. All-literal → rides genericExecApplier.
func PlanClusterSeedsPublish(endpoints []string, bootstrap string, now time.Time) (*Command, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("cluster: plan seeds-publish: at least one endpoint required")
	}
	if len(endpoints) > maxSeedEndpoints {
		return nil, fmt.Errorf("cluster: plan seeds-publish: too many endpoints (%d > %d)", len(endpoints), maxSeedEndpoints)
	}
	for _, e := range endpoints {
		if err := validateSeedEndpoint(e); err != nil {
			return nil, fmt.Errorf("cluster: plan seeds-publish: %w", err)
		}
	}
	if err := validateBootstrapEndpoint(bootstrap); err != nil {
		return nil, fmt.Errorf("cluster: plan seeds-publish: %w", err)
	}
	epLit, err := LitText(strings.Join(endpoints, "\n"))
	if err != nil {
		return nil, fmt.Errorf("cluster: plan seeds-publish: endpoints literal: %w", err)
	}
	bsLit, err := LitText(bootstrap)
	if err != nil {
		return nil, fmt.Errorf("cluster: plan seeds-publish: bootstrap literal: %w", err)
	}
	upEndpoints := `INSERT INTO cluster_meta(key,value) VALUES(` + MustLitText(metaKeySeedEndpoints) + `, ` + epLit +
		`) ON CONFLICT(key) DO UPDATE SET value=excluded.value`
	upBootstrap := `INSERT INTO cluster_meta(key,value) VALUES(` + MustLitText(metaKeySeedBootstrap) + `, ` + bsLit +
		`) ON CONFLICT(key) DO UPDATE SET value=excluded.value`
	return NewCommand(OpClusterSeedsPublish, Stmt(upEndpoints), Stmt(upBootstrap), seedGenBumpStmt(now)), nil
}

func validateSeedEndpoint(e string) error {
	if len(e) > maxSeedEndpointLen {
		return fmt.Errorf("endpoint too long (%d > %d)", len(e), maxSeedEndpointLen)
	}
	u, err := url.Parse(e)
	if err != nil || u.Host == "" {
		return fmt.Errorf("bad endpoint url %q", e)
	}
	switch u.Scheme {
	case "nats", "tls", "wss":
		return nil
	default:
		// Plaintext ws:// is refused: it is the agent's first CONNECT target and carries the session
		// PIN as a cleartext nats.Token. (Stage-C m3: the publish allowlist now matches its own error.)
		return fmt.Errorf("endpoint scheme must be nats/tls/wss, got %q", u.Scheme)
	}
}

func validateBootstrapEndpoint(b string) error {
	if b == "" {
		return nil // bootstrap URL is optional in the bundle
	}
	if len(b) > maxSeedEndpointLen {
		return fmt.Errorf("bootstrap url too long")
	}
	u, err := url.Parse(b)
	if err != nil || u.Host == "" {
		return fmt.Errorf("bad bootstrap url %q", b)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("bootstrap url scheme must be http(s), got %q", u.Scheme)
	}
	return nil
}
