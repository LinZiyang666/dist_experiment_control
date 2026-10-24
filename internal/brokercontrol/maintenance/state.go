package maintenance

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

const MaxMaintenanceHomes = 1024

// Baseline is historical state, not a manufactured healthy observation. P13
// pauses only matching-home dwell growth and resumes from these values after
// expiry; a planned return retains its pending decision across leader changes.
// origin: hot-update S6 plan §7.2 / T43
type MaintenanceBaseline struct {
	Home          string `json:"home"`
	InstanceID    string `json:"instance_id"`
	DwellNS       int64  `json:"dwell_ns"`
	PlannedDown   bool   `json:"planned_down"`
	ReturnPending bool   `json:"return_pending"`
}

func MaintenanceBaselineDigest(b MaintenanceBaseline) (string, error) {
	if b.InstanceID == "" || b.DwellNS < 0 {
		return "", brokermaint.ErrInvalid
	}
	raw, err := json.Marshal(b)
	if err != nil {
		return "", brokermaint.ErrInvalid
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

// P02 owns the sole private codec. These are value projections for its control
// partition; no NATS connection, timer, callback or verification key is exported.
type MaintenanceObservation struct {
	Lease     brokermaint.LocalLease `json:"lease"`
	Challenge brokermaint.Challenge  `json:"challenge"`
	// Retain the proof that actually imposed the minimum. Saving only the most
	// recent proof would let import accidentally replace the earlier expiry.
	ExpiryAnchor    brokermaint.LocalLease `json:"expiry_anchor"`
	ExpiryChallenge brokermaint.Challenge  `json:"expiry_challenge"`
	Baseline        MaintenanceBaseline    `json:"baseline"`
	Ended           bool                   `json:"ended"`
}

type MaintenanceSnapshot struct {
	ObserverBootID string                   `json:"observer_boot_id"`
	Observations   []MaintenanceObservation `json:"observations"`
}

// Each observer owns one cache. Leadership does not own or rebase its deadlines.
type MaintenanceCache struct {
	mu     sync.Mutex
	bootID string
	homes  map[string]MaintenanceObservation
}

func NewMaintenanceCache(observerBootID string) (*MaintenanceCache, error) {
	if observerBootID == "" {
		return nil, brokermaint.ErrIdentity
	}
	return &MaintenanceCache{bootID: observerBootID, homes: make(map[string]MaintenanceObservation)}, nil
}

func validateBaseline(r brokermaint.Record, b MaintenanceBaseline) error {
	digest, err := MaintenanceBaselineDigest(b)
	if err != nil || b.Home != r.Home || b.InstanceID != r.InstanceID || digest != r.BaselineDigest {
		return brokermaint.ErrIdentity
	}
	return nil
}

// Verify signatures outside the cache lock, then recheck the previous lease
// while committing. Two delayed replies cannot race to install fresh lifetime.
// origin: hot-update S6 plan §7.1 / T39
func (c *MaintenanceCache) Observe(r brokermaint.Record, p brokermaint.Proof, challenge brokermaint.Challenge, baseline MaintenanceBaseline, nowBootNS int64, minimumAppliedIndex uint64, verify brokermaint.VerifySignature) error {
	if err := validateBaseline(r, baseline); err != nil {
		return err
	}
	lease, err := brokermaint.ObserveProof(r, p, challenge, c.bootID, nowBootNS, minimumAppliedIndex, nil, verify)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	old, exists := c.homes[r.Home]
	anchor, anchorChallenge := lease, challenge
	if exists {
		if old.Lease.Record.CutID == r.CutID {
			if old.Ended || brokermaint.ValidateTransition(old.Lease.Record, r) != nil || old.Baseline != baseline {
				return brokermaint.ErrConflict
			}
			if old.Lease.ExpiresLocalBootNS < lease.ExpiresLocalBootNS {
				lease.ExpiresLocalBootNS = old.Lease.ExpiresLocalBootNS
				anchor, anchorChallenge = old.ExpiryAnchor, old.ExpiryChallenge
			}
		} else if !old.Ended && nowBootNS < old.Lease.ExpiresLocalBootNS {
			return brokermaint.ErrConflict
		}
	} else if len(c.homes) >= MaxMaintenanceHomes {
		return brokermaint.ErrInvalid
	}
	if nowBootNS >= lease.ExpiresLocalBootNS {
		return brokermaint.ErrExpired
	}
	c.homes[r.Home] = MaintenanceObservation{Lease: lease, Challenge: challenge, ExpiryAnchor: anchor, ExpiryChallenge: anchorChallenge, Baseline: baseline}
	return nil
}

// End is called only after the authority owner commits/readbacks this exact
// transition. It keeps the signed observation and baseline for image/history.
func (c *MaintenanceCache) End(expected, ended brokermaint.Record) error {
	if ended.Phase != brokermaint.MaintEnded || brokermaint.ValidateTransition(expected, ended) != nil {
		return brokermaint.ErrConflict
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	old, ok := c.homes[expected.Home]
	if !ok || old.Lease.Record != expected {
		return brokermaint.ErrConflict
	}
	old.Ended = true
	c.homes[expected.Home] = old
	return nil
}

func (c *MaintenanceCache) Lookup(current brokermaint.Record, nowBootNS int64) (MaintenanceBaseline, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	old, ok := c.homes[current.Home]
	if !ok {
		return MaintenanceBaseline{}, false
	}
	return old.Baseline, !old.Ended && brokermaint.Protects(old.Lease, current, nowBootNS)
}

func (c *MaintenanceCache) Snapshot() MaintenanceSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := MaintenanceSnapshot{ObserverBootID: c.bootID, Observations: make([]MaintenanceObservation, 0, len(c.homes))}
	for _, o := range c.homes {
		s.Observations = append(s.Observations, o)
	}
	sort.Slice(s.Observations, func(i, j int) bool { return s.Observations[i].Lease.Record.Home < s.Observations[j].Lease.Record.Home })
	return s
}

// Restore verifies the original nonce/t0 and signature, without using import
// time. Expired observations remain expired but still carry historical dwell.
// Reboot loses this cache's clock domain and must refuse planned protection.
// origin: hot-update S6 plan §7.1 / T39
func RestoreMaintenanceCache(s MaintenanceSnapshot, observerBootID string, verify brokermaint.VerifySignature) (*MaintenanceCache, error) {
	if s.ObserverBootID != observerBootID || len(s.Observations) > MaxMaintenanceHomes {
		return nil, brokermaint.ErrIdentity
	}
	c, err := NewMaintenanceCache(observerBootID)
	if err != nil {
		return nil, err
	}
	for _, o := range s.Observations {
		if validateBaseline(o.Lease.Record, o.Baseline) != nil {
			return nil, brokermaint.ErrIdentity
		}
		validated, err := brokermaint.ObserveProof(o.Lease.Record, o.Lease.Proof, o.Challenge, observerBootID, o.Challenge.SentBootNS, 0, nil, verify)
		if err != nil {
			return nil, err
		}
		anchored, err := brokermaint.ObserveProof(o.ExpiryAnchor.Record, o.ExpiryAnchor.Proof, o.ExpiryChallenge, observerBootID, o.ExpiryChallenge.SentBootNS, 0, nil, verify)
		if err != nil {
			return nil, err
		}
		if brokermaint.ValidateTransition(anchored.Record, validated.Record) != nil || o.ExpiryAnchor.ExpiresLocalBootNS != anchored.ExpiresLocalBootNS {
			return nil, brokermaint.ErrIdentity
		}
		minimum := min(anchored.ExpiresLocalBootNS, validated.ExpiresLocalBootNS)
		if o.Lease.ExpiresLocalBootNS != minimum {
			return nil, brokermaint.ErrInvalid
		}
		if _, duplicate := c.homes[o.Lease.Record.Home]; duplicate {
			return nil, brokermaint.ErrConflict
		}
		c.homes[o.Lease.Record.Home] = o
	}
	return c, nil
}

// PlanMaintenanceRecord runs inside the authority owner's live writer planning
// callback. Exact canonical bytes guard both the planning read and applied SQL;
// an old holder/phase cannot overwrite a newer cut even if term/epoch matches.
// P16 owns Propose, commit/readback and observer membership acquisition.
// origin: hot-update S6 plan §7.1 / T39
func PlanMaintenanceRecord(db *sql.DB, expected *brokermaint.Record, next brokermaint.Record, baseline MaintenanceBaseline) (*cluster.Command, error) {
	raw, err := brokermaint.CanonicalRecordBytes(next)
	if err != nil {
		return nil, err
	}
	if err := validateBaseline(next, baseline); err != nil {
		return nil, err
	}
	key := brokermaint.MetaKeyPrefix + next.Home
	var current string
	err = db.QueryRow("SELECT value FROM cluster_meta WHERE key = ?", key).Scan(&current)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if expected == nil {
		if !errors.Is(err, sql.ErrNoRows) || next.Phase != brokermaint.MaintPrepared {
			return nil, brokermaint.ErrConflict
		}
	} else {
		old, e := brokermaint.CanonicalRecordBytes(*expected)
		if e != nil || err != nil || current != string(old) || expected.Home != next.Home {
			return nil, brokermaint.ErrConflict
		}
		replacement := expected.CutID != next.CutID && next.Phase == brokermaint.MaintPrepared
		unowned := false
		if replacement && expected.Holder != next.Holder {
			lock, e := cluster.ReadUpgradeLock(db)
			marker, merr := cluster.UpgradeHolderMarker(next.Holder)
			unowned = e == nil && merr == nil && lock.Marker == marker
		}
		if !unowned {
			if _, e := ReadMaintenanceBaseline(db, *expected); e != nil {
				return nil, e
			}
		}
		if expected.CutID == next.CutID {
			if e = brokermaint.ValidateTransition(*expected, next); e != nil {
				return nil, e
			}
		} else if !unowned && (expected.Phase != brokermaint.MaintEnded || next.Phase != brokermaint.MaintPrepared) {
			return nil, brokermaint.ErrConflict
		}
	}
	keySQL, err := cluster.LitText(key)
	if err != nil {
		return nil, err
	}
	valueSQL, err := cluster.LitText(string(raw))
	if err != nil {
		return nil, err
	}
	var statement string
	if expected == nil {
		statement = "INSERT INTO cluster_meta(key,value) SELECT " + keySQL + "," + valueSQL + " WHERE NOT EXISTS (SELECT 1 FROM cluster_meta WHERE key=" + keySQL + ")"
	} else {
		oldSQL, e := cluster.LitText(current)
		if e != nil {
			return nil, e
		}
		statement = "UPDATE cluster_meta SET value=" + valueSQL + " WHERE key=" + keySQL + " AND value=" + oldSQL
	}
	baselineRaw, err := json.Marshal(baseline)
	if err != nil {
		return nil, err
	}
	baselineKeySQL, err := cluster.LitText(brokermaint.MetaKeyPrefix + "baseline/" + next.Home)
	if err != nil {
		return nil, err
	}
	baselineValueSQL, err := cluster.LitText(string(baselineRaw))
	if err != nil {
		return nil, err
	}
	// The second statement is eligible only if the exact new record exists.
	// A stale zero-row CAS cannot overwrite a newer cut's historical baseline.
	// Digest binding makes an already-applied identical cut idempotent. Both
	// rows commit in one existing FSM transaction, including on single brokers.
	baselineSQL := "INSERT INTO cluster_meta(key,value) SELECT " + baselineKeySQL + "," + baselineValueSQL + " WHERE EXISTS (SELECT 1 FROM cluster_meta WHERE key=" + keySQL + " AND value=" + valueSQL + ") ON CONFLICT(key) DO UPDATE SET value=excluded.value"
	return cluster.NewCommand(cluster.OpClusterMetaSet, cluster.Stmt(statement), cluster.Stmt(baselineSQL)), nil
}

// The slash keeps the baseline key disjoint from every valid home identifier.
// Followers load these exact bytes rather than inventing fresh local history.
// origin: hot-update S6 plan §7.2 / T43
func ReadMaintenanceBaseline(db *sql.DB, record brokermaint.Record) (MaintenanceBaseline, error) {
	if err := brokermaint.ValidateRecord(record); err != nil {
		return MaintenanceBaseline{}, err
	}
	var raw string
	if err := db.QueryRow("SELECT value FROM cluster_meta WHERE key=?", brokermaint.MetaKeyPrefix+"baseline/"+record.Home).Scan(&raw); err != nil {
		return MaintenanceBaseline{}, err
	}
	var baseline MaintenanceBaseline
	if len(raw) > brokermaint.MaxRecordBytes || json.Unmarshal([]byte(raw), &baseline) != nil {
		return MaintenanceBaseline{}, brokermaint.ErrInvalid
	}
	canonical, err := json.Marshal(baseline)
	if err != nil || !bytes.Equal([]byte(raw), canonical) {
		return MaintenanceBaseline{}, brokermaint.ErrInvalid
	}
	if err := validateBaseline(record, baseline); err != nil {
		return MaintenanceBaseline{}, err
	}
	return baseline, nil
}

// Confirmation is the exact required observer set captured by P16/P17 from
// current voters and reachable affected routes, not an ACK-selected cohort.
type MaintenanceObserver struct {
	ID                string
	NID               string
	ConnectedBrokerID string
	IngressServerID   string
	RouteDigest       string
	Nonce             string
	ProofDigest       string
	Capability        string
	SentBootNS        int64
}

type MaintenanceAck struct {
	ObserverID string
	Reply      proto.BrokerMaintenanceAck
}

var ErrMaintenanceObserverUnsupported = errors.New("maintenance_observer_unsupported")

// MaintenanceCutDeadline aggregates signed, installed-hold receipts. t0 plus
// remaining is a conservative lower bound, consuming RTT a second time rather
// than pretending a late ACK bought protection. Every observer must leave the
// full normal request window; the component's 120s watchdog is irrelevant.
// origin: hot-update S6 plan §7.1 / T39
func MaintenanceCutDeadline(r brokermaint.Record, issuerBootID string, nowBootNS int64, required []MaintenanceObserver, acks []MaintenanceAck, verify func(MaintenanceObserver, []byte, string) error) (int64, error) {
	if err := brokermaint.ValidateRecord(r); err != nil {
		return 0, err
	}
	if issuerBootID == "" || issuerBootID != r.IssuerBootID || r.Phase != brokermaint.MaintActive || nowBootNS < r.IssuedBootNS {
		return 0, brokermaint.ErrIdentity
	}
	if len(required) == 0 || len(required) > MaxMaintenanceHomes || len(acks) != len(required) || verify == nil {
		return 0, ErrMaintenanceObserverUnsupported
	}
	byID := make(map[string]MaintenanceAck, len(acks))
	for _, ack := range acks {
		if _, duplicate := byID[ack.ObserverID]; duplicate {
			return 0, brokermaint.ErrConflict
		}
		byID[ack.ObserverID] = ack
	}
	deadline := r.ExpiresBootNS
	seen := make(map[string]bool, len(required))
	for _, observer := range required {
		if observer.ID == "" || seen[observer.ID] {
			return 0, brokermaint.ErrConflict
		}
		seen[observer.ID] = true
		ack, ok := byID[observer.ID]
		if !ok || observer.Capability != brokermaint.Capability {
			return 0, ErrMaintenanceObserverUnsupported
		}
		a := ack.Reply
		if !a.Accepted || a.CutID != r.CutID || a.Nonce != observer.Nonce || a.NID != observer.NID || a.InstanceID != r.InstanceID || a.ConnectedBrokerID != observer.ConnectedBrokerID || a.IngressServerID != observer.IngressServerID || a.RouteDigest != observer.RouteDigest || a.ProofDigest != observer.ProofDigest {
			return 0, brokermaint.ErrIdentity
		}
		// Route mode is part of the signed record, not inferred from empty SQL home.
		if r.Mode == "single" {
			if a.ConnectedBrokerID != "" || a.IngressServerID != r.IngressServerID {
				return 0, brokermaint.ErrIdentity
			}
		} else if a.ConnectedBrokerID != r.Home || a.IngressServerID != "" {
			return 0, brokermaint.ErrIdentity
		}
		b, err := brokermaint.CanonicalAckBytes(a.CutID, a.Nonce, a.NID, a.ConnectedBrokerID, a.IngressServerID, a.InstanceID, a.RouteDigest, a.ProofDigest, a.Accepted, a.RemainingNS)
		if err != nil || a.Sig == "" || verify(observer, b, a.Sig) != nil {
			return 0, brokermaint.ErrIdentity
		}
		if observer.SentBootNS < r.IssuedBootNS || observer.SentBootNS > nowBootNS || observer.SentBootNS > math.MaxInt64-a.RemainingNS {
			return 0, brokermaint.ErrInvalid
		}
		bound := observer.SentBootNS + a.RemainingNS
		if bound < deadline {
			deadline = bound
		}
	}
	if deadline <= nowBootNS || time.Duration(deadline-nowBootNS) < upgradebudget.RequestLatency {
		return 0, brokermaint.ErrExpired
	}
	return deadline, nil
}
