package port

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
)

var ErrMaintenanceProtected = errors.New("port: home protected by maintenance")
var ErrMutationGuard = errors.New("port: mutation guard changed or incomplete")

const maintenanceKeyPrefix = "broker_image_maintenance."

func planAllocateIntent(db *sql.DB, intent AllocationIntent, replacing *Allocation, where string) (*Allocation, *cluster.Command, error) {
	allocation, cmd, err := planAllocate(db, intent.SID, intent.NID, intent.Name, intent.LocalPort, intent.DesiredPort,
		intent.CreatedByFP, intent.HomeBroker, intent.RebuildOff, intent.Config, replacing,
		intent.Name == ProxyPortName, where)
	if err == nil && intent.Name == ProxyPortName {
		cmd.Op = cluster.OpProxyAllocate
	}
	return allocation, cmd, err
}

func allocationNameSQL(sid, nid, name string, proxyScope bool) (string, error) {
	lits, err := cluster.LitTextAll(sid, nid, name)
	if err != nil {
		return "", err
	}
	scope := `sid=` + lits[0] + ` AND name=` + lits[2] + ` AND state='ALLOCATED'`
	if proxyScope {
		scope += ` AND nid=` + lits[1]
	}
	return scope, nil
}

func allocationAbsenceSQL(intent AllocationIntent) (string, error) {
	if intent.SID == "" || intent.NID == "" || intent.Name == "" {
		return "", ErrMutationGuard
	}
	scope, err := allocationNameSQL(intent.SID, intent.NID, intent.Name, intent.Name == ProxyPortName)
	if err != nil {
		return "", err
	}
	return `NOT EXISTS (SELECT 1 FROM port_allocations WHERE ` + scope + `)`, nil
}

func mutationPredicate(db *sql.DB, guard MutationGuard) (string, error) {
	if db == nil || guard.NowBootNS <= 0 {
		return "", ErrMutationGuard
	}
	identity := "1"
	if guard.Expected != nil {
		a := *guard.Expected
		if guard.ExpectAbsent || a.State != StateAllocated || a.Port <= 0 || a.Epoch < 0 ||
			a.SID == "" || a.NID == "" || a.Name == "" || a.TokenHash == "" || guard.FromHome != a.HomeBroker {
			return "", ErrMutationGuard
		}
		var err error
		identity, err = allocationIdentitySQL(a, StateAllocated)
		if err != nil {
			return "", err
		}
		// origin: hot-update S6 plan §7.2 / T42
		// A guard assembled before the writer callback is merely an expectation;
		// it cannot substitute for this read of the current committed allocation.
		var exists int
		if err := db.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE ` + identity).Scan(&exists); err != nil {
			return "", fmt.Errorf("port: guarded allocation lookup: %w", err)
		}
		if exists == 0 {
			return "", ErrNotFound
		}
	} else if !guard.ExpectAbsent || guard.FromHome != "" {
		return "", ErrMutationGuard
	}
	maintenance, err := maintenancePredicate(db, guard)
	if err != nil {
		return "", err
	}
	return identity + ` AND ` + maintenance, nil
}

func maintenancePredicate(db *sql.DB, guard MutationGuard) (string, error) {
	sid, nid := guard.Authority.SID, guard.Authority.ConfiguredNID
	if guard.Expected != nil {
		sid, nid = guard.Expected.SID, guard.Expected.NID
		if guard.Authority == (upgradeauthority.Guard{}) {
			guard.Authority = guard.Expected.Authority
		} else if guard.Expected.Authority != (upgradeauthority.Guard{}) && guard.Authority != guard.Expected.Authority {
			return "", ErrMutationGuard
		}
	}
	// Rotation's INSERT repeats this creation predicate after its guarded free;
	// a stale command cannot manufacture an allocation in a recreated session.
	// origin: hot-update S8 plan §6.2 / TestPortMutationRejectsRecreatedSession
	authority, err := allocationAuthoritySQL(db, guard.Authority, sid, nid)
	if err != nil {
		return "", err
	}
	predicates := []string{authority}
	// The fixed from/to order is part of deterministic SQL rendering. Iterating
	// ExpectedMaintenance would let a Go map change the replicated bytes.
	for index, home := range []string{guard.FromHome, guard.ToHome} {
		if home == "" || (index == 1 && home == guard.FromHome) {
			continue
		}
		expected, ok := guard.ExpectedMaintenance[home]
		if !ok {
			return "", ErrMutationGuard
		}
		key, err := cluster.LitText(maintenanceKeyPrefix + home)
		if err != nil {
			return "", err
		}
		var current string
		err = db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, maintenanceKeyPrefix+home).Scan(&current)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if expected != "" {
				return "", ErrMutationGuard
			}
			predicates = append(predicates, `NOT EXISTS (SELECT 1 FROM cluster_meta WHERE key=`+key+`)`)
		case err != nil:
			return "", fmt.Errorf("port: guarded maintenance lookup: %w", err)
		default:
			// origin: hot-update S6 plan §7.2 / T42
			// Empty values are malformed records, not evidence that a row is absent.
			if current == "" || current != expected {
				return "", ErrMutationGuard
			}
			if err := maintenanceAllowsMutation(current, home, guard); err != nil {
				return "", err
			}
			value, err := cluster.LitText(current)
			if err != nil {
				return "", err
			}
			predicates = append(predicates, `EXISTS (SELECT 1 FROM cluster_meta WHERE key=`+key+` AND value=`+value+`)`)
		}
	}
	return strings.Join(predicates, ` AND `), nil
}

// The record is public JSON with frozen field order, not a second private codec.
// P15 verifies signature/nonce against its challenge before caching LocalLease;
// this consumer additionally binds that cache entry to the writer's current
// canonical record. The frozen guard carries no key or challenge verifier.
func maintenanceAllowsMutation(current, home string, guard MutationGuard) error {
	var record brokermaint.Record
	if err := json.Unmarshal([]byte(current), &record); err != nil {
		return ErrMaintenanceUnknown
	}
	canonical, err := json.Marshal(record)
	if err != nil || string(canonical) != current || record.Home != home ||
		record.CutID == "" || record.Holder == "" || record.InstanceID == "" || record.IssuerBootID == "" ||
		record.Generation == 0 || record.RequiredCapability != brokermaint.Capability ||
		record.IssuedBootNS <= 0 || record.ExpiresBootNS <= record.IssuedBootNS ||
		record.ExpiresBootNS-record.IssuedBootNS > int64(8*time.Second) {
		return ErrMaintenanceUnknown
	}
	if record.Mode != "single" && record.Mode != "sole-voter" && record.Mode != "quorum" {
		return ErrMaintenanceUnknown
	}
	if record.Phase == brokermaint.MaintEnded {
		return nil
	}
	if record.Phase != brokermaint.MaintPrepared && record.Phase != brokermaint.MaintActive {
		return ErrMaintenanceUnknown
	}
	lease, ok := guard.Proofs[home]
	// An absent cache cannot protect an unobserved cut forever. An invalid
	// cache entry remains an explicit authority error, never a verified lease.
	// origin: hot-update S6 internal review rv-cluster-6
	if !ok {
		return nil
	}
	proof := lease.Proof
	normalized := lease.Record
	normalized.Phase = record.Phase
	proofRecord, _ := json.Marshal(lease.Record)
	digest := sha256.Sum256(proofRecord)
	if normalized != record || lease.ExpiresLocalBootNS <= 0 ||
		proof.CutID != record.CutID || proof.IssuerBootID != record.IssuerBootID ||
		proof.Nonce == "" || proof.Sig == "" || proof.RecordDigest != hex.EncodeToString(digest[:]) ||
		proof.ExpiresBootNS != record.ExpiresBootNS || proof.ResponseBootNS < record.IssuedBootNS ||
		proof.ResponseBootNS >= proof.ExpiresBootNS || proof.AppliedIndex == 0 {
		return ErrMaintenanceUnknown
	}
	// origin: hot-update S6 plan §7.1 / §7.2 / T42
	// Only the local fixed proof expiry is comparable with local BOOTTIME.
	// Reject extended cache entries; issuer BOOTTIME is a different clock domain.
	if lease.ExpiresLocalBootNS > guard.NowBootNS &&
		lease.ExpiresLocalBootNS-guard.NowBootNS > proof.ExpiresBootNS-proof.ResponseBootNS {
		return ErrMaintenanceUnknown
	}
	if guard.NowBootNS < lease.ExpiresLocalBootNS {
		return ErrMaintenanceProtected
	}
	return nil
}
