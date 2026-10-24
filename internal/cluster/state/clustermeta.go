package state

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/brokermaint"
)

// BrokerMaintenanceKeyPrefix is the only production maintenance namespace.
// origin: hot-update S6 plan §7.1
const BrokerMaintenanceKeyPrefix = "broker_image_maintenance."

func brokerMaintenanceKey(home string) (string, error) {
	if home == "" || len(home) > 256 || strings.ContainsAny(home, "\r\n\x00") || !utf8.ValidString(home) {
		return "", errors.New("cluster: invalid maintenance home")
	}
	return BrokerMaintenanceKeyPrefix + home, nil
}

func ReadBrokerMaintenance(db *sql.DB, home string) (brokermaint.Record, error) {
	var record brokermaint.Record
	key, err := brokerMaintenanceKey(home)
	if err != nil {
		return record, err
	}
	var raw string
	if err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, key).Scan(&raw); err != nil {
		return record, err
	}
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return record, errors.New("cluster: invalid maintenance record")
	}
	if record.Home != home {
		return brokermaint.Record{}, errors.New("cluster: maintenance home mismatch")
	}
	return record, nil
}

// PlanBrokerMaintenance uses the existing literal command and a whole-record
// CAS. Phase progression never alters the original expiry or dwell baseline.
// Validation is repeated at the writer seam even for account-signed requests.
// origin: hot-update S6 plan §7.1
func PlanBrokerMaintenance(db *sql.DB, record brokermaint.Record, lock UpgradeLockState) (*Command, error) {
	key, err := brokerMaintenanceKey(record.Home)
	if err != nil {
		return nil, err
	}
	marker, err := UpgradeHolderMarker(record.Holder)
	if err != nil || marker != lock.Marker || lock.Lease == "" || record.CutID == "" || record.InstanceID == "" || record.IssuerBootID == "" || record.Generation == 0 || record.Term == 0 || record.ConfigIndex == 0 || record.VoterDigest == "" || record.BaselineDigest == "" || record.RequiredCapability != brokermaint.Capability || record.IssuedBootNS <= 0 || record.ExpiresBootNS <= record.IssuedBootNS || record.ExpiresBootNS-record.IssuedBootNS > int64(8*time.Second) {
		return nil, errors.New("cluster: invalid maintenance identity or lifetime")
	}
	if record.Mode != "quorum" && record.Mode != "sole-voter" {
		return nil, errors.New("cluster: invalid replicated maintenance mode")
	}
	for _, v := range []string{record.CutID, record.InstanceID, record.IssuerBootID, record.VoterDigest, record.BaselineDigest, record.IngressServerID} {
		if len(v) > 256 || strings.ContainsAny(v, "\r\n\x00") || !utf8.ValidString(v) {
			return nil, errors.New("cluster: invalid maintenance identifier")
		}
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	var previous string
	err = db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, key).Scan(&previous)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if errors.Is(err, sql.ErrNoRows) {
		if record.Phase != brokermaint.MaintPrepared {
			return nil, errors.New("cluster: maintenance must start prepared")
		}
	} else {
		var old brokermaint.Record
		if json.Unmarshal([]byte(previous), &old) != nil {
			return nil, errors.New("cluster: corrupt maintenance predecessor")
		}
		// A replacement holder is positive proof that the previous owner lost
		// the roll lock. Never retain an unowned cut as permanent home authority.
		// origin: hot-update S6 internal review rv-cluster-6
		unowned := old.Holder != record.Holder && old.CutID != record.CutID && record.Phase == brokermaint.MaintPrepared
		same := record
		same.Phase = old.Phase
		if same != old && !unowned && (old.Phase != brokermaint.MaintEnded || record.Phase != brokermaint.MaintPrepared || old.CutID == record.CutID) {
			return nil, errors.New("cluster: maintenance identity or expiry changed")
		}
		switch record.Phase {
		case brokermaint.MaintPrepared:
			if old.Phase != brokermaint.MaintPrepared && !unowned && (old.Phase != brokermaint.MaintEnded || old.CutID == record.CutID) {
				return nil, errors.New("cluster: maintenance phase regressed")
			}
		case brokermaint.MaintActive:
			if old.Phase != brokermaint.MaintPrepared && old.Phase != brokermaint.MaintActive {
				return nil, errors.New("cluster: maintenance phase regressed")
			}
		case brokermaint.MaintEnded:
			if old.Phase != brokermaint.MaintPrepared && old.Phase != brokermaint.MaintActive && old.Phase != brokermaint.MaintEnded {
				return nil, errors.New("cluster: invalid maintenance predecessor")
			}
		default:
			return nil, errors.New("cluster: invalid maintenance phase")
		}
	}
	guard := upgradeLockCAS(lock) + ` AND ` + metaValueCAS(key, previous)
	return NewCommand(OpClusterMetaSet, Stmt(`INSERT INTO cluster_meta(key,value) SELECT `+MustLitText(key)+`,`+MustLitText(string(raw))+` WHERE `+guard+` ON CONFLICT(key) DO UPDATE SET value=excluded.value`)), nil
}

// The leader supplies its own elapsed observation (at least MaxLifetime), or
// positive evidence that this record's holder no longer owns the lock. The
// command binds both observed rows so renewal/replacement cannot be erased.
// origin: hot-update S6 internal review rv-cluster-6
func PlanExpireBrokerMaintenance(db *sql.DB, expected brokermaint.Record, lock UpgradeLockState) (*Command, error) {
	current, err := ReadBrokerMaintenance(db, expected.Home)
	if err != nil {
		return nil, err
	}
	if current != expected || current.Phase == brokermaint.MaintEnded {
		return nil, brokermaint.ErrConflict
	}
	ended := current
	ended.Phase = brokermaint.MaintEnded
	raw, err := brokermaint.CanonicalRecordBytes(ended)
	if err != nil {
		return nil, err
	}
	old, err := brokermaint.CanonicalRecordBytes(current)
	if err != nil {
		return nil, err
	}
	key, err := brokerMaintenanceKey(expected.Home)
	if err != nil {
		return nil, err
	}
	return NewCommand(OpClusterMetaSet, Stmt(`UPDATE cluster_meta SET value=`+MustLitText(string(raw))+` WHERE key=`+MustLitText(key)+` AND `+metaValueCAS(key, string(old))+` AND `+upgradeLockCAS(lock))), nil
}

// genericExecApplier execs an op's baked statements inside the FSM's
// transaction, in Body order. It is the shared Applier for every D2 op: because
// the leader bakes ALL values into Statement.SQL as literals (sqlbake.go), the
// Apply side is a pure "exec the leader-rendered SQL" with no per-op branching
// and no out-of-band Args. (OpClusterMetaSet rides the same applier carrying its
// one string value via Args, which is JSON-round-trip-stable.)
//
// Consequence the determinism lint must own (d2-plan §6): this generic applier
// SEVERS the static call graph from a per-op Plan, so an import-reachability lint
// rooted at the Applier is tautologically clean and CANNOT prove Plan-side
// determinism. The real determinism guarantor is §13.2 multi-FSM equivalence;
// the lint is a tripwire, not a proof.
func ApplyStatements(tx *sql.Tx, cmd *Command) error {
	for i, st := range cmd.Body {
		if _, err := tx.Exec(st.SQL, st.Args...); err != nil {
			return fmt.Errorf("cluster: exec stmt %d: %w", i, err)
		}
	}
	return nil
}
