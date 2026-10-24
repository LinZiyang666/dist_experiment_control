package rollcontrol

import (
	"database/sql"
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/upgraderoll"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// A committed reservation binding distinguishes a first acquire from a lost
// holder. Resume may join the exact live holder or its release tombstone; it
// cannot recreate authority after expiry, unlock or another parent.
// origin: hot-update S8 plan §3.1 / §5.1; S8 internal review cluster-roll F8
func reserveCluster(node *cluster.Node, r Request) error {
	if upgraderoll.ValidatePlan(r.Plan) != nil || !upgradetx.SameCut(r.Plan.Parent, r.Parent.Handle) || r.Parent.ScopeDigest != upgraderoll.ScopeDigest(r.Plan.Scope) || r.Plan.Deadline != r.Parent.ExpiresAt {
		return Unknown("invalid original reservation plan")
	}
	binding := "upgrade_roll_reserved." + r.Parent.Handle.TxID
	err := node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		var prior string
		err := db.QueryRow("SELECT value FROM cluster_meta WHERE key=?", binding).Scan(&prior)
		if err == nil {
			if prior != Hash(r.Plan) {
				return nil, Unknown("reservation plan changed")
			}
			var released string
			e := db.QueryRow("SELECT value FROM cluster_meta WHERE key=?", releaseKey(r.Parent)).Scan(&released)
			if e == nil && released == prior {
				return nil, nil
			}
			state, e := cluster.ReadUpgradeLock(db)
			marker, _ := cluster.UpgradeHolderMarker(r.Parent.Handle.TxID)
			expiry, x := time.Parse(time.RFC3339Nano, state.Lease)
			if e != nil || x != nil || state.Marker != marker || !time.Now().Before(expiry) {
				return nil, Unknown("original reservation expired or changed")
			}
			return nil, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if time.Now().UnixNano() >= r.Plan.Deadline {
			return nil, Unknown("original parent deadline expired")
		}
		ops, err := cluster.NonTerminalOperations(db)
		if err != nil {
			return nil, err
		}
		if len(ops) != 0 {
			return nil, Unknown("membership operation is in flight")
		}
		cmd, err := cluster.PlanSetUpgradeActiveExpected(db, r.Parent.Handle.TxID, time.Now())
		if err != nil {
			return nil, err
		}
		marker, _ := cluster.UpgradeHolderMarker(r.Parent.Handle.TxID)
		cmd.Body = append(cmd.Body, cluster.Stmt("INSERT INTO cluster_meta(key,value) SELECT "+cluster.MustLitText(binding)+","+cluster.MustLitText(Hash(r.Plan))+" WHERE EXISTS(SELECT 1 FROM cluster_meta WHERE key="+cluster.MustLitText(cluster.MetaKeyUpgradeActive)+" AND value="+cluster.MustLitText(marker)+")"))
		return cmd, nil
	})
	if err != nil {
		return err
	}
	var saved string
	if err = node.RODB().QueryRow("SELECT value FROM cluster_meta WHERE key=?", binding).Scan(&saved); err != nil || saved != Hash(r.Plan) {
		return Unknown("reservation binding has no committed readback")
	}
	return nil
}
