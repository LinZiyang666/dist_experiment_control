// origin: hot-update S8 plan §12.1; S8 BROKERUPGRADE-SLIM responsibility extraction
package agentroute

import (
	"database/sql"
	"errors"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// agentTransactionKeyPrefix names the replicated record of the latest
// register's S8 transaction capability. The upgrade-tx subject is queue-grouped,
// so a follower must read the same fact the leader learned at register time;
// an in-memory map would refuse capable agents on N-1 of N brokers.
const agentTransactionKeyPrefix = "upgrade_tx_agent."

// AgentTransactionCapable reads the current advertisement afresh on every
// request. Absence, an unreadable row or a downgraded register is not capable.
func AgentTransactionCapable(db *sql.DB, sid, nid string) bool {
	var value string
	if db == nil || db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, agentTransactionKeyPrefix+sid+"/"+nid).Scan(&value) != nil {
		return false
	}
	return value == proto.CapUpgradeTXV1
}

// PlanAgentTransactionCapability records the latest register's advertisement,
// including a downgrade to none. It returns nil when nothing changed, so an
// idle fleet's periodic registers make no writes. A cold reinstall of an old
// binary therefore stops the broker forwarding transactions to that agent.
// origin: hot-update S8 plan §6.1 / §7.2 / §11.2 P15
func PlanAgentTransactionCapability(db *sql.DB, sid, nid string, capabilities []string) (*cluster.Command, error) {
	value := ""
	for _, capability := range capabilities {
		if capability == proto.CapUpgradeTXV1 {
			value = capability
		}
	}
	key := agentTransactionKeyPrefix + sid + "/" + nid
	var previous string
	err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, key).Scan(&previous)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if previous == value {
		return nil, nil
	}
	return cluster.NewCommand(cluster.OpClusterMetaSet, cluster.Stmt(`INSERT INTO cluster_meta(key,value) VALUES(`+cluster.MustLitText(key)+`,`+cluster.MustLitText(value)+`) ON CONFLICT(key) DO UPDATE SET value=excluded.value`)), nil
}

// RememberAgentTransaction replicates the latest register's S8 capability so
// every queue-group member routes upgrade-tx from the same fact. Only a change
// is proposed (write re-plans it under the writer); a failed write keeps the
// previous record and returns the error for the caller's log.
func RememberAgentTransaction(read *sql.DB, write func(func(*sql.DB) (*cluster.Command, error)) error, sid, nid string, capabilities []string) error {
	plan := func(db *sql.DB) (*cluster.Command, error) {
		return PlanAgentTransactionCapability(db, sid, nid, capabilities)
	}
	if read == nil || write == nil {
		return nil
	}
	if command, err := plan(read); err != nil || command == nil {
		return err
	}
	return write(plan)
}
