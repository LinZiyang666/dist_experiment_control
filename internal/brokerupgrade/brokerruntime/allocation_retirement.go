package brokerruntime

import (
	"database/sql"
	"strconv"
	"strings"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// ReleasedAllocation joins the journal's original allocation/epoch to the
// current store's token hash and the imported tunnel's sealed revoke. A deleted or
// reused row, a disconnected listener, or an unrelated terminal is no proof.
// The store retains only the hash; no raw token is read or reconstructed.
// origin: hot-update S8 plan §4.2 / §6.2; S8 internal review P13-COHORT F2
func ReleasedAllocation(db *sql.DB, domain, id, origin string, released func(int, string, string, string) bool) bool {
	if db == nil || released == nil || domain != string(upgradetx.DomainBrokerRuntime) || !strings.HasPrefix(id, "tunnel:") {
		return false
	}
	port, err := strconv.Atoi(strings.TrimPrefix(id, "tunnel:"))
	if err != nil || port <= 0 || port > 65535 || id != "tunnel:"+strconv.Itoa(port) {
		return false
	}
	var sid, nid, hash string
	var epoch int64
	if db.QueryRow("SELECT sid,nid,token_hash,epoch FROM port_allocations WHERE port=? AND state='ALLOCATED'", port).Scan(&sid, &nid, &hash, &epoch) != nil ||
		origin != "allocation:"+sid+"/"+nid+"@"+strconv.FormatInt(epoch, 10) || hash == "" {
		return false
	}
	return released(port, sid, nid, hash)
}
