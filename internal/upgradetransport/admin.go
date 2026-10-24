// Package upgradetransport owns local upgrade request/reply lifetimes.
package upgradetransport

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// CallBrokerAdmin uses one caller deadline across dial/write/read. Closing the
// socket on cancellation interrupts IO and AfterFunc's completion is joined, so
// a cancelled roll cannot leave a transport goroutine or descriptor behind.
// origin: hot-update S6 plan §5 / T45
func CallBrokerAdmin(ctx context.Context, path string, req adminsock.Request) (*adminsock.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerAdminBudget(req))
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	joined := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = conn.Close(); close(joined) })
	defer func() {
		if !stop() {
			<-joined
		}
	}()
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, err
	}
	var resp adminsock.Response
	// Bound the response, including a malicious server that never sends LF.
	line, err := bufio.NewReaderSize(conn, 64<<10).ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(line, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// A transaction mutation owns its reply until the durable cut is armed and
// the server can drain it before exec. The ordinary five-second read budget
// closed that socket during legitimate arm work, making DrainArmed reject the
// accepted cut as a broken reply. The caller's earlier absolute deadline still
// wins; ordinary business/read requests retain their control latency budget.
// origin: S8 internal review activation-and-route-migration F7
func brokerAdminBudget(req adminsock.Request) time.Duration {
	if req.Upgrade != nil && (req.Upgrade.Op == upgradewire.OpStart || req.Upgrade.Op == upgradewire.OpRollback) {
		return upgradebudget.ComponentHandover
	}
	return upgradebudget.RequestLatency
}
