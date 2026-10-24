// Package clusterio defines neutral tracked transport contracts without Raft or NATS.
// origin: hot-update S6 plan §2.1 / §12.1 P00
package clusterio

import (
	"net"

	"github.com/LinZiyang666/tether/internal/controlbudget"
)

type SnapshotTicket struct {
	RPCID        uint64 `json:"rpc_id"`
	Index        uint64 `json:"index"`
	Term         uint64 `json:"term"`
	ConfigIndex  uint64 `json:"config_index"`
	ConfigDigest string `json:"config_digest"`
	PayloadBytes int64  `json:"payload_bytes"`
}

// Tracker fences new RPCs and joins the actual accepted connection owners.
type Tracker interface {
	Track(net.Conn) (net.Conn, error)
	Fence(controlbudget.Deadline) error
	Join(controlbudget.Deadline) error
	Counts() (connections uint32, readers uint32)
}
