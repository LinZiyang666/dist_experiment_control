package cluster

import (
	"time"

	"github.com/LinZiyang666/tether/internal/cluster/state"
)

// SQL planning stays below Raft; this facade preserves the cluster API.
// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM

func LitText(s string) (string, error) {
	return state.LitText(s)
}

func MustLitText(s string) string {
	return state.MustLitText(s)
}

func LitInt(n int64) string {
	return state.LitInt(n)
}

func LitTime(t time.Time) string {
	return state.LitTime(t)
}

func LitNull() string {
	return state.LitNull()
}

func LitTextAll(ss ...string) ([]string, error) {
	return state.LitTextAll(ss...)
}
