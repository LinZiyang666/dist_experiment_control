// Package upgradecut describes the required sealed upgrade adjunct.
// It is separate from the existing closed image/private codecs. P07 validates
// it before taking any FD; P17 adapts the final S7 runtime interfaces to it.
// origin: hot-update S8 plan §2.2 / §6.1 / P00
package upgradecut

import (
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

const Version = "upgrade-cut-v1"

type Authority struct {
	SID         string `json:"sid"`
	Incarnation string `json:"incarnation"`
}

// A broker carries the complete authority map, not one session's incarnation.
// LockFD is an auxiliary index, not an independently reopened lock pathname.
type State struct {
	Attempt     upgradetx.Handle `json:"attempt"`
	SourceSHA   string           `json:"source_sha"`
	TargetSHA   string           `json:"target_sha"`
	ParentToken string           `json:"parent_token"`
	Authorities []Authority      `json:"authorities"`
	Owner       upgradetx.Owner  `json:"owner"`
	LockFD      uint32           `json:"lock_fd"`
	Scope       upgradetx.Scope  `json:"scope"`
}

type EncodeFunc = func(state State) ([]byte, error)
type DecodeFunc = func(data []byte) (State, error)
type TakeFunc = func(state State, takeFD func(uint32) (uintptr, error)) (upgradelock.Lease, error)
