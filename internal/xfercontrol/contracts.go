// Package xfercontrol exposes its frozen transfer contracts here.
// Implementation constructors belong to the leaf owner; aliases keep config
// ports identical without a reverse dependency from xfercontract.
// origin: hot-update S7 plan §8.1 / P00
package xfercontrol

import "github.com/LinZiyang666/tether/internal/xfercontract"

type Config = xfercontract.ControlConfig
type Controller = xfercontract.Controller
type Factory = xfercontract.ControlFactory

// PullSelectionProbeReq extends the read-only capability probe with a source
// inspection. An absent path preserves the frozen capability-only request;
// this query never obtains a permit, pins a worker or opens an OBJ attachment.
// origin: hot-update S7 plan §2 / §7 / S7 internal review INTEG-O F4
type PullSelectionProbeReq struct {
	CallID     string `json:"call_id"`
	SID        string `json:"sid"`
	NID        string `json:"nid"`
	SourcePath string `json:"source_path,omitempty"`
}

// The pointer distinguishes an empty regular file from an unanswered inspection.
// Size is selection evidence only; prepare independently pins and seals its
// source, so a pathname replacement cannot rewrite the admitted engine.
// origin: hot-update S7 plan §6.1 / §7 / S7 internal review INTEG-O F4
type PullSelectionProbeResp struct {
	Version             string `json:"version"`
	HistoryGuardVersion string `json:"history_guard_version"`
	BucketConfigVersion string `json:"bucket_config_version"`
	InstanceID          string `json:"instance_id"`
	MaxPayload          int64  `json:"max_payload"`
	MaxMsgSize          int64  `json:"max_msg_size"`
	Code                string `json:"code"`
	Error               string `json:"error"`
	SourceSize          *int64 `json:"source_size,omitempty"`
}
