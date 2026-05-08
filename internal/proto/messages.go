package proto

import "time"

// SessionCreateReq — ctl pub on ctrl.by.<A>.session.create.req.
type SessionCreateReq struct {
	Name string `json:"name"`
	PIN  string `json:"pin,omitempty"` // optional: tetherd random-generates if empty
}

// SessionCreateResp — tetherd reply.
type SessionCreateResp struct {
	SID       string    `json:"sid"`
	OwnerFP   string    `json:"owner_fp"`
	CreatedAt time.Time `json:"created_at"`
	Error     string    `json:"error,omitempty"`
}

// NodeRegisterReq — agent pub on ctrl.s.<S>.node.<N>.register.req.
// First field is proto_version per J.2 strict same-version handshake.
// More reconciliation fields (boot_id, local_processes[], local_ports[]) land in P8.
type NodeRegisterReq struct {
	ProtoVersion   int    `json:"proto_version"`
	ReleaseVersion string `json:"release_version"`
	NID            string `json:"nid"`
	OS             string `json:"os"`
	Arch           string `json:"arch"`
	BootID         string `json:"boot_id,omitempty"`
}

// NodeRegisterResp — tetherd reply with reconciliation directives (G.1).
type NodeRegisterResp struct {
	OK    bool   `json:"ok"`
	Code  string `json:"code,omitempty"`  // e.g. "proto_mismatch", "session_not_found_or_deleting"
	Error string `json:"error,omitempty"` // human-readable; populated when OK=false
	// Reconciliation arrays land in P8.
}

// HeartbeatPayload — agent core pub on ctrl.s.<S>.node.<N>.heartbeat (no reply).
type HeartbeatPayload struct {
	Ts time.Time `json:"ts"`
}

// ErrorReply is the canonical shape for any req that responds with an error
// instead of its normal success body. Code is machine-readable; Message is for
// humans.
type ErrorReply struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
