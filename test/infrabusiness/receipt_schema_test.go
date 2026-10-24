//go:build linux && infrabusiness_integration

package infrabusiness_test

import (
	i "github.com/LinZiyang666/tether/internal/infracontract"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// These tests keep the independent business ledger separate from NC receipts.
// A new authenticated CID is expected during a cut; a replacement application
// socket, child or invocation is an interruption even if its final hash matches.
// The live producer is P32, not an implementation hidden in this acceptance test.
// Missing supply fails explicitly; oracle controls never stand in for that lane.
const businessReceiptVersion = 2

type businessReceipt struct {
	TransferSealNS     int64
	TransferSealSHA256 string
	Version            uint16
	ObserverBootID     string
	TransferFinals     []transferFinalReceipt
	TransferPhases     []transferPhaseReceipt
	RequiredPhases     []phaseRequirement
	Scenario           string
	SourceSHA256       string
	SchemaSHA256       string
	DependencyOrder    []string
	Executed           []string
	Hosts              []string
	Required           []businessIdentity
	Hops               []hopReceipt
	Streams            []streamReceipt
	Slots              []slotReceipt
	WindowNS           int64
	Auth               []authReceipt
	Routes             []routeReceipt
	Transfers          []transferReceipt
	Faults             []faultReceipt
	Addresses          []addressReceipt
	RequiredNC         []ncIdentity
	Attachments        []attachmentReceipt
}

type businessIdentity struct {
	ID, Role, Nonce, TaskID, TransferID, NUID string
	Direction, Session, Bucket, SHA256        string
	AuditIdentity                             x.Identity
	PID, CTLPID                               int32
	StartTicks, CTLStartTicks, Device, Inode  uint64
	Cookie                                    uint64
	Size, RateLimitBPS, StartedNS, DeadlineNS int64
}

type hopReceipt struct {
	Step                          uint32
	Tx                            i.ID
	BootID                        string
	RequiredNC                    []cutNC
	RequiredAuth                  []authRequirement
	Host, Component, Action       string
	StartNS, CutNS, EndNS         int64
	Before, Current               i.Process
	OldExited, TemporaryExited    bool
	LiveBusiness, OldOwnerPending uint32
	Evidence                      string
}

type streamReceipt struct {
	Original, Current  businessIdentity
	Accepts, Redials   uint32
	CarrierTransition  string
	UpBytes, DownBytes uint64
	UpSent, UpRead     string
	DownSent, DownRead string
	MaxSilenceNS       int64
	FINUp, FINDown     bool
	Revoked            bool
	NativeRC           int
	Controls           []string
}

type slotReceipt struct {
	Index                                 int
	ScheduledNS, DispatchedNS, TerminalNS int64
	Kind, ID, RequestDigest, EffectDigest string
	Effects                               uint32
	NativeRC                              int
	Attempts                              []attemptReceipt
}

type attemptReceipt struct {
	StartNS, EndNS int64
	Code           string
	Sent           bool
}

type faultReceipt struct {
	Kind, RequestDigest, SentEvidence, EffectEvidence, ReplyCutEvidence string
	SentRequestDigest, EffectRequestDigest, ReplyRequestDigest          string
	Effects                                                             uint32
}

type addressReceipt struct {
	Case, BrokerURL, TunnelAddr, ConfigDigest string
	Flagless, Connected, Rejected             bool
	NATSCID                                   uint64
	TunnelCookie                              uint64
}

type fixtureArtifact struct {
	Kind, Path, SHA256, Version string
	Race                        bool
}

type fixtureSupply struct {
	Version                    uint16
	Root, Workload             string
	SourceSHA256, SchemaSHA256 string
	Artifacts                  []fixtureArtifact
}
