package proto

import (
	"errors"
	"strings"

	"github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
)

// Only the wire operations belong in subject trees. Prepare/commit/finalize
// retain their legacy subjects; config is a broker forwarding operation.
// Census has a parser/builder for broker routing but grants no ctl authority.
// origin: hot-update S7 plan §6.3 / P01
type TransferOperation = xfervalue.Operation

const (
	TransferOpProbe    = xfervalue.OpProbe
	TransferOpPermit   = xfervalue.OpPermit
	TransferOpStatus   = xfervalue.OpStatus
	TransferOpSource   = xfervalue.OpSource
	TransferOpReserve  = xfervalue.OpReserve
	TransferOpTerminal = xfervalue.OpTerminal
	TransferOpCancel   = xfervalue.OpCancel
	TransferOpCensus   = xfervalue.OpCensus
)

var transferSubjectOperations = [...]struct {
	op    TransferOperation
	agent bool
}{
	{TransferOpProbe, false}, {TransferOpPermit, false}, {TransferOpStatus, false},
	{TransferOpSource, true}, {TransferOpReserve, true}, {TransferOpTerminal, true},
	{TransferOpCancel, true}, {TransferOpCensus, false},
}

func transferSubjectOperation(op TransferOperation, agent bool) bool {
	for _, entry := range transferSubjectOperations {
		if entry.op == op {
			return !agent || entry.agent
		}
	}
	return false
}

// Builders and parsers validate the same tokens, preventing a caller from
// emitting a wildcard or newline that a receiver would interpret differently.
// origin: hot-update S7 plan §6.2–§6.3 / P01
func validTransferToken(s string) bool {
	if len(s) == 0 || len(s) > 256 {
		return false
	}
	for _, c := range []byte(s) {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

func SubjCtrlTransferRequest(actor, sid, nid, tid string, op TransferOperation) (string, error) {
	if ValidateActorToken(actor) != nil || ValidateSID(sid) != nil || ValidateNID(nid) != nil ||
		!validTransferToken(tid) || !transferSubjectOperation(op, false) {
		return "", errors.New("invalid transfer control subject")
	}
	return SubjectPrefix + ".ctrl.by." + actor + ".s." + sid + ".node." + nid + ".transfer." + tid + "." + string(op) + ".req", nil
}

func ParseCtrlTransferRequest(subject string) (actor, sid, nid, tid string, op TransferOperation, ok bool) {
	p := strings.Split(subject, ".")
	if len(p) != 13 || p[0]+"."+p[1] != SubjectPrefix || p[2] != "ctrl" || p[3] != "by" ||
		p[5] != "s" || p[7] != "node" || p[9] != "transfer" || p[12] != "req" {
		return "", "", "", "", "", false
	}
	if _, err := SubjCtrlTransferRequest(p[4], p[6], p[8], p[10], TransferOperation(p[11])); err != nil {
		return "", "", "", "", "", false
	}
	return p[4], p[6], p[8], p[10], TransferOperation(p[11]), true
}

func SubjEvTransferRequest(sid, nid, tid string, op TransferOperation) (string, error) {
	if ValidateSID(sid) != nil || ValidateNID(nid) != nil || !validTransferToken(tid) || !transferSubjectOperation(op, true) {
		return "", errors.New("invalid transfer agent subject")
	}
	return SubjectPrefix + ".s." + sid + ".ev.node." + nid + ".transfer." + tid + "." + string(op) + ".req", nil
}

func ParseEvTransferRequest(subject string) (sid, nid, tid string, op TransferOperation, ok bool) {
	p := strings.Split(subject, ".")
	if len(p) != 11 || p[0]+"."+p[1] != SubjectPrefix || p[2] != "s" || p[4] != "ev" ||
		p[5] != "node" || p[7] != "transfer" || p[10] != "req" {
		return "", "", "", "", false
	}
	if _, err := SubjEvTransferRequest(p[3], p[6], p[8], TransferOperation(p[9])); err != nil {
		return "", "", "", "", false
	}
	return p[3], p[6], p[8], TransferOperation(p[9]), true
}

// Registration owners use these builders in both cold and hot tables. A
// wildcard is available only for a known operation, never arbitrary leaves.
func SubjCtrlTransferRequestWildcard(op TransferOperation) (string, error) {
	if !transferSubjectOperation(op, false) {
		return "", errors.New("unknown transfer operation")
	}
	return SubjectPrefix + ".ctrl.by.*.s.*.node.*.transfer.*." + string(op) + ".req", nil
}

func SubjEvTransferRequestWildcard(op TransferOperation) (string, error) {
	if !transferSubjectOperation(op, true) {
		return "", errors.New("unknown agent transfer operation")
	}
	return SubjectPrefix + ".s.*.ev.node.*.transfer.*." + string(op) + ".req", nil
}

// The caller obtains actorFP from the NATS-proven actor token/auth principal,
// not from this body. CallID stays the logical operation's idempotence key.
// origin: hot-update S7 plan §6.2–§6.3 / P01
func ValidateTransferSubjectIdentity(actorFP, sid, nid, tid string, identity Identity) error {
	if actorFP == "" || identity.ActorFP != actorFP || identity.SID != sid || identity.NID != nid || identity.TransferID != tid {
		return errors.New(CodeTransferIntentConflict)
	}
	return ValidateTransferWire(identity)
}

// Header names are shared with writers/readers and the wire inventory. CAS
// headers remain necessary outside the server's bounded MsgID dedupe window.
// origin: hot-update S7 plan §2.2 / P01
const (
	TransferHeaderVersion         = "Tether-Xfer-Version"
	TransferHeaderIndex           = "Tether-Xfer-Index"
	TransferHeaderOffset          = "Tether-Xfer-Offset"
	TransferHeaderLength          = "Tether-Xfer-Length"
	TransferHeaderSHA256          = "Tether-Xfer-SHA256"
	TransferHeaderIntent          = "Tether-Xfer-Intent"
	TransferHeaderMsgID           = "Nats-Msg-Id"
	TransferHeaderSubjectSequence = "Nats-Expected-Last-Subject-Sequence"
	TransferHeaderStreamSequence  = "Nats-Expected-Last-Sequence"
	TransferChunkWireVersion      = "1"
)

func validateWireIdentity(x Identity) error {
	if x == (Identity{}) {
		return nil
	}
	if ValidateSID(x.SID) != nil || ValidateNID(x.NID) != nil ||
		!validTransferText(x.ActorFP) || !validTransferToken(x.AgentInstance) ||
		!validTransferToken(x.TransferID) || !validTransferToken(x.IntentDigest) ||
		(x.EntryNonce != "" && !validTransferToken(x.EntryNonce)) ||
		(x.ObjectVersion != "" && !validTransferToken(x.ObjectVersion)) {
		return errors.New("invalid transfer identity")
	}
	return nil
}

func validateWireOrigin(x Origin) error {
	if x == (Origin{}) {
		return nil
	}
	if !validTransferToken(x.BrokerID) || !validTransferToken(x.InstanceID) || !validTransferToken(x.BootID) {
		return errors.New("invalid transfer origin")
	}
	return nil
}

func validateWireTransferProbeReq(x TransferProbeReq) error {
	if ValidateSID(x.SID) != nil || ValidateNID(x.NID) != nil {
		return errors.New("invalid transfer probe identity")
	}
	return nil
}

func validateWireTransferCensusReq(x TransferCensusReq) error {
	if ValidateSID(x.SID) != nil || x.Origin == (Origin{}) {
		return errors.New("invalid transfer census identity")
	}
	return nil
}

func validateWireTransferPermitReq(x TransferPermitReq) error {
	if !validTransferToken(x.TransferID) || !validTransferToken(x.AgentInstance) {
		return errors.New("invalid transfer permit request identity")
	}
	return nil
}
