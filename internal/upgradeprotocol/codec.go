// Package upgradeprotocol owns pure signing and version encodings below proto.
// It imports no transport or execution owner; proto retains the public DTOs.
// origin: hot-update S8 plan §2.2 / §7.1 / §12.1 / P13
package upgradeprotocol

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/brokermaint"
)

// Trigger is an in-process input to the canonical encoder, not a wire DTO.
// Explicit conversion omits Sig so the signed bytes cannot include themselves.
// The maintenance record retains its established frozen serialization.
// origin: hot-update S8 plan §7.1 / P13
type Trigger struct {
	Op           string
	TargetNode   string
	TransferTo   string
	ToVersion    string
	ExpectSHA256 string
	SID          string
	IssuedAt     string
	Holder       string
	Scope        string
	CutID        string
	ActorFP      string
	Upgrade      json.RawMessage
	Maintenance  *brokermaint.Record
}

func Canonical(r *Trigger) []byte {
	if Validate(r) != nil {
		return nil
	}
	old := r.Op + "\n" + r.TargetNode + "\n" + r.TransferTo + "\n" +
		r.ToVersion + "\n" + r.ExpectSHA256 + "\n" + r.SID + "\n" + r.IssuedAt
	// Do not add even a separator on the old path: a new ctl must still be
	// able to sign an ordinary trigger that a previous broker can verify.
	if r.Holder == "" && r.Scope == "" && r.CutID == "" && r.Maintenance == nil && len(r.Upgrade) == 0 {
		return []byte(old)
	}
	// Record's frozen field order and required JSON tags are the canonical
	// schema; nil is deliberately encoded as null rather than an empty record.
	maintenance, err := json.Marshal(r.Maintenance)
	if err != nil {
		return nil
	}
	signed := old + "\nbroker-image-v1\n" + r.Holder + "\n" + r.Scope + "\n" +
		r.CutID + "\n" + base64.RawStdEncoding.EncodeToString(maintenance)
	if len(r.Upgrade) != 0 {
		// Marshal RawMessage exactly as the outer request marshaller does:
		// compaction and HTML escaping must not change the signed bytes on wire.
		// Duplicate/unknown fields are still rejected by the transaction decoder.
		// origin: hot-update S8 plan §7.1 / P13
		envelope, err := json.Marshal(r.Upgrade)
		if err != nil {
			return nil
		}
		signed += "\nupgrade-tx-v1\n" + r.ActorFP + "\n" + base64.RawStdEncoding.EncodeToString(envelope)
	}
	return []byte(signed)
}

func Validate(r *Trigger) error {
	if r == nil {
		return errors.New("missing upgrade request")
	}
	// An extension cannot ride an old op, nor an actor ride an unsigned path.
	// This is structural validation only; the adapter still checks op fields,
	// account signature, actor, target node, expiry and target-issued grant.
	// origin: hot-update S8 plan §7.1 / P13
	if r.Op == "upgrade-tx" {
		if r.TargetNode == "" || r.ActorFP == "" || len(r.Upgrade) == 0 || len(r.Upgrade) > 1<<20 || !json.Valid(r.Upgrade) {
			return errors.New("missing or invalid transaction envelope")
		}
		if r.TransferTo != "" || r.ToVersion != "" || r.ExpectSHA256 != "" || r.SID != "" || r.Maintenance != nil {
			return errors.New("legacy fields are not allowed on a transaction trigger")
		}
	} else if r.ActorFP != "" || len(r.Upgrade) != 0 {
		return errors.New("transaction extension requires transaction operation")
	}
	if r.Scope != "" && r.Scope != "whole-host" && r.Scope != "broker-daemon" {
		return errors.New("unknown upgrade scope")
	}
	identifiers := []string{r.Op, r.TargetNode, r.TransferTo, r.ToVersion,
		r.ExpectSHA256, r.SID, r.IssuedAt, r.Holder, r.Scope, r.CutID, r.ActorFP}
	if m := r.Maintenance; m != nil {
		if m.CutID != r.CutID {
			return errors.New("maintenance cut does not match upgrade cut")
		}
		identifiers = append(identifiers, m.CutID, m.Holder, m.Home, m.InstanceID,
			m.IssuerBootID, m.Mode, m.IngressServerID, m.VoterDigest,
			m.BaselineDigest, m.RequiredCapability)
	}
	for _, id := range identifiers {
		// JSON replaces invalid UTF-8, which would otherwise let distinct
		// in-memory record identifiers share the same signed representation.
		if strings.ContainsAny(id, "\r\n") || !utf8.ValidString(id) {
			return errors.New("invalid upgrade identifier")
		}
	}
	return nil
}

// These helpers retain the N-1 first/last line shapes while the proto wrapper
// stays within its unchanged legacy file budget. Only the current ABI's reader
// is advertised; no adjacent epoch or unfinished runtime capability is inferred.
// origin: hot-update S8 plan §6.1 / P13
func VersionLine(release, subjectToken string) string {
	return "tether " + release + " (proto " + subjectToken + ")"
}
func ImageABILine(epoch int) string { return "image-abi v" + strconv.Itoa(epoch) }

// ImageVersionLines advertises the S8 transaction reader of this binary: the
// required upgrade-cut-v1 adjunct domain and the three callable adapters
// (upgrade-tx-v1 journal/route, authority-incarnation-v1 writer and
// upgrade-cut-v1 importer). It is turned on only now that every component
// this one binary runs has each exporter behind them — the agent's runtime
// port, census and adjunct (FIX-AGENTWIRE/FIX-DESCRIBE) and the broker's
// store-cut exporter, identity census and adjunct importer (BROKER-EXPORTER);
// advertising earlier let a reader be selected that refused every real cut.
// The domain line keeps its position and the ABI line stays last (N-1).
// origin: hot-update S8 plan §6.1 / §11.2 P13 P17; S8 BROKER-EXPORTER R-11
func ImageVersionLines(domains, abi string) []string {
	return []string{domains + " upgrade-cut-v1", "image-read-epochs 1", "image-features upgrade-tx-v1 authority-incarnation-v1 upgrade-cut-v1", abi}
}
