package brokermaint

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
)

// A maintenance record is an authority boundary, not a component watchdog.
// origin: hot-update S6 plan §7.1 / T39
const (
	MaxLifetime    = 8 * time.Second
	MaxRecordBytes = 16 << 10
	MetaKeyPrefix  = "broker_image_maintenance."
)

var (
	ErrInvalid  = errors.New("broker maintenance: invalid record or proof")
	ErrExpired  = errors.New("broker maintenance: expired")
	ErrIdentity = errors.New("broker maintenance: identity mismatch")
	ErrConflict = errors.New("broker maintenance: record conflict")
)

// Identifiers have one bounded, unambiguous representation across SQL, JSON and
// signed messages. Errors deliberately exclude input and signature material.
func validIdentifier(s string) bool {
	if len(s) == 0 || len(s) > 256 {
		return false
	}
	for _, c := range []byte(s) {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && !strings.ContainsRune("._-", rune(c)) {
			return false
		}
	}
	return true
}

func validDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range []byte(s) {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func ValidateRecord(r Record) error {
	for _, id := range []string{r.CutID, r.Holder, r.InstanceID, r.IssuerBootID} {
		if !validIdentifier(id) {
			return ErrInvalid
		}
	}
	if r.Generation == 0 || r.RequiredCapability != Capability || !validDigest(r.BaselineDigest) {
		return ErrInvalid
	}
	switch r.Phase {
	case MaintPrepared, MaintActive, MaintEnded:
	default:
		return ErrInvalid
	}
	// Ordered nonnegative operands make subtraction safe even near MaxInt64.
	if r.IssuedBootNS < 0 || r.ExpiresBootNS <= r.IssuedBootNS || r.ExpiresBootNS-r.IssuedBootNS > int64(MaxLifetime) {
		return ErrInvalid
	}
	switch r.Mode {
	case "single":
		if r.Home != "" || !validIdentifier(r.IngressServerID) || r.Term != 0 || r.ConfigIndex != 0 || r.VoterDigest != "" {
			return ErrInvalid
		}
	case "sole-voter", "quorum":
		if !validIdentifier(r.Home) || r.IngressServerID != "" || r.Term == 0 || r.ConfigIndex == 0 || !validDigest(r.VoterDigest) {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}

// CanonicalRecordBytes retains the frozen declaration order and explicit zeros.
// This is also the literal value replicated through existing cluster_meta ops.
func CanonicalRecordBytes(r Record) ([]byte, error) {
	if err := ValidateRecord(r); err != nil {
		return nil, err
	}
	return json.Marshal(r)
}

// DecodeRecord accepts only the exact canonical form. This rejects duplicates,
// omitted zeros, unknown fields and alternate number encodings, even if a
// permissive JSON reader would produce the same Go value.
func DecodeRecord(b []byte) (Record, error) {
	var r Record
	if len(b) == 0 || len(b) > MaxRecordBytes {
		return r, ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(&r) != nil {
		return Record{}, ErrInvalid
	}
	if d.Decode(new(any)) != io.EOF {
		return Record{}, ErrInvalid
	}
	want, err := CanonicalRecordBytes(r)
	if err != nil || !bytes.Equal(b, want) {
		return Record{}, ErrInvalid
	}
	return r, nil
}

func RecordDigest(r Record) (string, error) {
	b, err := CanonicalRecordBytes(r)
	if err != nil {
		return "", err
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:]), nil
}

// Phase changes cannot rewrite the original issuer expiry, topology or dwell
// baseline. Aborted preparation may end without ever entering ACTIVE.
// origin: hot-update S6 plan §7.1 / T39
func ValidateTransition(previous, next Record) error {
	if err := ValidateRecord(previous); err != nil {
		return err
	}
	if err := ValidateRecord(next); err != nil {
		return err
	}
	copy := next
	copy.Phase = previous.Phase
	if copy != previous {
		return ErrIdentity
	}
	switch previous.Phase {
	case MaintPrepared:
		if next.Phase != MaintPrepared && next.Phase != MaintActive && next.Phase != MaintEnded {
			return ErrConflict
		}
	case MaintActive:
		if next.Phase != MaintActive && next.Phase != MaintEnded {
			return ErrConflict
		}
	case MaintEnded:
		if next.Phase != MaintEnded {
			return ErrConflict
		}
	default:
		return ErrInvalid
	}
	return nil
}

// These unsigned shapes intentionally omit Sig, including the nested proof.
// They avoid a proto import cycle; P25 projects its frozen wire carriers here.
type unsignedProof struct {
	CutID          string `json:"cut_id"`
	Nonce          string `json:"nonce"`
	IssuerBootID   string `json:"issuer_boot_id"`
	ResponseBootNS int64  `json:"response_boot_ns"`
	ExpiresBootNS  int64  `json:"expires_boot_ns"`
	RecordDigest   string `json:"record_digest"`
	AppliedIndex   uint64 `json:"applied_index"`
}

func proofBody(p Proof) (unsignedProof, error) {
	if !validIdentifier(p.CutID) || !validIdentifier(p.Nonce) || !validIdentifier(p.IssuerBootID) || !validDigest(p.RecordDigest) || p.ResponseBootNS < 0 || p.ExpiresBootNS < 0 {
		return unsignedProof{}, ErrInvalid
	}
	return unsignedProof{p.CutID, p.Nonce, p.IssuerBootID, p.ResponseBootNS, p.ExpiresBootNS, p.RecordDigest, p.AppliedIndex}, nil
}

func CanonicalProofBytes(p Proof) ([]byte, error) {
	u, err := proofBody(p)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(u)
	return append([]byte("broker-maint-proof-v1\n"), b...), err
}

// ACKs bind this digest of the complete unsigned proof, not merely a CutID.
// Signature verification remains mandatory before installing that proof.
func ProofDigest(p Proof) (string, error) {
	b, err := CanonicalProofBytes(p)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(b)
	return hex.EncodeToString(digest[:]), nil
}

func CanonicalRequestBytes(r Record, p Proof, nid, connectedBrokerID, ingressServerID, routeDigest string) ([]byte, error) {
	if err := ValidateRecord(r); err != nil {
		return nil, err
	}
	// A proof-less request is the distinct nonce challenge leg, never a hold.
	// origin: hot-update S6 internal review rv-cluster-4
	challenge := p == (Proof{})
	checkedRoute := routeDigest
	if challenge && routeDigest == "" {
		checkedRoute = strings.Repeat("0", 64)
	}
	if err := validateRoute(r.Mode, nid, connectedBrokerID, ingressServerID, checkedRoute); err != nil {
		return nil, err
	}
	if r.Mode == "single" && ingressServerID != r.IngressServerID || r.Mode != "single" && connectedBrokerID != r.Home {
		return nil, ErrIdentity
	}
	u := unsignedProof{}
	var err error
	if !challenge {
		u, err = proofBody(p)
	}
	if err != nil {
		return nil, err
	}
	if challenge && (r.Phase != MaintPrepared || routeDigest != "") {
		return nil, ErrInvalid
	}
	digest, err := RecordDigest(r)
	if err != nil || !challenge && (p.CutID != r.CutID || p.IssuerBootID != r.IssuerBootID || p.ExpiresBootNS != r.ExpiresBootNS || p.RecordDigest != digest) {
		return nil, ErrIdentity
	}
	b, err := json.Marshal(struct {
		Record            Record        `json:"record"`
		Proof             unsignedProof `json:"proof"`
		NID               string        `json:"nid"`
		ConnectedBrokerID string        `json:"connected_broker_id"`
		IngressServerID   string        `json:"ingress_server_id"`
		RouteDigest       string        `json:"route_digest"`
	}{r, u, nid, connectedBrokerID, ingressServerID, routeDigest})
	return append([]byte("broker-maint-request-v1\n"), b...), err
}

func validateRoute(mode, nid, connected, ingress, route string) error {
	if !validIdentifier(nid) || !validDigest(route) {
		return ErrInvalid
	}
	if mode == "single" {
		if connected != "" || !validIdentifier(ingress) {
			return ErrInvalid
		}
	} else if !validIdentifier(connected) || ingress != "" {
		return ErrInvalid
	}
	return nil
}

func CanonicalAckBytes(cutID, nonce, nid, connectedBrokerID, ingressServerID, instanceID, routeDigest, proofDigest string, accepted bool, remainingNS int64) ([]byte, error) {
	if !validIdentifier(cutID) || !validIdentifier(nonce) || !validIdentifier(instanceID) || (proofDigest == "" && (accepted || remainingNS != 0) || proofDigest != "" && !validDigest(proofDigest)) || remainingNS < 0 || remainingNS > int64(MaxLifetime) {
		return nil, ErrInvalid
	}
	mode := "quorum"
	if connectedBrokerID == "" {
		mode = "single"
	}
	if err := validateRoute(mode, nid, connectedBrokerID, ingressServerID, routeDigest); err != nil {
		return nil, err
	}
	b, err := json.Marshal(struct {
		CutID             string `json:"cut_id"`
		Nonce             string `json:"nonce"`
		NID               string `json:"nid"`
		ConnectedBrokerID string `json:"connected_broker_id"`
		IngressServerID   string `json:"ingress_server_id"`
		InstanceID        string `json:"instance_id"`
		RouteDigest       string `json:"route_digest"`
		ProofDigest       string `json:"proof_digest"`
		Accepted          bool   `json:"accepted"`
		RemainingNS       int64  `json:"remaining_ns"`
	}{cutID, nonce, nid, connectedBrokerID, ingressServerID, instanceID, routeDigest, proofDigest, accepted, remainingNS})
	return append([]byte("broker-maint-ack-v1\n"), b...), err
}
