package brokermaint

import "math"

// Challenge belongs to the observer's kernel clock. Keep it with the cache in
// the image: a nonce without its original t0 cannot safely reconstruct expiry.
// origin: hot-update S6 plan §7.1 / T39
type Challenge struct {
	Nonce          string `json:"nonce"`
	ObserverBootID string `json:"observer_boot_id"`
	IssuerBootID   string `json:"issuer_boot_id"`
	SentBootNS     int64  `json:"sent_boot_ns"`
}

// Signatures are supplied by the pinned identity owner. The leaf neither loads
// credentials nor chooses a verification key from untrusted proof fields.
type VerifySignature func([]byte, string) error
type SignProof func([]byte) (string, error)

func MakeProof(r Record, nonce, issuerBootID string, responseBootNS int64, appliedIndex uint64, sign SignProof) (Proof, error) {
	if err := ValidateRecord(r); err != nil {
		return Proof{}, err
	}
	if issuerBootID == "" || issuerBootID != r.IssuerBootID {
		return Proof{}, ErrIdentity
	}
	if responseBootNS < r.IssuedBootNS {
		return Proof{}, ErrInvalid
	}
	if responseBootNS >= r.ExpiresBootNS {
		return Proof{}, ErrExpired
	}
	digest, err := RecordDigest(r)
	if err != nil {
		return Proof{}, err
	}
	p := Proof{CutID: r.CutID, Nonce: nonce, IssuerBootID: issuerBootID, ResponseBootNS: responseBootNS, ExpiresBootNS: r.ExpiresBootNS, RecordDigest: digest, AppliedIndex: appliedIndex}
	b, err := CanonicalProofBytes(p)
	if err != nil || sign == nil {
		return Proof{}, ErrInvalid
	}
	p.Sig, err = sign(b)
	if err != nil || p.Sig == "" {
		return Proof{}, ErrInvalid
	}
	return p, nil
}

// ObserveProof consumes network RTT and queuing from the issuer's fixed expiry.
// It never compares the two machines' BOOTTIME values and never uses wall time.
// A later proof, leadership change or replay may shorten, but never renew, a cut.
// origin: hot-update S6 plan §7.1 / T39
func ObserveProof(r Record, p Proof, c Challenge, observerBootID string, nowBootNS int64, minimumAppliedIndex uint64, previous *LocalLease, verify VerifySignature) (LocalLease, error) {
	if err := ValidateRecord(r); err != nil {
		return LocalLease{}, err
	}
	if r.Phase == MaintEnded {
		return LocalLease{}, ErrExpired
	}
	if observerBootID == "" || c.ObserverBootID != observerBootID || c.IssuerBootID != r.IssuerBootID || p.IssuerBootID != r.IssuerBootID || c.Nonce != p.Nonce || p.CutID != r.CutID {
		return LocalLease{}, ErrIdentity
	}
	if !validIdentifier(c.ObserverBootID) || c.SentBootNS < 0 || nowBootNS < c.SentBootNS || p.ResponseBootNS < r.IssuedBootNS || p.ExpiresBootNS != r.ExpiresBootNS || p.AppliedIndex < minimumAppliedIndex {
		return LocalLease{}, ErrInvalid
	}
	digest, err := RecordDigest(r)
	if err != nil || p.RecordDigest != digest {
		return LocalLease{}, ErrIdentity
	}
	b, err := CanonicalProofBytes(p)
	if err != nil || verify == nil || p.Sig == "" {
		return LocalLease{}, ErrInvalid
	}
	if verify(b, p.Sig) != nil {
		return LocalLease{}, ErrIdentity
	}
	if p.ResponseBootNS >= p.ExpiresBootNS {
		return LocalLease{}, ErrExpired
	}
	remaining := p.ExpiresBootNS - p.ResponseBootNS
	if c.SentBootNS > math.MaxInt64-remaining {
		return LocalLease{}, ErrInvalid
	}
	expiry := c.SentBootNS + remaining
	if previous != nil {
		if ValidateTransition(previous.Record, r) != nil || previous.ExpiresLocalBootNS < 0 {
			return LocalLease{}, ErrIdentity
		}
		if previous.ExpiresLocalBootNS < expiry {
			expiry = previous.ExpiresLocalBootNS
		}
	}
	if nowBootNS >= expiry {
		return LocalLease{}, ErrExpired
	}
	return LocalLease{Record: r, ExpiresLocalBootNS: expiry, Proof: p}, nil
}

// Protects requires exact current replicated bytes as well as a still-live
// local observation. A cache for an old term/cut cannot guard a new allocation.
func Protects(lease LocalLease, current Record, nowBootNS int64) bool {
	return current.Phase != MaintEnded && nowBootNS >= 0 && nowBootNS < lease.ExpiresLocalBootNS && MatchesLease(lease, current)
}

// Matching a historical baseline remains meaningful after ENDED, while that
// phase can never protect writes. The validated cache owns the local clock map.
func MatchesLease(lease LocalLease, current Record) bool {
	if ValidateRecord(current) != nil || !sameProtectedRecord(lease.Record, current) || lease.ExpiresLocalBootNS <= 0 {
		return false
	}
	p := lease.Proof
	digest, err := RecordDigest(lease.Record)
	return err == nil && p.Nonce != "" && p.Sig != "" && p.CutID == current.CutID && p.IssuerBootID == current.IssuerBootID && p.RecordDigest == digest && p.ExpiresBootNS == current.ExpiresBootNS && p.ResponseBootNS >= current.IssuedBootNS && p.ResponseBootNS < p.ExpiresBootNS && (current.Mode == "single" || p.AppliedIndex > 0)
}

// A replicated phase advance preserves the signed original deadline. The proof
// remains bound to its original phase; all other fields must still match.
// origin: hot-update S6 internal review rv-cluster-6
func sameProtectedRecord(cached, current Record) bool {
	return ValidateTransition(cached, current) == nil
}
